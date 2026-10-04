//! The driver the scenarios run on: records submitted one at a time to a
//! [`Backend`], and every completion checked as it is reaped, as the
//! simulator checks its own (simulator.md, 5): a valid completion of
//! the operation's shape, one per submission, with the record handed back,
//! each of its boxes the same `Box`, written only where the contract says.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::mem;

use skein_io::kernel::{Addr, Complete, Done, Entry, Error, Family, Fd, Kind, Op, OpenHow, Stat, Submit};
use skein_lib::{Duration, Queue, Time, Token};

use crate::{Backend, Item};

/// How long a scenario waits for a completion it expects before it fails:
/// on the ring, a SYN retransmitted after a full accept queue made room
/// takes a second.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);

/// How long a scenario waits for a completion it expects may not come, such
/// as a connect to a full accept queue or a send to a peer that does not
/// read.
pub(crate) const BRIEFLY: Duration = Duration::from_millis(300);

/// How many cancels of one target a scenario submits, when the backend
/// cannot submit them.
const ATTEMPTS: u32 = 32;

/// How long a retry waits for an answer that needs the peer's reset or
/// acknowledgement to have arrived.
const RETRY: Duration = Duration::from_millis(1);

/// Room for the completions of one reap.
const ROOM: u32 = 64;

/// The bytes each receive of a transfer asks for.
const CHUNK: usize = 4096;

/// A scenario's view of a backend.
#[derive(Debug)]
pub(crate) struct Run<'b, B: Backend> {
    backend: &'b mut B,
    next: u64,
    submissions: Queue<Submit>,
    completions: Queue<Complete>,
    /// Submitted, not yet completed.
    flights: BTreeMap<Token, Flight<B::Process>>,
    /// Completed, not yet taken by the scenario.
    arrived: BTreeMap<Token, Complete>,
    /// Every descriptor issued and not yet closed, by process.
    open: BTreeSet<(B::Process, Fd)>,
    processes: Vec<B::Process>,
}

/// What a submission was, to check its completion hands it back.
#[derive(Debug)]
struct Flight<P> {
    process: P,
    summary: Summary,
    /// Each box of bytes the record carries (a buffer, a path, a name): its
    /// address, which the completion hands back in the same `Box`, and what
    /// it held.
    boxes: Vec<(usize, Box<[u8]>)>,
    /// A `List`'s entries: their address, and what they held.
    entries: Option<(usize, Box<[Entry]>)>,
}

impl<'b, B: Backend> Run<'b, B> {
    pub(crate) fn new(backend: &'b mut B) -> Run<'b, B> {
        Run {
            backend,
            next: 1,
            submissions: Queue::with_capacity(1),
            completions: Queue::with_capacity(ROOM),
            flights: BTreeMap::new(),
            arrived: BTreeMap::new(),
            open: BTreeSet::new(),
            processes: Vec::new(),
        }
    }

    pub(crate) fn process(&mut self) -> B::Process {
        let process = self.backend.open();
        self.processes.push(process);
        process
    }

    /// Submits `op` on `process`, which the backend takes now.
    pub(crate) fn start(&mut self, process: B::Process, op: Op) -> Token {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).expect("tokens never run out");
        let mut boxes = Vec::new();
        for held in boxes_of(&op) {
            boxes.push((held.as_ptr().addr(), Box::from(held)));
        }
        let entries = entries_of(&op).map(|held| (held.as_ptr().addr(), Box::from(held)));
        self.flights.insert(token, Flight { process, summary: Summary::of(&op), boxes, entries });
        self.submissions.push(Submit { op: token, kind: op });
        self.backend.submit(process, &mut self.submissions);
        assert!(self.submissions.is_empty(), "the backend takes every record of a scenario");
        token
    }

    /// The completion of `token`, which must come.
    pub(crate) fn wait(&mut self, process: B::Process, token: Token) -> Complete {
        let complete = self.within(process, token, PATIENCE);
        complete.expect("an operation that can complete does, within the patience of the suite")
    }

    /// The completion of `token` if it comes within `bound`.
    pub(crate) fn within(&mut self, process: B::Process, token: Token, bound: Duration) -> Option<Complete> {
        let deadline = self.later(bound);
        loop {
            self.reap(process);
            if let Some(complete) = self.arrived.remove(&token) {
                return Some(complete);
            }
            let now = self.backend.now();
            if now >= deadline {
                return None;
            }
            self.backend.pass(process, deadline.saturating_since(now));
        }
    }

    /// The completion of whichever of two operations, each on its own
    /// process or both on one, comes first: its token says which.
    pub(crate) fn either(&mut self, a: (B::Process, Token), b: (B::Process, Token)) -> Complete {
        let deadline = self.later(PATIENCE);
        loop {
            self.reap(a.0);
            self.reap(b.0);
            for token in [a.1, b.1] {
                if let Some(complete) = self.arrived.remove(&token) {
                    return complete;
                }
            }
            let now = self.backend.now();
            assert!(now < deadline, "one of two operations that can complete does, within the patience of the suite");
            self.backend.pass(a.0, deadline.saturating_since(now));
        }
    }

    /// Whether `token` has completed and been reaped.
    pub(crate) fn arrived(&self, token: Token) -> bool {
        self.arrived.contains_key(&token)
    }

    /// Cancels `target`, in flight on `process`: the answer of every
    /// `Cancel` submitted, and the target's completion.
    pub(crate) fn cancel(&mut self, process: B::Process, target: Token) -> (Vec<Result<Done, Error>>, Complete) {
        let cancel = self.start(process, Op::Cancel { target });
        self.settle_cancel(process, target, cancel)
    }

    /// Waits for `cancel` and for its `target`, both on `process`. A cancel
    /// the backend could not submit leaves the target running, so another
    /// is submitted while the target has not completed: the answer of
    /// every one, in order, and the target's completion.
    pub(crate) fn settle_cancel(
        &mut self,
        process: B::Process,
        target: Token,
        cancel: Token,
    ) -> (Vec<Result<Done, Error>>, Complete) {
        let mut answers = Vec::new();
        let mut cancel = cancel;
        for _ in 0..ATTEMPTS {
            let answer = self.wait(process, cancel).result;
            answers.push(answer);
            let unsubmitted = matches!(answer, Err(Error::InvalidArgument | Error::Other(_)));
            if !unsubmitted || self.arrived(target) {
                let complete = self.wait(process, target);
                return (answers, complete);
            }
            cancel = self.start(process, Op::Cancel { target });
        }
        unexpected("a Cancel the backend submits, within a few attempts", &answers)
    }

    /// `process` enters the kernel, deciding what waited on it.
    pub(crate) fn enter(&mut self, process: B::Process) {
        self.backend.enter(process);
    }

    /// Lets `span` pass with no process entering the kernel.
    pub(crate) fn sleep(&mut self, span: Duration) {
        self.backend.sleep(span);
    }

    /// Sends on `fd` until a `Send` fails, or for as long as the peer's
    /// reset may take to arrive: every answer, in order.
    pub(crate) fn send_until_refused(&mut self, process: B::Process, fd: Fd) -> Vec<Result<Done, Error>> {
        self.retry(process, Retried::Send(fd))
    }

    /// Shuts `fd` down until a `Shutdown` fails, or for as long as the
    /// peer's reset or acknowledgement may take to arrive: every answer.
    pub(crate) fn shutdown_until_refused(&mut self, process: B::Process, fd: Fd) -> Vec<Result<Done, Error>> {
        self.retry(process, Retried::Shutdown(fd))
    }

    fn retry(&mut self, process: B::Process, retried: Retried) -> Vec<Result<Done, Error>> {
        let deadline = self.later(BRIEFLY);
        let mut answers = Vec::new();
        loop {
            let op = match retried {
                Retried::Send(fd) => Op::send(fd, Box::from(*b"lost"), 0).expect("bytes to send"),
                Retried::Shutdown(fd) => Op::Shutdown { fd },
            };
            let answer = self.call(process, op).result;
            answers.push(answer);
            if answer.is_err() || self.backend.now() >= deadline {
                return answers;
            }
            self.backend.pass(process, RETRY);
        }
    }

    /// Submits `op` and waits for its completion.
    pub(crate) fn call(&mut self, process: B::Process, op: Op) -> Complete {
        let token = self.start(process, op);
        self.wait(process, token)
    }

    pub(crate) fn later(&self, span: Duration) -> Time {
        self.backend.now().checked_add(span).expect("a time in reach")
    }

    fn reap(&mut self, process: B::Process) {
        let mut completions = mem::replace(&mut self.completions, Queue::with_capacity(0));
        self.backend.reap(process, &mut completions);
        while let Some(complete) = completions.pop() {
            self.check(process, complete);
        }
        self.completions = completions;
    }

    /// Checks a completion against the contract's records rules, and keeps
    /// it for the scenario.
    fn check(&mut self, process: B::Process, complete: Complete) {
        assert!(complete.is_valid(), "every completion is one the contract allows for its operation");
        let flight = self.flights.remove(&complete.op);
        let flight = flight.expect("every completion answers a submission still in flight, once");
        assert!(flight.process == process, "a completion comes back to the process that submitted it");
        assert!(flight.summary == Summary::of(&complete.kind), "the completion hands back the operation submitted");
        handed_back(&flight, &complete);
        self.track(process, &complete);
        self.arrived.insert(complete.op, complete);
    }

    /// Keeps the descriptors of each process: a `Close` releases one,
    /// whatever its result.
    fn track(&mut self, process: B::Process, complete: &Complete) {
        let opened = match (&complete.kind, &complete.result) {
            (Op::Close { fd }, _) => {
                assert!(self.open.remove(&(process, *fd)), "a Close is of a descriptor open in its process");
                None
            }
            (_, Ok(Done::Fd(fd) | Done::Accepted { fd, .. })) => Some(*fd),
            (_, Ok(Done::Nothing | Done::Count(_) | Done::Bound(_) | Done::Stat(_)) | Err(_)) => None,
        };
        if let Some(fd) = opened {
            assert!(self.open.insert((process, fd)), "a new descriptor is not one already open in its process");
        }
    }

    /// Nothing left in flight or unlooked at, no descriptor open, and each
    /// process settled as its backend sees it.
    pub(crate) fn finish(self) {
        assert!(self.flights.is_empty(), "every submission of the scenario completed");
        assert!(self.arrived.is_empty(), "every completion of the scenario was looked at");
        assert!(self.open.is_empty(), "the scenario closed every descriptor");
        for process in self.processes {
            self.backend.assert_settled(process);
        }
    }
}

// Sockets, a call each.
impl<B: Backend> Run<'_, B> {
    pub(crate) fn socket(&mut self, process: B::Process, family: Family) -> Fd {
        match self.call(process, Op::Socket { family }).result {
            Ok(Done::Fd(fd)) => fd,
            other => unexpected("a Socket on loopback succeeds", &other),
        }
    }

    pub(crate) fn bind(&mut self, process: B::Process, fd: Fd, addr: Addr) -> Result<Addr, Error> {
        match self.call(process, Op::Bind { fd, addr }).result {
            Ok(Done::Bound(bound)) => Ok(bound),
            Err(error) => Err(error),
            other => unexpected("a Bind answers with the address bound", &other),
        }
    }

    pub(crate) fn listen(&mut self, process: B::Process, fd: Fd, backlog: u32) -> Result<Done, Error> {
        self.call(process, Op::Listen { fd, backlog }).result
    }

    /// A socket listening on `addr`, which may ask for port 0, and the
    /// address it was bound to.
    pub(crate) fn listener(&mut self, process: B::Process, addr: Addr, backlog: u32) -> (Fd, Addr) {
        let fd = self.socket(process, Family::of(&addr));
        let bound = self.bind(process, fd, addr).expect("a loopback address with a free port binds");
        assert_eq!(self.listen(process, fd, backlog), Ok(Done::Nothing), "a bound socket listens");
        (fd, bound)
    }

    pub(crate) fn connect(&mut self, process: B::Process, fd: Fd, addr: Addr) -> Result<Done, Error> {
        self.call(process, Op::Connect { fd, addr }).result
    }

    pub(crate) fn accept(&mut self, process: B::Process, listener: Fd) -> (Fd, Addr) {
        match self.call(process, Op::Accept { fd: listener }).result {
            Ok(Done::Accepted { fd, peer }) => (fd, peer),
            other => unexpected("an Accept of a waiting connection", &other),
        }
    }

    /// A connection to `listener` at `addr`, from a new socket of `client`:
    /// the client's socket, the accepted one, and the peer the accept named.
    pub(crate) fn connection(
        &mut self,
        client: B::Process,
        server: B::Process,
        listener: Fd,
        addr: Addr,
    ) -> (Fd, Fd, Addr) {
        let fd = self.socket(client, Family::of(&addr));
        let accept = self.start(server, Op::Accept { fd: listener });
        assert_eq!(self.connect(client, fd, addr), Ok(Done::Nothing), "a connect to a listener");
        match self.wait(server, accept).result {
            Ok(Done::Accepted { fd: accepted, peer }) => (fd, accepted, peer),
            other => unexpected("an Accept of a connection made", &other),
        }
    }

    /// One `Send` of all of `bytes`: what it answered.
    pub(crate) fn send(&mut self, process: B::Process, fd: Fd, bytes: &[u8]) -> Result<Done, Error> {
        let op = Op::send(fd, Box::from(bytes), 0).expect("bytes to send");
        self.call(process, op).result
    }

    /// Sends all of `bytes`, a short send continued from where it stopped,
    /// to a peer whose buffer has room for them.
    pub(crate) fn send_all(&mut self, process: B::Process, fd: Fd, bytes: &[u8]) {
        let mut from = 0_u32;
        while usize_of(from) < bytes.len() {
            let op = Op::send(fd, Box::from(bytes), from).expect("bytes left to send");
            match self.call(process, op).result {
                Ok(Done::Count(n)) => from = from.checked_add(n).expect("no more than were left"),
                other => unexpected("a Send on a healthy connection", &other),
            }
        }
    }

    /// One `Recv` of up to `len` bytes: the bytes, empty at the end of the
    /// stream, or the error.
    pub(crate) fn recv(&mut self, process: B::Process, fd: Fd, len: usize) -> Result<Vec<u8>, Error> {
        let op = Op::recv(fd, vec![0; len].into_boxed_slice()).expect("room to receive");
        received(self.call(process, op))
    }

    /// Receives until `len` bytes arrived, through short receives.
    pub(crate) fn recv_exact(&mut self, process: B::Process, fd: Fd, len: usize) -> Vec<u8> {
        let mut got = Vec::with_capacity(len);
        while got.len() < len {
            let left = len.checked_sub(got.len()).expect("fewer than asked for");
            match self.recv(process, fd, left) {
                Ok(bytes) if !bytes.is_empty() => got.extend_from_slice(&bytes),
                other => unexpected("bytes sent and not yet received arrive", &other),
            }
        }
        got
    }

    /// Carries `bytes` from `tx` on `from` to `rx` on `to`, a `Send` and a
    /// `Recv` in flight together so that neither waits on a full buffer:
    /// what arrived. Short sends are continued; a stream that ends before
    /// every byte was sent fails the scenario.
    pub(crate) fn transfer(&mut self, from: B::Process, tx: Fd, to: B::Process, rx: Fd, bytes: &[u8]) -> Vec<u8> {
        let mut got = Vec::with_capacity(bytes.len());
        let mut offset = 0_u32;
        let mut sending = None;
        let mut receiving = None;
        loop {
            if sending.is_none() && usize_of(offset) < bytes.len() {
                let op = Op::send(tx, Box::from(bytes), offset).expect("bytes left to send");
                sending = Some(self.start(from, op));
            }
            if receiving.is_none() && got.len() < bytes.len() {
                let op = Op::recv(rx, vec![0; CHUNK].into_boxed_slice()).expect("room to receive");
                receiving = Some(self.start(to, op));
            }
            let complete = match (sending, receiving) {
                (Some(send), Some(recv)) => self.either((from, send), (to, recv)),
                // Every byte arrived, and the send's completion is late.
                (Some(send), None) => self.wait(from, send),
                // Every byte was offset.
                (None, Some(recv)) => self.wait(to, recv),
                (None, None) => return got,
            };
            if Some(complete.op) == sending {
                sending = None;
                match complete.result {
                    Ok(Done::Count(n)) => offset = offset.checked_add(n).expect("no more than were left"),
                    other => unexpected("a Send on a healthy connection", &other),
                }
                continue;
            }
            receiving = None;
            let more = received(complete).expect("bytes offset on a healthy connection are received");
            if more.is_empty() {
                assert!(sending.is_none(), "a stream ends only after every byte offset");
                return got;
            }
            got.extend_from_slice(&more);
        }
    }

    pub(crate) fn shutdown(&mut self, process: B::Process, fd: Fd) -> Result<Done, Error> {
        self.call(process, Op::Shutdown { fd }).result
    }

    pub(crate) fn close(&mut self, process: B::Process, fd: Fd) {
        let closed = self.call(process, Op::Close { fd }).result;
        assert_eq!(closed, Ok(Done::Nothing), "a Close of a descriptor nothing else uses succeeds");
    }
}

/// Entries of a directory, each name with its kind.
pub type Entries = BTreeSet<(Vec<u8>, Kind)>;

/// How many bytes a `List` names hold: the least the contract allows, so
/// that it holds no more than a few names.
pub(crate) const NAMES: usize = 255;

/// The bytes each `Read` of a file asks for: few, so a file takes several.
const READ_CHUNK: usize = 7;

// Files, a call each.
impl<B: Backend> Run<'_, B> {
    /// A root laid out as `tree` says, opened for `process`, which the
    /// scenario closes.
    pub(crate) fn root(&mut self, process: B::Process, tree: &[Item]) -> Fd {
        let fd = self.backend.root(process, tree);
        assert!(self.open.insert((process, fd)), "a root is a descriptor not already open in its process");
        fd
    }

    pub(crate) fn open(&mut self, process: B::Process, root: Fd, path: &[u8], how: OpenHow) -> Result<Fd, Error> {
        match self.call(process, Op::Open { root, path: Box::from(path), how }).result {
            Ok(Done::Fd(fd)) => Ok(fd),
            Err(error) => Err(error),
            other => unexpected("an Open answers with a descriptor", &other),
        }
    }

    /// Opens `path` to read, reads all of it and closes it.
    pub(crate) fn contents(&mut self, process: B::Process, root: Fd, path: &[u8]) -> Result<Vec<u8>, Error> {
        let file = self.open(process, root, path, OpenHow::Read)?;
        let (read, _) = self.read_all(process, file);
        self.close(process, file);
        read
    }

    /// Reads the file open on `fd` from its start to its end, a few bytes a
    /// `Read`, a short one continued: its bytes, or the first error; and
    /// how the `Read`s counted, against what was there.
    pub(crate) fn read_all(&mut self, process: B::Process, fd: Fd) -> (Result<Vec<u8>, Error>, Shortness) {
        let mut got = Vec::new();
        let mut counts = Vec::new();
        loop {
            let at = u64::try_from(got.len()).expect("a usize fits a u64");
            let op = Op::read(fd, vec![0; READ_CHUNK].into_boxed_slice(), at).expect("room to read");
            let complete = self.call(process, op);
            match (complete.kind, complete.result) {
                (Op::Read { buf, .. }, Ok(Done::Count(n))) => {
                    let n = usize_of(n);
                    if n == 0 {
                        break;
                    }
                    counts.push((got.len(), n));
                    got.extend_from_slice(buf.get(..n).expect("a count within its buffer"));
                }
                (_, Err(error)) => return (Err(error), Shortness::default()),
                (_, other) => unexpected("a Read answers with a count", &other),
            }
        }
        let mut shortness = Shortness::default();
        for (at, n) in counts {
            let there = got.len().saturating_sub(at).min(READ_CHUNK);
            shortness.saw(n < there);
        }
        (Ok(got), shortness)
    }

    /// Writes all of `bytes` at `at`, a short `Write` continued from where
    /// it stopped: how the `Write`s counted, or the first error.
    pub(crate) fn write_all(&mut self, process: B::Process, fd: Fd, at: u64, bytes: &[u8]) -> Result<Shortness, Error> {
        let mut from = 0_u32;
        let mut shortness = Shortness::default();
        while usize_of(from) < bytes.len() {
            let offset = at.checked_add(u64::from(from)).expect("an offset in reach");
            let op = Op::write(fd, Box::from(bytes), from, offset).expect("bytes left to write");
            match self.call(process, op).result {
                Ok(Done::Count(n)) => {
                    let left = bytes.len().saturating_sub(usize_of(from));
                    shortness.saw(usize_of(n) < left);
                    from = from.checked_add(n).expect("no more than were left");
                }
                Err(error) => return Err(error),
                other => unexpected("a Write answers with a count", &other),
            }
        }
        Ok(shortness)
    }

    pub(crate) fn sync(&mut self, process: B::Process, fd: Fd) -> Result<Done, Error> {
        self.call(process, Op::Sync { fd }).result
    }

    pub(crate) fn stat(&mut self, process: B::Process, fd: Fd) -> Result<Stat, Error> {
        match self.call(process, Op::Stat { fd }).result {
            Ok(Done::Stat(stat)) => Ok(stat),
            Err(error) => Err(error),
            other => unexpected("a Stat answers with what it found", &other),
        }
    }

    pub(crate) fn rename(
        &mut self,
        process: B::Process,
        (from_dir, from): (Fd, &[u8]),
        (to_dir, to): (Fd, &[u8]),
    ) -> Result<Done, Error> {
        let op = Op::Rename { from_dir, from: Box::from(from), to_dir, to: Box::from(to) };
        self.call(process, op).result
    }

    pub(crate) fn remove(&mut self, process: B::Process, dir: Fd, name: &[u8], directory: bool) -> Result<Done, Error> {
        self.call(process, Op::Remove { dir, name: Box::from(name), directory }).result
    }

    pub(crate) fn make_directory(&mut self, process: B::Process, dir: Fd, name: &[u8]) -> Result<Done, Error> {
        self.call(process, Op::MakeDirectory { dir, name: Box::from(name) }).result
    }

    /// One `List` of `fd` with room for `entries`, and `names` bytes: each
    /// entry's name and kind.
    pub(crate) fn list(
        &mut self,
        process: B::Process,
        fd: Fd,
        entries: usize,
        names: usize,
    ) -> Result<Vec<(Vec<u8>, Kind)>, Error> {
        let op = Op::List { fd, entries: vec![Entry::BLANK; entries].into(), names: vec![0; names].into() };
        let complete = self.call(process, op);
        match (complete.kind, complete.result) {
            (Op::List { entries, names, .. }, Ok(Done::Count(n))) => {
                let mut listed = Vec::new();
                for entry in entries.get(..usize_of(n)).expect("a count within the entries") {
                    listed.push((entry.name(&names).expect("a name within names").to_vec(), entry.kind));
                }
                Ok(listed)
            }
            (_, Err(error)) => Err(error),
            (_, other) => unexpected("a List answers with a count", &other),
        }
    }

    /// `List`s of `fd` with room for `entries` each until the end: every
    /// entry, each seen once, and how many each `List` counted.
    pub(crate) fn list_all(
        &mut self,
        process: B::Process,
        fd: Fd,
        entries: usize,
    ) -> Result<(Entries, Vec<usize>), Error> {
        let mut all = BTreeSet::new();
        let mut counts = Vec::new();
        loop {
            let listed = self.list(process, fd, entries, NAMES)?;
            if listed.is_empty() {
                return Ok((all, counts));
            }
            counts.push(listed.len());
            for entry in listed {
                assert!(all.insert(entry), "the contract: a List hands back each entry once");
            }
        }
    }
}

/// Whether the `Read`s or the `Write`s of a scenario counted fewer bytes
/// than they could, and whether they counted all: the outcomes the contract
/// allows, which the simulator draws among.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Shortness {
    pub short: bool,
    pub full: bool,
}

impl Shortness {
    fn saw(&mut self, short: bool) {
        if short {
            self.short = true;
        } else {
            self.full = true;
        }
    }

    /// Both, as seen over several.
    #[must_use]
    pub fn and(self, other: Shortness) -> Shortness {
        Shortness { short: self.short || other.short, full: self.full || other.full }
    }
}

/// An operation without its buffers, their lengths standing in for them:
/// what a completion must hand back, whatever its buffers now hold. The
/// simulator's trace summarises an operation the same way, but the suite
/// depends on no backend.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Summary {
    Socket { family: Family },
    Bind { fd: Fd, addr: Addr },
    Listen { fd: Fd, backlog: u32 },
    Accept { fd: Fd },
    Connect { fd: Fd, addr: Addr },
    Recv { fd: Fd, len: usize },
    Send { fd: Fd, len: usize, from: u32 },
    Shutdown { fd: Fd },
    Close { fd: Fd },
    Open { root: Fd, path: usize, how: OpenHow },
    Read { fd: Fd, len: usize, at: u64 },
    Write { fd: Fd, len: usize, from: u32, at: u64 },
    Sync { fd: Fd },
    Stat { fd: Fd },
    Rename { from_dir: Fd, from: usize, to_dir: Fd, to: usize },
    Remove { dir: Fd, name: usize, directory: bool },
    MakeDirectory { dir: Fd, name: usize },
    List { fd: Fd, entries: usize, names: usize },
    Cancel { target: Token },
}

impl Summary {
    fn of(op: &Op) -> Summary {
        match op {
            Op::Socket { family } => Summary::Socket { family: *family },
            Op::Bind { fd, addr } => Summary::Bind { fd: *fd, addr: *addr },
            Op::Listen { fd, backlog } => Summary::Listen { fd: *fd, backlog: *backlog },
            Op::Accept { fd } => Summary::Accept { fd: *fd },
            Op::Connect { fd, addr } => Summary::Connect { fd: *fd, addr: *addr },
            Op::Recv { fd, buf } => Summary::Recv { fd: *fd, len: buf.len() },
            Op::Send { fd, bytes, from } => Summary::Send { fd: *fd, len: bytes.len(), from: *from },
            Op::Shutdown { fd } => Summary::Shutdown { fd: *fd },
            Op::Close { fd } => Summary::Close { fd: *fd },
            Op::Open { root, path, how } => Summary::Open { root: *root, path: path.len(), how: *how },
            Op::Read { fd, buf, at } => Summary::Read { fd: *fd, len: buf.len(), at: *at },
            Op::Write { fd, bytes, from, at } => Summary::Write { fd: *fd, len: bytes.len(), from: *from, at: *at },
            Op::Sync { fd } => Summary::Sync { fd: *fd },
            Op::Stat { fd } => Summary::Stat { fd: *fd },
            Op::Rename { from_dir, from, to_dir, to } => {
                Summary::Rename { from_dir: *from_dir, from: from.len(), to_dir: *to_dir, to: to.len() }
            }
            Op::Remove { dir, name, directory } => {
                Summary::Remove { dir: *dir, name: name.len(), directory: *directory }
            }
            Op::MakeDirectory { dir, name } => Summary::MakeDirectory { dir: *dir, name: name.len() },
            Op::List { fd, entries, names } => Summary::List { fd: *fd, entries: entries.len(), names: names.len() },
            Op::Cancel { target } => Summary::Cancel { target: *target },
        }
    }
}

/// An operation retried until it fails.
#[derive(Clone, Copy, Debug)]
enum Retried {
    Send(Fd),
    Shutdown(Fd),
}

/// The boxes of bytes a record carries: a buffer, a path, names.
fn boxes_of(op: &Op) -> Vec<&[u8]> {
    match op {
        Op::Recv { buf, .. } | Op::Read { buf, .. } => vec![buf],
        Op::Send { bytes, .. } | Op::Write { bytes, .. } => vec![bytes],
        Op::Open { path, .. } => vec![path],
        Op::Rename { from, to, .. } => vec![from, to],
        Op::Remove { name, .. } | Op::MakeDirectory { name, .. } => vec![name],
        Op::List { names, .. } => vec![names],
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Cancel { .. } => Vec::new(),
    }
}

/// A `List`'s entries.
fn entries_of(op: &Op) -> Option<&[Entry]> {
    match op {
        Op::List { entries, .. } => Some(entries),
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::Cancel { .. } => None,
    }
}

/// Checks that a completion hands back each of its record's boxes in the
/// `Box` it went down in, written only where the contract says: a `Recv`'s
/// or a `Read`'s buffer up to its count, a `List`'s entries up to its count
/// and its names where those entries lie; everything else untouched.
fn handed_back<P>(flight: &Flight<P>, complete: &Complete) {
    let count = match complete.result {
        Ok(Done::Count(n)) => Some(usize_of(n)),
        Ok(Done::Nothing | Done::Fd(_) | Done::Accepted { .. } | Done::Bound(_) | Done::Stat(_)) | Err(_) => None,
    };
    let back = boxes_of(&complete.kind);
    assert_eq!(back.len(), flight.boxes.len(), "a record comes back with the boxes it went down with");
    for ((address, held), back) in flight.boxes.iter().zip(back) {
        assert!(back.as_ptr().addr() == *address, "a buffer comes back in the Box it went down in");
        let untouched = match (&complete.kind, count) {
            (Op::Recv { .. } | Op::Read { .. }, Some(n)) => back.get(n..) == held.get(n..),
            (Op::List { entries, .. }, Some(n)) => names_untouched(held, back, entries.get(..n).unwrap_or_default()),
            _ => back == &**held,
        };
        assert!(untouched, "a record's bytes come back untouched, but for what its count says was written");
    }
    if let (Some((address, held)), Some(back)) = (&flight.entries, entries_of(&complete.kind)) {
        assert!(back.as_ptr().addr() == *address, "a List's entries come back in the Box they went down in");
        let n = count.unwrap_or(0);
        assert!(back.get(n..) == held.get(n..), "a List's entries past its count come back untouched");
    }
}

/// Whether a `List`'s names are as they were but where `listed` lies.
fn names_untouched(held: &[u8], back: &[u8], listed: &[Entry]) -> bool {
    let mut written = vec![false; back.len()];
    for entry in listed {
        let start = usize_of(entry.start);
        for at in start..start.saturating_add(usize_of(entry.len)) {
            if let Some(slot) = written.get_mut(at) {
                *slot = true;
            }
        }
    }
    for (at, (before, after)) in held.iter().zip(back).enumerate() {
        if before != after && !written.get(at).copied().unwrap_or(false) {
            return false;
        }
    }
    true
}

/// The bytes a `Recv` completion received, or its error.
pub(crate) fn received(complete: Complete) -> Result<Vec<u8>, Error> {
    match (complete.kind, complete.result) {
        (Op::Recv { buf, .. }, Ok(Done::Count(n))) => {
            Ok(buf.get(..usize_of(n)).expect("a count within its buffer").to_vec())
        }
        (_, Err(error)) => Err(error),
        (_, other) => unexpected("a Recv answers with a count", &other),
    }
}

pub(crate) fn usize_of(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits a usize")
}

/// Fails the scenario on an answer the step expected not to see.
#[expect(clippy::panic, reason = "a scenario fails loudly, on an ordinary assertion")]
pub(crate) fn unexpected<T: Debug>(expected: &str, seen: &T) -> ! {
    panic!("expected {expected}, saw {seen:?}");
}
