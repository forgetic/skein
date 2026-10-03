//! The driver the scenarios run on: records submitted one at a time to a
//! [`Backend`], and every completion checked as it is reaped, as the
//! simulator checks its own (overview.md, section 9): a valid completion of
//! the operation's shape, one per submission, with the record handed back.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::mem;

use skein_io::kernel::{Addr, Complete, Done, Error, Family, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token};

use super::Backend;
use crate::trace::Summary;

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
    /// A `Recv`'s or a `Send`'s buffer: its address, which the completion
    /// hands back in the same `Box`, and what it held.
    buffer: Option<(usize, Box<[u8]>)>,
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
        let buffer = buffer(&op).map(|held| (held.as_ptr().addr(), Box::from(held)));
        self.flights.insert(token, Flight { process, summary: Summary::of(&op), buffer });
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
        if let (Some((address, held)), Some(back)) = (&flight.buffer, buffer(&complete.kind)) {
            assert!(back.as_ptr().addr() == *address, "a buffer comes back in the Box it went down in");
            let untouched = match (&complete.kind, complete.result) {
                // A Recv wrote only the bytes it counts.
                (Op::Recv { .. }, Ok(Done::Count(n))) => back.get(usize_of(n)..) == held.get(usize_of(n)..),
                _ => back == &**held,
            };
            assert!(untouched, "a Send's bytes, and a Recv's buffer past its count, come back untouched");
        }
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
            (_, Ok(Done::Nothing | Done::Count(_) | Done::Bound(_)) | Err(_)) => None,
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

/// An operation retried until it fails.
#[derive(Clone, Copy, Debug)]
enum Retried {
    Send(Fd),
    Shutdown(Fd),
}

/// The buffer a `Recv` or a `Send` carries.
fn buffer(op: &Op) -> Option<&[u8]> {
    match op {
        Op::Recv { buf, .. } => Some(buf),
        Op::Send { bytes, .. } => Some(bytes),
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Cancel { .. } => None,
    }
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
