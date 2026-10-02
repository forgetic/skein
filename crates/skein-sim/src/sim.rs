//! The world: processes, their descriptors and operations in flight, the
//! sockets and the network between them, time, and the trace.

use alloc::collections::{BTreeMap, VecDeque};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::net::SocketAddr;

use skein_io::kernel::{Addr, Complete, Done, Error, Family, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Rng, Time, Token, Wall};

use crate::config::Config;
use crate::net::{
    EPHEMERAL_FIRST, EPHEMERAL_LAST, Fate, Listener, Socket, SocketId, State, Stream, bindable, loopback, overlaps,
};
use crate::trace::{self, Entry, Event, Summary};

/// A simulated process: a plain handle, from [`Sim::spawn_process`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Pid(u32);

impl Pid {
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl fmt::Display for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pid {}", self.0)
    }
}

/// The first descriptor a process is given, as if 0, 1 and 2 were taken.
const FIRST_FD: i32 = 3;

/// How many trace entries a failure prints.
const TAIL: usize = 24;

#[derive(Debug)]
struct Process {
    /// The next descriptor number: numbers are never reused.
    next_fd: i32,
    /// The open descriptors, and the socket each names.
    fds: BTreeMap<Fd, SocketId>,
    /// Every operation submitted and not yet reaped.
    flights: BTreeMap<Token, Flight>,
    /// Completions delivered and not yet reaped, in delivery order.
    ready: VecDeque<Complete>,
}

/// An operation in flight, until its completion is reaped.
#[derive(Debug)]
struct Flight {
    /// The world's name for this submission, so a late cancel never lands on
    /// a later operation that reuses the token.
    serial: u64,
    kind: Summary,
    /// The record, while the operation waits; the completion holds it after.
    held: Option<Op>,
    /// Its completion was made.
    done: bool,
}

/// Something the world will do at a later time.
#[derive(Debug)]
enum Due {
    /// Deliver a completion made earlier, after its latency.
    Post { pid: Pid, complete: Complete },
    /// A raced `Cancel` reaches its target.
    Land { pid: Pid, cancel: Token, target: Token, serial: u64 },
}

/// The simulated kernel (overview.md, section 9), for every process of one
/// world, deterministic from its seed. See the crate documentation.
#[derive(Debug)]
pub struct Sim {
    seed: u64,
    config: Config,
    rng: Rng,
    now: Time,
    processes: Vec<Process>,
    sockets: BTreeMap<SocketId, Socket>,
    next_socket: SocketId,
    next_serial: u64,
    next_due: u64,
    schedule: BTreeMap<(Time, u64), Due>,
    /// Sockets whose waiting operations may now complete.
    pokes: VecDeque<SocketId>,
    trace: Vec<Entry>,
}

impl Sim {
    #[must_use]
    pub fn new(seed: u64, config: Config) -> Sim {
        Sim {
            seed,
            config,
            rng: Rng::new(seed),
            now: Time::ZERO,
            processes: Vec::new(),
            sockets: BTreeMap::new(),
            next_socket: 0,
            next_serial: 0,
            next_due: 0,
            schedule: BTreeMap::new(),
            pokes: VecDeque::new(),
            trace: Vec::new(),
        }
    }

    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// The monotonic time of the world.
    #[must_use]
    pub const fn now(&self) -> Time {
        self.now
    }

    /// The wall-clock time of the world: the configured start, plus the time
    /// that has passed.
    #[must_use]
    pub const fn wall(&self) -> Wall {
        Wall::from_nanos(self.config.wall.as_nanos().saturating_add(self.now.as_nanos()))
    }

    /// A new process, with no descriptor and nothing in flight.
    pub fn spawn_process(&mut self) -> Pid {
        let pid = Pid(u32::try_from(self.processes.len()).expect("fewer than 2^32 processes"));
        self.processes.push(Process {
            next_fd: FIRST_FD,
            fds: BTreeMap::new(),
            flights: BTreeMap::new(),
            ready: VecDeque::new(),
        });
        pid
    }

    /// Takes every record in `submissions`, in order, as the shell's `Kernel`
    /// does. Each is checked against the broken invariants of the contract
    /// (`skein_io::kernel`) and fails the world on one.
    pub fn submit(&mut self, pid: Pid, submissions: &mut Queue<Submit>) {
        while let Some(submit) = submissions.pop() {
            self.submit_one(pid, submit);
            self.settle();
        }
    }

    /// Moves the completions delivered to `pid` into `completions`, oldest
    /// first, as many as it has room for. The rest wait for the next reap.
    pub fn reap(&mut self, pid: Pid, completions: &mut Queue<Complete>) {
        while completions.room() > 0 {
            let Some(complete) = self.process_mut(pid).ready.pop_front() else {
                return;
            };
            let Some(flight) = self.process_mut(pid).flights.remove(&complete.op) else {
                self.fail(pid, &format!("a completion of {:?}, which is not in flight", complete.op));
            };
            assert!(flight.done, "a delivered completion was made");
            let event = Event::Complete { op: complete.op, kind: flight.kind, result: complete.result };
            self.record(pid, event);
            completions.push(complete);
        }
    }

    /// Completions delivered to `pid` and not yet reaped.
    #[must_use]
    pub fn ready(&self, pid: Pid) -> u32 {
        u32::try_from(self.process(pid).ready.len()).expect("fewer than 2^32 completions")
    }

    /// Operations of `pid` submitted and not yet reaped.
    #[must_use]
    pub fn in_flight(&self, pid: Pid) -> u32 {
        u32::try_from(self.process(pid).flights.len()).expect("fewer than 2^32 operations")
    }

    /// Descriptors of `pid` that are open.
    #[must_use]
    pub fn open_fds(&self, pid: Pid) -> u32 {
        u32::try_from(self.process(pid).fds.len()).expect("fewer than 2^32 descriptors")
    }

    /// When the world next does something by itself: a completion delivered
    /// after its latency, or a raced cancel landing. `None` when the world is
    /// idle: nothing more happens until a process submits.
    #[must_use]
    pub fn next_due(&self) -> Option<Time> {
        self.schedule.first_key_value().map(|((at, _), _)| *at)
    }

    /// Moves time to [`Sim::next_due`] and does what was due then. False, and
    /// time unmoved, when the world is idle.
    pub fn advance(&mut self) -> bool {
        match self.next_due() {
            Some(at) => {
                self.advance_to(at);
                true
            }
            None => false,
        }
    }

    /// Moves time to `at`, doing everything due until then, in order: for a
    /// process's own deadline. Time never moves back.
    pub fn advance_to(&mut self, at: Time) {
        assert!(at >= self.now, "time moves forward only");
        while let Some(entry) = self.schedule.first_entry() {
            let (when, _) = *entry.key();
            if when > at {
                break;
            }
            let due = entry.remove();
            self.now = when;
            match due {
                Due::Post { pid, complete } => self.process_mut(pid).ready.push_back(complete),
                Due::Land { pid, cancel, target, serial } => {
                    let op = self.unpark(pid, cancel);
                    self.land(pid, cancel, op, target, serial);
                    self.settle();
                }
            }
        }
        self.now = at;
    }

    /// Fails the world unless `pid` has nothing in flight: every operation
    /// completed, and every completion reaped (testing-pyramid.md, 5).
    pub fn assert_quiescent(&self, pid: Pid) {
        let process = self.process(pid);
        if let Some((token, flight)) = process.flights.first_key_value() {
            let what =
                format!("not quiescent: {} in flight, the first {token:?}, {:?}", process.flights.len(), flight.kind);
            self.fail(pid, &what);
        }
    }

    /// Fails the world unless every descriptor of `pid` is closed.
    pub fn assert_no_open_fds(&self, pid: Pid) {
        let process = self.process(pid);
        if let Some((fd, _)) = process.fds.first_key_value() {
            self.fail(pid, &format!("{} descriptors open, the first {fd:?}", process.fds.len()));
        }
    }

    /// Every submission and every completion reaped so far.
    #[must_use]
    pub fn trace(&self) -> &[Entry] {
        &self.trace
    }

    /// The trace as text, under the seed that replays it.
    #[must_use]
    pub fn render_trace(&self) -> String {
        trace::render(self.seed, &self.trace)
    }
}

// Submission and the broken invariants.
impl Sim {
    fn submit_one(&mut self, pid: Pid, submit: Submit) {
        let Submit { op: token, kind } = submit;
        let summary = Summary::of(&kind);
        self.record(pid, Event::Submit { op: token, kind: summary });
        if !kind.is_valid() {
            self.fail(pid, &format!("an invalid record: {summary:?}"));
        }
        if self.process(pid).flights.contains_key(&token) {
            self.fail(pid, &format!("{token:?} is already in flight"));
        }
        let socket = self.check(pid, summary);
        let serial = self.next_serial;
        self.next_serial = serial.checked_add(1).expect("fewer than 2^64 submissions");
        let flight = Flight { serial, kind: summary, held: None, done: false };
        self.process_mut(pid).flights.insert(token, flight);
        match kind {
            Op::Socket { family } => self.socket(pid, token, kind, family),
            Op::Bind { addr, .. } => self.bind(pid, token, kind, on(socket), addr),
            Op::Listen { backlog, .. } => self.listen(pid, token, kind, on(socket), backlog),
            Op::Accept { .. } => self.accept(pid, token, kind, on(socket)),
            Op::Connect { addr, .. } => self.connect(pid, token, kind, on(socket), addr),
            Op::Recv { .. } => self.recv(pid, token, kind, on(socket)),
            Op::Send { .. } => self.send(pid, token, kind, on(socket)),
            Op::Shutdown { .. } => self.shutdown(pid, token, kind, on(socket)),
            Op::Close { fd } => self.close(pid, token, kind, fd, on(socket)),
            Op::Cancel { target } => self.cancel(pid, token, kind, target),
        }
    }

    /// Fails the world on a broken invariant of the contract about `kind`'s
    /// descriptor or target, and answers the socket its descriptor names.
    fn check(&self, pid: Pid, kind: Summary) -> Option<SocketId> {
        let process = self.process(pid);
        if let Summary::Cancel { target } = kind {
            if let Some(flight) = process.flights.get(&target)
                && let Summary::Cancel { .. } = flight.kind
            {
                self.fail(pid, &format!("a Cancel of {target:?}, which is a Cancel"));
            }
            return None;
        }
        let fd = kind.fd()?;
        let Some(&id) = process.fds.get(&fd) else {
            self.fail(pid, &format!("{kind:?} on {fd:?}, which is not open in this process"));
        };
        let socket = self.sockets.get(&id).expect("an open descriptor names a socket");
        let mut on_fd = Vec::new();
        for flight in process.flights.values() {
            if flight.kind.fd() == Some(fd) {
                on_fd.push(flight.kind);
            }
        }
        let count = |wanted: fn(&Summary) -> bool| on_fd.iter().filter(|kind| wanted(kind)).count();
        if count(|kind| matches!(kind, Summary::Connect { .. })) > 0 {
            self.fail(pid, &format!("{kind:?} beside a Connect in flight on {fd:?}"));
        }
        if socket.closing_only && !matches!(kind, Summary::Close { .. }) {
            self.fail(pid, &format!("{kind:?} after a Connect on {fd:?} failed or was cancelled"));
        }
        let broken = match kind {
            Summary::Bind { addr, .. } | Summary::Connect { addr, .. } if Family::of(&addr) != socket.family => {
                Some("an address of another family than its socket's")
            }
            Summary::Connect { .. } if !on_fd.is_empty() => Some("a Connect beside another operation in flight"),
            Summary::Recv { .. } if count(|kind| matches!(kind, Summary::Recv { .. })) > 0 => {
                Some("a second Recv in flight")
            }
            Summary::Send { .. } if count(|kind| matches!(kind, Summary::Send { .. })) > 0 => {
                Some("a second Send in flight")
            }
            Summary::Accept { .. } if count(|kind| matches!(kind, Summary::Accept { .. })) > 0 => {
                Some("a second Accept in flight")
            }
            Summary::Shutdown { .. } if count(|kind| matches!(kind, Summary::Send { .. })) > 0 => {
                Some("a Shutdown while a Send is in flight")
            }
            Summary::Close { .. } if !on_fd.is_empty() => Some("a Close while another operation is in flight"),
            Summary::Socket { .. }
            | Summary::Bind { .. }
            | Summary::Listen { .. }
            | Summary::Accept { .. }
            | Summary::Connect { .. }
            | Summary::Recv { .. }
            | Summary::Send { .. }
            | Summary::Shutdown { .. }
            | Summary::Close { .. }
            | Summary::Cancel { .. } => None,
        };
        if let Some(broken) = broken {
            self.fail(pid, &format!("{broken}: {kind:?}, with {on_fd:?} in flight on {fd:?}"));
        }
        Some(id)
    }
}

// The operations.
impl Sim {
    fn socket(&mut self, pid: Pid, token: Token, op: Op, family: Family) {
        let id = self.new_socket(family, None, State::Fresh);
        let fd = self.open_fd(pid, id);
        self.complete(pid, token, op, Ok(Done::Fd(fd)));
    }

    fn bind(&mut self, pid: Pid, token: Token, op: Op, id: SocketId, addr: Addr) {
        let result = self.bound(id, addr);
        self.complete(pid, token, op, result);
    }

    fn bound(&mut self, id: SocketId, addr: Addr) -> Result<Done, Error> {
        let socket = self.sockets.get(&id).expect("checked at submit");
        let (State::Fresh, None) = (&socket.state, socket.local) else {
            return Err(Error::InvalidArgument);
        };
        if !bindable(addr.ip()) {
            return Err(Error::AddressNotAvailable);
        }
        let family = socket.family;
        let port = match addr.port() {
            0 => self.ephemeral(family).ok_or(Error::AddressInUse)?,
            port if self.listening_at(family, addr.ip(), port, id) => return Err(Error::AddressInUse),
            port => port,
        };
        let bound = SocketAddr::new(addr.ip(), port);
        self.sockets.get_mut(&id).expect("checked at submit").local = Some(bound);
        Ok(Done::Bound(bound))
    }

    fn listen(&mut self, pid: Pid, token: Token, op: Op, id: SocketId, backlog: u32) {
        let backlog = backlog.clamp(1, self.config.backlog.max(1));
        let socket = self.sockets.get(&id).expect("checked at submit");
        let result = match (&socket.state, socket.local) {
            (State::Fresh, Some(local)) if self.listening_at(socket.family, local.ip(), local.port(), id) => {
                Err(Error::AddressInUse)
            }
            (State::Fresh, Some(_)) => {
                let listener = Listener { backlog, queue: VecDeque::new(), connects: VecDeque::new(), accepter: None };
                self.socket_mut(id).state = State::Listening(listener);
                Ok(Done::Nothing)
            }
            (State::Listening(_), _) => {
                if let State::Listening(listener) = &mut self.socket_mut(id).state {
                    listener.backlog = backlog;
                }
                self.pokes.push_back(id);
                Ok(Done::Nothing)
            }
            // Linux binds an unbound socket to an ephemeral port here; io
            // always binds first, so the simulator refuses it rather than
            // guess at the address.
            (State::Fresh, None) | (State::Connecting { .. } | State::Connected(_), _) => Err(Error::InvalidArgument),
        };
        self.complete(pid, token, op, result);
    }

    fn accept(&mut self, pid: Pid, token: Token, op: Op, id: SocketId) {
        let State::Listening(listener) = &mut self.socket_mut(id).state else {
            self.complete(pid, token, op, Err(Error::InvalidArgument));
            return;
        };
        listener.accepter = Some(token);
        self.park(pid, token, op);
        self.pokes.push_back(id);
    }

    fn connect(&mut self, pid: Pid, token: Token, op: Op, id: SocketId, to: Addr) {
        match self.connecting(id, to) {
            Ok(listener) => {
                if let State::Listening(waiting) = &mut self.socket_mut(listener).state {
                    waiting.connects.push_back((pid, token, id));
                }
                self.socket_mut(id).state = State::Connecting { listener, to };
                self.park(pid, token, op);
                self.pokes.push_back(listener);
            }
            Err(error) => {
                self.socket_mut(id).closing_only = true;
                self.complete(pid, token, op, Err(error));
            }
        }
    }

    /// The listener a `Connect` from `id` to `to` waits on, with the socket
    /// bound to its source address; or why it failed.
    fn connecting(&mut self, id: SocketId, to: Addr) -> Result<SocketId, Error> {
        let socket = self.sockets.get(&id).expect("checked at submit");
        let State::Fresh = socket.state else {
            return Err(Error::InvalidArgument);
        };
        if !to.ip().is_loopback() {
            return Err(Error::Unreachable);
        }
        let family = socket.family;
        let local = socket.local;
        let ip = match local {
            Some(local) if !local.ip().is_unspecified() => local.ip(),
            Some(_) | None => loopback(family),
        };
        let port = match local {
            Some(local) => local.port(),
            None => self.ephemeral(family).ok_or(Error::AddressNotAvailable)?,
        };
        self.socket_mut(id).local = Some(SocketAddr::new(ip, port));
        let Some(listener) = self.listener_for(family, to) else {
            return Err(Error::Refused);
        };
        if self.rng.chance(self.config.faults.refuse) {
            return Err(Error::Refused);
        }
        Ok(listener)
    }

    fn recv(&mut self, pid: Pid, token: Token, op: Op, id: SocketId) {
        let State::Connected(stream) = &mut self.socket_mut(id).state else {
            self.complete(pid, token, op, Err(Error::NotConnected));
            return;
        };
        stream.receiver = Some(token);
        self.park(pid, token, op);
        self.maybe_reset(id);
        self.pokes.push_back(id);
    }

    fn send(&mut self, pid: Pid, token: Token, op: Op, id: SocketId) {
        let State::Connected(stream) = &mut self.socket_mut(id).state else {
            self.complete(pid, token, op, Err(Error::BrokenPipe));
            return;
        };
        stream.sender = Some(token);
        self.park(pid, token, op);
        self.maybe_reset(id);
        self.pokes.push_back(id);
    }

    fn shutdown(&mut self, pid: Pid, token: Token, op: Op, id: SocketId) {
        let mut peer = None;
        let result = match &mut self.socket_mut(id).state {
            State::Connected(stream) => match stream.fate {
                Fate::Reset | Fate::Dead => Err(Error::NotConnected),
                Fate::Open if stream.shut && stream.ended => Err(Error::NotConnected),
                Fate::Open => {
                    stream.shut = true;
                    peer = stream.peer;
                    Ok(Done::Nothing)
                }
            },
            State::Fresh | State::Connecting { .. } | State::Listening(_) => Err(Error::NotConnected),
        };
        if let Some(peer) = peer {
            self.stream_mut(peer).ended = true;
            self.pokes.push_back(peer);
        }
        self.complete(pid, token, op, result);
    }

    fn close(&mut self, pid: Pid, token: Token, op: Op, fd: Fd, id: SocketId) {
        self.process_mut(pid).fds.remove(&fd);
        let socket = self.sockets.remove(&id).expect("checked at submit");
        match socket.state {
            State::Fresh => {}
            State::Connecting { .. } => unreachable!("a Close beside a Connect fails the world at submit"),
            State::Listening(listener) => {
                assert!(listener.accepter.is_none(), "a Close beside an Accept fails the world at submit");
                for waiting in listener.queue {
                    let server = self.sockets.remove(&waiting).expect("a queued connection is a socket");
                    if let State::Connected(stream) = server.state
                        && let Some(client) = stream.peer
                    {
                        self.reset_end(client);
                    }
                }
                for (client_pid, client_token, client) in listener.connects {
                    let socket = self.socket_mut(client);
                    socket.state = State::Fresh;
                    socket.closing_only = true;
                    let connect = self.unpark(client_pid, client_token);
                    self.complete(client_pid, client_token, connect, Err(Error::Refused));
                }
            }
            State::Connected(stream) => {
                if let Some(peer) = stream.peer {
                    if stream.inbox.is_empty() {
                        let end = self.stream_mut(peer);
                        end.peer = None;
                        end.ended = true;
                        self.pokes.push_back(peer);
                    } else {
                        self.reset_end(peer);
                    }
                }
            }
        }
        self.complete(pid, token, op, Ok(Done::Nothing));
    }

    fn cancel(&mut self, pid: Pid, token: Token, op: Op, target: Token) {
        let waiting = match self.process(pid).flights.get(&target) {
            Some(flight) if flight.held.is_some() => Some(flight.serial),
            Some(_) | None => None,
        };
        let Some(serial) = waiting else {
            self.complete(pid, token, op, Err(Error::TooLate));
            return;
        };
        if self.rng.chance(self.config.faults.cancel_race) {
            let late = self.rng.between(1, self.config.faults.latency_max.as_nanos().max(1));
            self.park(pid, token, op);
            self.schedule(late, Due::Land { pid, cancel: token, target, serial });
        } else {
            self.land(pid, token, op, target, serial);
        }
    }

    /// A `Cancel` reaches its target: it wins if the target still waits,
    /// and is too late otherwise.
    fn land(&mut self, pid: Pid, token: Token, op: Op, target: Token, serial: u64) {
        let wins = match self.process(pid).flights.get(&target) {
            Some(flight) => flight.serial == serial && flight.held.is_some(),
            None => false,
        };
        if !wins {
            self.complete(pid, token, op, Err(Error::TooLate));
            return;
        }
        let stopped = self.unpark(pid, target);
        self.withdraw(pid, target, &stopped);
        if self.rng.chance(500) {
            self.complete(pid, target, stopped, Err(Error::Cancelled));
            self.complete(pid, token, op, Ok(Done::Nothing));
        } else {
            self.complete(pid, token, op, Ok(Done::Nothing));
            self.complete(pid, target, stopped, Err(Error::Cancelled));
        }
    }

    /// Stops a waiting operation from waiting.
    fn withdraw(&mut self, pid: Pid, token: Token, op: &Op) {
        let id = match op {
            Op::Accept { fd } | Op::Connect { fd, .. } | Op::Recv { fd, .. } | Op::Send { fd, .. } => {
                *self.process(pid).fds.get(fd).expect("an operation in flight keeps its descriptor open")
            }
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. }
            | Op::Cancel { .. } => unreachable!("only accepts, connects, receives and sends wait"),
        };
        let socket = self.socket_mut(id);
        match (&mut socket.state, op) {
            (State::Listening(listener), Op::Accept { .. }) => listener.accepter = None,
            (State::Connected(stream), Op::Recv { .. }) => stream.receiver = None,
            (State::Connected(stream), Op::Send { .. }) => stream.sender = None,
            (State::Connecting { listener, .. }, Op::Connect { .. }) => {
                let listener = *listener;
                socket.state = State::Fresh;
                socket.closing_only = true;
                if let State::Listening(waiting) = &mut self.socket_mut(listener).state {
                    let mut kept = VecDeque::new();
                    for entry in waiting.connects.drain(..) {
                        if entry != (pid, token, id) {
                            kept.push_back(entry);
                        }
                    }
                    waiting.connects = kept;
                }
            }
            (state, op) => unreachable!("a waiting {op:?} on a socket in state {state:?}"),
        }
    }
}

// Progress: waiting operations complete when their socket can serve them.
impl Sim {
    fn settle(&mut self) {
        while let Some(id) = self.pokes.pop_front() {
            self.poke(id);
        }
    }

    fn poke(&mut self, id: SocketId) {
        let Some(socket) = self.sockets.get(&id) else {
            return;
        };
        match &socket.state {
            State::Listening(_) => self.serve_listener(id),
            State::Connected(stream) => {
                let (receiver, sender) = (stream.receiver, stream.sender);
                // Which of the two hears of a reset first is the world's choice.
                let send_first = receiver.is_some() && sender.is_some() && self.rng.chance(500);
                if send_first {
                    self.try_send(id);
                }
                self.try_recv(id);
                if !send_first {
                    self.try_send(id);
                }
            }
            State::Fresh | State::Connecting { .. } => {}
        }
    }

    fn serve_listener(&mut self, id: SocketId) {
        loop {
            let socket = self.sockets.get(&id).expect("poked while it exists");
            let State::Listening(listener) = &socket.state else {
                return;
            };
            if listener.accepter.is_some() && !listener.queue.is_empty() {
                self.accept_one(id);
            } else if !listener.connects.is_empty() && listener.queue.len() < usize_of(listener.backlog) {
                self.establish(id);
            } else {
                return;
            }
        }
    }

    fn accept_one(&mut self, id: SocketId) {
        let owner = self.sockets.get(&id).expect("a listener").owner;
        let (pid, _) = owner.expect("a listener has a descriptor");
        let State::Listening(listener) = &mut self.socket_mut(id).state else {
            unreachable!("accepting on a listener");
        };
        let server = listener.queue.pop_front().expect("a connection waits");
        let token = listener.accepter.take().expect("an accept waits");
        let fd = self.open_fd(pid, server);
        let socket = self.socket_mut(server);
        socket.owner = Some((pid, fd));
        let State::Connected(stream) = &socket.state else {
            unreachable!("a queued connection is connected");
        };
        let peer = stream.remote;
        let op = self.unpark(pid, token);
        self.complete(pid, token, op, Ok(Done::Accepted { fd, peer }));
    }

    fn establish(&mut self, id: SocketId) {
        let State::Listening(listener) = &mut self.socket_mut(id).state else {
            unreachable!("establishing on a listener");
        };
        let (pid, token, client) = listener.connects.pop_front().expect("a connect waits");
        let socket = self.sockets.get(&client).expect("a waiting connect's socket");
        let (State::Connecting { to, .. }, Some(source)) = (&socket.state, socket.local) else {
            unreachable!("a waiting connect is connecting from its source address");
        };
        let to = *to;
        let family = socket.family;
        let server = self.new_socket(family, Some(to), State::Fresh);
        self.socket_mut(server).state = State::Connected(Stream::new(client, source));
        self.socket_mut(client).state = State::Connected(Stream::new(server, to));
        if let State::Listening(listener) = &mut self.socket_mut(id).state {
            listener.queue.push_back(server);
        }
        let op = self.unpark(pid, token);
        self.complete(pid, token, op, Ok(Done::Nothing));
    }

    fn try_recv(&mut self, id: SocketId) {
        let short = self.config.faults.short_recv;
        let Some(socket) = self.sockets.get_mut(&id) else {
            return;
        };
        let (Some((pid, _)), State::Connected(stream)) = (socket.owner, &mut socket.state) else {
            return;
        };
        let Some(token) = stream.receiver else {
            return;
        };
        let flight = held(&mut self.processes, pid, token);
        let Some(Op::Recv { buf, .. }) = flight else {
            unreachable!("a socket's receiver is a waiting Recv");
        };
        let result = if stream.inbox.is_empty() {
            match stream.fate {
                Fate::Reset => {
                    stream.fate = Fate::Dead;
                    Err(Error::Reset)
                }
                Fate::Dead => Ok(Done::Count(0)),
                Fate::Open if stream.ended => Ok(Done::Count(0)),
                Fate::Open => return,
            }
        } else {
            let mut n = stream.inbox.len().min(buf.len());
            if n > 1 && self.rng.chance(short) {
                n = usize_from(self.rng.between(1, u64_of(n)));
            }
            for slot in buf.iter_mut().take(n) {
                *slot = stream.inbox.pop_front().expect("n is at most the bytes received");
            }
            Ok(Done::Count(u32::try_from(n).expect("a Recv buffer's length fits a u32")))
        };
        stream.receiver = None;
        if let (Ok(Done::Count(1..)), Some(peer)) = (result, stream.peer) {
            self.pokes.push_back(peer);
        }
        let op = self.unpark(pid, token);
        self.complete(pid, token, op, result);
    }

    fn try_send(&mut self, id: SocketId) {
        let Some(socket) = self.sockets.get(&id) else {
            return;
        };
        let (Some((pid, _)), State::Connected(stream)) = (socket.owner, &socket.state) else {
            return;
        };
        let Some(token) = stream.sender else {
            return;
        };
        let (fate, shut, peer) = (stream.fate, stream.shut, stream.peer);
        let Some(Op::Send { bytes, from, .. }) = held(&mut self.processes, pid, token) else {
            unreachable!("a socket's sender is a waiting Send");
        };
        let left = bytes.get(usize_from(u64::from(*from))..).expect("a valid Send has bytes left");
        let result = match (fate, peer) {
            (Fate::Reset, _) => {
                self.stream_mut(id).fate = Fate::Dead;
                Err(Error::Reset)
            }
            (Fate::Dead, _) => Err(Error::BrokenPipe),
            (Fate::Open, _) if shut => Err(Error::BrokenPipe),
            // The peer closed with nothing unread: as on Linux, the bytes are
            // accepted, the peer answers with a reset, and the next Send
            // fails. Nothing reads them.
            (Fate::Open, None) => {
                let n = cut(&mut self.rng, self.config.faults.short_send, left.len().min(usize_of(self.config.buffer)));
                self.stream_mut(id).fate = Fate::Dead;
                Ok(Done::Count(u32::try_from(n).expect("a Send's length fits a u32")))
            }
            (Fate::Open, Some(peer)) => {
                let buffer = usize_of(self.config.buffer);
                let other = self.sockets.get(&peer).expect("a linked peer exists");
                let State::Connected(end) = &other.state else {
                    unreachable!("a linked peer is connected");
                };
                let room = buffer.saturating_sub(end.inbox.len());
                if room == 0 {
                    return;
                }
                let n = cut(&mut self.rng, self.config.faults.short_send, left.len().min(room));
                let sent = left.get(..n).expect("n is at most the bytes left");
                let end = match &mut self.sockets.get_mut(&peer).expect("a linked peer exists").state {
                    State::Connected(end) => end,
                    State::Fresh | State::Connecting { .. } | State::Listening(_) => {
                        unreachable!("a linked peer is connected")
                    }
                };
                end.inbox.extend(sent.iter().copied());
                self.pokes.push_back(peer);
                Ok(Done::Count(u32::try_from(n).expect("a Send's length fits a u32")))
            }
        };
        self.stream_mut(id).sender = None;
        let op = self.unpark(pid, token);
        self.complete(pid, token, op, result);
    }

    /// The reset fault: the connection of `id` breaks, on both ends.
    fn maybe_reset(&mut self, id: SocketId) {
        let stream = self.stream_mut(id);
        let (Fate::Open, Some(peer)) = (stream.fate, stream.peer) else {
            return;
        };
        if self.rng.chance(self.config.faults.reset) {
            self.reset_end(id);
            self.reset_end(peer);
        }
    }

    /// This end of a connection is reset: its next `Recv` (once the bytes
    /// already received are read) or `Send` hears of it. The link to the peer
    /// is cut, from this side.
    fn reset_end(&mut self, id: SocketId) {
        let stream = self.stream_mut(id);
        if let Some(peer) = stream.peer.take()
            && let Some(Socket { state: State::Connected(end), .. }) = self.sockets.get_mut(&peer)
        {
            end.peer = None;
        }
        let stream = self.stream_mut(id);
        if stream.fate == Fate::Open {
            stream.fate = Fate::Reset;
        }
        self.pokes.push_back(id);
    }
}

// Bookkeeping.
impl Sim {
    fn complete(&mut self, pid: Pid, token: Token, op: Op, result: Result<Done, Error>) {
        let complete = Complete { op: token, kind: op, result };
        if !complete.is_valid() {
            self.fail(pid, &format!("the simulator broke the contract with {complete:?}"));
        }
        let Some(flight) = self.process_mut(pid).flights.get_mut(&token) else {
            self.fail(pid, &format!("a completion of {token:?}, which is not in flight"));
        };
        if flight.done || flight.held.is_some() {
            self.fail(pid, &format!("a second completion of {token:?}"));
        }
        flight.done = true;
        let latency = self.latency();
        if latency == 0 {
            self.process_mut(pid).ready.push_back(complete);
        } else {
            self.schedule(latency, Due::Post { pid, complete });
        }
    }

    fn latency(&mut self) -> u64 {
        let faults = self.config.faults;
        if faults.latency_max.as_nanos() == 0 || !self.rng.chance(faults.latency) {
            return 0;
        }
        self.rng.between(1, faults.latency_max.as_nanos())
    }

    fn schedule(&mut self, after: u64, due: Due) {
        let at = self.now.checked_add(Duration::from_nanos(after)).expect("time does not run out");
        let order = self.next_due;
        self.next_due = order.checked_add(1).expect("fewer than 2^64 events");
        self.schedule.insert((at, order), due);
    }

    fn park(&mut self, pid: Pid, token: Token, op: Op) {
        let flight = self.process_mut(pid).flights.get_mut(&token).expect("parked while in flight");
        flight.held = Some(op);
    }

    fn unpark(&mut self, pid: Pid, token: Token) -> Op {
        let flight = self.process_mut(pid).flights.get_mut(&token).expect("unparked while in flight");
        flight.held.take().expect("a waiting operation holds its record")
    }

    fn new_socket(&mut self, family: Family, local: Option<Addr>, state: State) -> SocketId {
        let id = self.next_socket;
        self.next_socket = id.checked_add(1).expect("fewer than 2^64 sockets");
        self.sockets.insert(id, Socket { owner: None, family, local, closing_only: false, state });
        id
    }

    fn open_fd(&mut self, pid: Pid, id: SocketId) -> Fd {
        let process = self.process_mut(pid);
        let fd = Fd::new(process.next_fd);
        process.next_fd = process.next_fd.checked_add(1).expect("fewer than 2^31 descriptors");
        process.fds.insert(fd, id);
        self.socket_mut(id).owner = Some((pid, fd));
        fd
    }

    /// A free port in the ephemeral range, from a random start: one no
    /// socket of `family` holds, on any address.
    fn ephemeral(&mut self, family: Family) -> Option<u16> {
        let span = u32::from(EPHEMERAL_LAST - EPHEMERAL_FIRST) + 1;
        let start = u32::try_from(self.rng.below(u64::from(span))).expect("below a u32");
        for step in 0..span {
            let offset = start.checked_add(step).and_then(|n| n.checked_rem(span)).expect("a span of ports");
            let port = EPHEMERAL_FIRST.checked_add(u16::try_from(offset).expect("within the range"))?;
            let mut taken = false;
            for socket in self.sockets.values() {
                if socket.family == family && socket.local.is_some_and(|local| local.port() == port) {
                    taken = true;
                }
            }
            if !taken {
                return Some(port);
            }
        }
        None
    }

    /// Whether a socket other than `except` listens on `port` at an address
    /// that overlaps `ip`.
    fn listening_at(&self, family: Family, ip: core::net::IpAddr, port: u16, except: SocketId) -> bool {
        for (id, socket) in &self.sockets {
            if let (State::Listening(_), Some(local)) = (&socket.state, socket.local)
                && *id != except
                && socket.family == family
                && local.port() == port
                && overlaps(local.ip(), ip)
            {
                return true;
            }
        }
        false
    }

    /// The listener a connection to `to` reaches.
    fn listener_for(&self, family: Family, to: Addr) -> Option<SocketId> {
        for (id, socket) in &self.sockets {
            if let (State::Listening(_), Some(local)) = (&socket.state, socket.local)
                && socket.family == family
                && local.port() == to.port()
                && overlaps(local.ip(), to.ip())
            {
                return Some(*id);
            }
        }
        None
    }

    fn record(&mut self, pid: Pid, event: Event) {
        self.trace.push(Entry { at: self.now, pid, event });
    }

    fn process(&self, pid: Pid) -> &Process {
        self.processes.get(usize_from(u64::from(pid.0))).expect("a process of this world")
    }

    fn process_mut(&mut self, pid: Pid) -> &mut Process {
        self.processes.get_mut(usize_from(u64::from(pid.0))).expect("a process of this world")
    }

    fn socket_mut(&mut self, id: SocketId) -> &mut Socket {
        self.sockets.get_mut(&id).expect("a socket of this world")
    }

    fn stream_mut(&mut self, id: SocketId) -> &mut Stream {
        match &mut self.socket_mut(id).state {
            State::Connected(stream) => stream,
            State::Fresh | State::Connecting { .. } | State::Listening(_) => unreachable!("a connected socket"),
        }
    }

    /// Fails the world, loudly, with the seed and the end of the trace.
    #[expect(clippy::panic, reason = "the simulator fails the world on a broken invariant (overview.md, 9)")]
    fn fail(&self, pid: Pid, what: &str) -> ! {
        let from = self.trace.len().saturating_sub(TAIL);
        let tail = trace::render(self.seed, self.trace.get(from..).unwrap_or_default());
        panic!("skein-sim: {pid} at {} ns: {what}\n{tail}", self.now.as_nanos());
    }
}

/// The socket of an operation on a descriptor, which `check` found.
fn on(socket: Option<SocketId>) -> SocketId {
    socket.expect("an operation on a descriptor names its socket")
}

/// The record a waiting operation holds.
fn held(processes: &mut [Process], pid: Pid, token: Token) -> Option<&mut Op> {
    let process = processes.get_mut(usize_from(u64::from(pid.0)))?;
    process.flights.get_mut(&token)?.held.as_mut()
}

/// `n`, or with the fault, a random count from 1 to `n`.
fn cut(rng: &mut Rng, chance: u32, n: usize) -> usize {
    if n > 1 && rng.chance(chance) { usize_from(rng.between(1, u64_of(n))) } else { n }
}

fn usize_of(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits a usize")
}

fn usize_from(n: u64) -> usize {
    usize::try_from(n).expect("a count of this world fits a usize")
}

fn u64_of(n: usize) -> u64 {
    u64::try_from(n).expect("a usize fits a u64")
}
