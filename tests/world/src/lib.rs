//! The world harness's own tests (testing.md, 6): processes that are
//! scripts of raw kernel records, so that what a test shows is the
//! harness's and not a service's, and a referee over what they received.
//!
//! - [`Script`]: a host that submits its acts one at a time, each once the
//!   last completed, records what it receives and when, and closes every
//!   descriptor it made once its acts are done or one fails. It may also
//!   hold heap past its worst case, or leak, for the checks that must catch
//!   it.
//! - [`Judge`]: a referee of [`Expect`]s over the scripts.
//!
//! The tests are `tests/world.rs`, under the counting allocator, and
//! `tests/no_allocator.rs`, without it.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};

use skein_io::kernel::{Complete, Done, Family, Fd, Op, Submit};
use skein_lib::{Queue, Time, Token, Wall, bytes};
use skein_world::{Expectation, Expectations, Host, Referee};

/// What a script does next, once the last act completed.
#[derive(Clone, Copy, Debug)]
pub enum Act {
    /// A socket: the next descriptor.
    Socket,
    /// The first descriptor bound to `127.0.0.1:port`.
    Bind(u16),
    /// The first descriptor listens.
    Listen,
    /// The first descriptor accepts: the next descriptor.
    Accept,
    /// The first descriptor connects to `127.0.0.1:port`.
    Connect(u16),
    /// The last descriptor sends these bytes, some of them.
    Send(&'static [u8]),
    /// The last descriptor receives up to this many bytes.
    Recv(u32),
    /// Nothing until this time.
    Wait(Time),
}

/// A process that is a script of raw kernel records.
#[derive(Debug)]
pub struct Script {
    acts: VecDeque<Act>,
    /// The descriptors it made, in order.
    fds: Vec<Fd>,
    /// The record in flight, if any.
    waiting: Option<Token>,
    /// Its acts are done, or one failed: it closes what it made.
    closing: bool,
    /// Everything closed.
    done: bool,
    /// Nothing until this time.
    pause: Option<Time>,
    next: u64,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    /// What it received, and when.
    pub received: Vec<(Time, Box<[u8]>)>,
    /// Bytes it allocates and keeps in its first turn, past its worst case
    /// if the test says so.
    hog: usize,
    held: Vec<Box<[u8]>>,
    /// Bytes it leaks in its first turn.
    leak: usize,
    worst: u64,
}

/// The worst case a script declares: room for its queues, what it receives
/// and its bookkeeping, with no care for precision.
pub const WORST: u64 = 64 * 1024;

impl Script {
    #[must_use]
    pub fn new(start: Time, acts: &[Act]) -> Script {
        Script {
            acts: acts.iter().copied().collect(),
            fds: Vec::with_capacity(4),
            waiting: None,
            closing: false,
            done: false,
            pause: Some(start),
            next: 1,
            completions: Queue::with_capacity(8),
            submissions: Queue::with_capacity(8),
            received: Vec::with_capacity(8),
            hog: 0,
            held: Vec::new(),
            leak: 0,
            worst: WORST,
        }
    }

    /// Allocates and keeps `bytes` in its first turn.
    #[must_use]
    pub fn hogging(mut self, bytes: usize) -> Script {
        self.hog = bytes;
        self
    }

    /// Leaks `bytes` in its first turn.
    #[must_use]
    pub fn leaking(mut self, bytes: usize) -> Script {
        self.leak = bytes;
        self
    }

    fn submit(&mut self, kind: Op) {
        let op = Token::new(self.next);
        self.next += 1;
        self.waiting = Some(op);
        self.submissions.push(Submit { op, kind });
    }

    fn first(&self) -> Fd {
        *self.fds.first().expect("a descriptor made")
    }

    fn last(&self) -> Fd {
        *self.fds.last().expect("a descriptor made")
    }

    fn landed(&mut self, now: Time, complete: &Complete) {
        assert_eq!(self.waiting.take(), Some(complete.op), "the record in flight completed");
        match &complete.result {
            Ok(Done::Fd(fd) | Done::Accepted { fd, .. }) => self.fds.push(*fd),
            Ok(Done::Count(n)) => {
                if let Op::Recv { buf, .. } = &complete.kind {
                    let n = usize::try_from(*n).expect("small");
                    self.received.push((now, bytes::copy_of(&buf[..n])));
                }
            }
            Ok(Done::Nothing | Done::Bound(_) | Done::Stat(_) | Done::Spawned { .. } | Done::Exit(_)) => {}
            Err(_) => self.closing = true,
        }
    }

    /// Whether it waits for a time still to come.
    fn paused(&self, now: Time) -> bool {
        match self.pause {
            Some(until) => now < until,
            None => false,
        }
    }

    /// Submits its next act, or its next close.
    fn proceed(&mut self, now: Time) {
        if self.waiting.is_some() || self.done || self.paused(now) {
            return;
        }
        self.pause = None;
        if !self.closing {
            match self.acts.pop_front() {
                Some(Act::Wait(until)) => {
                    self.pause = Some(until);
                    return self.proceed(now);
                }
                Some(act) => return self.start(act),
                None => self.closing = true,
            }
        }
        match self.fds.pop() {
            Some(fd) => self.submit(Op::Close { fd }),
            None => self.done = true,
        }
    }

    fn start(&mut self, act: Act) {
        let at = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let op = match act {
            Act::Socket => Op::Socket { family: Family::Ipv4 },
            Act::Bind(port) => Op::Bind { fd: self.first(), addr: at(port) },
            Act::Listen => Op::Listen { fd: self.first(), backlog: 4 },
            Act::Accept => Op::Accept { fd: self.first() },
            Act::Connect(port) => Op::Connect { fd: self.first(), addr: at(port) },
            Act::Send(sent) => Op::send(self.last(), Box::from(sent), 0).expect("bytes to send"),
            Act::Recv(len) => {
                let len = usize::try_from(len).expect("small");
                Op::recv(self.last(), bytes::zeroed(len)).expect("room to receive")
            }
            Act::Wait(_) => unreachable!("a wait submits nothing"),
        };
        self.submit(op);
    }
}

impl Host for Script {
    fn iterate(&mut self, now: Time, _wall: Wall) {
        if self.hog > 0 {
            self.held.push(bytes::zeroed(self.hog));
            self.hog = 0;
        }
        if self.leak > 0 {
            let _leaked: &mut [u8] = Box::leak(bytes::zeroed(self.leak));
            self.leak = 0;
        }
        while let Some(complete) = self.completions.pop() {
            self.landed(now, &complete);
        }
        self.proceed(now);
    }

    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }

    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }

    fn work_pending(&self, now: Time) -> bool {
        !self.completions.is_empty() || (self.waiting.is_none() && !self.done && !self.paused(now))
    }

    fn next_deadline(&self) -> Option<Time> {
        if self.done { None } else { self.pause }
    }

    fn is_empty(&self) -> bool {
        self.done && self.waiting.is_none() && self.completions.is_empty() && self.submissions.is_empty()
    }

    fn worst_case(&self) -> u64 {
        self.worst
    }

    fn operations(&self) -> u32 {
        8
    }
}

/// What a test expects of a script, by its index.
#[derive(Clone, Copy, Debug)]
pub enum Expect {
    /// Liveness: it receives something by `by`.
    Receives { at: usize, by: Time },
    /// Safety: it receives nothing until `until`.
    Silent { at: usize, until: Time },
}

impl Expectation<Script> for Expect {
    fn check(&self, now: Time, procs: &[Script]) -> Result<bool, String> {
        match *self {
            Expect::Receives { at, .. } => Ok(!procs[at].received.is_empty()),
            Expect::Silent { at, until } => {
                if !procs[at].received.is_empty() {
                    return Err("it received".to_owned());
                }
                Ok(now >= until)
            }
        }
    }

    fn deadline(&self) -> Time {
        match *self {
            Expect::Receives { by, .. } => by,
            Expect::Silent { until, .. } => until,
        }
    }
}

/// A referee of `Expect`s, which injects nothing.
#[derive(Debug)]
pub struct Judge {
    expectations: Expectations<Script, Expect>,
}

impl Judge {
    #[must_use]
    pub fn new(seed: u64, expectations: Vec<Expect>) -> Judge {
        Judge { expectations: Expectations::new(seed, expectations) }
    }
}

impl Referee<Script> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Script]) {}

    fn observe(&mut self, now: Time, procs: &[Script]) {
        self.expectations.observe(now, procs);
    }

    fn next_deadline(&self) -> Option<Time> {
        self.expectations.next_deadline()
    }

    fn overdue(&self, now: Time) -> Option<String> {
        self.expectations.overdue(now)
    }

    fn passed(&self) -> bool {
        self.expectations.passed()
    }
}

/// `n` milliseconds from the start.
#[must_use]
pub const fn ms(n: u64) -> Time {
    Time::from_nanos(n * 1_000_000)
}

/// The listener's port.
pub const PORT: u16 = 7000;

/// A server that accepts one connection and receives once from it; with a
/// pause first, so that its receive waits.
#[must_use]
pub fn server() -> Script {
    Script::new(Time::ZERO, &[Act::Socket, Act::Bind(PORT), Act::Listen, Act::Accept, Act::Recv(16)])
}

/// A client that connects at `start`, waits until `send`, and sends once.
#[must_use]
pub fn client(start: Time, send: Time) -> Script {
    Script::new(start, &[Act::Socket, Act::Connect(PORT), Act::Wait(send), Act::Send(b"hello")])
}
