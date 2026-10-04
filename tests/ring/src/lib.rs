//! The ring adapter's own tests (shell.md, 9), against the real kernel on
//! loopback: [`World`], a kernel driven by hand, whose records go down one
//! at a time and whose completions are checked against the contract of
//! `skein_io::kernel` as they come up. The tests are `tests/ring.rs`.
//!
//! A machine without `io_uring` (a seccomp profile, `io_uring_disabled`)
//! fails every test, saying so, rather than passing them silently.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr};

use skein_io::kernel::{Addr, Complete, Done, Error, Family, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token};
use skein_shell::{Clock, Config, Kernel, Wait};

/// How long a test waits for any one completion before it fails.
pub const PATIENCE: Duration = Duration::from_secs(10);

/// A kernel driven by hand: records go down one at a time, and every
/// completion is checked as it comes up.
pub struct World {
    pub kernel: Kernel,
    pub clock: Clock,
    pub submissions: Queue<Submit>,
    completions: Queue<Complete>,
    next: u64,
    /// Submitted, not yet completed: each completes exactly once.
    pub outstanding: BTreeSet<Token>,
    /// Completed, not yet looked at by the test.
    pub arrived: BTreeMap<Token, Complete>,
}

impl World {
    #[must_use]
    pub fn new(operations: u32) -> World {
        let kernel = match Kernel::open(Config { operations }) {
            Ok(kernel) => kernel,
            Err(error) => panic!("io_uring is not usable here, so the ring cannot be tested: {error}"),
        };
        World {
            kernel,
            clock: Clock::new(),
            submissions: Queue::with_capacity(operations.checked_mul(2).expect("a small world")),
            completions: Queue::with_capacity(operations),
            next: 1,
            outstanding: BTreeSet::new(),
            arrived: BTreeMap::new(),
        }
    }

    pub fn token(&mut self) -> Token {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).expect("tokens never run out");
        token
    }

    /// Submits `kind`, which the kernel must take now.
    pub fn start(&mut self, kind: Op) -> Token {
        let op = self.token();
        self.submit(op, kind);
        op
    }

    pub fn submit(&mut self, op: Token, kind: Op) {
        self.submissions.push(Submit { op, kind });
        self.kernel.submit(&mut self.submissions, Wait::No);
        assert!(self.submissions.is_empty(), "the kernel had room for the record");
        assert!(self.outstanding.insert(op), "a new token");
    }

    /// One turn of the loop: submit nothing, wait up to `until`, reap, and
    /// check what came up.
    pub fn turn(&mut self, until: Time) {
        self.kernel.submit(&mut self.submissions, Wait::Until(until));
        self.reap();
    }

    pub fn reap(&mut self) {
        let mut completions = std::mem::replace(&mut self.completions, Queue::with_capacity(0));
        self.kernel.reap(&mut completions);
        self.check(&mut completions);
        self.completions = completions;
    }

    /// Checks the completions reaped into `completions`, and keeps them.
    pub fn check(&mut self, completions: &mut Queue<Complete>) {
        while let Some(complete) = completions.pop() {
            assert!(complete.is_valid(), "the ring kept the contract: {complete:?}");
            assert!(self.outstanding.remove(&complete.op), "one completion per submission: {complete:?}");
            self.arrived.insert(complete.op, complete);
        }
    }

    /// The completion of `op`, waiting for it.
    pub fn wait(&mut self, op: Token) -> Complete {
        let deadline = self.later(PATIENCE);
        while !self.arrived.contains_key(&op) {
            assert!(self.clock.now().now < deadline, "{op:?} completed in time");
            self.turn(deadline);
        }
        self.arrived.remove(&op).expect("it arrived")
    }

    /// Submits `kind` and waits for its completion.
    pub fn run(&mut self, kind: Op) -> Complete {
        let op = self.start(kind);
        self.wait(op)
    }

    #[must_use]
    pub fn later(&self, span: Duration) -> Time {
        self.clock.now().now.checked_add(span).expect("a time in reach")
    }

    pub fn socket(&mut self, family: Family) -> Fd {
        match self.run(Op::Socket { family }).result {
            Ok(Done::Fd(fd)) => fd,
            other => panic!("a socket: {other:?}"),
        }
    }

    pub fn bind(&mut self, fd: Fd, addr: Addr) -> Result<Addr, Error> {
        match self.run(Op::Bind { fd, addr }).result {
            Ok(Done::Bound(bound)) => Ok(bound),
            Ok(other) => panic!("a bind answers with its address: {other:?}"),
            Err(error) => Err(error),
        }
    }

    pub fn listen(&mut self, fd: Fd) -> Result<Done, Error> {
        self.run(Op::Listen { fd, backlog: 16 }).result
    }

    /// A socket listening on `addr`, port 0, and the address it got.
    pub fn listener(&mut self, addr: Addr) -> (Fd, Addr) {
        let fd = self.socket(Family::of(&addr));
        let bound = self.bind(fd, addr).expect("a loopback address binds");
        assert_ne!(bound.port(), 0, "port 0 is resolved");
        assert_eq!(bound.ip(), addr.ip(), "the address asked for");
        assert_eq!(self.listen(fd), Ok(Done::Nothing), "a bound socket listens");
        (fd, bound)
    }

    /// A connection to `listener` at `addr`: the connecting socket, the
    /// accepted one, and the peer the accept saw.
    pub fn connection(&mut self, listener: Fd, addr: Addr) -> (Fd, Fd, Addr) {
        let client = self.socket(Family::of(&addr));
        let accept = self.start(Op::Accept { fd: listener });
        let connect = self.start(Op::Connect { fd: client, addr });
        assert_eq!(self.wait(connect).result, Ok(Done::Nothing), "a connect to a listener");
        match self.wait(accept).result {
            Ok(Done::Accepted { fd, peer }) => (client, fd, peer),
            other => panic!("an accept: {other:?}"),
        }
    }

    pub fn send(&mut self, fd: Fd, bytes: &[u8]) -> Result<Done, Error> {
        let sent = self.run(Op::send(fd, Box::from(bytes), 0).expect("bytes to send"));
        match sent.kind {
            Op::Send { bytes: back, .. } => assert_eq!(&*back, bytes, "a send's bytes come back untouched"),
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
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
            | Op::List { .. }
            | Op::Cancel { .. } => panic!("a send hands back its own record"),
        }
        sent.result
    }

    pub fn close(&mut self, fd: Fd) {
        assert_eq!(self.run(Op::Close { fd }).result, Ok(Done::Nothing), "{fd:?} closes");
    }

    /// Nothing left in flight, and every completion looked at.
    pub fn settle(self) {
        assert!(self.outstanding.is_empty(), "every submission completed: {:?}", self.outstanding);
        assert!(self.arrived.is_empty(), "every completion was looked at: {:?}", self.arrived);
        assert_eq!(self.kernel.in_flight(), 0, "nothing in flight");
    }
}

/// The part of a receive buffer a count says was filled.
#[must_use]
pub fn filled(buf: &[u8], n: u32) -> &[u8] {
    buf.get(..usize::try_from(n).expect("a u32 fits a usize")).expect("a count within its buffer")
}

#[must_use]
pub fn v4(port: u16) -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// Bytes no two offsets of a short span repeat in, to catch reordering.
#[must_use]
pub fn pattern(len: usize) -> Box<[u8]> {
    let mut bytes = vec![0_u8; len];
    for (at, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::try_from(at.wrapping_mul(31).wrapping_add(at >> 8) & 0xff).expect("masked to a byte");
    }
    bytes.into_boxed_slice()
}
