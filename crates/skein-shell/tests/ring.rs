//! The ring adapter against the real kernel, on loopback (testing-pyramid.md,
//! sections 2.6 and 8): records submitted directly, as io will, and every
//! completion checked against the contract of `skein_io::kernel` as it
//! arrives. Where the conformance suite will run against the ring.
//!
//! A machine without io_uring (a seccomp profile, `io_uring_disabled`) fails
//! every test here, saying so, rather than passing them silently.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use skein_io::kernel::{Addr, Complete, Done, Error, Family, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token};
use skein_shell::{Clock, Config, Kernel, OpenError, Wait};

/// How long a test waits for any one completion before it fails.
const PATIENCE: Duration = Duration::from_secs(10);

/// A kernel driven by hand: records go down one at a time, and every
/// completion is checked as it comes up.
struct World {
    kernel: Kernel,
    clock: Clock,
    submissions: Queue<Submit>,
    completions: Queue<Complete>,
    next: u64,
    /// Submitted, not yet completed: each completes exactly once.
    outstanding: BTreeSet<Token>,
    /// Completed, not yet looked at by the test.
    arrived: BTreeMap<Token, Complete>,
}

impl World {
    fn new(operations: u32) -> World {
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

    fn token(&mut self) -> Token {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).expect("tokens never run out");
        token
    }

    /// Submits `kind`, which the kernel must take now.
    fn start(&mut self, kind: Op) -> Token {
        let op = self.token();
        self.submit(op, kind);
        op
    }

    fn submit(&mut self, op: Token, kind: Op) {
        self.submissions.push(Submit { op, kind });
        self.kernel.submit(&mut self.submissions, Wait::No);
        assert!(self.submissions.is_empty(), "the kernel had room for the record");
        assert!(self.outstanding.insert(op), "a new token");
    }

    /// One turn of the loop: submit nothing, wait up to `until`, reap, and
    /// check what came up.
    fn turn(&mut self, until: Time) {
        self.kernel.submit(&mut self.submissions, Wait::Until(until));
        self.reap();
    }

    fn reap(&mut self) {
        let mut completions = std::mem::replace(&mut self.completions, Queue::with_capacity(0));
        self.kernel.reap(&mut completions);
        self.check(&mut completions);
        self.completions = completions;
    }

    /// Checks the completions reaped into `completions`, and keeps them.
    fn check(&mut self, completions: &mut Queue<Complete>) {
        while let Some(complete) = completions.pop() {
            assert!(complete.is_valid(), "the ring kept the contract: {complete:?}");
            assert!(self.outstanding.remove(&complete.op), "one completion per submission: {complete:?}");
            self.arrived.insert(complete.op, complete);
        }
    }

    /// The completion of `op`, waiting for it.
    fn wait(&mut self, op: Token) -> Complete {
        let deadline = self.later(PATIENCE);
        while !self.arrived.contains_key(&op) {
            assert!(self.clock.now().now < deadline, "{op:?} completed in time");
            self.turn(deadline);
        }
        self.arrived.remove(&op).expect("it arrived")
    }

    /// Submits `kind` and waits for its completion.
    fn run(&mut self, kind: Op) -> Complete {
        let op = self.start(kind);
        self.wait(op)
    }

    fn later(&self, span: Duration) -> Time {
        self.clock.now().now.checked_add(span).expect("a time in reach")
    }

    fn socket(&mut self, family: Family) -> Fd {
        match self.run(Op::Socket { family }).result {
            Ok(Done::Fd(fd)) => fd,
            other => panic!("a socket: {other:?}"),
        }
    }

    fn bind(&mut self, fd: Fd, addr: Addr) -> Result<Addr, Error> {
        match self.run(Op::Bind { fd, addr }).result {
            Ok(Done::Bound(bound)) => Ok(bound),
            Ok(other) => panic!("a bind answers with its address: {other:?}"),
            Err(error) => Err(error),
        }
    }

    fn listen(&mut self, fd: Fd) -> Result<Done, Error> {
        self.run(Op::Listen { fd, backlog: 16 }).result
    }

    /// A socket listening on `addr`, port 0, and the address it got.
    fn listener(&mut self, addr: Addr) -> (Fd, Addr) {
        let fd = self.socket(Family::of(&addr));
        let bound = self.bind(fd, addr).expect("a loopback address binds");
        assert_ne!(bound.port(), 0, "port 0 is resolved");
        assert_eq!(bound.ip(), addr.ip(), "the address asked for");
        assert_eq!(self.listen(fd), Ok(Done::Nothing), "a bound socket listens");
        (fd, bound)
    }

    /// A connection to `listener` at `addr`: the connecting socket, the
    /// accepted one, and the peer the accept saw.
    fn connection(&mut self, listener: Fd, addr: Addr) -> (Fd, Fd, Addr) {
        let client = self.socket(Family::of(&addr));
        let accept = self.start(Op::Accept { fd: listener });
        let connect = self.start(Op::Connect { fd: client, addr });
        assert_eq!(self.wait(connect).result, Ok(Done::Nothing), "a connect to a listener");
        match self.wait(accept).result {
            Ok(Done::Accepted { fd, peer }) => (client, fd, peer),
            other => panic!("an accept: {other:?}"),
        }
    }

    fn send(&mut self, fd: Fd, bytes: &[u8]) -> Result<Done, Error> {
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
            | Op::Cancel { .. } => panic!("a send hands back its own record"),
        }
        sent.result
    }

    /// What one receive of up to `len` bytes got.
    fn recv(&mut self, fd: Fd, len: usize) -> Result<Box<[u8]>, Error> {
        let received = self.run(Op::recv(fd, vec![0; len].into_boxed_slice()).expect("room to receive"));
        match (received.result, received.kind) {
            (Ok(Done::Count(n)), Op::Recv { buf, .. }) => Ok(Box::from(filled(&buf, n))),
            (Err(error), Op::Recv { buf, .. }) => {
                assert_eq!(buf.len(), len, "a failed receive hands its buffer back");
                Err(error)
            }
            other => panic!("a receive: {other:?}"),
        }
    }

    fn close(&mut self, fd: Fd) {
        assert_eq!(self.run(Op::Close { fd }).result, Ok(Done::Nothing), "{fd:?} closes");
    }

    /// Nothing left in flight, and every completion looked at.
    fn settle(self) {
        assert!(self.outstanding.is_empty(), "every submission completed: {:?}", self.outstanding);
        assert!(self.arrived.is_empty(), "every completion was looked at: {:?}", self.arrived);
        assert_eq!(self.kernel.in_flight(), 0, "nothing in flight");
    }
}

/// The part of a receive buffer a count says was filled.
fn filled(buf: &[u8], n: u32) -> &[u8] {
    buf.get(..usize::try_from(n).expect("a u32 fits a usize")).expect("a count within its buffer")
}

fn v4(port: u16) -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

fn v6(port: u16) -> Addr {
    SocketAddr::from((Ipv6Addr::LOCALHOST, port))
}

/// Bytes no two offsets of a short span repeat in, to catch reordering.
fn pattern(len: usize) -> Box<[u8]> {
    let mut bytes = vec![0_u8; len];
    for (at, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::try_from(at.wrapping_mul(31).wrapping_add(at >> 8) & 0xff).expect("masked to a byte");
    }
    bytes.into_boxed_slice()
}

#[test]
fn a_connection_carries_bytes_both_ways_and_each_side_ends_its_sending() {
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v4(0));
    let (client, server, peer) = world.connection(listener, addr);
    assert_eq!(peer.ip(), addr.ip(), "the peer is on loopback");
    assert_ne!(peer.port(), 0, "the peer's port");

    assert_eq!(world.send(client, b"hello"), Ok(Done::Count(5)));
    assert_eq!(world.recv(server, 64).as_deref(), Ok(&b"hello"[..]));
    assert_eq!(world.send(server, b"world!"), Ok(Done::Count(6)));
    assert_eq!(world.recv(client, 64).as_deref(), Ok(&b"world!"[..]));

    assert_eq!(world.run(Op::Shutdown { fd: client }).result, Ok(Done::Nothing));
    assert_eq!(world.recv(server, 64).as_deref(), Ok(&b""[..]), "the client's end");
    assert_eq!(world.run(Op::Shutdown { fd: client }).result, Ok(Done::Nothing), "a second shutdown");
    assert_eq!(world.send(server, b"after"), Ok(Done::Count(5)), "the other direction still works");
    assert_eq!(world.recv(client, 64).as_deref(), Ok(&b"after"[..]), "receiving works after a shutdown");
    assert_eq!(world.run(Op::Shutdown { fd: server }).result, Ok(Done::Nothing));
    assert_eq!(world.recv(client, 64).as_deref(), Ok(&b""[..]), "the server's end");

    for fd in [client, server, listener] {
        world.close(fd);
    }
    world.settle();
}

#[test]
fn an_ipv6_connection_carries_bytes() {
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v6(0));
    let (client, server, peer) = world.connection(listener, addr);
    assert_eq!(peer.ip(), addr.ip(), "the peer is on loopback");
    assert_eq!(world.send(client, b"over six"), Ok(Done::Count(8)));
    assert_eq!(world.recv(server, 64).as_deref(), Ok(&b"over six"[..]));
    for fd in [client, server, listener] {
        world.close(fd);
    }
    world.settle();
}

#[test]
fn an_ipv6_socket_is_v6_only() {
    let mut world = World::new(4);
    let fd = world.socket(Family::Ipv6);
    let mapped = SocketAddr::from((Ipv4Addr::LOCALHOST.to_ipv6_mapped(), 0));
    assert_eq!(world.bind(fd, mapped), Err(Error::InvalidArgument), "no IPv4-mapped address on a v6-only socket");
    world.close(fd);
    world.settle();
}

#[test]
fn a_large_transfer_takes_short_sends_and_many_receives() {
    const LEN: usize = 32 << 20;
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v4(0));
    let (client, server, _peer) = world.connection(listener, addr);
    let bytes = pattern(LEN);

    // Nothing reads yet, so the first send takes what the socket buffers hold.
    let first = world.run(Op::send(client, bytes.clone(), 0).unwrap());
    let Ok(Done::Count(first_sent)) = first.result else { panic!("a send: {first:?}") };
    assert!(usize::try_from(first_sent).unwrap() < LEN, "a short send: {first_sent} of {LEN}");

    let mut received = Vec::with_capacity(LEN);
    let mut reads = 0_u32;
    let mut from = first_sent;
    let mut sending = None;
    let mut receiving = None;
    let deadline = world.later(Duration::from_secs(60));
    while received.len() < LEN {
        assert!(world.clock.now().now < deadline, "the transfer finished in time");
        if sending.is_none() && usize::try_from(from).unwrap() < LEN {
            sending = Some(world.start(Op::send(client, bytes.clone(), from).unwrap()));
        }
        if receiving.is_none() {
            receiving = Some(world.start(Op::recv(server, vec![0; 64 << 10].into_boxed_slice()).unwrap()));
        }
        world.turn(deadline);
        if let Some(op) = sending
            && let Some(sent) = world.arrived.remove(&op)
        {
            let Ok(Done::Count(n)) = sent.result else { panic!("a send: {sent:?}") };
            from = from.checked_add(n).unwrap();
            sending = None;
        }
        if let Some(op) = receiving
            && let Some(got) = world.arrived.remove(&op)
        {
            let (Ok(Done::Count(n)), Op::Recv { buf, .. }) = (got.result, got.kind) else { panic!("a receive") };
            assert_ne!(n, 0, "the stream has not ended");
            received.extend_from_slice(filled(&buf, n));
            reads = reads.checked_add(1).unwrap();
            receiving = None;
        }
    }
    assert_eq!(usize::try_from(from).unwrap(), LEN, "every byte was sent once");
    assert!(sending.is_none(), "no send left");
    assert!(reads > 1, "many receives: {reads}");
    assert!(received == *bytes, "the bytes arrived in order");

    for fd in [client, server, listener] {
        world.close(fd);
    }
    world.settle();
}

#[test]
fn a_connect_where_nothing_listens_is_refused() {
    let mut world = World::new(4);
    // A bound socket that does not listen holds the port.
    let holder = world.socket(Family::Ipv4);
    let addr = world.bind(holder, v4(0)).unwrap();
    let client = world.socket(Family::Ipv4);
    assert_eq!(world.run(Op::Connect { fd: client, addr }).result, Err(Error::Refused));
    world.close(client);
    world.close(holder);
    world.settle();
}

#[test]
fn a_bind_where_a_socket_listens_is_address_in_use() {
    let mut world = World::new(4);
    let (listener, addr) = world.listener(v4(0));
    let other = world.socket(Family::Ipv4);
    assert_eq!(world.bind(other, addr), Err(Error::AddressInUse));
    world.close(other);
    world.close(listener);
    world.settle();
}

#[test]
fn two_sockets_bind_one_address_and_the_second_listen_fails() {
    let mut world = World::new(4);
    let first = world.socket(Family::Ipv4);
    let addr = world.bind(first, v4(0)).unwrap();
    let second = world.socket(Family::Ipv4);
    assert_eq!(world.bind(second, addr), Ok(addr), "both bind");
    assert_eq!(world.listen(first), Ok(Done::Nothing));
    assert_eq!(world.listen(second), Err(Error::AddressInUse));
    world.close(second);
    world.close(first);
    world.settle();
}

/// The cancel lands, or loses the race; either way the target says what it
/// did, and both complete once.
fn cancel_answer(cancel: &Complete) {
    assert!(
        cancel.result == Ok(Done::Nothing) || cancel.result == Err(Error::TooLate),
        "a cancel finds its target or is too late: {cancel:?}"
    );
}

#[test]
fn a_cancelled_accept_completes_once_as_cancelled() {
    let mut world = World::new(4);
    let (listener, _addr) = world.listener(v4(0));
    let accept = world.start(Op::Accept { fd: listener });
    let cancel = world.start(Op::Cancel { target: accept });
    cancel_answer(&world.wait(cancel));
    assert_eq!(world.wait(accept).result, Err(Error::Cancelled), "nothing connected");
    world.close(listener);
    world.settle();
}

#[test]
fn a_cancelled_recv_completes_once_as_cancelled_with_its_buffer() {
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v4(0));
    let (client, server, _peer) = world.connection(listener, addr);
    let recv = world.start(Op::recv(server, vec![0; 16].into_boxed_slice()).unwrap());
    let cancel = world.start(Op::Cancel { target: recv });
    cancel_answer(&world.wait(cancel));
    let received = world.wait(recv);
    assert_eq!(received.result, Err(Error::Cancelled), "nothing was sent");
    assert_eq!(received.kind, Op::recv(server, vec![0; 16].into_boxed_slice()).unwrap(), "the buffer comes back");
    for fd in [client, server, listener] {
        world.close(fd);
    }
    world.settle();
}

#[test]
fn a_cancel_of_what_is_not_in_flight_is_too_late() {
    let mut world = World::new(4);
    let fd = world.socket(Family::Ipv4);
    let never = world.token();
    assert_eq!(world.run(Op::Cancel { target: never }).result, Err(Error::TooLate));
    let done = world.start(Op::Listen { fd, backlog: 1 });
    assert_eq!(world.wait(done).result, Ok(Done::Nothing));
    assert_eq!(world.run(Op::Cancel { target: done }).result, Err(Error::TooLate), "it completed");
    world.close(fd);
    world.settle();
}

#[test]
fn closing_with_unread_data_resets_the_peer_once() {
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v4(0));
    let (client, server, _peer) = world.connection(listener, addr);
    assert_eq!(world.send(client, b"unread"), Ok(Done::Count(6)));
    world.close(server);

    assert_eq!(world.recv(client, 64), Err(Error::Reset), "the reset, reported once");
    assert_eq!(world.recv(client, 64).as_deref(), Ok(&b""[..]), "then the end");
    assert_eq!(world.send(client, b"more"), Err(Error::BrokenPipe));
    assert_eq!(world.run(Op::Shutdown { fd: client }).result, Err(Error::NotConnected));

    world.close(client);
    world.close(listener);
    world.settle();
}

#[test]
fn submit_takes_no_record_past_the_operations_configured() {
    let mut world = World::new(2);
    let (first, first_addr) = world.listener(v4(0));
    let (second, _second_addr) = world.listener(v4(0));

    let accept_first = world.token();
    let accept_second = world.token();
    let cancel = world.token();
    world.submissions.push(Submit { op: accept_first, kind: Op::Accept { fd: first } });
    world.submissions.push(Submit { op: accept_second, kind: Op::Accept { fd: second } });
    world.submissions.push(Submit { op: cancel, kind: Op::Cancel { target: accept_second } });
    world.kernel.submit(&mut world.submissions, Wait::No);
    assert_eq!(world.submissions.len(), 1, "the third record waits");
    assert_eq!((world.kernel.in_flight(), world.kernel.room()), (2, 0));
    world.outstanding.extend([accept_first, accept_second]);
    let soon = world.later(Duration::from_millis(20));
    world.kernel.submit(&mut world.submissions, Wait::Until(soon));
    assert_eq!(world.submissions.len(), 1, "still no room");

    // A client on a kernel of its own completes the first accept, which
    // frees a slot for the cancel.
    let mut client_world = World::new(2);
    let client = client_world.socket(Family::Ipv4);
    let connect = client_world.start(Op::Connect { fd: client, addr: first_addr });
    let Ok(Done::Accepted { fd: server, .. }) = world.wait(accept_first).result else { panic!("an accept") };
    assert_eq!(client_world.wait(connect).result, Ok(Done::Nothing));

    world.kernel.submit(&mut world.submissions, Wait::No);
    assert!(world.submissions.is_empty(), "the cancel found room");
    world.outstanding.insert(cancel);
    cancel_answer(&world.wait(cancel));
    assert_eq!(world.wait(accept_second).result, Err(Error::Cancelled));

    client_world.close(client);
    client_world.settle();
    for fd in [server, first, second] {
        world.close(fd);
    }
    world.settle();
}

#[test]
fn a_wait_with_a_deadline_returns_by_it_when_nothing_completes() {
    let mut world = World::new(4);
    for in_flight in [false, true] {
        let pending = if in_flight {
            let (listener, _addr) = world.listener(v4(0));
            Some((listener, world.start(Op::Accept { fd: listener })))
        } else {
            None
        };
        let deadline = world.later(Duration::from_millis(50));
        world.kernel.submit(&mut world.submissions, Wait::Until(deadline));
        let woke = world.clock.now().now;
        assert!(woke >= deadline, "it waited for the deadline");
        assert!(woke < deadline.checked_add(Duration::from_secs(1)).unwrap(), "and returned by it");
        world.reap();
        assert!(world.arrived.is_empty(), "nothing completed");
        if let Some((listener, accept)) = pending {
            let cancel = world.start(Op::Cancel { target: accept });
            cancel_answer(&world.wait(cancel));
            assert_eq!(world.wait(accept).result, Err(Error::Cancelled));
            world.close(listener);
        }
    }
    world.settle();
}

#[test]
fn a_deadline_already_past_does_not_wait() {
    let mut world = World::new(4);
    let before = world.clock.now().now;
    world.kernel.submit(&mut world.submissions, Wait::Until(Time::ZERO));
    let after = world.clock.now().now;
    assert!(after < before.checked_add(Duration::from_millis(500)).unwrap(), "it returned at once");
    world.settle();
}

#[test]
fn a_kernel_of_no_operations_does_not_open() {
    assert_eq!(Kernel::open(Config { operations: 0 }).unwrap_err(), OpenError::NoOperations);
}

#[test]
#[should_panic(expected = "io submits only valid records")]
fn an_invalid_record_is_refused_at_submit() {
    let mut world = World::new(4);
    world.submit(Token::new(1), Op::Recv { fd: Fd::new(0), buf: Box::from([]) });
}

#[test]
#[should_panic(expected = "io never reuses a token in flight")]
fn a_token_already_in_flight_is_refused_at_submit() {
    let mut world = World::new(4);
    let (listener, _addr) = world.listener(v4(0));
    let accept = world.start(Op::Accept { fd: listener });
    world.submit(accept, Op::Close { fd: listener });
}

#[test]
fn the_clock_moves_forward_and_the_seed_varies() {
    let clock = Clock::new();
    let first = clock.now();
    let second = clock.now();
    assert!(second.now >= first.now, "monotonic");
    assert!(first.wall.as_secs() > 1_700_000_000, "the wall clock is set: {first:?}");
    let seeds = [skein_shell::seed().unwrap(), skein_shell::seed().unwrap()];
    assert_ne!(seeds[0], seeds[1], "two seeds differ");
}

#[test]
fn a_kernel_dropped_with_operations_in_flight_leaves_their_memory_to_the_kernel() {
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v4(0));
    let (client, server, _peer) = world.connection(listener, addr);
    let _accept = world.start(Op::Accept { fd: listener });
    let _recv = world.start(Op::recv(server, vec![0; 16].into_boxed_slice()).expect("room to receive"));
    drop(world);

    // The descriptors outlive the ring: only a Close record closes one.
    let mut after = World::new(4);
    assert_eq!(after.send(client, b"late"), Ok(Done::Count(4)), "the connection is still open");
    for fd in [client, server, listener] {
        after.close(fd);
    }
    after.settle();
}

#[test]
fn a_wait_forever_returns_with_a_completion() {
    let mut world = World::new(4);
    let socket = world.start(Op::Socket { family: Family::Ipv4 });
    world.kernel.submit(&mut world.submissions, Wait::Forever);
    world.reap();
    let Some(Complete { result: Ok(Done::Fd(fd)), .. }) = world.arrived.remove(&socket) else {
        panic!("the socket completed before the wait returned")
    };
    world.close(fd);
    world.settle();
}

#[test]
#[should_panic(expected = "waiting forever with nothing in flight never returns")]
fn a_wait_forever_with_nothing_in_flight_is_refused() {
    let mut world = World::new(4);
    world.kernel.submit(&mut world.submissions, Wait::Forever);
}

#[test]
fn a_record_the_adapter_completes_itself_arrives_at_the_next_reap_and_cannot_be_cancelled() {
    let mut world = World::new(4);
    // SO_REUSEADDR, set before the Bind goes to the ring, fails on a
    // descriptor that does not exist, so the Bind never reaches the ring.
    let bind = world.token();
    let cancel = world.token();
    world.submissions.push(Submit { op: bind, kind: Op::Bind { fd: Fd::new(-1), addr: v4(0) } });
    world.submissions.push(Submit { op: cancel, kind: Op::Cancel { target: bind } });
    world.kernel.submit(&mut world.submissions, Wait::No);
    world.outstanding.extend([bind, cancel]);
    assert_eq!(world.wait(bind).result, Err(Error::Other(libc::EBADF)), "the backend's own failure");
    assert_eq!(world.wait(cancel).result, Err(Error::TooLate), "nothing in the ring to stop");
    world.settle();
}

#[test]
fn a_reap_takes_only_what_fits_and_the_rest_waits_without_blocking() {
    let mut world = World::new(4);
    let sockets = [world.token(), world.token(), world.token()];
    for op in sockets {
        world.submissions.push(Submit { op, kind: Op::Socket { family: Family::Ipv4 } });
    }
    world.kernel.submit(&mut world.submissions, Wait::No);
    world.outstanding.extend(sockets);

    let mut one = Queue::with_capacity(1);
    let deadline = world.later(PATIENCE);
    while one.is_empty() {
        world.kernel.submit(&mut world.submissions, Wait::Until(deadline));
        world.kernel.reap(&mut one);
    }
    assert_eq!(world.kernel.in_flight(), 2, "two completions wait");
    for _ in 0..2 {
        // Completions are waiting, so a wait with a far deadline returns at once.
        let before = world.clock.now().now;
        world.kernel.submit(&mut world.submissions, Wait::Until(deadline));
        let after = world.clock.now().now;
        assert!(
            after < before.checked_add(Duration::from_millis(500)).expect("in reach"),
            "no wait while completions are queued"
        );
        world.check(&mut one);
        world.kernel.reap(&mut one);
        assert_eq!(one.len(), 1, "one more, as much as fits");
    }
    world.check(&mut one);
    assert_eq!(world.kernel.in_flight(), 0, "all three reaped");
    for op in sockets {
        let Ok(Done::Fd(fd)) = world.wait(op).result else { panic!("a socket") };
        world.close(fd);
    }
    world.settle();
}
