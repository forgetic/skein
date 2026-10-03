//! The scenarios: each a function over a [`Backend`] that returns what it
//! saw, and a [`Check`] of it that names the rule of the contract
//! (`skein_io::kernel`, module documentation) each assertion holds the
//! backend to. Every completion is checked on the way too, by the driver.

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use skein_io::kernel::{Addr, Done, Error, Family, Fd, Op};
use skein_lib::{Duration, Token};

use crate::run::{BRIEFLY, Run, received, unexpected};
use crate::{Backend, Check};

/// The bytes each way of a connection's lifecycle: past the chaos world's
/// buffer many times over.
const LEN: usize = 4096;

/// The bytes each send of a backpressure scenario offers.
const CHUNK: usize = 256 << 10;

/// The bytes each receive of a backpressure scenario asks for.
const READ: usize = 64 << 10;

/// The most a backpressure scenario sends before it gives up on a stall:
/// more than loopback's socket buffers hold at their largest.
const MOST: u64 = 256 << 20;

/// How many connections a full accept queue scenario makes.
const CLIENTS: usize = 4;

/// The most descriptors a descriptor limit scenario opens.
const DESCRIPTORS: u32 = 64;

/// Longer than Linux takes to retransmit a SYN that a full accept queue
/// dropped: a second.
const SYN_RETRY: Duration = Duration::from_secs(2);

/// The loopback address of `family`.
const fn loopback(family: Family) -> IpAddr {
    match family {
        Family::Ipv4 => IpAddr::V4(Ipv4Addr::LOCALHOST),
        Family::Ipv6 => IpAddr::V6(Ipv6Addr::LOCALHOST),
    }
}

/// `port` on the loopback address of `family`.
fn at(family: Family, port: u16) -> Addr {
    SocketAddr::new(loopback(family), port)
}

/// Bytes no two nearby offsets repeat in, so a reordering shows.
fn pattern(len: usize, salt: u8) -> Vec<u8> {
    let mut bytes = vec![0_u8; len];
    for (at, byte) in bytes.iter_mut().enumerate() {
        let mixed = at.wrapping_mul(31).wrapping_add(at >> 8_u32).wrapping_add(usize::from(salt));
        *byte = u8::try_from(mixed & 0xff).expect("masked to a byte");
    }
    bytes
}

/// Fails, naming the rule and what was seen, unless `holds`.
fn rule<T: Debug>(holds: bool, rule: &str, seen: &T) {
    assert!(holds, "the contract: {rule}; saw {seen:?}");
}

/// Whether retried answers end with `last`, every earlier one `earlier`:
/// an answer that changes once the peer's reset or acknowledgement
/// arrives.
fn settles(
    answers: &[Result<Done, Error>],
    earlier: fn(Result<Done, Error>) -> bool,
    last: Result<Done, Error>,
) -> bool {
    match answers.split_last() {
        Some((end, before)) => *end == last && before.iter().all(|answer| earlier(*answer)),
        None => false,
    }
}

/// A `Send` that succeeded: before the peer's reset arrived, its bytes
/// lost.
fn sent(answer: Result<Done, Error>) -> bool {
    match answer {
        Ok(Done::Count(_)) => true,
        Ok(Done::Nothing | Done::Fd(_) | Done::Accepted { .. } | Done::Bound(_)) | Err(_) => false,
    }
}

/// A `Shutdown` that succeeded: before the connection was closed.
fn shut(answer: Result<Done, Error>) -> bool {
    answer == NOTHING
}

const NOTHING: Result<Done, Error> = Ok(Done::Nothing);

/// What a `Recv` answers at the end of the stream.
const END: Result<Vec<u8>, Error> = Ok(Vec::new());

/// A connection from birth to both ends closed: bind port 0, listen,
/// connect, accept, bytes both ways, each side's half-close, and a second
/// half-close while the connection lasts and once it is closed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Lifecycle {
    pub asked: Addr,
    pub bound: Addr,
    pub peer: Addr,
    pub forward_intact: bool,
    pub shutdown: Result<Done, Error>,
    pub end_at_server: Result<Vec<u8>, Error>,
    pub shutdown_again: Result<Done, Error>,
    pub send_after_shutdown: Result<Done, Error>,
    /// The server's bytes, sent after the client's half-close.
    pub backward_intact: bool,
    pub server_shutdown: Result<Done, Error>,
    pub end_at_client: Result<Vec<u8>, Error>,
    /// Each side's `Shutdowns` once both have ended their sending and read
    /// the other's end, until the connection is closed.
    pub closed_shutdowns: [Vec<Result<Done, Error>>; 2],
}

#[must_use]
pub fn lifecycle<B: Backend>(backend: &mut B, family: Family) -> Lifecycle {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let asked = at(family, 0);
    let (listener, bound) = run.listener(server, asked, 16);
    let (c, s, peer) = run.connection(client, server, listener, bound);
    let forward = pattern(LEN, 1);
    let forward_intact = run.transfer(client, c, server, s, &forward) == forward;
    let shutdown = run.shutdown(client, c);
    let end_at_server = run.recv(server, s, 64);
    let shutdown_again = run.shutdown(client, c);
    let send_after_shutdown = run.send(client, c, b"late");
    let backward = pattern(LEN, 2);
    let backward_intact = run.transfer(server, s, client, c, &backward) == backward;
    let server_shutdown = run.shutdown(server, s);
    let end_at_client = run.recv(client, c, 64);
    let closed_shutdowns = [run.shutdown_until_refused(client, c), run.shutdown_until_refused(server, s)];
    run.close(client, c);
    run.close(server, s);
    run.close(server, listener);
    run.finish();
    Lifecycle {
        asked,
        bound,
        peer,
        forward_intact,
        shutdown,
        end_at_server,
        shutdown_again,
        send_after_shutdown,
        backward_intact,
        server_shutdown,
        end_at_client,
        closed_shutdowns,
    }
}

impl Check for Lifecycle {
    fn check(&self) {
        let resolved = self.bound.ip() == self.asked.ip() && self.bound.port() != 0;
        rule(resolved, "Bind answers with the address bound, port 0 resolved", &self.bound);
        let from_loopback = self.peer.ip() == self.asked.ip() && self.peer.port() != 0;
        rule(from_loopback, "Accept names the peer: a connection from this host comes from loopback", &self.peer);
        assert!(self.forward_intact, "the contract: the bytes sent arrive, in order");
        assert_eq!(self.shutdown, NOTHING, "the contract: Shutdown ends this side's sending");
        assert_eq!(self.end_at_server, END, "the contract: a Recv of zero bytes means the stream ended");
        assert_eq!(self.shutdown_again, NOTHING, "the contract: a second Shutdown is Ok while the connection lasts");
        assert_eq!(
            self.send_after_shutdown,
            Err(Error::BrokenPipe),
            "the contract: no Send after this side's Shutdown"
        );
        assert!(self.backward_intact, "the contract: Recv keeps working after this side's Shutdown");
        assert_eq!(self.server_shutdown, NOTHING, "the contract: Shutdown ends this side's sending");
        assert_eq!(self.end_at_client, END, "the contract: a Recv of zero bytes means the stream ended");
        for shutdowns in &self.closed_shutdowns {
            let closed = settles(shutdowns, shut, Err(Error::NotConnected));
            rule(closed, "a second Shutdown is NotConnected once the connection is closed", shutdowns);
        }
    }
}

/// A peer that sends and closes, nothing unread: its bytes, then the end,
/// and this side's half-close.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GracefulClose {
    pub bytes_intact: bool,
    pub end: Result<Vec<u8>, Error>,
    /// This side's `Shutdown`s, until the connection is closed.
    pub shutdowns: Vec<Result<Done, Error>>,
}

#[must_use]
pub fn graceful_close<B: Backend>(backend: &mut B) -> GracefulClose {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    run.send_all(client, c, b"goodbye");
    run.close(client, c);
    let bytes_intact = run.recv_exact(server, s, 7) == b"goodbye";
    let end = run.recv(server, s, 64);
    let shutdowns = run.shutdown_until_refused(server, s);
    run.close(server, s);
    run.finish();
    GracefulClose { bytes_intact, end, shutdowns }
}

impl Check for GracefulClose {
    fn check(&self) {
        assert!(self.bytes_intact, "the contract: the bytes sent before a Close arrive");
        assert_eq!(self.end, END, "the contract: a Recv of zero bytes means the stream ended");
        let first = self.shutdowns.first().copied();
        assert_eq!(first, Some(NOTHING), "the contract: Shutdown is Ok while the connection lasts");
        let closed = self.shutdowns.len() > 1 && settles(&self.shutdowns, shut, Err(Error::NotConnected));
        rule(closed, "a second Shutdown is NotConnected once the connection is closed", &self.shutdowns);
    }
}

/// `Send`s to a peer that closed with nothing unread.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PeerClosed {
    pub end: Result<Vec<u8>, Error>,
    /// Every `Send`, until one failed.
    pub sends: Vec<Result<Done, Error>>,
    pub recv: Result<Vec<u8>, Error>,
    /// Every `Shutdown`, until one failed.
    pub shutdowns: Vec<Result<Done, Error>>,
}

#[must_use]
pub fn send_after_peer_closed<B: Backend>(backend: &mut B) -> PeerClosed {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    run.close(client, c);
    let end = run.recv(server, s, 64);
    let sends = run.send_until_refused(server, s);
    let recv = run.recv(server, s, 64);
    let shutdowns = run.shutdown_until_refused(server, s);
    run.close(server, s);
    run.finish();
    PeerClosed { end, sends, recv, shutdowns }
}

impl Check for PeerClosed {
    fn check(&self) {
        assert_eq!(self.end, END, "the contract: a Recv of zero bytes means the stream ended");
        peer_gone(&self.sends, &self.recv, &self.shutdowns);
    }
}

/// The rule for a connection whose peer closed with nothing unread: a
/// `Send` may succeed, its bytes lost, until the peer's reset arrives, then
/// fails with `BrokenPipe`, never `Reset`; a `Recv` then gives the end and
/// a `Shutdown` fails with `NotConnected`.
fn peer_gone(sends: &[Result<Done, Error>], recv: &Result<Vec<u8>, Error>, shutdowns: &[Result<Done, Error>]) {
    let broken = settles(sends, sent, Err(Error::BrokenPipe));
    rule(broken, "a Send after the peer closed may succeed until its reset arrives, then fails BrokenPipe", &sends);
    assert_eq!(*recv, END, "the contract: then a Recv gives Ok(Count(0))");
    let closed = settles(shutdowns, shut, Err(Error::NotConnected));
    rule(closed, "then a Shutdown fails with NotConnected", &shutdowns);
}

/// A connect where a socket is bound but nothing listens.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Refused {
    pub connect: Result<Done, Error>,
}

#[must_use]
pub fn refused<B: Backend>(backend: &mut B) -> Refused {
    let mut run = Run::new(backend);
    let process = run.process();
    let holder = run.socket(process, Family::Ipv4);
    let addr = run.bind(process, holder, at(Family::Ipv4, 0)).expect("port 0 binds");
    let client = run.socket(process, Family::Ipv4);
    let connect = run.connect(process, client, addr);
    run.close(process, client);
    run.close(process, holder);
    run.finish();
    Refused { connect }
}

impl Check for Refused {
    fn check(&self) {
        assert_eq!(self.connect, Err(Error::Refused), "the contract: Refused means nothing listened there");
    }
}

/// Where `AddressInUse` comes from (`SO_REUSEADDR`, a backend default): a
/// `Bind` against a listener, a second `Listen` on one address, and none
/// against a connection.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AddressInUse {
    /// A `Bind` of a listener's address, and of the unspecified address on
    /// its port, which overlaps it.
    pub bind_on_listener: [Result<Addr, Error>; 2],
    /// Two sockets bound to one address, then each listening.
    pub shared_binds: [Result<Addr, Error>; 2],
    pub shared_listens: [Result<Done, Error>; 2],
    /// A `Bind` and a `Listen` on the address of a closed listener whose
    /// accepted connection lives on.
    pub rebind: Result<Addr, Error>,
    pub relisten: Result<Done, Error>,
    /// A `Bind` of an address that is not this host's.
    pub foreign: Result<Addr, Error>,
}

#[must_use]
pub fn address_in_use<B: Backend>(backend: &mut B) -> AddressInUse {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let others = [run.socket(server, Family::Ipv4), run.socket(server, Family::Ipv4)];
    let everywhere = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), addr.port());
    let bind_on_listener = [run.bind(server, others[0], addr), run.bind(server, others[1], everywhere)];

    let shared = [run.socket(server, Family::Ipv4), run.socket(server, Family::Ipv4)];
    let first = run.bind(server, shared[0], at(Family::Ipv4, 0));
    let second = match first {
        Ok(bound) => run.bind(server, shared[1], bound),
        Err(error) => Err(error),
    };
    let shared_binds = [first, second];
    let shared_listens = [run.listen(server, shared[0], 16), run.listen(server, shared[1], 16)];

    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    let again = run.socket(server, Family::Ipv4);
    let rebind = run.bind(server, again, addr);
    let relisten = run.listen(server, again, 16);

    let stranger = run.socket(server, Family::Ipv4);
    let foreign = run.bind(server, stranger, SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 0)));

    for fd in [others[0], others[1], shared[0], shared[1], s, again, stranger] {
        run.close(server, fd);
    }
    run.close(client, c);
    run.finish();
    AddressInUse { bind_on_listener, shared_binds, shared_listens, rebind, relisten, foreign }
}

impl Check for AddressInUse {
    fn check(&self) {
        for bind in self.bind_on_listener {
            assert_eq!(bind, Err(Error::AddressInUse), "the contract: a Bind against a listening socket");
        }
        let [first, second] = self.shared_binds;
        rule(first.is_ok() && first == second, "two sockets bound to one address both bind", &self.shared_binds);
        assert_eq!(
            self.shared_listens,
            [NOTHING, Err(Error::AddressInUse)],
            "the contract: two sockets bound to one address, the second Listen fails"
        );
        assert!(self.rebind.is_ok(), "the contract: AddressInUse comes from Bind only against a listening socket");
        assert_eq!(self.relisten, NOTHING, "the contract: only a listener holds an address against a Listen");
        assert_eq!(self.foreign, Err(Error::AddressNotAvailable), "the contract: an address that is not this host's");
    }
}

/// `IPV6_V6ONLY`, a backend default: IPv6 sockets never meet IPv4 ones.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Ipv6Only {
    /// A `Bind`, and a `Connect`, of an IPv6 socket to an IPv4-mapped
    /// address.
    pub mapped_bind: Result<Addr, Error>,
    pub mapped_connect: Result<Done, Error>,
    /// An IPv4 connect to a port where only an IPv6 socket listens.
    pub across: Result<Done, Error>,
    /// An IPv4 socket listening on that port too.
    pub beside: Result<Done, Error>,
}

#[must_use]
pub fn ipv6_only<B: Backend>(backend: &mut B) -> Ipv6Only {
    let mut run = Run::new(backend);
    let process = run.process();
    let mapped = SocketAddr::new(IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()), 0);
    let v6 = run.socket(process, Family::Ipv6);
    let mapped_bind = run.bind(process, v6, mapped);
    run.close(process, v6);

    // The IPv4 port is taken first, so that nothing else listens on it.
    let holder = run.socket(process, Family::Ipv4);
    let port = run.bind(process, holder, at(Family::Ipv4, 0)).expect("port 0 binds").port();
    let (listener, _) = run.listener(process, at(Family::Ipv6, port), 16);
    let v4 = run.socket(process, Family::Ipv4);
    let across = run.connect(process, v4, at(Family::Ipv4, port));
    run.close(process, v4);
    let beside = run.listen(process, holder, 16);

    let v6 = run.socket(process, Family::Ipv6);
    let mapped_connect = run.connect(process, v6, SocketAddr::new(mapped.ip(), port));
    for fd in [v6, holder, listener] {
        run.close(process, fd);
    }
    run.finish();
    Ipv6Only { mapped_bind, mapped_connect, across, beside }
}

impl Check for Ipv6Only {
    fn check(&self) {
        assert_eq!(self.mapped_bind, Err(Error::InvalidArgument), "the contract: an IPv6 socket binds no IPv4 address");
        assert_eq!(
            self.mapped_connect,
            Err(Error::Unreachable),
            "the contract: an IPv6 socket reaches no IPv4 address"
        );
        assert_eq!(self.across, Err(Error::Refused), "the contract: families never mix");
        assert_eq!(self.beside, NOTHING, "the contract: families never mix, nor clash");
    }
}

/// Records the kernel refuses for the socket's state, which the contract
/// answers (those it lists as broken invariants are never submitted).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WrongState {
    /// `Recv`, `Send` and `Accept` on a fresh socket.
    pub fresh: [Result<Done, Error>; 3],
    pub bind_twice: Result<Addr, Error>,
    /// `Recv` and `Send` on a listener.
    pub listener: [Result<Done, Error>; 2],
    /// `Bind`, `Listen` and `Accept` on a connected socket.
    pub connected: [Result<Done, Error>; 3],
}

#[must_use]
pub fn wrong_state<B: Backend>(backend: &mut B) -> WrongState {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let fresh = run.socket(client, Family::Ipv4);
    let fresh_answers = [
        run.call(client, Op::Recv { fd: fresh, buf: Box::from([0; 8]) }).result,
        run.call(client, Op::Send { fd: fresh, bytes: Box::from(*b"x"), from: 0 }).result,
        run.call(client, Op::Accept { fd: fresh }).result,
    ];
    run.bind(client, fresh, at(Family::Ipv4, 0)).expect("port 0 binds");
    let bind_twice = run.bind(client, fresh, at(Family::Ipv4, 0));
    run.close(client, fresh);

    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let listener_answers = [
        run.call(server, Op::Recv { fd: listener, buf: Box::from([0; 8]) }).result,
        run.call(server, Op::Send { fd: listener, bytes: Box::from(*b"x"), from: 0 }).result,
    ];
    let (c, s, _) = run.connection(client, server, listener, addr);
    let connected = [
        run.call(client, Op::Bind { fd: c, addr: at(Family::Ipv4, 0) }).result,
        run.listen(server, s, 16),
        run.call(server, Op::Accept { fd: s }).result,
    ];
    run.close(client, c);
    run.close(server, s);
    run.close(server, listener);
    run.finish();
    WrongState { fresh: fresh_answers, bind_twice, listener: listener_answers, connected }
}

impl Check for WrongState {
    fn check(&self) {
        let [recv, send, accept] = self.fresh;
        assert_eq!(recv, Err(Error::NotConnected), "the contract: a Recv on a socket never connected");
        assert_eq!(send, Err(Error::BrokenPipe), "the contract: a Send on a socket never connected");
        assert_eq!(accept, Err(Error::InvalidArgument), "the contract: an Accept on a socket that does not listen");
        assert_eq!(self.bind_twice, Err(Error::InvalidArgument), "the contract: a second Bind");
        let [recv, send] = self.listener;
        assert_eq!(recv, Err(Error::NotConnected), "the contract: a Recv on a listener, never connected");
        assert_eq!(send, Err(Error::BrokenPipe), "the contract: a Send on a listener, never connected");
        for answer in self.connected {
            assert_eq!(answer, Err(Error::InvalidArgument), "the contract: Bind, Listen, Accept on a connected socket");
        }
    }
}

/// Connects to a listener whose queue holds one: some wait for room.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FullQueue {
    /// The connects that completed before any accept, and those that did
    /// not, each with its answer once it came.
    pub at_once: Vec<Result<Done, Error>>,
    pub delayed: Vec<Result<Done, Error>>,
}

#[must_use]
pub fn full_accept_queue<B: Backend>(backend: &mut B) -> FullQueue {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 1);
    let mut sockets = Vec::new();
    let mut connects = Vec::new();
    for _ in 0..CLIENTS {
        let fd = run.socket(client, Family::Ipv4);
        sockets.push(fd);
        connects.push(run.start(client, Op::Connect { fd, addr }));
    }
    let mut at_once = Vec::new();
    let mut waiting = Vec::new();
    for connect in connects {
        match run.within(client, connect, BRIEFLY) {
            Some(complete) => at_once.push(complete.result),
            None => waiting.push(connect),
        }
    }
    let mut accepted = Vec::new();
    for _ in 0..CLIENTS {
        accepted.push(run.accept(server, listener).0);
    }
    let mut delayed = Vec::new();
    for connect in waiting {
        delayed.push(run.wait(client, connect).result);
    }
    for fd in sockets {
        run.close(client, fd);
    }
    for fd in &accepted {
        run.close(server, *fd);
    }
    run.close(server, listener);
    run.finish();
    FullQueue { at_once, delayed }
}

impl Check for FullQueue {
    fn check(&self) {
        rule(!self.at_once.is_empty(), "at least one connection can always wait", self);
        rule(!self.delayed.is_empty(), "a full accept queue delays a Connect", self);
        for answer in self.at_once.iter().chain(&self.delayed) {
            assert_eq!(*answer, NOTHING, "the contract: a full accept queue never refuses a Connect");
        }
    }
}

/// A `Close` with received bytes unread: the peer's next operation fails
/// with `Reset`, and the connection is then dead.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UnreadClose {
    /// The bytes the peer had received before the reset, read after it.
    pub bytes_intact: bool,
    /// The operations until one met the reset: a `Recv` after those bytes,
    /// or `Send`s, which may succeed until the reset arrives.
    pub reset: Vec<Result<Done, Error>>,
    /// After it: a `Recv`, a `Send` and a `Shutdown`.
    pub after: [Result<Done, Error>; 3],
}

/// The peer reads the bytes it had, then meets the reset with a `Recv`.
#[must_use]
pub fn unread_close_meets_recv<B: Backend>(backend: &mut B) -> UnreadClose {
    unread_close(backend, false)
}

/// The peer meets the reset with a `Send`, then reads the bytes it had.
#[must_use]
pub fn unread_close_meets_send<B: Backend>(backend: &mut B) -> UnreadClose {
    unread_close(backend, true)
}

fn unread_close<B: Backend>(backend: &mut B, send_first: bool) -> UnreadClose {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    let before = pattern(16, 3);
    run.send_all(server, s, &before);
    run.send_all(client, c, b"unread");
    run.close(server, s);
    let (bytes_intact, reset) = if send_first {
        let reset = run.send_until_refused(client, c);
        (run.recv_exact(client, c, before.len()) == before, reset)
    } else {
        let intact = run.recv_exact(client, c, before.len()) == before;
        let reset = run.call(client, Op::Recv { fd: c, buf: Box::from([0; 64]) }).result;
        (intact, vec![reset])
    };
    let after = [
        run.call(client, Op::Recv { fd: c, buf: Box::from([0; 64]) }).result,
        run.send(client, c, b"x"),
        run.shutdown(client, c),
    ];
    run.close(client, c);
    run.finish();
    UnreadClose { bytes_intact, reset, after }
}

impl Check for UnreadClose {
    fn check(&self) {
        assert!(self.bytes_intact, "the contract: the bytes already received stay readable");
        let reset = settles(&self.reset, sent, Err(Error::Reset));
        rule(reset, "a Close with received data unread resets the peer: its next operation fails", &self.reset);
        assert_eq!(
            self.after,
            [Ok(Done::Count(0)), Err(Error::BrokenPipe), Err(Error::NotConnected)],
            "the contract: after a reset, Recv gives 0, Send fails BrokenPipe, Shutdown NotConnected"
        );
    }
}

/// A reset reaching an end that already received the peer's end of
/// stream: no reset is reported.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ResetAfterEnd {
    pub end: Result<Vec<u8>, Error>,
    /// After the peer closed with bytes unread: two `Recv`s, then `Send`s
    /// and `Shutdown`s, each until one failed.
    pub recvs: [Result<Done, Error>; 2],
    pub sends: Vec<Result<Done, Error>>,
    pub shutdowns: Vec<Result<Done, Error>>,
}

#[must_use]
pub fn reset_after_end_of_stream<B: Backend>(backend: &mut B) -> ResetAfterEnd {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    run.send_all(client, c, b"unread");
    assert_eq!(run.shutdown(server, s), NOTHING, "a connected socket shuts down");
    let end = run.recv(client, c, 64);
    run.close(server, s);
    let recvs = [
        run.call(client, Op::Recv { fd: c, buf: Box::from([0; 64]) }).result,
        run.call(client, Op::Recv { fd: c, buf: Box::from([0; 64]) }).result,
    ];
    let sends = run.send_until_refused(client, c);
    let shutdowns = run.shutdown_until_refused(client, c);
    run.close(client, c);
    run.finish();
    ResetAfterEnd { end, recvs, sends, shutdowns }
}

impl Check for ResetAfterEnd {
    fn check(&self) {
        assert_eq!(self.end, END, "the contract: a Recv of zero bytes means the stream ended");
        let rule_ = "the contract: an end that already received the peer's end of stream hears of no reset";
        assert_eq!(self.recvs, [Ok(Done::Count(0)), Ok(Done::Count(0))], "{rule_}");
        rule(settles(&self.sends, sent, Err(Error::BrokenPipe)), rule_, &self.sends);
        rule(settles(&self.shutdowns, shut, Err(Error::NotConnected)), rule_, &self.shutdowns);
    }
}

/// A connection whose client sent and closed before it was accepted.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClosedBeforeAccept {
    pub accepted: bool,
    pub bytes_intact: bool,
    pub end: Result<Vec<u8>, Error>,
    /// Every `Send`, then every `Shutdown`, each until one failed, and a
    /// `Recv` between them.
    pub sends: Vec<Result<Done, Error>>,
    pub recv: Result<Vec<u8>, Error>,
    pub shutdowns: Vec<Result<Done, Error>>,
}

#[must_use]
pub fn closed_before_accept<B: Backend>(backend: &mut B) -> ClosedBeforeAccept {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let fd = run.socket(client, Family::Ipv4);
    assert_eq!(run.connect(client, fd, addr), NOTHING, "a connect to a listener");
    run.send_all(client, fd, b"hi");
    run.close(client, fd);
    let (s, peer) = run.accept(server, listener);
    let accepted = peer.ip() == addr.ip();
    let bytes_intact = run.recv_exact(server, s, 2) == b"hi";
    let end = run.recv(server, s, 64);
    let sends = run.send_until_refused(server, s);
    let recv = run.recv(server, s, 64);
    let shutdowns = run.shutdown_until_refused(server, s);
    run.close(server, s);
    run.close(server, listener);
    run.finish();
    ClosedBeforeAccept { accepted, bytes_intact, end, sends, recv, shutdowns }
}

impl Check for ClosedBeforeAccept {
    fn check(&self) {
        assert!(self.accepted, "the contract: a connection closed while it waits is still accepted");
        assert!(self.bytes_intact, "the contract: the bytes sent before the Close arrive");
        assert_eq!(self.end, END, "the contract: then the stream ends");
        peer_gone(&self.sends, &self.recv, &self.shutdowns);
    }
}

/// A sender to a peer that does not read stops completing; reading
/// resumes it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Backpressure {
    /// The bytes accepted before a `Send` stalled, if one did.
    pub stalled_after: Option<u64>,
    /// The stalled `Send`'s answer, once the peer read.
    pub resumed: Option<Result<Done, Error>>,
}

#[must_use]
pub fn backpressure<B: Backend>(backend: &mut B) -> Backpressure {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    let chunk: Box<[u8]> = pattern(CHUNK, 4).into_boxed_slice();
    let mut total = 0_u64;
    let mut from = 0_u32;
    let mut stalled = None;
    while total < MOST {
        let send = run.start(client, Op::send(c, chunk.clone(), from).expect("bytes left in the chunk"));
        let Some(complete) = run.within(client, send, BRIEFLY) else {
            stalled = Some(send);
            break;
        };
        let Ok(Done::Count(n)) = complete.result else {
            unexpected("a Send on a healthy connection", &complete.result);
        };
        total = total.checked_add(u64::from(n)).expect("a count of bytes");
        from = from.checked_add(n).expect("no more than were left");
        if usize::try_from(from).expect("a u32 fits a usize") == CHUNK {
            from = 0;
        }
    }
    let Some(send) = stalled else {
        run.close(client, c);
        drain(&mut run, server, s);
        run.close(server, s);
        run.finish();
        return Backpressure { stalled_after: None, resumed: None };
    };
    // Read until the stalled send completes, a receive in flight beside it.
    let mut recv = run.start(server, Op::recv(s, vec![0; READ].into_boxed_slice()).expect("room"));
    let resumed = loop {
        let complete = run.either((client, send), (server, recv));
        if complete.op == send {
            break complete.result;
        }
        let bytes = received(complete);
        rule(bytes.as_ref().is_ok_and(|bytes| !bytes.is_empty()), "the bytes accepted are received", &bytes);
        recv = run.start(server, Op::recv(s, vec![0; READ].into_boxed_slice()).expect("room"));
    };
    run.close(client, c);
    let last = run.wait(server, recv);
    if received(last).is_ok_and(|bytes| !bytes.is_empty()) {
        drain(&mut run, server, s);
    }
    run.close(server, s);
    run.finish();
    Backpressure { stalled_after: Some(total), resumed: Some(resumed) }
}

/// Receives on `fd` until its stream ends.
fn drain<B: Backend>(run: &mut Run<'_, B>, process: B::Process, fd: Fd) {
    while run.recv(process, fd, READ).is_ok_and(|bytes| !bytes.is_empty()) {}
}

impl Check for Backpressure {
    fn check(&self) {
        rule(self.stalled_after.is_some(), "a sender to a peer that does not read stops completing sends", self);
        rule(matches!(self.resumed, Some(Ok(Done::Count(_)))), "reading resumes a stalled send", self);
    }
}

/// The operation a `Cancel` aims at, which says what its own result is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Target {
    Accept,
    Recv,
    Connect,
}

/// When a racing target's `Cancel` is submitted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Race {
    /// Before what the target waits for arrives.
    CancelFirst,
    /// After it arrived, while the target's process was away: the target
    /// is not decided yet, and a `Cancel` can still stop it.
    ArrivedAway,
    /// After it arrived and the target's process entered the kernel: the
    /// target completed, and the `Cancel` is too late.
    ArrivedEntered,
}

/// How a `Cancel` and its target paired, by the last `Cancel`'s answer.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Pairing {
    /// `Ok`: the target `Cancelled`.
    Stopped,
    /// `TooLate`: the target `Cancelled`, interrupted by the kernel.
    Interrupted,
    /// `TooLate`: the target's own result.
    Completed,
    /// Not submitted: the target ran on to its own result.
    RanOn,
}

/// A `Cancel` of a waiting operation: what each `Cancel` answered, and how
/// its target completed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Cancelling {
    pub of: Target,
    /// Every `Cancel`'s answer, in order: one more after each the backend
    /// could not submit while the target ran on.
    pub cancels: Vec<Result<Done, Error>>,
    pub target: Result<Done, Error>,
    /// Whether the target could complete by itself while it was cancelled.
    /// One that could not completes `Cancelled`.
    pub could_complete: bool,
    /// Whether the target had completed when the `Cancel` was submitted.
    pub decided_first: bool,
    /// What arrived for the target (bytes for a `Recv`, a connection for an
    /// `Accept`) was taken once, by the target or by the next operation.
    pub taken_once: bool,
}

impl Cancelling {
    /// How the last `Cancel` and the target paired, which [`Check`] holds
    /// to the contract.
    #[must_use]
    pub fn pairing(&self) -> Pairing {
        match (self.cancels.last(), self.target) {
            (Some(Ok(Done::Nothing)), _) => Pairing::Stopped,
            (Some(Err(Error::TooLate)), Err(Error::Cancelled)) => Pairing::Interrupted,
            (Some(Err(Error::TooLate)), _) => Pairing::Completed,
            (Some(Err(Error::InvalidArgument | Error::Other(_))), _) => Pairing::RanOn,
            (other, _) => unexpected("a Cancel answers Ok, TooLate, or that it was not submitted", &other),
        }
    }

    /// Whether the target completed with its own result: what its
    /// operation does when nothing stops it.
    fn own(&self) -> bool {
        match (self.of, self.target) {
            (Target::Recv, Ok(Done::Count(n))) => n > 0,
            (Target::Accept, Ok(Done::Accepted { .. })) | (Target::Connect, Ok(Done::Nothing)) => true,
            (Target::Recv | Target::Accept | Target::Connect, _) => false,
        }
    }
}

/// An `Accept` with nothing connecting.
#[must_use]
pub fn cancel_accept<B: Backend>(backend: &mut B) -> Cancelling {
    let mut run = Run::new(backend);
    let server = run.process();
    let (listener, _) = run.listener(server, at(Family::Ipv4, 0), 16);
    let accept = run.start(server, Op::Accept { fd: listener });
    let (cancels, target) = run.cancel(server, accept);
    run.close(server, listener);
    run.finish();
    let target = target.result;
    Cancelling { of: Target::Accept, cancels, target, could_complete: false, decided_first: false, taken_once: true }
}

/// A `Recv` with nothing sent; bytes sent after it go to the next.
#[must_use]
pub fn cancel_recv<B: Backend>(backend: &mut B) -> Cancelling {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    let recv = run.start(server, Op::recv(s, vec![0; 16].into_boxed_slice()).expect("room"));
    let (cancels, target) = run.cancel(server, recv);
    run.send_all(client, c, b"after");
    let taken_once = run.recv_exact(server, s, 5) == b"after";
    run.close(client, c);
    run.close(server, s);
    run.finish();
    let target = target.result;
    Cancelling { of: Target::Recv, cancels, target, could_complete: false, decided_first: false, taken_once }
}

/// A `Recv` cancelled as its bytes arrive, as `race` says. Either way they
/// are received once, by the target or after it.
#[must_use]
pub fn cancel_recv_racing_bytes<B: Backend>(backend: &mut B, race: Race) -> Cancelling {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let (c, s, _) = run.connection(client, server, listener, addr);
    run.close(server, listener);
    let sent = pattern(16, 5);
    let recv = run.start(server, Op::recv(s, vec![0; 64].into_boxed_slice()).expect("room"));
    if race == Race::CancelFirst {
        let cancel = run.start(server, Op::Cancel { target: recv });
        run.send_all(client, c, &sent);
        return finish_racing_recv(run, (client, c), (server, s), (recv, cancel), &sent, race);
    }
    run.send_all(client, c, &sent);
    if race == Race::ArrivedEntered {
        run.enter(server);
    }
    let cancel = run.start(server, Op::Cancel { target: recv });
    finish_racing_recv(run, (client, c), (server, s), (recv, cancel), &sent, race)
}

fn finish_racing_recv<B: Backend>(
    mut run: Run<'_, B>,
    (client, c): (B::Process, Fd),
    (server, s): (B::Process, Fd),
    (recv, cancel): (Token, Token),
    sent: &[u8],
    race: Race,
) -> Cancelling {
    let (cancels, target) = run.settle_cancel(server, recv, cancel);
    let result = target.result;
    let mut got = received(target).unwrap_or_default();
    let left = sent.len().checked_sub(got.len()).expect("no more than was sent");
    got.extend(run.recv_exact(server, s, left));
    run.close(client, c);
    run.close(server, s);
    run.finish();
    Cancelling {
        of: Target::Recv,
        cancels,
        target: result,
        could_complete: true,
        decided_first: race == Race::ArrivedEntered,
        taken_once: got == sent,
    }
}

/// An `Accept` cancelled as a connection arrives, as `race` says. Either
/// way the connection is accepted once, by the target or by the next
/// `Accept`.
#[must_use]
pub fn cancel_accept_racing_a_connect<B: Backend>(backend: &mut B, race: Race) -> Cancelling {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let fd = run.socket(client, Family::Ipv4);
    let accept = run.start(server, Op::Accept { fd: listener });
    let cancel = if race == Race::CancelFirst {
        let cancel = run.start(server, Op::Cancel { target: accept });
        assert_eq!(run.connect(client, fd, addr), NOTHING, "a connect to a listener");
        cancel
    } else {
        assert_eq!(run.connect(client, fd, addr), NOTHING, "a connect to a listener");
        if race == Race::ArrivedEntered {
            run.enter(server);
        }
        run.start(server, Op::Cancel { target: accept })
    };
    let (cancels, target) = run.settle_cancel(server, accept, cancel);
    let accepted = match target.result {
        Ok(Done::Accepted { fd, .. }) => fd,
        Ok(_) | Err(_) => run.accept(server, listener).0,
    };
    run.send_all(client, fd, b"once");
    let taken_once = run.recv_exact(server, accepted, 4) == b"once";
    for (process, fd) in [(client, fd), (server, accepted), (server, listener)] {
        run.close(process, fd);
    }
    run.finish();
    Cancelling {
        of: Target::Accept,
        cancels,
        target: target.result,
        could_complete: true,
        decided_first: race == Race::ArrivedEntered,
        taken_once,
    }
}

/// Connects to a listener whose queue holds one until a `Connect` waits for
/// room: every socket, and the waiting `Connect`, on the last.
fn fill_queue<B: Backend>(run: &mut Run<'_, B>, client: B::Process, addr: Addr) -> (Vec<Fd>, Token) {
    let mut sockets = Vec::new();
    for _ in 0..CLIENTS {
        let fd = run.socket(client, Family::Ipv4);
        sockets.push(fd);
        let connect = run.start(client, Op::Connect { fd, addr });
        match run.within(client, connect, BRIEFLY) {
            Some(complete) => assert_eq!(complete.result, NOTHING, "a connect to a listener with room"),
            None => return (sockets, connect),
        }
    }
    unexpected("a full accept queue delays a connect", &sockets)
}

/// A `Connect` waiting for room in a full accept queue.
#[must_use]
pub fn cancel_connect<B: Backend>(backend: &mut B) -> Cancelling {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 1);
    let (sockets, connect) = fill_queue(&mut run, client, addr);
    let (cancels, target) = run.cancel(client, connect);
    // The connections that were made wait in the queue: closing the
    // listener resets them, and their clients only close.
    for fd in sockets {
        run.close(client, fd);
    }
    run.close(server, listener);
    run.finish();
    let target = target.result;
    Cancelling { of: Target::Connect, cancels, target, could_complete: true, decided_first: false, taken_once: true }
}

impl Check for Cancelling {
    fn check(&self) {
        let Some((_, earlier)) = self.cancels.split_last() else {
            unexpected("a Cancel submitted", self);
        };
        for answer in earlier {
            let unsubmitted = matches!(answer, Err(Error::InvalidArgument | Error::Other(_)));
            rule(unsubmitted, "a Cancel is asked again only after one the backend could not submit", self);
        }
        match self.pairing() {
            Pairing::Stopped => {
                assert_eq!(self.target, Err(Error::Cancelled), "the contract: a Cancel that stopped its target");
            }
            Pairing::Interrupted => {}
            Pairing::Completed => {
                rule(self.own(), "a Cancel too late: the target completes with what its operation does", self);
            }
            Pairing::RanOn => rule(self.own(), "a Cancel not submitted leaves its target running", self),
        }
        if !self.could_complete {
            assert_eq!(self.target, Err(Error::Cancelled), "the contract: a target with nothing to do is stopped");
        }
        if self.decided_first {
            let late = matches!(self.pairing(), Pairing::Completed | Pairing::RanOn) && self.own();
            rule(late, "a Cancel of a target that completed is too late, the target saying what it did", self);
        }
        assert!(self.taken_once, "the contract: a stopped Recv or Accept took nothing; what arrived is taken once");
    }
}

/// A `Connect` the network established while its client was away (the
/// SYN, retransmitted once an accept made room in the full queue),
/// cancelled before the client entered: the `Cancel` may stop it, yet the
/// connection reached the server, which accepts it and sees it end when the
/// client closes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StoppedConnect {
    pub cancelling: Cancelling,
    /// The server accepted every connection made, the cancelled one among
    /// them.
    pub reached: bool,
    /// Each accepted connection's `Recv` once the client closed every
    /// socket.
    pub ends: Vec<Result<Vec<u8>, Error>>,
}

#[must_use]
pub fn cancel_connect_established_while_away<B: Backend>(backend: &mut B) -> StoppedConnect {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 1);
    let (sockets, connect) = fill_queue(&mut run, client, addr);
    let mut accepted = vec![run.accept(server, listener).0];
    run.sleep(SYN_RETRY);
    let (cancels, target) = run.cancel(client, connect);
    while accepted.len() < sockets.len() {
        let accept = run.start(server, Op::Accept { fd: listener });
        let complete = match run.within(server, accept, BRIEFLY) {
            Some(complete) => complete,
            None => run.cancel(server, accept).1,
        };
        let Ok(Done::Accepted { fd, .. }) = complete.result else {
            break;
        };
        accepted.push(fd);
    }
    let reached = accepted.len() == sockets.len();
    for fd in sockets {
        run.close(client, fd);
    }
    let mut ends = Vec::new();
    for fd in &accepted {
        ends.push(run.recv(server, *fd, 64));
    }
    for fd in accepted {
        run.close(server, fd);
    }
    run.close(server, listener);
    run.finish();
    let target = target.result;
    let cancelling = Cancelling {
        of: Target::Connect,
        cancels,
        target,
        could_complete: true,
        decided_first: false,
        taken_once: true,
    };
    StoppedConnect { cancelling, reached, ends }
}

impl Check for StoppedConnect {
    fn check(&self) {
        self.cancelling.check();
        rule(self.reached, "a Connect established before its Cancel reached its peer, whatever the Cancel said", self);
        for end in &self.ends {
            assert_eq!(*end, END, "the contract: the peer sees the connection end when the client closes it");
        }
    }
}

/// A connection waiting in a listener's queue when the listener closes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ListenerClosed {
    /// The client's `Recv`s after the listener's `Close`.
    pub recvs: [Result<Done, Error>; 2],
}

#[must_use]
pub fn listener_close_resets_waiting<B: Backend>(backend: &mut B) -> ListenerClosed {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let fd = run.socket(client, Family::Ipv4);
    assert_eq!(run.connect(client, fd, addr), NOTHING, "a connect to a listener");
    run.close(server, listener);
    let recvs = [
        run.call(client, Op::Recv { fd, buf: Box::from([0; 8]) }).result,
        run.call(client, Op::Recv { fd, buf: Box::from([0; 8]) }).result,
    ];
    run.close(client, fd);
    run.finish();
    ListenerClosed { recvs }
}

impl Check for ListenerClosed {
    fn check(&self) {
        assert_eq!(
            self.recvs,
            [Err(Error::Reset), Ok(Done::Count(0))],
            "the contract: closing a listener resets the connections waiting on it"
        );
    }
}

/// Past the descriptor limit, which only the simulator can set low: a
/// failed `Accept` consumes no waiting connection. Needs a backend whose
/// processes hold fewer than 64 descriptors.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DescriptorLimit {
    pub socket: Result<Done, Error>,
    pub accept: Result<Done, Error>,
    /// The waiting connection, accepted once a descriptor was free, carries
    /// bytes.
    pub then_accepted: bool,
}

#[must_use]
pub fn accept_past_the_descriptor_limit<B: Backend>(backend: &mut B) -> DescriptorLimit {
    let mut run = Run::new(backend);
    let (client, server) = (run.process(), run.process());
    let (listener, addr) = run.listener(server, at(Family::Ipv4, 0), 16);
    let fd = run.socket(client, Family::Ipv4);
    assert_eq!(run.connect(client, fd, addr), NOTHING, "a connect to a listener");
    let mut spares = Vec::new();
    let mut socket = NOTHING;
    for _ in 0..DESCRIPTORS {
        socket = run.call(server, Op::Socket { family: Family::Ipv4 }).result;
        let Ok(Done::Fd(spare)) = socket else {
            break;
        };
        spares.push(spare);
    }
    let accept = run.call(server, Op::Accept { fd: listener }).result;
    if let Some(spare) = spares.pop() {
        run.close(server, spare);
    }
    let (s, _) = run.accept(server, listener);
    run.send_all(client, fd, b"hi");
    let then_accepted = run.recv_exact(server, s, 2) == b"hi";
    for spare in spares {
        run.close(server, spare);
    }
    for (process, fd) in [(client, fd), (server, s), (server, listener)] {
        run.close(process, fd);
    }
    run.finish();
    DescriptorLimit { socket, accept, then_accepted }
}

impl Check for DescriptorLimit {
    fn check(&self) {
        assert_eq!(self.socket, Err(Error::TooManyOpenFiles), "the contract: a Socket past the descriptor limit");
        assert_eq!(self.accept, Err(Error::TooManyOpenFiles), "the contract: an Accept past the descriptor limit");
        assert!(self.then_accepted, "the contract: a failed Accept consumed no waiting connection");
    }
}
