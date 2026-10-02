//! The kernel boundary (overview.md, section 6): the records io submits to a
//! backend and the completions the backend hands back. The ring adapter, the
//! simulator and any later backend implement exactly this, and the
//! conformance suite (overview.md, section 9) holds each of them to it.
//!
//! # The contract
//!
//! Records:
//!
//! - **Every submission completes exactly once,** cancelled or not, with the
//!   [`Submit`]'s `op` token on its [`Complete`].
//! - **The completion hands back the operation:** [`Complete::kind`] is the
//!   submitted [`Op`], every buffer inside it, whatever the result. The
//!   backend never drops, copies or replaces a `Box` (programming-style.md,
//!   5.3). A `Recv` comes back with `buf[..n]` filled, a `Send` with its
//!   `bytes` untouched.
//! - **One success shape per operation** ([`Shape`], tabled on [`Op`]). An
//!   error means the operation did nothing usable: a failed `Socket` or
//!   `Accept` made no descriptor.
//! - **Single-shot only:** one submission, one completion. Every `Cancel`
//!   takes an operation slot of its own, so a completion queue sized from
//!   io's operation slab cannot overflow.
//! - **Completions arrive in any order,** even on one descriptor.
//! - **Plain values only cross.** Kernel structures stay in the backend's
//!   in-flight table; error numbers map onto [`Error`], per operation.
//!
//! Cancelling:
//!
//! - **A cancelled operation completes** as `Err(Cancelled)`, or with what it
//!   did before the cancel landed. io keeps the entity *settling* until then
//!   (programming-style.md, 4.3).
//! - **A `Cancel` completes on its own,** before or after its target (the
//!   simulator randomises which): `Ok(Nothing)` if it found the target in
//!   flight, `Err(TooLate)` if the target had completed or could no longer be
//!   stopped, `Err(InvalidArgument)` or `Err(Other)` if the backend could not
//!   submit it, the target then not stopped. The target's own completion says
//!   what it did.
//!
//! Sockets:
//!
//! - **`Bind` answers with the address bound** ([`Done::Bound`]), port 0
//!   resolved; the backend reads it with `getsockname`. `AddressInUse` comes
//!   from `Bind` only against a listening socket or one bound without
//!   `SO_REUSEADDR`; two sockets bound to one address both bind, and the
//!   second `Listen` fails.
//! - **`Listen`'s backlog is a hint** the backend may clamp; at least one
//!   connection can always wait. A full accept queue delays a `Connect` and
//!   never refuses it: `Refused` means nothing listens there.
//! - **A failed `Accept`** (`TooManyOpenFiles`, `NoBufferSpace`) consumed no
//!   waiting connection. A connection reset while waiting is still accepted,
//!   and its first `Recv` or `Send` fails with `Reset`.
//! - **A `Recv` of zero bytes means the stream ended:** the peer shut down or
//!   closed, or a reset was already reported. It does not prove a graceful
//!   close. A `Recv` buffer is never empty ([`Op::recv`]).
//! - **A `Send`'s count is bytes the kernel accepted** past `from`, at least
//!   one and no more than were left. io continues a short send from
//!   `from + n` (overview.md, 5.1).
//! - **`Shutdown` ends this side's sending:** it completes `Ok` once the end
//!   is queued, behind the bytes of every completed `Send`. A second
//!   `Shutdown` is `Ok` while the connection lasts and `NotConnected` once it
//!   is closed. `Recv` keeps working after it.
//! - **A reset is reported to exactly one operation,** as `Reset`. After it,
//!   a `Recv` gives `Ok(Count(0))`, a `Send` fails with `BrokenPipe` and a
//!   `Shutdown` with `NotConnected`.
//! - **An [`Fd`] is closed only by `Close`,** which releases it whatever its
//!   result. A `Close` with received data unread makes the peer see `Reset`;
//!   closing a listener resets the connections waiting on it.
//!
//! Broken invariants, which io never commits and backends may assume never
//! happen. The simulator fails the world on each; the ring asserts the first
//! two at submit, and is otherwise unspecified:
//!
//! - a record that is not [`Op::is_valid`];
//! - a token already in flight;
//! - a `Cancel` whose target is a `Cancel`;
//! - a `Bind` or `Connect` address of another family than its socket's;
//! - per descriptor, more than one `Recv`, one `Send` or one `Accept` in
//!   flight, or anything in flight beside a `Connect`;
//! - after a `Connect` that failed or was cancelled, any operation on its
//!   descriptor but `Close`;
//! - a `Shutdown` while a `Send` on its descriptor is in flight;
//! - a `Close` while any other operation on its descriptor is in flight, or
//!   any operation on a descriptor after its `Close`.
//!
//! Backend defaults, not records, until a service pulls one: every
//! descriptor is close-on-exec; a socket that binds gets `SO_REUSEADDR`;
//! every IPv6 socket gets `IPV6_V6ONLY`, so families never mix; connected
//! and accepted sockets get `TCP_NODELAY`; a `Send` never raises `SIGPIPE`
//! (`MSG_NOSIGNAL`).

use alloc::boxed::Box;
use core::net::SocketAddr;

use skein_lib::Token;

/// A descriptor, in whatever backend issued it: a file descriptor on the
/// ring, a table slot in the simulator. Only io and its backend ever see one,
/// and it is closed only by [`Op::Close`]. The simulator never reuses a
/// descriptor number, so an operation on a closed one is caught.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Fd(i32);

impl Fd {
    /// Made by a backend, for a descriptor it issued.
    #[must_use]
    pub const fn new(raw: i32) -> Fd {
        Fd(raw)
    }

    #[must_use]
    pub const fn raw(self) -> i32 {
        self.0
    }
}

/// An address and port, IPv4 or IPv6.
///
/// `core::net`'s, which is plain data (no allocation, no lookup, no
/// formatting needed) and passes the subset's lints. Names are resolved
/// before io (overview.md, 10.5).
pub type Addr = SocketAddr;

/// The address family of a socket.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Family {
    Ipv4,
    Ipv6,
}

impl Family {
    /// The family of a socket that binds or connects to `addr`.
    #[must_use]
    pub const fn of(addr: &Addr) -> Family {
        match addr {
            SocketAddr::V4(_) => Family::Ipv4,
            SocketAddr::V6(_) => Family::Ipv6,
        }
    }
}

/// An operation, submitted to the backend with the token io names it by.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Submit {
    pub op: Token,
    pub kind: Op,
}

/// An operation's completion: the token it was submitted with, the operation
/// itself handed back with its buffers, and what came of it.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Complete {
    pub op: Token,
    pub kind: Op,
    pub result: Result<Done, Error>,
}

/// What io asks the kernel to do. Each operation's success value is one
/// [`Shape`] of [`Done`]:
///
/// | Op | `Ok` |
/// |---|---|
/// | `Socket` | `Done::Fd`, the new socket |
/// | `Bind` | `Done::Bound`, the address bound, its port resolved if it was 0 |
/// | `Listen`, `Connect`, `Shutdown`, `Close` | `Done::Nothing` |
/// | `Accept` | `Done::Accepted`, the new socket and its peer |
/// | `Recv` | `Done::Count`, bytes received into `buf[..n]`, 0 at the end |
/// | `Send` | `Done::Count`, bytes sent from `from`, at least 1 |
/// | `Cancel` | `Done::Nothing`, the target was found in flight |
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Op {
    // Sockets.
    /// A new TCP socket of `family`.
    Socket {
        family: Family,
    },
    Bind {
        fd: Fd,
        addr: Addr,
    },
    /// The backlog is a hint, which the backend may clamp to its kernel's; at
    /// least one connection can always wait.
    Listen {
        fd: Fd,
        backlog: u32,
    },
    /// The next connection waiting on a listener.
    Accept {
        fd: Fd,
    },
    Connect {
        fd: Fd,
        addr: Addr,
    },
    /// Up to `buf.len()` bytes, into `buf`, which is never empty.
    Recv {
        fd: Fd,
        buf: Box<[u8]>,
    },
    /// Some of `bytes[from..]`, which is never empty.
    Send {
        fd: Fd,
        bytes: Box<[u8]>,
        from: u32,
    },
    /// Ends the sending direction: the peer's receives then reach the end.
    Shutdown {
        fd: Fd,
    },
    Close {
        fd: Fd,
    },
    // Files beneath a root, processes and the synchronous operations
    // (overview.md, sections 5.2, 5.3 and 6) go here, when a user pulls them.
    /// Asks the operation named `target` to stop early.
    Cancel {
        target: Token,
    },
}

/// The success values, one [`Shape`] per operation (see [`Op`]).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Done {
    /// Nothing more than that it happened.
    Nothing,
    /// Bytes received into the start of `buf`, or sent from `from`.
    Count(u32),
    /// A new socket.
    Fd(Fd),
    /// A new socket, accepted from `peer`.
    Accepted { fd: Fd, peer: Addr },
    /// The address a socket was bound to, with the port the kernel chose when
    /// the `Bind` asked for port 0.
    Bound(Addr),
}

/// The kinds of [`Done`], by which an operation's success value is checked
/// against its operation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Shape {
    Nothing,
    Count,
    Fd,
    Accepted,
    Bound,
}

/// The errors io handles by name. Each backend maps its kernel's error numbers
/// onto these, per operation (the Linux names are given for the ring
/// adapter), and anything else onto `Other`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Error {
    /// `Connect`: nothing listens at the address (`ECONNREFUSED`). A full
    /// accept queue delays a `Connect`, never refuses it.
    Refused,
    /// `Recv`, `Send`, `Connect`: the peer reset the connection
    /// (`ECONNRESET`), reported to one operation only. `Accept`:
    /// `ECONNABORTED`, not seen on Linux, where a connection reset while it
    /// waits is still accepted and its first `Recv` or `Send` fails with this.
    Reset,
    /// `Send`: the connection can send no more, after a reset was reported or
    /// after this side's own `Shutdown` (`EPIPE`).
    BrokenPipe,
    /// `Shutdown`: the socket is not connected: after a reset or a timeout was
    /// reported, or after both sides closed (`ENOTCONN`).
    NotConnected,
    /// `Bind`: a listening socket holds the address, or a socket bound without
    /// `SO_REUSEADDR`. `Listen`: another socket listens on it; two sockets
    /// bound to one address both bind, and the second `Listen` fails
    /// (`EADDRINUSE`).
    AddressInUse,
    /// `Bind`: the address is not this host's. `Connect`: no local port is
    /// free (`EADDRNOTAVAIL`).
    AddressNotAvailable,
    /// `Connect`, `Recv`, `Send`: no route to the network or the host
    /// (`ENETUNREACH`, `EHOSTUNREACH`).
    Unreachable,
    /// `Connect`, `Recv`, `Send`: the kernel's own timeout, such as a connect
    /// that was never answered or retransmissions that went unacknowledged
    /// (`ETIMEDOUT`). io's own deadlines are not this: they end in a `Cancel`.
    TimedOut,
    /// `Socket`, `Accept`: the process or the system has no descriptor left
    /// (`EMFILE`, `ENFILE`). A failed `Accept` consumed no waiting connection.
    TooManyOpenFiles,
    /// Any operation but `Cancel`: the kernel is out of memory for buffers or
    /// sockets (`ENOBUFS`, `ENOMEM`). A failed `Accept` consumed no waiting
    /// connection.
    NoBufferSpace,
    /// Any operation but `Cancel`: a `Cancel` stopped it before it did
    /// anything (`ECANCELED`, and `EINTR` on an operation io cancelled).
    Cancelled,
    /// `Cancel` only: the target had already completed, or was too far along
    /// to stop (`ENOENT`, `EALREADY`; on any other operation those are
    /// `Other`). Its own completion says what it did.
    TooLate,
    /// Any operation: the record was one the kernel refuses, such as `Listen`
    /// on a connected socket (`EINVAL`, `EAFNOSUPPORT`). A bug in io, reported
    /// rather than asserted, since the kernel judged it. On a `Cancel`, the
    /// backend could not submit it, and the target was not stopped.
    InvalidArgument,
    /// Anything else, with the backend's own code, for diagnostics only. On a
    /// `Cancel`, the backend could not submit it, and the target was not
    /// stopped.
    Other(i32),
}

impl Op {
    /// A `Recv` into `buf`, or `buf` handed back when it is empty (a zero count
    /// would read as the end of the stream) or longer than a count can say.
    pub fn recv(fd: Fd, buf: Box<[u8]>) -> Result<Op, Box<[u8]>> {
        if buf.is_empty() || u32::try_from(buf.len()).is_err() {
            return Err(buf);
        }
        Ok(Op::Recv { fd, buf })
    }

    /// A `Send` of `bytes[from..]`, or `bytes` handed back when nothing is left
    /// to send from `from` or they are longer than a count can say.
    pub fn send(fd: Fd, bytes: Box<[u8]>, from: u32) -> Result<Op, Box<[u8]>> {
        if u32::try_from(bytes.len()).is_err() || left(&bytes, from).is_none() {
            return Err(bytes);
        }
        Ok(Op::Send { fd, bytes, from })
    }

    /// Whether the record is one [`Op::recv`] and [`Op::send`] would build.
    /// Every other operation is valid as it stands.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match self {
            Op::Recv { buf, .. } => !buf.is_empty() && u32::try_from(buf.len()).is_ok(),
            Op::Send { bytes, from, .. } => u32::try_from(bytes.len()).is_ok() && left(bytes, *from).is_some(),
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. }
            | Op::Cancel { .. } => true,
        }
    }

    /// The shape of this operation's success value.
    #[must_use]
    pub const fn shape(&self) -> Shape {
        match self {
            Op::Socket { .. } => Shape::Fd,
            Op::Accept { .. } => Shape::Accepted,
            Op::Recv { .. } | Op::Send { .. } => Shape::Count,
            Op::Bind { .. } => Shape::Bound,
            Op::Listen { .. } | Op::Connect { .. } | Op::Shutdown { .. } | Op::Close { .. } | Op::Cancel { .. } => {
                Shape::Nothing
            }
        }
    }
}

impl Done {
    #[must_use]
    pub const fn shape(&self) -> Shape {
        match self {
            Done::Nothing => Shape::Nothing,
            Done::Count(_) => Shape::Count,
            Done::Fd(_) => Shape::Fd,
            Done::Accepted { .. } => Shape::Accepted,
            Done::Bound(_) => Shape::Bound,
        }
    }
}

impl Complete {
    /// Whether a backend kept the contract with this completion, given that the
    /// operation was valid: a success of the operation's shape, with a count
    /// its buffer allows or the address its `Bind` asked for; for a `Cancel`,
    /// only `TooLate`, `InvalidArgument` or `Other` as an error, and for every
    /// other operation any error but `TooLate`. For the simulator and the
    /// conformance suite to check; io trusts its backend.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        let error = match &self.result {
            Ok(done) => return done.shape() == self.kind.shape() && fits(&self.kind, done),
            Err(error) => *error,
        };
        match &self.kind {
            Op::Cancel { .. } => fails_a_cancel(error),
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. } => !fails_a_cancel_only(error),
        }
    }
}

/// Whether `error` may answer a `Cancel`.
fn fails_a_cancel(error: Error) -> bool {
    match error {
        Error::TooLate | Error::InvalidArgument | Error::Other(_) => true,
        Error::Refused
        | Error::Reset
        | Error::BrokenPipe
        | Error::NotConnected
        | Error::AddressInUse
        | Error::AddressNotAvailable
        | Error::Unreachable
        | Error::TimedOut
        | Error::TooManyOpenFiles
        | Error::NoBufferSpace
        | Error::Cancelled => false,
    }
}

/// Whether `error` answers a `Cancel` and nothing else.
fn fails_a_cancel_only(error: Error) -> bool {
    match error {
        Error::TooLate => true,
        Error::Refused
        | Error::Reset
        | Error::BrokenPipe
        | Error::NotConnected
        | Error::AddressInUse
        | Error::AddressNotAvailable
        | Error::Unreachable
        | Error::TimedOut
        | Error::TooManyOpenFiles
        | Error::NoBufferSpace
        | Error::Cancelled
        | Error::InvalidArgument
        | Error::Other(_) => false,
    }
}

/// How many bytes of `bytes` are left from `from`, or `None` when none are.
fn left(bytes: &[u8], from: u32) -> Option<usize> {
    let from = usize::try_from(from).ok()?;
    match bytes.len().checked_sub(from) {
        Some(0) | None => None,
        Some(left) => Some(left),
    }
}

/// Whether a success value fits the operation it answers, beyond its shape,
/// which is checked apart: a count within its buffer, a bound address that is
/// the one asked for.
fn fits(op: &Op, done: &Done) -> bool {
    match done {
        Done::Count(n) => match usize::try_from(*n) {
            Ok(n) => counts(op, n),
            Err(_) => false,
        },
        Done::Bound(bound) => binds(op, bound),
        Done::Nothing | Done::Fd(_) | Done::Accepted { .. } => true,
    }
}

/// Whether a `Bind` bound the address it asked for: the same IP, and the same
/// port, or a port the kernel chose when it asked for port 0.
fn binds(op: &Op, bound: &Addr) -> bool {
    match op {
        Op::Bind { addr, .. } => {
            let port = match addr.port() {
                0 => bound.port() != 0,
                asked => bound.port() == asked,
            };
            bound.ip() == addr.ip() && port
        }
        Op::Socket { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Cancel { .. } => false,
    }
}

/// Whether a count fits the buffer of the operation it answers. Only `Recv`
/// and `Send` have one.
fn counts(op: &Op, n: usize) -> bool {
    match op {
        Op::Recv { buf, .. } => n <= buf.len(),
        Op::Send { bytes, from, .. } => match left(bytes, *from) {
            Some(left) => n >= 1 && n <= left,
            None => false,
        },
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Cancel { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

    use skein_lib::Token;

    use super::{Addr, Complete, Done, Error, Family, Fd, Op, Shape};

    const FD: Fd = Fd::new(3);
    const NEW: Fd = Fd::new(4);

    fn v4() -> Addr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 8080))
    }

    fn bytes(len: usize) -> Box<[u8]> {
        Box::from(&[7; 8][..len])
    }

    fn complete(kind: Op, result: Result<Done, Error>) -> Complete {
        Complete { op: Token::new(1), kind, result }
    }

    /// One of every operation, valid. A new operation makes the match in
    /// `documented` fail to build until it is added there, and here.
    fn every_op() -> [Op; 10] {
        [
            Op::Socket { family: Family::Ipv4 },
            Op::Bind { fd: FD, addr: v4() },
            Op::Listen { fd: FD, backlog: 16 },
            Op::Accept { fd: FD },
            Op::Connect { fd: FD, addr: v4() },
            Op::Recv { fd: FD, buf: bytes(4) },
            Op::Send { fd: FD, bytes: bytes(4), from: 1 },
            Op::Shutdown { fd: FD },
            Op::Close { fd: FD },
            Op::Cancel { target: Token::new(2) },
        ]
    }

    /// The success each operation documents on `Op`, written out again by an
    /// exhaustive match.
    fn documented(op: &Op) -> Done {
        match op {
            Op::Socket { .. } => Done::Fd(NEW),
            Op::Accept { .. } => Done::Accepted { fd: NEW, peer: v4() },
            Op::Recv { .. } | Op::Send { .. } => Done::Count(1),
            Op::Bind { .. } => Done::Bound(v4()),
            Op::Listen { .. } | Op::Connect { .. } | Op::Shutdown { .. } | Op::Close { .. } | Op::Cancel { .. } => {
                Done::Nothing
            }
        }
    }

    #[test]
    fn every_operation_succeeds_with_its_documented_shape_and_no_other() {
        let candidates =
            [Done::Nothing, Done::Count(1), Done::Fd(NEW), Done::Accepted { fd: NEW, peer: v4() }, Done::Bound(v4())];
        for op in every_op() {
            assert!(op.is_valid(), "the examples are valid");
            let expected = documented(&op);
            assert_eq!(op.shape(), expected.shape());
            let mut kind = op;
            for done in candidates {
                let answer = complete(kind, Ok(done));
                assert_eq!(answer.is_valid(), done == expected, "only the documented shape fits");
                kind = answer.kind;
            }
        }
    }

    #[test]
    fn the_shapes_are_as_the_table_on_op_says() {
        let [socket, bind, listen, accept, connect, recv, send, shutdown, close, cancel] = every_op();
        assert_eq!(socket.shape(), Shape::Fd);
        assert_eq!(accept.shape(), Shape::Accepted);
        assert_eq!(bind.shape(), Shape::Bound);
        assert_eq!((recv.shape(), send.shape()), (Shape::Count, Shape::Count));
        for op in [listen, connect, shutdown, close, cancel] {
            assert_eq!(op.shape(), Shape::Nothing);
        }
    }

    #[test]
    fn a_bind_answers_with_the_address_it_asked_for_and_port_zero_resolved() {
        let ip = Ipv4Addr::LOCALHOST;
        let bind = |port: u16| Op::Bind { fd: FD, addr: SocketAddr::from((ip, port)) };
        let cases = [
            (8080, SocketAddr::from((ip, 8080)), true),
            (8080, SocketAddr::from((ip, 8081)), false),
            (0, SocketAddr::from((ip, 40000)), true),
            (0, SocketAddr::from((ip, 0)), false),
            (8080, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 8080)), false),
            (8080, SocketAddr::from((Ipv6Addr::LOCALHOST, 8080)), false),
        ];
        for (port, bound, valid) in cases {
            assert_eq!(complete(bind(port), Ok(Done::Bound(bound))).is_valid(), valid, "port {port}");
        }
    }

    #[test]
    fn a_cancel_may_fail_to_be_submitted() {
        for error in [Error::InvalidArgument, Error::Other(5)] {
            assert!(complete(Op::Cancel { target: Token::new(2) }, Err(error)).is_valid(), "a backend failure");
        }
    }

    #[test]
    fn a_recv_buffer_is_never_empty() {
        assert_eq!(Op::recv(FD, bytes(0)), Err(bytes(0)));
        assert!(!Op::Recv { fd: FD, buf: bytes(0) }.is_valid());
        assert_eq!(Op::recv(FD, bytes(3)), Ok(Op::Recv { fd: FD, buf: bytes(3) }));
    }

    #[test]
    fn a_send_has_something_left_to_send_from_its_offset() {
        assert_eq!(Op::send(FD, bytes(3), 0), Ok(Op::Send { fd: FD, bytes: bytes(3), from: 0 }));
        assert_eq!(Op::send(FD, bytes(3), 2), Ok(Op::Send { fd: FD, bytes: bytes(3), from: 2 }));
        assert_eq!(Op::send(FD, bytes(3), 3), Err(bytes(3)));
        assert_eq!(Op::send(FD, bytes(3), 4), Err(bytes(3)));
        assert_eq!(Op::send(FD, bytes(0), 0), Err(bytes(0)));
        assert!(!Op::Send { fd: FD, bytes: bytes(3), from: 3 }.is_valid());
    }

    #[test]
    fn a_recv_count_is_at_most_its_buffer_and_zero_is_the_end() {
        for (n, valid) in [(0, true), (1, true), (4, true), (5, false)] {
            let answer = complete(Op::Recv { fd: FD, buf: bytes(4) }, Ok(Done::Count(n)));
            assert_eq!(answer.is_valid(), valid, "count {n}");
        }
    }

    #[test]
    fn a_send_count_is_at_least_one_and_at_most_what_was_left() {
        for (n, valid) in [(0, false), (1, true), (3, true), (4, false)] {
            let answer = complete(Op::Send { fd: FD, bytes: bytes(4), from: 1 }, Ok(Done::Count(n)));
            assert_eq!(answer.is_valid(), valid, "count {n}");
        }
    }

    #[test]
    fn too_late_answers_a_cancel_only() {
        assert!(complete(Op::Cancel { target: Token::new(2) }, Err(Error::TooLate)).is_valid());
        assert!(complete(Op::Cancel { target: Token::new(2) }, Ok(Done::Nothing)).is_valid());
        assert!(!complete(Op::Accept { fd: FD }, Err(Error::TooLate)).is_valid());
        assert!(complete(Op::Accept { fd: FD }, Err(Error::Cancelled)).is_valid());
    }

    #[test]
    fn a_cancel_fails_only_as_too_late_or_unsubmitted() {
        let cancel = || Op::Cancel { target: Token::new(2) };
        for error in [Error::Cancelled, Error::Reset, Error::NoBufferSpace, Error::NotConnected] {
            assert!(!complete(cancel(), Err(error)).is_valid(), "not an answer to a cancel");
        }
        for error in [Error::TooLate, Error::InvalidArgument, Error::Other(5)] {
            assert!(complete(cancel(), Err(error)).is_valid(), "an answer to a cancel");
        }
    }

    #[test]
    fn an_error_hands_the_buffer_back_too() {
        let answer = complete(Op::Recv { fd: FD, buf: bytes(4) }, Err(Error::Reset));
        assert!(answer.is_valid(), "any operation may fail");
        assert_eq!(answer.kind, Op::Recv { fd: FD, buf: bytes(4) });
    }

    #[test]
    fn the_family_follows_the_address() {
        assert_eq!(Family::of(&v4()), Family::Ipv4);
        assert_eq!(Family::of(&SocketAddr::from((Ipv6Addr::LOCALHOST, 80))), Family::Ipv6);
    }

    #[test]
    fn a_descriptor_is_its_raw_number() {
        assert_eq!(Fd::new(-1).raw(), -1_i32);
        assert_eq!(Fd::new(7), Fd::new(7));
    }
}
