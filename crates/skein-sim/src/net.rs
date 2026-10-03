//! The simulated network: sockets, listeners and the byte streams between
//! connected sockets, over loopback addresses only.

use alloc::collections::VecDeque;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use skein_io::kernel::{Addr, Error, Family, Fd};
use skein_lib::Token;

use crate::sim::Pid;

/// A socket's name in the world, never reused. A socket has one descriptor
/// at most, in one process; one waiting in a listener's queue has none yet.
pub(crate) type SocketId = u64;

/// Linux's default ephemeral range, `ip_local_port_range`.
pub(crate) const EPHEMERAL_FIRST: u16 = 32768;
pub(crate) const EPHEMERAL_LAST: u16 = 60999;

#[derive(Debug)]
pub(crate) struct Socket {
    /// The process and descriptor that name it, once it has one.
    pub(crate) owner: Option<(Pid, Fd)>,
    pub(crate) family: Family,
    /// The address bound, by `Bind` or by a `Connect` that picked one.
    pub(crate) local: Option<Addr>,
    /// A `Connect` on it failed or was cancelled: only `Close` may follow.
    pub(crate) closing_only: bool,
    pub(crate) state: State,
}

#[derive(Debug)]
pub(crate) enum State {
    /// Made, and maybe bound.
    Fresh,
    /// A `Connect` waits for room in the queue of `listener`.
    Connecting {
        listener: SocketId,
        to: Addr,
    },
    Listening(Listener),
    Connected(Stream),
}

#[derive(Debug)]
pub(crate) struct Listener {
    /// The backlog, clamped: at least one, at most the world's.
    pub(crate) backlog: u32,
    /// Established connections waiting for an `Accept`, oldest first.
    pub(crate) queue: VecDeque<SocketId>,
    /// `Connect`s waiting for room in the queue, oldest first.
    pub(crate) connects: VecDeque<(Pid, Token, SocketId)>,
    /// The `Accept` in flight that waits for a connection.
    pub(crate) accepter: Option<Token>,
}

/// One end of a connection.
#[derive(Debug)]
pub(crate) struct Stream {
    /// The other end, while both are open and the connection is not reset.
    pub(crate) peer: Option<SocketId>,
    pub(crate) remote: Addr,
    /// Bytes received and not yet read, at most the world's buffer.
    pub(crate) inbox: VecDeque<u8>,
    /// The peer's end of stream is behind the bytes in `inbox`.
    pub(crate) ended: bool,
    /// This end has shut down its sending.
    pub(crate) shut: bool,
    pub(crate) fate: Fate,
    /// The `Recv` in flight that waits for bytes.
    pub(crate) receiver: Option<Token>,
    /// The `Send` in flight that waits for room.
    pub(crate) sender: Option<Token>,
}

/// Whether the connection broke, and whether this end has heard.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Fate {
    Open,
    /// Broke with `Reset` or `TimedOut`, to be reported to the next `Send`,
    /// or to the next `Recv` once the bytes already received are read.
    Failing(Error),
    /// Broke and reported, or broke after the peer's end of stream arrived
    /// (never reported), or after the peer closed: a `Recv` gives what is
    /// left then 0, a `Send` fails with `BrokenPipe`, a `Shutdown` with
    /// `NotConnected`.
    Dead,
}

impl Stream {
    pub(crate) const fn new(peer: SocketId, remote: Addr) -> Stream {
        Stream {
            peer: Some(peer),
            remote,
            inbox: VecDeque::new(),
            ended: false,
            shut: false,
            fate: Fate::Open,
            receiver: None,
            sender: None,
        }
    }
}

/// An address a socket of this host may bind: a loopback one, or the
/// unspecified one, which listens on every loopback address of its family.
pub(crate) fn bindable(ip: IpAddr) -> bool {
    ip.is_loopback() || ip.is_unspecified()
}

/// Whether `ip` is an IPv4-mapped IPv6 address, which an IPv6 socket,
/// `IPV6_V6ONLY` by default, can neither bind nor reach.
pub(crate) const fn mapped(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().is_some(),
        IpAddr::V4(_) => false,
    }
}

/// Whether two bound addresses can meet the same connection.
pub(crate) fn overlaps(a: IpAddr, b: IpAddr) -> bool {
    a == b || a.is_unspecified() || b.is_unspecified()
}

/// The source address of a connection from this host.
pub(crate) const fn loopback(family: Family) -> IpAddr {
    match family {
        Family::Ipv4 => IpAddr::V4(Ipv4Addr::LOCALHOST),
        Family::Ipv6 => IpAddr::V6(Ipv6Addr::LOCALHOST),
    }
}
