//! The records between io and the layer above (io.md, 2): the requests io
//! takes, the events it tells, and its errors, which are what the layer above
//! can act on, not the kernel's.

use skein_lib::Token;
use skein_lib::stream::{self, Fault};

use crate::kernel::{self, Addr};

/// What the layer above asks of io. Each names an entity by a token: its
/// owner's, for an entity it asks io to make, and io's (from `Listening`,
/// `Accepted` or `Connecting`) for one that exists. A request naming an
/// entity that is gone, or closing, is dropped (io.md, 3.1).
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// A listener at `addr`: told `Listening` or `Failed`, then `Closed`.
    Listen { owner: Token, addr: Addr },
    /// A connection to `addr`: told `Connecting` at once, then `Connected` or
    /// `Failed`, then `Closed`; or, refused for want of a socket, `Failed`
    /// and `Closed` only.
    Connect { owner: Token, addr: Addr },
    /// The answer to `Accepted` that takes the socket: `owner` is told about
    /// it from now on, and its `Closed` ends it.
    Bind { socket: Token, owner: Token },
    /// The answer to `Accepted` that refuses the socket: io closes it, and
    /// tells no one.
    Reject { socket: Token },
    /// The stream vocabulary (lib.md, 7), to a socket once it is connected or
    /// bound.
    Stream { stream: Token, down: stream::Down },
    // Files, processes and signals (io.md, 5 to 7) go here when a user pulls
    // them: File { owner, root, op }, Spawn { owner, spawn },
    // Signal { child, signal }.
    /// A graceful close (io.md, 3): the output flushed and half-closed, the
    /// input discarded until the peer ends or the close deadline passes. Only
    /// `Closed` follows.
    Close { entity: Token },
    /// A close at once, whatever is in flight or queued. Only `Closed`
    /// follows.
    Abort { entity: Token },
}

/// What io tells the layer above. Every event names first the owner it is
/// for, by the owner's own token (programming-model.md, 4.2).
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// The listener listens at `addr`, its port resolved when it asked for
    /// port 0; `listener` names it in `Close` and `Abort`.
    Listening { owner: Token, listener: Token, addr: Addr },
    /// The listener of `owner` accepted a socket from `peer`, which `owner`
    /// answers with `Bind` or `Reject`. The listener accepts no other until
    /// it does.
    Accepted { owner: Token, socket: Token, peer: Addr },
    /// The connect has begun: `socket` names it in every request, so that its
    /// owner can close it before it connects.
    Connecting { owner: Token, socket: Token },
    /// The connection is made, and its stream runs.
    Connected { owner: Token },
    /// The stream vocabulary (lib.md, 7), once connected or bound.
    Stream { owner: Token, up: stream::Up },
    // Files, processes and signals go here when a user pulls them:
    // File { owner, result }, Spawned { owner, child, pipes },
    // Exited { owner, exit }, Shutdown { signal }.
    /// Told once. A listen or a connect failed: io closes what it made, and
    /// `Closed` follows without a request. Or a listener can accept no more:
    /// it stays, and its owner closes it.
    Failed { owner: Token, error: Error },
    /// The entity is gone, nothing of it in flight: the last event for
    /// `owner`.
    Closed { owner: Token },
}

/// Why a listen or a connect failed, as far as the layer above can act on it
/// (io.md, 2). Kernel errors stay below io.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Error {
    /// Nothing listened at the address.
    Refused,
    /// No route to the address.
    Unreachable,
    /// The kernel gave up on the peer, which never answered.
    TimedOut,
    /// The peer reset the connection as it was made.
    Reset,
    /// A refusal at io's entrance: no socket slot was free, or the kernel had
    /// no descriptor, local port or buffer left. Nothing was made; the request
    /// may be made again later.
    Busy,
    /// Anything else the kernel answered, which the layer above cannot act on:
    /// an address in use, or not this host's, among others.
    Other,
}

/// The `Error` of a `Socket`, `Bind` or `Listen` that failed.
pub(crate) fn setup_error(error: kernel::Error) -> Error {
    match error {
        kernel::Error::TooManyOpenFiles | kernel::Error::NoBufferSpace => Error::Busy,
        kernel::Error::Refused
        | kernel::Error::Reset
        | kernel::Error::BrokenPipe
        | kernel::Error::NotConnected
        | kernel::Error::AddressInUse
        | kernel::Error::AddressNotAvailable
        | kernel::Error::Unreachable
        | kernel::Error::TimedOut
        | kernel::Error::InvalidArgument
        | kernel::Error::Other(_) => Error::Other,
        kernel::Error::NotFound
        | kernel::Error::Exists
        | kernel::Error::NotADirectory
        | kernel::Error::IsADirectory
        | kernel::Error::NotEmpty
        | kernel::Error::Permission
        | kernel::Error::NoSpace
        | kernel::Error::ReadOnly
        | kernel::Error::TooManyLinks
        | kernel::Error::NameTooLong
        | kernel::Error::Escape
        | kernel::Error::NotAFile => {
            unreachable!("an operation on sockets never fails with a file's error (skein_io::kernel)")
        }
        kernel::Error::Cancelled | kernel::Error::TooLate => {
            unreachable!("io never cancels a socket's setup, and only a cancel is too late")
        }
    }
}

/// The `Error` of a `Connect` that failed, not cancelled.
pub(crate) fn connect_error(error: kernel::Error) -> Error {
    match error {
        kernel::Error::Refused => Error::Refused,
        kernel::Error::Unreachable => Error::Unreachable,
        kernel::Error::TimedOut => Error::TimedOut,
        kernel::Error::Reset => Error::Reset,
        // No local port free is the connect's own shortage, as a descriptor
        // or a buffer is.
        kernel::Error::TooManyOpenFiles | kernel::Error::NoBufferSpace | kernel::Error::AddressNotAvailable => {
            Error::Busy
        }
        kernel::Error::BrokenPipe
        | kernel::Error::NotConnected
        | kernel::Error::AddressInUse
        | kernel::Error::InvalidArgument
        | kernel::Error::Other(_) => Error::Other,
        kernel::Error::NotFound
        | kernel::Error::Exists
        | kernel::Error::NotADirectory
        | kernel::Error::IsADirectory
        | kernel::Error::NotEmpty
        | kernel::Error::Permission
        | kernel::Error::NoSpace
        | kernel::Error::ReadOnly
        | kernel::Error::TooManyLinks
        | kernel::Error::NameTooLong
        | kernel::Error::Escape
        | kernel::Error::NotAFile => {
            unreachable!("an operation on sockets never fails with a file's error (skein_io::kernel)")
        }
        kernel::Error::Cancelled | kernel::Error::TooLate => {
            unreachable!("a connect io cancelled is settled apart, and only a cancel is too late")
        }
    }
}

/// The `Fault` of a stream whose `Recv`, `Send` or `Shutdown` failed: `Reset`
/// when the peer is gone, `Other` otherwise.
pub(crate) fn stream_fault(error: kernel::Error) -> Fault {
    match error {
        kernel::Error::Reset
        | kernel::Error::TimedOut
        | kernel::Error::Unreachable
        | kernel::Error::BrokenPipe
        | kernel::Error::NotConnected => Fault::Reset,
        kernel::Error::Refused
        | kernel::Error::AddressInUse
        | kernel::Error::AddressNotAvailable
        | kernel::Error::TooManyOpenFiles
        | kernel::Error::InvalidArgument
        | kernel::Error::Other(_) => Fault::Other,
        kernel::Error::NotFound
        | kernel::Error::Exists
        | kernel::Error::NotADirectory
        | kernel::Error::IsADirectory
        | kernel::Error::NotEmpty
        | kernel::Error::Permission
        | kernel::Error::NoSpace
        | kernel::Error::ReadOnly
        | kernel::Error::TooManyLinks
        | kernel::Error::NameTooLong
        | kernel::Error::Escape
        | kernel::Error::NotAFile => {
            unreachable!("an operation on sockets never fails with a file's error (skein_io::kernel)")
        }
        kernel::Error::NoBufferSpace | kernel::Error::Cancelled | kernel::Error::TooLate => {
            unreachable!("no buffer is retried, io cancels only when settling, and only a cancel is too late")
        }
    }
}
