//! The kernel boundary (kernel.md): the records io submits to a
//! backend and the completions the backend hands back. The ring adapter, the
//! simulator and any later backend implement exactly this, and the
//! conformance suite (kernel.md, 8) holds each of them to it.
//!
//! # The contract
//!
//! Records:
//!
//! - **Every submission completes exactly once,** cancelled or not, with the
//!   [`Submit`]'s `op` token on its [`Complete`].
//! - **The completion hands back the operation:** [`Complete::kind`] is the
//!   submitted [`Op`], every buffer inside it, whatever the result. The
//!   backend never drops, copies or replaces a `Box` (programming-model.md,
//!   6.2). A `Recv` comes back with `buf[..n]` filled, a `Send` with its
//!   `bytes` untouched.
//! - **The backend allocates nothing that goes up** (kernel.md, section 4).
//!   A `Spawn` returns its original pipe table, each `parent` slot filled
//!   only on success; `Done::Spawned` carries the pidfd alone.
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
//!   (programming-model.md, 5.3).
//! - **A `Cancel` completes on its own,** before or after its target (the
//!   simulator randomises which):
//!   - `Ok(Nothing)`: it stopped the target, which completes
//!     `Err(Cancelled)`. A stopped `Recv`, `Send` or `Accept` took nothing:
//!     bytes or a connection that had arrived wait for the next one. A
//!     stopped `Connect` may still have reached its peer, which sees the
//!     connection end when io closes the socket;
//!   - `Err(TooLate)`: the target had completed or could no longer be
//!     stopped, and completes with its own result, or `Err(Cancelled)` when
//!     the kernel interrupted it;
//!   - `Err(InvalidArgument)` or `Err(Other)`: the backend could not submit
//!     it, and the target runs on.
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
//!   never refuses it: `Refused` means nothing listened there when the
//!   connect arrived. A `Connect` delayed too long fails with `TimedOut`.
//! - **A failed `Accept`** (`TooManyOpenFiles`, `NoBufferSpace`) consumed no
//!   waiting connection. A connection closed while waiting is still
//!   accepted, and gives its bytes, then the end of the stream. One reset
//!   while waiting is still accepted too; its first `Send`, or its first
//!   `Recv` after the bytes already received, fails with `Reset`.
//! - **A `Recv` of zero bytes means the stream ended:** the peer shut down or
//!   closed, or a reset was already reported. It does not prove a graceful
//!   close. A `Recv` buffer is never empty ([`Op::recv`]).
//! - **A `Send`'s count is bytes the kernel accepted** past `from`, at least
//!   one and no more than were left. io continues a short send from
//!   `from + n` (io.md, 3).
//! - **`Shutdown` ends this side's sending:** it completes `Ok` once the end
//!   is queued, behind the bytes of every completed `Send`. A second
//!   `Shutdown` is `Ok` while the connection lasts and `NotConnected` once it
//!   is closed. `Recv` keeps working after it.
//! - **A reset is reported to exactly one operation,** as `Reset` (or
//!   `TimedOut`, when this end gave up): to a `Send` at once, to a `Recv`
//!   after the bytes already received, which stay readable. After it, a
//!   `Recv` gives `Ok(Count(0))`, a `Send` fails with `BrokenPipe` and a
//!   `Shutdown` with `NotConnected`. An end that already received the peer's
//!   end of stream hears of no reset: its `Recv` drains to `Ok(Count(0))`,
//!   its `Send` fails with `BrokenPipe`, its `Shutdown` with `NotConnected`.
//! - **A `Send` after the peer closed** with nothing unread may succeed, its
//!   bytes lost, until the peer's reset arrives; then it fails with
//!   `BrokenPipe`, never `Reset`, a `Recv` gives `Ok(Count(0))` and a
//!   `Shutdown` fails with `NotConnected`.
//! - **An [`Fd`] is closed only by `Close`,** which releases it whatever its
//!   result. A `Close` with received data unread makes the peer see `Reset`;
//!   closing a listener resets the connections waiting on it.
//! - **Records the kernel refuses for the socket's state** are bugs in io,
//!   answered rather than assumed away: a `Recv` on a socket never connected
//!   (a listener among them) fails with `NotConnected`, a `Send` with
//!   `BrokenPipe`; a second `Bind`, a `Bind` or `Listen` on a connected
//!   socket, and an `Accept` on one that does not listen, with
//!   `InvalidArgument`.
//!
//! Files, beneath a root:
//!
//! - **A root is an open directory:** a descriptor the shell opened at
//!   startup, or one from an `Open` of a directory beneath another root
//!   ([`OpenHow::Directory`], or `Read` of a directory). An `Open` resolves
//!   its path beneath its root as `openat2` does with `RESOLVE_BENEATH` and
//!   `RESOLVE_NO_MAGICLINKS`: `..` above the root, an absolute path, and a
//!   symbolic link that leads out of it (an absolute one among them) fail
//!   with `Escape`; a symbolic link that stays beneath it is followed by
//!   the ordinary open modes. The `NoFollow` modes refuse any symbolic link
//!   in the path with `TooManyLinks`, for writes that must stay in the named
//!   directory. A
//!   loop of links, more than 40 in one resolution, and a magic link
//!   (`/proc/*/fd/*`, should a root hold one) fail with `TooManyLinks`. A name longer than 255 bytes, or a path of 4096 or
//!   more, fails with `NameTooLong`; an empty path with `NotFound`. A `..`
//!   that races a rename or a mount anywhere on the system, which
//!   `RESOLVE_BENEATH` answers with `EAGAIN`, has the backend submit the
//!   `Open` again, up to 16 times: still racing, it fails with `Other(11)`.
//! - **`Rename`, `Remove` and `MakeDirectory` act on one entry of an open
//!   directory,** named by an [`is_name`]: no `/`, not `.` or `..`. Only
//!   `openat2` resolves a path beneath a root, so whatever lies further down
//!   is opened first, as a directory, beneath the root. The entry itself is
//!   never followed: removing or renaming a symbolic link acts on the link.
//! - **`Open`** as [`OpenHow`] says: `Read` an existing file or directory,
//!   following a final symbolic link; `Directory` an existing directory;
//!   `Create` a new, empty file, to write, which fails with `Exists` if the
//!   name is taken by anything, a dangling symbolic link included. New files
//!   are made with the mode `Create` asks for, or `0o666`, and directories
//!   `0o777`, less the process's umask.
//! - **`Read` and `Write` are at an offset,** never at the descriptor's
//!   position. A `Read` counts the bytes read into `buf[..n]`: fewer than
//!   it asked for at the end of the file, and whenever the backend chooses
//!   (the simulator draws it); 0 means `at` is at or past the end. A `Read`
//!   of a directory fails with `IsADirectory`. A `Write` counts the bytes
//!   written from `bytes[from..]` to the file at `at`: at least one, and no
//!   more than were left. io continues a short one from `from + n` at
//!   `at + n`. Writing past the end leaves zeros in the gap.
//! - **`Append` writes at the descriptor's position**, selecting the current
//!   end for a startup file opened with `O_APPEND`. On an ordinary writable
//!   file it advances the position after each write. Counts are positive;
//!   a short append continues from `from + n`. Only one is in flight on a
//!   descriptor opened to write (kernel.md, 6.1 and 7).
//! - **`Sync`** flushes the file, or a directory's entries, to storage:
//!   once it completes, what was written or renamed before it survives a
//!   crash.
//! - **`Stat`** answers the [`Kind`], size and permission bits of what is
//!   open on its descriptor: for a file, its length in bytes; for anything
//!   else, whatever its filesystem says.
//! - **`Rename` is atomic:** `to` names the old entry or the moved one,
//!   never neither and never a mix. It replaces a file with a file, and an
//!   empty directory with a directory; one entry renamed over another that
//!   is the same file is left as it is. A directory never moves beneath
//!   itself. A symbolic link at `to` is replaced, not followed. Hence the
//!   idiom that replaces a file whole: `Stat` the old one for its mode,
//!   `Create` a new one with it beside the old, `Write` it, `Sync` it,
//!   `Close` it, `Rename` it over the old, then `Sync` the directory.
//! - **`List`** hands back the next entries of the directory open on `fd`,
//!   from where the last `List` of that descriptor stopped (the start, once
//!   opened), `.` and `..` left out: at most `entries.len()`, each with its
//!   kind and a name of its own in `names` ([`Entry`]); `Count(0)` at the
//!   end. It stops short when the next name does not fit what is left of
//!   `names`, or when the backend chooses, but never before one entry while
//!   any is left: `names` holds at least the longest name. A filesystem
//!   whose names may be longer (one that stores them in another encoding)
//!   fails a `List` with `NameTooLong` when the next name does not fit in
//!   all of `names`, and the next `List` meets it again. Entries come in
//!   no particular order, and one made or removed while a directory is
//!   listed may or may not be seen.
//! - **A file removed while open** stays readable and writable through its
//!   descriptor until its `Close`. A directory removed while open is empty
//!   for good: any name beneath it fails with `NotFound`, to an `Open`, a
//!   `Rename`, a `Remove` or a `MakeDirectory`, and so does a `List` of it.
//! - **An `Open`, `Read`, `Write` or `Sync` may be cancelled,** as an
//!   operation on sockets may, with the same outcomes (Cancelling, above):
//!   none waits on a peer, but on a filesystem that can stall (NFS, FUSE)
//!   one may wait long, and io's deadline gives up on it. A `Stat`,
//!   `Rename`, `Remove`, `MakeDirectory` or `List` completes without one.
//! - **Only files and directories open.** An `Open` of anything else beneath
//!   a root (a FIFO, a device, a socket) fails with `NotAFile`: the backend
//!   opens without blocking and never as a controlling terminal
//!   (`O_NONBLOCK | O_NOCTTY`), looks at what it opened, and closes what is
//!   neither, so no `Read` ever waits on a FIFO's writer. A device's own
//!   `open` may still do something; roots belong on a filesystem mounted
//!   `nodev`, where the kernel refuses a device with `Permission`. A file
//!   opened is blocking again for its `Read`s and `Write`s.
//!
//! Child processes and pipes:
//!
//! - `Spawn` opens its working directory beneath `root`, makes each pipe at
//!   the chosen child descriptor, fills missing standard descriptors from
//!   `/dev/null`, and leaves every other descriptor close-on-exec. It answers
//!   with a pidfd and the parent ends in request order. `Way::In` means the
//!   child reads; `Way::Out` means it writes.
//! - `PipeRead` and `PipeWrite` have the same count rules as `Recv` and
//!   `Send`, with EOF at a zero read count. A pipe is closed by `Close`.
//! - `Wait` observes an exit without reaping, or reaps after that observation;
//!   both answer `Exit`. The zombie retains the group's leader until its reap.
//! - `Signal` names the child or its group through the pidfd, never a numeric PID.
//! - `Usage` is synchronous: own and reaped children's CPU and resident peaks
//!   (kernel.md, sections 6.2 and 6.3). It cannot be cancelled.
//!
//! The errors each operation names, beyond `NoBufferSpace`,
//! `InvalidArgument` and `Other`, which any operation but `Cancel` may
//! answer, and `Cancelled`, which any operation a `Cancel` may stop may;
//! each variant of [`Error`] says which Linux error numbers it is, per
//! operation:
//!
//! | Op | Errors |
//! |---|---|
//! | the socket operations, and `Close` | any, but `TooLate` and the files' errors below |
//! | `Open` | `NotFound`, `Exists`, `NotADirectory`, `IsADirectory`, `Permission`, `NoSpace`, `ReadOnly`, `TooManyLinks`, `NameTooLong`, `Escape`, `NotAFile`, `TooManyOpenFiles` |
//! | `Read` | `IsADirectory` |
//! | `Write`, `Append` | `NoSpace`, `ReadOnly` |
//! | `Sync` | `NoSpace` |
//! | `Stat` | none |
//! | `Rename` | `NotFound`, `NotADirectory`, `IsADirectory`, `NotEmpty`, `Permission`, `NoSpace`, `ReadOnly`, `TooManyLinks`, `NameTooLong` |
//! | `Remove` | `NotFound`, `NotADirectory`, `IsADirectory`, `NotEmpty`, `Permission`, `ReadOnly`, `NameTooLong` |
//! | `MakeDirectory` | `NotFound`, `Exists`, `NotADirectory`, `Permission`, `NoSpace`, `ReadOnly`, `TooManyLinks`, `NameTooLong` |
//! | `List` | `NotFound`, `NotADirectory`, `NameTooLong` |
//! | `Usage` | only `Other` |
//! | `Cancel` | only `TooLate`, `InvalidArgument` or `Other` |
//!
//! Broken invariants, which io never commits and backends may assume never
//! happen. The simulator fails the world on each; the ring asserts the first
//! two at submit, and is otherwise unspecified:
//!
//! - a record that is not [`Op::is_valid`];
//! - a token already in flight;
//! - a `Cancel` whose target is a `Cancel`, a `Stat`, a `Rename`, a
//!   `Remove`, a `MakeDirectory`, a `List`, a `Spawn`, a `Signal` or a `Usage`;
//! - a `Wait` beside another on its pidfd, or a reap before a reported exit;
//! - any operation on a pidfd after its reap, except its `Close`;
//! - an operation on files ([`Op::is_file`]) on a socket's descriptor, or
//!   an operation on sockets on a file's;
//! - a `Read` on a descriptor not opened with [`OpenHow::Read`], a `Write`
//!   on one not opened with [`OpenHow::Create`], a `List` on one opened
//!   with `Create`;
//! - a `Bind` or `Connect` address of another family than its socket's;
//! - per descriptor, more than one `Recv`, one `Send` or one `Accept` in
//!   flight, or anything in flight beside a `Connect`;
//! - after a `Connect` that failed or was cancelled, any operation on its
//!   descriptor but `Close`;
//! - a `Connect` on a socket that is not fresh (bound or not, but never
//!   connecting, connected or listening);
//! - a `Listen` on a socket that is not bound;
//! - a `Shutdown` on a socket that is not a connection (a listener, or one
//!   never connected);
//! - a `Shutdown` while a `Send` on its descriptor is in flight;
//! - a `Close` while any other operation on its descriptor is in flight, or
//!   any operation on a descriptor after its `Close`. An operation is on
//!   every descriptor it names: an `Open` on its root, a `Rename` on both
//!   its directories.
//!
//! Dropping a backend with operations in flight is a shutdown path, not
//! part of the contract: those records are not handed back, and their
//! descriptors stay open until the process ends.
//!
//! Backend defaults, not records, until a service pulls one: every
//! descriptor is close-on-exec; an `Open` neither blocks nor takes a
//! controlling terminal; a new file is made `0o666` and a new directory
//! `0o777`, less the umask; a socket that binds gets `SO_REUSEADDR`;
//! every IPv6 socket gets `IPV6_V6ONLY`, so families never mix: its `Bind`
//! of an IPv4-mapped address fails with `InvalidArgument`, its `Connect`
//! to one with `Unreachable`; connected and accepted sockets get
//! `TCP_NODELAY`; a `Send` never raises `SIGPIPE` (`MSG_NOSIGNAL`).

use alloc::boxed::Box;
use core::net::SocketAddr;

use skein_lib::{Duration, Token};

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
/// before io (io.md, 4).
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
/// | `Open` | `Done::Fd`, the file or directory opened |
/// | `Read` | `Done::Count`, bytes read into `buf[..n]`, 0 at the end of the file |
/// | `Write`, `Append` | `Done::Count`, bytes written from `from`, at least 1 |
/// | `Sync`, `Rename`, `Remove`, `MakeDirectory` | `Done::Nothing` |
/// | `Stat` | `Done::Stat`, the kind and size of what is open |
/// | `List` | `Done::Count`, entries listed into `entries[..n]`, 0 at the end |
/// | `Spawn` | `Done::Spawned`, pidfd; parent ends written into the returned `Spawn` pipe slots |
/// | `Wait` | `Done::Exit`, exit code or signal |
/// | `Signal` | `Done::Nothing`, child or group through its pidfd |
/// | `Usage` | `Done::Usage`, own and reaped children resources |
/// | `ReadSignal` | `Done::ServiceSignal`, one blocked termination signal |
/// | `PipeRead`, `PipeWrite` | `Done::Count`, bytes read or written |
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
    /// Releases a socket's descriptor or a file's.
    Close {
        fd: Fd,
    },
    // Files, beneath a root (see the module documentation). Processes and
    // the other synchronous operations (io.md, 6; kernel.md) go here, when
    // a user pulls them.
    /// `path`, resolved beneath the directory open on `root`, opened as `how`
    /// says. The path holds no NUL byte.
    Open {
        root: Fd,
        path: Box<[u8]>,
        how: OpenHow,
    },
    /// Up to `buf.len()` bytes of the file from offset `at`, into `buf`,
    /// which is never empty.
    Read {
        fd: Fd,
        buf: Box<[u8]>,
        at: u64,
    },
    /// Some of `bytes[from..]`, which is never empty, written at offset `at`.
    Write {
        fd: Fd,
        bytes: Box<[u8]>,
        from: u32,
        at: u64,
    },
    /// A stream write at the descriptor's position, at the end when opened with append.
    Append {
        fd: Fd,
        bytes: Box<[u8]>,
        from: u32,
    },
    /// Flushes what was written to the file, or to the directory's entries.
    Sync {
        fd: Fd,
    },
    /// The kind and size of what is open on `fd`.
    Stat {
        fd: Fd,
    },
    /// Moves the entry `from` of the directory open on `from_dir` to the
    /// name `to` in the one open on `to_dir`, replacing what `to` named.
    Rename {
        from_dir: Fd,
        from: Box<[u8]>,
        to_dir: Fd,
        to: Box<[u8]>,
    },
    /// Removes the entry `name` of the directory open on `dir`: with
    /// `directory`, an empty directory; without, anything else.
    Remove {
        dir: Fd,
        name: Box<[u8]>,
        directory: bool,
    },
    /// Makes a directory, `name`, in the one open on `dir`, with `mode` less the umask.
    MakeDirectory {
        dir: Fd,
        name: Box<[u8]>,
        mode: u32,
    },
    /// The next entries of the directory open on `fd`, into `entries`,
    /// never empty, with their names in `names`, which holds at least
    /// [`LONGEST_NAME`] bytes.
    List {
        fd: Fd,
        entries: Box<[Entry]>,
        names: Box<[u8]>,
    },
    /// Starts a child with only the requested pipes and its standard
    /// descriptors (unassigned standard descriptors read/write /dev/null).
    Spawn {
        spawn: Box<Spawn>,
    },
    /// Observes a child exit, or reaps it after observation (kernel.md, section 6.2).
    Wait {
        pidfd: Fd,
        reap: bool,
    },
    /// Sends a signal to a child by pidfd, so a reused PID cannot be hit.
    Signal {
        pidfd: Fd,
        signal: Signal,
        to: Target,
    },
    /// Reads the process and reaped children resource usage (kernel.md, section 6.3).
    Usage,
    /// Reads one termination signal from the signalfd opened at startup
    /// (io.md, section 7; shell.md, section 6).
    ReadSignal {
        fd: Fd,
    },
    /// Reads from the parent's end of a child pipe.
    PipeRead {
        fd: Fd,
        buf: Box<[u8]>,
    },
    /// Writes to the parent's end of a child pipe.
    PipeWrite {
        fd: Fd,
        bytes: Box<[u8]>,
        from: u32,
    },
    /// Asks the operation named `target` to stop early.
    Cancel {
        target: Token,
    },
}

/// A process's command and descriptor table. Arguments exclude argv[0],
/// which is the program. Environment entries are `NAME=value` bytes.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Spawn {
    pub program: Box<[u8]>,
    pub args: Box<[Box<[u8]>]>,
    pub env: Box<[Box<[u8]>]>,
    pub root: Fd,
    pub dir: Box<[u8]>,
    pub pipes: Box<[Pipe]>,
}

/// A requested child pipe, sent by io and returned with its parent end (kernel.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Pipe {
    pub child: u32,
    pub way: Way,
    /// Empty on submission and failure; filled by the backend on success.
    pub parent: Option<Fd>,
}

/// The direction as seen by the child.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Way {
    In,
    Out,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
/// A termination signal requested by a child owner.
pub enum Signal {
    /// Ask the recipient to terminate.
    Terminate,
    /// End the recipient without allowing it to handle the signal.
    Kill,
}

/// A signal's recipient, selected by the pidfd's owner (kernel.md, section 6.2).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Target {
    /// The child alone.
    Child,
    /// Every member of the group the child leads.
    Group,
}

/// The process's own and reaped children's resources, returned by the backend.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Usage {
    pub own: Resources,
    pub children: Resources,
}

impl Usage {
    pub const ZERO: Usage = Usage { own: Resources::ZERO, children: Resources::ZERO };
}

/// CPU time and the largest resident size, returned in one usage part.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Resources {
    pub user: Duration,
    pub system: Duration,
    pub peak_rss_bytes: u64,
}

impl Resources {
    pub const ZERO: Resources = Resources { user: Duration::ZERO, system: Duration::ZERO, peak_rss_bytes: 0 };
}

/// A termination signal delivered to the service by its signalfd.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ServiceSignal {
    /// An interrupt request (`SIGINT`).
    Interrupt,
    /// A termination request (`SIGTERM`).
    Terminate,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
/// A child exit, reported by both observing and reaping waits.
pub enum Exit {
    /// The program's ordinary exit status.
    Code(u8),
    /// The signal that ended the program.
    Signal(u32),
}

/// How an [`Op::Open`] opens what its path names.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum OpenHow {
    /// An entry itself, links refused in its parents, for Stat and Close only (kernel.md, 6.1).
    PathNoFollow,
    /// An existing file or directory, to `Read` or `Stat` (`O_RDONLY`).
    Read,
    /// An existing file to read, refusing a symbolic link in any path part.
    ReadNoFollow,
    /// An existing directory: a root beneath this one, or a directory to
    /// `List` (`O_RDONLY | O_DIRECTORY`).
    Directory,
    /// An existing directory, refusing a symbolic link in any path part.
    DirectoryNoFollow,
    /// A new, empty file, to `Write` (`O_WRONLY | O_CREAT | O_EXCL`), made
    /// with `mode`'s permission bits (`0o777` at most), or `0o666` without
    /// one, less the process's umask: to keep an old file's when replacing
    /// it.
    Create { mode: Option<u32> },
    /// A new file, refusing a symbolic link in any parent path part.
    CreateNoFollow { mode: Option<u32> },
}

/// What a name beneath a root names.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Kind {
    File,
    Directory,
    /// A directory entry or a path-only descriptor naming a symbolic link.
    Symlink,
    /// A FIFO, socket or device, from List or path-only Stat; ordinary Open refuses it.
    Other,
}

/// What a `Stat` answers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Stat {
    pub kind: Kind,
    /// A file's length in bytes; for anything else, whatever its filesystem
    /// says.
    pub size: u64,
    /// Its permission bits (`0o777` at most).
    pub mode: u32,
    /// The filesystem user ID that owns it.
    pub owner: u32,
    pub links: u32,
}

/// The permission bits a mode may hold: an `OpenHow::Create`'s, a `Stat`'s.
pub const PERMISSIONS: u32 = 0o777;

/// One entry a `List` handed back: its kind, and where its name lies in the
/// `List`'s `names`, after the names of the entries before it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Entry {
    pub kind: Kind,
    /// The name is `names[start..start + len]`: an [`is_name`].
    pub start: u32,
    pub len: u32,
}

impl Entry {
    /// What io fills a `List`'s `entries` with, for the backend to write.
    pub const BLANK: Entry = Entry { kind: Kind::Other, start: 0, len: 0 };

    /// This entry's name, in the `names` of its `List`, or `None` when it
    /// does not lie within them.
    #[must_use]
    pub fn name(self, names: &[u8]) -> Option<&[u8]> {
        let start = usize::try_from(self.start).ok()?;
        let end = start.checked_add(usize::try_from(self.len).ok()?)?;
        names.get(start..end)
    }
}

/// The longest name a filesystem holds (`NAME_MAX`): a `List`'s `names`
/// holds at least this many bytes, so that one more entry always fits.
pub const LONGEST_NAME: usize = 255;

/// Whether `name` names one entry of a directory and nothing else: not
/// empty, not `.` or `..`, and with no `/` and no NUL byte. A name of more
/// than [`LONGEST_NAME`] bytes is one, which the kernel refuses with
/// `NameTooLong`.
#[must_use]
pub fn is_name(name: &[u8]) -> bool {
    if name.is_empty() || name == b"." || name == b".." {
        return false;
    }
    for &byte in name {
        if byte == b'/' || byte == 0 {
            return false;
        }
    }
    true
}

/// The success values, one [`Shape`] per operation (see [`Op`]).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Done {
    /// Nothing more than that it happened.
    Nothing,
    /// Bytes received into the start of `buf`, read into it, or sent or
    /// written from `from`; entries listed.
    Count(u32),
    /// A new socket, or a file or directory opened.
    Fd(Fd),
    /// A new socket, accepted from `peer`.
    Accepted {
        fd: Fd,
        peer: Addr,
    },
    /// The address a socket was bound to, with the port the kernel chose when
    /// the `Bind` asked for port 0.
    Bound(Addr),
    /// What a `Stat` found.
    Stat(Stat),
    /// The backend's child descriptor; parent pipe ends are in the returned spawn record.
    Spawned {
        pidfd: Fd,
    },
    Exit(Exit),
    /// One service termination signal read from a signalfd.
    ServiceSignal(ServiceSignal),
    /// The process and its reaped children, at submission.
    Usage(Usage),
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
    Stat,
    Spawned,
    Exit,
    ServiceSignal,
    /// Resource usage.
    Usage,
}

/// The errors io handles by name. Each backend maps its kernel's error numbers
/// onto these, per operation (the Linux names are given for the ring
/// adapter), and anything else onto `Other`. Which operation may answer
/// which is tabled in the module documentation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Error {
    /// `Connect`: nothing listened at the address when the connect arrived
    /// (`ECONNREFUSED`). A full accept queue delays a `Connect`, never refuses
    /// it.
    Refused,
    /// `Recv`, `Send`, `Connect`: the peer reset the connection
    /// (`ECONNRESET`), reported to one operation only, and to a `Recv` after
    /// the bytes already received. `Accept`:
    /// `ECONNABORTED`, not seen on Linux, where a connection reset while it
    /// waits is still accepted and its first `Recv` or `Send` fails with this.
    Reset,
    /// `Send`: the connection can send no more: after a reset was reported,
    /// after this side's own `Shutdown`, after the peer closed, or on a socket
    /// never connected (`EPIPE`).
    BrokenPipe,
    /// `Shutdown`: the socket is not connected: after a reset or a timeout,
    /// reported or not, or after both sides closed. `Recv`: the socket was
    /// never connected (`ENOTCONN`).
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
    /// (`ENETUNREACH`, `EHOSTUNREACH`), as from an IPv6 socket to an
    /// IPv4-mapped address.
    Unreachable,
    /// `Connect`, `Recv`, `Send`: the kernel's own timeout, such as a connect
    /// that was never answered or retransmissions that went unacknowledged
    /// (`ETIMEDOUT`), reported as a reset is. io's own deadlines are not this:
    /// they end in a `Cancel`.
    TimedOut,
    /// `Socket`, `Accept`, `Open`: the process or the system has no
    /// descriptor left (`EMFILE`, `ENFILE`). A failed `Accept` consumed no
    /// waiting connection.
    TooManyOpenFiles,
    /// Any operation but `Cancel`: the kernel is out of memory for buffers or
    /// sockets (`ENOBUFS`, `ENOMEM`). A failed `Accept` consumed no waiting
    /// connection.
    NoBufferSpace,
    /// Any operation on sockets, `Close`, `Open`, `Read`, `Write`, `Sync`: a
    /// `Cancel` stopped it (`ECANCELED`, and `EINTR` on an operation io
    /// cancelled). It did nothing, but for a `Connect`, which may still have
    /// reached its peer.
    Cancelled,
    /// `Cancel` only: the target had already completed, or was too far along
    /// to stop (`ENOENT`, `EALREADY`; on any other operation those are
    /// `Other`). Its own completion says what it did: its own result, or
    /// `Cancelled` when the kernel interrupted it.
    TooLate,
    /// `Open`: a directory on the path, or what it names without `Create`,
    /// does not exist, or the path is empty. `Rename`, `Remove`: the entry
    /// does not exist. `Open`, `MakeDirectory`, `List`, `Rename`, `Remove`:
    /// the directory was removed while open (`ENOENT`).
    NotFound,
    /// `Open` with `Create`, `MakeDirectory`: the name is taken (`EEXIST`).
    Exists,
    /// `Open`: the root, or a name the path goes through, is not a
    /// directory; or it names something else `Directory` opens. `Rename`,
    /// `Remove`, `MakeDirectory`, `List`: the descriptor is not a
    /// directory's; `Rename` of a directory over something else; `Remove`
    /// with `directory` of something else (`ENOTDIR`).
    NotADirectory,
    /// `Open` with `Create` of a path ending in `/`; `Read` of a directory;
    /// `Remove` without `directory` of a directory; `Rename` of anything else
    /// over a directory (`EISDIR`).
    IsADirectory,
    /// `Remove` with `directory`, `Rename`: the directory removed or renamed
    /// over has entries (`ENOTEMPTY`, `EEXIST`).
    NotEmpty,
    /// `Open`: a directory on the path may not be searched, or what `Read`
    /// or `Directory` opens may not be read, or the directory `Create` makes
    /// a file in may not be written. `Rename`, `Remove`, `MakeDirectory`: a
    /// directory an entry is made in, removed from or moved out of may not
    /// be written, or searched (`EACCES`, `EPERM`).
    Permission,
    /// `Open` with `Create`, `Write`, `Sync`, `Rename`, `MakeDirectory`: the
    /// filesystem, or the user's quota, is full (`ENOSPC`, `EDQUOT`). What
    /// failed did nothing; a `Write` that reached the limit counts what it
    /// wrote before it.
    NoSpace,
    /// `Open` with `Create`, `Write`, `Rename`, `Remove`, `MakeDirectory`:
    /// the filesystem is read-only (`EROFS`).
    ReadOnly,
    /// `Open`: a loop of symbolic links, more than 40 in one resolution, or
    /// a magic link, which `RESOLVE_NO_MAGICLINKS` refuses (`ELOOP`).
    /// `Rename`, `MakeDirectory`: the directory has as many links as its
    /// filesystem allows (`EMLINK`).
    TooManyLinks,
    /// `Open`, `Rename`, `Remove`, `MakeDirectory`: a name longer than
    /// [`LONGEST_NAME`] bytes, or a path of 4096 bytes or more
    /// (`ENAMETOOLONG`). `List`: the next name does not fit in all of its
    /// `names`, as a name longer than the longest may on some filesystems.
    NameTooLong,
    /// `Open`: the path leads out of its root, by `..` above it, an absolute
    /// path, or a symbolic link out of it (`EXDEV`, as `RESOLVE_BENEATH`
    /// answers). `EXDEV` on a `Rename`, the two directories on different
    /// filesystems, is `Other`.
    Escape,
    /// `Open`: the path names neither a file nor a directory, but a FIFO, a
    /// device or a socket, which the backend refuses once it has opened it,
    /// or, for a socket or a device with no driver, the kernel refuses
    /// (`ENXIO`).
    NotAFile,
    /// Any operation: the record was one the kernel refuses, such as `Listen`
    /// on a connected socket, or a `Rename` of a directory beneath itself
    /// (`EINVAL`, `EAFNOSUPPORT`). A bug in io, reported rather than
    /// asserted, since the kernel judged it. On a `Cancel`, the backend
    /// could not submit it, and the target was not stopped.
    InvalidArgument,
    /// Anything else, with the backend's own code, for diagnostics only. On a
    /// `Cancel`, the backend could not submit it, and the target was not
    /// stopped.
    Other(i32),
}

/// The operations on files, to table which errors each names.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Files {
    Open,
    Read,
    Write,
    Sync,
    Stat,
    Rename,
    Remove,
    MakeDirectory,
    List,
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

    /// A `Read` into `buf` at `at`, or `buf` handed back when it is empty (a
    /// zero count would read as the end of the file), longer than a count
    /// can say, or would read past the largest offset.
    pub fn read(fd: Fd, buf: Box<[u8]>, at: u64) -> Result<Op, Box<[u8]>> {
        if !reads(&buf, at) {
            return Err(buf);
        }
        Ok(Op::Read { fd, buf, at })
    }

    /// A `Write` of `bytes[from..]` at `at`, or `bytes` handed back when
    /// nothing is left to write from `from`, they are longer than a count can
    /// say, or would end past the largest offset.
    pub fn write(fd: Fd, bytes: Box<[u8]>, from: u32, at: u64) -> Result<Op, Box<[u8]>> {
        if !writes(&bytes, from, at) {
            return Err(bytes);
        }
        Ok(Op::Write { fd, bytes, from, at })
    }

    /// An `Append` of `bytes[from..]`, or the bytes returned when no valid count remains.
    pub fn append(fd: Fd, bytes: Box<[u8]>, from: u32) -> Result<Op, Box<[u8]>> {
        if u32::try_from(bytes.len()).is_err() || left(&bytes, from).is_none() {
            return Err(bytes);
        }
        Ok(Op::Append { fd, bytes, from })
    }

    /// Whether the record is one the kernel can be handed: a `Recv`, `Send`,
    /// `Read`, `Write` or `Append` that [`Op::recv`], [`Op::send`], [`Op::read`],
    /// [`Op::write`] and [`Op::append`] would build; an `Open` whose path has no NUL byte, and
    /// whose mode, to create, holds only [`PERMISSIONS`]; a
    /// `Rename`, `Remove` or `MakeDirectory` of names that are each an
    /// [`is_name`]; a `List` with room for an entry, `names` of at least
    /// [`LONGEST_NAME`] bytes, and both no longer than a count can say.
    /// Every other operation is valid as it stands.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match self {
            Op::Recv { buf, .. } | Op::PipeRead { buf, .. } => !buf.is_empty() && u32::try_from(buf.len()).is_ok(),
            Op::Send { bytes, from, .. } | Op::PipeWrite { bytes, from, .. } | Op::Append { bytes, from, .. } => {
                u32::try_from(bytes.len()).is_ok() && left(bytes, *from).is_some()
            }
            Op::Open { path, how, .. } => {
                let mode = match how {
                    OpenHow::Create { mode: Some(mode) } | OpenHow::CreateNoFollow { mode: Some(mode) } => {
                        *mode & !PERMISSIONS == 0
                    }
                    OpenHow::Create { mode: None }
                    | OpenHow::CreateNoFollow { mode: None }
                    | OpenHow::Read
                    | OpenHow::PathNoFollow
                    | OpenHow::ReadNoFollow
                    | OpenHow::Directory
                    | OpenHow::DirectoryNoFollow => true,
                };
                !path.contains(&0) && mode
            }
            Op::Read { buf, at, .. } => reads(buf, *at),
            Op::Write { bytes, from, at, .. } => writes(bytes, *from, *at),
            Op::Rename { from, to, .. } => is_name(from) && is_name(to),
            Op::Remove { name, .. } => is_name(name),
            Op::MakeDirectory { name, mode, .. } => is_name(name) && *mode & !PERMISSIONS == 0,
            Op::List { entries, names, .. } => {
                !entries.is_empty()
                    && u32::try_from(entries.len()).is_ok()
                    && names.len() >= LONGEST_NAME
                    && u32::try_from(names.len()).is_ok()
            }
            Op::Spawn { spawn } => valid_spawn(spawn),
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. }
            | Op::Sync { .. }
            | Op::Stat { .. }
            | Op::Wait { .. }
            | Op::Signal { .. }
            | Op::Usage
            | Op::ReadSignal { .. }
            | Op::Cancel { .. } => true,
        }
    }

    /// Whether this is an operation on files beneath a root: `Close` is on
    /// either a socket's descriptor or a file's, and `Cancel` on neither.
    #[must_use]
    pub const fn is_file(&self) -> bool {
        self.files().is_some()
    }

    /// The shape of this operation's success value.
    #[must_use]
    pub const fn shape(&self) -> Shape {
        match self {
            Op::Socket { .. } | Op::Open { .. } => Shape::Fd,
            Op::Accept { .. } => Shape::Accepted,
            Op::Recv { .. }
            | Op::Send { .. }
            | Op::Read { .. }
            | Op::Write { .. }
            | Op::Append { .. }
            | Op::List { .. }
            | Op::PipeRead { .. }
            | Op::PipeWrite { .. } => Shape::Count,
            Op::Bind { .. } => Shape::Bound,
            Op::Stat { .. } => Shape::Stat,
            Op::Spawn { .. } => Shape::Spawned,
            Op::Wait { .. } => Shape::Exit,
            Op::ReadSignal { .. } => Shape::ServiceSignal,
            Op::Usage => Shape::Usage,
            Op::Listen { .. }
            | Op::Connect { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. }
            | Op::Sync { .. }
            | Op::Rename { .. }
            | Op::Remove { .. }
            | Op::MakeDirectory { .. }
            | Op::Signal { .. }
            | Op::Cancel { .. } => Shape::Nothing,
        }
    }

    /// Which operation on files this is, if it is one.
    const fn files(&self) -> Option<Files> {
        match self {
            Op::Open { .. } => Some(Files::Open),
            Op::Read { .. } => Some(Files::Read),
            Op::Write { .. } | Op::Append { .. } => Some(Files::Write),
            Op::Sync { .. } => Some(Files::Sync),
            Op::Stat { .. } => Some(Files::Stat),
            Op::Rename { .. } => Some(Files::Rename),
            Op::Remove { .. } => Some(Files::Remove),
            Op::MakeDirectory { .. } => Some(Files::MakeDirectory),
            Op::List { .. } => Some(Files::List),
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. }
            | Op::Spawn { .. }
            | Op::Wait { .. }
            | Op::Signal { .. }
            | Op::Usage
            | Op::ReadSignal { .. }
            | Op::PipeRead { .. }
            | Op::PipeWrite { .. }
            | Op::Cancel { .. } => None,
        }
    }
}

fn valid_spawn(spawn: &Spawn) -> bool {
    if spawn.program.is_empty() || spawn.program.contains(&0) || spawn.dir.contains(&0) {
        return false;
    }
    for arg in &spawn.args {
        if arg.contains(&0) {
            return false;
        }
    }
    for env in &spawn.env {
        if env.contains(&0) || !env.contains(&b'=') {
            return false;
        }
    }
    for (at, pipe) in spawn.pipes.iter().enumerate() {
        if i32::try_from(pipe.child).is_err() || pipe.parent.is_some() {
            return false;
        }
        let Some(before) = spawn.pipes.get(..at) else {
            return false;
        };
        for prior in before {
            if prior.child == pipe.child {
                return false;
            }
        }
    }
    true
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
            Done::Stat(_) => Shape::Stat,
            Done::Spawned { .. } => Shape::Spawned,
            Done::Exit(_) => Shape::Exit,
            Done::ServiceSignal(_) => Shape::ServiceSignal,
            Done::Usage(_) => Shape::Usage,
        }
    }
}

impl Complete {
    /// Whether a backend kept the contract with this completion, given that the
    /// operation was valid: a success of the operation's shape, with a count
    /// its buffer allows, entries its names hold, or the address its `Bind`
    /// asked for; or an error the operation may answer, as the module
    /// documentation tables them. For the simulator and the conformance
    /// suite to check; io trusts its backend.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match &self.result {
            Ok(done) => done.shape() == self.kind.shape() && fits(&self.kind, done),
            Err(error) => may_fail(&self.kind, *error),
        }
    }
}

/// Whether `error` may answer `op`.
fn may_fail(op: &Op, error: Error) -> bool {
    if op.shape() == Shape::Usage {
        return only_other(error);
    }
    if let Op::Cancel { .. } = op {
        return fails_a_cancel(error);
    }
    if let Op::Spawn { spawn } = op {
        for pipe in &spawn.pipes {
            if pipe.parent.is_some() {
                return false;
            }
        }
        return error != Error::TooLate && error != Error::Cancelled;
    }
    match op.files() {
        Some(files) => fails_on_files(files, error),
        None => !fails_a_cancel_only(error) && !names_a_file(error),
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
        | Error::Cancelled
        | Error::NotFound
        | Error::Exists
        | Error::NotADirectory
        | Error::IsADirectory
        | Error::NotEmpty
        | Error::Permission
        | Error::NoSpace
        | Error::ReadOnly
        | Error::TooManyLinks
        | Error::NameTooLong
        | Error::Escape
        | Error::NotAFile => false,
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
        | Error::NotFound
        | Error::Exists
        | Error::NotADirectory
        | Error::IsADirectory
        | Error::NotEmpty
        | Error::Permission
        | Error::NoSpace
        | Error::ReadOnly
        | Error::TooManyLinks
        | Error::NameTooLong
        | Error::Escape
        | Error::NotAFile
        | Error::InvalidArgument
        | Error::Other(_) => false,
    }
}

/// Whether `error` is one only operations on files answer.
fn names_a_file(error: Error) -> bool {
    match error {
        Error::NotFound
        | Error::Exists
        | Error::NotADirectory
        | Error::IsADirectory
        | Error::NotEmpty
        | Error::Permission
        | Error::NoSpace
        | Error::ReadOnly
        | Error::TooManyLinks
        | Error::NameTooLong
        | Error::Escape
        | Error::NotAFile => true,
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
        | Error::TooLate
        | Error::InvalidArgument
        | Error::Other(_) => false,
    }
}

/// Whether `error` may answer the operation on files `op`: the table in the
/// module documentation.
fn fails_on_files(op: Files, error: Error) -> bool {
    match error {
        Error::NoBufferSpace | Error::InvalidArgument | Error::Other(_) => true,
        Error::TooManyOpenFiles | Error::Escape | Error::NotAFile => op == Files::Open,
        Error::NotFound | Error::NotADirectory | Error::NameTooLong => {
            among(op, &[Files::Open, Files::Rename, Files::Remove, Files::MakeDirectory, Files::List])
        }
        Error::Exists => among(op, &[Files::Open, Files::MakeDirectory]),
        Error::IsADirectory => among(op, &[Files::Open, Files::Read, Files::Rename, Files::Remove]),
        Error::NotEmpty => among(op, &[Files::Rename, Files::Remove]),
        Error::Permission => among(op, &[Files::Open, Files::Rename, Files::Remove, Files::MakeDirectory]),
        Error::NoSpace => among(op, &[Files::Open, Files::Write, Files::Sync, Files::Rename, Files::MakeDirectory]),
        Error::ReadOnly => among(op, &[Files::Open, Files::Write, Files::Rename, Files::Remove, Files::MakeDirectory]),
        Error::TooManyLinks => among(op, &[Files::Open, Files::Rename, Files::MakeDirectory]),
        Error::Refused
        | Error::Reset
        | Error::BrokenPipe
        | Error::NotConnected
        | Error::AddressInUse
        | Error::AddressNotAvailable
        | Error::Unreachable
        | Error::TimedOut
        | Error::TooLate => false,
        Error::Cancelled => among(op, &[Files::Open, Files::Read, Files::Write, Files::Sync]),
    }
}

fn among(op: Files, ops: &[Files]) -> bool {
    ops.contains(&op)
}

/// How many bytes of `bytes` are left from `from`, or `None` when none are.
fn left(bytes: &[u8], from: u32) -> Option<usize> {
    let from = usize::try_from(from).ok()?;
    match bytes.len().checked_sub(from) {
        Some(0) | None => None,
        Some(left) => Some(left),
    }
}

/// Whether `len` bytes from offset `at` end within the largest offset a file
/// has, `i64::MAX`: the kernel's offsets are signed, and the ring reads an
/// offset of `u64::MAX` as the descriptor's position.
fn within_offsets(at: u64, len: usize) -> bool {
    let (Ok(len), Ok(largest)) = (u64::try_from(len), u64::try_from(i64::MAX)) else {
        return false;
    };
    match at.checked_add(len) {
        Some(end) => end <= largest,
        None => false,
    }
}

/// Whether a `Read` into `buf` at `at` is valid.
fn reads(buf: &[u8], at: u64) -> bool {
    !buf.is_empty() && u32::try_from(buf.len()).is_ok() && within_offsets(at, buf.len())
}

/// Whether a `Write` of `bytes[from..]` at `at` is valid.
fn writes(bytes: &[u8], from: u32, at: u64) -> bool {
    let Some(left) = left(bytes, from) else {
        return false;
    };
    u32::try_from(bytes.len()).is_ok() && within_offsets(at, left)
}

/// Whether a success value fits the operation it answers, beyond its shape,
/// which is checked apart: a count within its buffer, entries within their
/// names, a bound address that is the one asked for.
fn fits(op: &Op, done: &Done) -> bool {
    match done {
        Done::Count(n) => match usize::try_from(*n) {
            Ok(n) => counts(op, n),
            Err(_) => false,
        },
        Done::Bound(bound) => binds(op, bound),
        Done::Stat(stat) => stat.mode & !PERMISSIONS == 0,
        Done::Spawned { .. } => match op {
            Op::Spawn { spawn } => {
                for pipe in &spawn.pipes {
                    if pipe.parent.is_none() {
                        return false;
                    }
                }
                true
            }
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
            | Op::Append { .. }
            | Op::Sync { .. }
            | Op::Stat { .. }
            | Op::Rename { .. }
            | Op::Remove { .. }
            | Op::MakeDirectory { .. }
            | Op::List { .. }
            | Op::Wait { .. }
            | Op::Signal { .. }
            | Op::PipeRead { .. }
            | Op::PipeWrite { .. }
            | Op::Usage
            | Op::ReadSignal { .. }
            | Op::Cancel { .. } => false,
        },
        Done::Nothing
        | Done::Fd(_)
        | Done::Accepted { .. }
        | Done::Exit(_)
        | Done::ServiceSignal(_)
        | Done::Usage(_) => true,
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
        | Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Append { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Cancel { .. } => false,
    }
}

/// Whether a count fits the buffer of the operation it answers: `Recv`,
/// `Send`, `Read`, `Write` and `List` have one.
fn counts(op: &Op, n: usize) -> bool {
    match op {
        Op::Recv { buf, .. } | Op::Read { buf, .. } | Op::PipeRead { buf, .. } => n <= buf.len(),
        Op::Send { bytes, from, .. }
        | Op::Write { bytes, from, .. }
        | Op::Append { bytes, from, .. }
        | Op::PipeWrite { bytes, from, .. } => match left(bytes, *from) {
            Some(left) => n >= 1 && n <= left,
            None => false,
        },
        Op::List { entries, names, .. } => match entries.get(..n) {
            Some(listed) => lists(listed, names),
            None => false,
        },
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Open { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::Cancel { .. } => false,
    }
}

/// Whether the entries a `List` counts each name an entry within `names`,
/// in order, no two sharing a byte.
fn lists(listed: &[Entry], names: &[u8]) -> bool {
    let mut end = 0_usize;
    for entry in listed {
        let Some(name) = entry.name(names) else {
            return false;
        };
        let Ok(start) = usize::try_from(entry.start) else {
            return false;
        };
        if start < end || !is_name(name) {
            return false;
        }
        let Some(next) = start.checked_add(name.len()) else {
            return false;
        };
        end = next;
    }
    true
}

fn only_other(error: Error) -> bool {
    match error {
        Error::Other(_) => true,
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
        | Error::TooLate
        | Error::NotFound
        | Error::Exists
        | Error::NotADirectory
        | Error::IsADirectory
        | Error::NotEmpty
        | Error::Permission
        | Error::NoSpace
        | Error::ReadOnly
        | Error::TooManyLinks
        | Error::NameTooLong
        | Error::Escape
        | Error::NotAFile
        | Error::InvalidArgument => false,
    }
}
