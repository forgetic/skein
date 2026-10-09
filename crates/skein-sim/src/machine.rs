//! The machine seam (simulator.md, 3): operations on files and program
//! selection passed to the embedder's fake machine, and its answers. Both
//! are data. The world owns the
//! simulator and the machine, takes the calls from [`Sim::calls`], hands
//! each to its machine, and gives the answers back with [`Sim::answer`],
//! as it moves records between its processes and the simulator: nothing
//! calls into the machine, and nothing in the machine calls back.
//!
//! The simulator keeps what is the kernel's: descriptors, which it maps to
//! the machine's handles; the checks of the contract; and the faults it
//! draws, before a call or instead of one (a short read asks the machine
//! for fewer bytes; a failure asks it nothing). The machine keeps what is
//! the filesystem's: what lies beneath its roots, and what each handle has
//! open. It answers every call, exactly as asked, once.
//!
//! [`Sim::calls`]: crate::Sim::calls
//! [`Sim::answer`]: crate::Sim::answer

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_io::kernel::{Error, Kind, OpenHow, Pipe, Stat};

/// The machine's name for a file or a directory it opened, which the
/// simulator holds behind a process's descriptor. Only the machine gives
/// it meaning, and it issues each once.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Handle(u64);

impl Handle {
    #[must_use]
    pub const fn new(raw: u64) -> Handle {
        Handle(raw)
    }

    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// The simulator's name for one call, which the answer echoes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Ticket(pub(crate) u64);

/// An operation on files, for the machine to answer.
#[derive(PartialEq, Eq, Debug)]
pub struct Call {
    pub ticket: Ticket,
    pub ask: Ask,
}

/// What the machine is asked, in its handles and with the bytes it needs.
/// Each is answered with the [`Reply`] it names, or an error the
/// operation may answer (`skein_io::kernel`'s table).
#[derive(PartialEq, Eq, Debug)]
pub enum Ask {
    /// Identify a program beneath `root`, in `dir`, and start its modeled
    /// behavior with exactly the requested child descriptors.
    Spawn {
        root: Handle,
        program: Box<[u8]>,
        args: Box<[Box<[u8]>]>,
        env: Box<[Box<[u8]>]>,
        dir: Box<[u8]>,
        pipes: Box<[Pipe]>,
    },
    /// `path`, resolved beneath the directory `root` names as `openat2`
    /// resolves with `RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS`, opened as
    /// `how` says: [`Reply::Opened`], a handle never issued before.
    Open { root: Handle, path: Box<[u8]>, how: OpenHow },
    /// The file's bytes from `at`, `len` of them or as many as there are:
    /// [`Reply::Read`], empty at or past its end.
    Read { file: Handle, at: u64, len: u32 },
    /// `bytes`, every one, written at `at`: [`Reply::Done`].
    Write { file: Handle, at: u64, bytes: Box<[u8]> },
    /// [`Reply::Done`].
    Sync { file: Handle },
    /// [`Reply::Stat`].
    Stat { file: Handle },
    /// [`Reply::Done`].
    Rename { from_dir: Handle, from: Box<[u8]>, to_dir: Handle, to: Box<[u8]> },
    /// [`Reply::Done`].
    Remove { dir: Handle, name: Box<[u8]>, directory: bool },
    /// [`Reply::Done`].
    MakeDirectory { dir: Handle, name: Box<[u8]> },
    /// The next entries of the directory, from where the last `List` of
    /// this handle stopped: at most `most`, their names `room` bytes
    /// together at most, and at least one while any is left. `room` is at
    /// least the longest name: [`Reply::Listed`].
    List { dir: Handle, most: u32, room: u32 },
    /// The handle is closed, and forgotten: [`Reply::Done`].
    Close { file: Handle },
}

/// The machine's answer to the call its ticket names.
#[derive(PartialEq, Eq, Debug)]
pub struct Answer {
    pub ticket: Ticket,
    pub result: Result<Reply, Error>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Reply {
    Program(Program),
    Opened(Handle),
    Read(Box<[u8]>),
    Stat(Stat),
    /// Each entry's kind and name, in the order to hand them back.
    Listed(Vec<(Kind, Box<[u8]>)>),
    Done,
}

/// A fake program's behavior. The machine selects it; the simulator runs
/// it against the child's pipes and keeps the process lifecycle.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Program {
    /// Copy bytes from one child descriptor to another until input ends.
    Echo { input: u32, output: u32 },
    /// Exit at startup with the given code.
    Exit(u8),
    /// Stay alive until signalled.
    Never,
    /// Start a child that holds the output pipes in the leader's group; optionally exit the leader.
    Fork { exit_leader: bool },
    /// Run a service hosted by the world in a process bound to these pipes.
    Service,
}
