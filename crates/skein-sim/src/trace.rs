//! The trace: every submission and every completion reaped, as plain values,
//! so a failing seed can be printed and replayed (testing-strategy.md, 6).

use alloc::string::String;
use core::fmt::{self, Write};

use skein_io::kernel::{Addr, Done, Error, Family, Fd, Op, OpenHow, Signal};
use skein_lib::{Time, Token};

use crate::sim::Pid;

/// One thing that crossed the boundary, when, and in which process.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Entry {
    pub at: Time,
    pub pid: Pid,
    pub event: Event,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// A record submitted.
    Submit { op: Token, kind: Summary },
    /// A completion reaped by its process.
    Complete { op: Token, kind: Summary, result: Result<Done, Error> },
    /// A fault drawn from the seed, in the process it affects.
    Fault(Fault),
}

/// The faults of [`crate::Faults`], as the trace names them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Fault {
    Latency,
    ShortRecv,
    ShortSend,
    Reset,
    Refuse,
    NoBuffer,
    TimedOut,
    CancelRace,
    CancelUnsubmitted,
    LateReset,
    ShortRead,
    ShortWrite,
    NoSpace,
    ReadOnly,
    IoError,
    Hung,
}

/// An operation without its buffers: their lengths stand in for them, and
/// the start of each path or name for it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Summary {
    Socket {
        family: Family,
    },
    Bind {
        fd: Fd,
        addr: Addr,
    },
    Listen {
        fd: Fd,
        backlog: u32,
    },
    Accept {
        fd: Fd,
    },
    Connect {
        fd: Fd,
        addr: Addr,
    },
    Recv {
        fd: Fd,
        len: usize,
    },
    Send {
        fd: Fd,
        len: usize,
        from: u32,
    },
    Shutdown {
        fd: Fd,
    },
    Close {
        fd: Fd,
    },
    Open {
        root: Fd,
        path: Text,
        how: OpenHow,
    },
    Read {
        fd: Fd,
        len: usize,
        at: u64,
    },
    Write {
        fd: Fd,
        len: usize,
        from: u32,
        at: u64,
    },
    /// A write at a writable descriptor's position, including startup append files.
    Append {
        fd: Fd,
        len: usize,
        from: u32,
    },
    Sync {
        fd: Fd,
    },
    Stat {
        fd: Fd,
    },
    Rename {
        from_dir: Fd,
        from: Text,
        to_dir: Fd,
        to: Text,
    },
    Remove {
        dir: Fd,
        name: Text,
        directory: bool,
    },
    MakeDirectory {
        dir: Fd,
        name: Text,
        mode: u32,
    },
    List {
        fd: Fd,
        entries: usize,
        names: usize,
    },
    Spawn {
        root: Fd,
        program: Text,
        pipes: usize,
    },
    Wait {
        pidfd: Fd,
        reap: bool,
    },
    Signal {
        pidfd: Fd,
        signal: u32,
        to: skein_io::kernel::Target,
    },
    /// Process and reaped children usage.
    Usage,
    ReadSignal {
        fd: Fd,
    },
    PipeRead {
        fd: Fd,
        len: usize,
    },
    PipeWrite {
        fd: Fd,
        len: usize,
        from: u32,
    },
    Cancel {
        target: Token,
    },
}

/// The start of a path or a name, and its length: enough of it to read a
/// trace by, and small, as every entry of a trace holds a summary.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Text {
    head: [u8; Text::HEAD],
    len: u32,
}

impl Text {
    const HEAD: usize = 16;

    #[must_use]
    pub fn of(bytes: &[u8]) -> Text {
        let mut head = [0; Text::HEAD];
        for (slot, byte) in head.iter_mut().zip(bytes) {
            *slot = *byte;
        }
        Text { head, len: u32::try_from(bytes.len()).unwrap_or(u32::MAX) }
    }
}

impl fmt::Debug for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = usize::try_from(self.len).unwrap_or(usize::MAX);
        let shown = self.head.get(..len.min(Text::HEAD)).unwrap_or_default();
        write!(f, "\"{}\"", shown.escape_ascii())?;
        if len > Text::HEAD {
            write!(f, "..({len} bytes)")?;
        }
        Ok(())
    }
}

impl Summary {
    #[must_use]
    pub fn of(op: &Op) -> Summary {
        match op {
            Op::Socket { family } => Summary::Socket { family: *family },
            Op::Bind { fd, addr } => Summary::Bind { fd: *fd, addr: *addr },
            Op::Listen { fd, backlog } => Summary::Listen { fd: *fd, backlog: *backlog },
            Op::Accept { fd } => Summary::Accept { fd: *fd },
            Op::Connect { fd, addr } => Summary::Connect { fd: *fd, addr: *addr },
            Op::Recv { fd, buf } => Summary::Recv { fd: *fd, len: buf.len() },
            Op::Send { fd, bytes, from } => Summary::Send { fd: *fd, len: bytes.len(), from: *from },
            Op::Shutdown { fd } => Summary::Shutdown { fd: *fd },
            Op::Close { fd } => Summary::Close { fd: *fd },
            Op::Open { root, path, how } => Summary::Open { root: *root, path: Text::of(path), how: *how },
            Op::Read { fd, buf, at } => Summary::Read { fd: *fd, len: buf.len(), at: *at },
            Op::Write { fd, bytes, from, at } => Summary::Write { fd: *fd, len: bytes.len(), from: *from, at: *at },
            Op::Append { fd, bytes, from } => Summary::Append { fd: *fd, len: bytes.len(), from: *from },
            Op::Sync { fd } => Summary::Sync { fd: *fd },
            Op::Stat { fd } => Summary::Stat { fd: *fd },
            Op::Rename { from_dir, from, to_dir, to } => {
                Summary::Rename { from_dir: *from_dir, from: Text::of(from), to_dir: *to_dir, to: Text::of(to) }
            }
            Op::Remove { dir, name, directory } => {
                Summary::Remove { dir: *dir, name: Text::of(name), directory: *directory }
            }
            Op::MakeDirectory { dir, name, mode } => {
                Summary::MakeDirectory { dir: *dir, name: Text::of(name), mode: *mode }
            }
            Op::List { fd, entries, names } => Summary::List { fd: *fd, entries: entries.len(), names: names.len() },
            Op::Spawn { spawn } => {
                Summary::Spawn { root: spawn.root, program: Text::of(&spawn.program), pipes: spawn.pipes.len() }
            }
            Op::Wait { pidfd, reap } => Summary::Wait { pidfd: *pidfd, reap: *reap },
            Op::Signal { pidfd, signal, to } => Summary::Signal {
                pidfd: *pidfd,
                to: *to,
                signal: match signal {
                    Signal::Terminate => 15,
                    Signal::Kill => 9,
                },
            },
            Op::ReadSignal { fd } => Summary::ReadSignal { fd: *fd },
            Op::PipeRead { fd, buf } => Summary::PipeRead { fd: *fd, len: buf.len() },
            Op::PipeWrite { fd, bytes, from } => Summary::PipeWrite { fd: *fd, len: bytes.len(), from: *from },
            Op::Usage => Summary::Usage,
            Op::Cancel { target } => Summary::Cancel { target: *target },
        }
    }

    /// The descriptor the operation is on, if any: a `Cancel` is on its
    /// target, not on a descriptor; an `Open` is on its root, and a `Rename`
    /// on its first directory (see [`Summary::fds`]).
    #[must_use]
    pub const fn fd(&self) -> Option<Fd> {
        self.fds()[0]
    }

    /// Whether the operation is one on files (`Op::is_file`).
    #[must_use]
    pub const fn is_file(&self) -> bool {
        match self {
            Summary::Open { .. }
            | Summary::Read { .. }
            | Summary::Write { .. }
            | Summary::Append { .. }
            | Summary::Sync { .. }
            | Summary::Stat { .. }
            | Summary::Rename { .. }
            | Summary::Remove { .. }
            | Summary::MakeDirectory { .. }
            | Summary::List { .. } => true,
            Summary::Spawn { .. }
            | Summary::Wait { .. }
            | Summary::Usage
            | Summary::Signal { .. }
            | Summary::ReadSignal { .. }
            | Summary::PipeRead { .. }
            | Summary::PipeWrite { .. }
            | Summary::Socket { .. }
            | Summary::Bind { .. }
            | Summary::Listen { .. }
            | Summary::Accept { .. }
            | Summary::Connect { .. }
            | Summary::Recv { .. }
            | Summary::Send { .. }
            | Summary::Shutdown { .. }
            | Summary::Close { .. }
            | Summary::Cancel { .. } => false,
        }
    }

    /// Whether a `Cancel` may target the operation: any but a `Cancel`, and
    /// the operations on files that complete promptly (`Op::is_file`).
    #[must_use]
    pub const fn cancellable(&self) -> bool {
        match self {
            Summary::Cancel { .. }
            | Summary::Spawn { .. }
            | Summary::Usage
            | Summary::Signal { .. }
            | Summary::Stat { .. }
            | Summary::Rename { .. }
            | Summary::Remove { .. }
            | Summary::MakeDirectory { .. }
            | Summary::List { .. } => false,
            Summary::Socket { .. }
            | Summary::Bind { .. }
            | Summary::Listen { .. }
            | Summary::Accept { .. }
            | Summary::Connect { .. }
            | Summary::Recv { .. }
            | Summary::Send { .. }
            | Summary::Shutdown { .. }
            | Summary::Close { .. }
            | Summary::Open { .. }
            | Summary::Read { .. }
            | Summary::Write { .. }
            | Summary::Append { .. }
            | Summary::Sync { .. }
            | Summary::Wait { .. }
            | Summary::PipeRead { .. }
            | Summary::ReadSignal { .. }
            | Summary::PipeWrite { .. } => true,
        }
    }

    /// Every descriptor the operation is on: a `Rename` is on both its
    /// directories.
    #[must_use]
    pub const fn fds(&self) -> [Option<Fd>; 2] {
        match self {
            Summary::Bind { fd, .. }
            | Summary::Listen { fd, .. }
            | Summary::Accept { fd }
            | Summary::Connect { fd, .. }
            | Summary::Recv { fd, .. }
            | Summary::Send { fd, .. }
            | Summary::Shutdown { fd }
            | Summary::Close { fd }
            | Summary::Open { root: fd, .. }
            | Summary::Read { fd, .. }
            | Summary::Write { fd, .. }
            | Summary::Append { fd, .. }
            | Summary::Sync { fd }
            | Summary::Stat { fd }
            | Summary::Remove { dir: fd, .. }
            | Summary::MakeDirectory { dir: fd, .. }
            | Summary::List { fd, .. }
            | Summary::Spawn { root: fd, .. }
            | Summary::Wait { pidfd: fd, .. }
            | Summary::Signal { pidfd: fd, .. }
            | Summary::ReadSignal { fd }
            | Summary::PipeRead { fd, .. }
            | Summary::PipeWrite { fd, .. } => [Some(*fd), None],
            Summary::Rename { from_dir, to_dir, .. } => [Some(*from_dir), Some(*to_dir)],
            Summary::Usage | Summary::Socket { .. } | Summary::Cancel { .. } => [None, None],
        }
    }
}

/// The trace as text, one entry a line, under the seed that replays it.
#[must_use]
#[expect(clippy::use_debug, reason = "the trace is printed for a person reading a failure")]
pub fn render(seed: u64, entries: &[Entry]) -> String {
    let mut out = String::new();
    writeln!(out, "skein-sim seed {seed}").expect("writing to a String");
    for entry in entries {
        writeln!(out, "{:>12} ns  {}  {:?}", entry.at.as_nanos(), entry.pid, entry.event).expect("writing to a String");
    }
    out
}
