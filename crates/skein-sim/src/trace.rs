//! The trace: every submission and every completion reaped, as plain values,
//! so a failing seed can be printed and replayed (testing-strategy.md, 6).

use alloc::string::String;
use core::fmt::{self, Write};

use skein_io::kernel::{Addr, Done, Error, Family, Fd, Op, OpenHow};
use skein_lib::{Time, Token};

use crate::sim::Pid;

/// One thing that crossed the boundary, when, and in which process.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Entry {
    pub at: Time,
    pub pid: Pid,
    pub event: Event,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
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
}

/// An operation without its buffers: their lengths stand in for them, and
/// the start of each path or name for it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Summary {
    Socket { family: Family },
    Bind { fd: Fd, addr: Addr },
    Listen { fd: Fd, backlog: u32 },
    Accept { fd: Fd },
    Connect { fd: Fd, addr: Addr },
    Recv { fd: Fd, len: usize },
    Send { fd: Fd, len: usize, from: u32 },
    Shutdown { fd: Fd },
    Close { fd: Fd },
    Open { root: Fd, path: Text, how: OpenHow },
    Read { fd: Fd, len: usize, at: u64 },
    Write { fd: Fd, len: usize, from: u32, at: u64 },
    Sync { fd: Fd },
    Stat { fd: Fd },
    Rename { from_dir: Fd, from: Text, to_dir: Fd, to: Text },
    Remove { dir: Fd, name: Text, directory: bool },
    MakeDirectory { dir: Fd, name: Text },
    List { fd: Fd, entries: usize, names: usize },
    Cancel { target: Token },
}

/// The start of a path or a name, and its length: enough of it to read a
/// trace by.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Text {
    head: [u8; Text::HEAD],
    len: usize,
}

impl Text {
    const HEAD: usize = 32;

    #[must_use]
    pub fn of(bytes: &[u8]) -> Text {
        let mut head = [0; Text::HEAD];
        for (slot, byte) in head.iter_mut().zip(bytes) {
            *slot = *byte;
        }
        Text { head, len: bytes.len() }
    }
}

impl fmt::Debug for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shown = self.head.get(..self.len.min(Text::HEAD)).unwrap_or_default();
        write!(f, "\"{}\"", shown.escape_ascii())?;
        if self.len > Text::HEAD {
            write!(f, "..({} bytes)", self.len)?;
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
            Op::Sync { fd } => Summary::Sync { fd: *fd },
            Op::Stat { fd } => Summary::Stat { fd: *fd },
            Op::Rename { from_dir, from, to_dir, to } => {
                Summary::Rename { from_dir: *from_dir, from: Text::of(from), to_dir: *to_dir, to: Text::of(to) }
            }
            Op::Remove { dir, name, directory } => {
                Summary::Remove { dir: *dir, name: Text::of(name), directory: *directory }
            }
            Op::MakeDirectory { dir, name } => Summary::MakeDirectory { dir: *dir, name: Text::of(name) },
            Op::List { fd, entries, names } => Summary::List { fd: *fd, entries: entries.len(), names: names.len() },
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
            | Summary::Sync { fd }
            | Summary::Stat { fd }
            | Summary::Remove { dir: fd, .. }
            | Summary::MakeDirectory { dir: fd, .. }
            | Summary::List { fd, .. } => [Some(*fd), None],
            Summary::Rename { from_dir, to_dir, .. } => [Some(*from_dir), Some(*to_dir)],
            Summary::Socket { .. } | Summary::Cancel { .. } => [None, None],
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
