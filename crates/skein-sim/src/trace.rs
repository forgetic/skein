//! The trace: every submission and every completion reaped, as plain values,
//! so a failing seed can be printed and replayed (testing-pyramid.md, 6).

use alloc::string::String;
use core::fmt::Write;

use skein_io::kernel::{Addr, Done, Error, Family, Fd, Op};
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
}

/// An operation without its buffers: their lengths stand in for them.
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
    Cancel { target: Token },
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
            Op::Cancel { target } => Summary::Cancel { target: *target },
        }
    }

    /// The descriptor the operation is on, if any: a `Cancel` is on its
    /// target, not on a descriptor.
    #[must_use]
    pub const fn fd(&self) -> Option<Fd> {
        match self {
            Summary::Bind { fd, .. }
            | Summary::Listen { fd, .. }
            | Summary::Accept { fd }
            | Summary::Connect { fd, .. }
            | Summary::Recv { fd, .. }
            | Summary::Send { fd, .. }
            | Summary::Shutdown { fd }
            | Summary::Close { fd } => Some(*fd),
            Summary::Socket { .. } | Summary::Cancel { .. } => None,
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
