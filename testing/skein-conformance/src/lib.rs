//! The conformance suite (kernel.md, 8; testing-strategy.md, 5): scripted
//! sequences of `skein_io::kernel` records that run against any [`Backend`],
//! each returning what it saw, and a [`Check`] of that against the rules of
//! the contract.
//!
//! `tests/conformance/sim` runs every scenario against the simulator over
//! many seeds, calm and with the faults loopback can show;
//! `tests/conformance/ring` runs them against the ring on the real kernel.
//! Each implements [`Backend`] there. Where the kernel answers one way, a
//! check demands that answer; where the simulator draws among answers the
//! contract allows (a short send, a cancel that loses its race), it accepts
//! each of them, and the kernel's must be one.
//!
//! The scenarios on files run beneath a root each lays out for itself
//! ([`Item`]): a scratch directory on the ring, the minimal fake machine in
//! the simulator (testing.md, 4).
//!
//! The faults beyond loopback (refused, reset and timed-out connections,
//! buffer exhaustion, a cancel the backend cannot submit), those of a disk
//! beyond a healthy one, and the descriptor limit cannot be provoked on the
//! ring. The scenarios that need them say so, and run on the simulator
//! only.
//!
//! Ordinary Rust (programming-model.md, 10.2), as the simulator is: it runs
//! in tests only.

#![forbid(unsafe_code)]

extern crate alloc;

mod files;
mod processes;
mod run;
mod scenarios;

use alloc::vec::Vec;

use skein_io::kernel::{Complete, Fd, Submit};
use skein_lib::{Duration, Queue, Time};

pub use files::{
    Entries, Escapes, FileLifecycle, Listing, MakeDirectories, Nested, OpenLimit, Permissions, Removes, Renames,
    Shortness, cancel_read, escapes, file_lifecycle, list, make_directory, nested_roots,
    open_past_the_descriptor_limit, permissions, remove, rename,
};
pub use processes::{Processes, processes};

pub use scenarios::{
    AddressInUse, Backpressure, Cancelling, ClosedBeforeAccept, DescriptorLimit, FullQueue, GracefulClose, Ipv6Only,
    Lifecycle, ListenerClosed, Pairing, PeerClosed, Race, Refused, ResetAfterEnd, StoppedConnect, Target, UnreadClose,
    WrongState, accept_past_the_descriptor_limit, address_in_use, backpressure, cancel_accept,
    cancel_accept_racing_a_connect, cancel_connect, cancel_connect_established_while_away, cancel_recv,
    cancel_recv_racing_bytes, closed_before_accept, full_accept_queue, graceful_close, ipv6_only, lifecycle,
    listener_close_resets_waiting, refused, reset_after_end_of_stream, send_after_peer_closed, unread_close_meets_recv,
    unread_close_meets_send, wrong_state,
};

/// What the suite drives: the calls the shell's `Kernel` offers, per
/// process, and a way to let time pass.
pub trait Backend {
    /// A process with descriptors of its own: on the ring, a `Kernel`; in
    /// the simulator, a `Pid`.
    type Process: Copy + Ord + core::fmt::Debug;

    fn open(&mut self) -> Self::Process;

    /// Takes every record of `records`, in order.
    fn submit(&mut self, process: Self::Process, records: &mut Queue<Submit>);

    /// Moves the completions delivered to `process` into `completions`, as
    /// many as fit.
    fn reap(&mut self, process: Self::Process, completions: &mut Queue<Complete>);

    /// The backend's monotonic time.
    fn now(&self) -> Time;

    /// `process` enters the kernel without submitting or waiting, as a
    /// loop's empty submit does: its waiting operations that can proceed
    /// are decided.
    fn enter(&mut self, process: Self::Process);

    /// Lets time pass until something may have completed for `process`, or
    /// for at most `bound`, every process entering the kernel first, as a
    /// world's loops would.
    fn pass(&mut self, process: Self::Process, bound: Duration);

    /// Lets `span` pass with no process entering the kernel: what waits on
    /// a process stays undecided.
    fn sleep(&mut self, span: Duration);

    /// Fails unless `process` has nothing in flight, as the backend sees it.
    fn assert_settled(&self, process: Self::Process);

    /// A new root for `process`, laid out as `tree` says, opened as the
    /// shell opens a root at startup (kernel.md, 6.1): a descriptor the
    /// scenario closes with a `Close`.
    fn root(&mut self, process: Self::Process, tree: &[Item]) -> Fd;
}

/// One thing a scenario's root holds when it starts: its path, relative to
/// the root, whose directories come earlier in the tree; what it is; and
/// its mode, of which the owner's bits count, given once the whole tree is
/// laid. The suite's own vocabulary: each backend lays it out its own way.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Item {
    pub path: Vec<u8>,
    pub made: Made,
    pub mode: u32,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Made {
    File(Vec<u8>),
    Directory,
    /// A symbolic link, to the path it holds.
    Link(Vec<u8>),
    /// A FIFO.
    Fifo,
}

impl Item {
    /// A file holding `bytes`, mode `0o644`.
    #[must_use]
    pub fn file(path: &[u8], bytes: &[u8]) -> Item {
        Item { path: path.to_vec(), made: Made::File(bytes.to_vec()), mode: 0o644 }
    }

    /// A directory, mode `0o755`.
    #[must_use]
    pub fn directory(path: &[u8]) -> Item {
        Item { path: path.to_vec(), made: Made::Directory, mode: 0o755 }
    }

    /// A symbolic link to `target`, whose mode does not count.
    #[must_use]
    pub fn link(path: &[u8], target: &[u8]) -> Item {
        Item { path: path.to_vec(), made: Made::Link(target.to_vec()), mode: 0o777 }
    }

    /// A FIFO, mode `0o644`.
    #[must_use]
    pub fn fifo(path: &[u8]) -> Item {
        Item { path: path.to_vec(), made: Made::Fifo, mode: 0o644 }
    }

    /// The same, with `mode`.
    #[must_use]
    pub fn mode(self, mode: u32) -> Item {
        Item { mode, ..self }
    }
}

/// What a scenario saw, checked against the rules of the contract it
/// exercises.
pub trait Check {
    /// Panics, naming the rule, on what the contract does not allow.
    fn check(&self);
}
