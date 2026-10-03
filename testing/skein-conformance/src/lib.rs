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
//! The faults beyond loopback (refused, reset and timed-out connections,
//! buffer exhaustion, a cancel the backend cannot submit) and the
//! descriptor limit cannot be provoked on the ring. The scenarios that need
//! them say so, and run on the simulator only.
//!
//! Ordinary Rust (programming-model.md, 10.2), as the simulator is: it runs
//! in tests only.

#![forbid(unsafe_code)]

extern crate alloc;

mod run;
mod scenarios;

use skein_io::kernel::{Complete, Submit};
use skein_lib::{Duration, Queue, Time};

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
}

/// What a scenario saw, checked against the rules of the contract it
/// exercises.
pub trait Check {
    /// Panics, naming the rule, on what the contract does not allow.
    fn check(&self);
}
