//! The conformance suite (overview.md, section 9; testing-pyramid.md,
//! section 5): scripted sequences of `skein_io::kernel` records that run
//! against any [`Backend`], each returning what it saw, and a [`Check`] of
//! that against the rules of the contract.
//!
//! `skein-sim`'s tests run every scenario against the simulator over many
//! seeds, calm and with the faults loopback can show ([`loopback_chaos`]);
//! `skein-shell`'s run them against the ring on the real kernel. Where the
//! kernel answers one way, a check demands that answer; where the
//! simulator draws among answers the contract allows (a short send, a
//! cancel that loses its race), it accepts each of them, and the kernel's
//! must be one.
//!
//! The faults beyond loopback (refused, reset and timed-out connections,
//! buffer exhaustion, a cancel the backend cannot submit) and the
//! descriptor limit cannot be provoked on the ring. The scenarios that need
//! them say so, and run on the simulator only.
//!
//! A public module rather than one behind a feature: the crate is test
//! support already, and a feature would have to be turned on by the
//! crate's own tests through a dependency on itself.

mod run;
mod scenarios;

use skein_io::kernel::{Complete, Submit};
use skein_lib::{Duration, Queue, Time};

use crate::config::{Config, Faults};
use crate::sim::{Pid, Sim};

pub use scenarios::{
    AddressInUse, Backpressure, Cancelling, ClosedBeforeAccept, DescriptorLimit, FullQueue, GracefulClose, Ipv6Only,
    Lifecycle, PeerClosed, Refused, ResetAfterEnd, UnreadClose, WrongState, accept_past_the_descriptor_limit,
    address_in_use, backpressure, cancel_accept, cancel_accept_racing_a_connect, cancel_connect, cancel_recv,
    cancel_recv_racing_bytes, closed_before_accept, full_accept_queue, graceful_close, ipv6_only, lifecycle, refused,
    reset_after_end_of_stream, send_after_peer_closed, unread_close_meets_recv, unread_close_meets_send, wrong_state,
};

/// What the suite drives: the calls the shell's `Kernel` offers, per
/// process, and a way to let time pass.
pub trait Backend {
    /// A process with descriptors of its own: on the ring, a `Kernel`; in
    /// the simulator, a [`Pid`].
    type Process: Copy + Ord + core::fmt::Debug;

    fn open(&mut self) -> Self::Process;

    /// Takes every record of `records`, in order.
    fn submit(&mut self, process: Self::Process, records: &mut Queue<Submit>);

    /// Moves the completions delivered to `process` into `completions`, as
    /// many as fit.
    fn reap(&mut self, process: Self::Process, completions: &mut Queue<Complete>);

    /// The backend's monotonic time.
    fn now(&self) -> Time;

    /// Lets time pass until something may have completed for `process`, or
    /// for at most `bound`.
    fn pass(&mut self, process: Self::Process, bound: Duration);

    /// Fails unless `process` has nothing in flight, as the backend sees it.
    fn assert_settled(&self, process: Self::Process);
}

/// What a scenario saw, checked against the rules of the contract it
/// exercises.
pub trait Check {
    /// Panics, naming the rule, on what the contract does not allow.
    fn check(&self);
}

impl Backend for Sim {
    type Process = Pid;

    fn open(&mut self) -> Pid {
        self.spawn_process()
    }

    fn submit(&mut self, process: Pid, records: &mut Queue<Submit>) {
        Sim::submit(self, process, records);
    }

    fn reap(&mut self, process: Pid, completions: &mut Queue<Complete>) {
        Sim::reap(self, process, completions);
    }

    fn now(&self) -> Time {
        Sim::now(self)
    }

    /// Moves to what is next due, or by `bound` when that is later or the
    /// world is idle.
    fn pass(&mut self, _process: Pid, bound: Duration) {
        let until = self.now().saturating_add(bound);
        match self.next_due() {
            Some(at) if at < until => self.advance_to(at),
            Some(_) | None => self.advance_to(until),
        }
    }

    fn assert_settled(&self, process: Pid) {
        self.assert_quiescent(process);
        self.assert_no_open_fds(process);
    }
}

/// The chaos loopback can show: [`Config::chaos`]'s small buffers and
/// short backlog, latency, short receives and sends, and raced cancels,
/// without the faults that model a network beyond it.
#[must_use]
pub const fn loopback_chaos() -> Config {
    let chaos = Config::chaos();
    let faults = Faults { reset: 0, refuse: 0, no_buffer: 0, timed_out: 0, cancel_unsubmitted: 0, ..chaos.faults };
    Config { faults, ..chaos }
}
