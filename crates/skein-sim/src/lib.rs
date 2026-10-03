//! The simulated kernel (overview.md, sections 7.2 and 9): a backend that
//! answers the records of `skein_io::kernel` as its contract allows, for
//! every process of one world, with every choice drawn from one seed.
//!
//! Ordinary Rust (programming-style.md, 9.2, last part): std is in, but no
//! clock, no thread, no OS randomness and no hash map with a random seed, so
//! a seed replays to the same [`trace`](Sim::trace).
//!
//! # Driving it
//!
//! - [`Sim::new`] makes a world from a seed and a [`Config`]
//!   ([`Config::calm`] is a well-behaved network, [`Config::chaos`] turns on
//!   every fault). [`Sim::spawn_process`] adds a process.
//! - Per process, the two calls the shell's `Kernel` offers a service:
//!   [`Sim::submit`] takes every record of a `Queue<Submit>`, and
//!   [`Sim::reap`] moves delivered completions into a `Queue<Complete>`, up
//!   to its room.
//! - Time moves only when asked: [`Sim::advance`] jumps to the next thing
//!   due (a late completion, a raced cancel, a connect's SYN timeout),
//!   [`Sim::advance_to`] to a given instant. When nothing is due, the world
//!   is idle. A world that hosts services moves time to the earlier of
//!   [`Sim::next_due`] and their earliest deadline.
//! - [`Sim::assert_quiescent`] and [`Sim::assert_no_open_fds`] check a
//!   process at the end; [`Sim::render_trace`] prints the run with its seed,
//!   every fault drawn included.
//!
//! # What it checks
//!
//! Every broken invariant of the contract fails the world at submit, with a
//! panic naming the seed and the end of the trace, as does any operation on a
//! descriptor the process does not have open. Every completion it makes is
//! checked with `Complete::is_valid`, and each token completes once.
//!
//! # Its choices, where the contract leaves one
//!
//! - **Descriptors** count up from 3 in each process and are never reused,
//!   up to `Config::max_fds` open at once.
//! - **Effects happen when the operation is decided:** at submit, or for a
//!   waiting operation once it can proceed and its process is in the
//!   kernel (its `submit`, after the records, or its `reap`), as the ring
//!   runs completions only when the loop enters it. Until then a `Cancel`
//!   still stops it. What the network does meanwhile (bytes landing in a
//!   buffer, a connection established into a queue) happens at once.
//!   Latency delays only the delivery of the completion. A completion
//!   decided but not reaped is still in flight.
//! - **A cancel** of an operation still waiting wins (`Ok(Nothing)`, the
//!   target `Err(Cancelled)`, in a random order). Of one already decided, or
//!   of a token not in flight, it is `Err(TooLate)`. With the `cancel_race`
//!   fault it lands late: the target may be decided first, or be interrupted
//!   (`TooLate`, and the target `Cancelled`). With `cancel_unsubmitted` it
//!   fails with `Other(11)` and the target runs on.
//! - **Ports:** port 0 picks a free port of the ephemeral range from a random
//!   start. Only a listener on an overlapping address clashes, as
//!   `SO_REUSEADDR` makes it on Linux. The unspecified address binds, and
//!   listens on every loopback address of its family; a connection's source
//!   is `127.0.0.1` or `::1`.
//! - **Buffers:** one receive buffer per socket, of `Config::buffer` bytes;
//!   sent bytes land in the peer's at once.
//! - **Faults beyond loopback:** `refuse`, `reset` and `timed_out` model a
//!   remote network, which loopback never is. A timed-out end reports
//!   `TimedOut`; its peer sees a reset.
#![forbid(unsafe_code)]

extern crate alloc;

mod config;
mod net;
mod sim;
mod trace;

pub use config::{Config, Faults};
pub use sim::{Pid, Sim};
pub use trace::{Entry, Event, Fault, Summary, render};
