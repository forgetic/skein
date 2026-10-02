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
//!   due (a late completion, a raced cancel), [`Sim::advance_to`] to a
//!   process's deadline. When nothing is due, the world is idle.
//! - [`Sim::assert_quiescent`] and [`Sim::assert_no_open_fds`] check a
//!   process at the end; [`Sim::render_trace`] prints the run with its seed.
//!
//! # What it checks
//!
//! Every broken invariant of the contract fails the world at submit, with a
//! panic naming the seed and the end of the trace: an invalid record, a token
//! already in flight, a cancel of a cancel, an address of the wrong family,
//! two receives, sends or accepts on one descriptor or anything beside a
//! connect, anything but a close after a failed connect, a shutdown during a
//! send, a close beside anything, and any operation on a descriptor the
//! process does not have open. Every completion it makes is checked with
//! `Complete::is_valid`, and each token completes once.
//!
//! # Its choices, where the contract leaves one
//!
//! - **Descriptors** count up from 3 in each process and are never reused.
//! - **Effects happen when the operation is decided,** at submit or when a
//!   waiting operation can proceed; latency delays only the delivery of the
//!   completion. A completion decided but not reaped is still in flight.
//! - **A cancel** of an operation still waiting wins (`Ok(Nothing)`, the
//!   target `Err(Cancelled)`, in a random order). Of one already decided, or
//!   of a token not in flight, it is `Err(TooLate)`. With the `cancel_race`
//!   fault it lands late, so the target may be decided first.
//! - **Ports:** port 0 picks a free port of the ephemeral range from a random
//!   start. Only a listener on an overlapping address clashes, as
//!   `SO_REUSEADDR` makes it on Linux. The unspecified address binds, and
//!   listens on every loopback address of its family; a connection's source
//!   is `127.0.0.1` or `::1`.
//! - **Buffers:** one receive buffer per socket, of `Config::buffer` bytes;
//!   sent bytes land in the peer's at once.
//! - **A reset** leaves the bytes already received readable (as Linux does);
//!   the reset is reported after them to a `Recv`, or at once to a `Send`.
//! - **Sending to a peer that closed** with nothing unread succeeds once, the
//!   bytes lost, and fails with `BrokenPipe` after (Linux's answer to the
//!   peer's reset in `CLOSE_WAIT`).
//! - **Records the kernel refuses for the socket's state:** `Recv` and
//!   `Shutdown` on an unconnected socket fail with `NotConnected`, `Send`
//!   with `BrokenPipe`; `Bind`, `Listen`, `Accept` and `Connect` with
//!   `InvalidArgument`. `Listen` on an unbound socket is refused too.

#![forbid(unsafe_code)]

extern crate alloc;

mod config;
mod net;
mod sim;
mod trace;

pub use config::{Config, Faults};
pub use sim::{Pid, Sim};
pub use trace::{Entry, Event, Summary, render};
