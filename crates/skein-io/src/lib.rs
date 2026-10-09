//! io, the lowest step layer of every service (io.md): it owns the sockets
//! and the operations in flight on them, turns requests about its entities
//! into kernel records (`kernel`) and completions into events, and absorbs
//! the mechanics in between: buffers in flight, short transfers, cancels and
//! settling, graceful close, the accept batch.
//!
//! Built for sockets (io.md, 3), child and inherited one-way pipe streams
//! (io.md, 3 and 6), and bounded file operations (io.md, 5). Service
//! Termination signals arrive through an adopted signalfd (io.md, 7).
//!
//! # Driving it
//!
//! An [`Io`] made from its [`Limits`], and four entry points, called by the
//! service's loop (programming-model.md, section 2), each emitting at most
//! its `MAX_OUT` into the queues the loop reserved room in:
//!
//! - in the up pass, [`resume`] while [`Io::is_ready`], then [`up`] for each
//!   completion the kernel handed back, then [`fire`] while a close deadline
//!   [`Io::is_due`];
//! - in the down pass, [`down`] for each request, while [`Io::takes`] one;
//! - at the reclaim point, [`Io::reclaim`].
//!
//! The records for the kernel go into the loop's `Queue<Submit>`; the
//! kernel's completions come back as `Complete`s. [`worst_case`] is io's part
//! of the service's memory, and [`operations`] the size of the ring.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod digest;
pub mod file;
pub mod file_layer;
pub mod kernel;
mod layer;
mod limits;
mod listener;
mod output;
mod pipe;
mod process;
mod records;
mod signals;
mod stream;
#[cfg(test)]
mod tests;

pub use layer::{Io, down, fire, resume, up};
pub use limits::{Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, operations, worst_case};
pub use records::{Error, Event, Measured, Request};
