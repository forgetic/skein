//! The echo example's protocol layer (examples.md, 3): it translates
//! between io's sockets and the domain's sessions, and decides nothing about
//! the domain (programming-model.md, 4). It owns the listener and the
//! connections; reads each line with a scan to `\n`, under the line limit,
//! into a call to the domain; writes each answer back; and keeps the
//! mechanics of the wire: room asked for before the next line is read, the
//! idle deadline, the refusals, and the close.
//!
//! # Driving it
//!
//! A [`Protocol`] made from its [`Limits`], the address to listen at and a
//! seed, and four entry points, called by the service's loop
//! (programming-model.md, section 2), each emitting at most its `MAX_OUT`
//! into the queues the loop reserved room in:
//!
//! - in the up pass, [`resume`] while [`Protocol::is_ready`], then [`up`]
//!   for each event io told, then [`fire`] while an idle deadline
//!   [`Protocol::is_due`];
//! - in the down pass, [`down`] for each request of the domain's;
//! - at the reclaim point, [`Protocol::reclaim`].
//!
//! Events up are the domain's calls and events; requests down are io's.
//! [`worst_case`] is the layer's part of the service's memory.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod conn;
mod layer;
mod limits;
#[cfg(test)]
mod tests;

pub use layer::{Protocol, down, fire, resume, up};
pub use limits::{BUSY, Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, TOO_LONG, worst_case};
