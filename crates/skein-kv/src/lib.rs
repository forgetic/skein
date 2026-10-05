//! A bounded ordered byte-key store. Owners supply record versions and key
//! spaces; the store knows only opaque keys, values and commit numbers.
//!
//! A store holds its map in memory. Commits are framed into an append-only
//! log and are visible only after the log's sync succeeds. Recovery accepts
//! the longest valid, contiguous prefix of frames after a snapshot.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod crc;
mod frame;
mod key;
mod limits;
mod map;
mod snapshot;
mod store;
#[cfg(test)]
mod tests;

pub use boundary::{Event, Failure, Op, Page, Range, Refusal, Request, Row};
pub use key::{KeyReader, KeyWriter};
pub use limits::{Limits, worst_case};
pub use store::{Store, down, fire, is_ready, up};

/// The most a step emits into each queue.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MaxOut {
    pub above: u32,
    pub below: u32,
}

pub const MAX_OUT: MaxOut = MaxOut { above: 64, below: 2 };
