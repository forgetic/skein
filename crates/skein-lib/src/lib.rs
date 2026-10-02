//! The building blocks every step crate shares (programming-style.md, 9.2):
//! typed handles and the slabs that issue them, bounded queues, lists, maps,
//! sets and stacks, the per-layer deadline table, the stream vocabulary and
//! the intake that meets its demands, a reader and a writer for sized bytes
//! and the decimal digits of a count to write with them, monotonic and wall
//! time, randomness, the tokens that cross layer boundaries, and the
//! environment a step reads.
//!
//! Application code does not hand-roll data structures: what is missing goes
//! here, written once and tested hard.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod btree;
pub mod bytes;
mod deadlines;
mod decimal;
mod env;
mod id;
mod intake;
mod list;
mod map;
mod queue;
mod reader;
mod rng;
mod set;
mod slab;
mod stack;
pub mod stream;
mod time;
mod token;
mod writer;

pub use deadlines::Deadlines;
pub use decimal::Decimal;
pub use env::Env;
pub use id::Id;
pub use intake::Intake;
pub use list::List;
pub use map::Map;
pub use queue::Queue;
pub use reader::Reader;
pub use rng::Rng;
pub use set::Set;
pub use slab::Slab;
pub use stack::Stack;
pub use time::{Duration, Time, Wall};
pub use token::{ReplyTo, Token};
pub use writer::{Overflow, Writer};
