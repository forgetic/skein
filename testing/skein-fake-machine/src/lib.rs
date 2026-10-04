//! skein's minimal fake machine (testing.md, 4; testing-strategy.md, 4.3):
//! a few files beneath a root, for skein's own worlds over the simulator,
//! which plays the kernel and passes the operations on files to the
//! machine the world owns (simulator.md, 3). Files only, for now: the
//! programs come with processes.
//!
//! - [`Machine`] is the filesystem, in its own vocabulary: directories,
//!   files and symbolic links, each with its owner's permissions; handles
//!   to what is open; paths resolved beneath a root as `openat2` resolves
//!   them with `RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS`. It refuses what a
//!   real filesystem refuses ([`Refusal`]), in the order Linux checks, and
//!   asserts what its client, the simulator, guarantees: a handle it issued
//!   and has not closed.
//! - [`lay`](Machine::lay) makes a new root laid out as a scenario's
//!   [`Item`]s say, and opens it, as the shell opens a root at startup.
//! - [`step`] is its face behind the simulator: one call of the seam in, one
//!   answer out, translated from and to the simulator's vocabulary, the
//!   shape of a step machine; [`serve`] is what a world does after each
//!   submit, every call waiting answered.
//!
//! A service's fake machine is the service's own (README.md).
//!
//! Ordinary Rust (programming-model.md, 10.2): it runs in tests only.

#![forbid(unsafe_code)]

extern crate alloc;

mod face;
mod fs;
#[cfg(test)]
mod tests;

pub use face::{serve, step};
pub use fs::{Facts, How, Is, Item, Listed, Machine, Made, Opened, Refusal};
