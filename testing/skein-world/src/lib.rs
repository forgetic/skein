//! The world harness (testing-strategy.md, 7; examples.md, 6): skein's,
//! since its examples are the second user of a world. One thread, one loop,
//! driving every process of a world through its own `iterate`, with a
//! referee beside them, and checking as it goes:
//!
//! - [`Host`]: what the harness needs of a process, a service or a fake,
//!   each following the programming model;
//! - [`Referee`], [`Expectation`] and [`Expectations`]: a scenario's
//!   expectations, safety on every observation and liveness as deadlines,
//!   and what the referee injects that belongs to no process;
//! - [`World`]: the loop over the simulator, which moves time only when the
//!   world is idle (simulator.md, 3), returns the run's trace for replay,
//!   checks each process's heap at every iteration against its own worst
//!   case, under the counting allocator (simulator.md, 5), and once
//!   settled checks that every process holds
//!   nothing and the simulator nothing in flight;
//! - [`real`]: the same processes and referee in one loop over the real
//!   ring, a ring each, on the real clock (testing-strategy.md, 2.8).
//!
//! [`domain`] supplies the reusable harness for domain-only worlds: a
//! delivery schedule, output-pressure stages, terminal ledgers, boundary
//! traces and an observation referee. Services keep their own fakes and
//! scenario expectations; these utilities are shared by temper and smith.
//!
//! A scenario's processes are one type, the scenario's own (an enum of its
//! service and its fakes, say), implementing [`Host`]; the boundary
//! contracts are each process's to keep, as its loop is its own.

extern crate alloc;

pub mod domain;
mod heap;
mod host;
pub mod real;
mod referee;
mod world;

pub use heap::Memory;
pub use host::Host;
pub use referee::{Expectation, Expectations, Referee};
pub use world::{Outcome, World};

/// Fails the world, loudly, with what it found: a world fails its test as
/// the simulator does (simulator.md, 5).
#[expect(clippy::panic, reason = "a world fails its test on what it finds, as the simulator does")]
pub(crate) fn fail(what: &str) -> ! {
    panic!("{what}")
}
