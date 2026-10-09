//! The world harness (testing-strategy.md, 7; examples.md, 6): skein's,
//! since its examples are the second user of a world. One thread, one loop,
//! driving every process of a world through its own `iterate`, with a
//! referee beside them, and checking as it goes:
//!
//! - [`Host`]: what the harness needs of a process, a service or a fake,
//!   each following the programming model;
//! - [`HostedProgram`] and [`Inherited`]: factories shared by simulated and
//!   real hosting, selected by the program of a spawn, and its child pipes;
//! - [`Machine`]: the fake machine answering other programs and file calls;
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
//!   shared ring, hosting spawns over real pipes with per-service signals,
//!   on the real clock (testing-strategy.md, 2.8).
//!
//! [`end_to_end`] starts shipped binaries on pipes or a controlling terminal,
//! captures stderr and exposes their shared-ring exit and signal operations.
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
pub mod end_to_end;
mod heap;
mod program;
pub mod real;
mod referee;
mod world;

pub use heap::Memory;
pub use program::{HostedProgram, Inherited, Machine, NoMachine, StartupRoot, StartupRoots};
pub use referee::{Expectation, Expectations, Referee};
pub use skein_shell::{Host, drive};
pub use world::{Killed, Outcome, World};

/// Fails the world, loudly, with what it found: a world fails its test as
/// the simulator does (simulator.md, 5).
#[expect(clippy::panic, reason = "a world fails its test on what it finds, as the simulator does")]
pub(crate) fn fail(what: &str) -> ! {
    panic!("{what}")
}
