//! io worlds (testing-strategy.md, 2.6; io.md, 8): io over the simulator,
//! with a scripted owner as the step above it, in one loop.
//!
//! What the test binaries share:
//!
//! - [`world`]: the loop, each process's io and owner over one simulator,
//!   and the checks every world makes (testing-strategy.md, 6): `MAX_OUT` at
//!   each call, io's contract in the [`ledger`], the invariants once settled,
//!   and a trace to replay;
//! - [`owner`]: the scripted owner, running each connection by a plan;
//! - [`referee`]: each scenario's expectations, safety and liveness;
//! - [`scenarios`]: the scenarios, from a seed and a configuration;
//! - [`census`]: what a run's trace shows of the faults and the races.
//!
//! The focused tests are `tests/scenarios.rs`, the sweeps over many seeds
//! `tests/fuzzy_scenarios.rs` (testing-strategy.md, 8). `tests/ring.rs` runs
//! one exchange through io over the real ring, and `tests/memory.rs` io's
//! worst case against the counting allocator.

pub mod append;
pub mod census;
pub mod cuts;
pub mod files;
pub mod ledger;
pub mod output;
pub mod owner;
pub mod private;
pub mod processes;
pub mod referee;
pub mod scenarios;
pub mod usage;
pub mod world;
