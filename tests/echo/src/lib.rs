//! The echo's worlds (examples.md, 7; testing-strategy.md, 2.7 and 2.8):
//! the echo and its fake clients as processes, each with its own `iterate`,
//! over the simulator or the real ring, through skein's world harness.
//!
//! What the test binaries share:
//!
//! - [`proc`]: the processes, the echo and the fake clients, as hosts;
//! - [`referee`]: each scenario's expectations, on what the fake clients
//!   saw, and what the referee injects: the echo's address told the clients
//!   once it listens, and its shutdown;
//! - [`scenarios`]: the scenarios, each a world from a seed and a
//!   simulator's configuration, under tiny limits;
//! - [`census`]: what a run's trace shows of the faults.
//!
//! The focused tests are `tests/scenarios.rs`, the sweeps over many seeds
//! `tests/fuzzy_scenarios.rs`; both declare the counting allocator, and every
//! simulated world checks memory at every iteration. `tests/real.rs` runs
//! the echo and its fake clients on the real ring.

pub mod census;
pub mod proc;
pub mod referee;
pub mod scenarios;
