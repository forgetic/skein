//! Generic domain-world machinery (testing-strategy.md, sections 2.2, 6 and 7).
//! Extracted from temper's world harness when smith became its second user.
//! Keeps deterministic deliveries, stage queues, terminal ledgers, traces and
//! observation-based scenario expectations. It never knows a service's policy,
//! fakes, limits or domain state and performs no IO. Worlds drive their own loop,
//! supply time and randomness, and check final quiescence; the referee watches
//! their boundary observations. This is separate from the process/simulator
//! `World` at the crate root, whose API is unchanged.
//!
//! `Schedule` delivers once in time/serial order; `Stage` reserves `max_out`
//! room before handing over an event; `Ledger` checks one terminal per open key.
//! `Referee` observes safety and arms liveness, emitting immediate or timed
//! stimuli; `assert_replays` compares both boundary trace and scenario outcome.
//! `heap` reuses skein's counting allocator and handed-output accounting.

mod ledger;
mod referee;
mod schedule;
mod stage;
mod trace;

pub use ledger::Ledger;
pub use referee::{Expectations, Failure, Judge, Referee, Verdict};
pub use schedule::{Key, Schedule};
pub use stage::Stage;
pub use trace::{Trace, assert_replays};

/// Counting allocator and handed-output-aware heap measurements already
/// provided by skein (programming-model.md, section 6.3). Declare `Counting`
/// globally in each memory-test binary; measure one step at a time, drop only
/// its handed-out outputs, then check before starting the next step.
pub mod heap {
    pub use skein_heap::{Counting, Measured, Meter};
}

use skein_lib::{Duration, Rng};

/// Inclusive latency span drawn from the world's injected seed; worlds own
/// the configuration and bound scheduled work (testing-strategy.md, section 3).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Span {
    /// Shortest delivery latency, no greater than `max`.
    pub min: Duration,
    /// Longest delivery latency, bounding when a scheduled terminal can arrive.
    pub max: Duration,
}

impl Span {
    /// Constructs an inclusive millisecond span. Panics if its bounds are reversed.
    #[must_use]
    pub const fn millis(min: u64, max: u64) -> Span {
        assert!(min <= max, "latency span has ordered bounds");
        Span { min: Duration::from_millis(min), max: Duration::from_millis(max) }
    }

    /// Draws one latency from the injected RNG, leaving the world's clock untouched.
    pub fn draw(self, rng: &mut Rng) -> Duration {
        Duration::from_nanos(rng.between(self.min.as_nanos(), self.max.as_nanos()))
    }
}
