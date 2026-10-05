//! Boundary traces and deterministic replay (testing-strategy.md, section 6).
//! Worlds provide observations and seeded runs; these helpers compare the
//! trace and scenario outcome without reading a clock or domain state.

use alloc::{format, string::String, vec::Vec};

use core::fmt::{Debug, Display};

use skein_lib::Time;

/// What crossed a world's boundaries, in order, each line with the time it
/// crossed at: a seed replays to the same trace (testing-strategy.md, section 6).
#[derive(Default, Debug)]
pub struct Trace {
    lines: Vec<String>,
}

impl Trace {
    /// Records one boundary observation at the world-supplied time.
    pub fn log<D: Display>(&mut self, now: Time, line: D) {
        self.lines.push(format!("{:>16} {line}", now.as_nanos()));
    }

    /// Returns recorded observations in their original order for replay checks.
    #[must_use]
    pub fn lines(&self) -> &[String] {
        &self.lines
    }
}

/// Checks that a world replays from its seed: `run` runs a world of a seed to
/// its end, and returns its trace and whatever else must come out the same
/// (its stats, the time it settled at). `seed` run twice comes out the same,
/// and `other` takes a course of its own. Returns the trace of `seed`, for the
/// caller to check that the world did something.
pub fn assert_replays<T: PartialEq + Debug, F: Fn(u64) -> (Vec<String>, T)>(
    seed: u64,
    other: u64,
    run: F,
) -> Vec<String> {
    let (trace, end) = run(seed);
    let again = run(seed);
    assert!(again.0 == trace, "seed {seed} replays to the same trace");
    assert_eq!(again.1, end, "seed {seed} replays to the same end");
    assert!(run(other).0 != trace, "seeds {seed} and {other} take courses of their own");
    trace
}
