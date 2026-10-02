//! The clock (overview.md, section 8; programming-style.md, section 8).

use skein_lib::{Time, Wall};

use crate::ring;

/// Reads the two times a step sees in its `Env`, once per iteration.
#[derive(Debug)]
pub struct Clock {
    monotonic: libc::clockid_t,
    wall: libc::clockid_t,
}

/// The times of one iteration, read together.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Now {
    /// `CLOCK_MONOTONIC`, for every deadline. The [`Wait::Until`] deadline the
    /// kernel waits for is on this clock.
    ///
    /// [`Wait::Until`]: crate::Wait::Until
    pub now: Time,
    /// `CLOCK_REALTIME`, for things about the world, never a deadline.
    pub wall: Wall,
}

impl Clock {
    #[must_use]
    pub fn new() -> Clock {
        Clock { monotonic: libc::CLOCK_MONOTONIC, wall: libc::CLOCK_REALTIME }
    }

    #[must_use]
    pub fn now(&self) -> Now {
        let now = ring::clock_nanos(self.monotonic).expect("the monotonic clock is past its origin");
        // A wall clock set before 1970 reads as the epoch: it is never a
        // deadline, and nothing about the world happened before then.
        let wall = ring::clock_nanos(self.wall).unwrap_or(0);
        Now { now: Time::from_nanos(now), wall: Wall::from_nanos(wall) }
    }
}

impl Default for Clock {
    fn default() -> Clock {
        Clock::new()
    }
}
