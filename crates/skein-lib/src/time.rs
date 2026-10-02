//! Time is data (section 8): nanoseconds on a monotonic clock that the shell or
//! the simulator reads, never a step.

/// A point in time, in nanoseconds since an arbitrary origin.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Time(u64);

/// A span of time, in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Duration(u64);

impl Time {
    /// The origin.
    pub const ZERO: Time = Time(0);

    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Time {
        Time(nanos)
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// `span` later, or `None` past the end of time.
    #[must_use]
    pub const fn checked_add(self, span: Duration) -> Option<Time> {
        match self.0.checked_add(span.0) {
            Some(nanos) => Some(Time(nanos)),
            None => None,
        }
    }

    /// `span` later, or the end of time: for deadlines so far out that
    /// "never" is the right reading.
    #[must_use]
    pub const fn saturating_add(self, span: Duration) -> Time {
        Time(self.0.saturating_add(span.0))
    }

    /// The span from `earlier` to this time, or zero if `earlier` is later.
    #[must_use]
    pub const fn saturating_since(self, earlier: Time) -> Duration {
        Duration(self.0.saturating_sub(earlier.0))
    }
}

impl Duration {
    pub const ZERO: Duration = Duration(0);

    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Duration {
        Duration(nanos)
    }

    /// Saturates at the longest span: a configuration value that large means "never".
    #[must_use]
    pub const fn from_millis(millis: u64) -> Duration {
        Duration(millis.saturating_mul(1_000_000))
    }

    /// Saturates at the longest span: a configuration value that large means "never".
    #[must_use]
    pub const fn from_secs(secs: u64) -> Duration {
        Duration(secs.saturating_mul(1_000_000_000))
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn checked_add(self, other: Duration) -> Option<Duration> {
        match self.0.checked_add(other.0) {
            Some(nanos) => Some(Duration(nanos)),
            None => None,
        }
    }

    #[must_use]
    pub const fn saturating_add(self, other: Duration) -> Duration {
        Duration(self.0.saturating_add(other.0))
    }

    #[must_use]
    pub const fn saturating_mul(self, factor: u64) -> Duration {
        Duration(self.0.saturating_mul(factor))
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, Time};

    #[test]
    fn arithmetic_is_checked_or_saturating() {
        let end = Time::from_nanos(u64::MAX);
        assert_eq!(end.checked_add(Duration::from_nanos(1)), None);
        assert_eq!(end.saturating_add(Duration::from_secs(1)), end);
        assert_eq!(Time::ZERO.saturating_since(end), Duration::ZERO);
        assert_eq!(Duration::from_secs(u64::MAX).as_nanos(), u64::MAX);
        assert_eq!(Duration::from_millis(3).as_nanos(), 3_000_000);
    }
}
