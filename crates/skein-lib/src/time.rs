//! Time is data (programming-model.md, section 9): nanoseconds on a monotonic
//! clock that the shell or the simulator reads, never a step; and wall time, a
//! separate type, for things about the world.

/// A point in time, in nanoseconds since an arbitrary origin.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Time(u64);

/// A span of time, in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Duration(u64);

/// A point in wall-clock time, in nanoseconds since the Unix epoch, read by the
/// shell or the simulator beside [`Time`] (programming-model.md, section 9).
///
/// It is for things about the world: a certificate's validity, a timestamp a
/// peer will read. It is a type of its own, with no arithmetic on spans, so
/// it is never confused with a monotonic `Time` and never arms a deadline:
/// the wall clock can jump.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Wall(u64);

impl Wall {
    /// The Unix epoch, 1970-01-01T00:00:00Z.
    pub const EPOCH: Wall = Wall(0);

    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Wall {
        Wall(nanos)
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Whole seconds since the epoch, as certificates and HTTP dates count.
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0.div_euclid(1_000_000_000)
    }
}

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
