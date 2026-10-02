//! The world's knobs: sizes, limits, the wall clock's start, and the faults,
//! each a probability drawn from the seed (overview.md, section 9).

use skein_lib::{Duration, Wall};

/// How a world behaves. [`Config::calm`] is a well-behaved network;
/// [`Config::chaos`] turns every fault on.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Config {
    /// The bytes a socket's receive buffer holds, at least 1: a `Send`
    /// completes with at most the room left in its peer's, and waits while
    /// there is none.
    pub buffer: u32,
    /// The most connections a listener's queue holds, whatever its `Listen`
    /// asked for (at least one always can).
    pub backlog: u32,
    /// The most descriptors a process holds open: past it, `Socket` and
    /// `Accept` fail with `TooManyOpenFiles`, and the `Accept` leaves its
    /// connection waiting.
    pub max_fds: u32,
    /// How long a `Connect` waits for room in a full accept queue before it
    /// fails with `TimedOut`, as Linux gives up retrying its SYN.
    pub syn_timeout: Duration,
    /// The wall-clock time when the world starts, at [`skein_lib::Time::ZERO`].
    pub wall: Wall,
    pub faults: Faults,
}

/// Each fault's chance, in thousandths (`Rng::chance`), and the most latency.
///
/// Loopback itself never refuses a listened address, resets a connection or
/// times one out. `refuse`, `reset` and `timed_out` model the network beyond
/// it, a remote peer or a path that can do all three, so that io meets them
/// in a world that has only loopback.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Faults {
    /// That a completion is delivered late, by up to `latency_max`. Latency
    /// is what reorders completions, even on one descriptor.
    pub latency: u32,
    pub latency_max: Duration,
    /// That a `Recv` takes fewer bytes than are there and fit (at least one).
    pub short_recv: u32,
    /// That a `Send` gives fewer bytes than have room (at least one).
    pub short_send: u32,
    /// That a `Recv` or a `Send` on an established connection finds it
    /// reset, on both ends.
    pub reset: u32,
    /// That a `Connect` to a listener is refused anyway, as by a remote host
    /// whose listener went away.
    pub refuse: u32,
    /// That a `Socket`, `Accept`, `Connect`, `Recv` or `Send` fails with
    /// `NoBufferSpace`, having done nothing.
    pub no_buffer: u32,
    /// That a `Connect` fails with `TimedOut`, or that a `Recv` or a `Send`
    /// on an established connection finds it timed out (the peer then sees
    /// it reset).
    pub timed_out: u32,
    /// That a `Cancel` of an operation still waiting lands late, by up to
    /// `latency_max`: the target may complete first (the cancel is then
    /// `TooLate`), or be interrupted as it is stopped (`TooLate`, and the
    /// target `Cancelled`).
    pub cancel_race: u32,
    /// That the backend cannot submit a `Cancel`: it fails with `Other`, and
    /// its target runs on.
    pub cancel_unsubmitted: u32,
}

impl Faults {
    /// No fault at all.
    pub const NONE: Faults = Faults {
        latency: 0,
        latency_max: Duration::from_nanos(0),
        short_recv: 0,
        short_send: 0,
        reset: 0,
        refuse: 0,
        no_buffer: 0,
        timed_out: 0,
        cancel_race: 0,
        cancel_unsubmitted: 0,
    };

    /// Every fault, often enough that a few hundred seeds meet each one.
    pub const CHAOS: Faults = Faults {
        latency: 500,
        latency_max: Duration::from_millis(5),
        short_recv: 300,
        short_send: 300,
        reset: 1,
        refuse: 50,
        no_buffer: 5,
        timed_out: 1,
        cancel_race: 500,
        cancel_unsubmitted: 100,
    };
}

/// 2026-01-01T00:00:00Z: a fixed start, so the wall clock replays too.
const START: Wall = Wall::from_nanos(1_767_225_600_000_000_000);

impl Config {
    /// A well-behaved network: no faults, roomy buffers, Linux's limits.
    #[must_use]
    pub const fn calm() -> Config {
        Config {
            buffer: 64 * 1024,
            backlog: 128,
            max_fds: 1024,
            syn_timeout: Duration::from_secs(127),
            wall: START,
            faults: Faults::NONE,
        }
    }

    /// Every fault on, and small buffers, so sends are cut and stall.
    #[must_use]
    pub const fn chaos() -> Config {
        Config {
            buffer: 64,
            backlog: 2,
            max_fds: 1024,
            syn_timeout: Duration::from_secs(127),
            wall: START,
            faults: Faults::CHAOS,
        }
    }
}
