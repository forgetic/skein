//! io's limits, what they imply for memory (programming-model.md, 6.3), and
//! the most each entry point emits (programming-model.md, section 2).

use alloc::boxed::Box;

use skein_lib::{Deadlines, Duration, Id, Intake, Queue, Set, Slab, Token};

use crate::layer::{Entity, Flight};

/// io's limits: the admission limits of its entities, and the caps on each
/// stream's bytes (programming-model.md, section 7).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Sockets at once, listeners included: a `Listen` or `Connect` past it is
    /// refused (`Error::Busy`), and a listener accepts only while one is free.
    pub sockets: u32,
    /// Refused `Listen`s and `Connect`s held until the next up pass tells
    /// them (io.md, 2). The loop hands io a request only while it can hold one
    /// more (`Io::takes`).
    pub refusals: u32,
    /// The cap on each stream's unparsed input, and so the largest read io can
    /// meet (`Limits::largest_read`).
    pub intake: u32,
    /// The most bytes one receive asks for.
    pub receive: u32,
    /// The cap on each stream's queued output, in bytes, its send in flight
    /// included, and so the largest room io can grant
    /// (`Limits::largest_room`).
    pub output: u32,
    /// The most `Send`s queued on a stream at once, beside the one in flight.
    pub sends: u32,
    /// The most accepts armed in one iteration, over every listener.
    pub accepts: u32,
    /// The backlog each listener asks the kernel for, a hint it may clamp.
    pub backlog: u32,
    /// How long a graceful close may take before it aborts.
    pub close_timeout: Duration,
}

impl Limits {
    /// The largest read demand io can meet: a fill or a scan of at most this
    /// many bytes, the intake's cap. Whoever stacks a machine on a stream
    /// checks at startup that the machine's largest demand is no larger (io.md,
    /// 2); io asserts it, as a larger one could never be met (lib.md, 7).
    #[must_use]
    pub const fn largest_read(&self) -> u32 {
        self.intake
    }

    /// The most output room io can grant at once: the output cap. The same
    /// startup check holds for room.
    #[must_use]
    pub const fn largest_room(&self) -> u32 {
        self.output
    }

    /// Whether io can run under these limits: a refusal held, a byte of
    /// intake, of receive and of output, a send queued, and an accept per
    /// iteration, at least.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        self.refusals > 0
            && self.intake > 0
            && self.receive > 0
            && self.output > 0
            && self.sends > 0
            && self.accepts > 0
    }
}

/// The most operations an entity has in flight at once: a `Recv`, a `Send`,
/// and a `Cancel` of each (io.md, 3.4).
const PER_SOCKET: u32 = 4;

/// The most operations io has in flight at once, cancels included, under
/// `limits`: the size of the ring (kernel.md, 5), or `None` past a `u32`.
#[must_use]
pub fn operations(limits: &Limits) -> Option<u32> {
    limits.sockets.checked_mul(PER_SOCKET)
}

/// The slots of the operation table: twice the most in flight, as an
/// operation retired in an iteration keeps its slot until the reclaim point,
/// and its entity may submit as many again before then.
pub(crate) fn flights(limits: &Limits) -> Option<u32> {
    operations(limits)?.checked_mul(2)
}

/// The most heap io holds under `limits`, or `None` past a `u64`
/// (programming-model.md, 6.3): its tables as their containers report them,
/// and per socket an intake, a receive buffer, and the output with its queue.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let sockets = limits.sockets;
    let tables = Slab::<Entity>::worst_case(sockets)?
        .checked_add(Slab::<Flight>::worst_case(flights(limits)?)?)?
        .checked_add(Deadlines::<Id<Entity>>::worst_case(sockets)?)?
        .checked_add(Set::<Id<Entity>>::worst_case(sockets)?.checked_mul(3)?)?
        .checked_add(Queue::<Token>::worst_case(limits.refusals)?)?;
    let stream = Intake::worst_case(limits.intake)?
        .checked_add(u64::from(limits.receive))?
        .checked_add(u64::from(limits.output))?
        .checked_add(Queue::<Box<[u8]>>::worst_case(limits.sends)?)?;
    tables.checked_add(stream.checked_mul(u64::from(sockets))?)
}

/// The most an entry point emits in one call: events into the queue up, and
/// records into the queue the loop hands the kernel. The loop reserves this
/// much room in each before the call (programming-model.md, section 2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    pub events: u32,
    pub submissions: u32,
}

/// `resume`: a refusal's `Failed` and `Closed`; or a connect's `Connecting`;
/// or a stream's `Bytes`, `Room` and `End`, and the receive the intake has
/// room for again; or a listener's accept.
pub const MAX_OUT_RESUME: MaxOut = MaxOut { events: 3, submissions: 1 };

/// `up`: a stream's `Bytes`, `Room` and `End`, or an entity's `Failed` and
/// `Closed`; the operation that follows the one completed, and a receive the
/// intake has room for again.
pub const MAX_OUT_UP: MaxOut = MaxOut { events: 3, submissions: 2 };

/// `fire`: the cancels of a closing stream's receive and send.
pub const MAX_OUT_FIRE: MaxOut = MaxOut { events: 0, submissions: 2 };

/// `down`: a bound socket's receive and its listener's next accept; a close's
/// discarding receive and half-close; an abort's two cancels.
pub const MAX_OUT_DOWN: MaxOut = MaxOut { events: 0, submissions: 2 };
