//! The protocol layer's limits, what they imply for memory
//! (programming-model.md, 6.3), and the most each entry point emits
//! (programming-model.md, section 2).

use skein_lib::{Deadlines, Duration, Id, Set, Slab};

use crate::conn::Conn;

/// The answer to an `Open` the domain refused (examples.md, 3.1).
pub const BUSY: &[u8] = b"busy\n";

/// The answer to a line past `Limits::line` (examples.md, 3.1).
pub const TOO_LONG: &[u8] = b"too long\n";

/// The protocol layer's limits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Connections at once: an accepted socket past it is rejected.
    pub conns: u32,
    /// The longest line, its `\n` included: the most a scan asks for, and the
    /// room each answer asks for. At least the longest refusal.
    pub line: u32,
    /// How long a connection may make no progress (a line read, room for an
    /// answer granted) before it is closed.
    pub idle: Duration,
    /// How far each idle deadline is spread past `idle`, drawn from the seed,
    /// so that connections opened together do not all expire together.
    pub spread: Duration,
    /// How long the listener waits to listen again after io refused it for
    /// want of resources (`Error::Busy`, io.md, 2). Never zero, or it would
    /// spin while the shortage lasts.
    pub retry: Duration,
}

impl Limits {
    /// The largest read the layer demands of io: a scan of `line` bytes.
    /// Startup checks it against io's intake (io.md, 2).
    #[must_use]
    pub const fn largest_read(&self) -> u32 {
        self.line
    }

    /// The largest room the layer demands of io: `line` bytes, for any
    /// answer. Startup checks it against io's output cap (io.md, 2).
    #[must_use]
    pub const fn largest_room(&self) -> u32 {
        self.line
    }

    /// Whether the layer can run under these limits: a connection, a line
    /// that holds the longest refusal, and an idle deadline and a retry that
    /// wait.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        let holds_refusals = match usize::try_from(self.line) {
            Ok(line) => line >= TOO_LONG.len() && line >= BUSY.len(),
            Err(_) => true,
        };
        self.conns > 0 && holds_refusals && self.idle.as_nanos() > 0 && self.retry.as_nanos() > 0
    }
}

/// The most heap the protocol layer holds under `limits`, or `None` past a
/// `u64` (programming-model.md, 6.3): its slab of connections, their idle
/// deadlines and its ready list; a line in flight per connection (the bytes
/// read, the domain's text, or the answer before io takes it, as one
/// request is in flight per connection); and one line more, for the copy a
/// step makes while it decodes a line or encodes its answer.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let conns = limits.conns;
    let tables = Slab::<Conn>::worst_case(conns)?
        .checked_add(Deadlines::<Id<Conn>>::worst_case(conns)?)?
        .checked_add(Set::<Id<Conn>>::worst_case(conns)?)?;
    let lines = u64::from(conns).checked_add(1)?.checked_mul(u64::from(limits.line))?;
    tables.checked_add(lines)
}

/// The most an entry point emits in one call: events up to the domain, and
/// requests down to io. The loop reserves this much room in each first.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    pub events: u32,
    pub requests: u32,
}

/// `resume`: the listener's `Listen`; or `Shutdown` told; or a connection's
/// `Gone`.
pub const MAX_OUT_RESUME: MaxOut = MaxOut { events: 1, requests: 1 };

/// `up`: a socket bound and its first demand, or rejected; a call up; or a
/// refusal sent and the close, and `Gone`.
pub const MAX_OUT_UP: MaxOut = MaxOut { events: 1, requests: 2 };

/// `fire`: an idle connection's close, and `Gone`.
pub const MAX_OUT_FIRE: MaxOut = MaxOut { events: 1, requests: 1 };

/// `down`: an answer sent and the next demand, or the close; a refusal sent
/// and the close; the listener's close.
pub const MAX_OUT_DOWN: u32 = 2;
