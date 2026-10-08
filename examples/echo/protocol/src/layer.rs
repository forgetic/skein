//! The protocol layer's state and its entry points (examples.md, 3.4): the
//! listener, the slab of connections, their idle deadlines and the ready
//! list; `resume`, `up`, `fire` and `down`, which the loop calls in that
//! order within an iteration, and `Protocol::reclaim` at its end.

use core::mem;

use skein_echo_domain::{Event as Call, Reply, Request as Domain};
use skein_io::kernel::Addr;
use skein_io::{Error, Event as Told, Request as Io};
use skein_lib::{Deadlines, Duration, Env, Id, Queue, Rng, Set, Slab, Time, Token};

use crate::conn::{self, Conn};
use crate::limits::Limits;

/// The owner token of the listener below. A connection's token is its
/// handle's, whose slot is below its slab's capacity, so never this.
const LISTENER: Token = Token::new(u64::MAX);

/// The protocol layer's state.
#[derive(Debug)]
pub struct Protocol {
    /// Where the listener listens.
    addr: Addr,
    listener: Listener,
    conns: Slab<Conn>,
    tables: Tables,
    shutdown: Shutdown,
}

/// What the layer keeps beside its connections, which a connection's
/// handlers touch while it is borrowed from its slab.
#[derive(Debug)]
pub(crate) struct Tables {
    /// One idle deadline per connection, while it waits on its peer.
    pub(crate) deadlines: Deadlines<Id<Conn>>,
    /// The connections that owe the domain a `Gone`, told by `resume`.
    ready: Set<Id<Conn>>,
    /// Spreads the idle deadlines.
    rng: Rng,
}

/// The listener (examples.md, 3.4).
#[derive(Debug)]
enum Listener {
    /// On the ready list: `resume` asks io to listen.
    Unopened,
    /// `Listen` asked; whether the domain asked it to stop meanwhile.
    Opening {
        stop: bool,
    },
    Listening {
        listener: Token,
        addr: Addr,
    },
    /// `Close` asked; why it stopped, if it failed.
    Closing {
        error: Option<Error>,
    },
    /// The listen failed: io closes what it made; whether the domain asked it
    /// to stop meanwhile.
    Failed {
        error: Error,
        stop: bool,
    },
    /// The listen was refused for want of resources (`error`, `Busy`): it
    /// is asked again at `at`.
    Backoff {
        at: Time,
        error: Error,
    },
    Closed {
        error: Option<Error>,
    },
}

/// The listener's terminal state, which holds nothing: the placeholder of
/// its transitions (programming-model.md, 5.4).
const CLOSED: Listener = Listener::Closed { error: None };

/// The service's shutdown from io or a lower-tier world (io.md, section 7),
/// told to the domain once from the ready list.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Shutdown {
    Running,
    Asked,
    Told,
}

impl Protocol {
    /// The layer under `limits`, which must be usable, to listen at `addr`
    /// once the loop first resumes it; `seed` spreads its idle deadlines.
    #[must_use]
    pub fn new(limits: &Limits, addr: Addr, seed: u64) -> Protocol {
        assert!(limits.is_usable(), "the protocol layer runs only under usable limits (Limits::is_usable)");
        Protocol {
            addr,
            listener: Listener::Unopened,
            conns: Slab::with_capacity(limits.conns),
            tables: Tables {
                deadlines: Deadlines::with_capacity(limits.conns),
                ready: Set::with_capacity(limits.conns),
                rng: Rng::new(seed),
            },
            shutdown: Shutdown::Running,
        }
    }

    /// Asks for the domain to be told `Shutdown`, from the ready list. Asked
    /// again, it changes nothing.
    pub fn shutdown(&mut self) {
        match self.shutdown {
            Shutdown::Running => self.shutdown = Shutdown::Asked,
            Shutdown::Asked | Shutdown::Told => {}
        }
    }

    /// Whether `resume` has something to do. While it has, the loop calls it
    /// at the start of the layer's stage, before io's events.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        let listen = match self.listener {
            Listener::Unopened => true,
            Listener::Opening { .. }
            | Listener::Listening { .. }
            | Listener::Closing { .. }
            | Listener::Failed { .. }
            | Listener::Backoff { .. }
            | Listener::Closed { .. } => false,
        };
        listen || self.shutdown == Shutdown::Asked || !self.tables.ready.is_empty()
    }

    /// When the earliest deadline falls due: a connection's idle deadline, or
    /// the listener's next listen.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let idle = self.tables.deadlines.next();
        match self.listener {
            Listener::Backoff { at, .. } => match idle {
                Some(idle) => Some(idle.min(at)),
                None => Some(at),
            },
            Listener::Unopened
            | Listener::Opening { .. }
            | Listener::Listening { .. }
            | Listener::Closing { .. }
            | Listener::Failed { .. }
            | Listener::Closed { .. } => idle,
        }
    }

    /// Whether a deadline is due at `now`. While one is, the loop calls
    /// `fire`, once io's events are all taken.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        match self.next_deadline() {
            Some(at) => at <= now,
            None => false,
        }
    }

    /// The address the listener listens at, its port resolved, once it
    /// listens and until it closes.
    #[must_use]
    pub const fn listening(&self) -> Option<Addr> {
        match self.listener {
            Listener::Listening { addr, .. } => Some(addr),
            Listener::Unopened
            | Listener::Opening { .. }
            | Listener::Closing { .. }
            | Listener::Failed { .. }
            | Listener::Backoff { .. }
            | Listener::Closed { .. } => None,
        }
    }

    /// Why the listener stopped, if it failed: its listen, or its accept.
    #[must_use]
    pub const fn failure(&self) -> Option<Error> {
        match self.listener {
            Listener::Failed { error, .. } => Some(error),
            Listener::Closing { error } | Listener::Closed { error } => error,
            Listener::Unopened | Listener::Opening { .. } | Listener::Listening { .. } | Listener::Backoff { .. } => {
                None
            }
        }
    }

    /// What refused the listen the listener waits to ask again, while it
    /// waits: a shortage, which `main` reports if it lasts.
    #[must_use]
    pub const fn retrying(&self) -> Option<Error> {
        match self.listener {
            Listener::Backoff { error, .. } => Some(error),
            Listener::Unopened
            | Listener::Opening { .. }
            | Listener::Listening { .. }
            | Listener::Closing { .. }
            | Listener::Failed { .. }
            | Listener::Closed { .. } => None,
        }
    }

    /// Connections present, retired ones included until the reclaim point.
    #[must_use]
    pub const fn conns(&self) -> u32 {
        self.conns.len()
    }

    /// Whether the layer holds nothing: its listener closed, and no
    /// connection, deadline or ready entry left.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let closed = match self.listener {
            Listener::Closed { .. } => true,
            Listener::Unopened
            | Listener::Opening { .. }
            | Listener::Listening { .. }
            | Listener::Closing { .. }
            | Listener::Failed { .. }
            | Listener::Backoff { .. } => false,
        };
        closed
            && self.conns.is_empty()
            && self.tables.deadlines.is_empty()
            && self.tables.ready.is_empty()
            && self.shutdown != Shutdown::Asked
    }

    /// The reclaim point: frees the connections retired in this iteration.
    pub fn reclaim(&mut self) {
        self.conns.reclaim();
    }
}

impl Tables {
    /// Arms `id`'s idle deadline afresh: `idle` from now, spread by up to
    /// `spread`.
    pub(crate) fn idle(&mut self, id: Id<Conn>, env: &Env<Limits>) {
        let spread = Duration::from_nanos(self.rng.below(env.limits.spread.as_nanos()));
        let at = env.now.saturating_add(env.limits.idle).saturating_add(spread);
        self.deadlines.arm(id, at).expect("room for a deadline per connection");
    }
}

/// Takes one entry of the ready list, emitting at most
/// [`MAX_OUT_RESUME`](crate::MAX_OUT_RESUME): the listener's `Listen`; or
/// `Shutdown` told the domain; or a connection's `Gone`.
pub fn resume(proto: &mut Protocol, _env: &Env<Limits>, up: &mut Queue<Call>, down: &mut Queue<Io>) {
    match proto.listener {
        Listener::Unopened => {
            proto.listener = listen(proto.addr, down);
            return;
        }
        Listener::Opening { .. }
        | Listener::Listening { .. }
        | Listener::Closing { .. }
        | Listener::Failed { .. }
        | Listener::Backoff { .. }
        | Listener::Closed { .. } => {}
    }
    if proto.shutdown == Shutdown::Asked {
        up.push(Call::Shutdown);
        proto.shutdown = Shutdown::Told;
        return;
    }
    let Some(id) = proto.tables.ready.pop_first() else {
        return;
    };
    let conn = proto.conns.get_mut(id).expect("a connection on the ready list is retired only off it");
    conn::resumed(conn, up);
    conclude(proto, id);
}

/// Takes one event io told, emitting at most
/// [`MAX_OUT_UP`](crate::MAX_OUT_UP). The loop hands the layer io's events
/// only once its ready list is empty.
///
/// An event names its owner, which is never stale: a connection retires
/// only once io told it `Closed` (programming-model.md, 5.2).
pub fn up(proto: &mut Protocol, env: &Env<Limits>, event: Told, up: &mut Queue<Call>, down: &mut Queue<Io>) {
    match event {
        Told::Listening { owner, listener, addr } => {
            assert!(owner == LISTENER, "only the listener listens");
            listening(proto, listener, addr, down);
        }
        Told::Accepted { owner, socket, peer: _ } => {
            assert!(owner == LISTENER, "only the listener accepts");
            accepted(proto, socket, env, down);
        }
        Told::Output { .. }
        | Told::Connecting { .. }
        | Told::Connected { .. }
        | Told::Spawned { .. }
        | Told::Exited { .. } => {
            unreachable!("the echo connects to no one and spawns no child")
        }
        Told::Shutdown { signal: _ } => proto.shutdown(),
        Told::Stream { owner, up: event } => {
            let id = Id::<Conn>::from_token(owner);
            let Protocol { conns, tables, .. } = proto;
            let conn = conns.get_mut(id).expect("a connection outlives what io tells it");
            conn::stream(conn, id, event, up, down);
            conn::follow(conn, id, env, tables, down);
            conclude(proto, id);
        }
        Told::Failed { owner, error } => {
            assert!(owner == LISTENER, "a bound socket fails on its stream, not as a listen or a connect");
            listener_failed(proto, error, down);
        }
        Told::Closed { owner } => {
            if owner == LISTENER {
                listener_closed(proto, env);
            } else {
                let id = Id::<Conn>::from_token(owner);
                conn::closed(proto.conns.get_mut(id).expect("a connection outlives what io tells it"));
                conclude(proto, id);
            }
        }
    }
}

/// Fires the earliest deadline due at `env.now`, if one is, emitting at most
/// [`MAX_OUT_FIRE`](crate::MAX_OUT_FIRE): the listener listens again, or an
/// idle connection is closed.
pub fn fire(proto: &mut Protocol, env: &Env<Limits>, up: &mut Queue<Call>, down: &mut Queue<Io>) {
    match proto.listener {
        Listener::Backoff { at, .. } if at <= env.now => {
            proto.listener = listen(proto.addr, down);
            return;
        }
        Listener::Unopened
        | Listener::Opening { .. }
        | Listener::Listening { .. }
        | Listener::Closing { .. }
        | Listener::Failed { .. }
        | Listener::Backoff { .. }
        | Listener::Closed { .. } => {}
    }
    let Some(id) = proto.tables.deadlines.expire(env.now) else {
        return;
    };
    let Protocol { conns, tables, .. } = proto;
    let conn = conns.get_mut(id).expect("a deadline is cancelled when its connection leaves the states that run it");
    conn::idled(conn, up, down);
    conn::follow(conn, id, env, tables, down);
    conclude(proto, id);
}

/// Takes one request of the domain's, emitting at most
/// [`MAX_OUT_DOWN`](crate::MAX_OUT_DOWN) requests to io.
///
/// A reply names its connection by its `ReplyTo`, which may be stale going
/// down (programming-model.md, 5.2), and is then dropped; but a connection
/// retires only once its call was answered, so it never is.
pub fn down(proto: &mut Protocol, env: &Env<Limits>, request: Domain, down: &mut Queue<Io>) {
    match request {
        Domain::Reply { to, reply } => replied(proto, env, to.into_token(), reply, down),
        Domain::Stop => stop(proto, down),
    }
}

fn replied(proto: &mut Protocol, env: &Env<Limits>, call: Token, reply: Reply, down: &mut Queue<Io>) {
    let id = Id::<Conn>::from_token(call);
    let Protocol { conns, tables, .. } = proto;
    let Some(conn) = conns.get_mut(id) else {
        return;
    };
    conn::replied(conn, reply, env, down);
    conn::follow(conn, id, env, tables, down);
    if conn.owes() {
        let _new: bool = tables.ready.insert(id).expect("room on the ready list for every connection");
    }
    conclude(proto, id);
}

/// An accepted socket: bound to a new connection, or rejected when the slab
/// is full, the refusal at the layer's entrance; or rejected because the
/// listener is closing, announced before io took the close.
fn accepted(proto: &mut Protocol, socket: Token, env: &Env<Limits>, down: &mut Queue<Io>) {
    match proto.listener {
        Listener::Listening { .. } => match proto.conns.insert(conn::opened(socket)) {
            Ok(id) => {
                down.push(Io::Bind { socket, owner: id.token() });
                let Protocol { conns, tables, .. } = proto;
                let conn = conns.get(id).expect("just made");
                conn::follow(conn, id, env, tables, down);
            }
            Err(_refused) => down.push(Io::Reject { socket }),
        },
        Listener::Closing { .. } => down.push(Io::Reject { socket }),
        Listener::Unopened
        | Listener::Opening { .. }
        | Listener::Failed { .. }
        | Listener::Backoff { .. }
        | Listener::Closed { .. } => unreachable!("io announces a socket only to a listener that listens"),
    }
}

/// io asked to listen at `addr`.
fn listen(addr: Addr, down: &mut Queue<Io>) -> Listener {
    down.push(Io::Listen { owner: LISTENER, addr });
    Listener::Opening { stop: false }
}

fn listening(proto: &mut Protocol, listener: Token, addr: Addr, down: &mut Queue<Io>) {
    let state = mem::replace(&mut proto.listener, CLOSED);
    proto.listener = match state {
        Listener::Opening { stop: false } => Listener::Listening { listener, addr },
        Listener::Opening { stop: true } => {
            down.push(Io::Close { entity: listener });
            Listener::Closing { error: None }
        }
        Listener::Unopened
        | Listener::Listening { .. }
        | Listener::Closing { .. }
        | Listener::Failed { .. }
        | Listener::Backoff { .. }
        | Listener::Closed { .. } => unreachable!("Listening is told once, to the Listen"),
    };
}

/// The listen failed, and io closes it; or the listener accepts no more, and
/// the layer closes it.
fn listener_failed(proto: &mut Protocol, error: Error, down: &mut Queue<Io>) {
    let state = mem::replace(&mut proto.listener, CLOSED);
    proto.listener = match state {
        Listener::Opening { stop } => Listener::Failed { error, stop },
        Listener::Listening { listener, .. } => {
            down.push(Io::Close { entity: listener });
            Listener::Closing { error: Some(error) }
        }
        // Told before io took the close.
        Listener::Closing { .. } => Listener::Closing { error: Some(error) },
        Listener::Unopened | Listener::Failed { .. } | Listener::Backoff { .. } | Listener::Closed { .. } => {
            unreachable!("a listener fails once, while it is open")
        }
    };
}

/// io closed the listener: after its close, or after its listen failed. A
/// listen refused for want of resources is asked again after
/// `Limits::retry`, as io's `Busy` allows (io.md, 2); any other failure
/// stops the listener for good, and the service with it.
fn listener_closed(proto: &mut Protocol, env: &Env<Limits>) {
    let state = mem::replace(&mut proto.listener, CLOSED);
    proto.listener = match state {
        Listener::Failed { error: error @ Error::Busy, stop: false } => {
            Listener::Backoff { at: env.now.saturating_add(env.limits.retry), error }
        }
        // A shortage is not a failure, stopped or not; any other failure is
        // kept, whether a stop came meanwhile or not, as a closing
        // listener's is.
        Listener::Failed { error: Error::Busy, stop: true } => Listener::Closed { error: None },
        Listener::Failed { error, .. } => Listener::Closed { error: Some(error) },
        Listener::Closing { error } => Listener::Closed { error },
        Listener::Unopened
        | Listener::Opening { .. }
        | Listener::Listening { .. }
        | Listener::Backoff { .. }
        | Listener::Closed { .. } => {
            unreachable!("io tells the listener Closed after its close, or after its listen failed")
        }
    };
}

/// The domain lets no one more in: the listener closes, once it can.
fn stop(proto: &mut Protocol, down: &mut Queue<Io>) {
    let state = mem::replace(&mut proto.listener, CLOSED);
    proto.listener = match state {
        Listener::Unopened | Listener::Backoff { .. } => Listener::Closed { error: None },
        Listener::Opening { .. } => Listener::Opening { stop: true },
        Listener::Failed { error, .. } => Listener::Failed { error, stop: true },
        Listener::Listening { listener, .. } => {
            down.push(Io::Close { entity: listener });
            Listener::Closing { error: None }
        }
        listener @ (Listener::Closing { .. } | Listener::Closed { .. }) => listener,
    };
}

/// Retires `id` if nothing names it any more: it leaves the deadlines and the
/// ready list.
fn conclude(proto: &mut Protocol, id: Id<Conn>) {
    let Some(conn) = proto.conns.get_mut(id) else {
        return;
    };
    if !conn.is_done() {
        return;
    }
    conn.retire();
    proto.conns.retire(id);
    proto.tables.deadlines.cancel(id);
    let _ready: bool = proto.tables.ready.remove(&id);
}
