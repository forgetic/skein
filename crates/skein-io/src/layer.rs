//! io's state and its entry points (io.md, 2): the slab of sockets, the
//! operations in flight, the close deadlines, the ready list and the
//! refusals; `resume`, `up`, `fire` and `down`, which the loop calls in that
//! order within an iteration, and `Io::reclaim` at its end.

use skein_lib::stream::{Down, OutputDown};
use skein_lib::{Deadlines, Env, Id, Queue, Set, Slab, Time, Token};

use crate::kernel::{self, Addr, Complete, Done, Family, Fd, Op, Submit, Way};
use crate::limits::{self, Limits};
use crate::listener::{self, Listener};
use crate::pipe::{self, Pipe};
use crate::process::{self, Child};
use crate::records::{Error, Event, Request};
use crate::signals::{self, Signals};
use crate::stream::{self, Stream};

/// io's state: every socket and every operation in flight.
#[derive(Debug)]
pub struct Io {
    pub(crate) entities: Slab<Entity>,
    pub(crate) tables: Tables,
}

/// A socket of either kind (io.md, 3.1): one slab, one namespace of tokens.
#[derive(Debug)]
pub(crate) enum Entity {
    Listener(Listener),
    Stream(Stream),
    Pipe(Pipe),
    Child(Child),
    Signals(Signals),
}

/// What io keeps beside its entities, which the handlers of an entity touch
/// while it is borrowed from its slab.
#[derive(Debug)]
pub(crate) struct Tables {
    /// The operations in flight, each for its entity.
    pub(crate) flights: Slab<Flight>,
    /// Each entity's deadlines: a graceful close's, and a retry's.
    pub(crate) deadlines: Deadlines<(Id<Entity>, Timer)>,
    pub(crate) ready: Ready,
    /// The owners of the `Listen`s and `Connect`s refused for want of a
    /// socket slot, oldest first, until `resume` tells them.
    pub(crate) refused: Queue<Token>,
    /// Accepts armed in this iteration, against `Limits::accepts`.
    pub(crate) armed: u32,
}

/// An operation in flight: the entity it is for, and what for.
#[derive(Debug)]
pub(crate) struct Flight {
    pub(crate) entity: Id<Entity>,
    pub(crate) purpose: Purpose,
}

/// What an operation is for. A `Discard` closes a socket a listener accepted
/// and no one will own; a `Cancel` names the operation it stops.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Purpose {
    Socket,
    Bind,
    Listen,
    Accept,
    Connect,
    Recv,
    Send,
    Shutdown,
    Close,
    Discard,
    Spawn,
    Wait,
    Signal,
    ReadSignal,
    PipeRead,
    PipeWrite,
    Cancel(Id<Flight>),
}

/// What an entity's deadline is for: a stream's graceful close (io.md, 3), or
/// a retry of what found the kernel out of buffers or descriptors (io.md,
/// 3.2 and 3.3). An entity has at most one of each.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) enum Timer {
    Close,
    Retry,
}

/// A completion, as an entity's handlers take it: the operation's record
/// handed back, and what came of it.
#[derive(Debug)]
pub(crate) struct Landed {
    pub(crate) entity: Id<Entity>,
    pub(crate) flight: Id<Flight>,
    pub(crate) purpose: Purpose,
    pub(crate) kind: Op,
    pub(crate) result: Result<Done, kernel::Error>,
}

/// The entities with something to do at the start of the next up pass
/// (programming-model.md, section 2), and the listeners waiting for a socket
/// slot or a descriptor.
///
/// An entity readied in an iteration goes on `next`, which the reclaim point
/// promotes to `now`, so that `resume` never takes the same entity twice in
/// one stage.
#[derive(Debug)]
pub(crate) struct Ready {
    now: Set<Id<Entity>>,
    next: Set<Id<Entity>>,
    starved: Set<Id<Entity>>,
}

impl Io {
    /// io with room for `limits`, which must be usable.
    #[must_use]
    pub fn new(limits: &Limits) -> Io {
        assert!(limits.is_usable(), "io runs only under usable limits (Limits::is_usable)");
        let flights = limits::flights(limits).expect("the operation table's size fits a u32");
        Io {
            entities: Slab::with_capacity(limits.sockets),
            tables: Tables {
                flights: Slab::with_capacity(flights),
                deadlines: Deadlines::with_capacity(limits::timers(limits).expect("two timers a socket fit a u32")),
                ready: Ready::with_capacity(limits.sockets),
                refused: Queue::with_capacity(limits.refusals),
                armed: 0,
            },
        }
    }

    /// Takes an inherited readable pipe descriptor at startup, returning its
    /// stream token. From then on the descriptor belongs to io and its
    /// `Stream`/`Close`/`Abort` vocabulary (io.md, sections 3 and 6). If the
    /// entity slab is full, the caller retains the descriptor.
    pub fn adopt_read_pipe(&mut self, fd: Fd) -> Result<Token, Fd> {
        self.adopt_pipe(fd, Way::Out)
    }

    /// Takes an inherited writable pipe descriptor at startup, with the same
    /// stream and ownership contract as [`Io::adopt_read_pipe`].
    pub fn adopt_write_pipe(&mut self, fd: Fd) -> Result<Token, Fd> {
        self.adopt_pipe(fd, Way::In)
    }

    fn adopt_pipe(&mut self, fd: Fd, way: Way) -> Result<Token, Fd> {
        match self.entities.insert(Entity::Pipe(Pipe::inherited(fd, way))) {
            Ok(id) => {
                self.tables.ready.mark(id);
                Ok(id.token())
            }
            Err(_) => Err(fd),
        }
    }

    /// Takes the signalfd opened after blocking termination signals at
    /// startup (shell.md, section 6). Each read becomes `Event::Shutdown`;
    /// `Close` or `Abort` on the returned token settles the read and closes
    /// the descriptor. On a full slab the caller retains the descriptor.
    pub fn adopt_signals(&mut self, fd: Fd) -> Result<Token, Fd> {
        match self.entities.insert(Entity::Signals(Signals::new(fd))) {
            Ok(id) => {
                self.tables.ready.mark(id);
                Ok(id.token())
            }
            Err(_) => Err(fd),
        }
    }

    /// Whether io can take another request: it holds a refusal for each
    /// `Listen` or `Connect` it refuses, until the next up pass. The loop
    /// hands io a request only while it can, as it reserves room in the
    /// submissions (io.md, 2).
    #[must_use]
    pub fn takes(&self) -> bool {
        self.tables.refused.room() > 0
    }

    /// Whether `resume` has something to do. While it has, the loop calls it
    /// at the start of io's stage, before any completion.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        !self.tables.refused.is_empty() || self.tables.ready.is_ready()
    }

    /// When the earliest deadline falls due: a graceful close's, or a
    /// retry's.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.tables.deadlines.next()
    }

    /// Whether a deadline is due at `now`. While one is, the loop calls
    /// `fire`, after the completions.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        match self.tables.deadlines.next() {
            Some(at) => at <= now,
            None => false,
        }
    }

    /// Sockets present, retired ones included until they are reclaimed.
    #[must_use]
    pub fn sockets(&self) -> u32 {
        self.entities.len()
    }

    /// Operations in flight, completed ones included until they are
    /// reclaimed.
    #[must_use]
    pub fn in_flight(&self) -> u32 {
        self.tables.flights.len()
    }

    /// Whether io holds nothing: no socket, no operation, no refusal untold,
    /// nothing ready. True once every entity is closed and reclaimed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
            && self.tables.flights.is_empty()
            && self.tables.deadlines.is_empty()
            && self.tables.refused.is_empty()
            && self.tables.ready.is_empty()
    }

    /// The reclaim point: frees what closed in this iteration, and renews the
    /// accept batch.
    pub fn reclaim(&mut self) {
        self.entities.reclaim();
        self.tables.flights.reclaim();
        self.tables.ready.promote();
        self.tables.armed = 0;
    }
}

impl Tables {
    /// Submits `op` for `entity`, naming it by a new flight's token.
    pub(crate) fn submit(
        &mut self,
        subs: &mut Queue<Submit>,
        entity: Id<Entity>,
        purpose: Purpose,
        op: Op,
    ) -> Id<Flight> {
        let flight = self
            .flights
            .insert(Flight { entity, purpose })
            .expect("the operation table holds twice the most an iteration has in flight");
        subs.push(Submit { op: flight.token(), kind: op });
        flight
    }

    /// Asks the kernel to stop `target`, an operation of `entity` that waits.
    pub(crate) fn cancel(&mut self, subs: &mut Queue<Submit>, entity: Id<Entity>, target: Id<Flight>) {
        let _cancel: Id<Flight> =
            self.submit(subs, entity, Purpose::Cancel(target), Op::Cancel { target: target.token() });
    }
}

impl Ready {
    fn with_capacity(sockets: u32) -> Ready {
        Ready {
            now: Set::with_capacity(sockets),
            next: Set::with_capacity(sockets),
            starved: Set::with_capacity(sockets),
        }
    }

    fn is_ready(&self) -> bool {
        !self.now.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.now.is_empty() && self.next.is_empty() && self.starved.is_empty()
    }

    /// `id` has something to do in the next up pass.
    pub(crate) fn mark(&mut self, id: Id<Entity>) {
        let _new: bool = self.next.insert(id).expect("room on the ready list for every socket");
    }

    /// The listener `id` waits for a socket slot or a descriptor.
    pub(crate) fn starve(&mut self, id: Id<Entity>) {
        let _new: bool = self.starved.insert(id).expect("room among the starved for every socket");
    }

    /// io freed a socket slot or a descriptor: the starved try again in the
    /// next up pass.
    pub(crate) fn wake(&mut self) {
        for _ in 0..self.starved.capacity() {
            let Some(id) = self.starved.pop_first() else {
                break;
            };
            self.mark(id);
        }
    }

    /// `id` is off every list.
    pub(crate) fn forget(&mut self, id: Id<Entity>) {
        let _now: bool = self.now.remove(&id);
        let _next: bool = self.next.remove(&id);
        let _starved: bool = self.starved.remove(&id);
    }

    fn pop(&mut self) -> Option<Id<Entity>> {
        self.now.pop_first()
    }

    fn promote(&mut self) {
        for _ in 0..self.next.capacity() {
            let Some(id) = self.next.pop_first() else {
                break;
            };
            let _new: bool = self.now.insert(id).expect("room on the ready list for every socket");
        }
    }
}

/// Takes one entry of the ready list, emitting at most
/// [`MAX_OUT_RESUME`](crate::MAX_OUT_RESUME): a refusal is told; a connect is
/// told `Connecting`; an open stream is delivered what its demand asked for;
/// an idle listener arms its accept.
pub fn resume(io: &mut Io, env: &Env<Limits>, up: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    if let Some(owner) = io.tables.refused.pop() {
        up.push(Event::Failed { owner, error: Error::Busy });
        up.push(Event::Closed { owner });
        return;
    }
    let Some(id) = io.tables.ready.pop() else {
        return;
    };
    let room = !io.entities.is_full();
    match io.entities.get_mut(id).expect("retiring an entity takes it off the ready list") {
        Entity::Listener(listener) => listener::resume(listener, id, room, env, &mut io.tables, subs),
        Entity::Stream(stream) => stream::resume(stream, id, env, &mut io.tables, up, subs),
        Entity::Pipe(pipe) => pipe::resume(pipe, id, env, &mut io.tables, up, subs),
        Entity::Child(_) => {}
        Entity::Signals(signals) => signals::resume(signals, id, &mut io.tables, subs),
    }
}

/// Takes one completion, emitting at most [`MAX_OUT_UP`](crate::MAX_OUT_UP).
/// The loop hands io completions only once its ready list is empty.
///
/// A completion names an operation in flight, and its entity outlives it: a
/// token travelling up is never stale (programming-model.md, 5.2).
pub fn up(io: &mut Io, env: &Env<Limits>, complete: Complete, up: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let Complete { op, kind, result } = complete;
    let flight = Id::<Flight>::from_token(op);
    let Flight { entity, purpose } = *io.tables.flights.get(flight).expect("a completion names an operation in flight");
    io.tables.flights.retire(flight);
    let landed = Landed { entity, flight, purpose, kind, result };
    match io.entities.get(entity).expect("an entity outlives its operations") {
        Entity::Stream(_) => {
            let Entity::Stream(stream) = io.entities.get_mut(entity).expect("live") else { unreachable!() };
            stream::landed(stream, landed, env, &mut io.tables, up, subs);
        }
        Entity::Listener(_) => listener::landed(io, env, landed, up, subs),
        Entity::Pipe(_) => {
            let Entity::Pipe(pipe) = io.entities.get_mut(entity).expect("live") else { unreachable!() };
            pipe::landed(pipe, landed, env, &mut io.tables, up, subs);
        }
        Entity::Child(_) => process::landed(io, landed, up, subs),
        Entity::Signals(_) => {
            let Entity::Signals(signals) = io.entities.get_mut(entity).expect("live") else { unreachable!() };
            signals::landed(signals, entity, landed, &mut io.tables, up, subs);
        }
    }
    conclude(io, entity, subs);
}

/// Fires the earliest deadline due at `env.now`, if one is, emitting at most
/// [`MAX_OUT_FIRE`](crate::MAX_OUT_FIRE): a graceful close it bounded becomes
/// an abort; what waited for a retry is submitted again.
pub fn fire(io: &mut Io, env: &Env<Limits>, up: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let Some((id, timer)) = io.tables.deadlines.expire(env.now) else {
        return;
    };
    let room = !io.entities.is_full();
    match io.entities.get_mut(id).expect("an entity's deadlines are cancelled when it is retired") {
        Entity::Stream(stream) => match timer {
            Timer::Close => stream::expired(stream, id, env, &mut io.tables, up, subs),
            Timer::Retry => stream::retried(stream, id, env, &mut io.tables, up, subs),
        },
        Entity::Listener(listener) => match timer {
            Timer::Retry => listener::retried(listener, id, room, env, &mut io.tables, subs),
            Timer::Close => unreachable!("only a stream closes gracefully"),
        },
        Entity::Pipe(_) | Entity::Child(_) | Entity::Signals(_) => {
            unreachable!("processes and signals have no io deadline")
        }
    }
    conclude(io, id, subs);
}

/// Takes one request, emitting at most [`MAX_OUT_DOWN`](crate::MAX_OUT_DOWN).
/// The loop hands io a request only while it [`takes`](Io::takes) one.
///
/// A request naming an entity that is gone is dropped: a token travelling
/// down may be stale (programming-model.md, 5.2). Events it causes wait for
/// the next up pass, held as the state of the entity that will tell them.
pub fn down(io: &mut Io, env: &Env<Limits>, request: Request, subs: &mut Queue<Submit>) {
    match request {
        Request::Listen { owner, addr } => listen(io, owner, addr, subs),
        Request::Connect { owner, addr } => connect(io, owner, addr, subs),
        Request::Bind { socket, owner } => answer(io, env, socket, Some(owner), subs),
        Request::Reject { socket } => answer(io, env, socket, None, subs),
        Request::Stream { stream, down } => stream_request(io, env, stream, down, subs),
        Request::Output { stream, down } => output_request(io, env, stream, down, subs),
        Request::Spawn { owner, spawn } => process::spawn(io, owner, spawn, subs),
        Request::Signal { child, signal } => process::signal(io, child, signal, subs),
        Request::Close { entity } => close(io, env, entity, false, subs),
        Request::Abort { entity } => close(io, env, entity, true, subs),
    }
}

fn listen(io: &mut Io, owner: Token, addr: Addr, subs: &mut Queue<Submit>) {
    let Ok(id) = io.entities.insert(Entity::Listener(listener::opened(owner, addr))) else {
        io.tables.refused.push(owner);
        return;
    };
    let _socket: Id<Flight> = io.tables.submit(subs, id, Purpose::Socket, Op::Socket { family: Family::of(&addr) });
}

fn connect(io: &mut Io, owner: Token, addr: Addr, subs: &mut Queue<Submit>) {
    let Ok(id) = io.entities.insert(Entity::Stream(stream::opened(owner, addr))) else {
        io.tables.refused.push(owner);
        return;
    };
    // Connecting is told from the ready list, before the socket's completion.
    io.tables.ready.mark(id);
    let _socket: Id<Flight> = io.tables.submit(subs, id, Purpose::Socket, Op::Socket { family: Family::of(&addr) });
}

/// `Bind` (an owner) or `Reject` (none) of an announced socket; then its
/// listener may accept again.
fn answer(io: &mut Io, env: &Env<Limits>, socket: Token, owner: Option<Token>, subs: &mut Queue<Submit>) {
    let id = Id::<Entity>::from_token(socket);
    let Some(entity) = io.entities.get_mut(id) else {
        return;
    };
    let stream = match entity {
        Entity::Stream(stream) => stream,
        Entity::Listener(_) | Entity::Pipe(_) | Entity::Child(_) | Entity::Signals(_) => {
            unreachable!("an answer names a socket announced to its owner")
        }
    };
    let listener = stream::answer(stream, id, owner, env, &mut io.tables, subs);
    let room = !io.entities.is_full();
    match io.entities.get_mut(listener) {
        Some(Entity::Listener(listening)) => {
            listener::answered(listening, listener, id, room, env, &mut io.tables, subs);
        }
        Some(Entity::Stream(_) | Entity::Pipe(_) | Entity::Child(_) | Entity::Signals(_)) => {
            unreachable!("a handle names the kind of entity it was made for")
        }
        // Closed and reclaimed since it announced the socket.
        None => {}
    }
}

fn stream_request(io: &mut Io, env: &Env<Limits>, token: Token, down: Down, subs: &mut Queue<Submit>) {
    let id = Id::<Entity>::from_token(token);
    let Some(entity) = io.entities.get_mut(id) else {
        return;
    };
    match entity {
        Entity::Stream(stream) => stream::request(stream, id, down, env, &mut io.tables, subs),
        Entity::Pipe(pipe) => pipe::request(pipe, id, down, env, &mut io.tables, subs),
        Entity::Listener(_) | Entity::Child(_) | Entity::Signals(_) => unreachable!("a stream request names a stream"),
    }
}

fn output_request(io: &mut Io, env: &Env<Limits>, token: Token, down: OutputDown, subs: &mut Queue<Submit>) {
    let id = Id::<Entity>::from_token(token);
    let Some(entity) = io.entities.get_mut(id) else { return };
    match entity {
        Entity::Stream(stream) => stream::output_request(stream, id, down, env, &mut io.tables, subs),
        Entity::Pipe(pipe) => pipe::output_request(pipe, id, down, env, &mut io.tables, subs),
        Entity::Listener(_) | Entity::Child(_) | Entity::Signals(_) => {}
    }
}

fn close(io: &mut Io, env: &Env<Limits>, token: Token, abort: bool, subs: &mut Queue<Submit>) {
    let id = Id::<Entity>::from_token(token);
    if let Some(Entity::Child(_)) = io.entities.get(id) {
        process::close(io, id, subs);
        return;
    }
    let Some(entity) = io.entities.get_mut(id) else {
        return;
    };
    match entity {
        Entity::Listener(listener) => listener::close(listener, id, &mut io.tables, subs),
        Entity::Stream(stream) => stream::close(stream, id, abort, env, &mut io.tables, subs),
        Entity::Pipe(pipe) => pipe::close(pipe, id, abort, &mut io.tables, subs),
        Entity::Child(_) => unreachable!("handled above"),
        Entity::Signals(signals) => signals::close(signals, id, &mut io.tables, subs),
    }
}

/// Retires `id` if its last transition closed it: it leaves every list, and
/// the listeners starved for a slot try again.
pub(crate) fn conclude(io: &mut Io, id: Id<Entity>, subs: &mut Queue<Submit>) {
    let closed = match io.entities.get(id) {
        Some(Entity::Listener(listener)) => listener.is_closed(),
        Some(Entity::Stream(stream)) => stream.is_closed(),
        Some(Entity::Pipe(pipe)) => pipe.is_closed(),
        Some(Entity::Child(child)) => child.is_closed(),
        Some(Entity::Signals(signals)) => signals.is_closed(),
        None => false,
    };
    if !closed {
        return;
    }
    let child = match io.entities.get(id) {
        Some(Entity::Pipe(pipe)) => pipe.child(),
        _ => None,
    };
    io.entities.retire(id);
    io.tables.ready.forget(id);
    io.tables.deadlines.cancel((id, Timer::Close));
    io.tables.deadlines.cancel((id, Timer::Retry));
    io.tables.ready.wake();
    if let Some(child) = child {
        process::release(io, child, subs);
    }
}

/// Whether a cancel's answer says the backend did not submit it, so that its
/// target runs on. Stopped (`Ok`) or too late, its target completes by itself.
pub(crate) fn unsubmitted(result: Result<Done, kernel::Error>) -> bool {
    match result {
        Ok(Done::Nothing) | Err(kernel::Error::TooLate) => false,
        Err(kernel::Error::InvalidArgument | kernel::Error::Other(_)) => true,
        Ok(
            Done::Count(_)
            | Done::Fd(_)
            | Done::Accepted { .. }
            | Done::Bound(_)
            | Done::Stat(_)
            | Done::Spawned { .. }
            | Done::Exit(_)
            | Done::ServiceSignal(_),
        ) => {
            unreachable!("a cancel answers with nothing")
        }
        Err(
            kernel::Error::Refused
            | kernel::Error::Reset
            | kernel::Error::BrokenPipe
            | kernel::Error::NotConnected
            | kernel::Error::AddressInUse
            | kernel::Error::AddressNotAvailable
            | kernel::Error::Unreachable
            | kernel::Error::TimedOut
            | kernel::Error::TooManyOpenFiles
            | kernel::Error::NoBufferSpace
            | kernel::Error::Cancelled
            | kernel::Error::NotFound
            | kernel::Error::Exists
            | kernel::Error::NotADirectory
            | kernel::Error::IsADirectory
            | kernel::Error::NotEmpty
            | kernel::Error::Permission
            | kernel::Error::NoSpace
            | kernel::Error::ReadOnly
            | kernel::Error::TooManyLinks
            | kernel::Error::NameTooLong
            | kernel::Error::Escape
            | kernel::Error::NotAFile,
        ) => unreachable!("a cancel fails only too late or unsubmitted (kernel.md, 5)"),
    }
}
