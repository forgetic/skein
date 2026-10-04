//! The listener (io.md, 3.2): a socket made, bound and listening, which
//! accepts one connection at a time while its owner has room, announces each,
//! and closes once whatever it has in flight has settled.

use core::mem;

use skein_lib::{Env, Id, Queue, Slab, Token};

use crate::kernel::{self, Addr, Done, Fd, Op, Submit};
use crate::layer::{self, Entity, Flight, Io, Landed, Purpose, Tables, Timer};
use crate::limits::Limits;
use crate::records::{self, Error, Event};
use crate::stream;

/// A listener and the owner it tells.
#[derive(Debug)]
pub(crate) struct Listener {
    owner: Token,
    state: State,
}

#[derive(Debug)]
enum State {
    /// Its `Socket` in flight.
    Socket {
        addr: Addr,
    },
    /// Its `Bind` in flight.
    Binding {
        fd: Fd,
    },
    /// Its `Listen` in flight, bound to `addr`.
    Arming {
        fd: Fd,
        addr: Addr,
    },
    Listening(Listening),
    /// Closing: waiting for the accept, its cancels and the discards, then
    /// the close.
    Settling(Settling),
    /// Its `Close` in flight.
    Releasing,
    /// Terminal: holds nothing.
    Closed,
}

#[derive(Debug)]
struct Listening {
    fd: Fd,
    accept: Accept,
    /// Closes in flight of sockets it accepted that no one will own.
    discards: u32,
}

/// What a listening listener's accept is doing.
#[derive(Debug)]
enum Accept {
    /// An `Accept` in flight.
    Armed(Id<Flight>),
    /// The socket announced, waiting for its owner's answer: the owner has no
    /// room until it answers.
    Answering(Id<Entity>),
    /// Waiting to be armed: on the ready list for the next iteration's accept
    /// batch, or among the starved for a socket slot or a descriptor.
    Idle,
    /// An error io cannot retry, told as `Failed`: no more accepts.
    Stopped,
}

#[derive(Debug)]
struct Settling {
    fd: Fd,
    /// The `Accept` in flight, cancelled.
    accept: Option<Id<Flight>>,
    cancels: u32,
    discards: u32,
}

/// The listener a step is about, what it can see of io besides itself, and
/// what an accept found.
#[derive(Clone, Copy, Debug)]
struct Me {
    id: Id<Entity>,
    owner: Token,
    /// A socket slot is free, for an accept to fill.
    room: bool,
}

/// A completion of a listener's operation, decoded.
#[derive(Debug)]
enum Happened {
    Socket(Result<Fd, kernel::Error>),
    Bound(Result<Addr, kernel::Error>),
    Listened(Result<(), kernel::Error>),
    Accepted(Accepted),
    Discarded,
    Cancelled { target: Id<Flight>, result: Result<Done, kernel::Error> },
    Released,
}

/// An `Accept` completed: which, and what became of it.
#[derive(Debug)]
struct Accepted {
    flight: Id<Flight>,
    outcome: Outcome,
}

#[derive(Debug)]
enum Outcome {
    /// A socket, now in the slab, announced.
    Announced { socket: Id<Entity>, peer: Addr },
    /// A socket no slot was left for, to discard.
    NoSlot(Fd),
    /// A socket the owner no longer wants, the listener closing.
    Unwanted(Fd),
    /// No descriptor left: tried again when io gives one back, or at the
    /// retry deadline, as another process may give one back first.
    Starved,
    /// The kernel out of buffers, or an error it gave no name: tried again at
    /// the retry deadline, so that an error that stays does not spin.
    Backoff,
    /// The named network error of the connection it took (accept(2)): the
    /// next accept takes the next one, in the next iteration.
    Again,
    /// Stopped by io's cancel.
    Cancelled,
    /// An error the listener cannot retry.
    Stopped,
}

/// A listener whose `Socket` was just submitted.
pub(crate) const fn opened(owner: Token, addr: Addr) -> Listener {
    Listener { owner, state: State::Socket { addr } }
}

impl Listener {
    pub(crate) const fn is_closed(&self) -> bool {
        match self.state {
            State::Closed => true,
            State::Socket { .. }
            | State::Binding { .. }
            | State::Arming { .. }
            | State::Listening(_)
            | State::Settling(_)
            | State::Releasing => false,
        }
    }

    const fn is_listening(&self) -> bool {
        match self.state {
            State::Listening(_) => true,
            State::Socket { .. }
            | State::Binding { .. }
            | State::Arming { .. }
            | State::Settling(_)
            | State::Releasing
            | State::Closed => false,
        }
    }
}

/// A completion of one of the listener's operations.
pub(crate) fn landed(io: &mut Io, env: &Env<Limits>, landed: Landed, up: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let id = landed.entity;
    let happened = decode(io, landed);
    let room = !io.entities.is_full();
    let listener = borrow(&mut io.entities, id);
    let me = Me { id, owner: listener.owner, room };
    let state = mem::replace(&mut listener.state, State::Closed);
    listener.state = step(state, me, happened, env, &mut io.tables, up, subs);
    tidy(listener, id, &mut io.tables);
}

/// The listener `id` was on the ready list, or woken: an idle accept is armed.
pub(crate) fn resume(
    listener: &mut Listener,
    id: Id<Entity>,
    room: bool,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    let me = Me { id, owner: listener.owner, room };
    match &mut listener.state {
        State::Listening(listening) => match listening.accept {
            Accept::Idle => listening.accept = arm(listening.fd, listening.discards, me, env, tables, subs),
            // Readied, then moved on: nothing to do.
            Accept::Armed(_) | Accept::Answering(_) | Accept::Stopped => {}
        },
        State::Socket { .. }
        | State::Binding { .. }
        | State::Arming { .. }
        | State::Settling(_)
        | State::Releasing
        | State::Closed => {}
    }
    tidy(listener, id, tables);
}

/// The retry deadline passed: an accept that found no buffer or descriptor
/// is armed again, if it can be.
pub(crate) fn retried(
    listener: &mut Listener,
    id: Id<Entity>,
    room: bool,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    let me = Me { id, owner: listener.owner, room };
    match &mut listener.state {
        State::Listening(listening) => match listening.accept {
            Accept::Idle => listening.accept = arm(listening.fd, listening.discards, me, env, tables, subs),
            Accept::Armed(_) | Accept::Answering(_) | Accept::Stopped => {
                unreachable!("a listener's retry deadline runs only while its accept is idle")
            }
        },
        State::Socket { .. }
        | State::Binding { .. }
        | State::Arming { .. }
        | State::Settling(_)
        | State::Releasing
        | State::Closed => unreachable!("a listener's retry deadline runs only while it listens"),
    }
    tidy(listener, id, tables);
}

/// The retry deadline a listener's state implies: it runs only while its
/// accept is idle. Armed where an accept fails, and cancelled here once the
/// accept is armed again or the listener closes.
fn tidy(listener: &Listener, id: Id<Entity>, tables: &mut Tables) {
    let idle = match &listener.state {
        State::Listening(listening) => match listening.accept {
            Accept::Idle => true,
            Accept::Armed(_) | Accept::Answering(_) | Accept::Stopped => false,
        },
        State::Socket { .. }
        | State::Binding { .. }
        | State::Arming { .. }
        | State::Settling(_)
        | State::Releasing
        | State::Closed => false,
    };
    if !idle {
        tables.deadlines.cancel((id, Timer::Retry));
    }
}

/// The accept is tried again once the retry deadline passes.
fn back_off(id: Id<Entity>, env: &Env<Limits>, tables: &mut Tables) {
    let at = env.now.saturating_add(env.limits.retry);
    tables.deadlines.arm((id, Timer::Retry), at).expect("a retry deadline for every socket");
}

/// The owner answered the socket this listener announced: it has room again.
pub(crate) fn answered(
    listener: &mut Listener,
    id: Id<Entity>,
    socket: Id<Entity>,
    room: bool,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    let me = Me { id, owner: listener.owner, room };
    match &mut listener.state {
        State::Listening(listening) => match listening.accept {
            Accept::Answering(announced) => {
                assert!(announced == socket, "a listener waits for the answer to the socket it announced last");
                listening.accept = arm(listening.fd, listening.discards, me, env, tables, subs);
            }
            Accept::Armed(_) | Accept::Idle | Accept::Stopped => {
                unreachable!("a listener accepts no other socket until the one it announced is answered")
            }
        },
        // Closing since: it accepts no more.
        State::Settling(_) | State::Releasing | State::Closed => {}
        State::Socket { .. } | State::Binding { .. } | State::Arming { .. } => {
            unreachable!("a listener announces sockets only once it listens")
        }
    }
    tidy(listener, id, tables);
}

/// `Close` or `Abort`: a listener has nothing to flush, so both stop its
/// accept and close it.
pub(crate) fn close(listener: &mut Listener, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) {
    let state = mem::replace(&mut listener.state, State::Closed);
    listener.state = match state {
        State::Listening(Listening { fd, accept, discards }) => {
            tables.ready.forget(id);
            let accept = match accept {
                Accept::Armed(flight) => {
                    tables.cancel(subs, id, flight);
                    Some(flight)
                }
                Accept::Answering(_) | Accept::Idle | Accept::Stopped => None,
            };
            let cancels = match accept {
                Some(_) => 1,
                None => 0,
            };
            settle(Settling { fd, accept, cancels, discards }, id, tables, subs)
        }
        // Closing already, or closed and not yet reclaimed.
        state @ (State::Settling(_) | State::Releasing | State::Closed) => state,
        State::Socket { .. } | State::Binding { .. } | State::Arming { .. } => {
            unreachable!("a listener is named above only once it listens")
        }
    };
    tidy(listener, id, tables);
}

fn borrow(entities: &mut Slab<Entity>, id: Id<Entity>) -> &mut Listener {
    match entities.get_mut(id).expect("an entity outlives its operations") {
        Entity::Listener(listener) => listener,
        Entity::Stream(_) => unreachable!("a listener's operation is for a listener"),
    }
}

/// What a completion means to the listener. An accepted socket takes its
/// slot here, before the listener is borrowed from the same slab, if the
/// listener still listens.
fn decode(io: &mut Io, landed: Landed) -> Happened {
    let Landed { entity, flight, purpose, kind: _, result } = landed;
    match purpose {
        Purpose::Socket => Happened::Socket(match result {
            Ok(Done::Fd(fd)) => Ok(fd),
            Ok(Done::Nothing | Done::Count(_) | Done::Accepted { .. } | Done::Bound(_) | Done::Stat(_)) => {
                unreachable!("a socket answers with its descriptor")
            }
            Err(error) => Err(error),
        }),
        Purpose::Bind => Happened::Bound(match result {
            Ok(Done::Bound(addr)) => Ok(addr),
            Ok(Done::Nothing | Done::Count(_) | Done::Fd(_) | Done::Accepted { .. } | Done::Stat(_)) => {
                unreachable!("a bind answers with the address bound")
            }
            Err(error) => Err(error),
        }),
        Purpose::Listen => Happened::Listened(match result {
            Ok(Done::Nothing) => Ok(()),
            Ok(Done::Count(_) | Done::Fd(_) | Done::Accepted { .. } | Done::Bound(_) | Done::Stat(_)) => {
                unreachable!("a listen answers with nothing")
            }
            Err(error) => Err(error),
        }),
        Purpose::Accept => Happened::Accepted(Accepted { flight, outcome: outcome(io, entity, result) }),
        // A descriptor is closed whatever its close answers.
        Purpose::Discard => Happened::Discarded,
        Purpose::Close => Happened::Released,
        Purpose::Cancel(target) => Happened::Cancelled { target, result },
        Purpose::Connect | Purpose::Recv | Purpose::Send | Purpose::Shutdown => {
            unreachable!("a listener never connects or streams")
        }
    }
}

/// What became of an accept, its socket in the slab if the listener still
/// listens and a slot is free.
fn outcome(io: &mut Io, listener: Id<Entity>, result: Result<Done, kernel::Error>) -> Outcome {
    let listening = match io.entities.get(listener).expect("an entity outlives its operations") {
        Entity::Listener(listener) => listener.is_listening(),
        Entity::Stream(_) => unreachable!("an accept is a listener's"),
    };
    match result {
        Ok(Done::Accepted { fd, peer }) => {
            if !listening {
                return Outcome::Unwanted(fd);
            }
            match io.entities.insert(Entity::Stream(stream::announced(fd, listener))) {
                Ok(socket) => Outcome::Announced { socket, peer },
                Err(_refused) => Outcome::NoSlot(fd),
            }
        }
        Ok(Done::Nothing | Done::Count(_) | Done::Fd(_) | Done::Bound(_) | Done::Stat(_)) => {
            unreachable!("an accept answers with a socket and its peer")
        }
        Err(kernel::Error::TooManyOpenFiles) => Outcome::Starved,
        // A failed accept on Linux may carry the network error of the
        // connection it took (accept(2)): the next may do.
        Err(kernel::Error::Reset | kernel::Error::TimedOut | kernel::Error::Unreachable) => Outcome::Again,
        Err(kernel::Error::NoBufferSpace | kernel::Error::Other(_)) => Outcome::Backoff,
        Err(kernel::Error::Cancelled) => Outcome::Cancelled,
        Err(
            kernel::Error::Refused
            | kernel::Error::BrokenPipe
            | kernel::Error::NotConnected
            | kernel::Error::AddressInUse
            | kernel::Error::AddressNotAvailable
            | kernel::Error::InvalidArgument,
        ) => Outcome::Stopped,
        Err(kernel::Error::TooLate) => unreachable!("only a cancel is too late"),
        Err(
            kernel::Error::NotFound
            | kernel::Error::Exists
            | kernel::Error::NotADirectory
            | kernel::Error::IsADirectory
            | kernel::Error::NotEmpty
            | kernel::Error::Permission
            | kernel::Error::NoSpace
            | kernel::Error::ReadOnly
            | kernel::Error::TooManyLinks
            | kernel::Error::NameTooLong
            | kernel::Error::Escape,
        ) => unreachable!("an operation on sockets never fails with a file's error (skein_io::kernel)"),
    }
}

/// The transition table of io.md, 3.2.
fn step(
    state: State,
    me: Me,
    happened: Happened,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> State {
    match state {
        State::Socket { addr } => match happened {
            Happened::Socket(Ok(fd)) => made(fd, addr, me, tables, subs),
            Happened::Socket(Err(error)) => unmade(me, error, up),
            Happened::Bound(_)
            | Happened::Listened(_)
            | Happened::Accepted(_)
            | Happened::Discarded
            | Happened::Cancelled { .. }
            | Happened::Released => unreachable!("a listener making its socket has nothing else in flight"),
        },
        State::Binding { fd } => match happened {
            Happened::Bound(Ok(addr)) => bound(fd, addr, me, env, tables, subs),
            Happened::Bound(Err(error)) => failed(fd, me, records::setup_error(error), tables, up, subs),
            Happened::Socket(_)
            | Happened::Listened(_)
            | Happened::Accepted(_)
            | Happened::Discarded
            | Happened::Cancelled { .. }
            | Happened::Released => unreachable!("a listener binding has nothing else in flight"),
        },
        State::Arming { fd, addr } => match happened {
            Happened::Listened(Ok(())) => listening(fd, addr, me, env, tables, up, subs),
            Happened::Listened(Err(error)) => failed(fd, me, records::setup_error(error), tables, up, subs),
            Happened::Socket(_)
            | Happened::Bound(_)
            | Happened::Accepted(_)
            | Happened::Discarded
            | Happened::Cancelled { .. }
            | Happened::Released => unreachable!("a listener arming has nothing else in flight"),
        },
        State::Listening(listening) => match happened {
            Happened::Accepted(accepted) => {
                State::Listening(accept_done(listening, accepted, me, env, tables, up, subs))
            }
            Happened::Discarded => State::Listening(discarded(listening, tables)),
            Happened::Socket(_)
            | Happened::Bound(_)
            | Happened::Listened(_)
            | Happened::Cancelled { .. }
            | Happened::Released => unreachable!("a listener listening has only accepts and discards in flight"),
        },
        State::Settling(settling) => match happened {
            Happened::Accepted(accepted) => settle_accept(settling, accepted, me, tables, subs),
            Happened::Discarded => settle_discard(settling, me, tables, subs),
            Happened::Cancelled { target, result } => settle_cancel(settling, target, result, me, tables, subs),
            Happened::Socket(_) | Happened::Bound(_) | Happened::Listened(_) | Happened::Released => {
                unreachable!("a listener settling waits for its accept, its cancels and its discards")
            }
        },
        State::Releasing => match happened {
            Happened::Released => {
                up.push(Event::Closed { owner: me.owner });
                State::Closed
            }
            Happened::Socket(_)
            | Happened::Bound(_)
            | Happened::Listened(_)
            | Happened::Accepted(_)
            | Happened::Discarded
            | Happened::Cancelled { .. } => unreachable!("a listener releasing has only its close in flight"),
        },
        State::Closed => unreachable!("a closed listener has nothing in flight"),
    }
}

/// Socket ok: bind it.
fn made(fd: Fd, addr: Addr, me: Me, tables: &mut Tables, subs: &mut Queue<Submit>) -> State {
    let _bind: Id<Flight> = tables.submit(subs, me.id, Purpose::Bind, Op::Bind { fd, addr });
    State::Binding { fd }
}

/// Socket failed: nothing was made, so nothing is closed.
fn unmade(me: Me, error: kernel::Error, up: &mut Queue<Event>) -> State {
    up.push(Event::Failed { owner: me.owner, error: records::setup_error(error) });
    up.push(Event::Closed { owner: me.owner });
    State::Closed
}

/// Bind ok: listen.
fn bound(fd: Fd, addr: Addr, me: Me, env: &Env<Limits>, tables: &mut Tables, subs: &mut Queue<Submit>) -> State {
    let backlog = env.limits.backlog;
    let _listen: Id<Flight> = tables.submit(subs, me.id, Purpose::Listen, Op::Listen { fd, backlog });
    State::Arming { fd, addr }
}

/// Bind or Listen failed: told, and the socket closed.
fn failed(fd: Fd, me: Me, error: Error, tables: &mut Tables, up: &mut Queue<Event>, subs: &mut Queue<Submit>) -> State {
    up.push(Event::Failed { owner: me.owner, error });
    release(fd, me.id, tables, subs)
}

/// Listen ok: told, with the address bound, and the first accept armed.
fn listening(
    fd: Fd,
    addr: Addr,
    me: Me,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> State {
    up.push(Event::Listening { owner: me.owner, listener: me.id.token(), addr });
    let accept = arm(fd, 0, me, env, tables, subs);
    State::Listening(Listening { fd, accept, discards: 0 })
}

/// An accept completed while listening.
fn accept_done(
    listening: Listening,
    accepted: Accepted,
    me: Me,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> Listening {
    let Listening { fd, accept, mut discards } = listening;
    match accept {
        Accept::Armed(flight) => assert!(flight == accepted.flight, "the accept that completed is the one armed"),
        Accept::Answering(_) | Accept::Idle | Accept::Stopped => unreachable!("an accept completes while armed"),
    }
    let accept = match accepted.outcome {
        Outcome::Announced { socket, peer } => {
            up.push(Event::Accepted { owner: me.owner, socket: socket.token(), peer });
            Accept::Answering(socket)
        }
        // A connect took the slot it was armed for: io refuses the socket,
        // and waits for its descriptor back before it accepts again.
        Outcome::NoSlot(socket) => {
            let _discard: Id<Flight> = tables.submit(subs, me.id, Purpose::Discard, Op::Close { fd: socket });
            discards = discards.checked_add(1).expect("one discard per accept");
            arm(fd, discards, me, env, tables, subs)
        }
        // Woken when io gives a descriptor back; but the descriptors may be
        // the system's, which another process gives back (ENFILE): retried
        // at the deadline too.
        Outcome::Starved => {
            tables.ready.starve(me.id);
            back_off(me.id, env, tables);
            Accept::Idle
        }
        Outcome::Backoff => {
            back_off(me.id, env, tables);
            Accept::Idle
        }
        Outcome::Again => {
            tables.ready.mark(me.id);
            Accept::Idle
        }
        Outcome::Stopped => {
            up.push(Event::Failed { owner: me.owner, error: Error::Other });
            Accept::Stopped
        }
        Outcome::Unwanted(_) | Outcome::Cancelled => {
            unreachable!("a listener listening still wants its sockets, and never cancels its accept")
        }
    };
    Listening { fd, accept, discards }
}

/// A discarded socket is closed: a descriptor is free, for the starved.
fn discarded(listening: Listening, tables: &mut Tables) -> Listening {
    let discards = listening.discards.checked_sub(1).expect("a discard in flight completed");
    tables.ready.wake();
    Listening { discards, ..listening }
}

/// Arms an accept if the listener's owner has room, and io a socket slot, no
/// discard in flight and room in this iteration's batch; otherwise the
/// listener waits, starved or until the next iteration.
fn arm(fd: Fd, discards: u32, me: Me, env: &Env<Limits>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Accept {
    if !me.room || discards > 0 {
        tables.ready.starve(me.id);
        return Accept::Idle;
    }
    if tables.armed >= env.limits.accepts {
        tables.ready.mark(me.id);
        return Accept::Idle;
    }
    tables.armed = tables.armed.checked_add(1).expect("no more accepts than the batch");
    Accept::Armed(tables.submit(subs, me.id, Purpose::Accept, Op::Accept { fd }))
}

/// The accept completed while closing: a socket it made is discarded.
fn settle_accept(
    settling: Settling,
    accepted: Accepted,
    me: Me,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> State {
    assert!(settling.accept == Some(accepted.flight), "the accept that completed is the one cancelled");
    let discards = match accepted.outcome {
        Outcome::Unwanted(socket) => {
            let _discard: Id<Flight> = tables.submit(subs, me.id, Purpose::Discard, Op::Close { fd: socket });
            settling.discards.checked_add(1).expect("one discard per accept")
        }
        Outcome::Starved | Outcome::Backoff | Outcome::Again | Outcome::Cancelled | Outcome::Stopped => {
            settling.discards
        }
        Outcome::Announced { .. } | Outcome::NoSlot(_) => unreachable!("a closing listener announces nothing"),
    };
    settle(Settling { accept: None, discards, ..settling }, me.id, tables, subs)
}

fn settle_discard(settling: Settling, me: Me, tables: &mut Tables, subs: &mut Queue<Submit>) -> State {
    let discards = settling.discards.checked_sub(1).expect("a discard in flight completed");
    tables.ready.wake();
    settle(Settling { discards, ..settling }, me.id, tables, subs)
}

/// A cancel completed: one that could not be submitted is tried again while
/// its accept still waits (kernel.md, 5).
fn settle_cancel(
    settling: Settling,
    target: Id<Flight>,
    result: Result<Done, kernel::Error>,
    me: Me,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> State {
    let mut cancels = settling.cancels.checked_sub(1).expect("a cancel in flight completed");
    if layer::unsubmitted(result) && settling.accept == Some(target) {
        tables.cancel(subs, me.id, target);
        cancels = cancels.checked_add(1).expect("one cancel per target");
    }
    settle(Settling { cancels, ..settling }, me.id, tables, subs)
}

/// Closes the listener once nothing of it is in flight.
fn settle(settling: Settling, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> State {
    if settling.accept.is_none() && settling.cancels == 0 && settling.discards == 0 {
        return release(settling.fd, id, tables, subs);
    }
    State::Settling(settling)
}

fn release(fd: Fd, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> State {
    let _close: Id<Flight> = tables.submit(subs, id, Purpose::Close, Op::Close { fd });
    State::Releasing
}
