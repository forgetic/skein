//! The stream (io.md, 3.3): a socket connecting or accepted, then open, with
//! one receive and one send in flight under its caps, then closing,
//! gracefully or at once, settling what it has in flight before its close.

use alloc::boxed::Box;
use core::mem;

use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Id, Intake, Queue, Token, bytes};

use crate::kernel::{self, Addr, Done, Fd, Op, Submit};
use crate::layer::{self, Entity, Flight, Landed, Purpose, Tables, Timer};
use crate::limits::Limits;
use crate::records::{self, Event};

/// A socket that is, or will be, one end of a stream.
#[derive(Debug)]
pub(crate) enum Stream {
    /// A connect whose `Socket` is in flight, `Connecting` not yet told: on
    /// the ready list.
    Opening {
        owner: Token,
        addr: Addr,
    },
    /// A connect whose `Socket` is in flight, `Connecting` told.
    Socket {
        owner: Token,
        addr: Addr,
    },
    /// Its `Connect` in flight.
    Connecting {
        owner: Token,
        fd: Fd,
        connect: Id<Flight>,
    },
    /// Accepted by `listener`, announced to its owner, which binds or rejects
    /// it. Nothing in flight.
    Announced {
        fd: Fd,
        listener: Id<Entity>,
    },
    Open(Open),
    Broken(Broken),
    Closing(Closing),
    Settling(Settling),
    /// Its `Close` in flight; the owner it tells, unless it was rejected.
    Releasing {
        owner: Option<Token>,
    },
    /// Terminal: holds nothing.
    Closed,
}

/// A stream running both ways for its owner.
#[derive(Debug)]
pub(crate) struct Open {
    owner: Token,
    fd: Fd,
    /// What arrived and was not yet demanded.
    intake: Intake,
    demand: Demand,
    /// The room the owner holds: what the last `Room` granted, less what it
    /// sent since. Each `Send` fits within it (lib.md, 7).
    granted: u32,
    reader: Reader,
    writer: Writer,
    output: Output,
}

/// What the owner's state needs (lib.md, 7): a read and output room, answered
/// once, by the bytes or the room, which ends both.
#[derive(Clone, Copy, Debug)]
struct Demand {
    read: Read,
    room: u32,
}

/// The receiving side of an open stream.
#[derive(Clone, Copy, Debug)]
enum Reader {
    /// A `Recv` in flight.
    Receiving(Id<Flight>),
    /// No `Recv` in flight: the intake is full.
    Full,
    /// No `Recv` in flight: the last found no buffer, and waits for the retry
    /// deadline.
    Stalled,
    /// The peer ended the stream; `End` is not told yet.
    Ended,
    /// `End` told.
    Told,
}

/// The sending side of an open stream. `finishing` is `Finish` asked for:
/// the half-close follows the last of the output.
#[derive(Debug)]
enum Writer {
    Idle,
    /// A `Send` in flight; more may be queued.
    Sending {
        flight: Id<Flight>,
        finishing: bool,
    },
    /// A `Send` that found no buffer, its bytes held for the retry deadline.
    Stalled {
        bytes: Box<[u8]>,
        from: u32,
        finishing: bool,
    },
    /// The half-close in flight.
    Shutting(Id<Flight>),
    /// A half-close that found no buffer, for the retry deadline.
    Unshut,
    /// Half-closed.
    Shut,
}

/// The output: the `Send`s queued behind the one in flight, and the bytes of
/// all of them, the one in flight (or stalled) included until all of it is
/// sent.
#[derive(Debug)]
struct Output {
    queue: Queue<Box<[u8]>>,
    bytes: u32,
}

/// An operation of the sending side in flight.
#[derive(Clone, Copy, Debug)]
enum Writing {
    Send(Id<Flight>),
    Shutdown(Id<Flight>),
}

/// The `Send` among what the sending side has in flight.
const fn sending(write: Option<Writing>) -> Option<Id<Flight>> {
    match write {
        Some(Writing::Send(flight)) => Some(flight),
        Some(Writing::Shutdown(_)) | None => None,
    }
}

/// The half-close among what the sending side has in flight.
const fn shutting(write: Option<Writing>) -> Option<Id<Flight>> {
    match write {
        Some(Writing::Shutdown(flight)) => Some(flight),
        Some(Writing::Send(_)) | None => None,
    }
}

/// A stream that failed, `Failed` told: what it still had in flight finishes,
/// and it waits for its owner's close.
#[derive(Debug)]
pub(crate) struct Broken {
    owner: Token,
    fd: Fd,
    recv: Option<Id<Flight>>,
    write: Option<Writing>,
}

/// A graceful close (io.md, 3): the output flushed and half-closed, the input
/// discarded until the peer ends, under the close deadline.
#[derive(Debug)]
pub(crate) struct Closing {
    owner: Token,
    fd: Fd,
    drain: Drain,
    flush: Flush,
    /// The `Send`s still to flush.
    queue: Queue<Box<[u8]>>,
}

/// What a graceful close discards.
#[derive(Clone, Copy, Debug)]
enum Drain {
    /// A discarding `Recv` in flight.
    Receiving(Id<Flight>),
    /// The last found no buffer: retried at the retry deadline.
    Stalled,
    /// The peer ended, or the receive failed: nothing more to discard.
    Done,
}

/// What a graceful close flushes.
#[derive(Debug)]
enum Flush {
    Sending(Id<Flight>),
    Stalled {
        bytes: Box<[u8]>,
        from: u32,
    },
    Shutting(Id<Flight>),
    Unshut,
    /// Half-closed, or the flush failed: nothing more is sent.
    Done,
}

/// Closing at once: every operation that waits is cancelled, and the stream
/// waits for each of them and each cancel before its close
/// (programming-model.md, 5.3).
#[derive(Debug)]
pub(crate) struct Settling {
    /// The owner told `Closed`, unless the socket was rejected.
    owner: Option<Token>,
    /// The descriptor to close, unless its `Socket` has yet to make it.
    fd: Option<Fd>,
    /// Its `Socket` in flight.
    socket: bool,
    connect: Option<Id<Flight>>,
    recv: Option<Id<Flight>>,
    write: Option<Writing>,
    cancels: u32,
}

/// A completion of a stream's operation, decoded.
#[derive(Debug)]
enum Happened {
    Socket(Result<Fd, kernel::Error>),
    Connected { flight: Id<Flight>, result: Result<(), kernel::Error> },
    Received(Received),
    Sent(Sent),
    Shut { flight: Id<Flight>, result: Result<(), kernel::Error> },
    Released,
    Cancelled { target: Id<Flight>, result: Result<Done, kernel::Error> },
}

/// A `Recv` completed, its buffer handed back.
#[derive(Debug)]
struct Received {
    flight: Id<Flight>,
    buf: Box<[u8]>,
    result: Result<u32, kernel::Error>,
}

/// A `Send` completed, its bytes handed back with the offset it sent from.
#[derive(Debug)]
struct Sent {
    flight: Id<Flight>,
    bytes: Box<[u8]>,
    from: u32,
    result: Result<u32, kernel::Error>,
}

/// A connect whose `Socket` was just submitted.
pub(crate) const fn opened(owner: Token, addr: Addr) -> Stream {
    Stream::Opening { owner, addr }
}

/// A socket `listener` just accepted.
pub(crate) const fn announced(fd: Fd, listener: Id<Entity>) -> Stream {
    Stream::Announced { fd, listener }
}

impl Stream {
    pub(crate) const fn is_closed(&self) -> bool {
        match self {
            Stream::Closed => true,
            Stream::Opening { .. }
            | Stream::Socket { .. }
            | Stream::Connecting { .. }
            | Stream::Announced { .. }
            | Stream::Open(_)
            | Stream::Broken(_)
            | Stream::Closing(_)
            | Stream::Settling(_)
            | Stream::Releasing { .. } => false,
        }
    }
}

impl Open {
    /// Whether a side waits for the retry deadline.
    const fn stalled(&self) -> bool {
        let reader = match self.reader {
            Reader::Stalled => true,
            Reader::Receiving(_) | Reader::Full | Reader::Ended | Reader::Told => false,
        };
        let writer = match self.writer {
            Writer::Stalled { .. } | Writer::Unshut => true,
            Writer::Idle | Writer::Sending { .. } | Writer::Shutting(_) | Writer::Shut => false,
        };
        reader || writer
    }
}

impl Closing {
    /// Whether a side waits for the retry deadline.
    const fn stalled(&self) -> bool {
        let drain = match self.drain {
            Drain::Stalled => true,
            Drain::Receiving(_) | Drain::Done => false,
        };
        let flush = match self.flush {
            Flush::Stalled { .. } | Flush::Unshut => true,
            Flush::Sending(_) | Flush::Shutting(_) | Flush::Done => false,
        };
        drain || flush
    }

    /// Whether it is drained and flushed.
    const fn done(&self) -> bool {
        match self.drain {
            Drain::Done => match self.flush {
                Flush::Done => true,
                Flush::Sending(_) | Flush::Stalled { .. } | Flush::Shutting(_) | Flush::Unshut => false,
            },
            Drain::Receiving(_) | Drain::Stalled => false,
        }
    }
}

impl Writer {
    /// Whether the owner may still send: not after `Finish`.
    const fn takes_sends(&self) -> bool {
        match self {
            Writer::Idle => true,
            Writer::Sending { finishing, .. } | Writer::Stalled { finishing, .. } => !*finishing,
            Writer::Shutting(_) | Writer::Unshut | Writer::Shut => false,
        }
    }
}

impl Output {
    /// Whether one more `Send` of `room` bytes fits under the caps.
    fn fits(&self, room: u32, cap: u32) -> bool {
        match self.bytes.checked_add(room) {
            Some(queued) => queued <= cap && self.queue.room() > 0,
            None => false,
        }
    }
}

/// A completion of one of the stream's operations.
pub(crate) fn landed(
    stream: &mut Stream,
    landed: Landed,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let id = landed.entity;
    let happened = decode(landed);
    let state = mem::replace(stream, Stream::Closed);
    *stream = step(state, id, happened, env, tables, up, subs);
    follow(stream, id, env, tables, up, subs);
}

/// The stream `id` was on the ready list: a connect is told `Connecting`; an
/// open stream is delivered what its demand asks for.
pub(crate) fn resume(
    stream: &mut Stream,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match *stream {
        Stream::Opening { owner, addr } => {
            up.push(Event::Connecting { owner, socket: id.token() });
            *stream = Stream::Socket { owner, addr };
        }
        // Readied, then moved on; or open, and delivered below.
        Stream::Socket { .. }
        | Stream::Connecting { .. }
        | Stream::Announced { .. }
        | Stream::Open(_)
        | Stream::Broken(_)
        | Stream::Closing(_)
        | Stream::Settling(_)
        | Stream::Releasing { .. }
        | Stream::Closed => {}
    }
    follow(stream, id, env, tables, up, subs);
}

/// `Bind` (`owner`) or `Reject` (none) of an announced socket; answers the
/// listener that announced it.
pub(crate) fn answer(
    stream: &mut Stream,
    id: Id<Entity>,
    owner: Option<Token>,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Id<Entity> {
    let state = mem::replace(stream, Stream::Closed);
    let (fd, listener) = match state {
        Stream::Announced { fd, listener } => (fd, listener),
        Stream::Opening { .. }
        | Stream::Socket { .. }
        | Stream::Connecting { .. }
        | Stream::Open(_)
        | Stream::Broken(_)
        | Stream::Closing(_)
        | Stream::Settling(_)
        | Stream::Releasing { .. }
        | Stream::Closed => unreachable!("an answer names a socket announced and not yet answered"),
    };
    *stream = match owner {
        Some(owner) => Stream::Open(open(owner, fd, id, env, tables, subs)),
        None => release(None, fd, id, tables, subs),
    };
    tidy(stream, id, tables);
    listener
}

/// A request in the stream vocabulary (lib.md, 7). Whatever it makes due up
/// is told by `resume`, from the ready list.
pub(crate) fn request(
    stream: &mut Stream,
    id: Id<Entity>,
    down: Down,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    match stream {
        Stream::Open(open) => match down {
            Down::Demand { read, room } => demand(open, read, room, id, env, tables),
            Down::Send(bytes) => queue(open, bytes, id, env, tables, subs),
            Down::Finish => finish(open, id, tables, subs),
        },
        // The stream failed, or its owner closed it: dropped.
        Stream::Broken(_) | Stream::Closing(_) | Stream::Settling(_) | Stream::Releasing { .. } | Stream::Closed => {}
        Stream::Opening { .. } | Stream::Socket { .. } | Stream::Connecting { .. } => {
            unreachable!("a stream request comes once the stream is connected")
        }
        Stream::Announced { .. } => unreachable!("a stream request comes once the socket is bound"),
    }
    tidy(stream, id, tables);
}

/// `Close` (graceful) or `Abort`.
pub(crate) fn close(
    stream: &mut Stream,
    id: Id<Entity>,
    abort: bool,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    let state = mem::replace(stream, Stream::Closed);
    *stream = match state {
        // Nothing to flush before it is connected: a close is an abort.
        Stream::Socket { owner, addr: _ } => {
            let settling = Settling {
                owner: Some(owner),
                fd: None,
                socket: true,
                connect: None,
                recv: None,
                write: None,
                cancels: 0,
            };
            Stream::Settling(settling)
        }
        Stream::Connecting { owner, fd, connect } => {
            tables.cancel(subs, id, connect);
            let settling = Settling {
                owner: Some(owner),
                fd: Some(fd),
                socket: false,
                connect: Some(connect),
                recv: None,
                write: None,
                cancels: 1,
            };
            Stream::Settling(settling)
        }
        Stream::Open(open) => {
            if abort {
                let (recv, write) = in_flight(open.reader, &open.writer);
                wind_down(open.owner, open.fd, recv, write, id, tables, subs)
            } else {
                close_open(open, id, env, tables, subs)
            }
        }
        // Nothing to flush once it failed: a close is an abort.
        Stream::Broken(broken) => wind_down(broken.owner, broken.fd, broken.recv, broken.write, id, tables, subs),
        Stream::Closing(closing) => {
            if abort {
                abort_closing(closing, id, tables, subs)
            } else {
                Stream::Closing(closing)
            }
        }
        // Closing already, or closed and not yet reclaimed.
        state @ (Stream::Settling(_) | Stream::Releasing { .. } | Stream::Closed) => state,
        Stream::Opening { .. } => unreachable!("a connect is named above only once it is told Connecting"),
        Stream::Announced { .. } => unreachable!("an announced socket is bound or rejected, not closed"),
    };
    tidy(stream, id, tables);
}

/// The close deadline of a graceful close passed: it aborts.
pub(crate) fn expired(
    stream: &mut Stream,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let state = mem::replace(stream, Stream::Closed);
    *stream = match state {
        Stream::Closing(closing) => abort_closing(closing, id, tables, subs),
        Stream::Opening { .. }
        | Stream::Socket { .. }
        | Stream::Connecting { .. }
        | Stream::Announced { .. }
        | Stream::Open(_)
        | Stream::Broken(_)
        | Stream::Settling(_)
        | Stream::Releasing { .. }
        | Stream::Closed => unreachable!("a close deadline runs only while closing gracefully"),
    };
    follow(stream, id, env, tables, up, subs);
}

/// The retry deadline passed: what found no buffer is submitted again.
pub(crate) fn retried(
    stream: &mut Stream,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match stream {
        Stream::Open(open) => {
            // A stalled reader receives again when it is delivered, below.
            open.reader = match open.reader {
                Reader::Stalled => Reader::Full,
                reader @ (Reader::Receiving(_) | Reader::Full | Reader::Ended | Reader::Told) => reader,
            };
            open.writer = match mem::replace(&mut open.writer, Writer::Idle) {
                Writer::Stalled { bytes, from, finishing } => {
                    Writer::Sending { flight: send(open.fd, bytes, from, id, tables, subs), finishing }
                }
                Writer::Unshut => Writer::Shutting(shutdown(open.fd, id, tables, subs)),
                writer @ (Writer::Idle | Writer::Sending { .. } | Writer::Shutting(_) | Writer::Shut) => writer,
            };
        }
        Stream::Closing(closing) => {
            closing.drain = match closing.drain {
                Drain::Stalled => Drain::Receiving(receive(closing.fd, env.limits.receive, id, tables, subs)),
                drain @ (Drain::Receiving(_) | Drain::Done) => drain,
            };
            closing.flush = match mem::replace(&mut closing.flush, Flush::Done) {
                Flush::Stalled { bytes, from } => Flush::Sending(send(closing.fd, bytes, from, id, tables, subs)),
                Flush::Unshut => Flush::Shutting(shutdown(closing.fd, id, tables, subs)),
                flush @ (Flush::Sending(_) | Flush::Shutting(_) | Flush::Done) => flush,
            };
        }
        Stream::Opening { .. }
        | Stream::Socket { .. }
        | Stream::Connecting { .. }
        | Stream::Announced { .. }
        | Stream::Broken(_)
        | Stream::Settling(_)
        | Stream::Releasing { .. }
        | Stream::Closed => unreachable!("a retry deadline runs only while an open or closing stream stalls"),
    }
    follow(stream, id, env, tables, up, subs);
}

/// What follows every transition in the up pass, in one place
/// (programming-model.md, 5.4): an open stream delivered what its demand asks
/// for, and the deadlines its state no longer needs cancelled.
fn follow(
    stream: &mut Stream,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match stream {
        Stream::Open(open) => deliver(open, id, env, tables, up, subs),
        Stream::Opening { .. }
        | Stream::Socket { .. }
        | Stream::Connecting { .. }
        | Stream::Announced { .. }
        | Stream::Broken(_)
        | Stream::Closing(_)
        | Stream::Settling(_)
        | Stream::Releasing { .. }
        | Stream::Closed => {}
    }
    tidy(stream, id, tables);
}

/// The deadlines a state implies, an exhaustive function of it: the close
/// deadline runs only while closing gracefully, the retry deadline only while
/// a side stalls. Each is armed where its state begins, and cancelled here
/// once its state is left.
fn tidy(stream: &Stream, id: Id<Entity>, tables: &mut Tables) {
    let (closing, stalled) = match stream {
        Stream::Open(open) => (false, open.stalled()),
        Stream::Closing(closing) => (true, closing.stalled()),
        Stream::Opening { .. }
        | Stream::Socket { .. }
        | Stream::Connecting { .. }
        | Stream::Announced { .. }
        | Stream::Broken(_)
        | Stream::Settling(_)
        | Stream::Releasing { .. }
        | Stream::Closed => (false, false),
    };
    if !closing {
        tables.deadlines.cancel((id, Timer::Close));
    }
    if !stalled {
        tables.deadlines.cancel((id, Timer::Retry));
    }
}

/// A side found no buffer: it is tried again once the retry deadline passes,
/// rather than at once (io.md, 3.3).
fn stall(id: Id<Entity>, env: &Env<Limits>, tables: &mut Tables) {
    let at = env.now.saturating_add(env.limits.retry);
    tables.deadlines.arm((id, Timer::Retry), at).expect("a retry deadline for every socket");
}

/// What a completion means to the stream.
fn decode(landed: Landed) -> Happened {
    let Landed { entity: _, flight, purpose, kind, result } = landed;
    match purpose {
        Purpose::Socket => Happened::Socket(match result {
            Ok(Done::Fd(fd)) => Ok(fd),
            Ok(Done::Nothing | Done::Count(_) | Done::Accepted { .. } | Done::Bound(_)) => {
                unreachable!("a socket answers with its descriptor")
            }
            Err(error) => Err(error),
        }),
        Purpose::Connect => Happened::Connected { flight, result: nothing(result) },
        Purpose::Recv => match kind {
            Op::Recv { fd: _, buf } => Happened::Received(Received { flight, buf, result: counted(result) }),
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. }
            | Op::Cancel { .. } => unreachable!("a completion hands back its own operation"),
        },
        Purpose::Send => match kind {
            Op::Send { fd: _, bytes, from } => Happened::Sent(Sent { flight, bytes, from, result: counted(result) }),
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Shutdown { .. }
            | Op::Close { .. }
            | Op::Cancel { .. } => unreachable!("a completion hands back its own operation"),
        },
        Purpose::Shutdown => Happened::Shut { flight, result: nothing(result) },
        // A descriptor is closed whatever its close answers.
        Purpose::Close => Happened::Released,
        Purpose::Cancel(target) => Happened::Cancelled { target, result },
        Purpose::Bind | Purpose::Listen | Purpose::Accept | Purpose::Discard => {
            unreachable!("a stream never binds, listens or accepts")
        }
    }
}

/// The result of an operation that succeeds with nothing.
fn nothing(result: Result<Done, kernel::Error>) -> Result<(), kernel::Error> {
    match result {
        Ok(Done::Nothing) => Ok(()),
        Ok(Done::Count(_) | Done::Fd(_) | Done::Accepted { .. } | Done::Bound(_)) => {
            unreachable!("a connect or a shutdown answers with nothing")
        }
        Err(error) => Err(error),
    }
}

/// The result of an operation that succeeds with a count.
fn counted(result: Result<Done, kernel::Error>) -> Result<u32, kernel::Error> {
    match result {
        Ok(Done::Count(n)) => Ok(n),
        Ok(Done::Nothing | Done::Fd(_) | Done::Accepted { .. } | Done::Bound(_)) => {
            unreachable!("a receive or a send answers with a count")
        }
        Err(error) => Err(error),
    }
}

/// The transition table of io.md, 3.3, for completions.
fn step(
    state: Stream,
    id: Id<Entity>,
    happened: Happened,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> Stream {
    match state {
        Stream::Opening { .. } => {
            unreachable!("the ready list is drained before completions, so a connect is told Connecting first")
        }
        Stream::Socket { owner, addr } => match happened {
            Happened::Socket(Ok(fd)) => made(owner, addr, fd, id, tables, subs),
            Happened::Socket(Err(error)) => {
                up.push(Event::Failed { owner, error: records::setup_error(error) });
                up.push(Event::Closed { owner });
                Stream::Closed
            }
            Happened::Connected { .. }
            | Happened::Received(_)
            | Happened::Sent(_)
            | Happened::Shut { .. }
            | Happened::Released
            | Happened::Cancelled { .. } => unreachable!("a socket being made has nothing else in flight"),
        },
        Stream::Connecting { owner, fd, connect } => match happened {
            Happened::Connected { flight, result } => {
                assert!(flight == connect, "the connect that completed is the one in flight");
                match result {
                    Ok(()) => connected(owner, fd, id, env, tables, up, subs),
                    Err(error) => unconnected(owner, fd, error, id, tables, up, subs),
                }
            }
            Happened::Socket(_)
            | Happened::Received(_)
            | Happened::Sent(_)
            | Happened::Shut { .. }
            | Happened::Released
            | Happened::Cancelled { .. } => unreachable!("a socket connecting has nothing else in flight"),
        },
        Stream::Announced { .. } => unreachable!("an announced socket has nothing in flight"),
        Stream::Open(open) => match happened {
            Happened::Received(received) => open_received(open, received, id, env, tables, up),
            Happened::Sent(sent) => open_sent(open, sent, id, env, tables, up, subs),
            Happened::Shut { flight, result } => open_shut(open, flight, result, id, env, tables, up),
            Happened::Socket(_) | Happened::Connected { .. } | Happened::Released | Happened::Cancelled { .. } => {
                unreachable!("an open stream has only its receive and its send in flight")
            }
        },
        Stream::Broken(broken) => Stream::Broken(broken_landed(broken, happened)),
        Stream::Closing(closing) => match happened {
            Happened::Received(received) => closing_received(closing, received, id, env, tables, subs),
            Happened::Sent(sent) => closing_sent(closing, sent, id, env, tables, subs),
            Happened::Shut { flight, result } => closing_shut(closing, flight, result, id, env, tables, subs),
            Happened::Socket(_) | Happened::Connected { .. } | Happened::Released | Happened::Cancelled { .. } => {
                unreachable!("a stream closing gracefully has only its receive and its send in flight")
            }
        },
        Stream::Settling(settling) => {
            let settling = settling_landed(settling, happened, id, tables, subs);
            settle(settling, id, tables, up, subs)
        }
        Stream::Releasing { owner } => match happened {
            Happened::Released => {
                if let Some(owner) = owner {
                    up.push(Event::Closed { owner });
                }
                Stream::Closed
            }
            Happened::Socket(_)
            | Happened::Connected { .. }
            | Happened::Received(_)
            | Happened::Sent(_)
            | Happened::Shut { .. }
            | Happened::Cancelled { .. } => unreachable!("a stream releasing has only its close in flight"),
        },
        Stream::Closed => unreachable!("a closed stream has nothing in flight"),
    }
}

/// Socket ok: connect it.
fn made(owner: Token, addr: Addr, fd: Fd, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Stream {
    let connect = tables.submit(subs, id, Purpose::Connect, Op::Connect { fd, addr });
    Stream::Connecting { owner, fd, connect }
}

/// Connect ok: told, and open.
fn connected(
    owner: Token,
    fd: Fd,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> Stream {
    up.push(Event::Connected { owner });
    Stream::Open(open(owner, fd, id, env, tables, subs))
}

/// Connect failed: told, and the socket closed.
fn unconnected(
    owner: Token,
    fd: Fd,
    error: kernel::Error,
    id: Id<Entity>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> Stream {
    up.push(Event::Failed { owner, error: records::connect_error(error) });
    release(Some(owner), fd, id, tables, subs)
}

/// An open stream, receiving from the start.
fn open(
    owner: Token,
    fd: Fd,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Open {
    let intake = Intake::with_capacity(env.limits.intake);
    let recv = receive(fd, intake.room().min(env.limits.receive), id, tables, subs);
    Open {
        owner,
        fd,
        intake,
        demand: Demand { read: Read::Nothing, room: 0 },
        granted: 0,
        reader: Reader::Receiving(recv),
        writer: Writer::Idle,
        output: Output { queue: Queue::with_capacity(env.limits.sends), bytes: 0 },
    }
}

/// The owner's demand answered, if the stream can: by the bytes it reads,
/// or else by the room it asks for, and either answer ends it (lib.md, 7).
/// Then the end, once nothing held can meet a demand; and a receive armed
/// again once the intake has room (io.md, 3.3).
fn deliver(
    open: &mut Open,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let owner = open.owner;
    // After the end, a demand is never answered with bytes, crossing it or
    // not; what the intake still holds is dropped with it.
    let bytes = match open.reader {
        Reader::Told => None,
        Reader::Receiving(_) | Reader::Full | Reader::Stalled | Reader::Ended => open.intake.meet(open.demand.read),
    };
    if let Some(bytes) = bytes {
        up.push(Event::Stream { owner, up: Up::Bytes(bytes) });
        open.demand = Demand { read: Read::Nothing, room: 0 };
    } else if open.demand.room > 0 && open.writer.takes_sends() && open.output.fits(open.demand.room, env.limits.output)
    {
        up.push(Event::Stream { owner, up: Up::Room });
        open.granted = open.demand.room;
        open.demand = Demand { read: Read::Nothing, room: 0 };
    }
    // With a read outstanding, the end comes once it can never be met; with
    // none, once nothing is held that a demand could take.
    let end = match open.reader {
        Reader::Ended => match open.demand.read {
            Read::Nothing => open.intake.is_empty(),
            Read::Fill(_) | Read::Scan { .. } | Read::Line { .. } => true,
        },
        Reader::Receiving(_) | Reader::Full | Reader::Stalled | Reader::Told => false,
    };
    if end {
        up.push(Event::Stream { owner, up: Up::End });
        open.reader = Reader::Told;
    }
    open.reader = match open.reader {
        Reader::Full if open.intake.room() > 0 => {
            let len = open.intake.room().min(env.limits.receive);
            Reader::Receiving(receive(open.fd, len, id, tables, subs))
        }
        reader @ (Reader::Receiving(_) | Reader::Full | Reader::Stalled | Reader::Ended | Reader::Told) => reader,
    };
}

/// A `Demand`: stated once the last is answered, never in place of it, or
/// withdrawing it (lib.md, 7); met, if it can be, from the ready list.
fn demand(open: &mut Open, read: Read, room: u32, id: Id<Entity>, env: &Env<Limits>, tables: &mut Tables) {
    let cap = env.limits.intake;
    let withdrawal = match read {
        Read::Nothing => room == 0,
        Read::Fill(n) => {
            assert!(n <= cap, "a fill past the intake's cap could never be met (Limits::largest_read)");
            false
        }
        Read::Scan { until, max } => {
            assert!(
                max <= cap && index(max) >= until.as_bytes().len(),
                "a scan within the intake's cap and long enough for its delimiter (Limits::largest_read)"
            );
            false
        }
        Read::Line { max } => {
            assert!(
                max <= cap && max > 0,
                "a scan within the intake's cap and long enough for its line end (Limits::largest_read)"
            );
            false
        }
    };
    let outstanding = match open.demand.read {
        Read::Nothing => open.demand.room > 0,
        Read::Fill(_) | Read::Scan { .. } | Read::Line { .. } => true,
    };
    assert!(
        withdrawal || !outstanding,
        "a demand is stated once the last is answered, never in place of it, or it withdraws it (lib.md, 7)"
    );
    assert!(room <= env.limits.output, "room past the output cap could never be granted (Limits::largest_room)");
    assert!(room == 0 || open.writer.takes_sends(), "no room is demanded after Finish");
    open.demand = Demand { read, room };
    tables.ready.mark(id);
}

/// A `Send`: sent at once by an idle writer, queued behind the one in flight
/// otherwise, within the room granted.
fn queue(
    open: &mut Open,
    bytes: Box<[u8]>,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    assert!(open.writer.takes_sends(), "no Send after Finish");
    if bytes.is_empty() {
        return;
    }
    let len = u32::try_from(bytes.len()).expect("a Send within the room granted, under a u32 cap");
    // Room granted is spent by what is sent within it (lib.md, 7).
    open.granted = open.granted.checked_sub(len).expect("a Send within the room granted: no more than the last Room");
    let queued = open.output.bytes.checked_add(len).expect("a Send within the room granted, under a u32 cap");
    assert!(queued <= env.limits.output, "a Send within the room granted: no more than the output cap");
    open.writer = match mem::replace(&mut open.writer, Writer::Idle) {
        Writer::Idle => Writer::Sending { flight: send(open.fd, bytes, 0, id, tables, subs), finishing: false },
        writer @ (Writer::Sending { .. } | Writer::Stalled { .. }) => {
            assert!(open.output.queue.room() > 0, "a Send within the room granted: no more than Limits::sends");
            open.output.queue.push(bytes);
            writer
        }
        Writer::Shutting(_) | Writer::Unshut | Writer::Shut => unreachable!("no Send after Finish"),
    };
    open.output.bytes = queued;
}

/// `Finish`: the half-close, once the output is flushed. Asked again, it is
/// already on its way.
fn finish(open: &mut Open, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) {
    open.demand.room = 0;
    open.writer = match mem::replace(&mut open.writer, Writer::Idle) {
        Writer::Idle => Writer::Shutting(shutdown(open.fd, id, tables, subs)),
        Writer::Sending { flight, finishing: _ } => Writer::Sending { flight, finishing: true },
        Writer::Stalled { bytes, from, finishing: _ } => Writer::Stalled { bytes, from, finishing: true },
        writer @ (Writer::Shutting(_) | Writer::Unshut | Writer::Shut) => writer,
    };
}

fn open_received(
    mut open: Open,
    received: Received,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
) -> Stream {
    let Received { flight, buf, result } = received;
    match open.reader {
        Reader::Receiving(receiving) => assert!(receiving == flight, "the receive that completed is the one in flight"),
        Reader::Full | Reader::Stalled | Reader::Ended | Reader::Told => {
            unreachable!("a receive completes while one is in flight")
        }
    }
    open.reader = Reader::Full;
    match result {
        Ok(0) => open.reader = Reader::Ended,
        Ok(n) => {
            let bytes = buf.get(..index(n)).expect("a receive counts no more than its buffer");
            open.intake.append(bytes).expect("a receive asks for no more than the intake has room for");
        }
        // It did nothing: received again once the retry deadline passes.
        Err(kernel::Error::NoBufferSpace) => {
            stall(id, env, tables);
            open.reader = Reader::Stalled;
        }
        Err(kernel::Error::Cancelled) => unreachable!("io cancels a receive only when settling"),
        Err(error) => return broken(open, records::stream_fault(error), up),
    }
    // Freed before the next receive's is made, when the stream is delivered:
    // one buffer per stream at once (io.md, 3.4).
    drop(buf);
    Stream::Open(open)
}

fn open_sent(
    mut open: Open,
    sent: Sent,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> Stream {
    let finishing = match open.writer {
        Writer::Sending { flight, finishing } => {
            assert!(flight == sent.flight, "the send that completed is the one in flight");
            finishing
        }
        Writer::Idle | Writer::Stalled { .. } | Writer::Shutting(_) | Writer::Unshut | Writer::Shut => {
            unreachable!("a send completes while one is in flight")
        }
    };
    let Sent { flight: _, bytes, from, result } = sent;
    open.writer = match result {
        Ok(n) => {
            let from = from.checked_add(n).expect("a send counts no more than was left");
            if index(from) < bytes.len() {
                // A short send: the rest from the same box, never copied.
                Writer::Sending { flight: send(open.fd, bytes, from, id, tables, subs), finishing }
            } else {
                let len = u32::try_from(bytes.len()).expect("queued output fits its u32 cap");
                open.output.bytes = open.output.bytes.checked_sub(len).expect("the output counts its send in flight");
                drop(bytes);
                next_writer(open.fd, &mut open.output.queue, finishing, id, tables, subs)
            }
        }
        // It did nothing: sent again, from the same offset, once the retry
        // deadline passes.
        Err(kernel::Error::NoBufferSpace) => {
            stall(id, env, tables);
            Writer::Stalled { bytes, from, finishing }
        }
        Err(kernel::Error::Cancelled) => unreachable!("io cancels a send only when settling"),
        Err(error) => {
            open.writer = Writer::Idle;
            return broken(open, records::stream_fault(error), up);
        }
    };
    Stream::Open(open)
}

fn open_shut(
    mut open: Open,
    flight: Id<Flight>,
    result: Result<(), kernel::Error>,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
) -> Stream {
    match open.writer {
        Writer::Shutting(shutting) => assert!(shutting == flight, "the half-close that completed is the one in flight"),
        Writer::Idle | Writer::Sending { .. } | Writer::Stalled { .. } | Writer::Unshut | Writer::Shut => {
            unreachable!("a half-close completes while one is in flight")
        }
    }
    open.writer = match result {
        Ok(()) => Writer::Shut,
        // It did nothing: asked again once the retry deadline passes.
        Err(kernel::Error::NoBufferSpace) => {
            stall(id, env, tables);
            Writer::Unshut
        }
        Err(kernel::Error::Cancelled) => unreachable!("io never cancels a half-close"),
        Err(error) => {
            open.writer = Writer::Shut;
            return broken(open, records::stream_fault(error), up);
        }
    };
    Stream::Open(open)
}

/// The stream failed: told at once, the intake and the output dropped.
fn broken(open: Open, fault: Fault, up: &mut Queue<Event>) -> Stream {
    up.push(Event::Stream { owner: open.owner, up: Up::Failed(fault) });
    let (recv, write) = in_flight(open.reader, &open.writer);
    Stream::Broken(Broken { owner: open.owner, fd: open.fd, recv, write })
}

/// What an open stream's two sides have in flight.
const fn in_flight(reader: Reader, writer: &Writer) -> (Option<Id<Flight>>, Option<Writing>) {
    let recv = match reader {
        Reader::Receiving(flight) => Some(flight),
        Reader::Full | Reader::Stalled | Reader::Ended | Reader::Told => None,
    };
    let write = match writer {
        Writer::Sending { flight, .. } => Some(Writing::Send(*flight)),
        Writer::Shutting(flight) => Some(Writing::Shutdown(*flight)),
        Writer::Idle | Writer::Stalled { .. } | Writer::Unshut | Writer::Shut => None,
    };
    (recv, write)
}

/// What a broken stream had in flight finishes; what it carried is dropped.
fn broken_landed(broken: Broken, happened: Happened) -> Broken {
    match happened {
        Happened::Received(received) => {
            assert!(broken.recv == Some(received.flight), "the receive that completed is the one in flight");
            Broken { recv: None, ..broken }
        }
        Happened::Sent(sent) => {
            assert!(sending(broken.write) == Some(sent.flight), "the send that completed is the one in flight");
            Broken { write: None, ..broken }
        }
        Happened::Shut { flight, result: _ } => {
            assert!(shutting(broken.write) == Some(flight), "the half-close that completed is in flight");
            Broken { write: None, ..broken }
        }
        Happened::Socket(_) | Happened::Connected { .. } | Happened::Released | Happened::Cancelled { .. } => {
            unreachable!("a broken stream has only its receive and its send in flight")
        }
    }
}

/// `Close` of an open stream: flush and half-close, discarding the input,
/// under the close deadline; or released at once if nothing is left to do.
fn close_open(open: Open, id: Id<Entity>, env: &Env<Limits>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Stream {
    let Open { owner, fd, intake: _, demand: _, granted: _, reader, writer, output } = open;
    // Discarding starts now, so a peer blocked on its upload drains, then
    // reads what is flushed to it.
    let drain = match reader {
        Reader::Receiving(flight) => Drain::Receiving(flight),
        Reader::Full => Drain::Receiving(receive(fd, env.limits.receive, id, tables, subs)),
        Reader::Stalled => Drain::Stalled,
        Reader::Ended | Reader::Told => Drain::Done,
    };
    let flush = match writer {
        Writer::Idle => Flush::Shutting(shutdown(fd, id, tables, subs)),
        Writer::Sending { flight, finishing: _ } => Flush::Sending(flight),
        Writer::Stalled { bytes, from, finishing: _ } => Flush::Stalled { bytes, from },
        Writer::Shutting(flight) => Flush::Shutting(flight),
        Writer::Unshut => Flush::Unshut,
        Writer::Shut => Flush::Done,
    };
    let closing = Closing { owner, fd, drain, flush, queue: output.queue };
    if closing.done() {
        return release(Some(owner), fd, id, tables, subs);
    }
    let at = env.now.saturating_add(env.limits.close_timeout);
    tables.deadlines.arm((id, Timer::Close), at).expect("a close deadline for every socket");
    Stream::Closing(closing)
}

fn closing_received(
    closing: Closing,
    received: Received,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Stream {
    let Received { flight, buf, result } = received;
    match closing.drain {
        Drain::Receiving(receiving) => assert!(receiving == flight, "the receive that completed is the one in flight"),
        Drain::Stalled | Drain::Done => unreachable!("a receive completes while one is in flight"),
    }
    // Freed before the next receive's is made: one buffer per stream at once
    // (io.md, 3.4).
    drop(buf);
    let drain = match result {
        Ok(0) => Drain::Done,
        Ok(_) => Drain::Receiving(receive(closing.fd, env.limits.receive, id, tables, subs)),
        Err(kernel::Error::NoBufferSpace) => {
            stall(id, env, tables);
            Drain::Stalled
        }
        Err(kernel::Error::Cancelled) => unreachable!("io cancels a receive only when settling"),
        // The peer is gone: nothing more to discard.
        Err(_failed) => Drain::Done,
    };
    closed_if_done(Closing { drain, ..closing }, id, tables, subs)
}

fn closing_sent(
    mut closing: Closing,
    sent: Sent,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Stream {
    match closing.flush {
        Flush::Sending(sending) => assert!(sending == sent.flight, "the send that completed is the one in flight"),
        Flush::Stalled { .. } | Flush::Shutting(_) | Flush::Unshut | Flush::Done => {
            unreachable!("a send completes while one is in flight")
        }
    }
    let Sent { flight: _, bytes, from, result } = sent;
    closing.flush = match result {
        Ok(n) => {
            let from = from.checked_add(n).expect("a send counts no more than was left");
            if index(from) < bytes.len() {
                Flush::Sending(send(closing.fd, bytes, from, id, tables, subs))
            } else {
                drop(bytes);
                match closing.queue.pop() {
                    Some(next) => Flush::Sending(send(closing.fd, next, 0, id, tables, subs)),
                    None => Flush::Shutting(shutdown(closing.fd, id, tables, subs)),
                }
            }
        }
        Err(kernel::Error::NoBufferSpace) => {
            stall(id, env, tables);
            Flush::Stalled { bytes, from }
        }
        Err(kernel::Error::Cancelled) => unreachable!("io cancels a send only when settling"),
        // The peer is gone: nothing more to flush.
        Err(_failed) => Flush::Done,
    };
    closed_if_done(closing, id, tables, subs)
}

fn closing_shut(
    closing: Closing,
    flight: Id<Flight>,
    result: Result<(), kernel::Error>,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Stream {
    match closing.flush {
        Flush::Shutting(shutting) => assert!(shutting == flight, "the half-close that completed is the one in flight"),
        Flush::Sending(_) | Flush::Stalled { .. } | Flush::Unshut | Flush::Done => {
            unreachable!("a half-close completes while one is in flight")
        }
    }
    let flush = match result {
        Err(kernel::Error::NoBufferSpace) => {
            stall(id, env, tables);
            Flush::Unshut
        }
        Err(kernel::Error::Cancelled) => unreachable!("io never cancels a half-close"),
        Ok(()) => Flush::Done,
        // The connection is gone: nothing more to flush either.
        Err(_failed) => Flush::Done,
    };
    closed_if_done(Closing { flush, ..closing }, id, tables, subs)
}

/// Drained and flushed: the socket closed, its deadlines cancelled as it
/// leaves `Closing`.
fn closed_if_done(closing: Closing, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Stream {
    if !closing.done() {
        return Stream::Closing(closing);
    }
    release(Some(closing.owner), closing.fd, id, tables, subs)
}

/// The deadline passed, or the owner aborted: what the close still had in
/// flight is cancelled, and what stalled is dropped.
fn abort_closing(closing: Closing, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Stream {
    let recv = match closing.drain {
        Drain::Receiving(flight) => Some(flight),
        Drain::Stalled | Drain::Done => None,
    };
    let write = match closing.flush {
        Flush::Sending(flight) => Some(Writing::Send(flight)),
        Flush::Shutting(flight) => Some(Writing::Shutdown(flight)),
        Flush::Stalled { .. } | Flush::Unshut | Flush::Done => None,
    };
    wind_down(closing.owner, closing.fd, recv, write, id, tables, subs)
}

/// Closes at once: cancels what waits, and settles.
fn wind_down(
    owner: Token,
    fd: Fd,
    recv: Option<Id<Flight>>,
    write: Option<Writing>,
    id: Id<Entity>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Stream {
    let mut cancels = 0_u32;
    if let Some(flight) = recv {
        tables.cancel(subs, id, flight);
        cancels = cancels.checked_add(1).expect("two cancels at most");
    }
    match write {
        Some(Writing::Send(flight)) => {
            tables.cancel(subs, id, flight);
            cancels = cancels.checked_add(1).expect("two cancels at most");
        }
        // A half-close never waits: it is waited for, not cancelled.
        Some(Writing::Shutdown(_)) | None => {}
    }
    let settling = Settling { owner: Some(owner), fd: Some(fd), socket: false, connect: None, recv, write, cancels };
    if waits(&settling) {
        return Stream::Settling(settling);
    }
    release(Some(owner), fd, id, tables, subs)
}

/// A completion while settling: whatever it did, that operation is done.
fn settling_landed(
    mut settling: Settling,
    happened: Happened,
    id: Id<Entity>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Settling {
    match happened {
        Happened::Socket(result) => {
            assert!(settling.socket, "the socket that completed is the one being made");
            settling.socket = false;
            // Made after all: it is closed like any other.
            if let Ok(fd) = result {
                settling.fd = Some(fd);
            }
        }
        // Made or not, a connect io stopped is followed by a close only; one
        // that reached its peer ends there when it does (kernel.md, 5).
        Happened::Connected { flight, result: _ } => {
            assert!(settling.connect == Some(flight), "the connect that completed is the one in flight");
            settling.connect = None;
        }
        Happened::Received(received) => {
            assert!(settling.recv == Some(received.flight), "the receive that completed is the one in flight");
            settling.recv = None;
        }
        Happened::Sent(sent) => {
            assert!(sending(settling.write) == Some(sent.flight), "the send that completed is the one in flight");
            settling.write = None;
        }
        Happened::Shut { flight, result: _ } => {
            assert!(shutting(settling.write) == Some(flight), "the half-close that completed is in flight");
            settling.write = None;
        }
        Happened::Cancelled { target, result } => {
            settling.cancels = settling.cancels.checked_sub(1).expect("a cancel in flight completed");
            // Not submitted: its target runs on, so it is asked again while
            // the target still waits (kernel.md, 5).
            let waiting = settling.connect == Some(target)
                || settling.recv == Some(target)
                || sending(settling.write) == Some(target);
            if layer::unsubmitted(result) && waiting {
                tables.cancel(subs, id, target);
                settling.cancels = settling.cancels.checked_add(1).expect("one cancel per target");
            }
        }
        Happened::Released => unreachable!("a stream settling closes only once it has settled"),
    }
    settling
}

/// Whether anything of a settling stream is still in flight.
const fn waits(settling: &Settling) -> bool {
    settling.socket
        || settling.connect.is_some()
        || settling.recv.is_some()
        || settling.write.is_some()
        || settling.cancels > 0
}

/// Settled: the socket closed, or, if none was made, the owner told.
fn settle(
    settling: Settling,
    id: Id<Entity>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) -> Stream {
    if waits(&settling) {
        return Stream::Settling(settling);
    }
    match settling.fd {
        Some(fd) => release(settling.owner, fd, id, tables, subs),
        None => {
            if let Some(owner) = settling.owner {
                up.push(Event::Closed { owner });
            }
            Stream::Closed
        }
    }
}

fn release(owner: Option<Token>, fd: Fd, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Stream {
    let _close: Id<Flight> = tables.submit(subs, id, Purpose::Close, Op::Close { fd });
    Stream::Releasing { owner }
}

/// The next queued `Send`, or, the output flushed, the half-close when
/// finishing.
fn next_writer(
    fd: Fd,
    queue: &mut Queue<Box<[u8]>>,
    finishing: bool,
    id: Id<Entity>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Writer {
    match queue.pop() {
        Some(bytes) => Writer::Sending { flight: send(fd, bytes, 0, id, tables, subs), finishing },
        None if finishing => Writer::Shutting(shutdown(fd, id, tables, subs)),
        None => Writer::Idle,
    }
}

fn receive(fd: Fd, len: u32, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Id<Flight> {
    let op = Op::recv(fd, bytes::zeroed(index(len))).expect("a receive asks for at least a byte");
    tables.submit(subs, id, Purpose::Recv, op)
}

fn send(
    fd: Fd,
    bytes: Box<[u8]>,
    from: u32,
    id: Id<Entity>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) -> Id<Flight> {
    let op = Op::send(fd, bytes, from).expect("a send has bytes left from its offset");
    tables.submit(subs, id, Purpose::Send, op)
}

fn shutdown(fd: Fd, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) -> Id<Flight> {
    tables.submit(subs, id, Purpose::Shutdown, Op::Shutdown { fd })
}

fn index(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits a usize")
}
