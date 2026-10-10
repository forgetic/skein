//! A connection (examples.md, 3.4): a socket io accepted and the layer
//! bound, carrying one line at a time between its peer and the domain. It
//! asks for room for an answer before it reads the line that needs it, so a
//! peer that does not read stops being read from (programming-model.md, 7);
//! it is closed when it makes no progress for the idle deadline; and it
//! retires once io told `Closed`, no call of its is out, and its session was
//! told gone, so that neither io nor the domain still names it.
//!
//! Each cell is a small handler over the state it leaves; what the new state
//! implies (its demand, whether its idle deadline runs) is applied after
//! every transition, in [`follow`] (programming-model.md, 5.4).
//!
//! Owner transitions (examples.md, section 3.3; shell.md, section 13):
//! | State | Owner request | Next and effects |
//! |---|---|---|
//! | Greeting, Reading, Draining | Drain | Closing; Close and Gone if admitted |
//! | Admitting, Answering | Drain | Same; ended noted until its reply |
//! | Closing | Drain | Same |
//! | Open | Abort | Closing; Abort, Gone if known, any call still awaited |
//! | Closing | Abort | Same; Abort unless Closed already arrived |
//! | Closing owing Gone | Resume | Gone; retire only after both bindings end |
//! No peer bytes or domain text are kept after they have moved to their owner.

use alloc::boxed::Box;
use core::mem;

use skein_echo_domain::{Event as Call, Reply};
use skein_io::Request as Io;
use skein_lib::stream::{Delimiter, Down, Read, Up};
use skein_lib::{Env, Id, Queue, ReplyTo, Token, Writer, bytes};

use crate::layer::Tables;
use crate::limits::{BUSY, Limits, TOO_LONG};

/// A connection: io's socket, and where it is in its life.
#[derive(Debug)]
pub(crate) struct Conn {
    socket: Token,
    state: State,
    stop: Stop,
}

/// Work requested by the owner, applied from the bounded ready list.
#[derive(Clone, Copy, Debug)]
enum Stop {
    /// No owner work pending.
    None,
    /// Finish a call out, then close.
    Drain,
    /// Abandon a remaining close.
    Abort,
}

/// The states of a connection, each holding what it holds (examples.md,
/// 3.4). Room for one answer is held from Greeting's or Draining's grant
/// until the answer is sent.
#[derive(Debug)]
enum State {
    /// Bound: room for an answer demanded. Idle deadline.
    Greeting,
    /// The domain asked to admit the peer; whether the peer ended meanwhile.
    Admitting {
        ended: bool,
    },
    /// Admitted as `session`: a line demanded. Idle deadline.
    Reading {
        session: Token,
    },
    /// A line handed to the domain; whether the peer ended meanwhile.
    Answering {
        session: Token,
        ended: bool,
    },
    /// The answer sent: room for the next demanded. Idle deadline.
    Draining {
        session: Token,
    },
    Closing(Closing),
    /// Terminal: holds nothing.
    Closed,
}

/// Closing: io asked to close the socket, gracefully.
#[derive(Debug)]
struct Closing {
    /// io told `Closed`.
    closed: bool,
    /// A call is out, whose reply is awaited.
    call: bool,
    /// The session the domain is still to be told is gone, from the ready
    /// list: a reply that came in the down pass admitted it, or found the
    /// peer ended.
    owed: Option<Token>,
}

/// What the domain answers into: a closing connection that awaits nothing.
const CLOSING: Closing = Closing { closed: false, call: false, owed: None };

/// A socket just bound, greeting its peer. [`follow`] states its demand.
pub(crate) const fn opened(socket: Token) -> Conn {
    Conn { socket, state: State::Greeting, stop: Stop::None }
}

impl Conn {
    /// Schedules the owner’s drain or abort.
    pub(crate) fn stop(&mut self, abort: bool) {
        self.stop = if abort { Stop::Abort } else { Stop::Drain };
    }

    /// Whether it owes the domain a `Gone`, for the ready list.
    pub(crate) const fn owes(&self) -> bool {
        match self.state {
            State::Closing(Closing { owed: Some(_), call: false, .. }) => true,
            State::Closing(_)
            | State::Greeting
            | State::Admitting { .. }
            | State::Reading { .. }
            | State::Answering { .. }
            | State::Draining { .. }
            | State::Closed => false,
        }
    }

    /// Whether nothing names it any more: io told `Closed`, no call is out,
    /// and nothing is owed the domain. It is then retired, once.
    pub(crate) const fn is_done(&self) -> bool {
        match self.state {
            State::Closing(Closing { closed, call, owed }) => closed && !call && owed.is_none(),
            // Retired already.
            State::Closed
            | State::Greeting
            | State::Admitting { .. }
            | State::Reading { .. }
            | State::Answering { .. }
            | State::Draining { .. } => false,
        }
    }

    /// Marks it closed for good, when it is retired.
    pub(crate) fn retire(&mut self) {
        assert!(self.is_done(), "a connection retires once nothing names it");
        self.state = State::Closed;
    }
}

/// What a state implies: its demand of io, and whether its idle deadline
/// runs (every demand of the echo waits on the peer, so the two go
/// together). Applied after every transition, so that entering a state that
/// waits on the peer arms its deadline afresh: each line read and each room
/// granted is progress.
pub(crate) fn follow(conn: &Conn, id: Id<Conn>, env: &Env<Limits>, tables: &mut Tables, down: &mut Queue<Io>) {
    let line = env.limits.line;
    let demand = match conn.state {
        State::Greeting | State::Draining { .. } => Some((Read::Nothing, line)),
        State::Reading { .. } => Some((Read::Scan { until: Delimiter::LF, max: line }, 0)),
        State::Admitting { .. } | State::Answering { .. } | State::Closing(_) | State::Closed => None,
    };
    match demand {
        Some((read, room)) => {
            down.push(Io::Stream { stream: conn.socket, down: Down::Demand { read, room } });
            tables.idle(id, env);
        }
        None => tables.deadlines.cancel(id),
    }
}

/// A stream event io told the connection.
pub(crate) fn stream(conn: &mut Conn, id: Id<Conn>, event: Up, up: &mut Queue<Call>, down: &mut Queue<Io>) {
    let socket = conn.socket;
    let state = mem::replace(&mut conn.state, State::Closed);
    conn.state = match event {
        Up::Bytes(bytes) => read(state, &bytes, id, socket, up, down),
        Up::Room => roomy(state, id, up),
        Up::End => ended(state, socket, up, down),
        Up::Failed(_) => failed(state, socket, up, down),
    };
}

/// The idle deadline passed: no progress, so the connection is closed.
pub(crate) fn idled(conn: &mut Conn, up: &mut Queue<Call>, down: &mut Queue<Io>) {
    let socket = conn.socket;
    let state = mem::replace(&mut conn.state, State::Closed);
    conn.state = match state {
        State::Greeting => close(socket, None, up, down),
        State::Reading { session } | State::Draining { session } => close(socket, Some(session), up, down),
        State::Admitting { .. } | State::Answering { .. } | State::Closing(_) | State::Closed => {
            unreachable!("the idle deadline runs only while greeting, reading or draining")
        }
    };
}

/// io told `Closed`: the socket is gone.
pub(crate) fn closed(conn: &mut Conn) {
    let state = mem::replace(&mut conn.state, State::Closed);
    conn.state = match state {
        State::Closing(closing) => State::Closing(Closing { closed: true, ..closing }),
        State::Greeting
        | State::Admitting { .. }
        | State::Reading { .. }
        | State::Answering { .. }
        | State::Draining { .. }
        | State::Closed => unreachable!("io tells Closed only after the connection's close"),
    };
}

/// The domain answered the connection's call: in the down pass, so a `Gone`
/// this owes waits on the ready list.
pub(crate) fn replied(conn: &mut Conn, reply: Reply, env: &Env<Limits>, down: &mut Queue<Io>) {
    let socket = conn.socket;
    let state = mem::replace(&mut conn.state, State::Closed);
    conn.state = match reply {
        Reply::Admitted { session } => admitted(state, session, socket, down),
        Reply::Busy => busy(state, socket, down),
        Reply::Echo(text) => echoed(state, &text, socket, env, down),
    };
}

/// `Gone` told, from the ready list.
pub(crate) fn resumed(conn: &mut Conn, up: &mut Queue<Call>, down: &mut Queue<Io>) {
    let stop = mem::replace(&mut conn.stop, Stop::None);
    match stop {
        Stop::Drain => {
            let state = mem::replace(&mut conn.state, State::Closed);
            conn.state = ended(state, conn.socket, up, down);
            return;
        }
        Stop::Abort => {
            let state = mem::replace(&mut conn.state, State::Closed);
            conn.state = abort(state, conn.socket, up, down);
            return;
        }
        Stop::None => {}
    }
    let state = mem::replace(&mut conn.state, State::Closed);
    conn.state = match state {
        State::Closing(Closing { owed: Some(session), call: false, closed }) => {
            up.push(Call::Gone { session });
            State::Closing(Closing { closed, call: false, owed: None })
        }
        State::Closing(_)
        | State::Greeting
        | State::Admitting { .. }
        | State::Reading { .. }
        | State::Answering { .. }
        | State::Draining { .. }
        | State::Closed => unreachable!("only a connection that owes a Gone is on the ready list"),
    };
}

/// An owner abort leaves calls to settle, but io no longer flushes output.
fn abort(state: State, socket: Token, up: &mut Queue<Call>, down: &mut Queue<Io>) -> State {
    match state {
        State::Closing(closing) => {
            if !closing.closed {
                down.push(Io::Abort { entity: socket });
            }
            State::Closing(closing)
        }
        State::Closed => State::Closed,
        State::Greeting
        | State::Admitting { .. }
        | State::Reading { .. }
        | State::Answering { .. }
        | State::Draining { .. } => abort_open(state, socket, up, down),
    }
}

fn abort_open(state: State, socket: Token, up: &mut Queue<Call>, down: &mut Queue<Io>) -> State {
    down.push(Io::Abort { entity: socket });
    match state {
        State::Greeting => State::Closing(CLOSING),
        State::Reading { session } | State::Draining { session } => {
            up.push(Call::Gone { session });
            State::Closing(CLOSING)
        }
        State::Admitting { .. } => State::Closing(Closing { call: true, ..CLOSING }),
        State::Answering { session, .. } => {
            up.push(Call::Gone { session });
            State::Closing(Closing { call: true, ..CLOSING })
        }
        State::Closing(_) | State::Closed => unreachable!("only open states abort here"),
    }
}

fn read(state: State, bytes: &[u8], id: Id<Conn>, socket: Token, up: &mut Queue<Call>, down: &mut Queue<Io>) -> State {
    match state {
        State::Reading { session } => match bytes.split_last() {
            Some((&b'\n', text)) => {
                up.push(Call::Line { session, reply_to: ReplyTo::new(id.token()), text: bytes::copy_of(text) });
                State::Answering { session, ended: false }
            }
            // The scan met its maximum without the end of the line.
            Some(_) => {
                down.push(Io::Stream { stream: socket, down: Down::Send(bytes::copy_of(TOO_LONG)) });
                close(socket, Some(session), up, down)
            }
            None => unreachable!("a scan delivers its delimiter, or its maximum, which is not zero"),
        },
        // Told before io took the close.
        State::Closing(closing) => State::Closing(closing),
        State::Greeting
        | State::Admitting { .. }
        | State::Answering { .. }
        | State::Draining { .. }
        | State::Closed => unreachable!("io delivers bytes only for a read demand"),
    }
}

fn roomy(state: State, id: Id<Conn>, up: &mut Queue<Call>) -> State {
    match state {
        State::Greeting => {
            up.push(Call::Open { reply_to: ReplyTo::new(id.token()) });
            State::Admitting { ended: false }
        }
        State::Draining { session } => State::Reading { session },
        State::Closing(closing) => State::Closing(closing),
        State::Admitting { .. } | State::Reading { .. } | State::Answering { .. } | State::Closed => {
            unreachable!("io grants room only for a room demand")
        }
    }
}

/// The peer ended its stream: every complete line before it was read. With
/// a call out, the close waits for its reply, so that its answer is sent.
fn ended(state: State, socket: Token, up: &mut Queue<Call>, down: &mut Queue<Io>) -> State {
    match state {
        State::Greeting => close(socket, None, up, down),
        State::Reading { session } | State::Draining { session } => close(socket, Some(session), up, down),
        State::Admitting { ended: _ } => State::Admitting { ended: true },
        State::Answering { session, ended: _ } => State::Answering { session, ended: true },
        State::Closing(closing) => State::Closing(closing),
        State::Closed => unreachable!("a closed connection is retired"),
    }
}

/// The stream broke: closed at once, which for a broken stream is an abort.
/// A call out is still answered, and only then retires the connection.
fn failed(state: State, socket: Token, up: &mut Queue<Call>, down: &mut Queue<Io>) -> State {
    match state {
        State::Greeting => close(socket, None, up, down),
        State::Reading { session } | State::Draining { session } => close(socket, Some(session), up, down),
        // The session is not known until the domain answers.
        State::Admitting { .. } => {
            down.push(Io::Close { entity: socket });
            State::Closing(Closing { call: true, ..CLOSING })
        }
        // The line is ahead of the `Gone` in the domain's queue, so the
        // domain answers it before the session ends.
        State::Answering { session, .. } => {
            down.push(Io::Close { entity: socket });
            up.push(Call::Gone { session });
            State::Closing(Closing { call: true, ..CLOSING })
        }
        State::Closing(closing) => State::Closing(closing),
        State::Closed => unreachable!("a closed connection is retired"),
    }
}

/// io asked to close the socket, gracefully, and the domain told that the
/// session, if there is one, is gone.
fn close(socket: Token, session: Option<Token>, up: &mut Queue<Call>, down: &mut Queue<Io>) -> State {
    down.push(Io::Close { entity: socket });
    if let Some(session) = session {
        up.push(Call::Gone { session });
    }
    State::Closing(CLOSING)
}

fn admitted(state: State, session: Token, socket: Token, down: &mut Queue<Io>) -> State {
    match state {
        State::Admitting { ended: false } => State::Reading { session },
        State::Admitting { ended: true } => {
            down.push(Io::Close { entity: socket });
            State::Closing(Closing { owed: Some(session), ..CLOSING })
        }
        State::Closing(closing) => State::Closing(Closing { call: false, owed: Some(session), ..answered(closing) }),
        State::Greeting | State::Reading { .. } | State::Answering { .. } | State::Draining { .. } | State::Closed => {
            unreachable!("one reply per call: only an Open is admitted")
        }
    }
}

/// Refused at the entrance: the refusal goes in the room greeting asked for.
fn busy(state: State, socket: Token, down: &mut Queue<Io>) -> State {
    match state {
        State::Admitting { .. } => {
            down.push(Io::Stream { stream: socket, down: Down::Send(bytes::copy_of(BUSY)) });
            down.push(Io::Close { entity: socket });
            State::Closing(CLOSING)
        }
        State::Closing(closing) => State::Closing(answered(closing)),
        State::Greeting | State::Reading { .. } | State::Answering { .. } | State::Draining { .. } | State::Closed => {
            unreachable!("one reply per call: only an Open is refused")
        }
    }
}

fn echoed(state: State, text: &[u8], socket: Token, env: &Env<Limits>, down: &mut Queue<Io>) -> State {
    match state {
        State::Answering { session, ended } => {
            let answer = encode(text, env.limits.line);
            down.push(Io::Stream { stream: socket, down: Down::Send(answer) });
            if ended {
                down.push(Io::Close { entity: socket });
                State::Closing(Closing { owed: Some(session), ..CLOSING })
            } else {
                State::Draining { session }
            }
        }
        // Closing already: the answer has no one to go to.
        State::Closing(closing) => State::Closing(answered(closing)),
        State::Greeting | State::Admitting { .. } | State::Reading { .. } | State::Draining { .. } | State::Closed => {
            unreachable!("one reply per call: only a line is echoed")
        }
    }
}

/// The reply to the call out has come.
fn answered(closing: Closing) -> Closing {
    assert!(closing.call, "one reply per call: a closing connection is answered only for its call out");
    Closing { call: false, ..closing }
}

/// A line's answer: its text and the end of line, in a box of exactly their
/// length, within the room the connection holds.
fn encode(text: &[u8], line: u32) -> Box<[u8]> {
    let len = text.len().checked_add(1).expect("a text shorter than memory");
    let fits = match u32::try_from(len) {
        Ok(len) => len <= line,
        Err(_) => false,
    };
    assert!(fits, "an answer fits the room held for it: the domain echoes a text no longer than the line it came in");
    let mut writer = Writer::new(len);
    writer.put(text).expect("the writer is sized for the text and its end of line");
    writer.put(b"\n").expect("the writer is sized for the text and its end of line");
    writer.finish()
}
