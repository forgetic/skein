//! The server-sent events reader (http.md, 4): a step machine that reads an
//! event stream (WHATWG HTML, 9.2) from the body below, by demand, and
//! hands the side above one event at a time, each when it asks for it. Its
//! other side, which writes one, is [`writer`].
//!
//! # Its two sides
//!
//! Below, a `lib::stream` (lib.md, 7): a response body, from the HTTP
//! client. The reader demands a line at a time: a scan to the first line
//! end, a CR or an LF ([`Read::Line`]), of at most [`Limits::chunk`], so a
//! delivery holds one line end at most, as its last byte, and a line longer
//! than a chunk comes in several. A line ends at LF, at CRLF, or at CR
//! alone: a CR ends its line at once, and a lone LF delivered right after
//! it is the CRLF's second byte, and is skipped. An LF-only stream is read
//! a delivery a line, as is a CR-only one; a CRLF one, two. The reader
//! keeps nothing of a delivery past the step that reads it, and sends
//! nothing, so it asks for no room.
//!
//! Above, the service's protocol layer ([`Request`] down, [`Event`] up):
//!
//! - **`Next` demands one event.** Exactly one event answers it: the next
//!   `Message`, or the stream's outcome, `Ended` or `Failed`, after which
//!   nothing follows but `Closed`. One `Next` at a time, and none after the
//!   outcome or the close: the side above's bug otherwise, asserted.
//! - **An event's data goes up whole,** in one box, once its blank line is
//!   read: the event's type and id may come after its data lines, an event
//!   the stream cuts short is dropped, and a decoder chooses what to do
//!   with the data by the type, or by the data itself (the `[DONE]` some
//!   providers end a stream with is not JSON). A machine stacked above
//!   reads it through [`Data`].
//! - **`Close` ends the reader in any state.** It withdraws what it
//!   demanded below, drops a `Next` not yet answered, and answers `Closed`,
//!   its one terminal event. It does not close the stream below.
//!
//! # Bounds
//!
//! A line holds at most [`Limits::line`] bytes before its ending; an
//! event, at most [`Limits::event`] bytes of its lines, endings and blank
//! line included, so a stream that never ends an event fails, whatever it
//! sends; an event's type or id, at most [`Limits::field`]. Past any of
//! them, the stream fails. Each entry point emits at most [`UP_MAX_OUT`] or
//! [`DOWN_MAX_OUT`]; [`worst_case`] is what a reader holds;
//! [`largest_demand`] is what whoever stacks it checks against the side
//! below at startup.

use core::mem;

use alloc::boxed::Box;

use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, List, Queue};

use crate::MaxOut;

mod lines;
pub mod writer;

use lines::{Lines, Step};
pub use skein_lib::stream::Held as Data;

/// The reader's limits (programming-model.md, 7): the same for every step
/// and for [`Reader::new`], which allocates by them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// The longest line, in bytes before its ending: past it,
    /// [`Error::LineTooLong`].
    pub line: u32,
    /// The longest event, in bytes read from the end of the last one to its
    /// blank line, endings included: past it, [`Error::EventTooLong`]. An
    /// event's data is at most this long.
    pub event: u32,
    /// The longest event type or id: past it, [`Error::FieldTooLong`].
    pub field: u32,
    /// The most bytes demanded at once: each scan's maximum, so the most a
    /// delivery holds. At least one.
    pub chunk: u32,
}

/// The most bytes the reader demands at once: a scan of [`Limits::chunk`].
///
/// Whoever stacks the reader checks at startup that the side below takes a
/// demand this large (lib.md, 7): for the HTTP client, its
/// `Limits::read`.
#[must_use]
pub fn largest_demand(limits: &Limits) -> u32 {
    limits.chunk
}

/// The most memory a reader holds under `limits`, in bytes
/// (programming-model.md, 6.3), or `None` if it does not fit a `u64` or the
/// limits cannot be honoured: a [`Limits::chunk`] of zero.
///
/// It is the event's data, at most [`Limits::event`]; its type, an `id`
/// field's value, the last event ID buffer and the last event ID, each at
/// most [`Limits::field`], all allocated with the reader; and the delivery
/// it reads, at most [`Limits::chunk`], dropped by the step that reads it.
/// An event's boxes are handed out when it goes up.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.chunk == 0 {
        return None;
    }
    let data = List::<u8>::worst_case(limits.event)?;
    let fields = List::<u8>::worst_case(limits.field)?.checked_mul(4)?;
    data.checked_add(fields)?.checked_add(u64::from(limits.chunk))
}

/// [`up`]'s: an event or the outcome, or the next demand.
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 1, below: 1 };

/// [`down`]'s: for a `Next`, an event or the outcome, or a demand; for a
/// `Close`, `Closed` and the demand withdrawn.
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 1, below: 1 };

/// From the side above.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// A demand for the next event. Exactly one [`Event`] answers it, a
    /// `Message`, `Ended` or `Failed`, unless a `Close` comes first.
    Next,
    /// Closes the reader, in any state. `Closed` answers it.
    Close,
}

/// To the side above.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// The next event, for a `Next`.
    Message(Message),
    /// For a `Next`: the stream ended. An event it cut short, before its
    /// blank line, is dropped, as the standard says. Nothing follows but
    /// `Closed`.
    Ended,
    /// For a `Next`: a line or an event past its limit, or the stream below
    /// failed. Nothing follows but `Closed`.
    Failed(Error),
    /// For a `Close`: the reader is closed. Terminal.
    Closed,
}

/// An event, dispatched at its blank line (WHATWG HTML, 9.2.6).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Message {
    /// Its type: the last `event` field's value, or `message`.
    pub name: Box<[u8]>,
    /// Its data: each `data` field's value, joined by LFs.
    pub data: Box<[u8]>,
    /// The last event ID, as the last `id` field before its blank line set
    /// it, in this event or an earlier one; empty if none did.
    pub id: Box<[u8]>,
}

/// Why a stream failed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// A line longer than [`Limits::line`].
    LineTooLong,
    /// An event longer than [`Limits::event`], or a stream that does not
    /// end one within it.
    EventTooLong,
    /// An event type or an id longer than [`Limits::field`].
    FieldTooLong,
    /// The stream below failed.
    Stream(Fault),
}

/// What a reader is waiting for. Machines keep no timers
/// (programming-model.md, 4): each says what it waits for, and the
/// connection arms the deadlines.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// For the side above to ask for the next event.
    Next,
    /// For the side below to meet its demand: the peer's progress. Every
    /// line is progress, a comment sent to keep the stream alive among
    /// them.
    Bytes,
    /// For the side above to close it: the stream's outcome went up.
    Close,
    /// For nothing: it is closed.
    Nothing,
}

/// A reader of one event stream: a connection's state for this machine.
#[derive(Debug)]
pub struct Reader {
    state: State,
    lines: Lines,
}

impl Reader {
    /// A reader under `limits`, the limits its steps will be given. It
    /// demands nothing until the side above asks for an event.
    #[must_use]
    pub fn new(limits: &Limits) -> Reader {
        assert!(limits.chunk > 0, "a scan holds the byte it ends at");
        Reader { state: State::Idle(Below::Open), lines: Lines::new(limits) }
    }

    /// What it is waiting for: a function of its state alone.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match self.state {
            State::Idle(_) => Waiting::Next,
            State::Reading => Waiting::Bytes,
            State::Over => Waiting::Close,
            State::Closed => Waiting::Nothing,
        }
    }

    /// The reconnection time the stream's `retry` fields set, in
    /// milliseconds, if one did: for a client that reconnects.
    #[must_use]
    pub fn retry(&self) -> Option<u64> {
        self.lines.retry()
    }

    /// The last event ID, as the stream's last blank line left it: what a
    /// client that reconnects sends as `Last-Event-ID`.
    #[must_use]
    pub fn last_event_id(&self) -> &[u8] {
        self.lines.last_id()
    }
}

/// An event from the stream below. Emits at most [`UP_MAX_OUT`].
pub fn up(reader: &mut Reader, env: &Env<Limits>, ev: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let lines = &mut reader.lines;
    let state = mem::replace(&mut reader.state, State::Closed);
    let was = Was::of(&state);
    reader.state = match state {
        State::Reading => match ev {
            Up::Bytes(delivery) => read(lines, limits, &delivery, above),
            Up::End => ended(lines, above),
            Up::Failed(fault) => fail(lines, Error::Stream(fault), above),
            Up::Room => unreachable!("the reader asks for no room"),
        },
        State::Idle(told) => match ev {
            Up::Bytes(_) => unreachable!("bytes delivered without a read demand"),
            Up::End => State::Idle(told.ended()),
            Up::Failed(fault) => State::Idle(told.failed(fault)),
            Up::Room => unreachable!("the reader asks for no room"),
        },
        State::Over => match ev {
            Up::Bytes(_) => unreachable!("bytes delivered after the outcome, which leaves no read demand"),
            Up::End | Up::Failed(_) => State::Over,
            Up::Room => unreachable!("the reader asks for no room"),
        },
        // What the close withdrew may have been met already, on its way.
        State::Closed => match ev {
            Up::Bytes(_) | Up::End | Up::Failed(_) => State::Closed,
            Up::Room => unreachable!("the reader asks for no room"),
        },
    };
    demand(was, &reader.state, limits, below);
}

/// A request from the side above. Emits at most [`DOWN_MAX_OUT`].
pub fn down(reader: &mut Reader, env: &Env<Limits>, rq: Request, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let lines = &mut reader.lines;
    let state = mem::replace(&mut reader.state, State::Closed);
    let was = Was::of(&state);
    reader.state = match rq {
        Request::Next => match state {
            State::Idle(told) => next(lines, told, above),
            State::Reading => unreachable!("a Next before the last one was answered"),
            State::Over => unreachable!("a Next after the stream's outcome"),
            State::Closed => unreachable!("a Next after Closed"),
        },
        Request::Close => close(lines, state, above),
    };
    demand(was, &reader.state, limits, below);
}

/// What the reader is doing about the side above's demand.
#[derive(Debug)]
enum State {
    /// Waiting for the side above to ask for the next event, with nothing
    /// demanded below, and what the side below told meanwhile.
    Idle(Below),
    /// Reading for the side above's `Next`, with a demand below.
    Reading,
    /// The stream's outcome, `Ended` or `Failed`, went up.
    Over,
    /// Closed: terminal, and the placeholder of every transition.
    Closed,
}

/// Whether a state had a demand outstanding below, which a close withdraws.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Was {
    Reading,
    NotReading,
}

impl Was {
    fn of(state: &State) -> Was {
        match state {
            State::Reading => Was::Reading,
            State::Idle(_) | State::Over | State::Closed => Was::NotReading,
        }
    }
}

/// What the side below told an idle reader, for its next `Next`: with no
/// demand outstanding, it may still end or fail (lib.md, 7).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Below {
    /// Nothing: the next event begins with the next byte demanded.
    Open,
    /// The stream ended.
    Ended,
    /// The stream failed before it ended.
    Failed(Fault),
}

impl Below {
    /// The stream ended. Nothing comes after an end or a failure, a second
    /// end included.
    fn ended(self) -> Below {
        match self {
            Below::Open => Below::Ended,
            Below::Ended | Below::Failed(_) => self,
        }
    }

    /// The stream failed. After the end, a failure says only that the
    /// stream can no longer send: what was read stands.
    fn failed(self, fault: Fault) -> Below {
        match self {
            Below::Open => Below::Failed(fault),
            Below::Ended | Below::Failed(_) => self,
        }
    }
}

/// States what the state `after` a transition demands below, in one place
/// after every transition (programming-model.md, 5.4): a reading state
/// states its demand, as every transition into one follows a delivery that
/// met the last, or leaves `Idle`, which has none; a close withdraws the
/// demand of a state that was reading.
fn demand(was: Was, after: &State, limits: &Limits, below: &mut Queue<Down>) {
    let read = match after {
        State::Reading => Read::Line { max: limits.chunk },
        State::Closed => match was {
            Was::Reading => Read::Nothing,
            Was::NotReading => return,
        },
        State::Idle(_) | State::Over => return,
    };
    below.push(Down::Demand { read, room: 0 });
}

/// `Next`, while idle.
fn next(lines: &mut Lines, told: Below, above: &mut Queue<Event>) -> State {
    match told {
        Below::Open => State::Reading,
        Below::Ended => ended(lines, above),
        Below::Failed(fault) => fail(lines, Error::Stream(fault), above),
    }
}

/// Reads a delivery for the side above's `Next`: a line, or a chunk of
/// one. Its one line end is its last byte, so it is read whole, and an
/// event it dispatches is the last thing in it.
fn read(lines: &mut Lines, limits: &Limits, delivery: &[u8], above: &mut Queue<Event>) -> State {
    match lines.read(limits, delivery) {
        Step::Message(message) => {
            above.push(Event::Message(message));
            State::Idle(Below::Open)
        }
        Step::Fail(error) => fail(lines, error, above),
        Step::More => State::Reading,
    }
}

/// The stream ended: an event it cut short is dropped.
fn ended(lines: &mut Lines, above: &mut Queue<Event>) -> State {
    lines.clear();
    above.push(Event::Ended);
    State::Over
}

fn fail(lines: &mut Lines, error: Error, above: &mut Queue<Event>) -> State {
    lines.clear();
    above.push(Event::Failed(error));
    State::Over
}

/// `Close`, in any state: what was demanded below is withdrawn by `demand`.
fn close(lines: &mut Lines, state: State, above: &mut Queue<Event>) -> State {
    match state {
        State::Idle(_) | State::Reading | State::Over => {}
        State::Closed => unreachable!("a Close after Closed"),
    }
    lines.clear();
    above.push(Event::Closed);
    State::Closed
}
