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
//! holds at most one delivery while its data is demanded, and sends
//! nothing, so it asks for no room.
//!
//! Above, Next opens an event at its first data line. Data is a stream
//! face: demands are met from bounded line pieces, and withdrawal scans
//! the remainder. End and Dispatched come together at the blank line.
//! A truncated event fails its data and is never dispatched; the next
//! Next receives the stream outcome. Close withdraws the body demand and
//! emits Closed. The reader knows no application meaning of the data.
//!
//! # Transitions
//!
//! | State | Input | Result |
//! | --- | --- | --- |
//! | Between events | Next | Find the first data line, or tell a held body outcome |
//! | Finding | First data line | Opened; wait for a data demand |
//! | Open | Data demand | Read line pieces until its demand is met |
//! | Reading | Demand met | Data bytes; wait for the next data demand |
//! | Open or reading | Data withdrawal | Scan and drop the remaining data |
//! | Reading or discarding | Blank line | Data End and Dispatched; between events |
//! | Open, reading or discarding | Body outcome or limit | Data Failed; hold the reader outcome for Next |
//! | Any active state | Close | Closed; withdraw an outstanding body read |
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

use alloc::boxed::Box;

use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Intake, List, Queue, bytes};

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
/// It is the data intake and one bounded line piece, each at most
/// [`Limits::chunk`]; and its type, ID field, last ID buffer and last ID,
/// each at most [`Limits::field`]. Emitted data and dispatch metadata
/// belong to their receiver (programming-model.md, 6.2).
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.chunk == 0 {
        return None;
    }
    let data = Intake::worst_case(limits.chunk)?;
    let fields = List::<u8>::worst_case(limits.field)?.checked_mul(4)?;
    data.checked_add(fields)?.checked_add(u64::from(limits.chunk))
}

/// [`up`]'s: an event or the outcome, or the next demand.
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 2, below: 1 };

/// [`down`]'s: for a `Next`, an event or the outcome, or a demand; for a
/// `Close`, `Closed` and the demand withdrawn.
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 1, below: 1 };

/// From the owner: an event demand, its data face, or the reader's close.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// Next event: Opened, Ended or Failed answers it.
    Next,
    /// An open event's byte stream, read by its owner.
    Data(Down),
    /// Ends the reader in any state; Closed is its terminal.
    Close,
}

/// To the owner: an event's stream and dispatch, or the reader's terminal.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// The first data line began, answering Next.
    Opened,
    /// The open event's stream: bytes, End at dispatch, or a cut's failure.
    Data(Up),
    /// The blank line's type and ID, immediately after the data's End.
    Dispatched(Dispatch),
    /// Between events the body ended, answering Next; only Closed follows.
    Ended,
    /// A body or limit failure, answering Next; only Closed follows.
    Failed(Error),
    /// Close ended the reader; nothing follows.
    Closed,
}

impl Clone for Event {
    fn clone(&self) -> Event {
        match self {
            Event::Opened => Event::Opened,
            Event::Data(data) => Event::Data(match data {
                Up::Bytes(bytes) => Up::Bytes(bytes.clone()),
                Up::Room => Up::Room,
                Up::End => Up::End,
                Up::Failed(fault) => Up::Failed(*fault),
            }),
            Event::Dispatched(dispatch) => Event::Dispatched(dispatch.clone()),
            Event::Ended => Event::Ended,
            Event::Failed(error) => Event::Failed(*error),
            Event::Closed => Event::Closed,
        }
    }
}

/// The metadata at an event's blank line, sent by the reader to its owner.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Dispatch {
    /// The final event field, or message when none was set.
    pub name: Box<[u8]>,
    /// The last ID, updated by this event or retained from an earlier one.
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

/// What the owner observes while deciding whether its progress deadline runs.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// The owner may ask for the next event.
    Next,
    /// A body read is outstanding; its delivery is progress.
    Bytes,
    /// An open event waits for a data demand.
    Above,
    /// The stream's outcome went up; the owner closes it.
    Close,
    /// The reader is closed.
    Nothing,
}

/// Reusable fields and one intake; never a whole event, owned by its connection.
#[derive(Debug)]
pub struct Reader {
    state: State,
    lines: Lines,
    intake: Intake,
    piece: Box<[u8]>,
    at: u32,
    body_wait: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum State {
    Idle(Below),
    Finding,
    Open,
    Reading(Read),
    Discarding,
    Over,
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Below {
    Open,
    Ended,
    Failed(Error),
}

impl Reader {
    /// Builds the reusable fields and data intake under stable nonzero chunk limits.
    #[must_use]
    pub fn new(limits: &Limits) -> Reader {
        assert!(limits.chunk > 0, "a scan holds the byte it ends at");
        Reader {
            state: State::Idle(Below::Open),
            lines: Lines::new(limits),
            intake: Intake::with_capacity(limits.chunk),
            piece: bytes::copy_of(b""),
            at: 0,
            body_wait: false,
        }
    }

    /// Its wait is a function of state alone, for the owner's deadline.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match self.state {
            State::Idle(_) => Waiting::Next,
            State::Finding | State::Reading(_) | State::Discarding => Waiting::Bytes,
            State::Open => Waiting::Above,
            State::Over => Waiting::Close,
            State::Closed => Waiting::Nothing,
        }
    }

    /// Reconnection milliseconds set by the stream's retry fields, if any.
    #[must_use]
    pub fn retry(&self) -> Option<u64> {
        self.lines.retry()
    }

    /// The ID left by the most recent blank line, for reconnection.
    #[must_use]
    pub fn last_event_id(&self) -> &[u8] {
        self.lines.last_id()
    }
}

/// Receives the body stream, emitting at most [`UP_MAX_OUT`].
pub fn up(reader: &mut Reader, env: &Env<Limits>, event: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    match reader.state {
        State::Closed => match event {
            Up::Bytes(_) | Up::End | Up::Failed(_) => return,
            Up::Room => unreachable!("the reader asks for no room"),
        },
        State::Over => match event {
            Up::End | Up::Failed(_) => return,
            Up::Bytes(_) => unreachable!("no body demand follows its outcome"),
            Up::Room => unreachable!("the reader asks for no room"),
        },
        State::Idle(_) | State::Finding | State::Open | State::Reading(_) | State::Discarding => {}
    }
    match event {
        Up::Bytes(piece) => {
            assert!(reader.body_wait && reader.piece.is_empty(), "a delivery meets the body's one demand");
            assert!(
                piece.len() <= usize::try_from(env.limits.chunk).expect("u32 fits usize"),
                "a line scan fits its chunk"
            );
            for byte in piece.get(..piece.len().saturating_sub(1)).expect("a delivery prefix") {
                assert!(*byte != b'\r' && *byte != b'\n', "a delivery stops at its first line ending");
            }
            reader.body_wait = false;
            reader.piece = piece;
            reader.at = 0;
            pump(reader, env, above);
        }
        Up::End => {
            reader.body_wait = false;
            finish_before_outcome(reader, env, above);
            outcome(reader, Below::Ended, above);
        }
        Up::Failed(fault) => {
            reader.body_wait = false;
            finish_before_outcome(reader, env, above);
            outcome(reader, Below::Failed(Error::Stream(fault)), above);
        }
        Up::Room => unreachable!("the reader asks for no room"),
    }
    demand(reader, env, below);
}

/// Receives an event/data demand or close, emitting at most [`DOWN_MAX_OUT`].
pub fn down(
    reader: &mut Reader,
    env: &Env<Limits>,
    request: Request,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    match request {
        Request::Next => match reader.state {
            State::Idle(told) => match told {
                Below::Open => {
                    reader.state = State::Finding;
                    pump(reader, env, above);
                }
                Below::Ended => {
                    reader.state = State::Over;
                    above.push(Event::Ended);
                }
                Below::Failed(error) => {
                    reader.state = State::Over;
                    above.push(Event::Failed(error));
                }
            },
            State::Finding | State::Open | State::Reading(_) | State::Discarding | State::Over | State::Closed => {
                unreachable!("Next is asked between events, before the outcome")
            }
        },
        Request::Data(request) => data_demand(reader, env, request, above),
        Request::Close => {
            match reader.state {
                State::Idle(_) | State::Finding | State::Open | State::Reading(_) | State::Discarding | State::Over => {
                }
                State::Closed => unreachable!("a Close after Closed"),
            }
            clear(reader);
            reader.state = State::Closed;
            above.push(Event::Closed);
            if reader.body_wait {
                reader.body_wait = false;
                below.push(Down::Demand { read: Read::Nothing, room: 0 });
            }
        }
    }
    demand(reader, env, below);
}

fn data_demand(reader: &mut Reader, env: &Env<Limits>, request: Down, above: &mut Queue<Event>) {
    let read = match request {
        Down::Demand { read, room } => {
            assert!(room == 0, "the event data is read only");
            read
        }
        Down::Send(_) | Down::Finish => unreachable!("event data is read only"),
    };
    match reader.state {
        State::Open => {}
        State::Reading(_) => assert!(read == Read::Nothing, "one data demand at a time"),
        State::Idle(_) | State::Finding | State::Discarding | State::Over | State::Closed => {
            unreachable!("data demands belong to an open event")
        }
    }
    let wanted = match read {
        Read::Nothing => {
            reader.intake.clear();
            reader.state = State::Discarding;
            pump(reader, env, above);
            return;
        }
        Read::Fill(count) => count,
        Read::Scan { max, .. } | Read::Line { max } => max,
    };
    assert!(wanted > 0 && wanted <= env.limits.chunk, "the data face meets the admitted read maximum");
    reader.state = State::Reading(read);
    pump(reader, env, above);
}

fn demand(reader: &mut Reader, env: &Env<Limits>, below: &mut Queue<Down>) {
    match reader.state {
        State::Finding | State::Reading(_) | State::Discarding => {
            assert!(reader.piece.is_empty(), "held line bytes are read before the next demand");
            if !reader.body_wait {
                reader.body_wait = true;
                below.push(Down::Demand { read: Read::Line { max: env.limits.chunk }, room: 0 });
            }
        }
        State::Idle(_) | State::Open | State::Over | State::Closed => {}
    }
}

fn clear(reader: &mut Reader) {
    reader.lines.clear();
    reader.intake.clear();
    reader.piece = bytes::copy_of(b"");
    reader.at = 0;
}

/// A body terminal also ends a held line piece. Validate its already
/// delivered tail before selecting the terminal's reason.
fn finish_before_outcome(reader: &mut Reader, env: &Env<Limits>, above: &mut Queue<Event>) {
    match reader.state {
        State::Open => {
            reader.state = State::Discarding;
            pump(reader, env, above);
        }
        State::Idle(_) | State::Finding | State::Reading(_) | State::Discarding | State::Over | State::Closed => {}
    }
}

fn outcome(reader: &mut Reader, told: Below, above: &mut Queue<Event>) {
    match reader.state {
        State::Idle(previous) => {
            reader.state = State::Idle(match previous {
                Below::Open => told,
                Below::Ended | Below::Failed(_) => previous,
            });
        }
        State::Finding => {
            clear(reader);
            reader.state = State::Over;
            match told {
                Below::Ended => above.push(Event::Ended),
                Below::Failed(error) => above.push(Event::Failed(error)),
                Below::Open => unreachable!("a terminal below"),
            }
        }
        State::Open | State::Reading(_) | State::Discarding => {
            clear(reader);
            reader.state = State::Idle(told);
            let fault = match told {
                Below::Ended => Fault::Other,
                Below::Failed(Error::Stream(fault)) => fault,
                Below::Failed(Error::LineTooLong | Error::EventTooLong | Error::FieldTooLong) => Fault::Invalid,
                Below::Open => unreachable!("a terminal below"),
            };
            above.push(Event::Data(Up::Failed(fault)));
        }
        State::Over | State::Closed => {}
    }
}

fn meet(reader: &mut Reader, above: &mut Queue<Event>) -> bool {
    match reader.state {
        State::Reading(read) => match reader.intake.meet(read) {
            Some(bytes) => {
                reader.state = State::Open;
                above.push(Event::Data(Up::Bytes(bytes)));
                true
            }
            None => false,
        },
        State::Finding | State::Discarding => false,
        State::Idle(_) | State::Open | State::Over | State::Closed => true,
    }
}

fn pump(reader: &mut Reader, env: &Env<Limits>, above: &mut Queue<Event>) {
    if meet(reader, above) {
        return;
    }
    let remaining = u32::try_from(reader.piece.len())
        .expect("a delivery fits chunk")
        .checked_sub(reader.at)
        .expect("inside the line piece");
    for _ in 0..remaining {
        let byte =
            *reader.piece.get(usize::try_from(reader.at).expect("u32 fits usize")).expect("inside the line piece");
        reader.at = reader.at.checked_add(1).expect("bounded by the delivery");
        let action = reader.lines.byte(&env.limits, byte);
        match action {
            Step::More => {}
            Step::Opened => {
                reader.state = State::Open;
                above.push(Event::Opened);
                finish_piece(reader);
                return;
            }
            Step::Data(byte) => match reader.state {
                State::Reading(_) => reader.intake.append(&[byte]).expect("a full intake meets every admitted read"),
                State::Discarding => {}
                State::Idle(_) | State::Finding | State::Open | State::Over | State::Closed => {
                    unreachable!("an open event's data")
                }
            },
            Step::Dispatched(dispatch) => {
                assert!(
                    usize::try_from(reader.at).expect("u32 fits usize") == reader.piece.len(),
                    "a blank line ends the delivery"
                );
                reader.intake.clear();
                reader.state = State::Idle(Below::Open);
                above.push(Event::Data(Up::End));
                above.push(Event::Dispatched(dispatch));
                finish_piece(reader);
                return;
            }
            Step::Fail(error) => {
                outcome(reader, Below::Failed(error), above);
                return;
            }
        }
        if meet(reader, above) {
            finish_piece(reader);
            return;
        }
    }
    finish_piece(reader);
}

fn finish_piece(reader: &mut Reader) {
    if usize::try_from(reader.at).expect("u32 fits usize") == reader.piece.len() {
        reader.piece = bytes::copy_of(b"");
        reader.at = 0;
    }
}
