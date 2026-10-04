//! The server-sent events writer (http.md, 4.3): a step machine that frames
//! each event the side above gives it as the standard's lines (WHATWG
//! HTML, 9.2.6), sized, and writes it into the body stream below, one at a
//! time, within the room it is granted.
//!
//! # Its two sides
//!
//! Below, a `lib::stream` (lib.md, 7), written: a response body, from the
//! HTTP server. The writer demands room only, of at most
//! [`Limits::chunk`], and sends an event in as many pieces as that takes,
//! each within the room granted; it reads nothing, so the stream's end
//! changes nothing for it, and room may still come after it.
//!
//! Above, the service's protocol layer ([`Request`] down, [`Event`] up):
//!
//! - **`Event` writes one event, and `Comment` one comment,** a block
//!   ended by a blank line each, so that a reader counts each on its own
//!   and a stream kept alive by comments runs on (http.md, 4.2). Exactly
//!   one event answers each: `Sent`, once all of it went down; `Refused`,
//!   for what the side above got wrong, writing nothing; or `Failed`, the
//!   stream's failure. One at a time: the side above's bug otherwise,
//!   asserted.
//! - **`Finish` ends the body:** it goes below, once nothing is being
//!   written.
//! - **`Close` ends the writer in any state.** It withdraws what it
//!   demanded below, drops an event not yet sent, and answers `Closed`,
//!   its one terminal event. It does not close the stream below.
//!
//! # Bounds
//!
//! An event or a comment is at most [`Limits::event`] bytes framed, its
//! blank line included. The writer holds the one it writes, and nothing
//! else: [`worst_case`]. Whoever stacks it checks [`largest_room`] against
//! the side below's at startup: the server's `Limits::send`.

use core::mem;

use alloc::boxed::Box;

use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Decimal, Env, Queue, bytes};

use crate::MaxOut;

/// The writer's limits (programming-model.md, 7): the same for every step.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// The longest event or comment framed, in bytes: its lines, their
    /// endings, and the blank line that ends it. Past it,
    /// [`Refusal::TooLong`]. A reader's `Limits::event` at least this
    /// reads every event the writer sends.
    pub event: u32,
    /// The most room the writer demands at once: each `Send` is at most
    /// this. At least 1.
    pub chunk: u32,
}

/// The most room the writer demands below at once: whoever stacks it
/// checks at startup that the side below grants this much (lib.md, 7), the
/// server's `Limits::send`.
#[must_use]
pub fn largest_room(limits: &Limits) -> u32 {
    limits.chunk
}

/// The most memory a writer holds under `limits`, in bytes
/// (programming-model.md, 6.3), or `None` if the limits cannot be
/// honoured: a chunk of nothing.
///
/// It is the event being written, framed, at most [`Limits::event`], held
/// until all of it went down. What the side above gives it is the side
/// above's to count, and a piece of the event is handed out when it is
/// sent.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.chunk == 0 {
        return None;
    }
    Some(u64::from(limits.event))
}

/// [`up`]'s: for room, a piece sent and the room for the next, or the last
/// piece and `Sent`; for a failure, `Failed`.
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 1, below: 2 };

/// [`down`]'s: for an event, the room for its first piece, or `Refused` or
/// `Failed`; for `Finish`, the stream's end; for `Close`, `Closed` and the
/// demand withdrawn.
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 1, below: 1 };

/// From the side above.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// Writes an event, while nothing else is being written. `Sent`,
    /// `Refused` or `Failed` answers it.
    Event(Outgoing),
    /// Writes a comment, as an event is written: `: ` and its text, and a
    /// blank line, to keep the stream alive. `Sent`, `Refused` or `Failed`
    /// answers it.
    Comment(Box<[u8]>),
    /// Ends the body, once nothing is being written.
    Finish,
    /// Closes the writer, in any state. `Closed` answers it.
    Close,
}

/// To the side above.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// For an `Event` or a `Comment`: all of it went down.
    Sent,
    /// For an `Event` or a `Comment`: refused, writing nothing. The writer
    /// is as it was.
    Refused(Refusal),
    /// For an `Event` or a `Comment`: the stream below failed, now or
    /// before. Nothing follows but `Closed`.
    Failed(Fault),
    /// For a `Close`: the writer is closed. Terminal.
    Closed,
}

/// An event to write (WHATWG HTML, 9.2.6).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Outgoing {
    /// Its type, the `event` field: none when empty, which a reader reads
    /// as `message`.
    pub name: Box<[u8]>,
    /// Its data: a `data` field for each line of it, the lines split at
    /// each LF, CRLF or CR, which a reader joins with LFs. Empty data is
    /// one empty line: every event written is one a reader dispatches.
    pub data: Box<[u8]>,
    /// The `id` field, if any: what a reader's last event ID becomes. An
    /// empty one resets it.
    pub id: Option<Box<[u8]>>,
    /// The `retry` field, if any: a reader's reconnection time, in
    /// milliseconds.
    pub retry: Option<u64>,
}

/// Why the writer refused an event or a comment, before writing anything:
/// a fault of the side above, which it can fix.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// The type holds a CR or an LF, which would end its line.
    Name,
    /// The id holds a CR or an LF, or a NUL, for which a reader ignores it.
    Id,
    /// The comment holds a CR or an LF.
    Comment,
    /// Framed, it is longer than [`Limits::event`].
    TooLong,
}

/// What a writer is waiting for. Machines keep no timers
/// (programming-model.md, 4): each says what it waits for, and the
/// connection arms the deadlines.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// For the side above: the next event, comment, or `Finish`.
    Above,
    /// For room below: the reader is not reading.
    Room,
    /// For the side above to close it: the body was finished, or the
    /// stream failed.
    Close,
    /// For nothing: it is closed.
    Nothing,
}

/// A writer of one event stream: a connection's state for this machine.
#[derive(Debug)]
pub struct Writer {
    state: State,
}

impl Writer {
    /// A writer, which demands nothing until the side above gives it
    /// something to write.
    #[must_use]
    pub fn new(limits: &Limits) -> Writer {
        assert!(limits.chunk > 0, "room for a byte at least");
        Writer { state: State::Idle(Below::Open) }
    }

    /// What it is waiting for: a function of its state alone.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match self.state {
            State::Idle(_) => Waiting::Above,
            State::Writing(_) => Waiting::Room,
            State::Finished | State::Over => Waiting::Close,
            State::Closed => Waiting::Nothing,
        }
    }
}

/// An event from the stream below. Emits at most [`UP_MAX_OUT`].
pub fn up(writer: &mut Writer, env: &Env<Limits>, ev: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let state = mem::replace(&mut writer.state, State::Closed);
    writer.state = match ev {
        Up::Room => match state {
            State::Writing(writing) => room(writing, limits.chunk, above, below),
            // An answer to the demand the close withdrew, on its way.
            State::Closed => State::Closed,
            State::Idle(_) | State::Finished | State::Over => unreachable!("room answers a demand for room"),
        },
        Up::Failed(fault) => match state {
            State::Writing(_) => {
                above.push(Event::Failed(fault));
                State::Over
            }
            State::Idle(Below::Open) => State::Idle(Below::Failed(fault)),
            state @ (State::Idle(Below::Failed(_)) | State::Finished | State::Over | State::Closed) => state,
        },
        // The writer reads nothing: room may still come after the end.
        Up::End => state,
        Up::Bytes(_) => unreachable!("the writer reads nothing"),
    };
}

/// A request from the side above. Emits at most [`DOWN_MAX_OUT`].
pub fn down(writer: &mut Writer, env: &Env<Limits>, rq: Request, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let state = mem::replace(&mut writer.state, State::Closed);
    writer.state = match rq {
        Request::Event(outgoing) => match state {
            State::Idle(told) => write(told, event(&outgoing, limits.event), limits.chunk, above, below),
            State::Writing(_) => unreachable!("an event while another is being written"),
            State::Finished => unreachable!("an event after Finish"),
            State::Over => unreachable!("an event after the stream failed"),
            State::Closed => unreachable!("an event after Closed"),
        },
        Request::Comment(text) => match state {
            State::Idle(told) => write(told, comment(&text, limits.event), limits.chunk, above, below),
            State::Writing(_) => unreachable!("a comment while an event is being written"),
            State::Finished => unreachable!("a comment after Finish"),
            State::Over => unreachable!("a comment after the stream failed"),
            State::Closed => unreachable!("a comment after Closed"),
        },
        Request::Finish => match state {
            State::Idle(Below::Open) => {
                below.push(Down::Finish);
                State::Finished
            }
            // The stream cannot send: there is nothing to end.
            State::Idle(Below::Failed(_)) => State::Finished,
            State::Writing(_) => unreachable!("a Finish while an event is being written"),
            State::Finished => unreachable!("a second Finish"),
            State::Over => unreachable!("a Finish after the stream failed"),
            State::Closed => unreachable!("a Finish after Closed"),
        },
        Request::Close => {
            match state {
                State::Writing(_) => below.push(Down::Demand { read: Read::Nothing, room: 0 }),
                State::Idle(_) | State::Finished | State::Over => {}
                State::Closed => unreachable!("a Close after Closed"),
            }
            above.push(Event::Closed);
            State::Closed
        }
    };
}

/// What the writer is doing.
#[derive(Debug)]
enum State {
    /// Waiting for the side above, and what the stream below told meanwhile.
    Idle(Below),
    /// Writing an event, with a demand for room outstanding below.
    Writing(Writing),
    /// The body's end went below.
    Finished,
    /// The stream's failure went up.
    Over,
    /// Closed: terminal, and the placeholder of every transition.
    Closed,
}

/// The stream below, as an idle writer knows it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Below {
    Open,
    /// It failed: the next event hears it.
    Failed(Fault),
}

/// An event being written.
#[derive(Debug)]
struct Writing {
    /// The event, framed.
    frame: Box<[u8]>,
    /// How much of it went down.
    sent: u32,
}

/// An event or a comment, framed, or why it is refused.
fn write(
    told: Below,
    frame: Result<Box<[u8]>, Refusal>,
    chunk: u32,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let frame = match frame {
        Ok(frame) => frame,
        Err(refusal) => {
            above.push(Event::Refused(refusal));
            return State::Idle(told);
        }
    };
    match told {
        Below::Open => {}
        Below::Failed(fault) => {
            above.push(Event::Failed(fault));
            return State::Over;
        }
    }
    let writing = Writing { frame, sent: 0 };
    below.push(Down::Demand { read: Read::Nothing, room: next(&writing, chunk) });
    State::Writing(writing)
}

/// Room for the next piece: what is left of the event, at most a chunk.
fn next(writing: &Writing, chunk: u32) -> u32 {
    let len = u32::try_from(writing.frame.len()).expect("an event within Limits::event");
    let left = len.checked_sub(writing.sent).expect("no more sent than the event");
    left.min(chunk)
}

/// Room granted: the next piece goes down; then the room for the one after
/// it, or `Sent`.
fn room(mut writing: Writing, chunk: u32, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    let piece = next(&writing, chunk);
    let len = writing.frame.len();
    if writing.sent == 0 && usize::try_from(piece).expect("a u32 fits a usize") == len {
        // All of it at once: the frame itself goes, uncopied.
        below.push(Down::Send(writing.frame));
        above.push(Event::Sent);
        return State::Idle(Below::Open);
    }
    let from = usize::try_from(writing.sent).expect("a u32 fits a usize");
    let to = from.checked_add(usize::try_from(piece).expect("a u32 fits a usize")).expect("within the event");
    below.push(Down::Send(bytes::copy_of(writing.frame.get(from..to).expect("a piece of the event"))));
    writing.sent = writing.sent.checked_add(piece).expect("within the event");
    if usize::try_from(writing.sent).expect("a u32 fits a usize") == len {
        above.push(Event::Sent);
        return State::Idle(Below::Open);
    }
    below.push(Down::Demand { read: Read::Nothing, room: next(&writing, chunk) });
    State::Writing(writing)
}

const EVENT: &[u8] = b"event: ";
const ID: &[u8] = b"id: ";
const RETRY: &[u8] = b"retry: ";
const DATA: &[u8] = b"data: ";

/// `outgoing` framed: its type, its id, its reconnection time, a `data`
/// field for each line of its data, and a blank line; or why it is
/// refused, in that order: the type, the id, then the length against
/// `most`, [`Limits::event`].
fn event(outgoing: &Outgoing, most: u32) -> Result<Box<[u8]>, Refusal> {
    for &byte in &*outgoing.name {
        if bytes::is_line_end(byte) {
            return Err(Refusal::Name);
        }
    }
    if let Some(id) = &outgoing.id {
        for &byte in &**id {
            if bytes::is_line_end(byte) || byte == 0 {
                return Err(Refusal::Id);
            }
        }
    }
    let mut len = Measure(Some(0));
    if !outgoing.name.is_empty() {
        len.field(EVENT, outgoing.name.len());
    }
    if let Some(id) = &outgoing.id {
        len.field(ID, id.len());
    }
    if let Some(retry) = outgoing.retry {
        len.field(RETRY, Decimal::of(retry).as_bytes().len());
    }
    let mut at = 0;
    // Bounded by the data: each line moves past its ending.
    for _ in 0..=outgoing.data.len() {
        let Some((line, next)) = line(&outgoing.data, at) else { break };
        len.field(DATA, line.len());
        at = next;
    }
    len.add(1);
    let len = match len.0 {
        Some(len) if len <= most => len,
        Some(_) | None => return Err(Refusal::TooLong),
    };
    let mut frame = skein_lib::Writer::new(usize::try_from(len).expect("a u32 fits a usize"));
    if !outgoing.name.is_empty() {
        field(&mut frame, EVENT, &outgoing.name);
    }
    if let Some(id) = &outgoing.id {
        field(&mut frame, ID, id);
    }
    if let Some(retry) = outgoing.retry {
        field(&mut frame, RETRY, Decimal::of(retry).as_bytes());
    }
    let mut at = 0;
    // Bounded by the data: each line moves past its ending.
    for _ in 0..=outgoing.data.len() {
        let Some((line, next)) = line(&outgoing.data, at) else { break };
        field(&mut frame, DATA, line);
        at = next;
    }
    put(&mut frame, b"\n");
    Ok(frame.finish())
}

/// `text` framed as a comment: a colon, a space and the text, or the colon
/// alone, then a blank line; or why it is refused, its bytes before its
/// length against `most`, [`Limits::event`].
fn comment(text: &[u8], most: u32) -> Result<Box<[u8]>, Refusal> {
    for &byte in text {
        if bytes::is_line_end(byte) {
            return Err(Refusal::Comment);
        }
    }
    let mut len = Measure(Some(0));
    len.add(1);
    if !text.is_empty() {
        len.add(1);
        len.add(text.len());
    }
    len.add(2);
    let len = match len.0 {
        Some(len) if len <= most => len,
        Some(_) | None => return Err(Refusal::TooLong),
    };
    let mut frame = skein_lib::Writer::new(usize::try_from(len).expect("a u32 fits a usize"));
    put(&mut frame, b":");
    if !text.is_empty() {
        put(&mut frame, b" ");
        put(&mut frame, text);
    }
    put(&mut frame, b"\n\n");
    Ok(frame.finish())
}

/// The line of `data` that begins at `at`, without its ending, and where
/// the next begins; `None` past the last. A line ends at LF, at CRLF, or at
/// CR alone (WHATWG HTML, 9.2.4): data with `k` endings is `k + 1` lines,
/// the last empty if the data ends with one.
fn line(data: &[u8], at: usize) -> Option<(&[u8], usize)> {
    if at > data.len() {
        return None;
    }
    let rest = data.get(at..)?;
    let Some(end) = bytes::line_end(rest) else { return Some((rest, data.len().checked_add(1)?)) };
    let line = rest.get(..end)?;
    // A CR and the LF after it are one ending.
    let crlf = rest.get(end) == Some(&b'\r') && rest.get(end.checked_add(1)?) == Some(&b'\n');
    let after = if crlf { end.checked_add(2)? } else { end.checked_add(1)? };
    Some((line, at.checked_add(after)?))
}

/// A frame's length, added up with checks: `None` past a `u32`.
struct Measure(Option<u32>);

impl Measure {
    fn add(&mut self, len: usize) {
        let Some(sum) = self.0 else { return };
        self.0 = match u32::try_from(len) {
            Ok(len) => sum.checked_add(len),
            Err(_) => None,
        };
    }

    /// A field: its name, its value, and an LF.
    fn field(&mut self, name: &[u8], value: usize) {
        self.add(name.len());
        self.add(value);
        self.add(1);
    }
}

/// A field, measured to fit: its name, its value, and an LF.
fn field(frame: &mut skein_lib::Writer, name: &[u8], value: &[u8]) {
    put(frame, name);
    put(frame, value);
    put(frame, b"\n");
}

/// Puts what was measured to fit.
fn put(frame: &mut skein_lib::Writer, bytes: &[u8]) {
    frame.put(bytes).expect("the frame was measured to fit");
}
