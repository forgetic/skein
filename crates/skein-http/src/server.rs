//! The HTTP/1.1 server connection (http.md, 5): a step machine that reads
//! one request at a time from the stream below, hands it up, and writes
//! the side above's response; the next request is read only once the
//! response is queued and there is room below for the one after it.
//!
//! # Its two sides
//!
//! Below, a `lib::stream` (lib.md, 7) to the client: TLS's plaintext, a
//! socket or a pipe. Before it reads a request, the server asks for room
//! for the longest response head it writes, [`Limits::response`], and
//! holds it: whatever the request comes to, its answer goes down at once.
//! It reads the request head a line at a time, by scans to LF of at most
//! what is left of [`Limits::head`], and the body by its framing: by
//! length, or by chunks. It writes the response head whole, sized
//! (programming-model.md, 8), and passes the body down as the side above
//! writes it, by length, in chunks, or to the end of the stream.
//!
//! Above, the service's protocol layer ([`Request`] down, [`Event`] up),
//! and the machines it stacks on the two bodies:
//!
//! - **`Next` asks for the next request.** Exactly one event answers it:
//!   `Call`, the request's head, once it is whole and sound; `Ended`, the
//!   client's end between requests; or `Failed`, a request the server
//!   rejected with an answer of its own, or the stream's failure.
//! - **A call is an exchange,** and exactly one terminal event ends it:
//!   `Done`, once the response is all queued below and the request body
//!   was read to its end or discarded, or `Failed`.
//! - **The request body is a stream** (`Body`), read: the server is its
//!   side below, and keeps its contract (lib.md, 7). Its first demand of an
//!   empty body gets `End`. `Discard` gives up the rest of it.
//! - **`Respond` gives the response's head,** once. A response the server
//!   refuses writes nothing and leaves the exchange as it was
//!   (`Refused`). Given before the request body is all read, unless the
//!   side above discards it, it gives up the rest of the body, whose stream
//!   hears `Failed(Fault::Other)`, on a connection not used again.
//! - **The response body is a stream** (`Reply`), written: room demanded,
//!   `Send`s within it, then `Finish`, or a withdrawal, as a machine
//!   stacked on it sends when it closes. A response that has no body (to
//!   `HEAD`, a 204, a 304, or one without) has no such stream.
//! - **`Close` ends the server in any state:** it withdraws what it
//!   demanded below, ends the exchange in progress without a word, and
//!   answers `Closed`, its one terminal event. It does not close the
//!   stream below, which its owner closes.
//!
//! # Bounds
//!
//! A request head is at most [`Limits::head`], with at most
//! [`Limits::headers`] fields; a chunk's size line and the trailer
//! section, at most [`Limits::head`] each; a body by length at most
//! [`Limits::body`]. A chunked body is not bounded: it is a stream, and
//! the side above stops reading it when it has had enough. A response head
//! is at most [`Limits::response`]. Each entry point emits at most
//! [`UP_MAX_OUT`] or [`DOWN_MAX_OUT`]; [`worst_case`] is what a server
//! holds; [`largest_read`] and [`largest_room`] are what whoever stacks it
//! checks against the caps of the stream below at startup.

use core::mem;

use alloc::boxed::Box;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, List, Queue, bytes};

use crate::body::{self, Face, Incoming, Pumped, Rest};
use crate::{Header, MaxOut, Method, Version};

mod head;
mod response;

use head::{Head, Parsed};
use response::Framing;
pub use response::Refusal;

/// The server's limits (programming-model.md, 7): the same for every step
/// and for [`Server::new`], which allocates by them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// The most bytes of a request head, its request line, fields, line
    /// endings and blank line: past it, [`Rejection::HeadTooLong`], or
    /// [`Rejection::TargetTooLong`] for a request line that does not fit.
    /// Also the longest chunk size line, and the longest trailer section.
    /// At least 2, a blank line.
    pub head: u32,
    /// The most fields a request head holds: past it,
    /// [`Rejection::TooManyHeaders`].
    pub headers: u32,
    /// The longest request body announced by length: past it,
    /// [`Rejection::BodyTooLong`]. A chunked body is not bounded here.
    pub body: u64,
    /// The most the side above demands of the request body at once, a
    /// fill's count or a scan's maximum: the cap of the intake that holds
    /// the body's carry-over. At least 1.
    pub read: u32,
    /// The longest response head the server writes, its blank line
    /// included: a longer response is refused with [`Refusal::TooLong`].
    /// Also the room the server sets aside before it reads each request, so
    /// at least the longest of its own answers.
    pub response: u32,
    /// The most room the side above demands at once for the response body.
    /// At least 1: with none, no body could be sent.
    pub send: u32,
}

/// The most bytes the server demands below at once: a line of a head or a
/// chunk's size, or a piece of the request body.
///
/// Whoever stacks the server checks at startup that the stream below's
/// intake holds it (lib.md, 7): a demand past that cap could never be met.
#[must_use]
pub fn largest_read(limits: &Limits) -> u32 {
    limits.head.max(limits.read).max(2)
}

/// The most room the server demands below at once: a response head, or a
/// chunk of what the side above demands for the response body. Whoever
/// stacks the server checks it against the stream below's output cap at
/// startup.
#[must_use]
pub fn largest_room(limits: &Limits) -> u32 {
    match response::chunk_room(limits.send) {
        Some(chunk) => limits.response.max(chunk),
        None => u32::MAX,
    }
}

/// The most memory a server holds under `limits`, in bytes
/// (programming-model.md, 6.3), or `None` if it does not fit a `u64` or
/// the limits cannot be honoured: a head shorter than a blank line, a read
/// of nothing, room for nothing of a response body, room set aside too
/// small for the server's own answers, or a chunk's room past a `u32`.
///
/// It is the intake of the request body's carry-over, allocated with the
/// server; the list of a request's fields (`headers`); the head being
/// read: its target's and its fields' bytes, and the line that holds the
/// next, within [`Limits::head`] each; the response head, held until the
/// request body is all read and room comes for it ([`Limits::response`]);
/// and, once the body is read, the delivery it reads (a line of its
/// framing, within the head already counted, or a piece, at most
/// [`Limits::read`]) or the carry-over an exchange leaves unread, moved out
/// of the intake when it ends. A delivery is made to the server's demand,
/// so it counts it. A response and a piece of the response body are
/// counted by the side above, which made them, and the step that takes one
/// reads it and drops it or passes it on (testing.md, 5); what goes down (a
/// head, a chunk, an answer of the server's own) and what goes up (a call,
/// the body's bytes) is handed out when it is emitted.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.head < 2 || limits.read == 0 || limits.send == 0 || limits.response < response::longest_answer() {
        return None;
    }
    response::chunk_room(limits.send)?;
    let intake = Intake::worst_case(limits.read)?;
    let fields = List::<Header>::worst_case(limits.headers)?;
    let head = u64::from(limits.head).checked_mul(2)?;
    intake
        .checked_add(fields)?
        .checked_add(head)?
        .checked_add(u64::from(limits.response))?
        .checked_add(u64::from(limits.read))
}

/// [`up`]'s: above, the stream's failure told to both bodies' streams and
/// the exchange; below, the response head and the reply's room, or a 100
/// (Continue) and the read it goes before.
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 3, below: 2 };

/// [`down`]'s: above, the request body's stream told it is given up, or
/// its end, and `Done`; below, a read withdrawn and the response head, or
/// a 100 (Continue) and the read it goes before.
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 2, below: 2 };

/// From the side above.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// Asks for the next request, while no exchange is in progress. `Call`,
    /// `Ended` or `Failed` answers it.
    Next,
    /// The response's head, for the call in progress, once: written and
    /// sent below once the request body is all read below, or given up. A
    /// refused one writes nothing, and `Refused` answers it. `Done` follows
    /// once the response is all queued, and only once the side above read
    /// the body's `End`, discarded it, or withdrew from a body all read
    /// below: a side above that never reads the body discards it.
    Respond(Response),
    /// The request body's stream, read (lib.md, 7): a demand of at most
    /// [`Limits::read`] and no room, or its withdrawal, after which the
    /// side above reads no more: it discards the rest, responds, or closes
    /// the server. A withdrawal may cross its demand's answer, even the
    /// body's end.
    Body(Down),
    /// The side above reads no more of the request body: the server reads
    /// the rest and drops it, so that the connection can be used again,
    /// or, on a connection not to be, reads no more. A demand outstanding
    /// is dropped unanswered.
    Discard,
    /// The response body's stream, written (lib.md, 7), once the response
    /// was given, if it has a body: a demand for room only, of at most
    /// [`Limits::send`]; a `Send` within the room granted; `Finish` once
    /// the response's length is sent, or, for one in chunks, when it is
    /// over. A withdrawal, as a machine stacked on the reply sends when it
    /// closes, means the side above writes no more: it closes the server
    /// next.
    Reply(Down),
    /// Closes the server, in any state. `Closed` answers it.
    Close,
}

/// To the side above.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// For a `Next`: a request's head. Its body follows on `Body`; `Done`
    /// or `Failed` ends the exchange.
    Call(Call),
    /// For a `Next`: the client ended the connection before another
    /// request, or with no line of one. The server waits for its close.
    Ended,
    /// The request body's stream: `Bytes`, `End`, or `Failed` when the
    /// exchange fails while it is read, or when a response given first
    /// gives it up (`Fault::Other`).
    Body(Up),
    /// The response body's stream: `Room`, or `Failed` once it can go no
    /// further.
    Reply(Up),
    /// For a `Respond`: refused, writing nothing. The exchange is as it
    /// was: the side above may respond again.
    Refused(Refusal),
    /// For a `Call`: the exchange is over, and whether the connection may
    /// carry another. If not, the server waits for its close. It waits for
    /// the response all queued below and for the request body's `End`, read
    /// by the side above, or its `Discard`, or a withdrawal once the body is
    /// all read below, or the body given up by a response that came first.
    Done(Reuse),
    /// For a `Call`, or for a `Next` that no `Call` answered: the exchange
    /// failed, or the request was rejected with an answer of the server's
    /// own, or the stream failed. The server waits for its close.
    Failed(Error),
    /// For a `Close`: the server is closed. Terminal.
    Closed,
}

/// Whether a connection carries another exchange once one is done.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Reuse {
    /// Both sides allow it: the server waits for the next `Next`.
    Keep,
    /// One of them does not, the request body was given up, or the stream
    /// ended: the server waits for its close.
    Close,
}

/// A request's head, as the client sent it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Call {
    pub method: Method,
    /// The request target, as it came: visible ASCII, at least one byte, in
    /// whatever form the client wrote it (RFC 9112, 3.2).
    pub target: Box<[u8]>,
    pub version: Version,
    /// The fields, in the order the head gave them.
    pub headers: Box<[Header]>,
    /// How the request body is framed.
    pub body: Body,
}

impl Call {
    /// The value of the first field named `name`, without regard to case:
    /// what a field that occurs once is read by.
    #[must_use]
    pub fn header(&self, name: &[u8]) -> Option<&[u8]> {
        for header in &self.headers {
            if header.is(name) {
                return Some(&header.value);
            }
        }
        None
    }
}

/// How a message body is framed: a request's, as its head says, or a
/// response's, as the side above asks.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Body {
    /// None.
    None,
    /// `Content-Length`: exactly this many bytes.
    Length(u64),
    /// `Transfer-Encoding: chunked`: for a response, a body of a length not
    /// known before it is written, which an HTTP/1.0 client gets to the end
    /// of the stream instead.
    Chunked,
}

/// The response the side above gives a call.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Response {
    /// The status code, from 200 to 599.
    pub status: u16,
    /// The fields the response carries, written in this order: none the
    /// server writes itself, `Content-Length`, `Transfer-Encoding` and
    /// `Connection`.
    pub headers: Box<[Header]>,
    /// How its body is framed: written as said, even to `HEAD`, but sent
    /// only for a request that is not `HEAD`, and never for a 204 or a 304,
    /// which take none.
    pub body: Body,
    /// Whether the connection ends with this exchange: the server says
    /// `Connection: close`, and does not use it again.
    pub close: bool,
}

/// What was wrong with a request's head: the server rejects it at the
/// entrance with a small, fixed answer of its own (programming-model.md,
/// 8), and the connection ends.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Rejection {
    /// The request line is not a method, a target and `HTTP/x.y`, each
    /// after one space: 400.
    RequestLine,
    /// The request line does not fit in [`Limits::head`]: 414.
    TargetTooLong,
    /// Another major version than HTTP/1: 505.
    Version,
    /// A method the server does not know: 501.
    Method,
    /// A field line that is not a field: no colon, a name that is not a
    /// token or whitespace before the colon, a control character in the
    /// value; or an obsolete fold: 400.
    Header,
    /// The head is longer than [`Limits::head`]: 431.
    HeadTooLong,
    /// The head holds more than [`Limits::headers`] fields: 431.
    TooManyHeaders,
    /// No `Host` in an HTTP/1.1 request, or more than one in any (RFC 9112,
    /// 3.2): 400.
    Host,
    /// The body's framing cannot be read: `Transfer-Encoding` in HTTP/1.0
    /// or beside a `Content-Length`, codings that do not end with `chunked`
    /// once, or a `Content-Length` that is not one length: 400.
    Framing,
    /// A transfer coding other than `chunked`, which the server does not
    /// undo: 501.
    Coding,
    /// A body by length past [`Limits::body`]: 413.
    BodyTooLong,
}

impl Rejection {
    /// The status of the answer the server writes for it.
    #[must_use]
    pub fn status(self) -> u16 {
        match self {
            Rejection::RequestLine | Rejection::Header | Rejection::Host | Rejection::Framing => 400,
            Rejection::BodyTooLong => 413,
            Rejection::TargetTooLong => 414,
            Rejection::HeadTooLong | Rejection::TooManyHeaders => 431,
            Rejection::Method | Rejection::Coding => 501,
            Rejection::Version => 505,
        }
    }
}

/// Why an exchange failed, or a `Next` was answered by no call.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// The request was rejected with the server's own answer, which went
    /// below.
    Rejected(Rejection),
    /// The stream ended partway through a request: its head, once its
    /// request line was read, or its body.
    Truncated,
    /// The stream below failed.
    Stream(Fault),
    /// A chunk's size line is not one: not hexadecimal, past a `u64`, or
    /// longer than [`Limits::head`].
    ChunkSize,
    /// A chunk's data is not followed by a line ending.
    Chunk,
    /// The trailer section is longer than [`Limits::head`].
    Trailer,
}

/// What the server is waiting for. Machines keep no timers
/// (programming-model.md, 4): each says what it waits for, and the
/// connection arms the deadlines.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// For the side above to ask for the next request.
    Next,
    /// For room below: the client is not reading what was sent.
    Room,
    /// For a request's head, or its next line, from the client: the
    /// connection is idle on its side, or the head comes slowly.
    Request,
    /// For the request body, or its framing, from the client.
    Body,
    /// For the side above: to respond, to demand the request body or
    /// discard it, or to demand room for its reply, send or finish it.
    Above,
    /// For the side above to close it: the connection is not to be used
    /// again.
    Close,
    /// For nothing: it is closed.
    Nothing,
}

/// A server connection: its state for this machine.
#[derive(Debug)]
pub struct Server {
    state: State,
    /// The request body's carry-over (lib.md, 7), under [`Limits::read`],
    /// empty between exchanges.
    intake: Intake,
}

impl Server {
    /// A server under `limits`, the limits its steps will be given. It
    /// demands nothing until the side above asks for a request.
    #[must_use]
    pub fn new(limits: &Limits) -> Server {
        assert!(
            worst_case(limits).is_some(),
            "the limits are honoured: a head of a blank line, a read of a byte, room for the server's own answers"
        );
        Server { state: State::Idle(Line::Open), intake: Intake::with_capacity(limits.read) }
    }

    /// What it is waiting for: a function of its state alone.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match &self.state {
            State::Idle(_) => Waiting::Next,
            State::Reading(reading) => {
                if reading.reserved {
                    Waiting::Request
                } else {
                    Waiting::Room
                }
            }
            State::Exchange(exchange) => match exchange.below {
                Some(Demand::Room(_)) => Waiting::Room,
                Some(Demand::Read(_)) => Waiting::Body,
                None => Waiting::Above,
            },
            State::Spent => Waiting::Close,
            State::Closed => Waiting::Nothing,
        }
    }
}

/// An event from the stream below. Emits at most [`UP_MAX_OUT`].
pub fn up(server: &mut Server, env: &Env<Limits>, ev: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let state = mem::replace(&mut server.state, State::Closed);
    server.state = match state {
        State::Idle(line) => State::Idle(idle(line, ev)),
        State::Reading(reading) => reading_up(reading, limits, ev, above, below),
        State::Exchange(exchange) => exchange_up(exchange, &mut server.intake, limits, ev, above, below),
        // An answer to a demand withdrawn, on its way (lib.md, 7); an end or
        // a failure, which changes nothing now.
        State::Spent => State::Spent,
        State::Closed => State::Closed,
    };
}

/// A request from the side above. Emits at most [`DOWN_MAX_OUT`].
pub fn down(server: &mut Server, env: &Env<Limits>, rq: Request, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let intake = &mut server.intake;
    let state = mem::replace(&mut server.state, State::Closed);
    server.state = match rq {
        Request::Close => close(state, intake, above, below),
        Request::Next => match state {
            State::Idle(line) => next(line, limits, above, below),
            State::Reading(_) | State::Exchange(_) => unreachable!("a Next while a request is in progress"),
            State::Spent => unreachable!("a Next on a connection not to be used again"),
            State::Closed => unreachable!("a Next after Closed"),
        },
        Request::Respond(response) => match state {
            State::Exchange(exchange) => respond(exchange, response, intake, limits, above, below),
            // After the exchange failed, a request on its way is dropped.
            State::Spent => State::Spent,
            State::Idle(_) | State::Reading(_) => unreachable!("a Respond with no call in progress"),
            State::Closed => unreachable!("a Respond after Closed"),
        },
        Request::Body(down) => match state {
            State::Exchange(exchange) => body_demanded(exchange, down, intake, limits, above, below),
            State::Spent => State::Spent,
            // A withdrawal on its way when the body's end went up with
            // `Done` (lib.md, 7): dropped.
            State::Idle(line) => match down {
                Down::Demand { read: Read::Nothing, room: 0 } => State::Idle(line),
                Down::Demand { .. } | Down::Send(_) | Down::Finish => {
                    unreachable!("a body demand with no call in progress")
                }
            },
            State::Reading(_) => unreachable!("a body demand with no call in progress"),
            State::Closed => unreachable!("a body demand after Closed"),
        },
        Request::Discard => match state {
            State::Exchange(exchange) => discard(exchange, intake, limits, above, below),
            // On its way when the body's end went up with `Done`, or the
            // exchange failed.
            State::Idle(_) | State::Spent => state,
            State::Reading(_) => unreachable!("a Discard with no call in progress"),
            State::Closed => unreachable!("a Discard after Closed"),
        },
        Request::Reply(down) => match state {
            State::Exchange(exchange) => reply(exchange, down, intake, limits, above, below),
            State::Spent => State::Spent,
            State::Idle(_) | State::Reading(_) => unreachable!("a reply with no call in progress"),
            State::Closed => unreachable!("a reply after Closed"),
        },
    };
}

/// What the server is doing.
#[derive(Debug)]
enum State {
    /// No request in progress: waiting for the side above's `Next`, on a
    /// connection that is open, or that ended or failed meanwhile.
    Idle(Line),
    /// The side above asked for the next request: room for its response,
    /// then its head.
    Reading(Reading),
    Exchange(Exchange),
    /// The connection is not to be used again: a request was rejected or
    /// failed, an exchange failed or was done with a connection that does
    /// not persist, or the client ended it. Waiting for the close.
    Spent,
    /// Closed: terminal, and the placeholder of every transition.
    Closed,
}

/// The stream below, as an idle server knows it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Line {
    Open,
    /// It ended: no request comes.
    Ended,
    Failed(Fault),
}

/// A request being read: first the room for its response, then its head,
/// a line at a time. Exactly one demand is outstanding below throughout.
#[derive(Debug)]
struct Reading {
    /// Whether the room for the response was granted: until it is, the
    /// demand is for it, and nothing is read.
    reserved: bool,
    head: Head,
}

/// An exchange in progress: the request body coming up, the response going
/// down, and the one demand outstanding below. The server reads the body
/// below only until the response head goes down, so a demand is either a
/// read or room, never both.
#[derive(Debug)]
struct Exchange {
    /// The request's: a response to `HEAD` sends no body.
    method: Method,
    version: Version,
    /// Whether the connection may carry another exchange, as far as the
    /// request, its body and the stream allow.
    keep: bool,
    /// The room set aside before the request was read.
    aside: Aside,
    /// The request body, while the side above's stream of it is open: from
    /// the call until its end went up, it was discarded to its end, or it
    /// was given up.
    body: Option<Incoming>,
    response: Responding,
    /// The demand outstanding below, if any: stated only when none is, and
    /// answered at most once (lib.md, 7).
    below: Option<Demand>,
    /// A read the server withdrew below, whose answer may still be on its
    /// way: once the body is given up, any bytes that come are its.
    withdrawn: bool,
}

/// The room set aside before a request was read, for what goes down first
/// for it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Aside {
    /// Held, for the response head.
    Held,
    /// Held, and the client waits for a 100 (Continue) before it sends the
    /// body: one goes in it before the body's first read below, unless a
    /// final response goes first.
    Continue,
    /// Spent: on the head, or on a 100 (Continue), after which the head
    /// asks for room of its own.
    Spent,
}

/// A demand the server stated below: a read for the request, or room for
/// the response.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Demand {
    Read(Read),
    Room(u32),
}

/// The response, going down.
#[derive(Debug)]
enum Responding {
    /// The side above has not responded.
    Awaited,
    /// The head, written, until the request body is all read below (or
    /// given up) and room comes to send it whole.
    Head { head: Box<[u8]>, reply: Reply, persist: bool },
    /// The head went down: the body, as the side above writes it.
    Sending { reply: Reply, persist: bool },
}

/// The response body, as the side above writes it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Reply {
    /// By length: this many bytes are still to come from the side above.
    Length { left: u64, room: Room },
    /// In chunks: each `Send` a chunk.
    Chunked { room: Room },
    /// To the end of the stream: each `Send` as it is.
    UntilEnd { room: Room },
    /// The side above finished a chunked body: the last chunk, waiting for
    /// room.
    Last,
    /// No body, or all of it went down.
    Finished,
    /// The side above withdrew its demand, as a machine stacked on the
    /// reply does when it closes: it writes no more, and closes the server
    /// next.
    Withdrawn,
}

/// The room of the response body's stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Room {
    /// Nothing outstanding: the side above has not demanded room, or has
    /// sent within what it was granted.
    Idle,
    /// The side above demands this much, which the server demands below,
    /// as a chunk's room for a chunked body.
    Wanted(u32),
    /// This much was granted: the side above may send it.
    Granted(u32),
}

/// An event below while idle: only the stream's end or failure, as nothing
/// is demanded.
fn idle(line: Line, ev: Up) -> Line {
    match ev {
        Up::End => match line {
            Line::Open => Line::Ended,
            Line::Ended | Line::Failed(_) => line,
        },
        // After the end, a failure says only that the stream cannot send.
        Up::Failed(fault) => match line {
            Line::Open => Line::Failed(fault),
            Line::Ended | Line::Failed(_) => line,
        },
        Up::Bytes(_) | Up::Room => unreachable!("an answer while the server demands nothing"),
    }
}

/// `Next`, while idle: room for the response first, on a connection still
/// open.
fn next(line: Line, limits: &Limits, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    match line {
        Line::Open => {
            below.push(Down::Demand { read: Read::Nothing, room: limits.response });
            State::Reading(Reading { reserved: false, head: Head::new(limits) })
        }
        Line::Ended => {
            above.push(Event::Ended);
            State::Spent
        }
        Line::Failed(fault) => {
            above.push(Event::Failed(Error::Stream(fault)));
            State::Spent
        }
    }
}

/// An event below while a request is read.
fn reading_up(
    mut reading: Reading,
    limits: &Limits,
    ev: Up,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    match ev {
        Up::Room => {
            assert!(!reading.reserved, "room answers the demand for it");
            reading.reserved = true;
            below.push(Down::Demand { read: line(&reading.head), room: 0 });
            State::Reading(reading)
        }
        Up::Bytes(bytes) => {
            assert!(reading.reserved, "bytes answer a line's demand, stated once the room was granted");
            match reading.head.line(&bytes) {
                Ok(Parsed::More) => {
                    below.push(Down::Demand { read: line(&reading.head), room: 0 });
                    State::Reading(reading)
                }
                Ok(Parsed::Complete) => match reading.head.request(limits) {
                    Ok(request) => called(request, above),
                    Err(rejection) => reject(rejection, above, below),
                },
                Err(rejection) => reject(rejection, above, below),
            }
        }
        Up::End => {
            // Room that may still come after the end: the server wants none.
            // A line's read crosses the end, and is never met.
            if !reading.reserved {
                below.push(Down::Demand { read: Read::Nothing, room: 0 });
            }
            if reading.head.begun() {
                above.push(Event::Failed(Error::Truncated));
            } else {
                above.push(Event::Ended);
            }
            State::Spent
        }
        Up::Failed(fault) => {
            above.push(Event::Failed(Error::Stream(fault)));
            State::Spent
        }
    }
}

/// The read of a head's next line: a scan to LF of at most what is left of
/// the head.
fn line(head: &Head) -> Read {
    let budget = head.budget();
    assert!(budget > 0, "a head with nothing left of its budget was rejected");
    Read::Scan { until: Delimiter::LF, max: budget }
}

/// A request rejected at the entrance: its answer goes down within the room
/// set aside for it, and the connection ends.
fn reject(rejection: Rejection, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    below.push(Down::Send(bytes::copy_of(response::answer(rejection))));
    above.push(Event::Failed(Error::Rejected(rejection)));
    State::Spent
}

/// A request's head, whole and sound: up it goes, and the exchange begins,
/// demanding nothing until the side above reads, discards or responds.
fn called(request: head::Request, above: &mut Queue<Event>) -> State {
    let rest = match request.call.body {
        Body::None | Body::Length(0) => Rest::Over,
        Body::Length(length) => Rest::Length(length),
        Body::Chunked => Rest::ChunkSize,
    };
    let exchange = Exchange {
        method: request.call.method,
        version: request.call.version,
        keep: request.persist,
        aside: if request.expects { Aside::Continue } else { Aside::Held },
        body: Some(Incoming { rest, face: Face::Idle }),
        response: Responding::Awaited,
        below: None,
        withdrawn: false,
    };
    above.push(Event::Call(request.call));
    State::Exchange(exchange)
}

/// An event below during an exchange.
fn exchange_up(
    mut exchange: Exchange,
    intake: &mut Intake,
    limits: &Limits,
    ev: Up,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    match ev {
        Up::Bytes(bytes) => {
            match exchange.below {
                Some(Demand::Read(_)) => exchange.below = None,
                Some(Demand::Room(_)) | None => {
                    // The answer to a read the server withdrew as it gave the
                    // body up, on its way (lib.md, 7).
                    assert!(exchange.withdrawn, "bytes answer a demand that reads");
                    return State::Exchange(exchange);
                }
            }
            let incoming = exchange.body.as_mut().expect("the body is read while the side above's stream is open");
            match body::delivered(incoming, intake, limits.head, bytes) {
                Ok(up) => {
                    if let Some(bytes) = up {
                        above.push(Event::Body(Up::Bytes(bytes)));
                    }
                    settle(exchange, intake, limits, above, below)
                }
                Err(bad) => fail(exchange, framing_error(bad), intake, above, below),
            }
        }
        Up::Room => {
            match exchange.below.take() {
                Some(Demand::Room(_)) => {}
                // The answer to room the server withdrew with the reply, on
                // its way (lib.md, 7).
                None if exchange.withdrawn => return State::Exchange(exchange),
                Some(Demand::Read(_)) | None => unreachable!("room answers a demand for room"),
            }
            exchange.response = match exchange.response {
                Responding::Head { head, reply, persist } => {
                    below.push(Down::Send(head));
                    Responding::Sending { reply, persist }
                }
                Responding::Sending { reply, persist } => {
                    let reply = match reply {
                        Reply::Length { left, room: Room::Wanted(room) } => {
                            above.push(Event::Reply(Up::Room));
                            Reply::Length { left, room: Room::Granted(room) }
                        }
                        Reply::Chunked { room: Room::Wanted(room) } => {
                            above.push(Event::Reply(Up::Room));
                            Reply::Chunked { room: Room::Granted(room) }
                        }
                        Reply::UntilEnd { room: Room::Wanted(room) } => {
                            above.push(Event::Reply(Up::Room));
                            Reply::UntilEnd { room: Room::Granted(room) }
                        }
                        Reply::Last => {
                            below.push(Down::Send(bytes::copy_of(response::LAST_CHUNK)));
                            Reply::Finished
                        }
                        Reply::Length { room: Room::Idle | Room::Granted(_), .. }
                        | Reply::Chunked { room: Room::Idle | Room::Granted(_) }
                        | Reply::UntilEnd { room: Room::Idle | Room::Granted(_) }
                        | Reply::Finished
                        | Reply::Withdrawn => unreachable!("room is demanded for the reply when it wants some"),
                    };
                    Responding::Sending { reply, persist }
                }
                Responding::Awaited => unreachable!("room is demanded for a response given"),
            };
            settle(exchange, intake, limits, above, below)
        }
        Up::End => {
            // A read outstanding crosses the end and is never met; room may
            // still come after it.
            match exchange.below {
                Some(Demand::Read(_)) => exchange.below = None,
                Some(Demand::Room(_)) | None => {}
            }
            let owed = match &exchange.body {
                Some(incoming) => !incoming.rest.is_over(),
                None => false,
            };
            if owed {
                return fail(exchange, Error::Truncated, intake, above, below);
            }
            // The request is all read below: the response still goes down,
            // and no other request follows.
            exchange.keep = false;
            settle(exchange, intake, limits, above, below)
        }
        Up::Failed(fault) => {
            // Nothing follows a failure: what was outstanding is over.
            exchange.below = None;
            fail(exchange, Error::Stream(fault), intake, above, below)
        }
    }
}

/// `Respond`: refused, or the head written, the request body given up if
/// it is not all read below and the side above is not discarding it to
/// keep the connection.
fn respond(
    mut exchange: Exchange,
    response: Response,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    match exchange.response {
        Responding::Awaited => {}
        Responding::Head { .. } | Responding::Sending { .. } => unreachable!("one response per call"),
    }
    let framing = response::framing(&response, exchange.method, exchange.version);
    let discarding = match &exchange.body {
        Some(incoming) => incoming.face == Face::Discarding && !incoming.rest.is_over(),
        None => false,
    };
    // The connection is kept only if what is left of the body is read to its
    // end before the head goes: the side above discards it.
    let persist =
        exchange.keep && !response.close && framing != Framing::UntilEnd && (discarding || body_over_below(&exchange));
    let head = match response::write(&response, exchange.version, persist, limits) {
        Ok(head) => head,
        Err(refusal) => {
            above.push(Event::Refused(refusal));
            return State::Exchange(exchange);
        }
    };
    // A final response answers instead of a 100 (Continue) not yet sent.
    if exchange.aside == Aside::Continue {
        exchange.aside = Aside::Held;
    }
    if !persist {
        give_up(&mut exchange, intake, above, below);
    }
    let reply = match framing {
        Framing::Nothing => Reply::Finished,
        Framing::Length(left) => Reply::Length { left, room: Room::Idle },
        Framing::Chunked => Reply::Chunked { room: Room::Idle },
        Framing::UntilEnd => Reply::UntilEnd { room: Room::Idle },
    };
    exchange.response = Responding::Head { head, reply, persist };
    settle(exchange, intake, limits, above, below)
}

/// Whether the request body is all read below: none, read to its end, or
/// given up.
fn body_over_below(exchange: &Exchange) -> bool {
    match &exchange.body {
        Some(incoming) => incoming.rest.is_over(),
        None => true,
    }
}

/// The rest of the request body is given up, on a connection not used
/// again: nothing more of it is read, a read outstanding for it is
/// withdrawn, and its stream, if the side above still reads it, hears
/// `Failed(Fault::Other)`, the fault for a side below that stops for a
/// reason of its own.
fn give_up(exchange: &mut Exchange, intake: &mut Intake, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    exchange.keep = false;
    if body_over_below(exchange) {
        return;
    }
    if let Some(incoming) = exchange.body.take() {
        match incoming.face {
            Face::Idle | Face::Demand(_) => above.push(Event::Body(Up::Failed(Fault::Other))),
            Face::Withdrawn | Face::Discarding => {}
        }
    }
    body::drain(intake);
    withdraw_read(exchange, below);
}

/// Withdraws a read outstanding for a body given up: its answer, on its
/// way, is dropped when it comes.
fn withdraw_read(exchange: &mut Exchange, below: &mut Queue<Down>) {
    match exchange.below {
        Some(Demand::Read(_)) => {
            below.push(Down::Demand { read: Read::Nothing, room: 0 });
            exchange.below = None;
            exchange.withdrawn = true;
        }
        Some(Demand::Room(_)) | None => {}
    }
}

/// The request body's stream, from the side above.
fn body_demanded(
    mut exchange: Exchange,
    down: Down,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let read = match down {
        Down::Demand { read, room } => {
            assert!(room == 0, "the request body's stream is read, not written");
            read
        }
        Down::Send(_) | Down::Finish => unreachable!("the request body's stream is read, not written"),
    };
    let Some(incoming) = &mut exchange.body else {
        // A withdrawal on its way when the body's end went up, or when a
        // response gave it up: dropped.
        assert!(read == Read::Nothing, "no body demand once the body is over");
        return State::Exchange(exchange);
    };
    incoming.face = match incoming.face {
        // A withdrawal: what is read for it from now on is dropped. One with
        // nothing outstanding was on its way when its demand's answer went
        // up (lib.md, 7): the side above reads no more all the same.
        Face::Idle | Face::Demand(_) if read == Read::Nothing => Face::Withdrawn,
        Face::Idle => {
            let wanted = match read {
                Read::Fill(n) => n,
                Read::Scan { max, .. } | Read::Line { max } => max,
                Read::Nothing => unreachable!("a withdrawal is matched above"),
            };
            assert!(wanted <= limits.read, "no demand past Limits::read");
            Face::Demand(read)
        }
        Face::Demand(_) => unreachable!("one demand at a time, stated after the last was answered"),
        Face::Withdrawn => unreachable!("a body demand after its withdrawal: the side above reads no more"),
        Face::Discarding => unreachable!("a body demand after Discard"),
    };
    settle(exchange, intake, limits, above, below)
}

/// The side above gives up the rest of the request body: read and dropped
/// on a connection that may be used again, given up on one that may not.
fn discard(
    mut exchange: Exchange,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let Some(incoming) = &mut exchange.body else {
        // On its way when the body's end went up, or when a response gave it
        // up.
        return State::Exchange(exchange);
    };
    match incoming.face {
        Face::Idle | Face::Demand(_) | Face::Withdrawn => {}
        Face::Discarding => unreachable!("a second Discard"),
    }
    incoming.face = Face::Discarding;
    // No other exchange follows, so no end to read the body to.
    if !exchange.keep {
        exchange.body = None;
        body::drain(intake);
        withdraw_read(&mut exchange, below);
    }
    settle(exchange, intake, limits, above, below)
}

/// The response body's stream, from the side above.
fn reply(
    mut exchange: Exchange,
    down: Down,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    if down == (Down::Demand { read: Read::Nothing, room: 0 }) {
        return reply_withdrawn(exchange, below);
    }
    let reply = match &mut exchange.response {
        Responding::Head { reply, .. } | Responding::Sending { reply, .. } => reply,
        Responding::Awaited => unreachable!("a reply before the response"),
    };
    *reply = match down {
        Down::Demand { read, room } => {
            assert!(read == Read::Nothing, "the response body's stream is written, not read");
            assert!(room <= limits.send, "no room past Limits::send");
            match *reply {
                Reply::Length { left, room: Room::Idle } => {
                    assert!(left > 0, "no room for a body all sent: Finish");
                    Reply::Length { left, room: Room::Wanted(room) }
                }
                Reply::Chunked { room: Room::Idle } => Reply::Chunked { room: Room::Wanted(room) },
                Reply::UntilEnd { room: Room::Idle } => Reply::UntilEnd { room: Room::Wanted(room) },
                Reply::Length { .. } | Reply::Chunked { .. } | Reply::UntilEnd { .. } => {
                    unreachable!("one demand at a time, stated after the last was answered")
                }
                Reply::Last | Reply::Finished => {
                    unreachable!("a reply demand for a body that has none, or after Finish")
                }
                Reply::Withdrawn => unreachable!("a reply demand after its withdrawal: the side above writes no more"),
            }
        }
        Down::Send(bytes) => {
            let len = u64::try_from(bytes.len()).expect("a length fits a u64");
            match *reply {
                Reply::Length { left, room: Room::Granted(granted) } => {
                    assert!(len <= u64::from(granted), "a Send within the room granted");
                    let left = left.checked_sub(len).expect("no more than the response's length");
                    below.push(Down::Send(bytes));
                    Reply::Length { left, room: Room::Idle }
                }
                Reply::Chunked { room: Room::Granted(granted) } => {
                    assert!(len <= u64::from(granted), "a Send within the room granted");
                    // An empty chunk would be the last: an empty piece is no
                    // chunk at all.
                    if !bytes.is_empty() {
                        below.push(Down::Send(response::chunk(&bytes)));
                    }
                    Reply::Chunked { room: Room::Idle }
                }
                Reply::UntilEnd { room: Room::Granted(granted) } => {
                    assert!(len <= u64::from(granted), "a Send within the room granted");
                    below.push(Down::Send(bytes));
                    Reply::UntilEnd { room: Room::Idle }
                }
                Reply::Length { .. } | Reply::Chunked { .. } | Reply::UntilEnd { .. } => {
                    unreachable!("a Send within the room granted")
                }
                Reply::Last | Reply::Finished => unreachable!("a Send for a body that has none, or after Finish"),
                Reply::Withdrawn => unreachable!("a Send after the reply's withdrawal"),
            }
        }
        Down::Finish => match *reply {
            Reply::Length { left, room } => {
                assert!(left == 0 && room == Room::Idle, "Finish once the response's length is sent");
                Reply::Finished
            }
            Reply::Chunked { room } => {
                assert!(room == Room::Idle, "Finish with no room outstanding");
                Reply::Last
            }
            Reply::UntilEnd { room } => {
                assert!(room == Room::Idle, "Finish with no room outstanding");
                Reply::Finished
            }
            Reply::Last | Reply::Finished => unreachable!("a Finish for a body that has none, or after Finish"),
            Reply::Withdrawn => unreachable!("a Finish after the reply's withdrawal"),
        },
    };
    settle(exchange, intake, limits, above, below)
}

/// The side above withdrew the reply's demand: it writes no more, so the
/// response can never end, and it closes the server next. Whatever the
/// server demanded below is withdrawn with it, and nothing more is
/// demanded; an answer on its way is dropped when it comes. A withdrawal
/// may cross its demand's answer, as room granted.
fn reply_withdrawn(mut exchange: Exchange, below: &mut Queue<Down>) -> State {
    let reply = match &mut exchange.response {
        Responding::Head { reply, .. } | Responding::Sending { reply, .. } => reply,
        Responding::Awaited => unreachable!("a reply's withdrawal before the response"),
    };
    *reply = match *reply {
        Reply::Length { .. } | Reply::Chunked { .. } | Reply::UntilEnd { .. } => Reply::Withdrawn,
        Reply::Last | Reply::Finished | Reply::Withdrawn => {
            unreachable!("a reply's withdrawal with no demand of the side above's to withdraw")
        }
    };
    if exchange.below.take().is_some() {
        below.push(Down::Demand { read: Read::Nothing, room: 0 });
        exchange.withdrawn = true;
    }
    State::Exchange(exchange)
}

/// `Close`, in any state.
fn close(state: State, intake: &mut Intake, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    match state {
        // A request being read always has one demand outstanding.
        State::Reading(_) => below.push(Down::Demand { read: Read::Nothing, room: 0 }),
        State::Exchange(exchange) => {
            if exchange.below.is_some() {
                below.push(Down::Demand { read: Read::Nothing, room: 0 });
            }
            body::drain(intake);
        }
        State::Idle(_) | State::Spent => {}
        State::Closed => unreachable!("a Close after Closed"),
    }
    above.push(Event::Closed);
    State::Closed
}

/// Where an exchange goes after every transition, in one place
/// (programming-model.md, 5.4): the side above's body demand met from the
/// intake, or the body's end; the response head sent, once the body is
/// all read below and room is held for it; the exchange done; or the next
/// demand below, if none is outstanding and the state wants one, a 100
/// (Continue) before the body's first read if the client waits for it.
fn settle(
    mut exchange: Exchange,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    if let Some(incoming) = &mut exchange.body {
        let (up, pumped) = body::pump(incoming, intake);
        if let Some(up) = up {
            above.push(Event::Body(up));
        }
        // A body the side above withdrew, all read below, is over as well:
        // it reads no more, and nothing is left for a discard to read.
        let over = match pumped {
            Pumped::Over => true,
            Pumped::Open => incoming.face == Face::Withdrawn && incoming.rest.is_over(),
        };
        if over {
            exchange.body = None;
            body::drain(intake);
        }
    }
    if exchange.aside == Aside::Held && body_over_below(&exchange) {
        exchange.response = match exchange.response {
            Responding::Head { head, reply, persist } if reply != Reply::Withdrawn => {
                exchange.aside = Aside::Spent;
                below.push(Down::Send(head));
                Responding::Sending { reply, persist }
            }
            responding @ (Responding::Awaited | Responding::Head { .. } | Responding::Sending { .. }) => responding,
        };
    }
    if exchange.body.is_none() {
        match exchange.response {
            Responding::Sending { reply: Reply::Finished, persist } => return done(exchange, persist, below, above),
            Responding::Sending {
                reply:
                    Reply::Length { .. } | Reply::Chunked { .. } | Reply::UntilEnd { .. } | Reply::Last | Reply::Withdrawn,
                ..
            }
            | Responding::Head { .. }
            | Responding::Awaited => {}
        }
    }
    if exchange.below.is_some() {
        return State::Exchange(exchange);
    }
    let Some(demand) = wanted(&exchange, intake, limits) else { return State::Exchange(exchange) };
    let down = match demand {
        Demand::Read(read) => {
            if exchange.aside == Aside::Continue {
                below.push(Down::Send(bytes::copy_of(response::CONTINUE)));
                exchange.aside = Aside::Spent;
            }
            Down::Demand { read, room: 0 }
        }
        Demand::Room(room) => Down::Demand { read: Read::Nothing, room },
    };
    below.push(down);
    exchange.below = Some(demand);
    State::Exchange(exchange)
}

/// What the exchange demands below, when nothing is outstanding: before
/// the head goes, what the request body needs for the side above's demand
/// or a discard; then room for the head, if what was set aside went to a
/// 100 (Continue); then room for what the side above demands of the reply,
/// as a chunk's room for a chunked body, and for the last chunk.
fn wanted(exchange: &Exchange, intake: &Intake, limits: &Limits) -> Option<Demand> {
    match &exchange.response {
        // The side above writes no more, and closes the server next.
        Responding::Head { reply: Reply::Withdrawn, .. } | Responding::Sending { reply: Reply::Withdrawn, .. } => None,
        Responding::Awaited => body_read(exchange, intake, limits),
        Responding::Head { head, .. } => {
            if !body_over_below(exchange) {
                return body_read(exchange, intake, limits);
            }
            assert!(exchange.aside == Aside::Spent, "a head with room set aside for it went down");
            Some(Demand::Room(u32::try_from(head.len()).expect("a head within Limits::response")))
        }
        Responding::Sending { reply, .. } => match *reply {
            Reply::Length { room: Room::Wanted(room), .. } | Reply::UntilEnd { room: Room::Wanted(room) } => {
                Some(Demand::Room(room))
            }
            Reply::Chunked { room: Room::Wanted(room) } => {
                Some(Demand::Room(response::chunk_room(room).expect("a chunk's room within a u32")))
            }
            Reply::Last => Some(Demand::Room(u32::try_from(response::LAST_CHUNK.len()).expect("five bytes"))),
            Reply::Length { room: Room::Idle | Room::Granted(_), .. }
            | Reply::Chunked { room: Room::Idle | Room::Granted(_) }
            | Reply::UntilEnd { room: Room::Idle | Room::Granted(_) }
            | Reply::Finished
            | Reply::Withdrawn => None,
        },
    }
}

/// What to read below for the request body, if anything.
fn body_read(exchange: &Exchange, intake: &Intake, limits: &Limits) -> Option<Demand> {
    let incoming = exchange.body.as_ref()?;
    let read = body::read(incoming, intake, limits.head, limits.read)?;
    Some(Demand::Read(read))
}

/// The exchange is done: the response is all queued below, and the request
/// body was read to its end, discarded, or given up.
fn done(exchange: Exchange, persist: bool, below: &mut Queue<Down>, above: &mut Queue<Event>) -> State {
    if persist && exchange.keep {
        assert!(exchange.below.is_none(), "a response all sent leaves nothing demanded");
        above.push(Event::Done(Reuse::Keep));
        return State::Idle(Line::Open);
    }
    if exchange.below.is_some() {
        // Room that may still come after the end: the server wants none.
        below.push(Down::Demand { read: Read::Nothing, room: 0 });
    }
    above.push(Event::Done(Reuse::Close));
    State::Spent
}

/// The exchange failed: whichever of its streams the side above still
/// reads or writes is told, then `Failed`; the server waits for its close.
fn fail(
    exchange: Exchange,
    error: Error,
    intake: &mut Intake,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    // What the side above's streams are told: the stream's own fault; an
    // end that cut the request short is no error of the client's data, but
    // one of the side below's own; anything else is.
    let fault = match error {
        Error::Stream(fault) => fault,
        Error::Truncated => Fault::Other,
        Error::Rejected(_) | Error::ChunkSize | Error::Chunk | Error::Trailer => Fault::Invalid,
    };
    match exchange.response {
        Responding::Head { reply, .. } | Responding::Sending { reply, .. } => match reply {
            Reply::Length { .. } | Reply::Chunked { .. } | Reply::UntilEnd { .. } => {
                above.push(Event::Reply(Up::Failed(fault)));
            }
            // Finished or withdrawn from the side above's side: it is told
            // nothing.
            Reply::Last | Reply::Finished | Reply::Withdrawn => {}
        },
        Responding::Awaited => {}
    }
    match exchange.body {
        Some(Incoming { face: Face::Idle | Face::Demand(_), .. }) => above.push(Event::Body(Up::Failed(fault))),
        // A stream the side above reads no more is told nothing.
        Some(Incoming { face: Face::Withdrawn | Face::Discarding, .. }) | None => {}
    }
    above.push(Event::Failed(error));
    body::drain(intake);
    if exchange.below.is_some() {
        // Room that may still come after the end: the server wants none.
        below.push(Down::Demand { read: Read::Nothing, room: 0 });
    }
    State::Spent
}

/// The error a chunked body's bad framing fails the exchange with.
fn framing_error(bad: body::Bad) -> Error {
    match bad {
        body::Bad::ChunkSize => Error::ChunkSize,
        body::Bad::Chunk => Error::Chunk,
        body::Bad::Trailer => Error::Trailer,
    }
}
