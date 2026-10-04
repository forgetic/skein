//! The HTTP/1.1 client connection (http.md, 3): a step machine that makes
//! one exchange at a time over the stream below, and uses the connection
//! again when both sides allow it.
//!
//! # Its two sides
//!
//! Below, a `lib::stream` (lib.md, 7) to the server: TLS's plaintext, a
//! socket or a pipe. The client writes each request head whole, sized
//! (programming-model.md, 8), within room it asks for, and passes the
//! request body down as the side above writes it. It reads the response
//! head a line at a time, by scans to LF of at most what is left of
//! [`Limits::head`], and the body by its framing: by length, by chunks (a
//! scanned size line, then the data), or to the end of the stream.
//!
//! Above, the service's protocol layer ([`Request`] down, [`Event`] up),
//! and the machines it stacks on the response body:
//!
//! - **`Call` starts an exchange,** one at a time. Exactly one terminal
//!   event answers it: `Done`, once the response body has been read to its
//!   end or discarded, or `Failed`. A call refused (`Refused`) writes
//!   nothing, and leaves the connection as it was.
//! - **The request body is a stream** (`Upload`), written: room demanded,
//!   `Send`s within it, then `Finish`, exactly the length the call
//!   announced in all. A final response that comes before the body is all
//!   sent stops the upload, which hears `Failed(Fault::Other)`, and the
//!   exchange ends with that response, on a connection not used again.
//! - **`Response` gives the final response's head.** Interim responses
//!   (1xx) are read and skipped.
//! - **The response body is a stream** (`Body`), read: the client is its
//!   side below, and keeps its contract (lib.md, 7). `End` is the body's
//!   end, and `Done` follows it at once.
//! - **`Discard`** gives up the rest of the body, a demand outstanding or
//!   withdrawn among it: the client reads and drops it, so that the
//!   connection can be used again, then says `Done`.
//! - **`Close` ends the client in any state:** it withdraws what it
//!   demanded below, ends the exchange in progress without a word, and
//!   answers `Closed`, its one terminal event. It does not close the
//!   stream below, which its owner closes.
//!
//! # Bounds
//!
//! A request head is at most [`Limits::request`]; a response's heads, its
//! interim ones included, at most [`Limits::head`] in all, with at most
//! [`Limits::headers`] fields each; a chunk's size line and the trailer
//! section, at most [`Limits::head`] each. The body is not bounded: it is
//! a stream, and the side above stops reading it when it has had enough.
//! Each entry point emits at most [`UP_MAX_OUT`] or [`DOWN_MAX_OUT`];
//! [`worst_case`] is what a client holds; [`largest_read`] and
//! [`largest_room`] are what whoever stacks it checks against the caps of
//! the stream below at startup.

use core::mem;

use alloc::boxed::Box;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, List, Queue};

use crate::{Header, MaxOut};

mod body;
mod head;
mod request;

use body::Pumped;
use head::Parsed;
pub use request::{Body, Call, Method, Refusal};

/// The client's limits (programming-model.md, 7): the same for every step
/// and for [`Client::new`], which allocates by them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// The longest request head the client writes, its blank line
    /// included: a longer call is refused with [`Refusal::TooLong`]. Also
    /// the room it asks for to send one.
    pub request: u32,
    /// The most bytes of a response's heads, interim heads included, lines
    /// and their endings: past it, [`Error::HeadTooLong`]. Also the
    /// longest chunk size line, and the longest trailer section. At least
    /// 2, a blank line.
    pub head: u32,
    /// The most fields a response head holds: past it,
    /// [`Error::TooManyHeaders`].
    pub headers: u32,
    /// The most the side above demands of the response body at once, a
    /// fill's count or a scan's maximum: the cap of the intake that holds
    /// the body's carry-over. At least 1.
    pub read: u32,
    /// The most room the side above demands at once for the request body.
    pub send: u32,
}

/// The most bytes the client demands below at once: a line of a head or a
/// chunk's size, or a piece of the body.
///
/// Whoever stacks the client checks at startup that the stream below's
/// intake holds it (lib.md, 7): a demand past that cap could never be met.
#[must_use]
pub fn largest_read(limits: &Limits) -> u32 {
    limits.head.max(limits.read).max(2)
}

/// The most room the client demands below at once: a request head, or what
/// the side above demands for its body. Whoever stacks the client checks it
/// against the stream below's output cap at startup.
#[must_use]
pub fn largest_room(limits: &Limits) -> u32 {
    limits.request.max(limits.send)
}

/// The most memory a client holds under `limits`, in bytes
/// (programming-model.md, 6.3), or `None` if it does not fit a `u64` or
/// the limits cannot be honoured: a head shorter than a blank line, or a
/// read of nothing.
///
/// It is the intake of the body's carry-over, allocated with the client;
/// the request head, held until room comes for it, with the list of the
/// response's fields; the head being read: its fields' bytes and the line
/// that holds the next, at most [`Limits::head`], and as much again while a
/// fold joins a value; and, once the body is read, the delivery it reads
/// (a line of its framing, within the twice [`Limits::head`] already
/// counted, or a piece, at most [`Limits::read`]) or the carry-over an
/// exchange leaves unread, moved out of the intake when it ends. A
/// delivery is the client's to count (lib.md, 7). A call is the side
/// above's, read and dropped by the step that writes it; what goes up (a
/// response, the body's bytes) is the side above's from when it is emitted.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.head < 2 || limits.read == 0 {
        return None;
    }
    let intake = Intake::worst_case(limits.read)?;
    let fields = List::<Header>::worst_case(limits.headers)?;
    let head = u64::from(limits.head).checked_mul(2)?;
    intake
        .checked_add(fields)?
        .checked_add(head)?
        .checked_add(u64::from(limits.request))?
        .checked_add(u64::from(limits.read))
}

/// [`up`]'s: above, a response and the upload stopped, a body's end and
/// `Done`, or a stream told it failed and `Failed`; below, the request head
/// sent and the next demand.
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 2, below: 2 };

/// [`down`]'s: above, a body's end and `Done`, or `Closed`; below, a
/// demand, a send, or a withdrawal.
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 2, below: 1 };

/// From the side above.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// Starts an exchange, while none is in progress. `Done` or `Failed`
    /// answers it.
    Call(Call),
    /// The request body's stream, written (lib.md, 7): a demand for room
    /// only, of at most [`Limits::send`]; a `Send` within the room granted;
    /// `Finish` once the call's length is sent. No withdrawal: the side
    /// above closes the client instead.
    Upload(Down),
    /// The response body's stream, read, once `Response` came: a demand of
    /// at most [`Limits::read`] and no room, or its withdrawal, after which
    /// the side above reads no more (lib.md, 7): it discards the rest, or
    /// closes the client.
    Body(Down),
    /// The side above reads no more of the response body: the client reads
    /// the rest and drops it, then says `Done`. A demand outstanding is
    /// dropped unanswered.
    Discard,
    /// Closes the client, in any state. `Closed` answers it.
    Close,
}

/// To the side above.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// The final response's head. Its body follows on `Body`.
    Response(Response),
    /// The request body's stream: `Room`, or `Failed` once the upload can
    /// go no further, the fault `Other` when a response came first.
    Upload(Up),
    /// The response body's stream: `Bytes`, `End`, or `Failed` when the
    /// exchange fails while it is read.
    Body(Up),
    /// For a `Call`: the exchange is over, and whether the connection may
    /// carry another. If not, the client waits for its close.
    Done(Reuse),
    /// For a `Call`: the exchange failed, or never began. Unless the call
    /// was refused, the client waits for its close.
    Failed(Error),
    /// For a `Close`: the client is closed. Terminal.
    Closed,
}

/// Whether a connection carries another exchange once one is done.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Reuse {
    /// Both sides allow it: the client waits for the next call.
    Keep,
    /// One of them does not, or the body ran to the end of the stream: the
    /// client waits for its close.
    Close,
}

/// A final response's head.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Response {
    pub version: Version,
    /// The status code, from 200 to 599. The reason phrase is not kept.
    pub status: u16,
    /// The fields, in the order the head gave them, each fold joined.
    pub headers: Box<[Header]>,
    /// How the body is framed.
    pub framing: Framing,
}

impl Response {
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

/// The version a response gave.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Version {
    Http10,
    /// HTTP/1.1, or a later 1.x, read as 1.1 (RFC 9112, 2.3).
    Http11,
}

/// How a response body is framed (RFC 9112, 6.3).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Framing {
    /// No body: a response to `HEAD`, a 204 or a 304.
    Empty,
    /// `Content-Length`: exactly this many bytes.
    Length(u64),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// Neither: the body runs to the end of the stream, and the connection
    /// ends with it.
    UntilEnd,
}

/// Why an exchange failed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// The client refused the call, writing nothing.
    Refused(Refusal),
    /// The stream had ended before the request was sent, so the server did
    /// not see it: another connection may take it.
    Closed,
    /// The stream ended before the response did: before its head was whole,
    /// or before its body's framing said it was over. Whether the server
    /// acted on the request is unknown.
    Truncated,
    /// The stream below failed.
    Stream(Fault),
    /// The status line is not one: not `HTTP/x.y`, a code outside 100 to
    /// 599, or a control character in the reason.
    Status,
    /// The response is of another major version than HTTP/1.
    Version,
    /// A header line is not a field: no colon, a name that is not a token
    /// or whitespace before the colon, a control character in the value,
    /// or a fold before any field.
    Header,
    /// The heads are longer than [`Limits::head`].
    HeadTooLong,
    /// A head holds more than [`Limits::headers`] fields.
    TooManyHeaders,
    /// The body's framing is refused: `Transfer-Encoding` with
    /// `Content-Length`, in HTTP/1.0, or other than `chunked` alone; or a
    /// `Content-Length` that is not one length.
    Framing,
    /// A chunk's size line is not one: not hexadecimal, past a `u64`, or
    /// longer than [`Limits::head`].
    ChunkSize,
    /// A chunk's data is not followed by a line ending.
    Chunk,
    /// The trailer section is longer than [`Limits::head`].
    Trailer,
    /// A 101: the client never asks to switch protocols.
    Upgrade,
}

/// What the client is waiting for. Machines keep no timers
/// (programming-model.md, 4): each says what it waits for, and the
/// connection arms the deadlines.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// For the side above to make a call: the connection is idle.
    Call,
    /// For room below, for the request: the peer is not reading it.
    Room,
    /// For the response's head: the peer has the request, or as much of it
    /// as the side above wrote, and has not answered.
    Response,
    /// For the response body, or its framing, from the peer.
    Body,
    /// For the side above: to demand room for the body, send it or finish
    /// it, or to demand the response body.
    Above,
    /// For the side above to close it: the connection is not to be used
    /// again.
    Close,
    /// For nothing: it is closed.
    Nothing,
}

/// A client connection: its state for this machine.
#[derive(Debug)]
pub struct Client {
    state: State,
    /// The response body's carry-over (lib.md, 7), under [`Limits::read`],
    /// empty between exchanges.
    intake: Intake,
}

impl Client {
    /// A client under `limits`, the limits its steps will be given, ready
    /// for a call. It demands nothing until it has one.
    #[must_use]
    pub fn new(limits: &Limits) -> Client {
        assert!(worst_case(limits).is_some(), "the limits are honoured: a head of a blank line, a read of a byte");
        Client { state: State::Idle(Line::Open), intake: Intake::with_capacity(limits.read) }
    }

    /// What it is waiting for: a function of its state alone.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match &self.state {
            State::Idle(Line::Open) => Waiting::Call,
            State::Idle(Line::Ended | Line::Failed(_)) | State::Spent => Waiting::Close,
            State::Closed => Waiting::Nothing,
            State::Exchange(exchange) => match exchange.below {
                None => Waiting::Above,
                Some(demand) if demand.room > 0 => Waiting::Room,
                Some(_) => match exchange.response {
                    Receiving::Head(_) => Waiting::Response,
                    Receiving::Body(_) => Waiting::Body,
                },
            },
        }
    }
}

/// An event from the stream below. Emits at most [`UP_MAX_OUT`].
pub fn up(client: &mut Client, env: &Env<Limits>, ev: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let state = mem::replace(&mut client.state, State::Closed);
    client.state = match state {
        State::Idle(line) => State::Idle(idle(line, ev)),
        State::Exchange(exchange) => exchange_up(exchange, &mut client.intake, limits, ev, above, below),
        // An answer to the demand an exchange's end withdrew, on its way
        // (lib.md, 7); an end or a failure, which changes nothing now.
        State::Spent => State::Spent,
        State::Closed => State::Closed,
    };
}

/// A request from the side above. Emits at most [`DOWN_MAX_OUT`].
pub fn down(client: &mut Client, env: &Env<Limits>, rq: Request, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let intake = &mut client.intake;
    let state = mem::replace(&mut client.state, State::Closed);
    client.state = match rq {
        Request::Close => close(state, intake, above, below),
        Request::Call(call) => match state {
            State::Idle(line) => call_made(line, call, intake, limits, above, below),
            State::Exchange(_) => unreachable!("a Call while an exchange is in progress"),
            State::Spent => unreachable!("a Call on a connection told not to be used again"),
            State::Closed => unreachable!("a Call after Closed"),
        },
        Request::Upload(down) => match state {
            State::Exchange(exchange) => upload(exchange, down, intake, limits, above, below),
            // After the exchange failed, a request on its way is dropped.
            State::Spent => State::Spent,
            State::Idle(_) => unreachable!("an upload with no exchange in progress"),
            State::Closed => unreachable!("an upload after Closed"),
        },
        Request::Body(down) => match state {
            State::Exchange(exchange) => body_demanded(exchange, down, intake, limits, above, below),
            State::Spent => State::Spent,
            State::Idle(_) => unreachable!("a body demand with no exchange in progress"),
            State::Closed => unreachable!("a body demand after Closed"),
        },
        Request::Discard => match state {
            State::Exchange(exchange) => discard(exchange, intake, limits, above, below),
            State::Spent => State::Spent,
            State::Idle(_) => unreachable!("a Discard with no exchange in progress"),
            State::Closed => unreachable!("a Discard after Closed"),
        },
    };
}

/// What the client is doing.
#[derive(Debug)]
enum State {
    /// No exchange: waiting for a call, on a connection that is open, or
    /// that ended or failed meanwhile.
    Idle(Line),
    Exchange(Exchange),
    /// The connection is not to be used again: an exchange failed, or was
    /// done with a connection that does not persist. Waiting for the close.
    Spent,
    /// Closed: terminal, and the placeholder of every transition.
    Closed,
}

/// The stream below, as an idle client knows it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Line {
    Open,
    /// It ended: a call would never be answered.
    Ended,
    Failed(Fault),
}

/// An exchange in progress: the request going down, the response coming
/// up, and the one demand outstanding below.
#[derive(Debug)]
struct Exchange {
    /// The call's method: a response to `HEAD` has no body.
    method: Method,
    /// Whether the call asked to close the connection after it.
    close: bool,
    request: Sending,
    response: Receiving,
    /// The demand outstanding below, if any: stated only when none is, and
    /// answered at most once (lib.md, 7).
    below: Option<Demand>,
}

/// A demand the client stated below.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Demand {
    read: Read,
    room: u32,
}

/// The request, going down.
#[derive(Debug)]
struct Sending {
    /// The head, written, until room comes to send it whole.
    head: Option<Box<[u8]>>,
    upload: Upload,
}

/// The request body, as the side above writes it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Upload {
    /// The call has none.
    None,
    /// This many bytes are still to come from the side above.
    Open { left: u64, room: Room },
    /// All of it went down, and the side above finished it.
    Finished,
    /// A final response came first, or the exchange failed: the side
    /// above was told `Failed`.
    Stopped,
}

/// The upload's room.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Room {
    /// Nothing outstanding: the side above has not demanded room, or has
    /// sent within what it was granted.
    Idle,
    /// The side above demands this much, which the client demands below.
    Wanted(u32),
    /// This much was granted: the side above may send it.
    Granted(u32),
}

/// The response, coming up.
#[derive(Debug)]
enum Receiving {
    /// A head is read, interim or final: once the request head is sent.
    Head(Reading),
    /// The final response went up: its body.
    Body(Download),
}

/// A response head being read.
#[derive(Debug)]
struct Reading {
    /// What is left of [`Limits::head`] for this exchange's heads.
    budget: u32,
    /// The status line, once read.
    status: Option<Status>,
    headers: List<Header>,
}

impl Reading {
    fn new(budget: u32, limits: &Limits) -> Reading {
        Reading { budget, status: None, headers: List::with_capacity(limits.headers) }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Status {
    version: Version,
    code: u16,
}

/// A response body being read.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Download {
    /// What is left of it below.
    rest: Rest,
    /// The side above's side of its stream.
    face: Face,
    /// Whether the response lets the connection carry another exchange,
    /// once this one is done.
    persist: bool,
}

/// What is left of a body below, by its framing.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Rest {
    /// By length: this many bytes, at least one.
    Length(u64),
    /// Chunked: this many bytes of a chunk's data, at least one.
    Chunk(u64),
    /// Chunked: the line ending after a chunk's data.
    ChunkEnd,
    /// Chunked: the next chunk's size line.
    ChunkSize,
    /// Chunked: the trailer section after the last chunk, this many bytes
    /// of it at most, its blank line included.
    Trailer(u32),
    /// To the end of the stream.
    UntilEnd,
    /// Nothing: the body is all read below.
    Over,
}

/// The side above's side of the body's stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Face {
    /// No demand outstanding.
    Idle,
    /// A demand outstanding, which the intake does not meet yet.
    Demand(Read),
    /// The side above withdrew its demand: it reads no more (lib.md, 7),
    /// and discards the rest or closes the client next.
    Withdrawn,
    /// The side above gave up the rest: read and dropped.
    Discarding,
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
        Up::Bytes(_) | Up::Room => unreachable!("an answer while the client demands nothing"),
    }
}

/// A call, while idle.
fn call_made(
    line: Line,
    call: Call,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    match line {
        Line::Open => {}
        Line::Ended => {
            above.push(Event::Failed(Error::Closed));
            return State::Spent;
        }
        Line::Failed(fault) => {
            above.push(Event::Failed(Error::Stream(fault)));
            return State::Spent;
        }
    }
    let head = match request::write(&call, limits) {
        Ok(head) => head,
        Err(refusal) => {
            above.push(Event::Failed(Error::Refused(refusal)));
            return State::Idle(Line::Open);
        }
    };
    let upload = match call.body {
        Body::None => Upload::None,
        Body::Length(left) => Upload::Open { left, room: Room::Idle },
    };
    let exchange = Exchange {
        method: call.method,
        close: call.close,
        request: Sending { head: Some(head), upload },
        response: Receiving::Head(Reading::new(limits.head, limits)),
        below: None,
    };
    settle(exchange, intake, limits, above, below)
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
            let demand = exchange.below.take().expect("bytes answer a demand");
            assert!(demand.read != Read::Nothing, "bytes answer a demand that reads");
            let parsed = match &mut exchange.response {
                Receiving::Head(reading) => head::line(reading, &bytes),
                Receiving::Body(download) => {
                    return match body::delivered(download, intake, limits, bytes, above) {
                        Ok(()) => settle(exchange, intake, limits, above, below),
                        Err(error) => fail(exchange, error, intake, above, below),
                    };
                }
            };
            match parsed {
                Ok(Parsed::More) => settle(exchange, intake, limits, above, below),
                Ok(Parsed::Complete) => head_complete(exchange, intake, limits, above, below),
                Err(error) => fail(exchange, error, intake, above, below),
            }
        }
        Up::Room => {
            let demand = exchange.below.take().expect("room answers a demand");
            assert!(demand.room > 0, "room answers a demand for room");
            match exchange.request.head.take() {
                Some(head) => below.push(Down::Send(head)),
                None => match exchange.request.upload {
                    Upload::Open { left, room: Room::Wanted(room) } => {
                        exchange.request.upload = Upload::Open { left, room: Room::Granted(room) };
                        above.push(Event::Upload(Up::Room));
                    }
                    Upload::Open { room: Room::Idle | Room::Granted(_), .. }
                    | Upload::None
                    | Upload::Finished
                    | Upload::Stopped => unreachable!("room is demanded for the head, or for the upload"),
                },
            }
            settle(exchange, intake, limits, above, below)
        }
        Up::End => ended(exchange, intake, limits, above, below),
        Up::Failed(fault) => {
            // Nothing follows a failure: what was outstanding is over.
            exchange.below = None;
            // A failure once the body is all read below leaves it whole: only
            // the connection is gone.
            match &mut exchange.response {
                Receiving::Body(download) if download.rest == Rest::Over => {
                    download.persist = false;
                    settle(exchange, intake, limits, above, below)
                }
                Receiving::Body(_) | Receiving::Head(_) => fail(exchange, Error::Stream(fault), intake, above, below),
            }
        }
    }
}

/// The stream ended during an exchange: no response can come, unless its
/// body runs to the end of the stream, or was all read already.
fn ended(
    mut exchange: Exchange,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    // A read outstanding crosses the end and is never met; room may still
    // come after it, and a failure below withdraws a demand for room.
    match exchange.below {
        Some(demand) if demand.room == 0 => exchange.below = None,
        Some(_) | None => {}
    }
    if exchange.request.head.is_some() {
        return fail(exchange, Error::Closed, intake, above, below);
    }
    match &mut exchange.response {
        Receiving::Head(_) => fail(exchange, Error::Truncated, intake, above, below),
        Receiving::Body(download) => match download.rest {
            Rest::UntilEnd | Rest::Over => {
                download.rest = Rest::Over;
                download.persist = false;
                settle(exchange, intake, limits, above, below)
            }
            Rest::Length(_) | Rest::Chunk(_) | Rest::ChunkEnd | Rest::ChunkSize | Rest::Trailer(_) => {
                fail(exchange, Error::Truncated, intake, above, below)
            }
        },
    }
}

/// A response head is whole: an interim one is skipped, a final one goes
/// up.
fn head_complete(
    mut exchange: Exchange,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let reading = match &mut exchange.response {
        Receiving::Head(reading) => reading,
        Receiving::Body(_) => unreachable!("a head is read before the body"),
    };
    let status = reading.status.expect("a head is complete only after its status line");
    match status.code {
        101 => fail(exchange, Error::Upgrade, intake, above, below),
        // The next head is read within what is left of the budget, if
        // anything is, into the same list of fields.
        100..=199 => {
            if reading.budget == 0 {
                return fail(exchange, Error::HeadTooLong, intake, above, below);
            }
            reading.status = None;
            reading.headers.clear();
            settle(exchange, intake, limits, above, below)
        }
        _ => {
            let headers = mem::replace(&mut reading.headers, List::with_capacity(0));
            final_head(exchange, status, headers, intake, limits, above, below)
        }
    }
}

/// A final response's head: up it goes, the upload stops if the body is
/// not all sent, and the body follows.
fn final_head(
    mut exchange: Exchange,
    status: Status,
    headers: List<Header>,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let framing = match head::framing(exchange.method, status, headers.as_slice()) {
        Ok(framing) => framing,
        Err(error) => return fail(exchange, error, intake, above, below),
    };
    let (rest, lasting) = match framing {
        Framing::Empty | Framing::Length(0) => (Rest::Over, true),
        Framing::Length(length) => (Rest::Length(length), true),
        Framing::Chunked => (Rest::ChunkSize, true),
        Framing::UntilEnd => (Rest::UntilEnd, false),
    };
    let persist = lasting && !exchange.close && head::persistent(status.version, headers.as_slice());
    let response = Response { version: status.version, status: status.code, headers: headers.into_boxed(), framing };
    above.push(Event::Response(response));
    match exchange.request.upload {
        Upload::Open { .. } => {
            exchange.request.upload = Upload::Stopped;
            above.push(Event::Upload(Up::Failed(Fault::Other)));
        }
        Upload::None | Upload::Finished | Upload::Stopped => {}
    }
    exchange.response = Receiving::Body(Download { rest, face: Face::Idle, persist });
    settle(exchange, intake, limits, above, below)
}

/// The request body's stream, from the side above.
fn upload(
    mut exchange: Exchange,
    down: Down,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let upload = exchange.request.upload;
    exchange.request.upload = match upload {
        // The upload stopped, and this was on its way.
        Upload::Stopped => return State::Exchange(exchange),
        Upload::None => unreachable!("an upload for a call without a body"),
        Upload::Finished => unreachable!("an upload after Finish"),
        Upload::Open { left, room } => match down {
            Down::Demand { read, room: wanted } => {
                assert!(read == Read::Nothing, "the request body's stream is written, not read");
                assert!(wanted > 0, "the upload's demand is not withdrawn: the side above closes the client instead");
                assert!(wanted <= limits.send, "no room past Limits::send");
                assert!(room == Room::Idle, "one demand at a time, stated after the last was answered");
                assert!(left > 0, "no room for a body all sent: Finish");
                Upload::Open { left, room: Room::Wanted(wanted) }
            }
            Down::Send(bytes) => {
                let granted = match room {
                    Room::Granted(granted) => granted,
                    Room::Idle | Room::Wanted(_) => unreachable!("a Send within the room granted"),
                };
                let len = u64::try_from(bytes.len()).expect("a length fits a u64");
                assert!(len <= u64::from(granted), "a Send within the room granted");
                let left = left.checked_sub(len).expect("no more than the call's length");
                below.push(Down::Send(bytes));
                Upload::Open { left, room: Room::Idle }
            }
            Down::Finish => {
                assert!(left == 0 && room == Room::Idle, "Finish once the call's length is sent");
                Upload::Finished
            }
        },
    };
    settle(exchange, intake, limits, above, below)
}

/// The response body's stream, from the side above.
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
            assert!(room == 0, "the response body's stream is read, not written");
            read
        }
        Down::Send(_) | Down::Finish => unreachable!("the response body's stream is read, not written"),
    };
    let download = match &mut exchange.response {
        Receiving::Body(download) => download,
        Receiving::Head(_) => unreachable!("a body demand before the response"),
    };
    download.face = match download.face {
        Face::Idle => {
            let wanted = match read {
                Read::Fill(n) => n,
                Read::Scan { max, .. } => max,
                Read::Nothing => unreachable!("a withdrawal with no demand outstanding"),
            };
            assert!(wanted <= limits.read, "no demand past Limits::read");
            Face::Demand(read)
        }
        // A withdrawal: what is read for it from now on is dropped.
        Face::Demand(_) if read == Read::Nothing => Face::Withdrawn,
        Face::Demand(_) => unreachable!("one demand at a time, stated after the last was answered"),
        Face::Withdrawn => unreachable!("a body demand after its withdrawal: the side above reads no more"),
        Face::Discarding => unreachable!("a body demand after Discard"),
    };
    settle(exchange, intake, limits, above, below)
}

/// The side above gives up the rest of the body.
fn discard(
    mut exchange: Exchange,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let download = match &mut exchange.response {
        Receiving::Body(download) => download,
        Receiving::Head(_) => unreachable!("a Discard before the response"),
    };
    match download.face {
        Face::Idle | Face::Demand(_) | Face::Withdrawn => {}
        Face::Discarding => unreachable!("a second Discard"),
    }
    download.face = Face::Discarding;
    // A body that runs to the end of the stream has no end to discard to:
    // the exchange is done, and the connection with it.
    if download.rest == Rest::UntilEnd {
        download.rest = Rest::Over;
        download.persist = false;
    }
    settle(exchange, intake, limits, above, below)
}

/// `Close`, in any state.
fn close(state: State, intake: &mut Intake, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    match state {
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
/// intake, or the body's end; the exchange done; or the next demand below,
/// if none is outstanding and the state wants one.
fn settle(
    mut exchange: Exchange,
    intake: &mut Intake,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let pumped = match &mut exchange.response {
        Receiving::Body(download) => body::pump(download, intake, above),
        Receiving::Head(_) => Pumped::Open,
    };
    match pumped {
        Pumped::Open => {}
        Pumped::Over => return done(exchange, intake, above, below),
    }
    if exchange.below.is_some() {
        return State::Exchange(exchange);
    }
    if let Some(demand) = wanted(&exchange, intake, limits) {
        below.push(Down::Demand { read: demand.read, room: demand.room });
        exchange.below = Some(demand);
    }
    State::Exchange(exchange)
}

/// What the exchange demands below, when nothing is outstanding: room for
/// the head, alone; while the body is uploaded, room for what the side
/// above demands, with the next line of a response that may come first,
/// and nothing until the side above demands; then what the response needs.
fn wanted(exchange: &Exchange, intake: &Intake, limits: &Limits) -> Option<Demand> {
    if let Some(head) = &exchange.request.head {
        let room = u32::try_from(head.len()).expect("a head within Limits::request");
        return Some(Demand { read: Read::Nothing, room });
    }
    let room = match exchange.request.upload {
        Upload::Open { room: Room::Wanted(room), .. } => room,
        // Reading alone now would hold a demand the upload's next could
        // not join, while the server waits for the body.
        Upload::Open { room: Room::Idle | Room::Granted(_), .. } => return None,
        Upload::None | Upload::Finished | Upload::Stopped => 0,
    };
    let read = match &exchange.response {
        Receiving::Head(reading) => {
            assert!(reading.budget > 0, "a head with nothing left of its budget has failed");
            Read::Scan { until: Delimiter::LF, max: reading.budget }
        }
        Receiving::Body(download) => match body::read(download, intake, limits) {
            Some(read) => read,
            None => Read::Nothing,
        },
    };
    if read == Read::Nothing && room == 0 {
        return None;
    }
    Some(Demand { read, room })
}

/// The exchange is done: its body's end went up, or the rest was
/// discarded.
fn done(exchange: Exchange, intake: &mut Intake, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    let download = match exchange.response {
        Receiving::Body(download) => download,
        Receiving::Head(_) => unreachable!("an exchange is done once its body is"),
    };
    let sent = match exchange.request.upload {
        Upload::None | Upload::Finished => true,
        Upload::Stopped => false,
        Upload::Open { .. } => unreachable!("a final response stops the upload"),
    };
    body::drain(intake);
    if download.persist && sent {
        assert!(exchange.below.is_none(), "a body read to its end leaves nothing demanded");
        above.push(Event::Done(Reuse::Keep));
        return State::Idle(Line::Open);
    }
    if exchange.below.is_some() {
        // A discard of a body that runs to the end of the stream: the read
        // for it is withdrawn, as the client reads no more.
        below.push(Down::Demand { read: Read::Nothing, room: 0 });
    }
    above.push(Event::Done(Reuse::Close));
    State::Spent
}

/// The exchange failed: whichever of its streams the side above still
/// reads or writes is told, then `Failed`; the client waits for its close.
fn fail(
    exchange: Exchange,
    error: Error,
    intake: &mut Intake,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let fault = match error {
        Error::Stream(fault) => fault,
        Error::Refused(_)
        | Error::Closed
        | Error::Truncated
        | Error::Status
        | Error::Version
        | Error::Header
        | Error::HeadTooLong
        | Error::TooManyHeaders
        | Error::Framing
        | Error::ChunkSize
        | Error::Chunk
        | Error::Trailer
        | Error::Upgrade => Fault::Invalid,
    };
    match exchange.request.upload {
        Upload::Open { .. } => above.push(Event::Upload(Up::Failed(fault))),
        Upload::None | Upload::Finished | Upload::Stopped => {}
    }
    match exchange.response {
        Receiving::Body(Download { face: Face::Idle | Face::Demand(_), .. }) => {
            above.push(Event::Body(Up::Failed(fault)));
        }
        // A stream the side above reads no more is told nothing.
        Receiving::Body(Download { face: Face::Withdrawn | Face::Discarding, .. }) | Receiving::Head(_) => {}
    }
    above.push(Event::Failed(error));
    body::drain(intake);
    if exchange.below.is_some() {
        // Room that may still come after the end: the client wants none.
        below.push(Down::Demand { read: Read::Nothing, room: 0 });
    }
    State::Spent
}
