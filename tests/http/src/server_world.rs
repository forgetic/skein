//! The server's machine world (testing-strategy.md, 2.4): one server, from
//! a seed, between its two neighbours, both played by the world in one
//! loop, for one request after another on one connection.
//!
//! - **The side below** is the client's stream: its bytes arrive in pieces
//!   cut at random, late, into an intake under its cap, and meet each read
//!   exactly; room is granted late, and each `Send` is held to it as io
//!   holds it (io.md, 3.3). The client pipelines its requests, or is
//!   patient and sends each once the last exchange is over; one that asks
//!   for a 100 (Continue) holds its body until the 100 comes, or a final
//!   response, after which it sends no more, or until it tires of waiting.
//!   The stream ends when the client's bytes run out, early when the
//!   settings cut them, idle or with a read on its way; or it fails, before
//!   its end or after it.
//! - **The side above** is a service: it asks for each request when it
//!   feels like it, reads the body with demands of every shape, slowly,
//!   withdraws a demand now and then, and responds at the moment its plan
//!   says (at once, partway through the body, once it read it, or once it
//!   discarded it), with a response now and then refused, then writes the
//!   reply in pieces within the room granted; it stops for a while, and
//!   closes after the last exchange or at a moment the settings draw.
//!
//! The world checks both of the server's sides as it goes
//! (testing-strategy.md, 6): `MAX_OUT` on each call; below, one demand at
//! a time, none past the caps, no read once the stream ended or failed, a
//! withdrawal only as the server stops reading, and each `Send` within
//! the room granted, one a grant; above, one answer for each `Next`, each
//! body answer for a demand and exactly what it reads, `End` and `Failed`
//! once and nothing after, room only for a demand, a refusal only for a
//! `Respond`, each stream still open told before the exchange's `Failed`,
//! one terminal event per call, `Closed` once and last; and `waiting()`
//! against what the neighbours see. [`check`] holds each exchange to the
//! reference reader of requests and a writer of the test's own.

use skein_http::MaxOut;
use skein_http::server::{
    self, Body, Call, Error, Event, Limits, Refusal, Rejection, Request, Response, Reuse, Server, Waiting,
};
use skein_http::{Method, Version};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue, Rng, Time, Wall};

use crate::reference::{self, RequestEnding};
use crate::requests;

/// How a world runs: the server's limits, and how its neighbours behave.
#[derive(Clone, Debug)]
pub struct Settings {
    pub limits: Limits,
    /// The side below's intake cap: at least the server's largest read.
    pub cap: u32,
    /// The side below's output cap: at least the server's largest room.
    pub output: u32,
    /// The longest piece the client's bytes arrive in.
    pub piece: u32,
    /// Per mille: how likely a piece arrives in an iteration.
    pub arrival: u32,
    /// Per mille: how likely room demanded is granted in an iteration.
    pub grant: u32,
    /// Per mille: how likely the side above acts in an iteration where it
    /// may.
    pub eagerness: u32,
    /// Per mille: how likely the client ends the stream in an iteration
    /// where it has nothing left to send and nothing is held.
    pub idle_end: u32,
    /// How the side above reads a body.
    pub reads: Reads,
    /// Per mille, per demand: how likely the side above withdraws a body
    /// demand rather than wait for it, and discards the rest.
    pub withdraw: u32,
    /// Whether the client sends each request only once the last exchange is
    /// over, rather than pipelining them.
    pub patient: bool,
    /// How many iterations a client that asked for a 100 (Continue) waits
    /// for it before it sends the body anyway.
    pub patience: u64,
    /// Where the client's bytes stop, if before their end.
    pub cut: Option<usize>,
    /// When the stream fails, if it does: once, at the first iteration from
    /// this one on, with this fault.
    pub failure: Option<(u64, Fault)>,
    /// When the side above closes, if it does whatever the server is doing.
    pub close: Option<u64>,
    /// When the side above stops acting, and for how many iterations.
    pub stall: Option<(u64, u64)>,
}

/// How the side above reads a body.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reads {
    /// With fills and scans of every size up to the limit, to LF, CRLF or
    /// a quote: it may not see the last bytes, which no demand meets.
    Any,
    /// A byte at a time: it sees every byte.
    Bytes,
}

impl Settings {
    /// Neighbours that are slow and cut the bytes anywhere, but never end
    /// the stream early, fail it or close before the last exchange.
    #[must_use]
    pub fn calm(rng: &mut Rng, limits: Limits) -> Settings {
        Settings {
            limits,
            cap: server::largest_read(&limits) + draw(rng, 0, 64),
            output: server::largest_room(&limits) + draw(rng, 0, 64),
            piece: draw(rng, 1, 96),
            arrival: draw(rng, 100, 1000),
            grant: draw(rng, 100, 1000),
            eagerness: draw(rng, 100, 1000),
            idle_end: draw(rng, 0, 1000),
            reads: if rng.chance(300) { Reads::Bytes } else { Reads::Any },
            withdraw: if rng.chance(300) { draw(rng, 0, 200) } else { 0 },
            patient: rng.chance(500),
            patience: rng.between(8, 400),
            cut: None,
            failure: None,
            close: None,
            stall: None,
        }
    }

    /// Neighbours as [`calm`](Settings::calm), and sometimes a stream that
    /// ends early or fails, a close at any moment, or a side above that
    /// stops for a while, for a client's `len` bytes.
    #[must_use]
    pub fn chaotic(rng: &mut Rng, limits: Limits, len: usize) -> Settings {
        let mut settings = Settings::calm(rng, limits);
        let span = 6 * len as u64 + 64;
        if rng.chance(150) {
            settings.cut = Some(usize::try_from(rng.below(len as u64 + 1)).expect("fits a usize"));
        }
        if rng.chance(150) {
            settings.failure = Some((rng.below(span), fault(rng)));
        }
        if rng.chance(150) {
            settings.close = Some(rng.below(span));
        }
        if rng.chance(150) {
            settings.stall = Some((rng.below(span), rng.between(16, 256)));
        }
        settings
    }

    /// The bytes the client sends of `client`.
    #[must_use]
    pub fn sent<'a>(&self, client: &'a [u8]) -> &'a [u8] {
        match self.cut {
            Some(cut) => &client[..cut.min(client.len())],
            None => client,
        }
    }
}

/// Limits drawn at random: mostly roomy enough for a generated request,
/// and now and then tiny, so that a sweep reaches each of them.
#[must_use]
pub fn limits(rng: &mut Rng) -> Limits {
    if rng.chance(250) {
        return Limits {
            head: draw(rng, 2, 160),
            headers: draw(rng, 0, 6),
            body: rng.below(200),
            read: draw(rng, 1, 16),
            response: draw(rng, 87, 400),
            send: draw(rng, 1, 16),
        };
    }
    Limits {
        head: draw(rng, 512, 4096),
        headers: draw(rng, 16, 64),
        body: rng.between(256, 1 << 20),
        read: draw(rng, 4, 256),
        response: draw(rng, 512, 2048),
        send: draw(rng, 1, 256),
    }
}

fn fault(rng: &mut Rng) -> Fault {
    [Fault::Reset, Fault::Invalid, Fault::Other][usize::try_from(rng.below(3)).expect("fits a usize")]
}

fn draw(rng: &mut Rng, low: u32, high: u32) -> u32 {
    u32::try_from(rng.between(u64::from(low), u64::from(high))).expect("fits a u32")
}

/// What the side above does with one call: the response it gives, the
/// body it writes for it, and when it responds.
#[derive(Clone, Debug)]
pub struct Plan {
    pub response: Response,
    pub reply: Vec<u8>,
    pub when: When,
}

/// When the side above responds to a call.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum When {
    /// At once, before it reads the body.
    First,
    /// After this many pieces of the body, or its end.
    Partway(u32),
    /// Once it read the body to its end.
    AfterBody,
    /// Once it discarded the body.
    Discarding,
}

/// A plan drawn at random: a response of [`requests::response`], given at
/// a moment drawn too.
#[must_use]
pub fn plan(rng: &mut Rng) -> Plan {
    let (response, reply) = requests::response(rng);
    let when = match rng.below(10) {
        0 | 1 => When::First,
        2 => When::Partway(draw(rng, 0, 4)),
        3 => When::Discarding,
        _ => When::AfterBody,
    };
    Plan { response, reply, when }
}

/// The response the side above gives after one was refused: one the server
/// takes whatever the call.
fn fallback() -> Response {
    Response { status: 503, headers: Box::new([]), body: Body::None, close: false }
}

/// What the server's answer to one `Next` came to, as the neighbours saw
/// it.
#[expect(clippy::struct_excessive_bools, reason = "a record of what was seen, a flag each")]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seen {
    /// The call, if one answered.
    pub call: Option<Call>,
    /// Where the request began in the client's bytes.
    pub start: usize,
    /// The body's bytes the side above received.
    pub body: Vec<u8>,
    /// The demand the body's `End` answered, if it came.
    pub ended: Option<Read>,
    /// The fault the body's stream failed with, if it did.
    pub body_failed: Option<Fault>,
    /// The fault the reply's stream failed with, if it did.
    pub reply_failed: Option<Fault>,
    /// The responses refused, in order.
    pub refused: Vec<(Response, Refusal)>,
    /// The response the server took, with whether its head was to keep
    /// the connection, as the world reckoned it then.
    pub response: Option<(Response, bool)>,
    /// The pieces of the reply the side above sent, in order.
    pub pieces: Vec<Vec<u8>>,
    /// Whether the side above finished the reply.
    pub finished: bool,
    /// Whether the side above discarded the body.
    pub discarded: bool,
    /// What the server sent below for it.
    pub sent: Vec<u8>,
    /// The answer to the `Next`, `Call` aside, or the call's terminal
    /// event.
    pub outcome: Option<Outcome>,
    /// Whether the stream ended or failed while it was in progress.
    pub below_over: bool,
    /// Whether the client had all of the request's body when the side above
    /// responded: the server read it to its end, or would.
    pub whole_below: bool,
}

/// How a `Next` was answered, or an exchange ended.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Outcome {
    Ended,
    Done(Reuse),
    Failed(Error),
}

/// What a run came to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Run {
    pub seen: Vec<Seen>,
    /// What the server waited for when the side above closed it.
    pub closed_while: Waiting,
    /// The fault the stream failed with, if it did.
    pub failed: Option<Fault>,
    pub fell: Fell,
    pub iterations: u64,
}

/// What fell in a run, of what its neighbours may inject: a sweep asserts
/// that each fell at least once (testing-strategy.md, 3).
#[expect(clippy::struct_excessive_bools, reason = "a record of what fell, a flag each")]
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Fell {
    /// The stream ended with nothing demanded.
    pub idle_end: bool,
    /// A demand crossed the end on its way: the room set aside for a
    /// request, which the end then withdraws.
    pub crossed_end: bool,
    /// The side above withdrew a body demand.
    pub withdrew: bool,
    /// The server withdrew a read as it gave the body up.
    pub gave_up: bool,
    /// An answer came after a demand was withdrawn below.
    pub late_answer: bool,
    /// The stream failed after its end.
    pub failed_after_end: bool,
    /// What the server waited for when the stream failed, if it did.
    pub failed_while: Option<Waiting>,
    /// An exchange carried on a connection used before.
    pub reused: bool,
    /// The next request's bytes arrived before the last exchange was over.
    pub pipelined: bool,
    /// A 100 (Continue) went below.
    pub continued: bool,
    /// A client tired of waiting for a 100 and sent its body anyway.
    pub tired: bool,
    /// A response waited for a discard to reach the body's end.
    pub head_waited: bool,
    /// Room came after the stream's end.
    pub room_after_end: bool,
}

/// Runs the world over `client`, the client's bytes for every request in
/// order, with the side above following `plans` one call after another,
/// and `settings`, from `seed`, until the side above closes the server.
#[must_use]
pub fn run(client: &[u8], plans: &[Plan], settings: &Settings, seed: u64) -> Run {
    let limits = settings.limits;
    assert!(settings.cap >= server::largest_read(&limits), "the side below's cap holds the largest read");
    assert!(settings.output >= server::largest_room(&limits), "the side below's output holds the largest room");
    let sent = settings.sent(client);
    let mut world = World {
        rng: Rng::new(seed),
        settings,
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
        server: Server::new(&limits),
        events: Queue::with_capacity(8),
        requests: Queue::with_capacity(8),
        below: Below {
            client: sent,
            arrived: 0,
            delivered: 0,
            requests: requests_in(sent, &limits),
            intake: Intake::with_capacity(settings.cap),
            demand: None,
            granted: 0,
            sends: 0,
            life: Life::Open,
            failed: None,
            withdrawn: None,
            continued: None,
            responded: None,
            waiting_since: None,
        },
        above: Above {
            plans,
            seen: Vec::new(),
            asked: false,
            exchange: None,
            closing: None,
            closed: false,
            spent: false,
        },
        fell: Fell::default(),
        iteration: 0,
    };
    let stalled = match settings.stall {
        Some((_, iterations)) => iterations,
        None => 0,
    };
    let budget = 64 * (client.len() as u64 + 64) + 512 * (plans.len() as u64 + 1) * 8 + stalled + settings.patience;
    while world.iteration < budget {
        if world.rng.chance(500) {
            world.below_acts();
            world.above_acts();
        } else {
            world.above_acts();
            world.below_acts();
        }
        world.iteration += 1;
        if world.above.closed {
            world.late();
            return Run {
                seen: world.above.seen,
                closed_while: world.above.closing.expect("closed after a close"),
                failed: world.below.failed,
                fell: world.fell,
                iterations: world.iteration,
            };
        }
    }
    panic!("seed {seed}: the world settles within {budget} iterations: {settings:?}");
}

/// Runs the world as [`run`], and checks each exchange against the
/// reference reader's reading of what the client sent for it, and what the
/// server wrote against a writer of the test's own: the call, or the
/// answer it was rejected with; the body; the response, its head and its
/// framed body; and the outcome.
#[must_use]
pub fn check(client: &[u8], plans: &[Plan], settings: &Settings, seed: u64) -> Run {
    let run = run(client, plans, settings, seed);
    let replay = || format!("seed {seed}, {settings:?}, client {}", client.escape_ascii());
    let sent = settings.sent(client);
    for (index, seen) in run.seen.iter().enumerate() {
        let what = || format!("request {index}: {seen:?}; {}", replay());
        let expected = reference::request(&sent[seen.start.min(sent.len())..], &settings.limits);
        if let Some(call) = &seen.call {
            let head = expected.head.as_ref().unwrap_or_else(|| panic!("the reference reads a head; {}", what()));
            assert_eq!(
                (call.method, &call.target[..], call.version, call.body),
                (head.method, &head.target[..], head.version, head.body),
                "the call; {}",
                what()
            );
            let headers: Vec<(Vec<u8>, Vec<u8>)> =
                call.headers.iter().map(|header| (header.name.to_vec(), header.value.to_vec())).collect();
            assert_eq!(headers, head.headers, "the fields; {}", what());
        }
        assert!(expected.body.starts_with(&seen.body), "the body is the reference's, in order; {}", what());
        if let Some(read) = seen.ended {
            let tail = &expected.body[seen.body.len()..];
            assert!(!meets(read, tail), "the end comes once nothing left meets the demand: {read:?}; {}", what());
        }
        match seen.outcome {
            None => {}
            Some(Outcome::Ended) => {
                assert_eq!(expected.ending, RequestEnding::None, "no request; {}", what());
                assert!(seen.sent.is_empty(), "nothing written for no request; {}", what());
            }
            Some(Outcome::Failed(Error::Rejected(rejection))) => {
                assert_eq!(
                    expected.ending,
                    RequestEnding::Rejected(rejection),
                    "the reference's rejection; {}",
                    what()
                );
                assert_eq!(seen.sent, requests::answer(rejection.status()), "the server's own answer; {}", what());
            }
            Some(Outcome::Failed(Error::Stream(fault))) => assert_eq!(run.failed, Some(fault), "{}", what()),
            Some(Outcome::Failed(Error::Truncated)) => {
                assert!(
                    expected.ending == RequestEnding::Failed(Error::Truncated) || seen.below_over,
                    "cut short as the reference reads it; {}",
                    what()
                );
            }
            Some(Outcome::Failed(error @ (Error::ChunkSize | Error::Chunk | Error::Trailer))) => {
                assert_eq!(expected.ending, RequestEnding::Failed(error), "the reference's framing error; {}", what());
            }
            Some(Outcome::Done(reuse)) => {
                let Some((_, persist)) = &seen.response else { panic!("done with a response; {}", what()) };
                let keep = *persist && !seen.below_over;
                assert_eq!(reuse == Reuse::Keep, keep, "the reuse; {}", what());
                if seen.whole_below {
                    assert_eq!(expected.ending, RequestEnding::Whole, "the request was whole; {}", what());
                }
            }
        }
        // What the server wrote for a call: a 100 (Continue) if it sent one,
        // the head as the test's writer writes it, and the reply framed.
        if let (Some(call), Some((response, persist))) = (&seen.call, &seen.response) {
            let mut written = Vec::new();
            if seen.sent.starts_with(CONTINUE) {
                assert!(expected.expects, "a 100 (Continue) only for a client that waits for it; {}", what());
                written.extend_from_slice(CONTINUE);
            }
            written.extend_from_slice(&requests::head(response, call.version, *persist));
            let until_end = requests::sends_body(call.method, response)
                && response.body == Body::Chunked
                && call.version == Version::Http10;
            for piece in &seen.pieces {
                if response.body == Body::Chunked && !until_end {
                    if !piece.is_empty() {
                        written.extend_from_slice(&requests::chunk(piece));
                    }
                } else {
                    written.extend_from_slice(piece);
                }
            }
            if seen.finished && response.body == Body::Chunked && !until_end {
                written.extend_from_slice(b"0\r\n\r\n");
            }
            assert!(
                written.starts_with(&seen.sent),
                "the server wrote what the test's writer writes: {}; {}",
                seen.sent.escape_ascii(),
                what()
            );
            if matches!(seen.outcome, Some(Outcome::Done(_))) {
                assert_eq!(seen.sent, written, "all of it, once done; {}", what());
            }
            if until_end || response.close {
                assert!(!persist, "{}", what());
            }
        }
    }
    run
}

const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// Each request in `client`, as the reference reads them one after
/// another: where it begins, where its head ends, where it ends, and
/// whether its client waits for a 100 (Continue). Past a request not whole,
/// none.
fn requests_in(client: &[u8], limits: &Limits) -> Vec<Bounds> {
    let mut bounds = Vec::new();
    let mut offset = 0;
    for _ in 0..1000 {
        if offset >= client.len() {
            break;
        }
        let read = reference::request(&client[offset..], limits);
        if read.ending != RequestEnding::Whole {
            break;
        }
        bounds.push(Bounds {
            start: offset,
            head_end: offset + read.head_end,
            end: offset + read.used,
            expects: read.expects,
        });
        offset += read.used;
    }
    bounds
}

/// Where a request lies in the client's bytes.
#[derive(Clone, Copy, Debug)]
struct Bounds {
    start: usize,
    head_end: usize,
    end: usize,
    expects: bool,
}

/// Whether `bytes` hold what `read` asks for, from their start.
fn meets(read: Read, bytes: &[u8]) -> bool {
    match read {
        Read::Nothing => false,
        Read::Fill(n) => bytes.len() >= usize::try_from(n).expect("fits a usize"),
        Read::Scan { until, max } => {
            let max = usize::try_from(max).expect("fits a usize");
            let window = &bytes[..max.min(bytes.len())];
            bytes.len() >= max || window.windows(until.as_bytes().len()).any(|found| found == until.as_bytes())
        }
        Read::Line { max } => {
            let max = usize::try_from(max).expect("fits a usize");
            bytes.len() >= max || bytes[..max.min(bytes.len())].iter().any(|&byte| byte == b'\r' || byte == b'\n')
        }
    }
}

/// Whether `bytes` are exactly what `read` asks for, as an intake meets it.
fn delivers(read: Read, bytes: &[u8]) -> bool {
    match read {
        Read::Nothing => false,
        Read::Fill(n) => bytes.len() == usize::try_from(n).expect("fits a usize"),
        Read::Scan { until, max } => {
            let delimiter = until.as_bytes();
            let first = bytes.windows(delimiter.len()).position(|window| window == delimiter);
            match first {
                Some(at) => at + delimiter.len() == bytes.len(),
                None => bytes.len() == usize::try_from(max).expect("fits a usize"),
            }
        }
        Read::Line { max } => match bytes.iter().position(|&byte| byte == b'\r' || byte == b'\n') {
            Some(at) => at + 1 == bytes.len(),
            None => bytes.len() == usize::try_from(max).expect("fits a usize"),
        },
    }
}

struct World<'a> {
    rng: Rng,
    settings: &'a Settings,
    env: Env<Limits>,
    server: Server,
    events: Queue<Event>,
    requests: Queue<Down>,
    below: Below<'a>,
    above: Above<'a>,
    fell: Fell,
    iteration: u64,
}

/// The client's stream.
struct Below<'a> {
    client: &'a [u8],
    /// How many of the client's bytes arrived so far.
    arrived: usize,
    /// How many of them went up to the server.
    delivered: usize,
    /// The requests in the client's bytes, as the reference reads them.
    requests: Vec<Bounds>,
    intake: Intake,
    /// The server's demand outstanding: its read, and its room.
    demand: Option<(Read, u32)>,
    /// The room the server holds: what the last `Room` granted, less what
    /// it sent since (io.md, 3.3).
    granted: u32,
    /// The `Send`s since the last `Room`.
    sends: u32,
    life: Life,
    failed: Option<Fault>,
    /// The demand the server withdrew, which an answer on its way may still
    /// meet.
    withdrawn: Option<(Read, u32)>,
    /// The last request whose 100 (Continue) came, by index.
    continued: Option<usize>,
    /// The last request whose final response's head came, by index.
    responded: Option<usize>,
    /// Since when the client has waited for a 100, and for which request.
    waiting_since: Option<(usize, u64)>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Life {
    Open,
    /// The end is on its way, decided with nothing demanded.
    Ending,
    /// The end arrived: room may still be granted, nothing read.
    Ended,
    /// The stream failed: nothing more.
    Failed,
}

/// The side above.
struct Above<'a> {
    plans: &'a [Plan],
    seen: Vec<Seen>,
    /// A `Next` outstanding.
    asked: bool,
    /// The exchange in progress, once a call came.
    exchange: Option<Exchange>,
    /// What the server waited for when the close went down.
    closing: Option<Waiting>,
    closed: bool,
    /// The connection is not to be used again.
    spent: bool,
}

/// The side above's side of an exchange.
#[expect(clippy::struct_excessive_bools, reason = "what the side above knows of the exchange, a flag each")]
struct Exchange {
    plan: Plan,
    /// The request's bounds in the client's bytes, as the reference reads
    /// them, and whether it reads it whole.
    bounds: Bounds,
    whole: bool,
    /// Whether the request asks to keep the connection, as the reference
    /// reads it.
    persist: bool,
    method: Method,
    version: Version,
    body: Face,
    /// How many pieces of the body went up.
    pieces: u32,
    responded: bool,
    reply: Reply,
    /// How much of the reply went down.
    written: usize,
    terminal: bool,
}

/// The side above's side of the request body's stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Face {
    Idle,
    Demanded(Read),
    /// Withdrawn: the side above reads no more, and discards the rest next.
    Withdrawn,
    /// Over: its end or failure came, or it was discarded.
    Over,
}

/// The side above's side of the reply's stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reply {
    /// No response yet, or one with no body.
    None,
    Idle,
    Demanded(u32),
    Granted(u32),
    Over,
}

impl World<'_> {
    /// What the side below may still deliver once the server is closed: an
    /// answer on its way for the demand the close withdrew, then the end.
    fn late(&mut self) {
        if let Some((read, room)) = self.below.withdrawn.take()
            && self.rng.chance(500)
        {
            if let Some(bytes) = self.below.intake.meet(read) {
                self.fell.late_answer = true;
                self.up(Up::Bytes(bytes));
            } else if room > 0 {
                self.fell.late_answer = true;
                self.up(Up::Room);
            }
        }
        if self.below.life == Life::Open && self.rng.chance(500) {
            self.end();
        }
        if self.below.failed.is_none() && self.rng.chance(300) {
            let fault = fault(&mut self.rng);
            self.fail(fault);
        }
    }

    /// What the `Next` outstanding, or the exchange in progress, saw.
    fn current(&mut self) -> Option<&mut Seen> {
        let open = self.above.asked || self.above.exchange.as_ref().is_some_and(|exchange| !exchange.terminal);
        if open { self.above.seen.last_mut() } else { None }
    }

    /// The stream ends: a read outstanding is never answered; room may be.
    fn end(&mut self) {
        self.below.life = Life::Ended;
        if let Some((_, 0)) = self.below.demand {
            self.below.demand = None;
        }
        if let Some(seen) = self.current() {
            seen.below_over = true;
        }
        self.up(Up::End);
    }

    fn fail(&mut self, fault: Fault) {
        self.fell.failed_after_end = self.below.life == Life::Ended;
        self.fell.failed_while = Some(self.server.waiting());
        self.below.life = Life::Failed;
        self.below.demand = None;
        self.below.failed = Some(fault);
        if let Some(seen) = self.current() {
            seen.below_over = true;
        }
        self.up(Up::Failed(fault));
    }

    /// How many of the client's bytes it has sent by now: all of them,
    /// unless it is patient, and sends each request once the last
    /// exchange is over; and, of a request whose client waits for a 100
    /// (Continue), its head, then its body once the 100 came or the client
    /// tired of waiting, and nothing more once a final response came first.
    fn may_arrive(&mut self) -> usize {
        let over = self.above.exchange.as_ref().is_none_or(|exchange| exchange.terminal);
        let mut limit = self.below.client.len();
        if self.settings.patient {
            let index =
                if over && !self.above.asked { self.above.seen.len() } else { self.above.seen.len().saturating_sub(1) };
            if let Some(bounds) = self.below.requests.get(index) {
                limit = bounds.end;
            }
        }
        // The request the client is sending now: the first not all sent.
        let Some(index) = self.below.requests.iter().position(|bounds| bounds.end > self.below.arrived) else {
            return limit;
        };
        let bounds = self.below.requests[index];
        if !bounds.expects || self.below.continued == Some(index) {
            return limit;
        }
        if self.below.responded == Some(index) {
            return limit.min(bounds.head_end);
        }
        if self.below.arrived < bounds.head_end {
            return limit.min(bounds.head_end);
        }
        let since = match self.below.waiting_since {
            Some((waiting, since)) if waiting == index => since,
            Some(_) | None => {
                self.below.waiting_since = Some((index, self.iteration));
                self.iteration
            }
        };
        if self.iteration.saturating_sub(since) < self.settings.patience {
            return limit.min(bounds.head_end);
        }
        if self.below.arrived == bounds.head_end {
            self.fell.tired = true;
        }
        limit
    }

    fn below_acts(&mut self) {
        let may_arrive = self.may_arrive().saturating_sub(self.below.arrived);
        let below = &mut self.below;
        let unsent = below.client.len() - below.arrived;
        if may_arrive > 0 && unsent > 0 && below.intake.room() > 0 && self.rng.chance(self.settings.arrival) {
            let piece = usize::try_from(self.rng.between(1, u64::from(self.settings.piece))).expect("fits a usize");
            let room = usize::try_from(below.intake.room()).expect("fits a usize");
            let len = piece.min(room).min(unsent).min(may_arrive);
            let arrived = &below.client[below.arrived..below.arrived + len];
            below.intake.append(arrived).expect("within the room");
            below.arrived += len;
            let over = self.above.exchange.as_ref().is_none_or(|exchange| exchange.terminal);
            if let Some(exchange) = &self.above.exchange
                && !over
                && below.arrived > exchange.bounds.end
            {
                self.fell.pipelined = true;
            }
        }
        if let Some((at, fault)) = self.settings.failure
            && self.iteration >= at
            && self.below.failed.is_none()
        {
            self.fail(fault);
            return;
        }
        match self.below.life {
            Life::Failed => return,
            Life::Ending => {
                self.end();
                return;
            }
            Life::Open | Life::Ended => {}
        }
        let ended = self.below.life == Life::Ended;
        let nothing_left = self.below.arrived == self.below.client.len() && self.below.intake.is_empty();
        let below = &mut self.below;
        match below.demand {
            Some((read, room)) => {
                let met = if ended { None } else { below.intake.meet(read) };
                if let Some(bytes) = met {
                    assert!(delivers(read, &bytes), "a delivery is exactly the demand");
                    below.demand = None;
                    below.delivered += bytes.len();
                    self.up(Up::Bytes(bytes));
                } else if room > 0 && self.rng.chance(self.settings.grant) {
                    below.demand = None;
                    below.granted = room;
                    below.sends = 0;
                    self.fell.room_after_end |= ended;
                    self.up(Up::Room);
                } else if !ended && nothing_left && self.rng.chance(self.settings.idle_end) {
                    self.end();
                } else if !ended && room == 0 && read != Read::Nothing && self.below.arrived == self.below.client.len()
                {
                    // All the client sends is here, and none meets the read.
                    self.end();
                }
            }
            None => {
                if !ended && nothing_left && self.rng.chance(self.settings.idle_end) {
                    self.fell.idle_end = true;
                    if self.rng.chance(500) {
                        self.below.life = Life::Ending;
                    } else {
                        self.end();
                    }
                }
            }
        }
    }

    fn is_stalled(&self) -> bool {
        match self.settings.stall {
            Some((from, iterations)) => (from..from + iterations).contains(&self.iteration),
            None => false,
        }
    }

    fn above_acts(&mut self) {
        if self.is_stalled() || self.above.closing.is_some() {
            return;
        }
        let closes_now = match self.settings.close {
            Some(at) => self.iteration >= at,
            None => false,
        };
        let over = self.above.exchange.as_ref().is_none_or(|exchange| exchange.terminal);
        let last = over && !self.above.asked && (self.above.spent || self.above.seen.len() >= self.above.plans.len());
        if closes_now || (last && self.rng.chance(self.settings.eagerness)) {
            self.above.closing = Some(self.server.waiting());
            self.down(Request::Close);
            return;
        }
        if !self.rng.chance(self.settings.eagerness) {
            return;
        }
        if over {
            if !self.above.asked && !self.above.spent && self.above.seen.len() < self.above.plans.len() {
                self.above.asked = true;
                let start = match self.above.seen.last() {
                    Some(last) => match self.below.requests.iter().find(|bounds| bounds.start == last.start) {
                        Some(bounds) => bounds.end,
                        None => self.below.client.len(),
                    },
                    None => 0,
                };
                self.fell.reused |= !self.above.seen.is_empty();
                self.above.seen.push(Seen {
                    call: None,
                    start,
                    body: Vec::new(),
                    ended: None,
                    body_failed: None,
                    reply_failed: None,
                    refused: Vec::new(),
                    response: None,
                    pieces: Vec::new(),
                    finished: false,
                    discarded: false,
                    sent: Vec::new(),
                    outcome: None,
                    below_over: false,
                    whole_below: false,
                });
                self.down(Request::Next);
            }
            return;
        }
        self.exchange_acts();
    }

    /// The side above's next move in the exchange in progress: by its plan,
    /// read, discard, withdraw, respond, then write the reply.
    fn exchange_acts(&mut self) {
        let exchange = self.above.exchange.as_mut().expect("an exchange in progress");
        let plan_when = exchange.plan.when;
        if !exchange.responded {
            let ready = match plan_when {
                When::First => true,
                When::Partway(after) => exchange.pieces >= after || exchange.body == Face::Over,
                When::AfterBody | When::Discarding => exchange.body == Face::Over,
            };
            if ready {
                self.respond();
                return;
            }
            match (plan_when, exchange.body) {
                (When::Discarding, Face::Idle | Face::Demanded(_)) | (_, Face::Withdrawn) => self.discard(),
                (_, Face::Idle) => self.demand_body(),
                (_, Face::Demanded(_)) => self.maybe_withdraw(),
                (_, Face::Over) => {}
            }
            return;
        }
        // Responded: the body, if still open, is read to its end or
        // discarded; the reply is written.
        match exchange.body {
            Face::Idle => self.demand_body(),
            Face::Withdrawn => self.discard(),
            Face::Demanded(_) => self.maybe_withdraw(),
            Face::Over => {}
        }
        self.reply_acts();
    }

    fn demand_body(&mut self) {
        let read = self.draw_read();
        self.above.exchange.as_mut().expect("an exchange").body = Face::Demanded(read);
        self.down(Request::Body(Down::Demand { read, room: 0 }));
    }

    fn maybe_withdraw(&mut self) {
        if self.rng.chance(self.settings.withdraw) {
            self.fell.withdrew = true;
            self.above.exchange.as_mut().expect("an exchange").body = Face::Withdrawn;
            self.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 }));
        }
    }

    fn discard(&mut self) {
        self.above.exchange.as_mut().expect("an exchange").body = Face::Over;
        if let Some(seen) = self.current() {
            seen.discarded = true;
        }
        self.down(Request::Discard);
    }

    /// The side above responds: with its plan's response, or, once that was
    /// refused, with one the server takes.
    fn respond(&mut self) {
        let exchange = self.above.exchange.as_ref().expect("an exchange");
        let refused = self.above.seen.last().is_some_and(|seen| !seen.refused.is_empty());
        let response = if refused { fallback() } else { exchange.plan.response.clone() };
        // Would the head keep the connection, as the world reckons it: the
        // request asks to, the response does not close, its body is not sent
        // to the end of the stream, nothing ended, and the body is all
        // read below, or the side above discards it.
        let whole_below = exchange.whole && self.below.delivered >= exchange.bounds.end;
        let until_end = requests::sends_body(exchange.method, &response)
            && response.body == Body::Chunked
            && exchange.version == Version::Http10;
        let seen_over = self.above.seen.last().is_some_and(|seen| seen.below_over);
        let discarding = self.above.seen.last().is_some_and(|seen| seen.discarded);
        let persist = exchange.persist && !response.close && !until_end && !seen_over && (whole_below || discarding);
        let refusal = requests::refusal(&response, exchange.version, persist, &self.settings.limits);
        self.fell.head_waited |= discarding && !whole_below && persist;
        let seen = self.above.seen.last_mut().expect("the exchange's");
        seen.whole_below = whole_below;
        seen.response = Some((response.clone(), persist));
        self.above.exchange.as_mut().expect("an exchange").responded = refusal.is_none();
        let sends = requests::sends_body(exchange_method(&self.above), &response);
        let refusals = self.above.seen.last().map_or(0, |seen| seen.refused.len());
        self.down(Request::Respond(response));
        let seen = self.above.seen.last_mut().expect("the exchange's");
        if seen.refused.len() > refusals {
            let (_, refused) = seen.refused.last().expect("just refused");
            assert_eq!(Some(*refused), refusal, "the refusal is the reference's");
            seen.response = None;
            return;
        }
        assert!(refusal.is_none(), "a response the reference refuses: {refusal:?}");
        if let Some(exchange) = self.above.exchange.as_mut()
            && !exchange.terminal
        {
            exchange.reply = if sends { Reply::Idle } else { Reply::None };
        }
    }

    fn reply_acts(&mut self) {
        let exchange = self.above.exchange.as_mut().expect("an exchange");
        let left = exchange.plan.reply.len() - exchange.written;
        let sends_room = self.settings.limits.send;
        match exchange.reply {
            Reply::Idle if left == 0 => {
                exchange.reply = Reply::Over;
                if let Some(seen) = self.current() {
                    seen.finished = true;
                }
                self.down(Request::Reply(Down::Finish));
            }
            Reply::Idle => {
                let most = u64::from(sends_room).min(left as u64);
                let room = u32::try_from(self.rng.between(1, most)).expect("fits a u32");
                exchange.reply = Reply::Demanded(room);
                self.down(Request::Reply(Down::Demand { read: Read::Nothing, room }));
            }
            Reply::Granted(room) => {
                let most = usize::try_from(room).expect("fits a usize").min(left);
                let length = usize::try_from(self.rng.between(0, most as u64)).expect("fits a usize");
                let piece = exchange.plan.reply[exchange.written..exchange.written + length].to_vec();
                exchange.written += length;
                exchange.reply = Reply::Idle;
                if let Some(seen) = self.current() {
                    seen.pieces.push(piece.clone());
                }
                self.down(Request::Reply(Down::Send(piece.into())));
            }
            Reply::None | Reply::Demanded(_) | Reply::Over => {}
        }
    }

    fn draw_read(&mut self) -> Read {
        let most = self.settings.limits.read;
        match self.settings.reads {
            Reads::Bytes => Read::Fill(1),
            Reads::Any => {
                let n = draw(&mut self.rng, 1, most);
                match self.rng.below(5) {
                    0 => Read::Fill(n),
                    1 => Read::Scan { until: Delimiter::LF, max: n },
                    2 if n >= 2 => Read::Scan { until: Delimiter::CRLF, max: n },
                    3 => Read::Line { max: n },
                    _ => Read::Scan { until: Delimiter::new(b"\"").expect("one byte"), max: n },
                }
            }
        }
    }

    fn up(&mut self, ev: Up) {
        server::up(&mut self.server, &self.env, ev, &mut self.events, &mut self.requests);
        self.route(server::UP_MAX_OUT, Stopping::No);
    }

    fn down(&mut self, rq: Request) {
        let stopping = match rq {
            Request::Close => Stopping::Closing,
            Request::Respond(_) | Request::Discard => Stopping::Maybe,
            Request::Next | Request::Body(_) | Request::Reply(_) => Stopping::No,
        };
        server::down(&mut self.server, &self.env, rq, &mut self.events, &mut self.requests);
        self.route(server::DOWN_MAX_OUT, stopping);
    }

    /// What one call emitted, to each side, checked.
    fn route(&mut self, max: MaxOut, stopping: Stopping) {
        assert!(self.events.len() <= max.above, "MAX_OUT honoured above: {:?}", self.events);
        assert!(self.requests.len() <= max.below, "MAX_OUT honoured below: {:?}", self.requests);
        let mut ending = false;
        while let Some(event) = self.events.pop() {
            ending |= matches!(event, Event::Done(_) | Event::Failed(_) | Event::Ended | Event::Closed);
            self.receive(event);
        }
        while let Some(request) = self.requests.pop() {
            self.send(request, stopping, ending);
        }
        self.check_waiting();
    }

    /// An event for the side above, checked against its contract.
    fn receive(&mut self, event: Event) {
        assert!(!self.above.closed, "nothing follows Closed: {event:?}");
        match event {
            Event::Call(call) => {
                assert!(self.above.asked, "a call answers a Next");
                self.above.asked = false;
                let index = self.above.seen.len() - 1;
                let plan = self.above.plans[index].clone();
                let seen = self.above.seen.last_mut().expect("the Next's");
                let start = seen.start;
                let read = reference::request(&self.below.client[start..], &self.settings.limits);
                let bounds =
                    Bounds { start, head_end: start + read.head_end, end: start + read.used, expects: read.expects };
                self.above.exchange = Some(Exchange {
                    plan,
                    bounds,
                    whole: read.ending == RequestEnding::Whole,
                    persist: read.persist,
                    method: call.method,
                    version: call.version,
                    body: Face::Idle,
                    pieces: 0,
                    responded: false,
                    reply: Reply::None,
                    written: 0,
                    terminal: false,
                });
                seen.call = Some(call);
            }
            Event::Ended => {
                assert!(self.above.asked, "Ended answers a Next");
                self.answered(Outcome::Ended);
            }
            Event::Failed(error) if self.above.asked => self.answered(Outcome::Failed(error)),
            Event::Body(Up::Bytes(bytes)) => {
                let exchange = self.above.exchange.as_mut().expect("bytes for an exchange");
                let Face::Demanded(read) = exchange.body else { panic!("bytes for a body demand outstanding") };
                assert!(delivers(read, &bytes), "exactly what the demand reads: {read:?}, {}", bytes.escape_ascii());
                exchange.body = Face::Idle;
                exchange.pieces += 1;
                self.current().expect("the exchange in progress").body.extend_from_slice(&bytes);
            }
            Event::Body(Up::End) => {
                let exchange = self.above.exchange.as_mut().expect("an end for an exchange");
                let Face::Demanded(read) = exchange.body else { panic!("the end answers a body demand") };
                exchange.body = Face::Over;
                self.current().expect("the exchange in progress").ended = Some(read);
            }
            Event::Body(Up::Failed(fault)) => {
                let exchange = self.above.exchange.as_mut().expect("a failure for an exchange");
                assert!(
                    matches!(exchange.body, Face::Idle | Face::Demanded(_)),
                    "nothing on a body over or withdrawn: {:?}",
                    exchange.body
                );
                exchange.body = Face::Over;
                if fault == Fault::Other && !exchange.terminal && exchange.responded {
                    self.fell.gave_up = true;
                }
                self.current().expect("the exchange in progress").body_failed = Some(fault);
            }
            Event::Body(Up::Room) => panic!("the request body is read"),
            Event::Reply(Up::Room) => {
                let exchange = self.above.exchange.as_mut().expect("room for an exchange");
                let Reply::Demanded(room) = exchange.reply else { panic!("room for a reply demand outstanding") };
                exchange.reply = Reply::Granted(room);
            }
            Event::Reply(Up::Failed(fault)) => {
                let exchange = self.above.exchange.as_mut().expect("a failure for an exchange");
                assert!(
                    matches!(exchange.reply, Reply::Idle | Reply::Demanded(_) | Reply::Granted(_)),
                    "nothing on a reply over: {:?}",
                    exchange.reply
                );
                exchange.reply = Reply::Over;
                self.current().expect("the exchange in progress").reply_failed = Some(fault);
            }
            Event::Reply(other @ (Up::Bytes(_) | Up::End)) => panic!("the reply is written: {other:?}"),
            Event::Refused(refusal) => {
                let seen = self.current().expect("a refusal in an exchange");
                let response = match &seen.response {
                    Some((response, _)) => response.clone(),
                    None => panic!("a refusal answers a Respond"),
                };
                seen.refused.push((response, refusal));
            }
            Event::Done(reuse) => self.terminal(Outcome::Done(reuse)),
            Event::Failed(error) => self.terminal(Outcome::Failed(error)),
            Event::Closed => {
                assert!(self.above.closing.is_some(), "Closed answers a Close");
                self.above.closed = true;
            }
        }
    }

    /// A `Next` answered by no call: the connection is not to be used
    /// again.
    fn answered(&mut self, outcome: Outcome) {
        self.above.asked = false;
        self.above.spent = true;
        self.above.seen.last_mut().expect("the Next's").outcome = Some(outcome);
    }

    fn terminal(&mut self, outcome: Outcome) {
        let exchange = self.above.exchange.as_mut().expect("a terminal event ends an exchange");
        assert!(!exchange.terminal, "one terminal event per call");
        // Each of the exchange's streams the side above still reads or
        // writes heard its end first.
        assert!(
            matches!(exchange.body, Face::Over | Face::Withdrawn),
            "the body's stream is over, or withdrawn, before {outcome:?}: {:?}",
            exchange.body
        );
        assert!(
            matches!(exchange.reply, Reply::None | Reply::Over),
            "the reply's stream is over before {outcome:?}: {:?}",
            exchange.reply
        );
        exchange.terminal = true;
        exchange.body = Face::Over;
        let seen = self.above.seen.last_mut().expect("the exchange's");
        seen.outcome = Some(outcome);
        self.above.spent = !matches!(outcome, Outcome::Done(Reuse::Keep));
    }

    /// A request for the side below, checked against its contract.
    fn send(&mut self, request: Down, stopping: Stopping, ending: bool) {
        let below = &mut self.below;
        match request {
            Down::Demand { read: Read::Nothing, room: 0 } => {
                assert!(
                    stopping != Stopping::No || ending,
                    "a withdrawal only as the server stops reading: closes, ends an exchange, or gives up a body"
                );
                let withdrawn = below.demand.take().expect("only a demand outstanding is withdrawn");
                if stopping == Stopping::Maybe {
                    self.fell.gave_up = true;
                }
                below.withdrawn = Some(withdrawn);
            }
            Down::Demand { read, room } => {
                assert!(below.demand.is_none(), "one demand at a time: {read:?} over {:?}", below.demand);
                assert!(read == Read::Nothing || room == 0, "a read or room, never both");
                self.fell.crossed_end |= below.life == Life::Ending;
                if read != Read::Nothing {
                    assert!(
                        below.life == Life::Open || below.life == Life::Ending,
                        "nothing read after the end or a failure"
                    );
                }
                assert!(below.life != Life::Failed, "nothing demanded after a failure");
                let wanted = match read {
                    Read::Nothing => 0,
                    Read::Fill(n) => n,
                    Read::Scan { max, .. } | Read::Line { max } => max,
                };
                assert!(wanted <= server::largest_read(&self.settings.limits), "no read past the largest declared");
                assert!(wanted <= below.intake.capacity(), "no read past the cap below");
                assert!(room <= server::largest_room(&self.settings.limits), "no room past the largest declared");
                assert!(room <= self.settings.output, "no room past the output cap");
                // A read of a body whose client waits for a 100 goes after the
                // 100, or after a final response.
                if read != Read::Nothing
                    && !self.above.asked
                    && let Some(exchange) = &self.above.exchange
                    && !exchange.terminal
                    && exchange.bounds.expects
                {
                    let index = self.above.seen.len() - 1;
                    assert!(
                        below.continued == Some(index) || below.responded == Some(index),
                        "a 100 (Continue) before the body of a client that waits for it is read"
                    );
                }
                below.demand = Some((read, room));
            }
            Down::Send(bytes) => {
                assert!(below.life != Life::Failed, "nothing sent after a failure");
                let len = u32::try_from(bytes.len()).expect("fits a u32");
                below.granted = below.granted.checked_sub(len).expect("a Send within the room granted");
                below.sends += 1;
                assert!(below.sends <= 1, "one Send a grant");
                // What the client learns: a 100 lets it send a body it holds
                // back; a final response first, never.
                let index = self.above.seen.len() - 1;
                if &*bytes == CONTINUE {
                    self.fell.continued = true;
                    below.continued = Some(index);
                } else if below.responded != Some(index) {
                    below.responded = Some(index);
                }
                let seen = self.above.seen.last_mut().expect("a Send for a request");
                seen.sent.extend_from_slice(&bytes);
            }
            Down::Finish => panic!("the server never finishes the stream"),
        }
    }

    /// The server waits for exactly what its neighbours see.
    fn check_waiting(&self) {
        let waiting = self.server.waiting();
        let over = self.above.exchange.as_ref().is_none_or(|exchange| exchange.terminal);
        let seen = if self.above.closed {
            Waiting::Nothing
        } else if self.above.spent && over && !self.above.asked {
            Waiting::Close
        } else if over && !self.above.asked {
            Waiting::Next
        } else {
            match self.below.demand {
                Some((_, room)) if room > 0 => Waiting::Room,
                Some(_) if self.above.asked => Waiting::Request,
                Some(_) => Waiting::Body,
                None => Waiting::Above,
            }
        };
        assert_eq!(waiting, seen, "the server waits for what its neighbours see");
    }
}

/// The method of the call in progress.
fn exchange_method(above: &Above<'_>) -> Method {
    above.exchange.as_ref().expect("an exchange").method
}

/// Whether a call may stop the server reading: a close always does; a
/// response or a discard may give the body up; nothing else does, unless
/// it ends an exchange.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stopping {
    No,
    Maybe,
    Closing,
}

/// The rejections a sweep expects to fall, each by its name.
#[must_use]
pub fn rejections() -> [Rejection; 11] {
    [
        Rejection::RequestLine,
        Rejection::TargetTooLong,
        Rejection::Version,
        Rejection::Method,
        Rejection::Header,
        Rejection::HeadTooLong,
        Rejection::TooManyHeaders,
        Rejection::Host,
        Rejection::Framing,
        Rejection::Coding,
        Rejection::BodyTooLong,
    ]
}
