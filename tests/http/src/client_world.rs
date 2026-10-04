//! The client's machine world (testing-strategy.md, 2.4): one client, from
//! a seed, between its two neighbours, both played by the world in one
//! loop, for one exchange after another on one connection.
//!
//! - **The side below** is the server's stream: its bytes arrive in pieces
//!   cut at random, late, into an intake under its cap, and meet each read
//!   exactly; room is granted late, one `Send` a grant; the stream ends
//!   when the server's bytes run out, early when the settings cut them,
//!   idle or with a read on its way; or it fails, before its end or after
//!   it.
//! - **The side above** makes the calls one at a time, uploads each body
//!   in pieces within the room it is granted, reads the response body with
//!   demands of every shape, slowly, withdraws one now and then, discards
//!   the rest now and then, stops for a while, and closes: after the last
//!   exchange, or at a moment the settings draw, whatever the client is
//!   doing.
//!
//! The world checks both of the client's streams as it goes
//! (testing-strategy.md, 6): `MAX_OUT` on each call; below, one demand at a
//! time, none past the caps, none once the stream ended or failed, one
//! withdrawn only as the client stops reading, and each `Send` within the
//! room granted; above, each answer for a demand and exactly what it
//! reads, `End` and `Failed` once and nothing after, one terminal event
//! per call, `Closed` once and last; and the client waiting for exactly
//! what its neighbours see. Each exchange is checked against the reference
//! reader by [`check`].

use skein_http::MaxOut;
use skein_http::client::{self, Call, Client, Error, Event, Limits, Request, Response, Reuse, Waiting};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue, Rng, Time, Wall};

use crate::generate;
use crate::reference::{self, Outcome};

/// How a world runs: the client's limits, and how its neighbours behave.
#[derive(Clone, Debug)]
pub struct Settings {
    pub limits: Limits,
    /// The side below's intake cap: at least the client's largest read.
    pub cap: u32,
    /// The side below's output cap: at least the client's largest room.
    pub output: u32,
    /// The longest piece the server's bytes arrive in.
    pub piece: u32,
    /// Per mille: how likely a piece arrives in an iteration.
    pub arrival: u32,
    /// Per mille: how likely room demanded is granted in an iteration.
    pub grant: u32,
    /// Per mille: how likely the side above acts in an iteration where it
    /// may.
    pub eagerness: u32,
    /// Per mille: how likely the side below ends the stream in an iteration
    /// where nothing is held and nothing is left to send.
    pub idle_end: u32,
    /// How the side above reads the body.
    pub reads: Reads,
    /// Per mille, per demand: how likely the side above withdraws a body
    /// demand rather than wait for it.
    pub withdraw: u32,
    /// Per mille, per exchange: how likely the side above discards the
    /// rest of a body, once it has read some.
    pub discard: u32,
    /// Per mille, per answer on the body: how likely the side above had
    /// withdrawn its demand, or discarded the rest, before the answer
    /// reached it, so that the two cross (lib.md, 7).
    pub cross: u32,
    /// Where the server's bytes stop, if before their end.
    pub cut: Option<usize>,
    /// When the stream fails, if it does: once, at the first iteration from
    /// this one on, with this fault.
    pub failure: Option<(u64, Fault)>,
    /// When the side above closes, if it does whatever the client is doing.
    pub close: Option<u64>,
    /// When the side above stops acting, and for how many iterations.
    pub stall: Option<(u64, u64)>,
    /// Whether the server answers only once it has the whole request of
    /// the exchange in progress: a client that waited for the response
    /// before it took the body would wait for ever.
    pub patient: bool,
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
            cap: client::largest_read(&limits) + draw(rng, 0, 64),
            output: client::largest_room(&limits) + draw(rng, 0, 64),
            piece: draw(rng, 1, 96),
            arrival: draw(rng, 100, 1000),
            grant: draw(rng, 100, 1000),
            eagerness: draw(rng, 100, 1000),
            idle_end: draw(rng, 0, 1000),
            reads: if rng.chance(300) { Reads::Bytes } else { Reads::Any },
            withdraw: if rng.chance(300) { draw(rng, 0, 200) } else { 0 },
            discard: if rng.chance(200) { draw(rng, 0, 1000) } else { 0 },
            cross: if rng.chance(200) { draw(rng, 0, 300) } else { 0 },
            cut: None,
            failure: None,
            close: None,
            stall: None,
            patient: rng.chance(200),
        }
    }

    /// Neighbours as [`calm`](Settings::calm), and sometimes a stream that
    /// ends early or fails, a close at any moment, or a side above that
    /// stops for a while, for a server's `len` bytes.
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

    /// The bytes the server sends of `server`.
    #[must_use]
    pub fn sent<'a>(&self, server: &'a [u8]) -> &'a [u8] {
        match self.cut {
            Some(cut) => &server[..cut.min(server.len())],
            None => server,
        }
    }
}

/// Limits drawn at random: mostly roomy enough for a generated exchange,
/// and now and then tiny, so that a sweep reaches each of them.
#[must_use]
pub fn limits(rng: &mut Rng) -> Limits {
    if rng.chance(250) {
        return Limits {
            request: draw(rng, 64, 512),
            head: draw(rng, 2, 160),
            headers: draw(rng, 0, 6),
            read: draw(rng, 1, 16),
            send: draw(rng, 1, 16),
        };
    }
    Limits {
        request: draw(rng, 512, 2048),
        head: draw(rng, 512, 4096),
        headers: draw(rng, 16, 64),
        read: draw(rng, 4, 256),
        send: draw(rng, 1, 256),
    }
}

fn fault(rng: &mut Rng) -> Fault {
    [Fault::Reset, Fault::Invalid, Fault::Other][usize::try_from(rng.below(3)).expect("fits a usize")]
}

fn draw(rng: &mut Rng, low: u32, high: u32) -> u32 {
    u32::try_from(rng.between(u64::from(low), u64::from(high))).expect("fits a u32")
}

/// A call the side above makes, and the body it uploads for it.
#[derive(Clone, Debug)]
pub struct Exchange {
    pub call: Call,
    pub upload: Vec<u8>,
}

/// What one exchange came to, as the neighbours saw it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seen {
    /// What the client sent below for it.
    pub sent: Vec<u8>,
    pub response: Option<Response>,
    /// The body's bytes the side above received.
    pub body: Vec<u8>,
    /// The demand the body's `End` answered, if it came.
    pub ended: Option<Read>,
    /// The fault the body's stream failed with, if it did.
    pub body_failed: Option<Fault>,
    /// The fault the upload's stream failed with, if it did.
    pub upload_failed: Option<Fault>,
    /// Whether the side above finished the upload.
    pub finished: bool,
    /// Whether the side above discarded the rest of the body.
    pub discarded: bool,
    /// The call's terminal event: `Done` or `Failed`.
    pub outcome: Option<Outcome>,
    /// Whether the stream ended or failed while it was in progress.
    pub below_over: bool,
}

/// What a run came to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Run {
    pub exchanges: Vec<Seen>,
    /// What the client waited for when the side above closed it.
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
    /// A read crossed the end on its way.
    pub crossed_end: bool,
    /// Room came while bytes could have met the same demand: the upload
    /// went on while a response waited.
    pub room_first: bool,
    /// A line of a response came while the upload waited for room.
    pub early_response: bool,
    /// The side above withdrew a body demand.
    pub withdrew: bool,
    /// A withdrawal crossed its demand's answer, during the body.
    pub crossed_withdrawal: bool,
    /// A withdrawal or a discard crossed the body's end, and was dropped.
    pub crossed_end_of_body: bool,
    /// An answer came after the demand was withdrawn below.
    pub late_answer: bool,
    /// The stream failed after its end.
    pub failed_after_end: bool,
    /// What the client waited for when the stream failed, if it did.
    pub failed_while: Option<Waiting>,
    /// An exchange carried on a connection used before.
    pub reused: bool,
}

/// Runs the world over `exchanges` and `server`, the server's bytes for
/// all of them in order, with `settings`, from `seed`, until the side
/// above closes the client.
#[must_use]
pub fn run(exchanges: &[Exchange], server: &[u8], settings: &Settings, seed: u64) -> Run {
    let limits = settings.limits;
    assert!(settings.cap >= client::largest_read(&limits), "the side below's cap holds the largest read");
    assert!(settings.output >= client::largest_room(&limits), "the side below's output holds the largest room");
    let mut world = World {
        rng: Rng::new(seed),
        settings,
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
        client: Client::new(&limits),
        events: Queue::with_capacity(8),
        requests: Queue::with_capacity(8),
        below: Below {
            unsent: settings.sent(server),
            arrived: 0,
            ends: response_ends(exchanges, settings.sent(server), &limits),
            intake: Intake::with_capacity(settings.cap),
            demand: None,
            granted: None,
            life: Life::Open,
            failed: None,
            withdrawn: None,
        },
        above: Above {
            exchanges,
            next: 0,
            seen: Vec::new(),
            uploaded: 0,
            upload: Face::Idle,
            body: Face::Idle,
            body_open: false,
            terminal: None,
            discard_after: None,
            closing: None,
            closed: false,
            spent: false,
            crossing: None,
        },
        fell: Fell::default(),
        iteration: 0,
    };
    let stalled = match settings.stall {
        Some((_, iterations)) => iterations,
        None => 0,
    };
    let budget = 64 * (server.len() as u64 + 64) + 256 * exchanges.len() as u64 * 8 + stalled + 4096;
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
                exchanges: world.above.seen,
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
/// reference reader's reading of what the server sent for it: the request
/// written, the response's head, the body's bytes, and the outcome.
#[must_use]
pub fn check(exchanges: &[Exchange], server: &[u8], settings: &Settings, seed: u64) -> Run {
    let run = run(exchanges, server, settings, seed);
    let replay = || format!("seed {seed}, {settings:?}, server {}", server.escape_ascii());
    let sent = settings.sent(server);
    let mut offset = 0;
    for (index, seen) in run.exchanges.iter().enumerate() {
        let exchange = &exchanges[index];
        let what = || format!("exchange {index}; {}", replay());
        // The request: its head and its body, whole once finished.
        let mut request = generate::request(&exchange.call);
        request.extend_from_slice(&exchange.upload);
        assert!(
            request.starts_with(&seen.sent),
            "the request written: {:?}; {}",
            seen.sent.escape_ascii().to_string(),
            what()
        );
        if seen.finished && seen.upload_failed.is_none() && seen.outcome.is_some() {
            assert_eq!(seen.sent.len(), request.len(), "the request sent whole; {}", what());
        }
        // The response, as the reference reads what the server sent for it.
        let rest = &sent[offset.min(sent.len())..];
        let expected = reference::response(rest, exchange.call.method, exchange.call.close, &settings.limits);
        if let Some(response) = &seen.response {
            let head = expected.head.as_ref().unwrap_or_else(|| panic!("the reference reads a head; {}", what()));
            assert_eq!(
                (response.version, response.status, response.framing),
                (head.version, head.status, head.framing),
                "{}",
                what()
            );
            let headers: Vec<(Vec<u8>, Vec<u8>)> =
                response.headers.iter().map(|header| (header.name.to_vec(), header.value.to_vec())).collect();
            assert_eq!(headers, head.headers, "the fields; {}", what());
        }
        assert!(expected.body.starts_with(&seen.body), "the body is the reference's, in order; {}", what());
        if let Some(read) = seen.ended {
            let tail = &expected.body[seen.body.len()..];
            assert!(
                !meets(read, tail),
                "the end comes once nothing left meets the demand: {read:?}, {}; {}",
                tail.escape_ascii(),
                what()
            );
            assert!(!seen.discarded, "no end after a discard; {}", what());
        }
        match seen.outcome {
            None => {}
            Some(Outcome::Failed(Error::Stream(fault))) => assert_eq!(run.failed, Some(fault), "{}", what()),
            // Nothing of the request went down: a pool may send it anywhere.
            Some(Outcome::Failed(Error::Closed(fault))) => {
                assert!(seen.sent.is_empty(), "closed before anything was sent; {}", what());
                match fault {
                    Some(fault) => assert_eq!(run.failed, Some(fault), "{}", what()),
                    None => assert!(rest.is_empty(), "closed only with nothing from the server; {}", what()),
                }
            }
            // Generated calls are refused only for a head past the limit,
            // and nothing of them is written.
            Some(Outcome::Failed(Error::Refused(refusal))) => {
                let head = generate::request(&exchange.call).len();
                assert_eq!(refusal, client::Refusal::TooLong, "{}", what());
                assert!(head > usize::try_from(settings.limits.request).expect("fits a usize"), "{}", what());
                assert!(seen.sent.is_empty(), "a refused call writes nothing; {}", what());
                continue;
            }
            Some(Outcome::Done(reuse)) => {
                let Outcome::Done(expected_reuse) = expected.outcome else {
                    panic!("the reference's outcome {:?}; {}", expected.outcome, what())
                };
                // A connection whose upload stopped, or whose stream ended or
                // failed during the exchange, is not used again.
                let lost = seen.upload_failed.is_some() || seen.below_over;
                let expected_reuse = if lost { Reuse::Close } else { expected_reuse };
                assert_eq!(reuse, expected_reuse, "the reuse; {}", what());
                if !seen.discarded && seen.ended.is_some() {
                    assert!(seen.body_failed.is_none(), "{}", what());
                }
            }
            Some(outcome @ Outcome::Failed(_)) => {
                assert_eq!(outcome, expected.outcome, "the reference's outcome; {}", what());
            }
        }
        if seen.outcome == Some(Outcome::Done(Reuse::Keep)) {
            offset += expected.used;
        }
    }
    run
}

/// Where each exchange's response ends in `server`, as the reference reads
/// them one after another; past a response that fails, the rest.
fn response_ends(exchanges: &[Exchange], server: &[u8], limits: &Limits) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut offset = 0;
    for exchange in exchanges {
        let rest = &server[offset.min(server.len())..];
        let read = reference::response(rest, exchange.call.method, exchange.call.close, limits);
        offset = match read.outcome {
            Outcome::Done(Reuse::Keep) => offset + read.used,
            Outcome::Done(Reuse::Close) | Outcome::Failed(_) => server.len(),
        };
        ends.push(offset);
    }
    ends
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
    }
}

struct World<'a> {
    rng: Rng,
    settings: &'a Settings,
    env: Env<Limits>,
    client: Client,
    events: Queue<Event>,
    requests: Queue<Down>,
    below: Below<'a>,
    above: Above<'a>,
    fell: Fell,
    iteration: u64,
}

/// The server's stream.
struct Below<'a> {
    unsent: &'a [u8],
    /// How many of the server's bytes arrived so far.
    arrived: usize,
    /// Where each exchange's response ends in the server's bytes, as the
    /// reference reads them: what a patient server holds back.
    ends: Vec<usize>,
    intake: Intake,
    /// The client's demand outstanding: its read, and its room.
    demand: Option<(Read, u32)>,
    /// Room granted and not yet sent in.
    granted: Option<u32>,
    life: Life,
    failed: Option<Fault>,
    /// The demand the client withdrew, which an answer on its way may still
    /// meet.
    withdrawn: Option<(Read, u32)>,
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

/// The side above's side of one of the client's streams.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Face {
    /// No demand outstanding.
    Idle,
    /// A demand outstanding: room for the upload, a read for the body.
    Demanded(Read, u32),
    /// Room granted: the side above may send this much.
    Granted(u32),
    /// The body's demand withdrawn: the side above reads no more, and
    /// discards the rest next.
    Withdrawn,
    /// The stream ended or failed: nothing more on it.
    Over,
}

/// The side above.
struct Above<'a> {
    exchanges: &'a [Exchange],
    /// The next call to make.
    next: usize,
    seen: Vec<Seen>,
    /// How much of the current exchange's body went down.
    uploaded: usize,
    upload: Face,
    body: Face,
    /// Whether the current exchange's response came, and its body is read.
    body_open: bool,
    /// The current exchange's terminal event, once it came.
    terminal: Option<Outcome>,
    /// After how many body deliveries the side above discards the rest.
    discard_after: Option<u32>,
    /// What the client waited for when the close went down.
    closing: Option<Waiting>,
    closed: bool,
    /// The connection is not to be used again.
    spent: bool,
    /// A request the side above sent before an answer reached it, sent
    /// once the call that answered is routed.
    crossing: Option<Request>,
}

impl World<'_> {
    /// What the side below may still deliver once the client is closed: an
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

    fn current(&mut self) -> Option<&mut Seen> {
        if self.above.terminal.is_some() {
            return None;
        }
        self.above.seen.last_mut()
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
        self.fell.failed_while = Some(self.client.waiting());
        self.below.life = Life::Failed;
        self.below.demand = None;
        self.below.granted = None;
        self.below.failed = Some(fault);
        if let Some(seen) = self.current() {
            seen.below_over = true;
        }
        self.up(Up::Failed(fault));
    }

    /// Whether the server holds its answer: it is patient, and the request
    /// of the exchange in progress is not all sent yet.
    /// How many of the server's bytes may have arrived by now: all of them,
    /// unless the server is patient, which answers each exchange once its
    /// request is whole.
    fn may_arrive(&self) -> usize {
        if !self.settings.patient {
            return usize::MAX;
        }
        let Some(current) = self.above.next.checked_sub(1) else { return 0 };
        let before = match current.checked_sub(1) {
            Some(previous) => self.below.ends[previous],
            None => 0,
        };
        let seen = self.above.seen.last().expect("a call made");
        let exchange = &self.above.exchanges[current];
        let whole = generate::request(&exchange.call).len() + exchange.upload.len();
        if seen.sent.len() < whole && self.above.terminal.is_none() { before } else { self.below.ends[current] }
    }

    fn below_acts(&mut self) {
        let may_arrive = self.may_arrive().saturating_sub(self.below.arrived);
        let below = &mut self.below;
        if may_arrive > 0
            && !below.unsent.is_empty()
            && below.intake.room() > 0
            && self.rng.chance(self.settings.arrival)
        {
            let piece = usize::try_from(self.rng.between(1, u64::from(self.settings.piece))).expect("fits a usize");
            let room = usize::try_from(below.intake.room()).expect("fits a usize");
            let (arrived, rest) = below.unsent.split_at(piece.min(room).min(below.unsent.len()).min(may_arrive));
            below.intake.append(arrived).expect("within the room");
            below.unsent = rest;
            below.arrived += arrived.len();
        }
        if let Some((at, fault)) = self.settings.failure
            && self.iteration >= at
            && below.failed.is_none()
        {
            self.fail(fault);
            return;
        }
        match below.life {
            Life::Failed => return,
            Life::Ending => {
                self.end();
                return;
            }
            Life::Open | Life::Ended => {}
        }
        let ended = below.life == Life::Ended;
        match below.demand {
            Some((read, room)) => {
                let met = if ended { None } else { below.intake.meet(read) };
                if let Some(bytes) = met {
                    assert!(delivers(read, &bytes), "a delivery is exactly the demand");
                    self.fell.early_response |= room > 0;
                    below.demand = None;
                    self.up(Up::Bytes(bytes));
                } else if room > 0 && self.rng.chance(self.settings.grant) {
                    if read != Read::Nothing && !below.intake.is_empty() {
                        self.fell.room_first = true;
                    }
                    below.demand = None;
                    below.granted = Some(room);
                    self.up(Up::Room);
                } else if !ended
                    && below.unsent.is_empty()
                    && below.intake.is_empty()
                    && self.rng.chance(self.settings.idle_end)
                {
                    self.end();
                } else if !ended && below.unsent.is_empty() && read != Read::Nothing && room == 0 {
                    // It can never be met.
                    self.end();
                }
            }
            None => {
                if !ended
                    && below.unsent.is_empty()
                    && below.intake.is_empty()
                    && self.rng.chance(self.settings.idle_end)
                {
                    self.fell.idle_end = true;
                    if self.rng.chance(500) {
                        below.life = Life::Ending;
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
        // The last exchange is over, or the connection is not to be used.
        let last = self.above.terminal.is_some() && (self.above.spent || self.above.next == self.above.exchanges.len());
        let done = last || self.client.waiting() == Waiting::Close;
        if closes_now || (done && self.rng.chance(self.settings.eagerness)) {
            self.above.closing = Some(self.client.waiting());
            self.down(Request::Close);
            return;
        }
        if !self.rng.chance(self.settings.eagerness) {
            return;
        }
        // The next call, once the last is done on a connection kept.
        if self.client.waiting() == Waiting::Call
            && (self.above.next == 0 || self.above.terminal.is_some())
            && self.above.next < self.above.exchanges.len()
        {
            self.call();
            return;
        }
        if self.above.terminal.is_some() || self.above.next == 0 {
            return;
        }
        self.upload_acts();
        self.body_acts();
    }

    fn call(&mut self) {
        let exchange = &self.above.exchanges[self.above.next];
        self.fell.reused |= self.above.next > 0;
        self.above.next += 1;
        self.above.seen.push(Seen {
            sent: Vec::new(),
            response: None,
            body: Vec::new(),
            ended: None,
            body_failed: None,
            upload_failed: None,
            finished: false,
            discarded: false,
            outcome: None,
            below_over: false,
        });
        self.above.terminal = None;
        self.above.uploaded = 0;
        self.above.upload = match exchange.call.body {
            client::Body::Length(_) => Face::Idle,
            client::Body::None => Face::Over,
        };
        self.above.body = Face::Idle;
        self.above.body_open = false;
        self.above.discard_after =
            if self.rng.chance(self.settings.discard) { Some(draw(&mut self.rng, 0, 4)) } else { None };
        self.down(Request::Call(exchange.call.clone()));
    }

    fn upload_acts(&mut self) {
        let exchange = &self.above.exchanges[self.above.next - 1];
        let left = exchange.upload.len() - self.above.uploaded;
        match self.above.upload {
            Face::Idle if left == 0 => {
                self.above.upload = Face::Over;
                if let Some(seen) = self.current() {
                    seen.finished = true;
                }
                self.down(Request::Upload(Down::Finish));
            }
            Face::Idle => {
                let most = self.settings.limits.send.min(u32::try_from(left).expect("fits a u32"));
                let room = draw(&mut self.rng, 1, most);
                self.above.upload = Face::Demanded(Read::Nothing, room);
                self.down(Request::Upload(Down::Demand { read: Read::Nothing, room }));
            }
            Face::Granted(room) => {
                let most = usize::try_from(room).expect("fits a usize").min(left);
                let len = usize::try_from(self.rng.between(0, most as u64)).expect("fits a usize");
                let piece = exchange.upload[self.above.uploaded..self.above.uploaded + len].to_vec();
                self.above.uploaded += len;
                self.above.upload = Face::Idle;
                self.down(Request::Upload(Down::Send(piece.into())));
            }
            Face::Demanded(..) | Face::Withdrawn | Face::Over => {}
        }
    }

    fn body_acts(&mut self) {
        if !self.above.body_open {
            return;
        }
        match self.above.body {
            Face::Idle if self.above.discard_after == Some(0) => self.discard(),
            Face::Idle => {
                let read = self.draw_read();
                self.above.body = Face::Demanded(read, 0);
                self.down(Request::Body(Down::Demand { read, room: 0 }));
            }
            // A withdrawal, as a machine stacked on the body withdraws its
            // demand as it closes; the rest is discarded next.
            Face::Demanded(..) if self.rng.chance(self.settings.withdraw) => {
                self.fell.withdrew = true;
                self.above.body = Face::Withdrawn;
                self.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 }));
            }
            Face::Withdrawn => self.discard(),
            Face::Demanded(..) | Face::Granted(_) | Face::Over => {}
        }
    }

    fn discard(&mut self) {
        self.above.body = Face::Over;
        if let Some(seen) = self.current() {
            seen.discarded = true;
        }
        self.down(Request::Discard);
    }

    fn draw_read(&mut self) -> Read {
        let most = self.settings.limits.read;
        match self.settings.reads {
            Reads::Bytes => Read::Fill(1),
            Reads::Any => {
                let n = draw(&mut self.rng, 1, most);
                match self.rng.below(4) {
                    0 => Read::Fill(n),
                    1 => Read::Scan { until: Delimiter::LF, max: n },
                    2 if n >= 2 => Read::Scan { until: Delimiter::CRLF, max: n },
                    _ => Read::Scan { until: Delimiter::new(b"\"").expect("one byte"), max: n },
                }
            }
        }
    }

    fn up(&mut self, ev: Up) {
        let max = client::UP_MAX_OUT;
        client::up(&mut self.client, &self.env, ev, &mut self.events, &mut self.requests);
        self.route(max, false);
    }

    fn down(&mut self, rq: Request) {
        let closing = rq == Request::Close;
        client::down(&mut self.client, &self.env, rq, &mut self.events, &mut self.requests);
        self.route(client::DOWN_MAX_OUT, closing);
    }

    /// What one call emitted, to each side, checked.
    fn route(&mut self, max: MaxOut, closing: bool) {
        assert!(self.events.len() <= max.above, "MAX_OUT honoured above: {:?}", self.events);
        assert!(self.requests.len() <= max.below, "MAX_OUT honoured below: {:?}", self.requests);
        let mut ending = false;
        while let Some(event) = self.events.pop() {
            ending |= matches!(event, Event::Done(_) | Event::Failed(_) | Event::Closed);
            self.receive(event);
        }
        while let Some(request) = self.requests.pop() {
            self.send(request, closing || ending);
        }
        self.check_waiting();
        if let Some(crossing) = self.above.crossing.take()
            && self.above.closing.is_none()
        {
            self.cross(crossing);
        }
    }

    /// A withdrawal or a discard the side above sent before the answer it
    /// crosses reached it: during the body, it takes effect; after the
    /// body's end, the client drops it.
    fn cross(&mut self, crossing: Request) {
        if self.above.terminal.is_some() {
            self.fell.crossed_end_of_body = true;
        } else {
            match crossing {
                Request::Discard => {
                    self.above.body = Face::Over;
                    self.current().expect("the exchange in progress").discarded = true;
                }
                Request::Body(_) => {
                    self.fell.crossed_withdrawal = true;
                    self.above.body = Face::Withdrawn;
                }
                Request::Call(_) | Request::Upload(_) | Request::Close => {
                    unreachable!("only a withdrawal or a discard")
                }
            }
        }
        self.down(crossing);
    }

    /// Whether the side above had sent a withdrawal or a discard before
    /// the answer now reaching it.
    fn maybe_cross(&mut self) {
        if self.rng.chance(self.settings.cross) {
            self.above.crossing = Some(if self.rng.chance(500) {
                Request::Discard
            } else {
                Request::Body(Down::Demand { read: Read::Nothing, room: 0 })
            });
        }
    }

    /// An event for the side above, checked against its contract.
    fn receive(&mut self, event: Event) {
        assert!(!self.above.closed, "nothing follows Closed: {event:?}");
        match event {
            Event::Response(response) => {
                assert!(self.above.terminal.is_none() && !self.above.body_open, "one response per exchange");
                self.above.body_open = true;
                let seen = self.current().expect("a response for the exchange in progress");
                seen.response = Some(response);
            }
            Event::Upload(Up::Room) => {
                let Face::Demanded(Read::Nothing, room) = self.above.upload else {
                    panic!("room for a room demand outstanding: {:?}", self.above.upload)
                };
                self.above.upload = Face::Granted(room);
            }
            Event::Upload(Up::Failed(fault)) => {
                assert!(self.above.upload != Face::Over, "nothing after the upload is over");
                self.above.upload = Face::Over;
                self.current().expect("the exchange in progress").upload_failed = Some(fault);
            }
            Event::Upload(other @ (Up::Bytes(_) | Up::End)) => panic!("the upload is written: {other:?}"),
            Event::Body(Up::Bytes(bytes)) => {
                let Face::Demanded(read, 0) = self.above.body else { panic!("bytes for a body demand outstanding") };
                assert!(delivers(read, &bytes), "exactly what the demand reads: {read:?}, {}", bytes.escape_ascii());
                self.above.body = Face::Idle;
                if let Some(after) = &mut self.above.discard_after {
                    *after = after.saturating_sub(1);
                }
                self.current().expect("the exchange in progress").body.extend_from_slice(&bytes);
                self.maybe_cross();
            }
            Event::Body(Up::End) => {
                let Face::Demanded(read, 0) = self.above.body else { panic!("the end answers a body demand") };
                self.above.body = Face::Over;
                self.current().expect("the exchange in progress").ended = Some(read);
                self.maybe_cross();
            }
            Event::Body(Up::Failed(fault)) => {
                assert!(
                    matches!(self.above.body, Face::Idle | Face::Demanded(..)) && self.above.body_open,
                    "nothing on a body over, withdrawn or discarded: {:?}",
                    self.above.body
                );
                self.above.body = Face::Over;
                self.current().expect("the exchange in progress").body_failed = Some(fault);
            }
            Event::Body(Up::Room) => panic!("the body is read"),
            Event::Done(reuse) => self.terminal(Outcome::Done(reuse)),
            Event::Failed(error) => self.terminal(Outcome::Failed(error)),
            Event::Closed => {
                assert!(self.above.closing.is_some(), "Closed answers a Close");
                self.above.closed = true;
            }
        }
    }

    fn terminal(&mut self, outcome: Outcome) {
        // Each of the exchange's streams the side above still writes or reads
        // heard its end first (http.md, 3.4): a failure tells a stream not
        // withdrawn or discarded that it failed; `Done` follows the body's
        // end or a discard, and an upload finished or stopped.
        let refused = matches!(outcome, Outcome::Failed(Error::Refused(_)));
        assert!(
            refused || self.above.upload == Face::Over,
            "the upload's stream is over before {outcome:?}: {:?}",
            self.above.upload
        );
        assert!(
            !self.above.body_open || matches!(self.above.body, Face::Over | Face::Withdrawn),
            "the body's stream is over, or withdrawn, before {outcome:?}: {:?}",
            self.above.body
        );
        let seen = self.current().expect("one terminal event per call");
        seen.outcome = Some(outcome);
        self.above.terminal = Some(outcome);
        // A refused call leaves the connection as it was.
        self.above.spent = !matches!(outcome, Outcome::Done(Reuse::Keep) | Outcome::Failed(Error::Refused(_)));
        // Neither stream says more.
        self.above.upload = Face::Over;
        self.above.body = Face::Over;
        self.above.body_open = false;
    }

    /// A request for the side below, checked against its contract.
    fn send(&mut self, request: Down, stopping: bool) {
        let below = &mut self.below;
        match request {
            Down::Demand { read: Read::Nothing, room: 0 } => {
                assert!(stopping, "a withdrawal only as the client stops reading");
                let withdrawn = below.demand.take().expect("only a demand outstanding is withdrawn");
                below.withdrawn = Some(withdrawn);
            }
            Down::Demand { read, room } => {
                assert!(below.demand.is_none(), "one demand at a time: {read:?} over {:?}", below.demand);
                // One stated while the end is on its way crosses it.
                self.fell.crossed_end |= below.life == Life::Ending;
                assert!(
                    below.life == Life::Open || below.life == Life::Ending,
                    "nothing demanded after the end or a failure"
                );
                let wanted = match read {
                    Read::Nothing => 0,
                    Read::Fill(n) => n,
                    Read::Scan { max, .. } => max,
                };
                assert!(wanted <= client::largest_read(&self.settings.limits), "no read past the largest declared");
                assert!(wanted <= below.intake.capacity(), "no read past the cap below");
                assert!(room <= client::largest_room(&self.settings.limits), "no room past the largest declared");
                assert!(room <= self.settings.output, "no room past the output cap");
                assert!(below.granted.is_none(), "room granted is sent in before more is demanded");
                below.demand = Some((read, room));
            }
            Down::Send(bytes) => {
                assert!(below.life != Life::Failed, "nothing sent after a failure");
                let granted = below.granted.take().expect("a Send within room granted");
                assert!(bytes.len() <= usize::try_from(granted).expect("fits a usize"), "a Send within room granted");
                if let Some(seen) = self.above.seen.last_mut() {
                    seen.sent.extend_from_slice(&bytes);
                }
            }
            Down::Finish => panic!("the client never finishes the stream"),
        }
    }

    /// The client waits for exactly what its neighbours see.
    fn check_waiting(&self) {
        let waiting = self.client.waiting();
        let seen = if self.above.closed {
            Waiting::Nothing
        } else if self.above.terminal.is_some() || self.above.next == 0 {
            match waiting {
                Waiting::Call | Waiting::Close => waiting,
                Waiting::Room | Waiting::Response | Waiting::Body | Waiting::Above | Waiting::Nothing => {
                    panic!("between exchanges, the client waits for a call or its close: {waiting:?}")
                }
            }
        } else {
            match self.below.demand {
                Some((_, room)) if room > 0 => Waiting::Room,
                Some(_) if self.above.body_open => Waiting::Body,
                Some(_) => Waiting::Response,
                None => Waiting::Above,
            }
        };
        assert_eq!(waiting, seen, "the client waits for what its neighbours see");
        if !self.above.closed && self.above.terminal.is_some() && !self.above.spent && self.below.life == Life::Open {
            assert_eq!(waiting, Waiting::Call, "a connection kept waits for the next call");
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
    }
}
