//! The event stream reader's machine world (testing-strategy.md, 2.4): one
//! reader, from a seed, between its two neighbours, both played by the
//! world in one loop.
//!
//! - **The side below** is a body: its bytes arrive in pieces cut at
//!   random, late, into an intake under its cap, and meet each demand
//!   exactly; it ends when the bytes run out, early when the settings cut
//!   them, sometimes with nothing demanded and a demand crossing that end
//!   on its way; or it fails, before its end or after it.
//! - **The side above** asks for one event at a time, when it feels like
//!   it, stops asking for a while, and closes: after the outcome, or at a
//!   moment the settings draw, whatever the reader is doing.
//!
//! The world checks the reader's contracts as it goes (testing-strategy.md,
//! 6): `MAX_OUT` on each call; one answer per `Next`, at most one outcome,
//! `Closed` once, last; one demand at a time, none past the largest
//! declared or the cap below, none once the stream ended, one withdrawn
//! only by a close; no room asked for and nothing sent; and the reader
//! waiting for exactly what its neighbours see. [`check`] holds a run to
//! the reference reader.

use skein_http::MaxOut;
use skein_http::sse::{self, Error, Event, Limits, Reader, Request, Waiting};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue, Rng, Time, Wall};

use crate::reference::{self, Ending};

/// How a world runs: the reader's limits, and how its neighbours behave.
#[derive(Clone, Debug)]
pub struct Settings {
    pub limits: Limits,
    /// The side below's cap: at least the reader's largest demand.
    pub cap: u32,
    pub piece: u32,
    /// Per mille: how likely a piece arrives in an iteration.
    pub arrival: u32,
    /// Per mille: how likely the side above asks, or closes after the
    /// outcome, in an iteration where it may.
    pub eagerness: u32,
    /// Per mille: how likely the side below ends the stream in an iteration
    /// where nothing is demanded and nothing is left.
    pub idle_end: u32,
    pub cut: Option<usize>,
    pub failure: Option<(u64, Fault)>,
    pub close: Option<u64>,
    pub stall: Option<(u64, u64)>,
}

impl Settings {
    /// Neighbours that are slow and cut the bytes anywhere, but never end
    /// the stream early, fail it or close before the outcome.
    #[must_use]
    pub fn calm(rng: &mut Rng, limits: Limits) -> Settings {
        Settings {
            limits,
            cap: sse::largest_demand(&limits) + draw(rng, 0, 64),
            piece: draw(rng, 1, 96),
            arrival: draw(rng, 100, 1000),
            eagerness: draw(rng, 100, 1000),
            idle_end: draw(rng, 0, 1000),
            cut: None,
            failure: None,
            close: None,
            stall: None,
        }
    }

    /// Neighbours as [`calm`](Settings::calm), and sometimes a stream that
    /// ends early or fails, a close at any moment, or a side above that
    /// stops asking for a while, for a stream of `len` bytes.
    #[must_use]
    pub fn chaotic(rng: &mut Rng, limits: Limits, len: usize) -> Settings {
        let mut settings = Settings::calm(rng, limits);
        let span = 2 * len as u64 + 16;
        if rng.chance(150) {
            settings.cut = Some(usize::try_from(rng.below(len as u64 + 1)).expect("fits a usize"));
        }
        if rng.chance(150) {
            settings.failure = Some((rng.below(span), fault(rng)));
        }
        if rng.chance(150) {
            settings.close = Some(rng.below(span));
        }
        if rng.chance(200) {
            settings.stall = Some((rng.below(span), rng.between(16, 256)));
        }
        settings
    }

    /// The bytes the body holds of `stream`.
    #[must_use]
    pub fn sent<'a>(&self, stream: &'a [u8]) -> &'a [u8] {
        match self.cut {
            Some(cut) => &stream[..cut.min(stream.len())],
            None => stream,
        }
    }
}

/// Limits drawn at random: mostly roomy enough for a generated stream, and
/// now and then tiny, so that a sweep reaches each of them.
#[must_use]
pub fn limits(rng: &mut Rng) -> Limits {
    if rng.chance(300) {
        return Limits {
            line: draw(rng, 0, 24),
            event: draw(rng, 0, 64),
            field: draw(rng, 0, 6),
            chunk: draw(rng, 1, 8),
        };
    }
    Limits { line: draw(rng, 64, 512), event: draw(rng, 256, 4096), field: draw(rng, 8, 64), chunk: draw(rng, 1, 64) }
}

fn fault(rng: &mut Rng) -> Fault {
    [Fault::Reset, Fault::Invalid, Fault::Other][usize::try_from(rng.below(3)).expect("fits a usize")]
}

fn draw(rng: &mut Rng, low: u32, high: u32) -> u32 {
    u32::try_from(rng.between(u64::from(low), u64::from(high))).expect("fits a u32")
}

/// What a run came to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Run {
    pub events: Vec<reference::Event>,
    /// The outcome the side above received, unless it closed first.
    pub outcome: Option<Ending>,
    pub closed_while: Waiting,
    pub failed: Option<Fault>,
    pub fell: Fell,
    /// The most bytes the stream below held, nothing demanded, while the
    /// side above had stopped asking.
    pub held_back: u32,
    /// The reader's reconnection time and last event ID when it was closed.
    pub retry: Option<u64>,
    pub last_id: Vec<u8>,
    pub iterations: u64,
}

/// What fell in a run, of what its neighbours may inject.
#[expect(clippy::struct_excessive_bools, reason = "a record of what fell, a flag each")]
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Fell {
    pub idle_end: bool,
    pub crossed_end: bool,
    pub late_delivery: bool,
    pub failed_after_end: bool,
    pub failed_while: Option<Waiting>,
    /// An event came from what a delivery held after the last one.
    pub held_rest: bool,
    /// A scan was to CR, after a line that ended with a CR alone.
    pub scanned_to_cr: bool,
}

/// Runs the world over `stream`, a body's bytes, with `settings`, from
/// `seed`, until the side above has closed the reader.
#[must_use]
pub fn run(stream: &[u8], settings: &Settings, seed: u64) -> Run {
    let limits = settings.limits;
    assert!(settings.cap >= sse::largest_demand(&limits), "the side below's cap holds the largest demand");
    let mut world = World {
        rng: Rng::new(seed),
        settings,
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
        reader: Reader::new(&limits),
        events: Queue::with_capacity(8),
        requests: Queue::with_capacity(8),
        below: Below {
            unsent: settings.sent(stream),
            intake: Intake::with_capacity(settings.cap),
            demand: None,
            life: Life::Open,
            failed: None,
            withdrawn: None,
        },
        above: Above { pending: false, events: Vec::new(), outcome: None, closing: None, closed: false },
        fell: Fell::default(),
        iteration: 0,
        held_back: 0,
    };
    let stalled = match settings.stall {
        Some((_, iterations)) => iterations,
        None => 0,
    };
    let budget = 16 * (stream.len() as u64 + 16) + stalled;
    while world.iteration < budget {
        if world.rng.chance(500) {
            world.below_acts();
            world.above_acts();
        } else {
            world.above_acts();
            world.below_acts();
        }
        if world.is_stalled() && world.below.demand.is_none() {
            world.held_back = world.held_back.max(world.below.intake.len());
        }
        world.iteration += 1;
        if world.above.closed {
            world.late();
            return Run {
                events: world.above.events,
                outcome: world.above.outcome,
                closed_while: world.above.closing.expect("closed after a close"),
                failed: world.below.failed,
                fell: world.fell,
                held_back: world.held_back,
                retry: world.reader.retry(),
                last_id: world.reader.last_event_id().to_vec(),
                iterations: world.iteration,
            };
        }
    }
    panic!("seed {seed}: the world settles within {budget} iterations: {settings:?}");
}

/// Runs the world as [`run`], and checks what the side above received
/// against the reference reader's reading of what the body held: the
/// events in order, and the outcome, unless the stream failed or the side
/// above closed first.
#[must_use]
pub fn check(stream: &[u8], settings: &Settings, seed: u64) -> Run {
    let run = run(stream, settings, seed);
    let expected = reference::events(settings.sent(stream), &settings.limits);
    let replay = || format!("seed {seed}, {settings:?}, stream {}", stream.escape_ascii());
    assert!(
        expected.events.starts_with(&run.events),
        "the events are the reference's, in order: {:?} against {:?}; {}",
        run.events,
        expected.events,
        replay()
    );
    match run.outcome {
        Some(Ending::Failed(Error::Stream(fault))) => {
            assert_eq!(run.failed, Some(fault), "the stream's own fault; {}", replay());
        }
        Some(outcome) => {
            assert_eq!(outcome, expected.ending, "the reference's outcome; {}", replay());
            assert_eq!(run.events.len(), expected.events.len(), "all the reference's events; {}", replay());
            if outcome == Ending::Ended {
                assert_eq!((run.retry, &run.last_id), (expected.retry, &expected.last_id), "{}", replay());
            }
        }
        None => assert!(settings.close.is_some(), "only an early close leaves no outcome; {}", replay()),
    }
    run
}

struct World<'a> {
    rng: Rng,
    settings: &'a Settings,
    env: Env<Limits>,
    reader: Reader,
    events: Queue<Event>,
    requests: Queue<Down>,
    below: Below<'a>,
    above: Above,
    fell: Fell,
    iteration: u64,
    held_back: u32,
}

struct Below<'a> {
    unsent: &'a [u8],
    intake: Intake,
    demand: Option<Read>,
    life: Life,
    failed: Option<Fault>,
    withdrawn: Option<Read>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Life {
    Open,
    Ending,
    Over,
}

struct Above {
    pending: bool,
    events: Vec<reference::Event>,
    outcome: Option<Ending>,
    closing: Option<Waiting>,
    closed: bool,
}

impl World<'_> {
    fn late(&mut self) {
        if let Some(read) = self.below.withdrawn.take()
            && self.rng.chance(500)
            && let Some(bytes) = self.below.intake.meet(read)
        {
            self.fell.late_delivery = true;
            self.up(Up::Bytes(bytes));
        }
        if self.below.life != Life::Over && self.rng.chance(500) {
            self.end();
        }
        if self.below.failed.is_none() && self.rng.chance(300) {
            let fault = fault(&mut self.rng);
            self.fail(fault);
        }
    }

    fn end(&mut self) {
        self.below.life = Life::Over;
        self.below.demand = None;
        self.up(Up::End);
    }

    fn fail(&mut self, fault: Fault) {
        let below = &mut self.below;
        self.fell.failed_after_end = below.life == Life::Over;
        self.fell.failed_while = Some(self.reader.waiting());
        below.life = Life::Over;
        below.demand = None;
        below.failed = Some(fault);
        self.up(Up::Failed(fault));
    }

    fn below_acts(&mut self) {
        let below = &mut self.below;
        if !below.unsent.is_empty() && below.intake.room() > 0 && self.rng.chance(self.settings.arrival) {
            let piece = usize::try_from(self.rng.between(1, u64::from(self.settings.piece))).expect("fits a usize");
            let room = usize::try_from(below.intake.room()).expect("fits a usize");
            let (arrived, rest) = below.unsent.split_at(piece.min(room).min(below.unsent.len()));
            below.intake.append(arrived).expect("within the room");
            below.unsent = rest;
        }
        if let Some((at, fault)) = self.settings.failure
            && self.iteration >= at
            && below.failed.is_none()
        {
            self.fail(fault);
            return;
        }
        match below.life {
            Life::Over => return,
            Life::Ending => {
                self.end();
                return;
            }
            Life::Open => {}
        }
        match below.demand {
            Some(read) => {
                if let Some(bytes) = below.intake.meet(read) {
                    below.demand = None;
                    self.up(Up::Bytes(bytes));
                } else if below.unsent.is_empty() {
                    self.end();
                }
            }
            None => {
                if below.unsent.is_empty() && below.intake.is_empty() && self.rng.chance(self.settings.idle_end) {
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
        if self.is_stalled() {
            return;
        }
        let above = &mut self.above;
        if above.closing.is_some() {
            return;
        }
        let closes_now = match self.settings.close {
            Some(at) => self.iteration >= at,
            None => false,
        } || (above.outcome.is_some() && self.rng.chance(self.settings.eagerness));
        if closes_now {
            above.closing = Some(self.reader.waiting());
            above.pending = false;
            self.down(Request::Close);
            return;
        }
        if above.outcome.is_none() && !above.pending && self.rng.chance(self.settings.eagerness) {
            above.pending = true;
            self.down(Request::Next);
        }
    }

    fn up(&mut self, ev: Up) {
        sse::up(&mut self.reader, &self.env, ev, &mut self.events, &mut self.requests);
        self.route(sse::UP_MAX_OUT, false);
    }

    fn down(&mut self, rq: Request) {
        let held = rq == Request::Next && self.below.demand.is_none();
        let closing = rq == Request::Close;
        sse::down(&mut self.reader, &self.env, rq, &mut self.events, &mut self.requests);
        // An event answered a Next at once: from what a delivery held.
        if held && !self.events.is_empty() && self.requests.is_empty() {
            self.fell.held_rest |= matches!(self.events.iter().next(), Some(Event::Message(_)));
        }
        self.route(sse::DOWN_MAX_OUT, closing);
    }

    fn route(&mut self, max: MaxOut, closing: bool) {
        assert!(self.events.len() <= max.above, "MAX_OUT honoured above: {:?}", self.events);
        assert!(self.requests.len() <= max.below, "MAX_OUT honoured below: {:?}", self.requests);
        while let Some(event) = self.events.pop() {
            self.above.receive(event);
        }
        while let Some(request) = self.requests.pop() {
            // One stated while the end is on its way crosses it (lib.md, 7).
            let crossing = self.below.life == Life::Ending;
            let limits = &self.settings.limits;
            let to_cr = self.below.receive(&request, closing, limits);
            self.fell.scanned_to_cr |= to_cr;
            self.fell.crossed_end |= crossing && self.below.demand.is_some();
        }
        let waiting = self.reader.waiting();
        let seen = if self.above.closed {
            Waiting::Nothing
        } else if self.above.outcome.is_some() {
            Waiting::Close
        } else if self.above.pending {
            Waiting::Bytes
        } else {
            Waiting::Next
        };
        assert_eq!(waiting, seen, "the reader waits for what its neighbours see");
        assert_eq!(
            self.below.demand.is_some(),
            waiting == Waiting::Bytes,
            "a demand is outstanding exactly while it waits for bytes"
        );
    }
}

impl Above {
    fn receive(&mut self, event: Event) {
        assert!(!self.closed, "nothing follows Closed: {event:?}");
        match event {
            Event::Message(message) => {
                assert!(self.pending, "an event answers a Next");
                self.pending = false;
                self.events.push(reference::Event {
                    name: message.name.to_vec(),
                    data: message.data.to_vec(),
                    id: message.id.to_vec(),
                });
            }
            Event::Ended | Event::Failed(_) => {
                assert!(self.pending, "an outcome answers a Next: {event:?}");
                assert!(self.outcome.is_none(), "one outcome");
                self.pending = false;
                self.outcome = Some(match event {
                    Event::Failed(error) => Ending::Failed(error),
                    Event::Ended | Event::Message(_) | Event::Closed => Ending::Ended,
                });
            }
            Event::Closed => {
                assert!(self.closing.is_some(), "Closed answers a Close");
                self.closed = true;
            }
        }
    }
}

impl Below<'_> {
    /// A request from the reader, checked; whether it scans to CR.
    fn receive(&mut self, request: &Down, closing: bool, limits: &Limits) -> bool {
        let &Down::Demand { read, room } = request else {
            panic!("the reader sends nothing down: {request:?}");
        };
        assert_eq!(room, 0, "the reader asks for no room");
        let (until, max) = match read {
            Read::Nothing => {
                assert!(closing, "only a close withdraws a demand");
                assert!(self.demand.is_some(), "only a demand outstanding is withdrawn");
                self.withdrawn = self.demand.take();
                return false;
            }
            Read::Fill(n) => panic!("the reader scans, it does not fill: {n}"),
            Read::Scan { until, max } => (until, max),
        };
        assert!(!closing, "a close demands nothing");
        assert!(self.demand.is_none(), "one demand at a time: {read:?} over {:?}", self.demand);
        assert!(self.life != Life::Over, "nothing is demanded once the stream's end or failure arrived: {read:?}");
        assert_eq!(max, sse::largest_demand(limits), "each scan of the chunk");
        assert!(max <= self.intake.capacity(), "no demand past the cap below");
        let to_cr = until == Delimiter::new(b"\r").expect("one byte");
        assert!(to_cr || until == Delimiter::LF, "a scan to the end of a line");
        self.demand = Some(read);
        to_cr
    }
}
