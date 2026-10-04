//! The machine world (testing-strategy.md, 2.4): one tokenizer, from a
//! seed, between its two neighbours, both played by the world in one loop.
//!
//! - **The side below** receives the peer's bytes in pieces cut at random,
//!   late, into an intake under its cap, and meets each demand exactly,
//!   at most once. It ends when the peer's bytes run out, early when the
//!   settings cut them, sometimes with nothing demanded, and then a demand
//!   may cross the end on its way; or it fails, before its end or after
//!   it.
//! - **The side above** asks for one token at a time, when it feels like
//!   it, stops asking for a while, and closes: after the outcome, or at a
//!   moment the settings draw, whatever the tokenizer is doing then.
//!
//! The world checks the machine's contracts as it goes (testing-strategy.md,
//! 6): `MAX_OUT` honoured on each call; one answer per `Next`, at most one
//! outcome, and `Closed` once, last; one demand at a time, none past the
//! largest the tokenizer declares or the cap below, none after the stream
//! ended, and one withdrawn only by a close; no room asked for and nothing
//! sent; and the tokenizer waiting for exactly what its neighbours see.

use skein_json::Token;
use skein_json::tokenizer::{self as json, Error, Event, Limits, MaxOut, Request, Tokenizer, Waiting};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue, Rng, Time, Wall};

use crate::{Decoded, Outcome, reference};

/// How a world runs: the tokenizer's limits, and how its neighbours behave.
#[derive(Clone, Debug)]
pub struct Settings {
    pub limits: Limits,
    /// The side below's cap: its intake holds this many bytes, at least the
    /// tokenizer's largest demand.
    pub cap: u32,
    /// The longest piece the peer's bytes arrive in.
    pub piece: u32,
    /// Per mille: how likely a piece arrives in an iteration.
    pub arrival: u32,
    /// Per mille: how likely the side above asks for a token, or closes
    /// after the outcome, in an iteration where it may.
    pub eagerness: u32,
    /// Per mille: how likely the side below reports the end of the stream
    /// in an iteration where nothing is demanded and nothing is left.
    pub idle_end: u32,
    /// Where the peer's bytes stop, if before the document's end: the
    /// stream ends early.
    pub cut: Option<usize>,
    /// When the stream fails, if it does: once, at the first iteration from
    /// this one on, before its end or after it, with this fault.
    pub failure: Option<(u64, Fault)>,
    /// When the side above closes, if it does whatever the tokenizer is
    /// doing: from this iteration on.
    pub close: Option<u64>,
    /// When the side above stops asking, if it does, and for how many
    /// iterations: meanwhile it neither asks for a token nor closes.
    pub stall: Option<(u64, u64)>,
}

impl Settings {
    /// Neighbours that are slow and cut the bytes anywhere, but never end
    /// the stream early, fail it or close before the outcome: the document
    /// decodes whole.
    #[must_use]
    pub fn calm(rng: &mut Rng, limits: Limits) -> Settings {
        let largest = json::largest_demand(&limits);
        Settings {
            limits,
            cap: largest + draw(rng, 0, 64),
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
    /// stops asking for a while, for a document of `len` bytes.
    #[must_use]
    pub fn chaotic(rng: &mut Rng, limits: Limits, len: usize) -> Settings {
        let mut settings = Settings::calm(rng, limits);
        // A moment within the run of a document this long, roughly.
        let span = 4 * len as u64 + 8;
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

    /// The bytes the peer sends of `document`.
    #[must_use]
    pub fn sent<'a>(&self, document: &'a [u8]) -> &'a [u8] {
        match self.cut {
            Some(cut) => &document[..cut.min(document.len())],
            None => document,
        }
    }
}

/// Limits drawn at random: mostly roomy enough for a generated document,
/// and now and then tiny, so that a sweep reaches each of them.
#[must_use]
pub fn limits(rng: &mut Rng) -> Limits {
    if rng.chance(300) {
        return Limits {
            depth: draw(rng, 0, 4),
            string: draw(rng, 0, 16),
            number: draw(rng, 1, 8),
            chunk: draw(rng, 1, 8),
            length: draw(rng, 0, 256),
        };
    }
    Limits {
        depth: draw(rng, 6, 16),
        string: draw(rng, 64, 256),
        number: draw(rng, 24, 64),
        chunk: draw(rng, 1, 64),
        length: draw(rng, 1 << 12, 1 << 16),
    }
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
    /// The tokens the side above received, in order.
    pub tokens: Vec<Token>,
    /// The outcome it received, unless it closed first.
    pub outcome: Option<Outcome>,
    /// What the tokenizer waited for when the side above closed it.
    pub closed_while: Waiting,
    /// The fault the stream failed with, if the side below reported one.
    pub failed: Option<Fault>,
    /// What else the neighbours injected that fell.
    pub fell: Fell,
    /// The most bytes the stream below held, nothing demanded, while the
    /// side above had stopped asking.
    pub held_back: u32,
    /// The iterations the run took.
    pub iterations: u64,
}

/// What fell in a run, of what its neighbours may inject: a sweep asserts
/// that each fell at least once (testing-strategy.md, 3).
#[expect(clippy::struct_excessive_bools, reason = "a record of what fell, a flag each")]
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Fell {
    /// The side below reported the end with nothing demanded.
    pub idle_end: bool,
    /// A demand crossed the end on its way, and was never answered.
    pub crossed_end: bool,
    /// The end came while the tokenizer held the byte that ended a number.
    pub byte_then_end: bool,
    /// A delivery reached the tokenizer after its close, for the demand the
    /// close withdrew.
    pub late_delivery: bool,
    /// The stream failed after its end.
    pub failed_after_end: bool,
    /// What the tokenizer waited for when the stream failed, if it did.
    pub failed_while: Option<Waiting>,
}

impl Run {
    /// What the document decoded to, if the side above heard its outcome.
    #[must_use]
    pub fn decoded(&self) -> Option<Decoded> {
        Some(Decoded { tokens: self.tokens.clone(), outcome: self.outcome? })
    }
}

/// Runs the world over `document`, the peer's bytes, with `settings`, from
/// `seed`, until the side above has closed the tokenizer.
#[must_use]
pub fn run(document: &[u8], settings: &Settings, seed: u64) -> Run {
    let limits = settings.limits;
    assert!(settings.cap >= json::largest_demand(&limits), "the side below's cap holds the largest demand");
    let mut world = World {
        rng: Rng::new(seed),
        settings,
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
        tokenizer: Tokenizer::new(&limits),
        // Room for more than MAX_OUT, so that the world, not the queue,
        // catches a call that emits more.
        events: Queue::with_capacity(8),
        requests: Queue::with_capacity(8),
        below: Below {
            unsent: settings.sent(document),
            intake: Intake::with_capacity(settings.cap),
            demand: None,
            life: Life::Open,
            failed: None,
            fell: Fell::default(),
            withdrawn: None,
        },
        above: Above { pending: false, tokens: Vec::new(), outcome: None, closing: None, closed: false },
        iteration: 0,
        held_back: 0,
        held_byte: false,
    };
    let stalled = match settings.stall {
        Some((_, iterations)) => iterations,
        None => 0,
    };
    let budget = 64 * (document.len() as u64 + 16) + stalled;
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
                tokens: world.above.tokens,
                outcome: world.above.outcome,
                closed_while: world.above.closing.expect("closed after a close"),
                failed: world.below.failed,
                fell: world.below.fell,
                held_back: world.held_back,
                iterations: world.iteration,
            };
        }
    }
    panic!("seed {seed}: the world settles within {budget} iterations: {settings:?}");
}

/// Runs the world as [`run`], and checks what the side above received
/// against the reference parser's reading of what the peer sent: the
/// tokens in order, and the outcome, unless the stream failed or the side
/// above closed first.
#[must_use]
pub fn check(document: &[u8], settings: &Settings, seed: u64) -> Run {
    let run = run(document, settings, seed);
    let expected = reference::parse(settings.sent(document), &settings.limits);
    let replay = || format!("seed {seed}, {settings:?}, document {}", document.escape_ascii());
    assert!(
        expected.tokens.starts_with(&run.tokens),
        "the tokens are the reference's, in order: {:?} against {:?}; {}",
        run.tokens,
        expected.tokens,
        replay()
    );
    match run.outcome {
        Some(Outcome::Failed(Error::Stream(fault))) => {
            assert_eq!(run.failed, Some(fault), "the stream's own fault; {}", replay());
        }
        Some(outcome) => {
            assert_eq!(outcome, expected.outcome, "the reference's outcome; {}", replay());
            assert_eq!(run.tokens.len(), expected.tokens.len(), "all the reference's tokens; {}", replay());
        }
        None => assert!(settings.close.is_some(), "only an early close leaves no outcome; {}", replay()),
    }
    run
}

/// The quote every scan is to.
const QUOTE: Delimiter = Delimiter::new(b"\"").expect("one byte");

struct World<'a> {
    rng: Rng,
    settings: &'a Settings,
    env: Env<Limits>,
    tokenizer: Tokenizer,
    events: Queue<Event>,
    requests: Queue<Down>,
    below: Below<'a>,
    above: Above,
    iteration: u64,
    held_back: u32,
    /// Whether the tokenizer holds the byte that ended a number, as the
    /// world sees it: a number sent up for a byte that is not whitespace,
    /// and no `Next` since.
    held_byte: bool,
}

/// The stream below.
struct Below<'a> {
    /// The peer's bytes not yet received.
    unsent: &'a [u8],
    intake: Intake,
    /// The demand outstanding, met at most once.
    demand: Option<Read>,
    life: Life,
    failed: Option<Fault>,
    fell: Fell,
    /// The demand a close withdrew, which a delivery already on its way
    /// may still meet.
    withdrawn: Option<Read>,
}

/// Where the stream below is in its life.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Life {
    Open,
    /// The end is on its way, decided with nothing demanded: a demand
    /// stated meanwhile crosses it, and is never answered.
    Ending,
    /// The end or a failure arrived: nothing more is delivered but a
    /// failure after the end.
    Over,
}

/// The side above.
struct Above {
    /// A `Next` not yet answered.
    pending: bool,
    tokens: Vec<Token>,
    outcome: Option<Outcome>,
    /// What the tokenizer waited for when the close went down.
    closing: Option<Waiting>,
    closed: bool,
}

impl World<'_> {
    /// What the side below may still deliver once the tokenizer is closed:
    /// a delivery already on its way for the demand the close withdrew,
    /// then the stream's end. The tokenizer emits nothing for either, which
    /// `route` checks.
    fn late(&mut self) {
        if let Some(read) = self.below.withdrawn.take()
            && self.rng.chance(500)
            && let Some(bytes) = self.below.intake.meet(read)
        {
            self.below.fell.late_delivery = true;
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

    /// The stream ends: a demand outstanding, or one that crossed the end,
    /// is never answered.
    fn end(&mut self) {
        self.below.life = Life::Over;
        self.below.demand = None;
        self.below.fell.byte_then_end |= self.held_byte;
        self.up(Up::End);
    }

    /// The stream fails: before its end or after it, once.
    fn fail(&mut self, fault: Fault) {
        let below = &mut self.below;
        below.fell.failed_after_end = below.life == Life::Over;
        below.fell.failed_while = Some(self.tokenizer.waiting());
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
        // A failure may come after the end too (lib.md, 7), once.
        if let Some((at, fault)) = self.settings.failure
            && self.iteration >= at
            && below.failed.is_none()
        {
            self.fail(fault);
            return;
        }
        if below.life == Life::Over {
            return;
        }
        if below.life == Life::Ending {
            self.end();
            return;
        }
        match below.demand {
            Some(read) => {
                if let Some(bytes) = below.intake.meet(read) {
                    assert_eq!(read_len(read, &bytes), bytes.len(), "a delivery is exactly the demand");
                    below.demand = None;
                    self.up(Up::Bytes(bytes));
                } else if below.unsent.is_empty() {
                    // It can never be met.
                    self.end();
                }
            }
            None => {
                if below.unsent.is_empty() && below.intake.is_empty() && self.rng.chance(self.settings.idle_end) {
                    below.fell.idle_end = true;
                    // Now, or on its way, which a demand may cross.
                    if self.rng.chance(500) {
                        below.life = Life::Ending;
                    } else {
                        self.end();
                    }
                }
            }
        }
    }

    /// Whether the side above has stopped asking for now.
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
            above.closing = Some(self.tokenizer.waiting());
            // The close drops a Next not yet answered.
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
        // A byte that is not whitespace, and ends a number, is held.
        let ends_a_number = match &ev {
            Up::Bytes(bytes) => matches!(**bytes, [byte] if !b" \t\n\r".contains(&byte)),
            Up::Room | Up::End | Up::Failed(_) => false,
        };
        let before = self.above.tokens.len();
        json::up(&mut self.tokenizer, &self.env, ev, &mut self.events, &mut self.requests);
        self.route(json::UP_MAX_OUT, false);
        if ends_a_number && self.above.tokens.len() > before {
            self.held_byte |= matches!(self.above.tokens.last(), Some(Token::Number(_)));
        }
    }

    fn down(&mut self, rq: Request) {
        if rq == Request::Next {
            self.held_byte = false;
        }
        json::down(&mut self.tokenizer, &self.env, rq, &mut self.events, &mut self.requests);
        self.route(json::DOWN_MAX_OUT, rq == Request::Close);
    }

    /// What one call emitted, to each side, checked.
    fn route(&mut self, max: MaxOut, closing: bool) {
        assert!(self.events.len() <= max.above, "MAX_OUT honoured above: {:?}", self.events);
        assert!(self.requests.len() <= max.below, "MAX_OUT honoured below: {:?}", self.requests);
        while let Some(event) = self.events.pop() {
            self.above.receive(event);
        }
        while let Some(request) = self.requests.pop() {
            let limits = &self.settings.limits;
            self.below.receive(&request, closing, limits);
        }
        let waiting = self.tokenizer.waiting();
        let seen = if self.above.closed {
            Waiting::Nothing
        } else if self.above.outcome.is_some() {
            Waiting::Close
        } else if self.above.pending {
            Waiting::Bytes
        } else {
            Waiting::Next
        };
        assert_eq!(waiting, seen, "the tokenizer waits for what its neighbours see");
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
            Event::Token(token) => {
                assert!(self.pending, "a token answers a Next: {token:?}");
                self.pending = false;
                self.tokens.push(token);
            }
            Event::Done | Event::Failed(_) => {
                assert!(self.pending, "an outcome answers a Next: {event:?}");
                assert!(self.outcome.is_none(), "one outcome: {event:?}");
                self.pending = false;
                self.outcome = Some(match event {
                    Event::Failed(error) => Outcome::Failed(error),
                    Event::Done | Event::Token(_) | Event::Closed => Outcome::Done,
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
    fn receive(&mut self, request: &Down, closing: bool, limits: &Limits) {
        let &Down::Demand { read, room } = request else {
            panic!("the tokenizer sends nothing down: {request:?}");
        };
        assert_eq!(room, 0, "the tokenizer asks for no room");
        let wanted = match read {
            Read::Nothing => {
                assert!(closing, "only a close withdraws a demand");
                assert!(self.demand.is_some(), "only a demand outstanding is withdrawn");
                self.withdrawn = self.demand.take();
                return;
            }
            Read::Fill(n) => n,
            Read::Scan { until, max } => {
                assert_eq!(until, QUOTE, "a scan is to a string's quote");
                max
            }
            Read::Line { .. } => panic!("the tokenizer scans to a string's quote, not to a line's end: {read:?}"),
        };
        assert!(!closing, "a close demands nothing");
        assert!(self.demand.is_none(), "one demand at a time: {read:?} over {:?}", self.demand);
        // One stated while the end is on its way crosses it (lib.md, 7).
        assert!(self.life != Life::Over, "nothing is demanded once the stream's end or failure arrived: {read:?}");
        self.fell.crossed_end |= self.life == Life::Ending;
        assert!(wanted >= 1, "a demand is for something: {read:?}");
        assert!(wanted <= json::largest_demand(limits), "no demand past the largest declared: {read:?}");
        assert!(wanted <= self.intake.capacity(), "no demand past the cap below: {read:?}");
        self.demand = Some(read);
    }
}

/// How long a delivery for `read` may be: a fill's count, or for a scan
/// the bytes delivered, up to its maximum.
fn read_len(read: Read, bytes: &[u8]) -> usize {
    match read {
        Read::Nothing => 0,
        Read::Fill(n) => usize::try_from(n).expect("fits a usize"),
        Read::Scan { max, .. } | Read::Line { max } => {
            assert!(bytes.len() <= usize::try_from(max).expect("fits a usize"), "a scan delivers at most its maximum");
            bytes.len()
        }
    }
}
