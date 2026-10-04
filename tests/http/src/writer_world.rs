//! The event stream writer's machine world (testing-strategy.md, 2.4): one
//! writer, from a seed, between its two neighbours, both played by the
//! world in one loop.
//!
//! - **The side below** is a response body, written: it grants room late,
//!   one `Send` a grant, and keeps what is sent; now and then it says the
//!   stream ended, which a writer reads nothing of, and room comes after
//!   it all the same; or it fails, idle or with room asked for.
//! - **The side above** writes events and comments of every shape, one at
//!   a time, now and then one the writer must refuse, when it feels like
//!   it; it stops for a while, finishes after the last, or closes at a
//!   moment the settings draw, whatever the writer is doing.
//!
//! The world checks the writer's contracts as it goes (testing-strategy.md,
//! 6): `MAX_OUT` on each call; below, room only, one demand at a time,
//! none past a chunk, none after `Finish` or a failure, one withdrawn only
//! by a close, and one `Send` a grant within it; above, one answer for each
//! event or comment, `Failed` once and nothing after it, `Closed` once and
//! last; and the writer waiting for exactly what its neighbours see.
//! [`check`] reads what was written with the reference reader, and with
//! the reader's own world, and holds it to what the side above wrote.

use skein_http::MaxOut;
use skein_http::sse::writer::{self, Event, Limits, Outgoing, Refusal, Request, Waiting, Writer};
use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Queue, Rng, Time, Wall};

use crate::generate::{draw, pick, text};
use crate::reference::{self, Ending};
use crate::sse_world;

/// How a world runs: the writer's limits, and how its neighbours behave.
#[derive(Clone, Debug)]
pub struct Settings {
    pub limits: Limits,
    /// Per mille: how likely room demanded is granted in an iteration.
    pub grant: u32,
    /// Per mille: how likely the side above acts in an iteration where it
    /// may.
    pub eagerness: u32,
    /// Per mille, per iteration: how likely the side below says the stream
    /// ended, once.
    pub end: u32,
    /// When the stream fails, if it does: once, at the first iteration from
    /// this one on, with this fault.
    pub failure: Option<(u64, Fault)>,
    /// When the side above closes, if it does whatever the writer is doing.
    pub close: Option<u64>,
    /// When the side above stops acting, and for how many iterations.
    pub stall: Option<(u64, u64)>,
}

impl Settings {
    /// Neighbours that are slow, but never fail the stream or close before
    /// the last item.
    #[must_use]
    pub fn calm(rng: &mut Rng, limits: Limits) -> Settings {
        Settings {
            limits,
            grant: u32::try_from(rng.between(100, 1000)).expect("fits a u32"),
            eagerness: u32::try_from(rng.between(100, 1000)).expect("fits a u32"),
            end: if rng.chance(300) { 20 } else { 0 },
            failure: None,
            close: None,
            stall: None,
        }
    }

    /// Neighbours as [`calm`](Settings::calm), and sometimes a stream that
    /// fails, a close at any moment, or a side above that stops for a
    /// while, for `items` items of about `len` bytes in all.
    #[must_use]
    pub fn chaotic(rng: &mut Rng, limits: Limits, len: usize) -> Settings {
        let mut settings = Settings::calm(rng, limits);
        let span = 4 * len as u64 + 64;
        if rng.chance(200) {
            settings.failure = Some((rng.below(span), fault(rng)));
        }
        if rng.chance(200) {
            settings.close = Some(rng.below(span));
        }
        if rng.chance(150) {
            settings.stall = Some((rng.below(span), rng.between(16, 256)));
        }
        settings
    }
}

/// Limits drawn at random: mostly roomy enough for a generated event, and
/// now and then tiny, so that a sweep reaches each of them.
#[must_use]
pub fn limits(rng: &mut Rng) -> Limits {
    if rng.chance(250) {
        return Limits { event: u32::try_from(rng.between(0, 48)).expect("fits"), chunk: draw32(rng, 1, 8) };
    }
    Limits { event: draw32(rng, 256, 4096), chunk: draw32(rng, 1, 256) }
}

fn draw32(rng: &mut Rng, low: u32, high: u32) -> u32 {
    u32::try_from(rng.between(u64::from(low), u64::from(high))).expect("fits a u32")
}

fn fault(rng: &mut Rng) -> Fault {
    [Fault::Reset, Fault::Invalid, Fault::Other][usize::try_from(rng.below(3)).expect("fits a usize")]
}

/// What the side above writes: an event, or a comment.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Item {
    Event(Outgoing),
    Comment(Vec<u8>),
}

/// Items drawn at random: events of every field, their data's lines ended
/// by LF, CRLF or CR, comments, and now and then one with something the
/// writer refuses.
#[must_use]
pub fn items(rng: &mut Rng) -> Vec<Item> {
    let mut items = Vec::new();
    for _ in 0..draw(rng, 0, 8) {
        if rng.chance(150) {
            let mut comment = Vec::new();
            if rng.chance(700) {
                comment.extend_from_slice(pick(rng, &["ping", "keep-alive", ""]).as_bytes());
            }
            if rng.chance(30) {
                comment.extend_from_slice(pick(rng, &["\n", "\r", "a\r\nb"]).as_bytes());
            }
            items.push(Item::Comment(comment));
            continue;
        }
        let name = if rng.chance(500) {
            pick(rng, &["message_start", "content_block_delta", "ping", "x", "message"]).as_bytes().to_vec()
        } else {
            Vec::new()
        };
        let mut data = Vec::new();
        for line in 0..draw(rng, 1, 4) {
            if line > 0 {
                data.extend_from_slice(pick(rng, &["\n", "\n", "\r\n", "\r"]).as_bytes());
            }
            text(rng, 0, 40, &mut data);
        }
        if rng.chance(100) {
            data.extend_from_slice(pick(rng, &["\n", "\r\n", "\r"]).as_bytes());
        }
        let id = if rng.chance(200) {
            let mut id = Vec::new();
            text(rng, 0, 6, &mut id);
            Some(id.into_boxed_slice())
        } else {
            None
        };
        let retry = if rng.chance(100) { Some(rng.below(100_000)) } else { None };
        let mut outgoing = Outgoing { name: name.into(), data: data.into(), id, retry };
        if rng.chance(30) {
            match rng.below(3) {
                0 => outgoing.name = (*pick(rng, &[&b"a\nb"[..], b"\r", b"x\r\n"])).into(),
                1 => outgoing.id = Some((*pick(rng, &[&b"a\0"[..], b"\n", b"x\ry"])).into()),
                _ => outgoing.data = vec![b'd'; 5000].into(),
            }
        }
        items.push(Item::Event(outgoing));
    }
    items
}

/// Why the writer must refuse `item` under `limits`, if it must: the first
/// thing wrong, in the order of http.md, 4.3, read independently of the
/// writer's checks.
#[must_use]
pub fn refusal(item: &Item, limits: &Limits) -> Option<Refusal> {
    let line_end = |byte: &u8| *byte == b'\r' || *byte == b'\n';
    let framed = match item {
        Item::Comment(text) => {
            if text.iter().any(line_end) {
                return Some(Refusal::Comment);
            }
            frame(item)
        }
        Item::Event(outgoing) => {
            if outgoing.name.iter().any(line_end) {
                return Some(Refusal::Name);
            }
            if let Some(id) = &outgoing.id
                && id.iter().any(|byte| line_end(byte) || *byte == 0)
            {
                return Some(Refusal::Id);
            }
            frame(item)
        }
    };
    if framed.len() > usize::try_from(limits.event).expect("fits a usize") { Some(Refusal::TooLong) } else { None }
}

/// The bytes `item` must be written as: a writer of the test's own.
#[must_use]
pub fn frame(item: &Item) -> Vec<u8> {
    let mut out = Vec::new();
    match item {
        Item::Comment(text) if text.is_empty() => out.extend_from_slice(b":\n\n"),
        Item::Comment(text) => {
            out.extend_from_slice(b": ");
            out.extend_from_slice(text);
            out.extend_from_slice(b"\n\n");
        }
        Item::Event(outgoing) => {
            if !outgoing.name.is_empty() {
                out.extend_from_slice(b"event: ");
                out.extend_from_slice(&outgoing.name);
                out.push(b'\n');
            }
            if let Some(id) = &outgoing.id {
                out.extend_from_slice(b"id: ");
                out.extend_from_slice(id);
                out.push(b'\n');
            }
            if let Some(retry) = outgoing.retry {
                out.extend_from_slice(format!("retry: {retry}\n").as_bytes());
            }
            for line in lines(&outgoing.data) {
                out.extend_from_slice(b"data: ");
                out.extend_from_slice(line);
                out.push(b'\n');
            }
            out.push(b'\n');
        }
    }
    out
}

/// `data`'s lines, split at each LF, CRLF or CR: one more than its endings.
fn lines(data: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut at = 0;
    while at < data.len() {
        match data[at] {
            b'\r' if data.get(at + 1) == Some(&b'\n') => {
                lines.push(&data[start..at]);
                at += 2;
                start = at;
            }
            b'\r' | b'\n' => {
                lines.push(&data[start..at]);
                at += 1;
                start = at;
            }
            _ => at += 1,
        }
    }
    lines.push(&data[start..]);
    lines
}

/// What a run came to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Run {
    /// What the writer sent below.
    pub written: Vec<u8>,
    /// Each item the side above gave, with how the writer answered it, if
    /// it did.
    pub answered: Vec<(Item, Option<Event>)>,
    /// Whether the body's end went below.
    pub finished: bool,
    pub closed_while: Waiting,
    pub failed: Option<Fault>,
    pub fell: Fell,
    pub iterations: u64,
}

/// What fell in a run, of what its neighbours may inject.
#[expect(clippy::struct_excessive_bools, reason = "a record of what fell, a flag each")]
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Fell {
    /// An event went down in more than one piece.
    pub pieces: bool,
    /// The stream ended, which the writer read nothing of.
    pub ended: bool,
    /// Room came after the stream's end.
    pub room_after_end: bool,
    /// The stream failed with nothing being written: the next item heard it.
    pub failed_idle: bool,
    /// What the writer waited for when the stream failed, if it did.
    pub failed_while: Option<Waiting>,
}

/// Runs the world over `items`, with `settings`, from `seed`, until the
/// side above has closed the writer.
#[must_use]
pub fn run(items: &[Item], settings: &Settings, seed: u64) -> Run {
    let limits = settings.limits;
    let mut world = World {
        rng: Rng::new(seed),
        settings,
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
        writer: Writer::new(&limits),
        events: Queue::with_capacity(8),
        requests: Queue::with_capacity(8),
        items,
        next: 0,
        pending: false,
        answered: Vec::new(),
        demand: None,
        granted: None,
        withdrawn: None,
        written: Vec::new(),
        finished: false,
        ended: false,
        failed: None,
        closing: None,
        closed: false,
        over: false,
        pieces: 0,
        fell: Fell::default(),
        iteration: 0,
    };
    let stalled = match settings.stall {
        Some((_, iterations)) => iterations,
        None => 0,
    };
    let len: usize = items.iter().map(|item| frame(item).len()).sum();
    let budget = 16 * (len as u64 + 64) + 64 * items.len() as u64 + stalled + 1024;
    while world.iteration < budget {
        if world.rng.chance(500) {
            world.below_acts();
            world.above_acts();
        } else {
            world.above_acts();
            world.below_acts();
        }
        world.iteration += 1;
        if world.closed {
            world.late();
            return Run {
                written: world.written,
                answered: world.answered,
                finished: world.finished,
                closed_while: world.closing.expect("closed after a close"),
                failed: world.failed,
                fell: world.fell,
                iterations: world.iteration,
            };
        }
    }
    panic!("seed {seed}: the world settles within {budget} iterations: {settings:?}");
}

/// Runs the world as [`run`], and holds it to what the side above wrote:
/// each item refused exactly when the test refuses it; what was written,
/// the frames of the items sent, in order, and a prefix of the next; read
/// back by the reference reader and by the reader's own world, the events
/// sent, each as it was written.
#[must_use]
pub fn check(items: &[Item], settings: &Settings, seed: u64) -> Run {
    let run = run(items, settings, seed);
    let replay = || format!("seed {seed}, {settings:?}, items {items:?}");
    let mut expected = Vec::new();
    let mut sent = Vec::new();
    let mut last = None;
    for (item, answer) in &run.answered {
        let refusal = refusal(item, &settings.limits);
        match answer {
            Some(Event::Refused(refused)) => {
                assert_eq!(Some(*refused), refusal, "the test's refusal; {}", replay());
            }
            Some(Event::Sent) => {
                assert_eq!(refusal, None, "an item the test refuses; {}", replay());
                expected.extend_from_slice(&frame(item));
                sent.push(item.clone());
            }
            Some(Event::Failed(fault)) => {
                assert_eq!(run.failed, Some(*fault), "the stream's own fault; {}", replay());
                last = Some(frame(item));
            }
            None => {
                if refusal.is_none() {
                    last = Some(frame(item));
                }
            }
            Some(Event::Closed) => unreachable!("Closed answers a Close"),
        }
    }
    assert!(run.written.starts_with(&expected), "the frames of the items sent, in order; {}", replay());
    let rest = &run.written[expected.len()..];
    match last {
        Some(last) => assert!(last.starts_with(rest), "and a prefix of the one being written; {}", replay()),
        None => assert!(rest.is_empty(), "and nothing else; {}", replay()),
    }
    // Read back, what was sent is each event as it was written.
    let messages = messages(&sent);
    let field = settings.limits.event.max(1);
    let reader = skein_http::sse::Limits { line: field, event: field, field, chunk: 64 };
    let read = reference::events(&run.written, &reader);
    assert_eq!(read.ending, Ending::Ended, "{}", replay());
    assert_eq!(read.events, messages, "the reference reads each event as it was written; {}", replay());
    let mut rng = Rng::new(seed);
    let below = sse_world::Settings::calm(&mut rng, reader);
    let reread = sse_world::check(&run.written, &below, seed);
    assert_eq!(reread.events, messages, "the reader reads each event as it was written; {}", replay());
    run
}

/// The events a reader dispatches for `sent`, as the writer was given them:
/// a type of none read as `message`, the data's line endings as LFs, and
/// the last event ID each `id` left.
fn messages(sent: &[Item]) -> Vec<reference::Event> {
    let mut messages = Vec::new();
    let mut last_id = Vec::new();
    for item in sent {
        let Item::Event(outgoing) = item else { continue };
        if let Some(id) = &outgoing.id {
            last_id = id.to_vec();
        }
        let name = if outgoing.name.is_empty() { b"message".to_vec() } else { outgoing.name.to_vec() };
        messages.push(reference::Event { name, data: lines(&outgoing.data).join(&b'\n'), id: last_id.clone() });
    }
    messages
}

#[expect(clippy::struct_excessive_bools, reason = "what the neighbours know of the stream, a flag each")]
struct World<'a> {
    rng: Rng,
    settings: &'a Settings,
    env: Env<Limits>,
    writer: Writer,
    events: Queue<Event>,
    requests: Queue<Down>,
    items: &'a [Item],
    /// The next item to give.
    next: usize,
    /// An item given, not yet answered.
    pending: bool,
    answered: Vec<(Item, Option<Event>)>,
    /// The writer's demand outstanding: the room it asks for.
    demand: Option<u32>,
    /// Room granted and not yet sent in.
    granted: Option<u32>,
    /// The demand the writer withdrew, which room on its way may still meet.
    withdrawn: Option<u32>,
    written: Vec<u8>,
    finished: bool,
    ended: bool,
    failed: Option<Fault>,
    closing: Option<Waiting>,
    closed: bool,
    /// The writer answered `Failed`: nothing more is given.
    over: bool,
    /// The pieces of the item being written.
    pieces: u32,
    fell: Fell,
    iteration: u64,
}

impl World<'_> {
    /// What the side below may still deliver once the writer is closed: the
    /// room on its way for the demand the close withdrew.
    fn late(&mut self) {
        if self.withdrawn.take().is_some() && self.rng.chance(500) {
            self.up(Up::Room);
        }
        if self.failed.is_none() && self.rng.chance(300) {
            let fault = fault(&mut self.rng);
            self.failed = Some(fault);
            self.up(Up::Failed(fault));
        }
    }

    fn below_acts(&mut self) {
        if let Some((at, fault)) = self.settings.failure
            && self.iteration >= at
            && self.failed.is_none()
        {
            self.fell.failed_idle |= self.demand.is_none() && !self.pending;
            self.fell.failed_while = Some(self.writer.waiting());
            self.failed = Some(fault);
            self.demand = None;
            self.up(Up::Failed(fault));
            return;
        }
        if self.failed.is_some() {
            return;
        }
        if !self.ended && self.rng.chance(self.settings.end) {
            self.ended = true;
            self.fell.ended = true;
            self.up(Up::End);
            return;
        }
        if let Some(room) = self.demand
            && self.rng.chance(self.settings.grant)
        {
            self.demand = None;
            self.granted = Some(room);
            self.fell.room_after_end |= self.ended;
            self.up(Up::Room);
        }
    }

    fn is_stalled(&self) -> bool {
        match self.settings.stall {
            Some((from, iterations)) => (from..from + iterations).contains(&self.iteration),
            None => false,
        }
    }

    fn above_acts(&mut self) {
        if self.is_stalled() || self.closing.is_some() {
            return;
        }
        let closes_now = match self.settings.close {
            Some(at) => self.iteration >= at,
            None => false,
        };
        let done = self.finished || self.over;
        if closes_now || (done && self.rng.chance(self.settings.eagerness)) {
            self.closing = Some(self.writer.waiting());
            self.down(Request::Close);
            return;
        }
        if self.pending || done || !self.rng.chance(self.settings.eagerness) {
            return;
        }
        let Some(item) = self.items.get(self.next).cloned() else {
            self.finished = true;
            self.down(Request::Finish);
            return;
        };
        self.next += 1;
        self.pending = true;
        self.pieces = 0;
        self.answered.push((item.clone(), None));
        let rq = match item {
            Item::Event(outgoing) => Request::Event(outgoing),
            Item::Comment(text) => Request::Comment(text.into()),
        };
        self.down(rq);
    }

    fn up(&mut self, ev: Up) {
        writer::up(&mut self.writer, &self.env, ev, &mut self.events, &mut self.requests);
        self.route(writer::UP_MAX_OUT, false);
    }

    fn down(&mut self, rq: Request) {
        let closing = rq == Request::Close;
        writer::down(&mut self.writer, &self.env, rq, &mut self.events, &mut self.requests);
        self.route(writer::DOWN_MAX_OUT, closing);
    }

    /// What one call emitted, to each side, checked.
    fn route(&mut self, max: MaxOut, closing: bool) {
        assert!(self.events.len() <= max.above, "MAX_OUT honoured above: {:?}", self.events);
        assert!(self.requests.len() <= max.below, "MAX_OUT honoured below: {:?}", self.requests);
        while let Some(request) = self.requests.pop() {
            self.send(request, closing);
        }
        while let Some(event) = self.events.pop() {
            self.receive(event);
        }
        let seen = if self.closed {
            Waiting::Nothing
        } else if self.finished || self.over {
            Waiting::Close
        } else if self.demand.is_some() {
            Waiting::Room
        } else {
            Waiting::Above
        };
        assert_eq!(self.writer.waiting(), seen, "the writer waits for what its neighbours see");
    }

    fn receive(&mut self, event: Event) {
        assert!(!self.closed, "nothing follows Closed: {event:?}");
        match event {
            Event::Sent | Event::Refused(_) | Event::Failed(_) => {
                assert!(self.pending, "an answer for an item given: {event:?}");
                assert!(self.demand.is_none(), "nothing demanded once answered");
                self.pending = false;
                self.fell.pieces |= event == Event::Sent && self.pieces > 1;
                self.over |= matches!(event, Event::Failed(_));
                let (_, answer) = self.answered.last_mut().expect("the item answered");
                *answer = Some(event);
            }
            Event::Closed => {
                assert!(self.closing.is_some(), "Closed answers a Close");
                self.closed = true;
            }
        }
    }

    fn send(&mut self, request: Down, closing: bool) {
        match request {
            Down::Demand { read: Read::Nothing, room: 0 } => {
                assert!(closing, "only a close withdraws a demand");
                self.withdrawn = Some(self.demand.take().expect("only a demand outstanding is withdrawn"));
            }
            Down::Demand { read, room } => {
                assert_eq!(read, Read::Nothing, "the writer reads nothing");
                assert!(self.demand.is_none(), "one demand at a time");
                assert!(self.granted.is_none(), "room granted is sent in before more is demanded");
                assert!(room > 0 && room <= writer::largest_room(&self.settings.limits), "room within a chunk");
                assert!(self.failed.is_none() && !self.finished, "nothing demanded after a failure or Finish");
                self.demand = Some(room);
            }
            Down::Send(bytes) => {
                let granted = self.granted.take().expect("a Send within room granted");
                assert!(bytes.len() <= usize::try_from(granted).expect("fits"), "a Send within the room granted");
                assert!(self.failed.is_none(), "nothing sent after a failure");
                self.pieces += 1;
                self.written.extend_from_slice(&bytes);
            }
            Down::Finish => {
                assert!(self.finished, "Finish goes below for the side above's");
                assert!(self.demand.is_none() && self.granted.is_none(), "Finish with nothing outstanding");
            }
        }
    }
}
