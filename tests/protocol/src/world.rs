//! A protocol world (testing-strategy.md, 2.5): both ends of an LLM
//! streaming exchange, built as two services would build them, joined by
//! bytes cut and joined at random, in one loop with a referee beside them.
//!
//! Each iteration, the referee injects what belongs to no end, a reset of
//! the wire; the wire carries a piece each way, now and then; each end's
//! stream below answers its stack, and each end's user acts, in an order
//! drawn from the seed; then the referee observes what the users saw. Each
//! machine's calls are held to its `MAX_OUT`, and each stream below to the
//! contract of a stream, as the world goes (testing-strategy.md, 6). The
//! referee ends the run once its expectations are met, both ends settled
//! among them, and fails it with what is pending once one is overdue. A
//! seed replays to the same run.
//!
//! The referee (testing-strategy.md, 7; skein-world's [`Referee`]) watches
//! what the users saw, never the machines. Safety, at every observation:
//! what the top of one end sent is what the top of the other received, in
//! order, each event with the last event ID and reconnection time the
//! reader's face shows; and what the server's writer sent and the client's
//! user has not read is never more than the caps between them. Liveness,
//! each a [`Goal`] with its deadline: each scenario's outcome, and what it
//! aims at, a writer or an upload held back by a slow reader at the other
//! end, each seen as a user sees it, a write that waits.

use std::fmt;

use skein_http::client::{self, Reuse};
use skein_http::server;
use skein_http::sse;
use skein_json::Token;
use skein_lib::stream::Fault;
use skein_lib::{Rng, Time};
use skein_world::{Expectation, Expectations, Referee};

use crate::client_end;
use crate::server_end::{self, framed_len};
use crate::wire::{self, Bottom};

/// What a world's ends are, and how the wire and the users behave.
#[derive(Clone, Debug)]
pub struct Settings {
    pub client: client_end::Limits,
    pub server: server_end::Limits,
    /// The client's stream below: its intake's cap and its output's.
    pub client_intake: u32,
    pub client_output: u32,
    /// The server's stream below: its intake's cap and its output's.
    pub server_intake: u32,
    pub server_output: u32,
    /// The longest piece the wire carries at once, each way.
    pub piece: usize,
    /// Per mille: how likely the wire carries a piece in an iteration.
    pub carry: u32,
    /// Per mille: how likely room demanded is granted in an iteration.
    pub grant: u32,
    /// Per mille: how likely a user acts in an iteration.
    pub eagerness: u32,
    /// When the referee resets the wire, if it does: both ends' streams
    /// fail.
    pub reset: Option<u64>,
    /// How long a closed end drains what comes before the other end's
    /// sends meet a reset, as a socket's linger.
    pub linger: u64,
}

impl Settings {
    /// Roomy limits, caps drawn from the seed down to the least the stacks
    /// allow, and neighbours of every pace.
    #[must_use]
    pub fn drawn(rng: &mut Rng) -> Settings {
        let client = client_end::Limits {
            client: client::Limits { request: 1024, head: 4096, headers: 32, read: 256, send: 128 },
            reader: sse::Limits { line: 4096, event: 8192, field: 64, chunk: 64 },
            json: skein_json::tokenizer::Limits { depth: 16, string: 4096, number: 32, chunk: 64, length: 1 << 16 },
        };
        let server = server_end::Limits {
            server: server::Limits { head: 4096, headers: 32, body: 1 << 20, read: 256, response: 512, send: 128 },
            writer: skein_http::sse::writer::Limits { event: 8192, chunk: 128 },
            json: client.json,
        };
        let slack = |rng: &mut Rng| u32::try_from(rng.below(512)).expect("fits a u32");
        Settings {
            client,
            server,
            client_intake: client::largest_read(&client.client) + slack(rng),
            client_output: client::largest_room(&client.client) + slack(rng),
            server_intake: server::largest_read(&server.server) + slack(rng),
            server_output: server::largest_room(&server.server) + slack(rng),
            piece: usize::try_from(rng.between(1, 300)).expect("fits a usize"),
            carry: u32::try_from(rng.between(200, 1000)).expect("fits a u32"),
            grant: u32::try_from(rng.between(200, 1000)).expect("fits a u32"),
            eagerness: u32::try_from(rng.between(200, 1000)).expect("fits a u32"),
            reset: None,
            linger: rng.between(0, 200),
        }
    }

    /// What the server's writer may have sent that the client's user has
    /// not read, in bytes of events as the writer frames them: what the
    /// server's stream below holds, what the client's holds, the client's
    /// carry-over, and the event the reader is reading.
    #[must_use]
    pub fn in_flight(&self) -> u64 {
        u64::from(self.server_output)
            + u64::from(self.client_intake)
            + u64::from(self.client.client.read)
            + u64::from(self.client.reader.event)
    }
}

/// What a scenario sets up, in the ends' own terms.
#[derive(Clone, Debug)]
pub struct Scenario {
    /// Each call's request document, as the client's user writes it.
    pub requests: Vec<Vec<Token>>,
    /// The events the server's user answers each call with: each one's
    /// type and data's document.
    pub answers: Vec<Vec<(Vec<u8>, Vec<Token>)>>,
    pub client: client_end::Script,
    pub server: server_end::Script,
    pub expect: Expect,
    /// What a slow user at one end holds back at the other, if the
    /// scenario aims at it.
    pub holds: Option<Hold>,
    /// The least a closed end lingers, draining what comes, that the
    /// expectation needs: a response given before the upload ends reaches a
    /// client still uploading only if the server's end drains its upload
    /// meanwhile, as io's close does until its deadline (io.md, 3.3).
    pub linger: u64,
}

/// What the referee holds a scenario to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Expect {
    /// Each request goes whole to the server's top, and every event to the
    /// client's, each call on the connection the last one kept.
    Stream,
    /// The server answers the request at once with an error, while the
    /// client uploads: the upload stops, and the client reads the error.
    Early,
    /// An end closes, or the wire resets, partway: what came stands, the
    /// other end hears it, and both settle.
    Partway,
}

/// What a slow user at one end holds back at the other.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Hold {
    /// The client's user stops reading: the server's writer waits.
    Writer,
    /// The server's user stops reading the request: the client's upload
    /// waits.
    Upload,
}

/// What a run came to: what each end's user saw.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Run {
    pub client: client_end::Seen,
    pub server: server_end::Seen,
    pub fell: Fell,
    pub iterations: u64,
}

/// What fell in a run.
#[expect(clippy::struct_excessive_bools, reason = "a record of what fell, a flag each")]
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Fell {
    /// An event the server's user wrote waited while the client's user did
    /// not read: the slow reader held the writer back.
    pub writer_held: bool,
    /// The client's user's upload waited for room while the server's user
    /// did not read: the slow consumer held it back.
    pub upload_held: bool,
    /// The client's upload stopped as the response came first.
    pub upload_stopped: bool,
    /// The server's writer heard its stream fail.
    pub writer_failed: bool,
    /// A stream reset under an end still open.
    pub reset: bool,
    /// The most calls a connection carried.
    pub calls: usize,
    /// How many events came with an id or a reconnection time, checked
    /// against the reader's face.
    pub ids: usize,
    /// The most the writer had sent beyond what the client read.
    pub most_in_flight: u64,
}

/// A process of the world: an end, the client's or the server's. The
/// world holds the client's first.
#[derive(Debug)]
pub enum Proc {
    Client(client_end::End),
    Server(server_end::End),
}

/// The two ends of `procs`.
fn ends(procs: &[Proc]) -> (&client_end::End, &server_end::End) {
    let [Proc::Client(client), Proc::Server(server)] = procs else { panic!("the client's end, then the server's") };
    (client, server)
}

fn ends_mut(procs: &mut [Proc]) -> (&mut client_end::End, &mut server_end::End) {
    let [Proc::Client(client), Proc::Server(server)] = procs else { panic!("the client's end, then the server's") };
    (client, server)
}

/// A world's clock: an iteration a nanosecond.
fn at(iteration: u64) -> Time {
    Time::from_nanos(iteration)
}

/// How long a slow user's write waits, at the least, for the referee to
/// see it held back.
const HELD: u64 = 100;

/// Runs `scenario` with `settings`, from `seed`, until the referee's
/// expectations are met, checking as it goes; an expectation overdue fails
/// it.
#[must_use]
pub fn run(scenario: &Scenario, settings: &Settings, seed: u64) -> Run {
    let mut rng = Rng::new(seed);
    let mut procs = [
        Proc::Client(client_end::End::new(
            settings.client,
            Bottom::new(settings.client_intake, settings.client_output),
            scenario.client.clone(),
        )),
        Proc::Server(server_end::End::new(
            settings.server,
            Bottom::new(settings.server_intake, settings.server_output),
            scenario.server.clone(),
        )),
    ];
    let mut referee = Watch::new(scenario, settings, seed);
    let linger = settings.linger.max(scenario.linger);
    for now in 0.. {
        referee.act(at(now), &mut procs);
        let (client, server) = ends_mut(&mut procs);
        // The wire, each way.
        if rng.chance(settings.carry) {
            let piece = usize::try_from(rng.between(1, settings.piece as u64)).expect("fits a usize");
            referee.fell.reset |= wire::carry(&mut client.bottom, &mut server.bottom, piece, now, linger);
        }
        if rng.chance(settings.carry) {
            let piece = usize::try_from(rng.between(1, settings.piece as u64)).expect("fits a usize");
            referee.fell.reset |= wire::carry(&mut server.bottom, &mut client.bottom, piece, now, linger);
        }
        // Each end: its stream below, then its user, in an order drawn.
        let grant_client = rng.chance(settings.grant);
        let grant_server = rng.chance(settings.grant);
        let client_acts = rng.chance(settings.eagerness);
        let server_acts = rng.chance(settings.eagerness);
        if rng.chance(500) {
            client.below_acts(now, grant_client);
            server.below_acts(now, grant_server);
        } else {
            server.below_acts(now, grant_server);
            client.below_acts(now, grant_client);
        }
        if client_acts {
            client.act(now);
        }
        if server_acts {
            server.act(now);
        }
        referee.observe(at(now), &procs);
        if let Some(overdue) = referee.overdue(at(now)) {
            let (client, server) = ends(&procs);
            panic!(
                "seed {seed}: overdue at iteration {now}:\n{overdue}client {:?}, server {:?}, {settings:?}",
                client.seen, server.seen
            );
        }
        if referee.passed() {
            let (client, server) = ends(&procs);
            return Run {
                client: client.seen.clone(),
                server: server.seen.clone(),
                fell: referee.fell,
                iterations: now,
            };
        }
    }
    unreachable!("a run ends when the referee passes it or finds it overdue")
}

/// The deadline of a scenario's goals, in iterations: the bytes between
/// the ends at the wire's pace, the room each send waits for and the user's
/// moves at theirs, four times over, and the stalls, a close and the linger
/// it waits through.
fn deadline(scenario: &Scenario, settings: &Settings) -> u64 {
    let mut bytes = 0;
    let mut items = 0;
    for body in &scenario.client.bodies {
        bytes += body.len() as u64 + 256;
        items += 2;
    }
    for answer in &scenario.server.answers {
        for item in answer {
            bytes += framed_len(item);
            items += 1;
        }
    }
    if let Some((_, body)) = &scenario.server.early {
        bytes += body.len() as u64;
    }
    let wire = bytes * 2000 / (u64::from(settings.carry) * (settings.piece as u64 + 1)) + 1;
    let grants = (bytes / 64 + items) * 1000 / u64::from(settings.grant);
    let moves = (bytes / 64 + 8 * items) * 1000 / u64::from(settings.eagerness);
    let stalls = scenario.client.stall.map_or(0, |(_, iterations)| iterations)
        + scenario.server.stall.map_or(0, |(_, iterations)| iterations);
    let close = scenario.client.close.or(scenario.server.close).unwrap_or(0);
    4 * (wire + grants + moves) + stalls + close + settings.linger.max(scenario.linger) + 5_000
}

/// How long the wire takes, in iterations, to carry what the caps between
/// the ends hold, twice over.
fn fill(settings: &Settings) -> u64 {
    settings.in_flight() * 4000 / (u64::from(settings.carry) * (settings.piece as u64 + 1))
}

/// What the referee holds the users to by a deadline (testing-strategy.md,
/// 7): each scenario's outcome, settled ends, and what a scenario aims at.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Goal {
    /// Every request went whole to the server's top, every event to the
    /// client's, and each call was done on a connection kept, at both ends.
    Streamed { requests: Vec<Vec<Token>>, events: usize, by: Time },
    /// The client read the early error's status and document, and both ends
    /// were done with a connection not kept; the server read none of the
    /// body.
    Answered { status: u16, document: Vec<Token>, by: Time },
    /// The end that did not close heard of it: the client an outcome for
    /// each call it made, the server the connection's end.
    Heard { client: bool, by: Time },
    /// A slow user at one end held the other's write back.
    Held { hold: Hold, by: Time },
    /// Every user closed, and with it every machine.
    Settled { by: Time },
}

impl Expectation<Proc> for Goal {
    fn check(&self, now: Time, procs: &[Proc]) -> Result<bool, String> {
        let (client, server) = ends(procs);
        let (client, server) = (&client.seen, &server.seen);
        let met = match self {
            Goal::Streamed { requests, events, .. } => {
                let kept = vec![client_end::Outcome::Done(Reuse::Keep); requests.len()];
                let done = vec![server_end::Outcome::Done(server::Reuse::Keep); requests.len()];
                server.requests == *requests
                    && client.events.len() == *events
                    && client.outcomes == kept
                    && client.ended == vec![sse::Event::Ended; requests.len()]
                    && server.outcomes.starts_with(&done)
            }
            Goal::Answered { status, document, .. } => {
                if !server.requests.is_empty() {
                    return Err("the server read the body it answered before".into());
                }
                client.statuses == [*status]
                    && client.documents == [document.clone()]
                    && client.outcomes == [client_end::Outcome::Done(Reuse::Close)]
                    && server.outcomes == [server_end::Outcome::Done(server::Reuse::Close)]
            }
            Goal::Heard { client: true, .. } => client.calls > 0 && client.outcomes.len() == client.calls,
            Goal::Heard { client: false, .. } => {
                server.outcomes.last().is_some_and(|outcome| *outcome != server_end::Outcome::Done(server::Reuse::Keep))
            }
            Goal::Held { hold: Hold::Writer, .. } => writer_held(now, client, server),
            Goal::Held { hold: Hold::Upload, .. } => upload_held(now, client, server),
            Goal::Settled { .. } => {
                let (client, server) = ends(procs);
                client.settled() && server.settled()
            }
        };
        Ok(met)
    }

    fn deadline(&self) -> Time {
        match self {
            Goal::Streamed { by, .. }
            | Goal::Answered { by, .. }
            | Goal::Heard { by, .. }
            | Goal::Held { by, .. }
            | Goal::Settled { by } => *by,
        }
    }
}

/// Whether the server's user has waited a while, all of it while the
/// client's user did not read, for an event it wrote to go: what it sees
/// of a writer held back.
fn writer_held(now: Time, client: &client_end::Seen, server: &server_end::Seen) -> bool {
    let now = now.as_nanos();
    match (client.stalled, client.resumed, server.writing_since) {
        (Some(from), None, Some(since)) => since.max(from) + HELD <= now,
        _ => false,
    }
}

/// Whether the client's user has waited a while, all of it while the
/// server's user did not read the request, for room for its upload: what
/// it sees of an upload held back.
fn upload_held(now: Time, client: &client_end::Seen, server: &server_end::Seen) -> bool {
    let now = now.as_nanos();
    match (server.stalled, server.resumed, client.upload_since) {
        (Some(from), None, Some(since)) => since.max(from) + HELD <= now,
        _ => false,
    }
}

/// An event the client's user is to receive: its type, its data, and the
/// last event ID and reconnection time the reader shows once it came.
#[derive(Debug)]
struct Due {
    name: Vec<u8>,
    data: Vec<u8>,
    id: Vec<u8>,
    retry: Option<u64>,
}

/// The scenario's referee, a step machine beside the ends: it sees what
/// their users saw, and injects the reset.
struct Watch<'a> {
    scenario: &'a Scenario,
    settings: &'a Settings,
    seed: u64,
    goals: Expectations<Proc, Goal>,
    /// Every event the client's user is to receive, in order.
    events: Vec<Due>,
    /// The documents of the events, in order.
    documents: Vec<Vec<Token>>,
    /// Whether the wire was reset.
    reset: bool,
    fell: Fell,
    /// How many of the client's events, its documents, and the server's
    /// requests were checked so far: each is checked once, as it comes.
    checked_events: usize,
    checked_documents: usize,
    checked_requests: usize,
    /// The bytes of the events the client received, as the writer frames
    /// them.
    received: u64,
}

impl<'a> Watch<'a> {
    fn new(scenario: &'a Scenario, settings: &'a Settings, seed: u64) -> Watch<'a> {
        let by = at(deadline(scenario, settings));
        let mut goals = vec![Goal::Settled { by }];
        let expect = if settings.reset.is_some() { Expect::Partway } else { scenario.expect };
        match expect {
            Expect::Stream => {
                let events = scenario.answers.iter().map(Vec::len).sum();
                goals.push(Goal::Streamed { requests: scenario.requests.clone(), events, by });
            }
            Expect::Early => {
                let (status, body) = scenario.server.early.as_ref().expect("an early error");
                let document = skein_json_world::reference::parse(body, &json_limits_tokenizer()).tokens;
                goals.push(Goal::Answered { status: *status, document, by });
            }
            Expect::Partway => {
                // The end that did not close hears of it; after a reset,
                // both do.
                if scenario.client.close.is_none() {
                    goals.push(Goal::Heard { client: true, by });
                }
                if scenario.server.close.is_none() {
                    goals.push(Goal::Heard { client: false, by });
                }
            }
        }
        // What a slow user holds back, unless a reset cuts the wire first,
        // or the wire is too slow to fill the caps between the ends before
        // the user reads again: the other end is then held by the wire.
        let stall = match scenario.holds {
            Some(Hold::Writer) => scenario.client.stall,
            Some(Hold::Upload) => scenario.server.stall,
            None => None,
        };
        if let (Some(hold), Some((_, iterations))) = (scenario.holds, stall)
            && settings.reset.is_none()
            && iterations >= fill(settings) + 2 * HELD
        {
            goals.push(Goal::Held { hold, by });
        }
        let mut events = Vec::new();
        let mut documents = Vec::new();
        for (items, answer) in scenario.server.answers.iter().zip(&scenario.answers) {
            // A reader for each call: what it shows starts afresh.
            let mut id = Vec::new();
            let mut retry = None;
            for (item, (_, document)) in items.iter().zip(answer) {
                if let Some(this) = &item.id {
                    id = this.to_vec();
                }
                retry = item.retry.or(retry);
                events.push(Due { name: item.name.to_vec(), data: item.data.to_vec(), id: id.clone(), retry });
                documents.push(document.clone());
            }
        }
        Watch {
            scenario,
            settings,
            seed,
            goals: Expectations::new(seed, goals),
            events,
            documents,
            reset: false,
            fell: Fell::default(),
            checked_events: 0,
            checked_documents: 0,
            checked_requests: 0,
            received: 0,
        }
    }
}

impl fmt::Debug for Watch<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Watch").field("seed", &self.seed).field("goals", &self.goals).finish_non_exhaustive()
    }
}

impl Referee<Proc> for Watch<'_> {
    /// The reset, at its iteration: both ends' streams fail, as a wire cut
    /// under them does.
    fn act(&mut self, now: Time, procs: &mut [Proc]) {
        if self.reset || self.settings.reset.is_none_or(|reset| at(reset) > now) {
            return;
        }
        self.reset = true;
        let (client, server) = ends_mut(procs);
        self.fell.reset |= !client.bottom.is_closed() || !server.bottom.is_closed();
        client.bottom.fail(Fault::Reset);
        server.bottom.fail(Fault::Reset);
    }

    /// Safety, at every observation: what one top received is what the
    /// other sent, in order, each event with what the reader's face shows;
    /// and the writer is held to what the caps between them hold. Then the
    /// goals.
    fn observe(&mut self, now: Time, procs: &[Proc]) {
        let seed = self.seed;
        let (client_end, server_end) = ends(procs);
        let (client, server) = (&client_end.seen, &server_end.seen);
        assert!(client.events.len() <= self.events.len(), "seed {seed}: no event the server did not write");
        for index in self.checked_events..client.events.len() {
            let (name, data) = &client.events[index];
            let Due { name: expected_name, data: expected_data, id, retry } = &self.events[index];
            assert_eq!(name, expected_name, "seed {seed}: event {index}'s type");
            assert_eq!(data, expected_data, "seed {seed}: event {index}'s data, as the server wrote it");
            assert_eq!(
                client.ids[index],
                (id.clone(), *retry),
                "seed {seed}: the reader's last event ID and reconnection time once event {index} came"
            );
            if !id.is_empty() || retry.is_some() {
                self.fell.ids += 1;
            }
            self.received += framed_len(&server_end::Item {
                name: name.as_slice().into(),
                data: data.as_slice().into(),
                id: if id.is_empty() { None } else { Some(id.as_slice().into()) },
                retry: *retry,
            });
        }
        self.checked_events = client.events.len();
        // The documents of the events, when every response was one.
        if client.statuses.iter().all(|status| *status == 200) {
            for index in self.checked_documents..client.documents.len() {
                assert_eq!(client.documents[index], self.documents[index], "seed {seed}: event {index}'s document");
            }
            self.checked_documents = client.documents.len();
        }
        for index in self.checked_requests..server.requests.len() {
            assert!(
                self.scenario.requests[index].starts_with(&server.requests[index]),
                "seed {seed}: the server read the client's request {index}, token for token"
            );
        }
        self.checked_requests = server.requests.len();
        // Flow control: what the writer sent that the client has not read.
        let in_flight = server.framed.saturating_sub(self.received);
        let bound = self.settings.in_flight();
        assert!(
            in_flight <= bound,
            "seed {seed}: the writer ran {in_flight} bytes ahead of the reader, past the {bound} the caps hold"
        );
        self.fell.most_in_flight = self.fell.most_in_flight.max(in_flight);
        self.fell.writer_held |= writer_held(now, client, server);
        self.fell.upload_held |= upload_held(now, client, server);
        self.fell.upload_stopped |= client.upload_failed == Some(Fault::Other);
        self.fell.writer_failed |= server.writer_failed;
        self.fell.calls = self.fell.calls.max(client.calls);
        self.goals.observe(now, procs);
    }

    fn next_deadline(&self) -> Option<Time> {
        let reset = match self.settings.reset {
            Some(reset) if !self.reset => Some(at(reset)),
            Some(_) | None => None,
        };
        match (reset, self.goals.next_deadline()) {
            (Some(reset), Some(goal)) => Some(reset.min(goal)),
            (Some(next), None) | (None, Some(next)) => Some(next),
            (None, None) => None,
        }
    }

    fn overdue(&self, now: Time) -> Option<String> {
        self.goals.overdue(now)
    }

    fn passed(&self) -> bool {
        self.goals.passed()
    }
}

/// The JSON writer's limits each end writes its documents under.
#[must_use]
pub fn json_limits() -> skein_json::writer::Limits {
    skein_json::writer::Limits { depth: 16, length: 1 << 16 }
}

fn json_limits_tokenizer() -> skein_json::tokenizer::Limits {
    skein_json::tokenizer::Limits { depth: 16, string: 4096, number: 32, chunk: 64, length: 1 << 16 }
}
