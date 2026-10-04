//! A protocol world (testing-strategy.md, 2.5): both ends of an LLM
//! streaming exchange, built as two services would build them, joined by
//! bytes cut and joined at random, in one loop with a referee beside them.
//!
//! Each iteration, the wire carries a piece each way, now and then; each
//! end's stream below answers its stack, and each end's user acts, in an
//! order drawn from the seed; then the referee observes what the users
//! saw. Each machine's calls are held to its `MAX_OUT`, and each stream
//! below to the contract of a stream, as the world goes
//! (testing-strategy.md, 6). Once both ends are closed, every machine of
//! theirs is closed too. A seed replays to the same run.
//!
//! The referee (testing-strategy.md, 7) watches what the users saw, never
//! the machines: what the top of one end sent is what the top of the other
//! received, in order; and what the server's writer sent and the client's
//! user has not read is never more than the caps between them, so a slow
//! reader at the client's top stops the writer at the server's. It holds
//! each scenario to its outcome by the world's deadline.

use skein_http::client::{self, Reuse};
use skein_http::server;
use skein_http::sse;
use skein_json::Token;
use skein_lib::Rng;
use skein_lib::stream::Fault;

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
    /// When the wire resets, if it does: both ends' streams fail.
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
    /// The request's document, as the client's user writes it.
    pub request: Vec<Token>,
    /// Each event the server's user answers with: its type and its data's
    /// document.
    pub events: Vec<(Vec<u8>, Vec<Token>)>,
    pub client: client_end::Script,
    pub server: server_end::Script,
    pub expect: Expect,
    /// The least a closed end lingers, draining what comes, that the
    /// expectation needs: a response given before the upload ends reaches a
    /// client still uploading only if the server's end drains its upload
    /// meanwhile, as io's close does until its deadline (io.md, 3.3).
    pub linger: u64,
}

/// What the referee holds a scenario to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Expect {
    /// The request goes whole to the server's top, and every event to the
    /// client's, on a connection kept.
    Stream,
    /// The server answers the request at once with an error, while the
    /// client uploads: the upload stops, and the client reads the error.
    Early,
    /// An end closes, or the wire resets, partway: what came stands, and
    /// both ends settle.
    Partway,
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
    /// The server's writer waited for room while the client's user did not
    /// read: the slow reader held it back.
    pub writer_held: bool,
    /// The client's upload stopped as the response came first.
    pub upload_stopped: bool,
    /// The server's writer heard its stream fail.
    pub writer_failed: bool,
    /// A stream reset under an end still open.
    pub reset: bool,
    /// The most the writer had sent beyond what the client read.
    pub most_in_flight: u64,
}

/// Runs `scenario` with `settings`, from `seed`, until both ends are
/// closed, checking as it goes, and the referee's expectations once
/// settled.
#[must_use]
pub fn run(scenario: &Scenario, settings: &Settings, seed: u64) -> Run {
    let mut rng = Rng::new(seed);
    let mut client = client_end::End::new(
        settings.client,
        Bottom::new(settings.client_intake, settings.client_output),
        scenario.client.clone(),
    );
    let mut server = server_end::End::new(
        settings.server,
        Bottom::new(settings.server_intake, settings.server_output),
        scenario.server.clone(),
    );
    let mut referee =
        Referee { scenario, settings, seed, fell: Fell::default(), events: 0, documents: 0, requests: 0, received: 0 };
    let budget = 400_000;
    for now in 0..budget {
        // The wire, each way.
        let linger = settings.linger.max(scenario.linger);
        if rng.chance(settings.carry) {
            let piece = usize::try_from(rng.between(1, settings.piece as u64)).expect("fits a usize");
            referee.fell.reset |= wire::carry(&mut client.bottom, &mut server.bottom, piece, now, linger);
        }
        if rng.chance(settings.carry) {
            let piece = usize::try_from(rng.between(1, settings.piece as u64)).expect("fits a usize");
            referee.fell.reset |= wire::carry(&mut server.bottom, &mut client.bottom, piece, now, linger);
        }
        if settings.reset == Some(now) {
            referee.fell.reset |= !client.bottom.is_closed() || !server.bottom.is_closed();
            client.bottom.fail(Fault::Reset);
            server.bottom.fail(Fault::Reset);
        }
        // Each end: its stream below, then its user, in an order drawn.
        let grant_client = rng.chance(settings.grant);
        let grant_server = rng.chance(settings.grant);
        let client_acts = rng.chance(settings.eagerness);
        let server_acts = rng.chance(settings.eagerness);
        if rng.chance(500) {
            client.below_acts(grant_client);
            server.below_acts(grant_server);
        } else {
            server.below_acts(grant_server);
            client.below_acts(grant_client);
        }
        if client_acts {
            client.act(now);
        }
        if server_acts {
            server.act(now);
        }
        referee.observe(&client, &server);
        if client.settled() && server.settled() {
            referee.passed(&client, &server);
            return Run { client: client.seen, server: server.seen, fell: referee.fell, iterations: now };
        }
    }
    panic!(
        "seed {seed}: the world settles within {budget} iterations: client {:?}, server {:?}, {settings:?}",
        client.seen, server.seen
    );
}

/// The scenario's expectations, a step machine beside the ends: it sees
/// what their users saw.
struct Referee<'a> {
    scenario: &'a Scenario,
    settings: &'a Settings,
    seed: u64,
    fell: Fell,
    /// How many of the client's events, its documents, and the server's
    /// requests were checked so far: each is checked once, as it comes.
    events: usize,
    documents: usize,
    requests: usize,
    /// The bytes of the events the client received, as the writer frames
    /// them.
    received: u64,
}

impl Referee<'_> {
    /// Safety, at every observation: what one top received is what the
    /// other sent, in order; and the writer is held to what the caps
    /// between them hold.
    fn observe(&mut self, client: &client_end::End, server: &server_end::End) {
        let seed = self.seed;
        let written = &self.scenario.server.events;
        assert!(client.seen.events.len() <= written.len(), "seed {seed}: no event the server did not write");
        for (index, (name, data)) in client.seen.events.iter().enumerate().skip(self.events) {
            let (expected_name, expected_data) = &written[index];
            assert_eq!(name[..], expected_name[..], "seed {seed}: event {index}'s type");
            assert_eq!(data[..], expected_data[..], "seed {seed}: event {index}'s data, as the server wrote it");
            self.received += framed_len(name, data);
        }
        self.events = client.seen.events.len();
        if client.seen.status == Some(200) {
            for index in self.documents..client.seen.documents.len() {
                let document = &client.seen.documents[index];
                assert_eq!(document, &self.scenario.events[index].1, "seed {seed}: event {index}'s document");
            }
            self.documents = client.seen.documents.len();
        }
        for index in self.requests..server.seen.requests.len() {
            assert!(
                self.scenario.request.starts_with(&server.seen.requests[index]),
                "seed {seed}: the server read the client's request, token for token"
            );
        }
        self.requests = server.seen.requests.len();
        // Flow control: what the writer sent that the client has not read.
        let in_flight = server.seen.framed.saturating_sub(self.received);
        let bound = self.settings.in_flight();
        assert!(
            in_flight <= bound,
            "seed {seed}: the writer ran {in_flight} bytes ahead of the reader, past the {bound} the caps hold"
        );
        self.fell.most_in_flight = self.fell.most_in_flight.max(in_flight);
        let stalled = client.seen.stalled.is_some() && client.seen.ended.is_none();
        self.fell.writer_held |= stalled && server.writer_blocked();
        self.fell.upload_stopped |= client.seen.upload_failed == Some(Fault::Other);
        self.fell.writer_failed |= server.seen.writer_failed;
    }

    /// Liveness, once settled: the scenario came to its outcome.
    fn passed(&self, client: &client_end::End, server: &server_end::End) {
        let seed = self.seed;
        let what = || format!("seed {seed}: client {:?}, server {:?}", client.seen, server.seen);
        match self.scenario.expect {
            Expect::Stream => {
                assert_eq!(
                    server.seen.requests,
                    std::slice::from_ref(&self.scenario.request),
                    "the request, whole; {}",
                    what()
                );
                assert_eq!(client.seen.status, Some(200), "{}", what());
                assert_eq!(client.seen.events.len(), self.scenario.events.len(), "every event; {}", what());
                assert_eq!(client.seen.ended, Some(sse::Event::Ended), "{}", what());
                assert_eq!(client.seen.outcome, Some(client_end::Outcome::Done(Reuse::Keep)), "{}", what());
                assert_eq!(
                    server.seen.outcomes.first(),
                    Some(&server_end::Outcome::Done(server::Reuse::Keep)),
                    "{}",
                    what()
                );
            }
            Expect::Early => {
                let (status, body) = self.scenario.server.early.as_ref().expect("an early error");
                assert_eq!(client.seen.status, Some(*status), "the error; {}", what());
                assert!(server.seen.requests.is_empty(), "the body never read; {}", what());
                let tokens = skein_json_world::reference::parse(body, &json_limits_tokenizer()).tokens;
                assert_eq!(client.seen.documents, [tokens], "the error's document; {}", what());
                assert_eq!(client.seen.outcome, Some(client_end::Outcome::Done(Reuse::Close)), "{}", what());
                assert_eq!(server.seen.outcomes, [server_end::Outcome::Done(server::Reuse::Close)], "{}", what());
            }
            Expect::Partway => {}
        }
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
