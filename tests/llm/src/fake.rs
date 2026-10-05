//! Actual shared Client connected to the independently scripted HTTP/SSE peer.
//! Transport queues enforce demand/room and never decode provider bytes.

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_http::{server as http, sse::writer as sse};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue, Time, Token, Wall};
use skein_llm::{Call, Credential, Endpoint, Provider, client};

/// Deterministic fragmenting actual-client exchange, with bounded byte intakes.
#[expect(missing_debug_implementations, reason = "client and peer own credential-bearing HTTP state")]
pub struct Exchange {
    pub machine: client::Client,
    pub seen: Vec<client::Event>,
    pub queries: Vec<api::Query>,
    pub requests: Vec<u8>,
    pub responses: Vec<u8>,
    pub env: Env<client::Limits>,
    pub server: provider::Server,
    pub service: provider::Service,
    /// Holds actual issued calls for routing tests; normal stories use the scripted domain.
    pub manual_replies: bool,
    pub pending: Vec<fake::Event>,
    fake: fake::Domain,
    fake_env: Env<fake::Config>,
    peer_env: Env<provider::Limits>,
    credential: Credential,
    client_above: Queue<client::Event>,
    client_below: Queue<Down>,
    peer_above: Queue<provider::Event>,
    peer_below: Queue<Down>,
    replies: Queue<fake::Request>,
    to_client: Intake,
    to_peer: Intake,
    client_demand: Option<(Read, u32)>,
    peer_demand: Option<(Read, u32)>,
    client_grant: u32,
    peer_grant: u32,
    ticks: u32,
}

/// Shared peer limits compatible with this world's small client limits.
#[must_use]
pub fn limits(bounds: &client::Limits) -> provider::Limits {
    provider::Limits {
        calls: 4,
        http: http::Limits { head: 4096, headers: 32, body: 32768, read: 256, response: 4096, send: 128 },
        sse: sse::Limits { event: 16384, chunk: 128 },
        documents: documents::Limits { anthropic: bounds.dialect, openai: bounds.dialect, model_ceiling: 4096 },
    }
}

/// Script-domain configuration has no application tool/body vocabulary.
#[must_use]
pub fn config() -> fake::Config {
    fake::Config {
        calls: 4,
        query_bytes: 32768,
        script_bytes: 32768,
        answer_bytes: 32768,
        latency_min: skein_lib::Duration::ZERO,
        latency_max: skein_lib::Duration::ZERO,
        overloaded: 0,
        rate_limited: 0,
        retry_after: skein_lib::Duration::ZERO,
        unavailable: 0,
        too_long: 0,
        unauthorized: 0,
        refused: 0,
        no_calls: 0,
        answer_tokens: 1,
        calls_per_answer: 1,
        malformed: 0,
        tool_rounds: 0,
    }
}

impl Exchange {
    /// Validates the actual Client and peer independently; scripts are caller data.
    #[must_use]
    pub fn new(input: Call, bounds: client::Limits, scripts: Box<[api::Script]>) -> Self {
        let endpoint = input.endpoint.clone();
        let credential = Credential {
            access_token: input.credential.access_token.clone(),
            account_id: input.credential.account_id.clone(),
        };
        let machine = client::Client::prepare(input, &bounds).expect("actual shared Client admission");
        Self::prepared(machine, endpoint, credential, bounds, scripts)
    }

    /// Adopts the caller's one prepared Client without preparing another call.
    /// The endpoint, credential and unchanged limits must be those used for
    /// admission; they configure only the independent peer and its environment.
    /// Caller scripts are admitted under the shared script-domain bounds.
    ///
    /// The caller moves the Client and peer metadata here. Price the Client
    /// once, the peer's retained credential and service target separately,
    /// and any caller-retained application translation or observation storage.
    #[must_use]
    pub fn prepared(
        machine: client::Client,
        endpoint: Endpoint,
        credential: Credential,
        bounds: client::Limits,
        scripts: Box<[api::Script]>,
    ) -> Self {
        let provider = match endpoint.provider {
            Provider::OpenAiCodex => documents::Provider::OpenAi,
            Provider::Anthropic => documents::Provider::Anthropic,
        };
        let peer_limits = limits(&bounds);
        let service = provider::Service::new(
            provider::Config { provider, path: endpoint.target, headers: Box::new([]) },
            &peer_limits,
        )
        .expect("bounded independent peer");
        let server = provider::Server::new(Token::new(2), &peer_limits).expect("bounded peer connection");
        Self {
            machine,
            seen: Vec::new(),
            queries: Vec::new(),
            requests: Vec::new(),
            responses: Vec::new(),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: bounds },
            server,
            service,
            manual_replies: false,
            pending: Vec::new(),
            fake: fake::Domain::try_scripted(&config(), 3, scripts).expect("bounded caller scripts"),
            fake_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config() },
            peer_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: peer_limits },
            credential,
            client_above: Queue::with_capacity(client::MAX_OUT.above),
            client_below: Queue::with_capacity(client::MAX_OUT.below),
            peer_above: Queue::with_capacity(provider::MAX_UP),
            peer_below: Queue::with_capacity(provider::MAX_DOWN),
            replies: Queue::with_capacity(fake::MAX_OUT),
            to_client: Intake::with_capacity(32768),
            to_peer: Intake::with_capacity(32768),
            client_demand: None,
            peer_demand: None,
            client_grant: 0,
            peer_grant: 0,
            ticks: 0,
        }
    }

    /// Installs the caller's one iteration time in the actual Client, script
    /// domain and byte peer before their entries run. Call before `start` or
    /// the iteration's `request`, `tick`, `reply` and `settle` entries.
    ///
    /// The caller supplies nondecreasing monotonic `now`. `wall` may move
    /// independently in either direction and never arms a deadline. This
    /// replaces only time inputs: it does not start the exchange, fire timers,
    /// deliver bytes or settle lower effects.
    pub fn at(&mut self, now: Time, wall: Wall) {
        self.env.now = now;
        self.env.wall = wall;
        self.fake_env.now = now;
        self.fake_env.wall = wall;
        self.peer_env.now = now;
        self.peer_env.wall = wall;
    }

    pub fn start(&mut self) {
        provider::start(
            &mut self.server,
            &mut self.service,
            &self.credential,
            &self.peer_env,
            &mut self.peer_above,
            &mut self.peer_below,
        );
        client::down(
            &mut self.machine,
            &self.env,
            client::Request::Start,
            &mut self.client_above,
            &mut self.client_below,
        );
        self.take();
    }

    pub fn request(&mut self, request: client::Request) {
        client::down(&mut self.machine, &self.env, request, &mut self.client_above, &mut self.client_below);
        self.take();
    }

    pub fn settle(&mut self) {
        provider::closed(
            &mut self.server,
            &mut self.service,
            &self.peer_env,
            &mut self.peer_above,
            &mut self.peer_below,
        );
        client::closed(&mut self.machine, &self.env, &mut self.client_above, &mut self.client_below);
        self.take();
    }

    /// Receives the actual owned terminal after caller routing through the shared service.
    pub fn reply(&mut self, reply: fake::Request) {
        provider::down(
            &mut self.server,
            &mut self.service,
            &self.credential,
            &self.peer_env,
            reply,
            &mut self.peer_above,
            &mut self.peer_below,
        );
        self.take();
    }

    fn take(&mut self) {
        while let Some(event) = self.client_above.pop() {
            self.seen.push(event);
        }
        while let Some(event) = self.peer_above.pop() {
            match event {
                provider::Event::Domain(event) => {
                    match &event {
                        fake::Event::Call { query, .. } => self.queries.push(query.clone()),
                    }
                    if self.manual_replies {
                        self.pending.push(event);
                    } else {
                        fake::step(&mut self.fake, &self.fake_env, event, &mut self.replies);
                    }
                }
                provider::Event::Close | provider::Event::Closed => {}
            }
        }
        while let Some(down) = self.client_below.pop() {
            match down {
                Down::Demand { read: Read::Nothing, room: 0 } => self.client_demand = None,
                Down::Demand { read, room } => {
                    assert!(self.client_demand.is_none(), "prior client demand settled");
                    self.client_demand = Some((read, room));
                }
                Down::Send(data) => {
                    assert!(
                        self.client_grant >= u32::try_from(data.len()).expect("bounded send"),
                        "client send follows sufficient room"
                    );
                    self.client_grant = 0;
                    self.requests.extend_from_slice(&data);
                    self.to_peer.append(&data).expect("bounded client wire intake");
                }
                Down::Finish => panic!("HTTP client never finishes this lower stream"),
            }
        }
        while let Some(down) = self.peer_below.pop() {
            match down {
                Down::Demand { read: Read::Nothing, room: 0 } => self.peer_demand = None,
                Down::Demand { read, room } => {
                    assert!(self.peer_demand.is_none(), "prior peer demand settled");
                    self.peer_demand = Some((read, room));
                }
                Down::Send(data) => {
                    assert!(
                        self.peer_grant >= u32::try_from(data.len()).expect("bounded send"),
                        "peer send follows sufficient room"
                    );
                    self.peer_grant = 0;
                    self.responses.extend_from_slice(&data);
                    self.to_client.append(&data).expect("bounded peer wire intake");
                }
                Down::Finish => panic!("peer response is framed without finishing the lower stream"),
            }
        }
    }

    /// One bounded local step or one actual room/read delivery. No provider parsing lives here.
    pub fn tick(&mut self, demand: bool) -> bool {
        self.ticks += 1;
        if self.machine.has_work() {
            client::resume(&mut self.machine, &self.env, &mut self.client_above, &mut self.client_below);
            self.take();
            return true;
        }
        if self.server.has_work() {
            provider::resume(
                &mut self.server,
                &mut self.service,
                &self.credential,
                &self.peer_env,
                &mut self.peer_above,
                &mut self.peer_below,
            );
            self.take();
            return true;
        }
        if self.fake.is_due(self.fake_env.now) {
            fake::fire(&mut self.fake, &self.fake_env, &mut self.replies);
        }
        if let Some(reply) = self.replies.pop() {
            provider::down(
                &mut self.server,
                &mut self.service,
                &self.credential,
                &self.peer_env,
                reply,
                &mut self.peer_above,
                &mut self.peer_below,
            );
            self.fake.reclaim();
            self.take();
            return true;
        }
        if demand && self.machine.waiting() == client::Waiting::Next {
            self.request(client::Request::Next);
            return true;
        }
        if let Some((read, room)) = self.client_demand {
            let event = if room > 0 && self.to_peer.room() >= room {
                self.client_grant = room;
                Some(Up::Room)
            } else if room == 0 {
                self.to_client.meet(read).map(Up::Bytes)
            } else {
                None
            };
            if let Some(event) = event {
                self.client_demand = None;
                client::up(&mut self.machine, &self.env, event, &mut self.client_above, &mut self.client_below);
                self.take();
                return true;
            }
        }
        if let Some((read, room)) = self.peer_demand {
            let event = if room > 0 && self.to_client.room() >= room {
                self.peer_grant = room;
                Some(Up::Room)
            } else if room == 0 {
                self.to_peer.meet(read).map(Up::Bytes)
            } else {
                None
            };
            if let Some(event) = event {
                self.peer_demand = None;
                provider::up(
                    &mut self.server,
                    &mut self.service,
                    &self.credential,
                    &self.peer_env,
                    event,
                    &mut self.peer_above,
                    &mut self.peer_below,
                );
                self.take();
                return true;
            }
        }
        false
    }

    /// Drives the actual stack through complete HTTP drainage; never synthesizes a terminal.
    pub fn run(&mut self) {
        for _ in 0..100_000_u32 {
            if !self.tick(true) {
                assert!(
                    self.machine.waiting() == client::Waiting::Idle
                        || self.machine.waiting() == client::Waiting::Closing,
                    "wire story ended only after actual protocol progress: {:?}",
                    self.machine.waiting()
                );
                return;
            }
        }
        panic!("actual client/peer story exceeded its bounded steps");
    }
}

#[cfg(test)]
mod tests {
    use super::Exchange;
    use skein_fake_llm_domain::api;
    use skein_lib::{Duration, Time, Token, Wall};
    use skein_llm::{Block, Credential, Endpoint, Provider, client};

    const START: Time = Time::from_nanos(10_000_000_000);
    const DUE: Time = Time::from_nanos(11_000_000_000);

    fn delayed(provider: Provider) -> Exchange {
        let mut input = crate::call(19);
        match provider {
            Provider::OpenAiCodex => {}
            Provider::Anthropic => {
                input.endpoint = Endpoint::anthropic();
                input.credential = Credential::anthropic(b"fake-token".as_slice().into());
                input.prompt.cache_key = None;
            }
        }
        input.prompt.output_ceiling(provider, 4096).expect("shared provider configuration");
        input.prompt.instructions = b"clocked-script".as_slice().into();
        let endpoint = input.endpoint.clone();
        let credential = Credential {
            access_token: input.credential.access_token.clone(),
            account_id: input.credential.account_id.clone(),
        };
        let bounds = crate::limits();
        let machine = client::Client::prepare(input, &bounds).expect("one actual adopted Client");
        let scripts = Box::new([api::Script {
            cue: b"clocked-script".as_slice().into(),
            turns: Box::new([api::Turn {
                lines: Box::new([api::Line::Text { text: b"clocked exact".as_slice().into() }]),
                finish: api::Finish::Stop,
                tokens: 1,
            }]),
        }]);
        let mut world = Exchange::prepared(machine, endpoint, credential, bounds, scripts);
        // Configure only latency at startup, before any step. The Domain's
        // admitted capacities and owned-byte bounds remain unchanged.
        world.fake_env.limits.latency_min = Duration::from_secs(1);
        world.fake_env.limits.latency_max = Duration::from_secs(1);
        world
    }

    fn quiesce(world: &mut Exchange) {
        for _ in 0..100_000_u32 {
            if !world.tick(true) {
                return;
            }
        }
        panic!("clocked exchange exceeded bounded steps");
    }

    fn terminals(world: &Exchange) -> usize {
        world
            .seen
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    client::Event::Completed { .. } | client::Event::Failed { .. } | client::Event::Cancelled { .. }
                )
            })
            .count()
    }

    fn check_times(world: &Exchange, now: Time, wall: Wall) {
        assert_eq!((world.env.now, world.env.wall), (now, wall));
        assert_eq!((world.fake_env.now, world.fake_env.wall), (now, wall));
        assert_eq!((world.peer_env.now, world.peer_env.wall), (now, wall));
    }

    fn awaiting_answer(world: &mut Exchange) {
        world.at(START, Wall::from_nanos(50_000_000_000));
        check_times(world, START, Wall::from_nanos(50_000_000_000));
        assert_eq!(world.machine.waiting(), client::Waiting::Start);
        assert!(!world.machine.has_work());
        assert!(!world.server.has_work());
        assert!(world.queries.is_empty());
        assert!(world.requests.is_empty());
        assert!(world.responses.is_empty());
        assert!(world.seen.is_empty(), "installing time produces no events");
        assert_eq!(world.fake.calls(), 0);
        world.start();
        quiesce(world);
        assert_eq!(world.queries.len(), 1, "one actual request entered the script Domain");
        assert_eq!(world.queries[0].system.as_ref(), b"clocked-script");
        assert!(!world.requests.is_empty(), "the adopted Client sent real HTTP bytes");
        assert_eq!(world.fake.calls(), 1);
        assert_eq!(world.fake.next_deadline(), Some(DUE), "deadline uses the nonzero injected origin");
        assert!(world.responses.is_empty());
        assert_eq!(terminals(world), 0);
    }

    #[test]
    fn shared_iteration_time_drives_delayed_actual_completion_and_lower_settlement() {
        for provider in [Provider::OpenAiCodex, Provider::Anthropic] {
            let mut world = delayed(provider);
            awaiting_answer(&mut world);
            let before = Time::from_nanos(10_999_999_999);
            let forward = Wall::from_nanos(500_000_000_000);
            world.at(before, forward);
            check_times(&world, before, forward);
            assert!(!world.tick(true), "wall jump cannot make the monotonic timer due");
            assert!(world.responses.is_empty());
            assert_eq!(terminals(&world), 0);
            assert_eq!(world.fake.calls(), 1);

            world.at(DUE, Wall::EPOCH);
            check_times(&world, DUE, Wall::EPOCH);
            assert_eq!(world.fake.calls(), 1, "installing due time does not fire the timer");
            assert!(world.responses.is_empty());
            assert_eq!(terminals(&world), 0);
            world.run();
            assert_eq!(terminals(&world), 1);
            let completion = world.seen.iter().find_map(|event| match event {
                client::Event::Completed { owner, completion } => {
                    assert_eq!(*owner, Token::new(19));
                    Some(completion)
                }
                client::Event::Delta { .. }
                | client::Event::Block { .. }
                | client::Event::Failed { .. }
                | client::Event::Cancelled { .. }
                | client::Event::Reusable
                | client::Event::Close
                | client::Event::Closed => None,
            });
            let completion = completion.expect("one actual successful terminal");
            let [Block::Text { text, .. }] = completion.content.as_ref() else {
                panic!("one actual scripted text block");
            };
            assert_eq!(text.as_ref(), b"clocked exact");
            assert!(!world.responses.is_empty(), "actual byte peer encoded the delayed answer");
            assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Reusable)).count(), 1);
            assert_eq!(world.fake.calls(), 0, "actual delayed output was delivered and reclaimed");
            assert_eq!(world.fake.next_deadline(), None);

            world.request(client::Request::Close);
            assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Close)).count(), 1);
            assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 0);
            world.settle();
            assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
            let events = world.seen.len();
            world.settle();
            assert_eq!(world.seen.len(), events, "repeated lower settlement emits nothing");
            assert_eq!(terminals(&world), 1);
        }
    }

    #[test]
    fn installing_due_time_leaves_cancel_before_tick_and_late_fake_completion_ordered() {
        for provider in [Provider::OpenAiCodex, Provider::Anthropic] {
            let mut world = delayed(provider);
            awaiting_answer(&mut world);
            world.at(DUE, Wall::EPOCH);
            assert_eq!(world.fake.calls(), 1);
            assert_eq!(world.fake.next_deadline(), Some(DUE));
            assert!(world.responses.is_empty());
            assert_eq!(terminals(&world), 0);
            world.request(client::Request::Cancel);
            assert_eq!(terminals(&world), 0, "Cancel waits for actual lower settlement");
            assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Close)).count(), 1);
            world.settle();
            assert_eq!(terminals(&world), 1);
            assert_eq!(
                world
                    .seen
                    .iter()
                    .filter(|event| matches!(event, client::Event::Cancelled { owner } if *owner == Token::new(19)))
                    .count(),
                1
            );
            assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
            assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Reusable)).count(), 0);
            let events = world.seen.len();
            let responses = world.responses.len();
            assert_eq!(world.fake.calls(), 1, "lower settlement did not fire the independent fake");
            quiesce(&mut world);
            assert_eq!(world.fake.calls(), 0, "late actual fake terminal was fired and reclaimed");
            assert_eq!(world.fake.next_deadline(), None);
            assert_eq!(world.responses.len(), responses, "closed peer cannot encode a late answer");
            assert_eq!(world.seen.len(), events, "late fake completion cannot emit another client terminal");
            world.settle();
            assert_eq!(world.seen.len(), events, "repeated settlement remains inert");
        }
    }
}
