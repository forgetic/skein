//! Actual shared Client connected to the independently scripted HTTP/SSE peer.
//! Transport queues enforce demand/room and never decode provider bytes.

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_http::{server as http, sse::writer as sse};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue, Time, Token, Wall};
use skein_llm::{Call, Credential, Provider, client};

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
        let credential = Credential {
            access_token: input.credential.access_token.clone(),
            account_id: input.credential.account_id.clone(),
        };
        let provider = match input.endpoint.provider {
            Provider::OpenAiCodex => documents::Provider::OpenAi,
            Provider::Anthropic => documents::Provider::Anthropic,
        };
        let peer_limits = limits(&bounds);
        let service = provider::Service::new(
            provider::Config { provider, path: input.endpoint.target.clone(), headers: Box::new([]) },
            &peer_limits,
        )
        .expect("bounded independent peer");
        let server = provider::Server::new(Token::new(2), &peer_limits).expect("bounded peer connection");
        Self {
            machine: client::Client::prepare(input, &bounds).expect("actual shared Client admission"),
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
