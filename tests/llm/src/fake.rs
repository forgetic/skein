//! Actual shared Client connected to the independently scripted HTTP/SSE peer.
//! Transport queues enforce demand/room and never decode provider bytes.
//! Keeps peer service/script state, grants, intakes and caller observations;
//! knows neither application tool policy nor live IO. Preparation adopts one
//! Client, `observe` selects optional fixed observation caps before progress,
//! and start/tick/request/settle drive actual lower chronology. `extra_worst_case`
//! prices peer/world ownership separately from that Client and caller inputs.
//! Contract: docs/design/fake-llm.md, sections 2–5;
//! programming-model.md, sections 4.4 and 6.3.

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_http::{server as http, sse::writer as sse};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue, Time, Token, Wall};
use skein_llm::{Call, Credential, Endpoint, Provider, client};

/// Caller-selected finite observation storage for an actual byte-peer story.
/// Install before progress; callers may drain or `mem::take` records and price
/// handed-off storage separately. Buffers remaining in Exchange retain their
/// configured capacities; the next entry reserves a taken buffer again.
/// Exhaustion asserts before a copy or append, without a fabricated terminal.
/// Contract: docs/design/fake-llm.md,
/// sections 2–5; programming-model.md, section 6.3.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ObservationLimits {
    /// Maximum actual request heads retained beside query observations.
    pub heads: u32,
    /// Maximum owned header wrappers, names and values in each retained head.
    pub head_bytes: u64,
    /// Maximum currently retained Client event wrappers, supplied by the caller.
    /// Contract: docs/design/fake-llm.md, sections 2–5.
    pub events: u32,

    /// Maximum owned payload of each retained Client event, including replay
    /// token arrays and bytes. Contract: docs/design/fake-llm.md, sections 2–5.
    pub event_bytes: u64,

    /// Maximum currently retained independently decoded query copies.
    /// Contract: docs/design/fake-llm.md, sections 2–5.
    pub queries: u32,

    /// Maximum owned wrappers and bytes of each copied query, checked before
    /// cloning. Contract: docs/design/fake-llm.md, sections 2–5.
    pub query_bytes: u64,

    /// Maximum currently retained manual call records; zero admits no manual
    /// observation. Each query fits `query_bytes` before retention.
    /// Contract: docs/design/fake-llm.md, sections 2–5.
    pub pending: u32,

    /// Maximum retained actual client-to-peer HTTP bytes, checked before append.
    /// Contract: docs/design/fake-llm.md, sections 2–5.
    pub request_bytes: u32,

    /// Maximum retained actual peer-to-client HTTP bytes, checked before append.
    /// Contract: docs/design/fake-llm.md, sections 2–5.
    pub response_bytes: u32,
}

/// Checked peer/world heap beyond the caller's one actual Client. Includes the
/// provider Service/Server, scripted Domain, fixed queues, intakes and delivery
/// scratch, peer grant/target ownership and bounded observations. The Client,
/// caller input/configuration copies and records drained to callers are priced
/// by their owners. Endpoint fields other than target are not retained by this
/// peer. Contract: docs/design/fake-llm.md, sections 2–5;
/// programming-model.md, section 6.3.
#[must_use]
pub fn extra_worst_case(
    bounds: &client::Limits,
    observations: &ObservationLimits,
    endpoint: &Endpoint,
    credential: &Credential,
) -> Option<u64> {
    let peer = limits(bounds);
    if bounds.http.request.max(bounds.http.send) > 32768 || bounds.http.head.max(bounds.http.read) > 32768 {
        return None;
    }
    let queues = Queue::<client::Event>::worst_case(client::MAX_OUT.above)?
        .checked_add(Queue::<Down>::worst_case(client::MAX_OUT.below)?)?
        .checked_add(Queue::<provider::Event>::worst_case(provider::MAX_UP)?)?
        .checked_add(u64::from(provider::MAX_UP).checked_mul(provider::head_worst_case(&peer)?)?)?
        .checked_add(Queue::<Down>::worst_case(provider::MAX_DOWN)?)?
        .checked_add(Queue::<fake::Request>::worst_case(fake::MAX_OUT)?)?
        .checked_add(u64::from(fake::MAX_OUT).checked_mul(u64::from(config().answer_bytes))?)?;
    let traces = observation_worst_case(observations)?;
    // The fixed intakes also bound a delivered box while it enters either
    // receiver. Queue-owned wire sends can coexist with those deliveries.
    let wire = Intake::worst_case(32768)?
        .checked_mul(4)?
        .checked_add(
            u64::from(client::MAX_OUT.below).checked_mul(u64::from(bounds.http.request.max(bounds.http.send)))?,
        )?
        .checked_add(u64::from(provider::MAX_DOWN).checked_mul(u64::from(peer.http.response.max(peer.http.send)))?)?;
    provider::worst_case(&peer)?
        .checked_add(fake::worst_case(&config())?)?
        .checked_add(queues)?
        .checked_add(wire)?
        .checked_add(traces)?
        .checked_add(bytes(&endpoint.target)?)?
        .checked_add(bytes(&credential.access_token)?)?
        .checked_add(bytes(&credential.account_id)?)
}

fn observation_worst_case(observations: &ObservationLimits) -> Option<u64> {
    cells::<client::Event>(observations.events)?
        .checked_add(cells::<Box<[skein_http::Header]>>(observations.heads)?)?
        .checked_add(u64::from(observations.heads).checked_mul(observations.head_bytes)?)?
        .checked_add(u64::from(observations.events).checked_mul(observations.event_bytes)?)?
        .checked_add(cells::<api::Query>(observations.queries)?)?
        .checked_add(u64::from(observations.queries).checked_mul(observations.query_bytes)?)?
        .checked_add(cells::<fake::Event>(observations.pending)?)?
        .checked_add(u64::from(observations.pending).checked_mul(observations.query_bytes)?)?
        .checked_add(u64::from(observations.request_bytes))?
        .checked_add(u64::from(observations.response_bytes))
}

fn cells<T>(count: u32) -> Option<u64> {
    u64::from(count).checked_mul(u64::try_from(size_of::<T>()).ok()?)
}

fn bytes(value: &[u8]) -> Option<u64> {
    u64::try_from(value.len()).ok()
}

fn index(count: u32) -> usize {
    usize::try_from(count).expect("u32 fits usize")
}

fn reserve<T>(items: &mut Vec<T>, count: u32) {
    let capacity = index(count);
    assert!(items.len() <= capacity, "caller preserves observation lengths");
    assert!(
        items.capacity() == 0 || items.capacity() == capacity,
        "caller drains without replacing observation capacity"
    );
    if items.capacity() == 0 {
        items.reserve_exact(capacity);
    }
}

fn query_bytes(query: &api::Query) -> Option<u64> {
    let mut owned = bytes(&query.model)?
        .checked_add(bytes(&query.system)?)?
        .checked_add(cells::<api::ToolSpec>(u32::try_from(query.tools.len()).ok()?)?)?
        .checked_add(cells::<api::Message>(u32::try_from(query.messages.len()).ok()?)?)?;
    match &query.choice {
        api::ToolChoice::Auto | api::ToolChoice::None => {}
        api::ToolChoice::Only(names) => {
            owned = owned.checked_add(cells::<Box<[u8]>>(u32::try_from(names.len()).ok()?)?)?;
            for name in names {
                owned = owned.checked_add(bytes(name)?)?;
            }
        }
    }
    for tool in &query.tools {
        owned = owned
            .checked_add(bytes(&tool.name)?)?
            .checked_add(bytes(&tool.description)?)?
            .checked_add(bytes(&tool.parameters)?)?;
    }
    for message in &query.messages {
        owned = owned.checked_add(cells::<api::Part>(u32::try_from(message.parts.len()).ok()?)?)?;
        for part in &message.parts {
            let payload = match part {
                api::Part::Text { text } => bytes(text)?,
                api::Part::Opaque { bytes: value } => bytes(value)?,
                api::Part::ToolCall { id, name, arguments } => {
                    bytes(id)?.checked_add(bytes(name)?)?.checked_add(bytes(arguments)?)?
                }
                api::Part::ToolOutput { id, output, is_error: _ } => bytes(id)?.checked_add(bytes(output)?)?,
            };
            owned = owned.checked_add(payload)?;
        }
    }
    Some(owned)
}

fn replay_bytes(replay: Option<&skein_llm::Replay>) -> Option<u64> {
    let Some(replay) = replay else { return Some(0) };
    let document = replay.value.document();
    let owned = cells::<skein_json::Compact>(document.len())?.checked_add(u64::from(document.text_len()))?;
    Some(owned)
}

fn block_bytes(block: &skein_llm::Block) -> Option<u64> {
    match block {
        skein_llm::Block::Text { text, replay } | skein_llm::Block::Refusal { text, replay } => {
            bytes(text)?.checked_add(replay_bytes(replay.as_ref())?)
        }
        skein_llm::Block::ToolCall { id, name, arguments, replay } => bytes(id)?
            .checked_add(bytes(name)?)?
            .checked_add(bytes(arguments)?)?
            .checked_add(replay_bytes(replay.as_ref())?),
        skein_llm::Block::ToolResult { id, text, is_error: _ } => bytes(id)?.checked_add(bytes(text)?),
        skein_llm::Block::Oversize { id, name, .. } => bytes(id)?.checked_add(bytes(name)?),
        skein_llm::Block::Cut { id, name, arguments } => {
            bytes(id)?.checked_add(bytes(name)?)?.checked_add(bytes(arguments)?)
        }
        skein_llm::Block::Reasoning { replay } => replay_bytes(Some(replay)),
        skein_llm::Block::Dropped { .. } => Some(0),
    }
}

fn event_bytes(event: &client::Event) -> Option<u64> {
    match event {
        client::Event::Completed { completion, .. } => {
            let mut owned = cells::<skein_llm::Block>(u32::try_from(completion.content.len()).ok()?)?;
            for block in &completion.content {
                owned = owned.checked_add(block_bytes(block)?)?;
            }
            Some(owned)
        }
        client::Event::Block { block, .. } => block_bytes(block),
        client::Event::Delta { delta, .. } => match delta {
            skein_llm::Delta::Text { text, .. } | skein_llm::Delta::Reasoning { text, .. } => bytes(text),
            skein_llm::Delta::ToolArguments { delta, .. } => bytes(delta),
        },
        client::Event::Failed { detail, .. } => bytes(detail),
        client::Event::Cancelled { .. } | client::Event::Reusable | client::Event::Close | client::Event::Closed => {
            Some(0)
        }
    }
}

/// Deterministic fragmenting actual-client exchange, with bounded byte intakes.
#[expect(missing_debug_implementations, reason = "client and peer own credential-bearing HTTP state")]
pub struct Exchange {
    pub machine: client::Client,
    pub seen: Vec<client::Event>,
    pub queries: Vec<api::Query>,
    /// Actual ordered HTTP field names and values, before their decoded queries.
    pub heads: Vec<Box<[skein_http::Header]>>,
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

    observations: Option<ObservationLimits>,
}

/// Shared peer limits compatible with this world's small client limits.
#[must_use]
pub fn limits(bounds: &client::Limits) -> provider::Limits {
    // The peer may write past a client's receiving answer ceiling. Price that
    // independent bounded storage through provider::worst_case, including in
    // extra_worst_case; never make the test peer pre-filter an oversized call.
    let documents =
        skein_llm::DocumentLimits { answer_bytes: bounds.dialect.answer_bytes.max(32768), ..bounds.dialect };
    provider::Limits {
        calls: 4,
        http: http::Limits { head: 4096, headers: 32, body: 32768, read: 256, response: 4096, send: 128 },
        sse: sse::Limits { event: 16384, chunk: 128 },
        documents: documents::Limits { anthropic: documents, openai: documents, model_ceiling: 4096 },
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
        outside_choice: 0,
        tool_rounds: 0,
    }
}

impl Exchange {
    /// Validates the actual Client and peer independently; scripts are caller data.
    #[must_use]
    pub fn new(input: Call, bounds: client::Limits, scripts: Box<[api::Script]>) -> Self {
        Self::new_with_codex_echo(input, bounds, scripts, skein_llm::openai::Echo::NONE)
    }

    /// Construct one actual exchange with configured Codex metadata echoes.
    #[must_use]
    pub fn new_with_codex_echo(
        input: Call,
        bounds: client::Limits,
        scripts: Box<[api::Script]>,
        echo: skein_llm::openai::Echo,
    ) -> Self {
        Self::new_configured(
            input,
            bounds,
            scripts,
            documents::Options { echo, usage_fields: documents::UsageFields::ALL },
        )
    }

    /// Construct an exchange with the independent peer's echo and usage reporting.
    #[must_use]
    pub fn new_configured(
        input: Call,
        bounds: client::Limits,
        scripts: Box<[api::Script]>,
        options: documents::Options,
    ) -> Self {
        let endpoint = input.endpoint.clone();
        let credential = Credential {
            access_token: input.credential.access_token.clone(),
            account_id: input.credential.account_id.clone(),
        };
        let machine = client::Client::prepare(input, &bounds).expect("actual shared Client admission");
        Self::prepared_with_options(machine, endpoint, credential, bounds, scripts, options)
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
        Self::prepared_with_codex_echo(machine, endpoint, credential, bounds, scripts, skein_llm::openai::Echo::NONE)
    }

    /// Adopt the prepared Client with independently configured Codex echoes.
    #[must_use]
    pub fn prepared_with_codex_echo(
        machine: client::Client,
        endpoint: Endpoint,
        credential: Credential,
        bounds: client::Limits,
        scripts: Box<[api::Script]>,
        echo: skein_llm::openai::Echo,
    ) -> Self {
        Self::prepared_with_options(
            machine,
            endpoint,
            credential,
            bounds,
            scripts,
            documents::Options { echo, usage_fields: documents::UsageFields::ALL },
        )
    }

    /// Adopt the prepared Client under the peer's explicit reporting configuration.
    #[must_use]
    pub fn prepared_with_options(
        machine: client::Client,
        endpoint: Endpoint,
        credential: Credential,
        bounds: client::Limits,
        scripts: Box<[api::Script]>,
        options: documents::Options,
    ) -> Self {
        let provider = match endpoint.provider {
            Provider::OpenAiCodex => documents::Provider::OpenAi,
            Provider::Anthropic => documents::Provider::Anthropic,
        };
        let peer_limits = limits(&bounds);
        let service = provider::Service::new(
            provider::Config {
                usage_fields: options.usage_fields,
                echo: options.echo,
                provider,
                path: endpoint.target,
                headers: Box::new([]),
            },
            &peer_limits,
        )
        .expect("bounded independent peer");
        let server = provider::Server::new(Token::new(2), &peer_limits).expect("bounded peer connection");
        Self {
            machine,
            seen: Vec::new(),
            queries: Vec::new(),
            heads: Vec::new(),
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
            observations: None,
        }
    }

    /// Selects finite observation ownership before Start, without starting or
    /// firing any component. The caller reserves these buffers once; draining
    /// with `mem::take` is supported and the next entry reserves replacements.
    /// Price `extra_worst_case` separately from the actual Client. Existing
    /// constructors keep their unconstrained observation behaviour until this
    /// entrance is called. Contract: docs/design/fake-llm.md, sections 2–5;
    /// programming-model.md, section 6.3.
    pub fn observe(&mut self, observations: ObservationLimits) {
        assert_eq!(self.machine.waiting(), client::Waiting::Start, "observation ownership precedes Start");
        assert_eq!(self.ticks, 0, "observation ownership precedes progress");
        assert!(self.observations.is_none(), "observation limits are immutable");
        assert!(self.seen.is_empty() && self.queries.is_empty() && self.heads.is_empty() && self.pending.is_empty());
        assert!(self.requests.is_empty() && self.responses.is_empty());
        // Validate all container/payload products before reserving storage.
        observation_worst_case(&observations).expect("finite configured peer observation ownership");
        self.observations = Some(observations);
        self.reserve_observations();
    }

    /// Sets the independent Codex model's output ceiling before any wire progress.
    pub fn model_ceiling(&mut self, ceiling: u32) {
        assert_eq!(self.ticks, 0, "model configuration precedes progress");
        assert!(ceiling > 0, "the independent model has a nonzero ceiling");
        self.peer_env.limits.documents.model_ceiling = ceiling;
    }

    fn reserve_observations(&mut self) {
        if let Some(observations) = self.observations {
            reserve(&mut self.seen, observations.events);
            reserve(&mut self.queries, observations.queries);
            reserve(&mut self.heads, observations.heads);
            reserve(&mut self.pending, observations.pending);
            reserve(&mut self.requests, observations.request_bytes);
            reserve(&mut self.responses, observations.response_bytes);
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
        self.reserve_observations();
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
        self.reserve_observations();
        client::down(&mut self.machine, &self.env, request, &mut self.client_above, &mut self.client_below);
        self.take();
    }

    pub fn settle(&mut self) {
        self.reserve_observations();
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
        self.reserve_observations();
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

    fn observe_head(&mut self, headers: Box<[skein_http::Header]>) {
        if let Some(observations) = self.observations {
            assert!(self.heads.len() < index(observations.heads), "head observation ceiling");
            assert!(
                provider::head_bytes(&headers).is_some_and(|bytes| bytes <= observations.head_bytes),
                "head owned-byte ceiling before retention"
            );
        }
        self.heads.push(headers);
    }

    fn take(&mut self) {
        self.reserve_observations();
        while let Some(event) = self.client_above.pop() {
            if let Some(observations) = self.observations {
                assert!(self.seen.len() < index(observations.events), "Client event observation ceiling");
                assert!(
                    event_bytes(&event).is_some_and(|bytes| bytes <= observations.event_bytes),
                    "Client event owned-byte ceiling"
                );
            }
            self.seen.push(event);
        }
        while let Some(event) = self.peer_above.pop() {
            match event {
                provider::Event::Head { headers } => self.observe_head(headers),
                provider::Event::Domain(event) => {
                    match &event {
                        fake::Event::Call { query, .. } => {
                            if let Some(observations) = self.observations {
                                assert!(self.queries.len() < index(observations.queries), "query observation ceiling");
                                assert!(
                                    query_bytes(query).is_some_and(|bytes| bytes <= observations.query_bytes),
                                    "query owned-byte ceiling before cloning"
                                );
                                if self.manual_replies {
                                    assert!(
                                        self.pending.len() < index(observations.pending),
                                        "manual call observation ceiling"
                                    );
                                }
                            }
                            self.queries.push(query.clone());
                        }
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
                    if let Some(observations) = self.observations {
                        assert!(
                            self.requests
                                .len()
                                .checked_add(data.len())
                                .is_some_and(|length| length <= index(observations.request_bytes)),
                            "request tape ceiling before append"
                        );
                    }
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
                    if let Some(observations) = self.observations {
                        assert!(
                            self.responses
                                .len()
                                .checked_add(data.len())
                                .is_some_and(|length| length <= index(observations.response_bytes)),
                            "response tape ceiling before append"
                        );
                    }
                    self.responses.extend_from_slice(&data);
                    self.to_client.append(&data).expect("bounded peer wire intake");
                }
                Down::Finish => panic!("peer response is framed without finishing the lower stream"),
            }
        }
        self.service.reclaim();
    }

    /// One bounded local step or one actual room/read delivery. No provider parsing lives here.
    pub fn tick(&mut self, demand: bool) -> bool {
        self.reserve_observations();
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
