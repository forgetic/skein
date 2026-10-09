//! A real connection client and independent fake LLM process hosted by
//! Skein's world kit on simulated loopback (llm-connection.md, section 8;
//! fake-llm.md, section 3). The referee sees only owner events and peer
//! observations. Plaintext scenarios replay; TLS controls share this io
//! composition and the test trust chain without claiming replay.

use std::net::{Ipv4Addr, SocketAddr};

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_fake_peers::{Transport, llm};
use skein_io::{self as io, kernel};
use skein_lib::{Duration, Env, List, Queue, Time, Token, Wall};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, Request};
use skein_sim::Config;
use skein_world::{Host, Memory, Outcome, Referee, World};

const ROOM: u32 = 256;
const CUE: &[u8] = b"simulated-world";

fn limits(calls: u32) -> Limits {
    Limits {
        endpoints: 1,
        connections: calls,
        calls,
        per_endpoint: calls,
        idle_keep: Duration::from_secs(300),
        io: io::Limits {
            sockets: calls,
            refusals: 1,
            intake: 32_768,
            receive: 1024,
            output: 32_768,
            sends: 4,
            accepts: 1,
            backlog: 2,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        },
        tls: skein_tls::client::Limits { read: 4096, send: 4096, records: skein_tls::client::MAX_RECORD },
    }
}

/// The client's service loop; its public face is only the events its owner received.
pub struct Client {
    io: io::Io,
    limits: Limits,
    component: Option<Component>,
    address: Option<SocketAddr>,
    transport: Transport,
    io_events: Queue<io::Event>,
    events: Queue<Event>,
    requests: Queue<io::Request>,
    submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    received: Vec<Event>,
    calls: u32,
    input_bytes: usize,
    completed: u32,
    closed: bool,
    bound: u64,
}

impl Client {
    fn new(transport: Transport, calls: u32, input_bytes: usize) -> Self {
        let limits = limits(calls);
        let mut endpoints = List::with_capacity(1);
        endpoints
            .push(Endpoint {
                address: (Ipv4Addr::LOCALHOST, 80).into(),
                transport: skein_llm_connection::Transport::Plaintext,
                llm: skein_llm::Endpoint::codex(),
                limits: skein_llm_world::limits(),
                credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
            })
            .expect("endpoint bound");
        let bound = skein_llm_connection::worst_case(&limits, &endpoints).expect("component bound");
        Self {
            io: io::Io::new(&limits.io),
            limits,
            component: None,
            address: None,
            transport,
            io_events: Queue::with_capacity(ROOM),
            events: Queue::with_capacity(ROOM),
            requests: Queue::with_capacity(ROOM),
            submissions: Queue::with_capacity(ROOM),
            completions: Queue::with_capacity(ROOM),
            received: Vec::with_capacity(32),
            calls,
            input_bytes,
            completed: 0,
            closed: false,
            bound,
        }
    }

    fn start(&mut self, now: Time, wall: Wall) {
        let call = skein_llm_world::call(7);
        let mut endpoints = List::with_capacity(1);
        endpoints
            .push(Endpoint {
                address: self.address.expect("referee supplied the bound address"),
                transport: match self.transport {
                    Transport::Plaintext => skein_llm_connection::Transport::Plaintext,
                    Transport::Tls => skein_llm_connection::Transport::Tls {
                        server_name: skein_tls_world::pki::name(),
                        trust: skein_tls_world::pki::client(&[]),
                    },
                },
                llm: call.endpoint,
                limits: skein_llm_world::limits(),
                credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
            })
            .expect("one endpoint");
        let mut component = Component::new(endpoints, &self.limits).expect("configured component");
        let env = Env { now, wall, limits: self.limits };
        for index in 0..self.calls {
            let token = Token::new(u64::from(index).checked_add(7).expect("bounded calls"));
            let call = skein_llm_world::call(token.raw());
            let mut prompt = call.prompt;
            prompt.instructions = CUE.into();
            if self.input_bytes > 0 {
                prompt.messages = Box::new([skein_llm::Message {
                    role: skein_llm::Role::User,
                    content: Box::new([skein_llm::Block::Text {
                        text: vec![b'x'; self.input_bytes].into_boxed_slice(),
                        replay: None,
                    }]),
                }]);
            }
            component.down(
                &env,
                Request::Start {
                    call: token,
                    endpoint: 0,
                    prompt,
                    credential: call.credential,
                    deadlines: Deadlines { whole: Some(Duration::from_secs(10)), ..Deadlines::none() },
                },
                &mut self.events,
                &mut self.requests,
            );
            component.down(&env, Request::Next { call: token }, &mut self.events, &mut self.requests);
        }
        self.component = Some(component);
    }
}

impl Host for Client {
    fn iterate(&mut self, now: Time, wall: Wall) {
        if self.component.is_none() && self.address.is_some() {
            self.start(now, wall);
        }
        let io_env = Env { now, wall, limits: self.limits.io };
        let env = Env { now, wall, limits: self.limits };
        for _ in 0..ROOM {
            if self.io_events.room() < 3 || self.submissions.room() < 2 || !self.io.is_ready() {
                break;
            }
            io::resume(&mut self.io, &io_env, &mut self.io_events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.io_events.room() < 3 || self.submissions.room() < 2 {
                break;
            }
            let Some(complete) = self.completions.pop() else { break };
            io::up(&mut self.io, &io_env, complete, &mut self.io_events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.io_events.room() < 3 || self.submissions.room() < 2 || !self.io.is_due(now) {
                break;
            }
            io::fire(&mut self.io, &io_env, &mut self.io_events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.events.room() < skein_llm_connection::MAX_OUT.above
                || self.requests.room() < skein_llm_connection::MAX_OUT.below
            {
                break;
            }
            let Some(event) = self.io_events.pop() else { break };
            self.component.as_mut().expect("io follows Start").up(&env, event, &mut self.events, &mut self.requests);
        }
        if let Some(component) = &mut self.component
            && self.events.room() >= skein_llm_connection::MAX_OUT.above
            && self.requests.room() >= skein_llm_connection::MAX_OUT.below
            && (component.has_work() || component.next_deadline().is_some_and(|due| due <= now))
        {
            component.fire(&env, &mut self.events, &mut self.requests);
        }
        for _ in 0..ROOM {
            if self.requests.room() < skein_llm_connection::MAX_OUT.below {
                break;
            }
            let Some(event) = self.events.pop() else { break };
            match &event {
                Event::Delta { call, .. } | Event::Block { call, .. } => {
                    self.component.as_mut().expect("started").down(
                        &env,
                        Request::Next { call: *call },
                        &mut self.events,
                        &mut self.requests,
                    );
                }
                Event::Completed { .. } => {
                    self.completed += 1;
                    if self.completed == self.calls {
                        self.component.as_mut().expect("started").down(
                            &env,
                            Request::Close,
                            &mut self.events,
                            &mut self.requests,
                        );
                    }
                }
                Event::Closed => {
                    assert!(!self.closed, "one component Closed");
                    assert_eq!(self.completed, self.calls);
                    self.closed = true;
                }
                Event::Refused { .. } | Event::Failed { .. } | Event::Cancelled { .. } => {
                    panic!("positive call failed: {event:?}")
                }
            }
            assert!(self.received.len() < 32, "bounded owner observations");
            self.received.push(event);
        }
        for _ in 0..ROOM {
            if !self.io.takes() || self.submissions.room() < 2 {
                break;
            }
            let Some(request) = self.requests.pop() else { break };
            io::down(&mut self.io, &io_env, request, &mut self.submissions);
        }
        self.io.reclaim();
        if let Some(component) = &mut self.component {
            component.reclaim();
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.io.is_ready()
            || !self.io_events.is_empty()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || !self.completions.is_empty()
            || (self.component.is_none() && self.address.is_some())
            || self.component.as_ref().is_some_and(|component| {
                component.has_work() || component.next_deadline().is_some_and(|due| due <= now)
            })
    }
    fn next_deadline(&self) -> Option<Time> {
        [self.io.next_deadline(), self.component.as_ref().and_then(Component::next_deadline)]
            .into_iter()
            .flatten()
            .min()
    }
    fn is_empty(&self) -> bool {
        self.io.is_empty()
            && self.io_events.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
            && self.submissions.is_empty()
            && self.completions.is_empty()
            && self.closed
            && !self.component.as_ref().is_some_and(Component::has_work)
    }
    fn worst_case(&self) -> u64 {
        self.bound
            .checked_add(io::worst_case(&self.limits.io).expect("io bound"))
            .expect("combined bound")
            .checked_add(u64::from(ROOM) * 256 + 32 * 131_072)
            .expect("queues and retained owner records")
    }
    fn operations(&self) -> u32 {
        io::operations(&self.limits.io).expect("bounded io")
    }
}

/// Processes joined only by the world's simulated network.
pub enum Process {
    /// The connection service sends owner records to the referee and closes its sockets.
    Client(Box<Client>),
    /// The independent fake sends outside observations and settles on shutdown.
    Peer(Box<llm::Peer>),
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        match self {
            Self::Client(client) => client.iterate(now, wall),
            Self::Peer(peer) => peer.iterate(now, wall),
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        match self {
            Self::Client(client) => client.completions(),
            Self::Peer(peer) => peer.completions(),
        }
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        match self {
            Self::Client(client) => client.submissions(),
            Self::Peer(peer) => peer.submissions(),
        }
    }
    fn work_pending(&self, now: Time) -> bool {
        match self {
            Self::Client(client) => client.work_pending(now),
            Self::Peer(peer) => peer.work_pending(now),
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        match self {
            Self::Client(client) => client.next_deadline(),
            Self::Peer(peer) => peer.next_deadline(),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            Self::Client(client) => client.is_empty(),
            Self::Peer(peer) => peer.is_empty(),
        }
    }
    fn worst_case(&self) -> u64 {
        match self {
            Self::Client(client) => client.worst_case(),
            Self::Peer(peer) => peer.worst_case(),
        }
        .checked_add(4096)
        .expect("boxed process wrapper")
    }
    fn operations(&self) -> u32 {
        match self {
            Self::Client(client) => client.operations(),
            Self::Peer(peer) => peer.operations(),
        }
    }
}

struct Judge {
    activated: bool,
    calls: u32,
    answer: Vec<u8>,
    passed: bool,
    teardown_started: bool,
    query_times: std::collections::BTreeMap<Token, Time>,
    input_bytes: usize,
}

impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, processes: &mut [Process]) {
        let address = processes.iter().find_map(|process| match process {
            Process::Peer(peer) => peer.address(),
            Process::Client(_) => None,
        });
        if address.is_some() {
            self.activated = true;
        }
        for process in processes {
            match process {
                Process::Client(client) => client.address = address,
                Process::Peer(peer) => {
                    if self.passed {
                        peer.shutdown();
                        self.teardown_started = true;
                    }
                }
            }
        }
    }
    fn observe(&mut self, now: Time, processes: &[Process]) {
        let mut completed = 0;
        let mut client_closed = false;
        let mut queries = 0;
        let mut answers = 0;
        for process in processes {
            match process {
                Process::Client(client) => {
                    client_closed = client.closed;
                    for event in &client.received {
                        if let Event::Completed { completion, .. } = event {
                            assert_eq!(completion.content.len(), 1, "one literal scripted block");
                            match &completion.content[0] {
                                skein_llm::Block::Text { text, .. } => assert_eq!(
                                    text.as_ref(),
                                    self.answer,
                                    "the client's actual terminal is the literal script"
                                ),
                                block @ (skein_llm::Block::Refusal { .. }
                                | skein_llm::Block::ToolCall { .. }
                                | skein_llm::Block::ToolResult { .. }
                                | skein_llm::Block::Reasoning { .. }
                                | skein_llm::Block::Oversize { .. }
                                | skein_llm::Block::Cut { .. }
                                | skein_llm::Block::Dropped { .. }) => {
                                    panic!("expected scripted text, got {block:?}")
                                }
                            }
                            completed += 1;
                        }
                    }
                }
                Process::Peer(peer) => {
                    for observation in peer.observations() {
                        match observation {
                            llm::Observation::Query { connection, query } => {
                                assert_eq!(query.system.as_ref(), CUE);
                                self.query_times.entry(*connection).or_insert(now);
                                if self.input_bytes > 0 {
                                    assert_eq!(query.messages.len(), 1);
                                    assert_eq!(query.messages[0].parts.len(), 1);
                                    match &query.messages[0].parts[0] {
                                        api::Part::Text { text } => {
                                            assert_eq!(text.len(), self.input_bytes);
                                            assert!(text.iter().all(|byte| *byte == b'x'));
                                        }
                                        part @ (api::Part::Opaque { .. }
                                        | api::Part::ToolCall { .. }
                                        | api::Part::ToolOutput { .. }) => {
                                            panic!("expected literal user text, got {part:?}")
                                        }
                                    }
                                }
                                queries += 1;
                            }
                            llm::Observation::Answered { connection, result } => {
                                assert_eq!(*result, Ok(()));
                                let started = self.query_times[&connection.expect("live call route")];
                                assert!(
                                    now >= started.checked_add(Duration::from_millis(5)).expect("bounded script delay"),
                                    "batching must not shortcut the actual script latency"
                                );
                                answers += 1;
                            }
                            llm::Observation::Accepted { .. } | llm::Observation::Closed { .. } => {}
                        }
                    }
                }
            }
        }
        assert!(
            completed <= self.calls && queries <= self.calls && answers <= self.calls,
            "unique calls and terminals"
        );
        self.passed = client_closed && completed == self.calls && queries == self.calls && answers == self.calls;
    }
    fn next_deadline(&self) -> Option<Time> {
        if !self.activated || (self.passed && !self.teardown_started) {
            Some(Time::ZERO)
        } else if self.passed {
            None
        } else {
            Some(Time::from_nanos(20_000_000_000))
        }
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= Time::from_nanos(20_000_000_000) && !self.passed)
            .then(|| "scripted calls have not completed".to_owned())
    }
    fn passed(&self) -> bool {
        self.passed
    }
}

/// Runs the shared harness with separate process heaps; large fills every delayed-call slot.
#[must_use]
pub fn run(seed: u64, faulted: bool, transport: Transport, large: bool, memory: Memory) -> Outcome<Process> {
    run_with_input(seed, faulted, transport, large, memory, 0)
}

/// Runs an exact caller-owned text upload through the independently hosted peer.
#[must_use]
pub fn run_with_input(
    seed: u64,
    faulted: bool,
    transport: Transport,
    large: bool,
    memory: Memory,
    input_bytes: usize,
) -> Outcome<Process> {
    run_with_shutdown(seed, faulted, transport, large, memory, input_bytes, false)
}

/// A live peer that ignores half-close waits for io to settle the client.
#[must_use]
pub fn run_with_shutdown(
    seed: u64,
    faulted: bool,
    transport: Transport,
    large: bool,
    memory: Memory,
    input_bytes: usize,
    ignore_half_close: bool,
) -> Outcome<Process> {
    let calls = if large { 2 } else { 1 };
    let answer = if large { vec![b'x'; 2048] } else { b"simulated answer".to_vec() };
    let judge = Judge {
        activated: false,
        calls,
        answer: answer.clone(),
        passed: false,
        teardown_started: false,
        query_times: std::collections::BTreeMap::new(),
        input_bytes,
    };
    let mut config = Config::calm();
    config.wall = skein_tls_world::pki::VALID;
    config.buffer = if faulted { 256 } else { 1024 };
    if faulted {
        config.faults.short_recv = 300;
        config.faults.short_send = 300;
    }
    let mut world = World::new(seed, config, judge, memory);
    world.spawn(|| {
        let client_limits = limits(calls);
        let protocol_limits = skein_llm_world::fake::limits(&skein_llm_world::limits());
        let mut domain_limits = skein_llm_world::fake::config();
        domain_limits.calls = calls;
        domain_limits.answer_bytes =
            u32::try_from(answer.len() + std::mem::size_of::<api::Part>()).expect("exact answer cap");
        domain_limits.script_bytes = u32::try_from(
            answer.len()
                + CUE.len()
                + std::mem::size_of::<api::Script>()
                + std::mem::size_of::<api::Turn>()
                + std::mem::size_of::<api::Line>(),
        )
        .expect("exact script cap");
        domain_limits.latency_min = Duration::from_millis(5);
        domain_limits.latency_max = Duration::from_millis(5);
        let scripts = Box::new([api::Script {
            cue: CUE.into(),
            turns: Box::new([api::Turn {
                lines: Box::new([api::Line::Text { text: answer.clone().into_boxed_slice() }]),
                finish: api::Finish::Stop,
                tokens: 2,
            }]),
        }]);
        let domain = fake::Domain::try_scripted(&domain_limits, seed, scripts).expect("script admitted at exact cap");
        let mut io_limits = client_limits.io;
        io_limits.sockets = calls + 1;
        let limits = skein_fake_peers::Limits {
            io: io_limits,
            connections: calls,
            queue: 64,
            plaintext: 32_768,
            ciphertext: 32_768,
            observations: calls * 4,
            observation_bytes: calls * 32_768,
        };
        let mut peer = llm::Peer::new(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            transport,
            limits,
            provider::Config {
                echo: skein_llm::openai::Echo::NONE,
                provider: documents::Provider::OpenAi,
                path: skein_llm::Endpoint::codex().target,
                headers: Box::new([]),
            },
            skein_llm_world::call(7).credential,
            protocol_limits,
            domain,
            domain_limits,
        )
        .expect("bounded independent process");
        if ignore_half_close {
            peer.ignore_half_close();
        }
        Process::Peer(Box::new(peer))
    });
    world.spawn(|| Process::Client(Box::new(Client::new(transport, calls, input_bytes))));
    world.run()
}

/// Runs and replays the actual component against an independent plaintext process.
pub fn scenario(seed: u64, faulted: bool) {
    let first = run(seed, faulted, Transport::Plaintext, false, Memory::Unchecked);
    let second = run(seed, faulted, Transport::Plaintext, false, Memory::Unchecked);
    assert_replay(&first, &second);
    assert_eq!(first.end, second.end, "injected latency and shutdown replay");
}

/// Compares wire order, outside peer observations and every actual owner event.
pub fn assert_replay(first: &Outcome<Process>, second: &Outcome<Process>) {
    assert_eq!(first.trace, second.trace, "the actual two-process io trace replays");
    for (first, second) in first.procs.iter().zip(&second.procs) {
        match (first, second) {
            (Process::Peer(first), Process::Peer(second)) => {
                assert_eq!(first.observations(), second.observations(), "the independent peer's observations replay");
            }
            (Process::Client(first), Process::Client(second)) => assert_eq!(
                format!("{:?}", first.received),
                format!("{:?}", second.received),
                "actual owner payloads and terminals replay"
            ),
            (Process::Peer(_), Process::Client(_)) | (Process::Client(_), Process::Peer(_)) => {
                panic!("process order replays")
            }
        }
    }
}
