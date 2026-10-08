//! One service loop over io and the simulator, with the scripted fake LLM
//! peer as an accepted plaintext server on the other socket (llm-connection.md, 8).

use std::net::{Ipv4Addr, SocketAddr};

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_io::kernel::{Complete, Submit};
use skein_io::{Event as IoEvent, Io, Request as IoRequest};
use skein_lib::stream::Down;
use skein_lib::{Duration, Env, List, Queue, Token};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, Request};
use skein_sim::{Config, Pid, Sim};
use skein_world::domain::{Trace, assert_replays};

const LISTENER: Token = Token::new(1000);
const SERVER: Token = Token::new(1001);
const CLIENT_IO: Token = Token::new(1002);
const CALL: Token = Token::new(7);
const ROOM: u32 = 256;

fn limits() -> Limits {
    Limits {
        endpoints: 1,
        connections: 1,
        per_endpoint: 1,
        idle_keep: Duration::from_secs(1),
        io: skein_io::Limits {
            sockets: 3,
            refusals: 1,
            intake: 19_000,
            receive: 1024,
            output: 19_000,
            sends: 2,
            accepts: 1,
            backlog: 2,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(10),
        },
        tls: skein_tls::client::Limits { read: 4096, send: 4096, records: skein_tls::client::MAX_RECORD },
        llm: skein_llm_world::limits(),
    }
}

struct World {
    sim: Sim,
    pid: Pid,
    io: Io,
    limits: Limits,
    component: Option<Component>,
    component_owner: Option<Token>,
    io_events: Queue<IoEvent>,
    owner_events: Queue<Event>,
    requests: Queue<IoRequest>,
    submissions: Queue<Submit>,
    completions: Queue<Complete>,
    listener: Option<Token>,
    listener_closed: bool,
    server_socket: Option<Token>,
    server_closed: bool,
    client_closed: bool,
    trace: Trace,
    service: provider::Service,
    peer: provider::Server,
    peer_limits: provider::Limits,
    peer_credential: skein_llm::Credential,
    peer_up: Queue<provider::Event>,
    peer_down: Queue<Down>,
    domain: fake::Domain,
    fake_limits: fake::Config,
    replies: Queue<fake::Request>,
    completed: u32,
    fragments: u32,
}

impl World {
    fn new(seed: u64, faulted: bool) -> World {
        let mut config = Config::calm();

        config.buffer = 1024;
        if faulted {
            config.buffer = 256;
            config.faults.short_recv = 300;
            config.faults.short_send = 300;
        }
        let mut sim = Sim::new(seed, config);
        let pid = sim.spawn_process();
        let limits = limits();
        let peer_limits = skein_llm_world::fake::limits(&limits.llm);
        let service = provider::Service::new(
            provider::Config {
                provider: documents::Provider::OpenAi,
                path: skein_llm::Endpoint::codex().target,
                headers: Box::new([]),
            },
            &peer_limits,
        )
        .expect("scripted provider service");
        let peer = provider::Server::new(Token::new(2), &peer_limits).expect("one fake peer connection");
        let fake_limits = skein_llm_world::fake::config();
        let scripts = Box::new([api::Script {
            cue: b"simulated-world".as_slice().into(),
            turns: Box::new([api::Turn {
                lines: Box::new([api::Line::Text { text: b"simulated answer".as_slice().into() }]),
                finish: api::Finish::Stop,
                tokens: 2,
            }]),
        }]);
        let domain = fake::Domain::try_scripted(&fake_limits, seed, scripts).expect("scripted domain");
        let mut requests = Queue::with_capacity(ROOM);
        requests.push(IoRequest::Listen { owner: LISTENER, addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)) });
        World {
            sim,
            pid,
            io: Io::new(&limits.io),
            limits,
            component: None,
            component_owner: None,
            io_events: Queue::with_capacity(ROOM),
            owner_events: Queue::with_capacity(ROOM),
            requests,
            submissions: Queue::with_capacity(ROOM),
            completions: Queue::with_capacity(ROOM),
            listener: None,
            listener_closed: false,
            server_socket: None,
            server_closed: false,
            client_closed: false,
            trace: Trace::default(),
            service,
            peer,
            peer_limits,
            peer_credential: skein_llm::Credential {
                access_token: b"secret-test-token".as_slice().into(),
                account_id: b"account-test".as_slice().into(),
            },
            peer_up: Queue::with_capacity(ROOM),
            peer_down: Queue::with_capacity(ROOM),
            domain,
            fake_limits,
            replies: Queue::with_capacity(fake::MAX_OUT),
            completed: 0,
            fragments: 0,
        }
    }

    fn io_env(&self) -> Env<skein_io::Limits> {
        Env { now: self.sim.now(), wall: self.sim.wall(), limits: self.limits.io }
    }

    fn component_env(&self) -> Env<Limits> {
        Env { now: self.sim.now(), wall: self.sim.wall(), limits: self.limits }
    }

    fn peer_env(&self) -> Env<provider::Limits> {
        Env { now: self.sim.now(), wall: self.sim.wall(), limits: self.peer_limits }
    }

    fn fake_env(&self) -> Env<fake::Config> {
        Env { now: self.sim.now(), wall: self.sim.wall(), limits: self.fake_limits }
    }

    fn listen(&mut self, addr: SocketAddr) {
        let call = skein_llm_world::call(CALL.raw());
        let mut endpoints = List::with_capacity(1);
        endpoints
            .push(Endpoint { address: addr, transport: skein_llm_connection::Transport::Plaintext, llm: call.endpoint })
            .expect("one endpoint");
        let mut component = Component::new(endpoints, &self.limits).expect("bounded component");
        let env = self.component_env();
        let mut prompt = call.prompt;
        prompt.instructions = b"simulated-world".as_slice().into();
        component.down(
            &env,
            Request::Start {
                call: CALL,
                endpoint: 0,
                prompt,
                credential: call.credential,
                deadlines: Deadlines { whole: Some(Duration::from_secs(10)), ..Deadlines::none() },
            },
            &mut self.owner_events,
            &mut self.requests,
        );
        component.down(&env, Request::Next { call: CALL }, &mut self.owner_events, &mut self.requests);
        self.component = Some(component);
    }

    #[expect(clippy::too_many_lines, reason = "all io variants are routed in one exhaustive owner boundary")]
    fn io_event(&mut self, event: IoEvent) {
        self.trace.log(self.sim.now(), format!("io {event:?}"));
        let env = self.component_env();
        match event {
            IoEvent::Listening { owner, listener, addr } => {
                assert_eq!(owner, LISTENER);
                self.listener = Some(listener);
                self.listen(addr);
            }
            IoEvent::Accepted { owner, socket, .. } => {
                assert_eq!(owner, LISTENER);
                self.server_socket = Some(socket);
                self.requests.push(IoRequest::Bind { socket, owner: SERVER });
                self.requests.push(IoRequest::Close { entity: self.listener.expect("listener") });
                let peer_env = self.peer_env();
                provider::start(
                    &mut self.peer,
                    &mut self.service,
                    &self.peer_credential,
                    &peer_env,
                    &mut self.peer_up,
                    &mut self.peer_down,
                );
            }
            IoEvent::Connecting { owner, socket } => {
                assert_eq!(owner, CLIENT_IO);
                self.component.as_mut().expect("component").up(
                    &env,
                    IoEvent::Connecting { owner: self.component_owner.expect("route"), socket },
                    &mut self.owner_events,
                    &mut self.requests,
                );
            }
            IoEvent::Connected { owner } => {
                assert_eq!(owner, CLIENT_IO);
                self.component.as_mut().expect("component").up(
                    &env,
                    IoEvent::Connected { owner: self.component_owner.expect("route") },
                    &mut self.owner_events,
                    &mut self.requests,
                );
            }
            IoEvent::Stream { owner, up } => {
                if owner == SERVER {
                    let peer_env = self.peer_env();
                    provider::up(
                        &mut self.peer,
                        &mut self.service,
                        &self.peer_credential,
                        &peer_env,
                        up,
                        &mut self.peer_up,
                        &mut self.peer_down,
                    );
                } else {
                    assert_eq!(owner, CLIENT_IO);
                    self.component.as_mut().expect("component").up(
                        &env,
                        IoEvent::Stream { owner: self.component_owner.expect("route"), up },
                        &mut self.owner_events,
                        &mut self.requests,
                    );
                }
            }
            IoEvent::Output { .. } => panic!("this peer uses the classic stream boundary"),
            IoEvent::Failed { owner, error } => {
                if owner == CLIENT_IO {
                    self.component.as_mut().expect("component").up(
                        &env,
                        IoEvent::Failed { owner: self.component_owner.expect("route"), error },
                        &mut self.owner_events,
                        &mut self.requests,
                    );
                } else {
                    panic!("unexpected simulated server failure: {owner:?} {error:?}");
                }
            }
            IoEvent::Closed { owner } => {
                if owner == CLIENT_IO {
                    self.client_closed = true;
                    self.component.as_mut().expect("component").up(
                        &env,
                        IoEvent::Closed { owner: self.component_owner.expect("route") },
                        &mut self.owner_events,
                        &mut self.requests,
                    );
                    if let Some(socket) = self.server_socket {
                        self.requests.push(IoRequest::Abort { entity: socket });
                    }
                } else if owner == SERVER {
                    self.server_closed = true;
                    let peer_env = self.peer_env();
                    provider::closed(
                        &mut self.peer,
                        &mut self.service,
                        &peer_env,
                        &mut self.peer_up,
                        &mut self.peer_down,
                    );
                } else {
                    assert_eq!(owner, LISTENER);
                    self.listener_closed = true;
                }
            }
            IoEvent::Spawned { .. } | IoEvent::Exited { .. } | IoEvent::Shutdown { .. } => {
                panic!("no child process or service signal in this world")
            }
        }
    }

    fn owner_event(&mut self, event: Event) {
        self.trace.log(self.sim.now(), format!("owner {event:?}"));
        match event {
            Event::Delta { call, .. } | Event::Block { call, .. } => {
                self.fragments += 1;
                let env = self.component_env();
                self.component.as_mut().expect("component").down(
                    &env,
                    Request::Next { call },
                    &mut self.owner_events,
                    &mut self.requests,
                );
            }
            Event::Completed { call, completion } => {
                assert_eq!(call, CALL);
                assert!(!completion.content.is_empty());
                self.completed += 1;
            }
            Event::Refused { .. } | Event::Failed { .. } | Event::Cancelled { .. } => {
                panic!("positive simulated call failed: {event:?}");
            }
        }
    }

    fn peer_tick(&mut self) {
        let peer_env = self.peer_env();
        let fake_env = self.fake_env();
        if let Some(event) = self.peer_up.pop() {
            match event {
                provider::Event::Domain(input) => fake::step(&mut self.domain, &fake_env, input, &mut self.replies),
                provider::Event::Close | provider::Event::Closed => {}
            }
        }
        if self.domain.is_due(fake_env.now) {
            fake::fire(&mut self.domain, &fake_env, &mut self.replies);
        }
        if let Some(reply) = self.replies.pop() {
            provider::down(
                &mut self.peer,
                &mut self.service,
                &self.peer_credential,
                &peer_env,
                reply,
                &mut self.peer_up,
                &mut self.peer_down,
            );
        }
        if let Some(down) = self.peer_down.pop() {
            self.requests.push(IoRequest::Stream { stream: self.server_socket.expect("bound peer"), down });
        }
        if self.peer.has_work() {
            provider::resume(
                &mut self.peer,
                &mut self.service,
                &self.peer_credential,
                &peer_env,
                &mut self.peer_up,
                &mut self.peer_down,
            );
        }
    }

    fn iterate(&mut self) {
        self.sim.reap(self.pid, &mut self.completions);
        let io_env = self.io_env();
        for _ in 0_u32..ROOM {
            if !self.io.is_ready() {
                break;
            }
            skein_io::resume(&mut self.io, &io_env, &mut self.io_events, &mut self.submissions);
        }
        for _ in 0_u32..ROOM {
            let Some(completion) = self.completions.pop() else { break };
            skein_io::up(&mut self.io, &io_env, completion, &mut self.io_events, &mut self.submissions);
        }
        for _ in 0_u32..ROOM {
            if !self.io.is_due(io_env.now) {
                break;
            }
            skein_io::fire(&mut self.io, &io_env, &mut self.io_events, &mut self.submissions);
        }
        for _ in 0_u32..ROOM {
            let Some(event) = self.io_events.pop() else { break };
            self.io_event(event);
        }
        self.peer_tick();
        for _ in 0_u32..ROOM {
            let Some(event) = self.owner_events.pop() else { break };
            self.owner_event(event);
        }
        if let Some(component) = &self.component {
            let due = component.next_deadline().is_some_and(|deadline| deadline <= self.sim.now());
            if component.has_work() || due {
                let env = self.component_env();
                self.component.as_mut().expect("component").fire(&env, &mut self.owner_events, &mut self.requests);
            }
        }
        for _ in 0_u32..ROOM {
            if !self.io.takes() || self.requests.is_empty() {
                break;
            }
            let mut request = self.requests.pop().expect("nonempty queue");
            if let IoRequest::Connect { owner, .. } = &mut request {
                assert!(self.component_owner.replace(*owner).is_none(), "one component route");
                *owner = CLIENT_IO;
            }
            skein_io::down(&mut self.io, &io_env, request, &mut self.submissions);
        }
        self.io.reclaim();
        if let Some(component) = &mut self.component {
            component.reclaim();
        }
        self.service.reclaim();
        self.domain.reclaim();
        self.sim.submit(self.pid, &mut self.submissions);
    }

    fn run(&mut self) {
        for _ in 0_u32..100_000 {
            self.iterate();
            if self.completed == 1
                && self.client_closed
                && self.server_closed
                && self.listener_closed
                && self.io.is_empty()
            {
                assert_eq!(self.service.count(), 1);
                assert!(self.fragments > 0);
                self.sim.assert_quiescent(self.pid);
                self.sim.assert_no_open_fds(self.pid);
                return;
            }
            let busy = self.io.is_ready()
                || !self.io_events.is_empty()
                || !self.owner_events.is_empty()
                || !self.peer_up.is_empty()
                || !self.peer_down.is_empty()
                || !self.replies.is_empty()
                || !self.requests.is_empty()
                || !self.completions.is_empty()
                || self.sim.ready(self.pid) > 0
                || self.sim.deferred(self.pid)
                || self.component.as_ref().is_some_and(Component::has_work)
                || self.peer.has_work();
            if !busy {
                let mut next = self.sim.next_due();
                for timer in [self.io.next_deadline(), self.component.as_ref().and_then(Component::next_deadline)]
                    .into_iter()
                    .flatten()
                {
                    next = Some(next.map_or(timer, |prior| prior.min(timer)));
                }
                self.sim.advance_to(next.expect("an unfinished simulated connection has work or a deadline"));
            }
        }
        panic!("simulated LLM connection did not settle: {}", self.sim.render_trace());
    }
}

/// Run and replay one complete owner/io/fake-peer loop on a seeded simulator.
pub fn scenario(seed: u64, faulted: bool) {
    assert_replays(seed, seed.wrapping_add(1), |seed| {
        let mut world = World::new(seed, faulted);
        world.run();
        let mut trace = world.trace.lines().to_vec();
        trace.extend(world.sim.render_trace().lines().map(str::to_owned));
        (trace, (world.completed, world.fragments, world.service.count(), world.sim.now()))
    });
}
