//! The pooled component, owner script and independent byte peers over seeded
//! plaintext wires (llm-connection.md, section 8). The referee observes owner
//! terminals and physical settlement, and rejects events after Closed.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Intake, List, Queue, Time, Token, Wall};
use skein_llm::Credential;
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, MAX_OUT, Request};

use crate::plaintext::Wire;

/// An independently selected point for an owner close or abort.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    Waiting,
    Connecting,
    Head,
    Streaming,
    Draining,
    Idle,
    Closing,
}

/// Outside observations: each admitted call's terminal and the component's last word.
#[derive(Default, Debug)]
pub struct Judge {
    pub closed: bool,
    pub completed: u32,
    pub cancelled: u32,
    pub deltas: u32,
    pub refused: u32,
    pub failed: u32,
    pending: BTreeSet<Token>,
}

impl Judge {
    /// Register an owner's admitted call before driving it.
    pub fn start(&mut self, call: Token) {
        assert!(self.pending.insert(call), "one outstanding call per token");
    }

    /// Observe exactly one owner event, without reading the component's state.
    pub fn observe(&mut self, event: &Event) {
        assert!(!self.closed, "nothing follows Closed");
        match event {
            Event::Closed => {
                assert!(self.pending.is_empty(), "every call has its terminal before Closed");
                self.closed = true;
            }
            Event::Completed { call, .. } => {
                assert!(self.pending.remove(call), "one terminal per call");
                self.completed += 1;
            }
            Event::Cancelled { call } => {
                assert!(self.pending.remove(call), "one terminal per call");
                self.cancelled += 1;
            }
            Event::Failed { call, .. } => {
                assert!(self.pending.remove(call), "one terminal per call");
                self.failed += 1;
            }
            Event::Delta { call, .. } | Event::Block { call, .. } => {
                assert!(self.pending.contains(call), "output belongs to an active call");
                self.deltas += 1;
            }
            Event::Refused { .. } => self.refused += 1,
        }
    }

    /// Waiting solely for lower settlement must let the owning loop sleep.
    pub fn settlement(&self, work: bool, deadline: Option<Time>, now: Time) {
        assert!(!work, "io settlement alone is not runnable work");
        assert!(deadline.is_none_or(|deadline| deadline > now), "no past-due wake while only io settles");
    }
}

/// Tiny stream cuts are test overrides; the idle keep is beyond the story horizon.
#[must_use]
pub fn limits(calls: u32) -> Limits {
    Limits {
        endpoints: 1,
        connections: calls,
        calls,
        per_endpoint: calls,
        idle_keep: Duration::from_secs(300),
        io: skein_io::Limits {
            sockets: calls,
            refusals: 1,
            intake: 19_000,
            receive: 1024,
            output: 19_000,
            sends: 2,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(10),
        },
        tls: skein_tls::client::Limits { read: 4096, send: 4096, records: skein_tls::client::MAX_RECORD },
        llm: skein_llm_world::limits(),
    }
}

struct Peer {
    wire: Wire,
    owner: Token,
    service: provider::Service,
    server: provider::Server,
    domain: fake::Domain,
    credential: Credential,
    input: Intake,
    above: Queue<provider::Event>,
    below: Queue<Down>,
    replies: Queue<fake::Request>,
    demand: Option<(Read, u32)>,
    granted: u32,
    received: usize,
    tail: Vec<u8>,
    closing: bool,
    settled: bool,
}

impl Peer {
    fn new(owner: Token, seed: u64) -> Self {
        let call = skein_llm_world::call(7);
        let config = skein_llm_world::fake::config();
        let limits = skein_llm_world::fake::limits(&skein_llm_world::limits());
        let mut peer = Self {
            wire: Wire::new(seed),
            owner,
            service: provider::Service::new(
                provider::Config {
                    provider: documents::Provider::OpenAi,
                    path: call.endpoint.target,
                    headers: Box::new([]),
                },
                &limits,
            )
            .expect("peer service"),
            server: provider::Server::new(owner, &limits).expect("peer"),
            domain: fake::Domain::try_scripted(
                &config,
                seed,
                Box::new([api::Script {
                    cue: b"close-world".as_slice().into(),
                    turns: Box::new([api::Turn {
                        lines: Box::new([api::Line::Text { text: b"scripted answer".as_slice().into() }]),
                        finish: api::Finish::Stop,
                        tokens: 2,
                    }]),
                }]),
            )
            .expect("script"),
            credential: call.credential,
            input: Intake::with_capacity(32768),
            above: Queue::with_capacity(provider::MAX_UP),
            below: Queue::with_capacity(provider::MAX_DOWN),
            replies: Queue::with_capacity(fake::MAX_OUT),
            demand: None,
            granted: 0,
            received: 0,
            tail: Vec::new(),
            closing: false,
            settled: false,
        };
        provider::start(
            &mut peer.server,
            &mut peer.service,
            &peer.credential,
            &Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            &mut peer.above,
            &mut peer.below,
        );
        peer
    }

    fn tick(&mut self, hold_tail: bool) {
        if self.settled {
            return;
        }
        let limits = skein_llm_world::fake::limits(&skein_llm_world::limits());
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
        let domain_env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: skein_llm_world::fake::config() };
        if self.wire.received.len() > self.received {
            self.input.append(&self.wire.received[self.received..]).expect("bounded input");
            self.received = self.wire.received.len();
        }
        if let Some(event) = self.above.pop() {
            match event {
                provider::Event::Domain(event) => fake::step(&mut self.domain, &domain_env, event, &mut self.replies),
                provider::Event::Close | provider::Event::Closed => {}
            }
        }
        if let Some(reply) = self.replies.pop() {
            provider::down(
                &mut self.server,
                &mut self.service,
                &self.credential,
                &env,
                reply,
                &mut self.above,
                &mut self.below,
            );
            self.domain.reclaim();
            self.service.reclaim();
        }
        if let Some(down) = self.below.pop() {
            match down {
                Down::Demand { read: Read::Nothing, room: 0 } => self.demand = None,
                Down::Demand { read, room } => {
                    assert!(self.demand.is_none());
                    self.demand = Some((read, room));
                }
                Down::Send(bytes) => {
                    assert!(bytes.len() <= usize::try_from(self.granted).expect("room fits"));
                    self.granted = 0;
                    if hold_tail && bytes.ends_with(b"0\r\n\r\n") {
                        self.wire.write(&bytes[..bytes.len() - 5]);
                        self.tail.extend_from_slice(b"0\r\n\r\n");
                    } else {
                        self.wire.write(&bytes);
                    }
                }
                Down::Finish => {}
            }
        }
        if !hold_tail && !self.tail.is_empty() {
            self.wire.write(&self.tail);
            self.tail.clear();
        }
        if let Some((read, room)) = self.demand {
            let answer = match self.input.meet(read) {
                Some(bytes) => Some(Up::Bytes(bytes)),
                None if room > 0 => {
                    self.granted = room;
                    Some(Up::Room)
                }
                None => None,
            };
            if let Some(answer) = answer {
                self.demand = None;
                provider::up(
                    &mut self.server,
                    &mut self.service,
                    &self.credential,
                    &env,
                    answer,
                    &mut self.above,
                    &mut self.below,
                );
            }
        }
        if self.server.has_work() {
            provider::resume(
                &mut self.server,
                &mut self.service,
                &self.credential,
                &env,
                &mut self.above,
                &mut self.below,
            );
        }
        if self.domain.is_due(Time::ZERO) {
            fake::fire(&mut self.domain, &domain_env, &mut self.replies);
        }
    }
}

/// The actual component and several independent peers, with delayed physical closes.
pub struct World {
    pub component: Component,
    pub judge: Judge,
    pub events: Vec<String>,
    pub trace: Vec<String>,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<IoRequest>,
    peers: BTreeMap<Token, Peer>,
    connecting: Vec<Token>,
    seed: u64,
    calls: u32,
    pub hold_tail: bool,
    pub delay_close: bool,
    pub aborts: u32,
}

impl World {
    /// Admit several calls to the same endpoint; each has an independent byte peer.
    #[must_use]
    pub fn new(seed: u64, calls: u32) -> Self {
        Self::configured(seed, calls, calls, 1)
    }

    /// Configure endpoint concurrency independently of the declared conversations.
    #[must_use]
    pub fn configured(seed: u64, calls: u32, per_endpoint: u32, endpoint_count: u32) -> Self {
        let mut limits = limits(calls);
        limits.per_endpoint = per_endpoint;
        limits.endpoints = endpoint_count;
        let mut endpoints = List::with_capacity(endpoint_count);
        for endpoint in 0..endpoint_count {
            endpoints
                .push(Endpoint {
                    address: (Ipv4Addr::LOCALHOST, 80 + u16::try_from(endpoint).expect("test endpoint")).into(),
                    transport: skein_llm_connection::Transport::Plaintext,
                    llm: skein_llm::Endpoint::codex(),
                })
                .expect("endpoint");
        }
        let mut world = Self {
            component: Component::new(endpoints, &limits).expect("component"),
            judge: Judge::default(),
            events: Vec::new(),
            trace: Vec::new(),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            above: Queue::with_capacity(MAX_OUT.above),
            below: Queue::with_capacity(MAX_OUT.below),
            peers: BTreeMap::new(),
            connecting: Vec::new(),
            seed,
            calls,
            hold_tail: false,
            delay_close: false,
            aborts: 0,
        };
        for call in 7..7 + u64::from(calls) {
            world.start(call);
        }
        world
    }

    /// Submit a start; closing refusals are independent of an existing call token.
    pub fn start(&mut self, token: u64) {
        self.start_at(token, 0, Deadlines::none());
    }

    /// Admit a call to a selected endpoint with its own whole-call bound.
    pub fn start_at(&mut self, token: u64, endpoint: u32, deadlines: Deadlines) {
        let mut call = skein_llm_world::call(token);
        call.prompt.instructions = b"close-world".as_slice().into();
        self.component.down(
            &self.env,
            Request::Start {
                call: Token::new(token),
                endpoint,
                prompt: call.prompt,
                credential: call.credential,
                deadlines,
            },
            &mut self.above,
            &mut self.below,
        );
        if !matches!(self.above.iter().last(), Some(Event::Refused { call, .. }) if *call == Token::new(token)) {
            self.judge.start(Token::new(token));
            self.component.down(&self.env, Request::Next { call: Token::new(token) }, &mut self.above, &mut self.below);
        }
    }

    /// Set virtual time and deliver any due component deadlines.
    pub fn advance(&mut self, now: Time) {
        self.env.now = now;
        self.component.fire(&self.env, &mut self.above, &mut self.below);
    }

    /// Count physical connections, independently of the component's waiting records.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.peers.len()
    }

    /// Apply an owner request with its promised output reservation.
    pub fn request(&mut self, request: Request) {
        self.component.down(&self.env, request, &mut self.above, &mut self.below);
    }

    /// One bounded owner/byte-peer turn; the replay owns every stream cut.
    pub fn tick(&mut self) {
        if let Some(event) = self.above.pop() {
            self.events.push(format!("{event:?}"));
            self.judge.observe(&event);
            if let Event::Delta { call, .. } | Event::Block { call, .. } = event {
                self.request(Request::Next { call });
            }
        }
        if let Some(request) = self.below.pop() {
            match request {
                IoRequest::Connect { owner, .. } => {
                    let socket = owner;
                    self.peers.insert(socket, Peer::new(owner, self.seed + owner.raw()));
                    self.component.up(
                        &self.env,
                        IoEvent::Connecting { owner, socket },
                        &mut self.above,
                        &mut self.below,
                    );
                    self.connecting.push(owner);
                }
                IoRequest::Stream { stream, down } => self.peers.get_mut(&stream).expect("peer").wire.take(down),
                IoRequest::Abort { entity } => {
                    self.aborts += 1;
                    let peer = self.peers.get_mut(&entity).expect("physical peer");
                    peer.closing = true;
                    self.trace.push(format!("abort {entity:?}"));
                }
                IoRequest::Close { entity } => {
                    let peer = self.peers.get_mut(&entity).expect("physical peer");
                    peer.closing = true;
                    self.trace.push(format!("close {entity:?}"));
                }
                other @ (IoRequest::Listen { .. }
                | IoRequest::Bind { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Signal { .. }) => panic!("unexpected io {other:?}"),
            }
        }
        if let Some(owner) = self.connecting.pop() {
            self.component.up(&self.env, IoEvent::Connected { owner }, &mut self.above, &mut self.below);
        }
        for peer in self.peers.values_mut() {
            peer.tick(self.hold_tail);
            if !peer.closing
                && let Some(up) = peer.wire.answer()
            {
                self.component.up(
                    &self.env,
                    IoEvent::Stream { owner: peer.owner, up },
                    &mut self.above,
                    &mut self.below,
                );
            }
        }
        if self.component.has_work() || self.component.next_deadline().is_some_and(|deadline| deadline <= self.env.now)
        {
            self.component.fire(&self.env, &mut self.above, &mut self.below);
        }
        for peer in self.peers.values_mut() {
            if peer.closing && !peer.settled && !self.delay_close {
                peer.settled = true;
                self.component.up(&self.env, IoEvent::Closed { owner: peer.owner }, &mut self.above, &mut self.below);
                self.component.reclaim();
            }
        }
    }

    /// Run to an observable phase; hold the final chunk to make draining distinct.
    pub fn until(&mut self, point: Point) {
        if point == Point::Connecting || point == Point::Waiting {
            return;
        }
        if point == Point::Draining {
            self.hold_tail = true;
        }
        for _ in 0..40_000 {
            self.tick();
            let ready = match point {
                Point::Waiting | Point::Connecting => true,
                Point::Head => self.peers.values().any(|peer| !peer.wire.received.is_empty()),
                Point::Streaming => self.judge.deltas > 0 && self.judge.completed == 0,
                Point::Draining => self.judge.completed == self.calls,
                Point::Idle => {
                    self.judge.completed == self.calls
                        && !self.component.has_work()
                        && self.below.is_empty()
                        && self.above.is_empty()
                }
                Point::Closing => self.peers.values().any(|peer| peer.closing),
            };
            if ready {
                return;
            }
        }
        panic!("did not reach {point:?}");
    }

    /// Drain to the one component Closed and prove all physical bindings settled first.
    pub fn finish(&mut self) {
        self.delay_close = false;
        for _ in 0..40_000 {
            self.tick();
            if self.judge.closed {
                break;
            }
        }
        assert!(self.judge.closed);
        assert!(self.peers.values().all(|peer| peer.settled));
        assert!(self.above.is_empty() && self.below.is_empty());
        self.judge.settlement(self.component.has_work(), self.component.next_deadline(), self.env.now);
        for peer in self.peers.values_mut() {
            self.trace.append(&mut peer.wire.trace);
        }
    }
}

/// Cross an owner close with one call's progress, then replay the exact result.
#[must_use]
pub fn run(seed: u64, point: Point, abort: bool) -> (Vec<String>, Vec<String>) {
    let mut world = if point == Point::Waiting { World::configured(seed, 2, 1, 1) } else { World::new(seed, 1) };
    if point == Point::Closing {
        world.until(Point::Idle);
        world.delay_close = true;
        world.request(Request::Close);
        world.until(Point::Closing);
    } else {
        world.until(point);
    }
    if point != Point::Closing {
        world.request(if abort { Request::Abort } else { Request::Close });
    } else if abort {
        world.request(Request::Abort);
    } else {
        world.request(Request::Close);
    }
    world.finish();
    if abort {
        assert!(world.aborts > 0, "abort reaches every physical closing binding");
    } else {
        assert_eq!(world.judge.completed, if point == Point::Waiting { 2 } else { 1 });
    }
    (world.trace, world.events)
}
