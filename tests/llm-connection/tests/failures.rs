//! Timeouts and transport failures around the TLS and LLM boundaries.

use core::net::Ipv4Addr;

use skein_io::kernel::Addr;
use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::stream::{Down, Up};
use skein_lib::{Duration, Env, List, Queue, Time, Token};
use skein_llm::{Failure, Phase, client::Evidence};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, MAX_OUT, Request};
use skein_llm_connection_world::plaintext::Wire;
use skein_tls_world::{drive::Wire as TlsWire, pki, server::Server};
use skein_world::domain::assert_replays;

fn limits() -> Limits {
    Limits {
        endpoints: 1,
        connections: 1,
        calls: 1,
        per_endpoint: 1,
        memory: 16 * 1024 * 1024,
        idle_keep: Duration::from_secs(10),
        io: skein_io::Limits {
            sockets: 1,
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
    }
}

enum TransportWire {
    Plaintext(Wire),
    Handshake(Box<TlsWire>),
}

impl TransportWire {
    fn take(&mut self, down: Down) {
        match self {
            Self::Plaintext(wire) => wire.take(down),
            Self::Handshake(wire) => wire.take(down),
        }
    }

    fn answer(&mut self) -> Option<Up> {
        match self {
            Self::Plaintext(wire) => wire.answer(),
            Self::Handshake(wire) => wire.answer(),
        }
    }

    fn received(&self) -> &[u8] {
        match self {
            Self::Plaintext(wire) => &wire.received,
            Self::Handshake(wire) => &wire.server.received,
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        match self {
            Self::Plaintext(wire) => wire.write(bytes),
            Self::Handshake(wire) => {
                wire.server.write(bytes);
                wire.pull();
            }
        }
    }

    fn end(&mut self) {
        match self {
            Self::Plaintext(wire) => wire.eof = true,
            Self::Handshake(wire) => {
                wire.server.close_notify();
                wire.pull();
            }
        }
    }
}

struct World {
    component: Component,
    up: Queue<Event>,
    io: Queue<IoRequest>,
    wire: TransportWire,
    owner: Option<Token>,
    now: Time,
    terminals: Vec<Event>,
    closed: bool,
    owner_closed: bool,
}

impl World {
    fn new(deadlines: Deadlines, seed: u64, handshake: bool) -> World {
        let call = skein_llm_world::call(7);
        let mut endpoints = List::with_capacity(1);
        endpoints
            .push(Endpoint {
                address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
                transport: if handshake {
                    skein_llm_connection::Transport::Tls {
                        server_name: skein_tls::Name::new("skein.test").expect("test name"),
                        trust: pki::client(&[]),
                    }
                } else {
                    skein_llm_connection::Transport::Plaintext
                },
                llm: call.endpoint,
                limits: skein_llm_world::limits(),
                credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
            })
            .expect("one endpoint");
        let mut world = World {
            component: Component::new(endpoints, &limits()).expect("component"),
            up: Queue::with_capacity(MAX_OUT.above),
            io: Queue::with_capacity(MAX_OUT.below),
            wire: if handshake {
                TransportWire::Handshake(Box::new(TlsWire::new(Server::new(pki::Server::plain().config()))))
            } else {
                TransportWire::Plaintext(Wire::new(seed))
            },
            owner: None,
            now: Time::ZERO,
            terminals: Vec::new(),
            closed: false,
            owner_closed: false,
        };
        let env = world.env();
        world.component.down(
            &env,
            Request::Start {
                drop_reasoning: false,
                call: Token::new(7),
                endpoint: 0,
                prompt: call.prompt,
                credential: call.credential,
                deadlines,
            },
            &mut world.up,
            &mut world.io,
        );
        world.component.down(&env, Request::Next { call: Token::new(7) }, &mut world.up, &mut world.io);
        world
    }

    fn report(mut self) -> (Vec<String>, Vec<String>) {
        for _ in 0_u32..1000 {
            self.tick();
            if self.closed && self.io.is_empty() && self.up.is_empty() && !self.component.has_work() {
                break;
            }
        }
        assert!(self.closed, "the failed call settles its socket");
        assert!(self.io.is_empty() && self.up.is_empty() && !self.component.has_work());
        assert_eq!(self.terminals.len(), 1, "settlement cannot repeat the terminal");
        let trace = match self.wire {
            TransportWire::Plaintext(wire) => wire.trace,
            TransportWire::Handshake(_) => panic!("TLS ciphertext never replays"),
        };
        (trace, self.terminals.iter().map(|event| format!("{event:?}")).collect())
    }

    fn env(&self) -> Env<Limits> {
        Env { now: self.now, wall: pki::VALID, limits: limits() }
    }

    fn tick(&mut self) {
        let env = self.env();
        if let Some(event) = self.up.pop() {
            match event {
                Event::Delta { call, .. } | Event::Block { call, .. } => {
                    self.component.down(&env, Request::Next { call }, &mut self.up, &mut self.io);
                }
                Event::Closed => self.owner_closed = true,
                Event::Refused { .. } | Event::Completed { .. } | Event::Failed { .. } | Event::Cancelled { .. } => {
                    self.terminals.push(event);
                }
            }
        }
        if let Some(request) = self.io.pop() {
            match request {
                IoRequest::Connect { owner, .. } => {
                    self.owner = Some(owner);
                    self.component.up(
                        &env,
                        IoEvent::Connecting { owner, socket: Token::new(100) },
                        &mut self.up,
                        &mut self.io,
                    );
                    self.component.up(&env, IoEvent::Connected { owner }, &mut self.up, &mut self.io);
                }
                IoRequest::Stream { down, .. } => self.wire.take(down),
                IoRequest::Close { .. } | IoRequest::Abort { .. } => {
                    assert!(!self.closed, "one socket close");
                    self.closed = true;
                    self.component.up(
                        &env,
                        IoEvent::Closed { owner: self.owner.expect("owner") },
                        &mut self.up,
                        &mut self.io,
                    );
                    self.component.reclaim();
                }
                other @ (IoRequest::Listen { .. }
                | IoRequest::Bind { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Usage { .. }
                | IoRequest::Signal { .. }) => panic!("unexpected io: {other:?}"),
            }
        }
        if let Some(answer) = self.wire.answer() {
            self.component.up(
                &env,
                IoEvent::Stream { owner: self.owner.expect("owner"), up: answer },
                &mut self.up,
                &mut self.io,
            );
        }
        if self.component.has_work() {
            self.component.fire(&env, &mut self.up, &mut self.io);
        }
    }

    fn run_until_request(&mut self) {
        for _ in 0_u32..20_000 {
            self.tick();
            if complete_request(self.wire.received()) {
                return;
            }
        }
        panic!("the request was never sent");
    }

    fn run_until_head(&mut self) {
        for _ in 0_u32..20_000 {
            self.tick();
            if self.component.next_deadline() == Some(self.now.saturating_add(Duration::from_secs(1))) {
                return;
            }
        }
        panic!("the response head was never seen");
    }

    fn at(&mut self, now: Time) {
        self.now = now;
        let env = self.env();
        self.component.fire(&env, &mut self.up, &mut self.io);
        for _ in 0_u32..1000 {
            self.tick();
            if !self.terminals.is_empty() {
                return;
            }
        }
        panic!("no terminal after timer fired");
    }
}

fn complete_request(bytes: &[u8]) -> bool {
    let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else { return false };
    let head = String::from_utf8_lossy(&bytes[..end + 4]);
    let Some(length) = head
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .and_then(|text| text.trim().parse::<usize>().ok())
    else {
        return false;
    };
    bytes.len() >= end + 4 + length
}

fn assert_failure(world: &World, failure: Failure, evidence: Evidence) {
    assert_eq!(world.terminals.len(), 1);
    match &world.terminals[0] {
        Event::Failed { call, failure: got, evidence: seen, detail } => {
            assert_eq!(*call, Token::new(7));
            assert_eq!(*got, failure);
            assert_eq!(*seen, evidence);
            if let Failure::TimedOut { phase } = failure {
                let expected: &[u8] = match phase {
                    Phase::Connect => b"timed out connecting the socket",
                    Phase::Handshake => b"timed out completing the TLS handshake",
                    Phase::Head => b"timed out waiting for the response head",
                    Phase::Idle => b"timed out waiting for a response event",
                    Phase::Whole => b"timed out waiting for the whole call",
                };
                assert_eq!(detail.as_ref(), expected);
            }
        }
        other @ (Event::Closed
        | Event::Refused { .. }
        | Event::Delta { .. }
        | Event::Block { .. }
        | Event::Completed { .. }
        | Event::Cancelled { .. }) => panic!("expected failure, got {other:?}"),
    }
}

fn run_slow_head_expires_after_first_request_byte(seed: u64) -> World {
    let mut world = World::new(Deadlines { head: Some(Duration::from_secs(1)), ..Deadlines::none() }, seed, false);
    world.run_until_request();
    assert_eq!(world.component.next_deadline(), Some(Time::from_nanos(1_000_000_000)));
    world.at(Time::from_nanos(1_000_000_000));
    assert_failure(&world, Failure::TimedOut { phase: Phase::Head }, Evidence::Unknown);
    let env = world.env();
    world.component.down(&env, Request::Cancel { call: Token::new(7) }, &mut world.up, &mut world.io);
    for _ in 0_u32..1000 {
        world.tick();
    }
    assert_eq!(world.terminals.len(), 1, "a late cancel cannot replace a timeout");
    world
}

#[test]
fn handshake_deadline_expires_before_any_request_bytes() {
    let mut world = World::new(Deadlines { handshake: Some(Duration::from_secs(1)), ..Deadlines::none() }, 7, true);
    world.tick();
    assert_eq!(world.component.next_deadline(), Some(Time::from_nanos(1_000_000_000)));
    world.at(Time::from_nanos(1_000_000_000));
    assert_failure(&world, Failure::TimedOut { phase: Phase::Handshake }, Evidence::Unsent);
}

fn run_cancel_before_a_provider_terminal_waits_for_socket_close(seed: u64) -> World {
    let mut world = World::new(Deadlines::none(), seed, false);
    world.run_until_request();
    let env = world.env();
    world.component.down(&env, Request::Cancel { call: Token::new(7) }, &mut world.up, &mut world.io);
    assert!(world.terminals.is_empty());
    for _ in 0_u32..20_000 {
        world.tick();
        if !world.terminals.is_empty() {
            break;
        }
    }
    assert_eq!(world.terminals.len(), 1);
    match &world.terminals[0] {
        Event::Cancelled { call } => assert_eq!(*call, Token::new(7)),
        other @ (Event::Closed
        | Event::Refused { .. }
        | Event::Delta { .. }
        | Event::Block { .. }
        | Event::Completed { .. }
        | Event::Failed { .. }) => panic!("expected cancellation, got {other:?}"),
    }
    world
}

fn run_idle_stall_expires_after_response_head(seed: u64) -> World {
    let mut world = World::new(Deadlines { idle: Some(Duration::from_secs(1)), ..Deadlines::none() }, seed, false);
    world.run_until_request();
    world.wire.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
    world.run_until_head();
    world.at(Time::from_nanos(1_000_000_000));
    assert_failure(&world, Failure::TimedOut { phase: Phase::Idle }, Evidence::Response { status: 200 });
    world
}

fn run_a_provider_ping_rearms_idle_without_consuming_next(seed: u64) -> World {
    let mut world = World::new(Deadlines { idle: Some(Duration::from_secs(1)), ..Deadlines::none() }, seed, false);
    world.run_until_request();
    world.wire.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
    world.run_until_head();
    world.now = Time::from_nanos(500_000_000);
    let ping = b"data: {\"type\":\"ping\"}\n\n";
    let chunk = format!("{:x}\r\n", ping.len());
    world.wire.write(chunk.as_bytes());
    world.wire.write(ping);
    world.wire.write(b"\r\n");
    let expected = Some(Time::from_nanos(1_500_000_000));
    for _ in 0_u32..20_000 {
        world.tick();
        if world.component.next_deadline() == expected {
            break;
        }
    }
    assert_eq!(world.component.next_deadline(), expected);
    world.now = Time::from_nanos(1_000_000_000);
    let at_one = world.env();
    world.component.fire(&at_one, &mut world.up, &mut world.io);
    assert!(world.up.is_empty(), "a ping extends the idle interval");
    world.at(Time::from_nanos(1_500_000_000));
    assert_failure(&world, Failure::TimedOut { phase: Phase::Idle }, Evidence::Response { status: 200 });
    world
}

fn run_truncated_response_has_one_failed_terminal(seed: u64) -> World {
    let mut world = World::new(Deadlines::none(), seed, false);
    world.run_until_request();
    world.wire.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 100\r\n\r\ndata: {}");
    world.wire.end();
    for _ in 0_u32..20_000 {
        world.tick();
        if !world.terminals.is_empty() {
            break;
        }
    }
    assert_eq!(world.terminals.len(), 1);
    match &world.terminals[0] {
        Event::Failed { evidence, .. } => assert_eq!(*evidence, Evidence::Response { status: 200 }),
        other @ (Event::Closed
        | Event::Refused { .. }
        | Event::Delta { .. }
        | Event::Block { .. }
        | Event::Completed { .. }
        | Event::Cancelled { .. }) => panic!("expected truncation failure, got {other:?}"),
    }
    world
}

#[test]
fn slow_head_expires_after_first_request_byte() {
    assert_replays(7, 8, |seed| run_slow_head_expires_after_first_request_byte(seed).report());
}

#[test]
fn cancel_before_a_provider_terminal_waits_for_socket_close() {
    assert_replays(7, 8, |seed| run_cancel_before_a_provider_terminal_waits_for_socket_close(seed).report());
}

#[test]
fn idle_stall_expires_after_response_head() {
    assert_replays(7, 8, |seed| run_idle_stall_expires_after_response_head(seed).report());
}

#[test]
fn a_provider_ping_rearms_idle_without_consuming_next() {
    assert_replays(7, 8, |seed| run_a_provider_ping_rearms_idle_without_consuming_next(seed).report());
}

#[test]
fn truncated_response_has_one_failed_terminal() {
    assert_replays(7, 8, |seed| run_truncated_response_has_one_failed_terminal(seed).report());
}

fn run_a_slow_upload_keeps_its_head_deadline_alive_by_the_room_it_grants(seed: u64) -> World {
    let mut world = World::new(Deadlines { head: Some(Duration::from_secs(1)), ..Deadlines::none() }, seed, false);
    for tick in 0..20_000 {
        world.now = Time::from_nanos(tick * 100_000_000);
        world.tick();
        let env = world.env();
        world.component.fire(&env, &mut world.up, &mut world.io);
        assert!(world.terminals.is_empty(), "upload grants keep head alive");
        if complete_request(world.wire.received()) {
            break;
        }
    }
    assert!(world.now > Time::from_nanos(1_000_000_000), "the upload outlasts its progress interval");
    assert!(complete_request(world.wire.received()));
    let deadline = world.component.next_deadline().expect("head still runs after upload");
    world.at(deadline);
    assert_failure(&world, Failure::TimedOut { phase: Phase::Head }, Evidence::Unknown);
    world
}

fn run_a_steady_stream_outlasts_any_fixed_bound_and_completes(seed: u64) -> World {
    let mut world = World::new(Deadlines { idle: Some(Duration::from_secs(1)), ..Deadlines::none() }, seed, false);
    world.run_until_request();
    world.wire.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
    world.run_until_head();
    for tick in 1..=20 {
        world.now = Time::from_nanos(tick * 500_000_000);
        let ping = b"data: {\"type\":\"ping\"}\n\n";
        world.wire.write(format!("{:x}\r\n", ping.len()).as_bytes());
        world.wire.write(ping);
        world.wire.write(b"\r\n");
        let expected = Some(world.now.saturating_add(Duration::from_secs(1)));
        for _ in 0..20_000 {
            world.tick();
            if world.component.next_deadline() == expected {
                break;
            }
        }
        assert_eq!(world.component.next_deadline(), expected);
        let env = world.env();
        world.component.fire(&env, &mut world.up, &mut world.io);
        assert!(world.terminals.is_empty());
    }
    let response = skein_llm_world::text_response(true);
    let body = response.windows(4).position(|part| part == b"\r\n\r\n").expect("response head") + 4;
    world.wire.write(&response[body..]);
    for _ in 0..20_000 {
        world.tick();
        if !world.terminals.is_empty() {
            break;
        }
    }
    assert!(matches!(world.terminals.as_slice(), [Event::Completed { .. }]));
    // Let the ordinary idle keep settle the reusable socket in this progress story.
    world.now = Time::from_nanos(30_000_000_000);
    for _ in 0..1000 {
        world.tick();
        let env = world.env();
        world.component.fire(&env, &mut world.up, &mut world.io);
        if world.closed {
            break;
        }
    }
    world
}

fn run_a_stalled_drain_is_closed_by_idleness_without_a_second_terminal(seed: u64) -> World {
    let mut world = World::new(
        Deadlines { idle: Some(Duration::from_secs(1)), whole: Some(Duration::from_secs(5)), ..Deadlines::none() },
        seed,
        false,
    );
    world.run_until_request();
    let response = skein_llm_world::text_response(true);
    assert!(response.ends_with(b"0\r\n\r\n"));
    world.wire.write(&response[..response.len() - 5]);
    for _ in 0..20_000 {
        world.tick();
        if !world.terminals.is_empty() {
            break;
        }
    }
    assert!(matches!(world.terminals.as_slice(), [Event::Completed { .. }]));
    assert_eq!(world.component.next_deadline(), Some(Time::from_nanos(1_000_000_000)));
    world.now = Time::from_nanos(1_000_000_000);
    let env = world.env();
    world.component.fire(&env, &mut world.up, &mut world.io);
    for _ in 0..1000 {
        world.tick();
        if world.closed {
            break;
        }
    }
    assert!(world.closed, "idleness closes a stalled drain");
    assert_eq!(world.terminals.len(), 1);
    assert_eq!(world.component.next_deadline(), None);
    assert!(!world.component.has_work());
    world
}

#[test]
fn a_slow_upload_keeps_its_head_deadline_alive_by_the_room_it_grants() {
    assert_replays(7, 8, |seed| run_a_slow_upload_keeps_its_head_deadline_alive_by_the_room_it_grants(seed).report());
}

#[test]
fn a_steady_stream_outlasts_any_fixed_bound_and_completes() {
    assert_replays(7, 8, |seed| run_a_steady_stream_outlasts_any_fixed_bound_and_completes(seed).report());
}

#[test]
fn a_stalled_drain_is_closed_by_idleness_without_a_second_terminal() {
    assert_replays(7, 8, |seed| run_a_stalled_drain_is_closed_by_idleness_without_a_second_terminal(seed).report());
}

#[test]
fn a_close_while_handshaking_lets_the_tls_call_complete_and_says_closed_last() {
    let mut world = World::new(Deadlines::none(), 47, true);
    world.tick();
    let env = world.env();
    world.component.down(&env, Request::Close, &mut world.up, &mut world.io);
    world.run_until_request();
    world.wire.write(&skein_llm_world::text_response(true));
    for _ in 0..20_000 {
        world.tick();
        if world.owner_closed {
            break;
        }
    }
    assert!(world.owner_closed && world.closed);
    assert!(matches!(world.terminals.as_slice(), [Event::Completed { .. }]));
    assert!(!world.component.has_work());
    assert_eq!(world.component.next_deadline(), None);
}

#[test]
fn provider_pings_keep_idle_alive_but_do_not_extend_the_whole_call() {
    assert_replays(7, 8, |seed| {
        let mut world = World::new(
            Deadlines { idle: Some(Duration::from_secs(1)), whole: Some(Duration::from_secs(1)), ..Deadlines::none() },
            seed,
            false,
        );
        world.run_until_request();
        world.wire.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
        world.run_until_head();
        world.now = Time::from_nanos(500_000_000);
        let ping = b"data: {\"type\":\"ping\"}\n\n";
        world.wire.write(format!("{:x}\r\n", ping.len()).as_bytes());
        world.wire.write(ping);
        world.wire.write(b"\r\n");
        for _ in 0..1000 {
            world.tick();
        }
        assert!(world.terminals.is_empty());
        world.at(Time::from_nanos(1_000_000_000));
        assert_failure(&world, Failure::TimedOut { phase: Phase::Whole }, Evidence::Response { status: 200 });
        world.report()
    });
}

#[test]
fn a_connect_deadline_keeps_unsent_evidence_through_late_socket_settlement() {
    for seed in [7, 8] {
        let mut world =
            World::new(Deadlines { connect: Some(Duration::from_secs(1)), ..Deadlines::none() }, seed, false);
        let Some(IoRequest::Connect { owner, .. }) = world.io.pop() else { panic!("initial connect") };
        world.owner = Some(owner);
        world.at(Time::from_nanos(1_000_000_000));
        assert_failure(&world, Failure::TimedOut { phase: Phase::Connect }, Evidence::Unsent);
        let env = world.env();
        world.component.up(&env, IoEvent::Connecting { owner, socket: Token::new(100) }, &mut world.up, &mut world.io);
        let _settled = world.report();
    }
}
