//! Timeouts and transport failures around the TLS and LLM boundaries.

use core::net::Ipv4Addr;

use skein_io::kernel::Addr;
use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::{Duration, Env, List, Queue, Time, Token};
use skein_llm::{Failure, client::Evidence};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, MAX_OUT, Request};
use skein_tls_world::{drive::Wire, pki, server::Server};

fn limits() -> Limits {
    Limits {
        endpoints: 1,
        connections: 1,
        per_endpoint: 1,
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
        llm: skein_llm_world::limits(),
    }
}

struct World {
    component: Component,
    up: Queue<Event>,
    io: Queue<IoRequest>,
    wire: Wire,
    owner: Option<Token>,
    now: Time,
    terminals: Vec<Event>,
}

impl World {
    fn new(deadlines: Deadlines) -> World {
        let call = skein_llm_world::call(7);
        let mut endpoints = List::with_capacity(1);
        endpoints
            .push(Endpoint {
                address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
                server_name: skein_tls::Name::new("skein.test").expect("test name"),
                trust: pki::client(&[]),
                llm: call.endpoint,
            })
            .expect("one endpoint");
        let mut world = World {
            component: Component::new(endpoints, &limits()).expect("component"),
            up: Queue::with_capacity(MAX_OUT.above),
            io: Queue::with_capacity(MAX_OUT.below),
            wire: Wire::new(Server::new(pki::Server::plain().config())),
            owner: None,
            now: Time::ZERO,
            terminals: Vec::new(),
        };
        let env = world.env();
        world.component.down(
            &env,
            Request::Start {
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
            if complete_request(&self.wire.server.received) {
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
        Event::Failed { call, failure: got, evidence: seen, .. } => {
            assert_eq!(*call, Token::new(7));
            assert_eq!(*got, failure);
            assert_eq!(*seen, evidence);
        }
        other @ (Event::Refused { .. }
        | Event::Delta { .. }
        | Event::Block { .. }
        | Event::Completed { .. }
        | Event::Cancelled { .. }) => panic!("expected failure, got {other:?}"),
    }
}

#[test]
fn slow_head_expires_after_first_request_byte() {
    let mut world = World::new(Deadlines { head: Some(Duration::from_secs(1)), ..Deadlines::none() });
    world.run_until_request();
    assert_eq!(world.component.next_deadline(), Some(Time::from_nanos(1_000_000_000)));
    world.at(Time::from_nanos(1_000_000_000));
    assert_failure(&world, Failure::TimedOut, Evidence::Unknown);
    let env = world.env();
    world.component.down(&env, Request::Cancel { call: Token::new(7) }, &mut world.up, &mut world.io);
    for _ in 0_u32..1000 {
        world.tick();
    }
    assert_eq!(world.terminals.len(), 1, "a late cancel cannot replace a timeout");
}

#[test]
fn handshake_deadline_expires_before_any_request_bytes() {
    let mut world = World::new(Deadlines { handshake: Some(Duration::from_secs(1)), ..Deadlines::none() });
    world.tick();
    assert_eq!(world.component.next_deadline(), Some(Time::from_nanos(1_000_000_000)));
    world.at(Time::from_nanos(1_000_000_000));
    assert_failure(&world, Failure::TimedOut, Evidence::Unsent);
}

#[test]
fn cancel_before_a_provider_terminal_waits_for_socket_close() {
    let mut world = World::new(Deadlines::none());
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
        other @ (Event::Refused { .. }
        | Event::Delta { .. }
        | Event::Block { .. }
        | Event::Completed { .. }
        | Event::Failed { .. }) => panic!("expected cancellation, got {other:?}"),
    }
}

#[test]
fn idle_stall_expires_after_response_head() {
    let mut world = World::new(Deadlines { idle: Some(Duration::from_secs(1)), ..Deadlines::none() });
    world.run_until_request();
    world
        .wire
        .server
        .write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
    world.wire.pull();
    world.run_until_head();
    world.at(Time::from_nanos(1_000_000_000));
    assert_failure(&world, Failure::TimedOut, Evidence::Response);
}

#[test]
fn a_provider_ping_rearms_idle_without_consuming_next() {
    let mut world = World::new(Deadlines { idle: Some(Duration::from_secs(1)), ..Deadlines::none() });
    world.run_until_request();
    world
        .wire
        .server
        .write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
    world.wire.pull();
    world.run_until_head();
    world.now = Time::from_nanos(500_000_000);
    let ping = b"data: {\"type\":\"ping\"}\n\n";
    let chunk = format!("{:x}\r\n", ping.len());
    world.wire.server.write(chunk.as_bytes());
    world.wire.server.write(ping);
    world.wire.server.write(b"\r\n");
    world.wire.pull();
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
    assert_failure(&world, Failure::TimedOut, Evidence::Response);
}

#[test]
fn truncated_response_has_one_failed_terminal() {
    let mut world = World::new(Deadlines::none());
    world.run_until_request();
    world
        .wire
        .server
        .write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 100\r\n\r\ndata: {}");
    world.wire.server.close_notify();
    world.wire.pull();
    for _ in 0_u32..20_000 {
        world.tick();
        if !world.terminals.is_empty() {
            break;
        }
    }
    assert_eq!(world.terminals.len(), 1);
    match &world.terminals[0] {
        Event::Failed { evidence, .. } => assert_eq!(*evidence, Evidence::Response),
        other @ (Event::Refused { .. }
        | Event::Delta { .. }
        | Event::Block { .. }
        | Event::Completed { .. }
        | Event::Cancelled { .. }) => panic!("expected truncation failure, got {other:?}"),
    }
}
