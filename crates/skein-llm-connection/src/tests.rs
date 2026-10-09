use super::*;
use core::net::Ipv4Addr;
use skein_io::kernel::Addr;
use skein_lib::bytes;
use skein_lib::{Duration, Env, List, Queue, Time, Token, Wall};
use skein_llm::{Block, Credential, Message, Prompt, Role};

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
        llm: skein_llm::client::Limits {
            http: skein_http::client::Limits { request: 4096, head: 4096, headers: 32, read: 256, send: 31 },
            sse: skein_http::sse::Limits { line: 4096, event: 8192, field: 128, chunk: 128 },
            dialect: skein_llm::DocumentLimits {
                request_bytes: 8192,
                document_bytes: 8192,
                string_bytes: 4096,
                depth: 32,
                tokens: 1024,
                parts: 16,
                input_bytes: 2048,
                opaque_bytes: 2048,
                answer_bytes: 8192,
                detail_bytes: 256,
            },
            error_bytes: 4096,
        },
    }
}

fn endpoint() -> Endpoint {
    let mut roots = skein_tls::RootCertStore::empty();
    roots
        .add(skein_tls::CertificateDer::from(
            std::fs::read("../../tests/tls/fixtures/root.der").expect("fixture bytes"),
        ))
        .expect("fixture root");
    Endpoint {
        address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
        transport: Transport::Tls {
            server_name: skein_tls::Name::new("example.test").expect("valid test name"),
            trust: skein_tls::Config::new(roots, &[]).expect("valid test trust"),
        },
        llm: skein_llm::Endpoint::codex(),
    }
}

fn prompt() -> Prompt {
    Prompt {
        model: bytes::copy_of(b"test-model"),
        instructions: Box::new([]),
        tools: Box::new([]),
        messages: Box::new([Message {
            role: Role::User,
            content: Box::new([Block::Text { text: bytes::copy_of(b"hi"), replay: None }]),
        }]),
        reasoning_effort: None,
        cache_key: None,
        max_output_tokens: None,
    }
}

fn credential() -> Credential {
    Credential { access_token: bytes::copy_of(b"secret"), account_id: bytes::copy_of(b"account") }
}

#[test]
fn unknown_endpoint_is_refused() {
    let limits = limits();
    let component = Component::new(List::with_capacity(1), &limits).expect("empty configuration fits");
    assert_eq!(component.admit(0, Token::new(7), 0, prompt(), credential()).err(), Some(Refusal::Endpoint));
}

#[test]
fn full_pool_is_refused() {
    let limits = limits();
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    let component = Component::new(endpoints, &limits).expect("valid endpoints");
    drop(component.admit(0, Token::new(7), 0, prompt(), credential()).expect("valid call admitted"));
    assert_eq!(component.admit(1, Token::new(8), 0, prompt(), credential()).err(), Some(Refusal::Pool));
}

#[test]
fn invalid_and_oversized_requests_are_refused() {
    let limits = limits();
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    let component = Component::new(endpoints, &limits).expect("valid endpoints");
    let mut invalid = prompt();
    invalid.model = Box::new([]);
    assert_eq!(
        component.admit(0, Token::new(7), 0, invalid, credential()).err(),
        Some(Refusal::Client(skein_llm::Error::Invalid))
    );
    let mut oversized = prompt();
    oversized.instructions = Box::new([b'x'; 9000]);
    assert_eq!(
        component.admit(0, Token::new(7), 0, oversized, credential()).err(),
        Some(Refusal::Client(skein_llm::Error::Limit))
    );
}

#[test]
fn startup_rejects_impossible_limits() {
    let mut config = limits();
    config.io.intake = 1;
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    assert_eq!(Component::new(endpoints, &config).err(), Some(EndpointError::Stream));
    let mut config = limits();
    config.connections = 0;
    assert_eq!(Component::new(List::with_capacity(0), &config).err(), Some(EndpointError::Limits));
    let mut config = limits();
    config.endpoints = 0;
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    assert_eq!(Component::new(endpoints, &config).err(), Some(EndpointError::TooMany));
}

fn component() -> Component {
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    Component::new(endpoints, &limits()).expect("valid component")
}

fn start(component: &mut Component, up: &mut Queue<Event>, io: &mut Queue<Lower>) -> Token {
    let env = Env { now: Time::ZERO, wall: Wall::from_nanos(1_893_456_000_000_000_000), limits: limits() };
    component.down(
        &env,
        Request::Start {
            call: Token::new(7),
            endpoint: 0,
            prompt: prompt(),
            credential: credential(),
            deadlines: Deadlines::none(),
        },
        up,
        io,
    );
    match io.pop().expect("one connect") {
        Lower::Connect { owner, .. } => owner,
        other @ (Lower::Listen { .. }
        | Lower::Bind { .. }
        | Lower::Reject { .. }
        | Lower::Stream { .. }
        | Lower::Output { .. }
        | Lower::Spawn { .. }
        | Lower::Signal { .. }
        | Lower::Close { .. }
        | Lower::Abort { .. }) => panic!("expected connect, got {other:?}"),
    }
}

#[test]
fn failed_connect_has_one_unsent_terminal_and_settles() {
    let mut component = component();
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let owner = start(&mut component, &mut up, &mut io);
    let env = Env { now: Time::ZERO, wall: Wall::from_nanos(1_893_456_000_000_000_000), limits: limits() };
    component.up(&env, LowerEvent::Failed { owner, error: skein_io::Error::Refused }, &mut up, &mut io);
    match up.pop().expect("one failure") {
        Event::Failed { call, failure, evidence, .. } => {
            assert_eq!(call, Token::new(7));
            assert_eq!(failure, skein_llm::Failure::Unavailable);
            assert_eq!(evidence, skein_llm::client::Evidence::Unsent);
        }
        other @ (Event::Refused { .. }
        | Event::Delta { .. }
        | Event::Block { .. }
        | Event::Completed { .. }
        | Event::Cancelled { .. }) => panic!("expected failure, got {other:?}"),
    }
    component.up(&env, LowerEvent::Closed { owner }, &mut up, &mut io);
    assert!(up.is_empty(), "settlement does not repeat the terminal");
    component.reclaim();
}

#[test]
fn cancel_while_connecting_waits_for_socket_settlement() {
    let mut component = component();
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let owner = start(&mut component, &mut up, &mut io);
    let env = Env { now: Time::ZERO, wall: Wall::from_nanos(1_893_456_000_000_000_000), limits: limits() };
    component.down(&env, Request::Cancel { call: Token::new(7) }, &mut up, &mut io);
    assert!(up.is_empty());
    component.up(&env, LowerEvent::Connecting { owner, socket: Token::new(19) }, &mut up, &mut io);
    match io.pop() {
        Some(Lower::Abort { entity }) => assert_eq!(entity, Token::new(19)),
        other => panic!("expected abort, got {other:?}"),
    }
    component.up(&env, LowerEvent::Closed { owner }, &mut up, &mut io);
    match up.pop() {
        Some(Event::Cancelled { call }) => assert_eq!(call, Token::new(7)),
        other => panic!("expected cancellation, got {other:?}"),
    }
    assert!(up.is_empty());
    component.reclaim();
}

#[test]
fn connect_and_whole_deadlines_fail_unsent_calls_once() {
    for timed in [
        Deadlines { connect: Some(Duration::from_secs(1)), ..Deadlines::none() },
        Deadlines { whole: Some(Duration::from_secs(1)), ..Deadlines::none() },
    ] {
        let mut component = component();
        let mut up = Queue::with_capacity(MAX_OUT.above);
        let mut io = Queue::with_capacity(MAX_OUT.below);
        let start = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
        component.down(
            &start,
            Request::Start {
                call: Token::new(7),
                endpoint: 0,
                prompt: prompt(),
                credential: credential(),
                deadlines: timed,
            },
            &mut up,
            &mut io,
        );
        let owner = match io.pop() {
            Some(Lower::Connect { owner, .. }) => owner,
            other => panic!("expected connect, got {other:?}"),
        };
        assert_eq!(component.next_deadline(), Some(Time::from_nanos(1_000_000_000)));
        let fire = Env { now: Time::from_nanos(1_000_000_000), wall: Wall::EPOCH, limits: limits() };
        component.fire(&fire, &mut up, &mut io);
        match up.pop() {
            Some(Event::Failed { call, failure, evidence, .. }) => {
                assert_eq!(call, Token::new(7));
                assert_eq!(failure, skein_llm::Failure::TimedOut);
                assert_eq!(evidence, skein_llm::client::Evidence::Unsent);
            }
            other => panic!("expected timeout, got {other:?}"),
        }
        assert_eq!(component.next_deadline(), None);
        component.up(&fire, LowerEvent::Connecting { owner, socket: Token::new(19) }, &mut up, &mut io);
        component.up(&fire, LowerEvent::Closed { owner }, &mut up, &mut io);
        assert!(up.is_empty(), "late settlement cannot repeat a timeout");
        component.reclaim();
    }
}

#[test]
fn a_handshake_failure_has_one_unsent_terminal() {
    let mut component = component();
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let owner = start(&mut component, &mut up, &mut io);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    component.up(&env, LowerEvent::Connecting { owner, socket: Token::new(19) }, &mut up, &mut io);
    component.up(&env, LowerEvent::Connected { owner }, &mut up, &mut io);
    component.up(
        &env,
        LowerEvent::Stream { owner, up: skein_lib::stream::Up::Failed(skein_lib::stream::Fault::Reset) },
        &mut up,
        &mut io,
    );
    match up.pop() {
        Some(Event::Failed { call, failure, evidence, .. }) => {
            assert_eq!(call, Token::new(7));
            assert_eq!(failure, skein_llm::Failure::Unavailable);
            assert_eq!(evidence, skein_llm::client::Evidence::Unsent);
        }
        other => panic!("expected handshake failure, got {other:?}"),
    }
    component.down(&env, Request::Cancel { call: Token::new(7) }, &mut up, &mut io);
    component.up(&env, LowerEvent::Closed { owner }, &mut up, &mut io);
    assert!(up.is_empty());
    component.reclaim();
}

#[test]
fn a_full_pool_refuses_the_second_call() {
    let mut component = component();
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let _owner = start(&mut component, &mut up, &mut io);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    component.down(
        &env,
        Request::Start {
            call: Token::new(8),
            endpoint: 0,
            prompt: prompt(),
            credential: credential(),
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
    match up.pop() {
        Some(Event::Refused { call, why }) => {
            assert_eq!(call, Token::new(8));
            assert_eq!(why, Refusal::Pool);
        }
        other => panic!("expected pool refusal, got {other:?}"),
    }
}

#[test]
#[expect(clippy::disallowed_methods, reason = "test fixtures parse literal socket addresses outside step code")]
fn plaintext_admission_checks_the_exact_loopback_ranges() {
    for (address, admitted) in [
        ("127.0.0.1:80", true),
        ("127.0.0.0:80", true),
        ("127.255.255.255:80", true),
        ("126.255.255.255:80", false),
        ("128.0.0.0:80", false),
        ("0.0.0.0:80", false),
        ("[::1]:80", true),
        ("[::]:80", false),
        ("[::2]:80", false),
        ("[::ffff:127.0.0.1]:80", false),
    ] {
        let mut endpoints = List::with_capacity(1);
        endpoints
            .push(Endpoint {
                address: address.parse().expect("test address"),
                transport: Transport::Plaintext,
                llm: skein_llm::Endpoint::codex(),
            })
            .expect("one endpoint");
        assert_eq!(
            Component::new(endpoints, &limits()).err(),
            if admitted { None } else { Some(EndpointError::PlaintextAddress) },
            "{address}"
        );
    }
}

#[test]
fn plaintext_connect_starts_http_and_never_arms_a_handshake_deadline() {
    let mut endpoints = List::with_capacity(1);
    endpoints
        .push(Endpoint {
            address: Addr::from((Ipv4Addr::LOCALHOST, 80)),
            transport: Transport::Plaintext,
            llm: skein_llm::Endpoint::codex(),
        })
        .expect("one endpoint");
    let mut config = limits();
    // These capacities fit HTTP directly, but cannot fit TLS ciphertext.
    config.io.intake = 4096;
    config.io.output = 4096;
    let mut component = Component::new(endpoints, &config).expect("plaintext only needs HTTP capacities");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    component.down(
        &env,
        Request::Start {
            call: Token::new(7),
            endpoint: 0,
            prompt: prompt(),
            credential: credential(),
            deadlines: Deadlines {
                connect: Some(Duration::from_secs(2)),
                handshake: Some(Duration::from_secs(1)),
                ..Deadlines::none()
            },
        },
        &mut up,
        &mut io,
    );
    let owner = match io.pop() {
        Some(Lower::Connect { owner, .. }) => owner,
        other => panic!("expected connect, got {other:?}"),
    };
    component.up(&env, LowerEvent::Connecting { owner, socket: Token::new(19) }, &mut up, &mut io);
    component.up(&env, LowerEvent::Connected { owner }, &mut up, &mut io);
    assert_eq!(component.next_deadline(), None);
    let room = match io.pop() {
        Some(Lower::Stream { down: skein_lib::stream::Down::Demand { read, room }, .. }) => {
            assert_eq!(read, skein_lib::stream::Read::Nothing);
            room
        }
        other => panic!("expected HTTP output demand, got {other:?}"),
    };
    assert!(room > 0);
    component.up(&env, LowerEvent::Stream { owner, up: skein_lib::stream::Up::Room }, &mut up, &mut io);
    match io.pop() {
        Some(Lower::Stream { down: skein_lib::stream::Down::Send(bytes), .. }) => {
            assert!(bytes.starts_with(b"POST /"), "the first bytes are HTTP, without a TLS flight");
        }
        other => panic!("expected HTTP request bytes, got {other:?}"),
    }
    let fire = Env { now: Time::from_nanos(3_000_000_000), ..env };
    component.fire(&fire, &mut up, &mut io);
    assert!(up.is_empty(), "the configured TLS deadline never fires on plaintext");
}

#[test]
fn close_refuses_new_calls_without_touching_io() {
    let mut component = component();
    component.close();
    component.close();
    assert_eq!(component.admit(0, Token::new(7), 0, prompt(), credential()).err(), Some(Refusal::Closed));
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    component.down(
        &env,
        Request::Start {
            call: Token::new(8),
            endpoint: 0,
            prompt: prompt(),
            credential: credential(),
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
    match up.pop() {
        Some(Event::Refused { call, why }) => {
            assert_eq!(call, Token::new(8));
            assert_eq!(why, Refusal::Closed);
        }
        other => panic!("expected closed refusal, got {other:?}"),
    }
    component.fire(&env, &mut up, &mut io);
    assert!(up.is_empty() && io.is_empty() && !component.has_work());
}

#[test]
fn close_starts_one_binding_per_fire_and_waits_for_physical_settlement() {
    let mut limits = limits();
    limits.connections = 2;
    limits.per_endpoint = 2;
    limits.io.sockets = 2;
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint");
    let mut component = Component::new(endpoints, &limits).expect("two bounded bindings");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let mut owners = List::with_capacity(2);
    for number in 7..9 {
        component.down(
            &env,
            Request::Start {
                call: Token::new(number),
                endpoint: 0,
                prompt: prompt(),
                credential: credential(),
                deadlines: Deadlines { whole: Some(Duration::from_secs(1)), ..Deadlines::none() },
            },
            &mut up,
            &mut io,
        );
        let Some(Lower::Connect { owner, .. }) = io.pop() else { panic!("one connect") };
        owners.push(owner).expect("two connection owners");
        component.up(&env, LowerEvent::Connecting { owner, socket: Token::new(number + 100) }, &mut up, &mut io);
    }
    component.close();
    component.close();
    assert!(component.has_work());
    for number in 7..9 {
        component.fire(&env, &mut up, &mut io);
        match io.pop() {
            Some(Lower::Abort { entity }) => assert_eq!(entity, Token::new(number + 100)),
            other => panic!("expected abort, got {other:?}"),
        }
        assert!(io.is_empty(), "one fire starts only one binding's close");
        assert!(up.is_empty(), "active cancellation waits for physical settlement");
    }
    assert!(!component.has_work(), "closing bindings await io without spinning");
    assert!(component.next_deadline().is_none(), "physical closing belongs to io's deadlines");
    component.fire(&env, &mut up, &mut io);
    assert!(up.is_empty() && io.is_empty());
    for (index, owner) in owners.iter().enumerate() {
        component.up(&env, LowerEvent::Closed { owner: *owner }, &mut up, &mut io);
        match up.pop() {
            Some(Event::Cancelled { call }) => {
                assert_eq!(call, Token::new(u64::try_from(index).expect("two indices") + 7));
            }
            other => panic!("expected cancellation, got {other:?}"),
        }
        component.reclaim();
        component.up(&env, LowerEvent::Closed { owner: *owner }, &mut up, &mut io);
        assert!(up.is_empty(), "a stale settlement gives no second terminal");
    }
    assert!(!component.has_work() && component.next_deadline().is_none());
}
