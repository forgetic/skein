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
        calls: 1,
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
        limits: client_limits(),
        credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
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
        choice: skein_llm::ToolChoice::Auto,
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
    assert_eq!(component.admit(Token::new(7), 0, prompt(), credential()).err(), Some(Refusal::Endpoint));
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
        component.admit(Token::new(7), 0, invalid, credential()).err(),
        Some(Refusal::Client(skein_llm::Error::Invalid))
    );
    let mut oversized = prompt();
    oversized.instructions = Box::new([b'x'; 9000]);
    assert_eq!(
        component.admit(Token::new(7), 0, oversized, credential()).err(),
        Some(Refusal::Client(skein_llm::Error::Limit {
            which: skein_llm::Cap::String,
            bound: u64::from(client_limits().dialect.string_bytes)
        }))
    );
}

#[test]
fn startup_rejects_impossible_limits() {
    let mut config = limits();
    config.io.intake = 1;
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    assert_eq!(
        Component::new(endpoints, &config).err(),
        Some(EndpointError::TlsReadIoIntake { demand: skein_tls::client::LARGEST_READ, cap: 1 })
    );
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
        other @ (Event::Closed
        | Event::Refused { .. }
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
        assert!(!component.has_work(), "only io settlement remains, without a past-due wake");
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
fn a_start_past_calls_is_refused_by_name() {
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
            assert_eq!(why, Refusal::Calls { bound: 1 });
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
                limits: client_limits(),
                credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
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
            limits: client_limits(),
            credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
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
fn close_on_an_empty_component_says_closed_without_touching_io() {
    let mut component = component();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    component.down(&env, Request::Close, &mut up, &mut io);
    assert!(is_closed(up.pop()));
    assert!(up.is_empty() && io.is_empty());
    assert!(!component.has_work());
    assert_eq!(component.next_deadline(), None);
}

#[test]
#[should_panic(expected = "after Closed")]
fn a_start_after_closed_is_an_asserted_owner_bug() {
    let mut component = component();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    component.down(&env, Request::Close, &mut up, &mut io);
    component.down(
        &env,
        Request::Start {
            call: Token::new(7),
            endpoint: 0,
            prompt: prompt(),
            credential: credential(),
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
}

#[test]
fn abort_starts_one_binding_per_entrance_and_waits_for_physical_settlement() {
    let mut config = limits();
    config.connections = 2;
    config.calls = 2;
    config.per_endpoint = 2;
    config.io.sockets = 2;
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("endpoint");
    let mut component = Component::new(endpoints, &config).expect("component");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let mut owners = List::with_capacity(2);
    for call in 7..=8 {
        component.down(
            &env,
            Request::Start {
                call: Token::new(call),
                endpoint: 0,
                prompt: prompt(),
                credential: credential(),
                deadlines: Deadlines::none(),
            },
            &mut up,
            &mut io,
        );
        let owner = match io.pop().expect("connect") {
            Lower::Connect { owner, .. } => owner,
            other @ (Lower::Listen { .. }
            | Lower::Bind { .. }
            | Lower::Reject { .. }
            | Lower::Stream { .. }
            | Lower::Output { .. }
            | Lower::Spawn { .. }
            | Lower::Signal { .. }
            | Lower::Close { .. }
            | Lower::Abort { .. }) => panic!("{other:?}"),
        };
        component.up(&env, LowerEvent::Connecting { owner, socket: owner }, &mut up, &mut io);
        owners.push(owner).expect("two owners");
    }
    component.down(&env, Request::Abort, &mut up, &mut io);
    assert_eq!(io.len(), 1);
    assert!(is_abort(io.pop()));
    assert!(up.is_empty());
    assert!(component.has_work(), "the second binding still needs its abort");
    component.fire(&env, &mut up, &mut io);
    assert_eq!(io.len(), 1);
    assert!(is_abort(io.pop()));
    assert!(!component.has_work(), "physical settlement itself is not work");
    assert_eq!(component.next_deadline(), None);
    for owner in &owners {
        component.up(&env, LowerEvent::Closed { owner: *owner }, &mut up, &mut io);
        component.reclaim();
    }
    assert!(is_cancelled(up.pop()));
    assert!(is_cancelled(up.pop()));
    assert!(is_closed(up.pop()));
    assert!(up.is_empty() && io.is_empty());
}

fn is_closed(event: Option<Event>) -> bool {
    match event {
        Some(Event::Closed) => true,
        Some(
            Event::Refused { .. }
            | Event::Delta { .. }
            | Event::Block { .. }
            | Event::Completed { .. }
            | Event::Failed { .. }
            | Event::Cancelled { .. },
        )
        | None => false,
    }
}

fn is_cancelled(event: Option<Event>) -> bool {
    match event {
        Some(Event::Cancelled { .. }) => true,
        Some(
            Event::Closed
            | Event::Refused { .. }
            | Event::Delta { .. }
            | Event::Block { .. }
            | Event::Completed { .. }
            | Event::Failed { .. },
        )
        | None => false,
    }
}

fn is_abort(request: Option<Lower>) -> bool {
    match request {
        Some(Lower::Abort { .. }) => true,
        Some(
            Lower::Listen { .. }
            | Lower::Connect { .. }
            | Lower::Bind { .. }
            | Lower::Reject { .. }
            | Lower::Stream { .. }
            | Lower::Output { .. }
            | Lower::Spawn { .. }
            | Lower::Signal { .. }
            | Lower::Close { .. },
        )
        | None => false,
    }
}

#[test]
fn fewer_connections_than_conversations_is_rejected_by_name() {
    let mut config = limits();
    config.calls = 2;
    assert_eq!(
        Component::new(List::with_capacity(0), &config).err(),
        Some(EndpointError::ConnectionsCalls { connections: 1, calls: 2 })
    );
}

fn client_limits() -> skein_llm::client::Limits {
    skein_llm::client::Limits {
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
    }
}

#[test]
fn each_transport_supplies_its_native_pieces() {
    let config = limits();
    let mut destination = endpoint();
    destination.pieces(&config);
    assert_eq!(destination.limits.http.send, skein_tls::client::MAX_PLAINTEXT);
    assert_eq!(destination.limits.http.read, config.tls.read);
    assert_eq!(destination.limits.sse.chunk, config.tls.read);
    destination.transport = Transport::Plaintext;
    destination.pieces(&config);
    assert_eq!(destination.limits.http.send, config.io.output);
    assert_eq!(destination.limits.http.read, config.io.intake);
    assert_eq!(destination.limits.sse.chunk, config.io.intake);
}

#[test]
fn the_measured_head_accepts_a_credential_at_its_bound_and_refuses_one_byte_more() {
    let config = limits();
    let mut destination = endpoint();
    destination.llm.headers =
        Box::new([skein_http::Header { name: bytes::copy_of(b"x-owner"), value: bytes::copy_of(b"declared") }]);
    destination.credential = skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 };
    let mut endpoints = List::with_capacity(1);
    endpoints.push(destination).expect("endpoint");
    let component = Component::new(endpoints, &config).expect("measured endpoint");
    let mut at_bound = credential();
    at_bound.access_token = Box::new([b'x'; 2048]);
    at_bound.account_id = Box::new([b'y'; 128]);
    drop(
        component.admit(Token::new(7), 0, prompt(), at_bound).expect("the measured head holds the declared credential"),
    );
    let mut too_long = credential();
    too_long.access_token = Box::new([b'x'; 2049]);
    assert_eq!(
        component.admit(Token::new(8), 0, prompt(), too_long).err(),
        Some(Refusal::Client(skein_llm::Error::Limit { which: skein_llm::Cap::AccessToken, bound: 2048 }))
    );
}

#[test]
fn two_endpoints_drive_uploads_with_their_own_client_limits() {
    let mut config = limits();
    config.endpoints = 2;
    config.connections = 2;
    config.calls = 2;
    config.io.sockets = 2;
    let mut endpoints = List::with_capacity(2);
    for send in [16, 64] {
        let mut destination = endpoint();
        destination.transport = Transport::Plaintext;
        destination.limits.http.send = send;
        endpoints.push(destination).expect("endpoint");
    }
    let mut component = Component::new(endpoints, &config).expect("two endpoints");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    for index in 0..2 {
        component.down(
            &env,
            Request::Start {
                call: Token::new(u64::from(index) + 7),
                endpoint: index,
                prompt: prompt(),
                credential: credential(),
                deadlines: Deadlines::none(),
            },
            &mut up,
            &mut io,
        );
        let owner = match io.pop().expect("connect") {
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
        };
        component.up(&env, LowerEvent::Connecting { owner, socket: owner }, &mut up, &mut io);
        component.up(&env, LowerEvent::Connected { owner }, &mut up, &mut io);
        component.up(&env, LowerEvent::Stream { owner, up: skein_lib::stream::Up::Room }, &mut up, &mut io);
        let expected = if index == 0 { 16 } else { 64 };
        let mut found = false;
        for _ in 0..io.len() {
            match io.pop().expect("io request") {
                Lower::Stream { down: skein_lib::stream::Down::Demand { room, .. }, .. } if room == expected => {
                    found = true;
                }
                Lower::Stream { .. }
                | Lower::Connect { .. }
                | Lower::Close { .. }
                | Lower::Abort { .. }
                | Lower::Listen { .. }
                | Lower::Bind { .. }
                | Lower::Reject { .. }
                | Lower::Output { .. }
                | Lower::Spawn { .. }
                | Lower::Signal { .. } => {}
            }
        }
        assert!(found, "the endpoint's upload piece is used");
    }
}

#[test]
fn construction_names_each_stream_capacity_relationship() {
    for check in 0_u32..7 {
        let mut config = limits();
        let mut destination = endpoint();
        let read = skein_llm::client::largest_read(&destination.limits);
        let head = skein_llm::client::request_head(&destination.llm, &destination.credential, &destination.limits)
            .expect("head bound");
        let expected = match check {
            0 => {
                destination.transport = Transport::Plaintext;
                config.io.intake = 1;
                EndpointError::HttpReadIoIntake { demand: read, cap: 1 }
            }
            1 => {
                destination.transport = Transport::Plaintext;
                config.io.output = 1;
                EndpointError::HttpSendIoOutput { demand: head, cap: 1 }
            }
            2 => {
                config.tls.read = 1;
                EndpointError::HttpReadTlsRead { demand: read, cap: 1 }
            }
            3 => {
                config.tls.send = 1;
                EndpointError::HttpSendTlsSend { demand: head, cap: 1 }
            }
            4 => {
                config.io.intake = 1;
                EndpointError::TlsReadIoIntake { demand: skein_tls::client::LARGEST_READ, cap: 1 }
            }
            5 => {
                config.io.output = 1;
                EndpointError::TlsSendIoOutput { demand: skein_tls::client::largest_room(&config.tls), cap: 1 }
            }
            6 => {
                destination.limits.sse.chunk = 257;
                EndpointError::SseChunkHttpRead { demand: 257, cap: 256 }
            }
            _ => unreachable!("seven relationships"),
        };
        let mut endpoints = List::with_capacity(1);
        endpoints.push(destination).expect("endpoint");
        assert_eq!(Component::new(endpoints, &config).err(), Some(expected));
    }
}
