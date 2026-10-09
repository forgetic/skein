//! The component and a sample owner over a seeded in-memory plaintext stream.

use core::net::Ipv4Addr;

use skein_io::kernel::Addr;
use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::Wall;
use skein_lib::{Duration, Env, List, Queue, Time, Token};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, MAX_OUT, Request};
use skein_llm_connection_world::plaintext::Wire;
use skein_world::domain::assert_replays;

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

fn complete_requests(bytes: &[u8]) -> usize {
    let mut cursor = 0_usize;
    let mut count = 0_usize;
    while cursor < bytes.len() {
        let Some(head_end) = bytes[cursor..].windows(4).position(|part| part == b"\r\n\r\n") else {
            break;
        };
        let head_end = cursor + head_end + 4;
        let head = String::from_utf8_lossy(&bytes[cursor..head_end]);
        let Some(length) = head
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .and_then(|text| text.trim().parse::<usize>().ok())
        else {
            break;
        };
        let end = head_end + length;
        if end > bytes.len() {
            break;
        }
        count += 1;
        cursor = end;
    }
    count
}

#[expect(clippy::too_many_lines, reason = "the story drives both calls through one connection")]
fn run(seed: u64) -> (Vec<String>, (u32, u32, bool)) {
    let call = skein_llm_world::call(7);
    let mut endpoints = List::with_capacity(1);
    endpoints
        .push(Endpoint {
            address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
            transport: skein_llm_connection::Transport::Plaintext,
            llm: call.endpoint,
            limits: skein_llm_world::limits(),
            credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
        })
        .expect("one endpoint");
    let mut component = Component::new(endpoints, &limits()).expect("valid component");
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    component.down(
        &env,
        Request::Start {
            drop_reasoning: false,
            call: Token::new(7),
            endpoint: 0,
            prompt: call.prompt,
            credential: call.credential,
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
    component.down(&env, Request::Next { call: Token::new(7) }, &mut up, &mut io);
    let mut wire = Wire::new(seed);
    let response = skein_llm_world::text_response(false);
    let mut owner = None;
    let mut responded = 0_usize;
    let mut second_started = false;
    let mut completed = 0_u32;
    let mut fragments = 0_u32;
    for _ in 0_u32..20_000 {
        if let Some(event) = up.pop() {
            wire.trace.push(format!("owner {event:?}"));
            match event {
                Event::Delta { call, .. } | Event::Block { call, .. } => {
                    assert!(call == Token::new(7) || call == Token::new(8));
                    fragments += 1;
                    component.down(&env, Request::Next { call }, &mut up, &mut io);
                }
                Event::Completed { call, completion } => {
                    assert_eq!(call, Token::new(u64::from(completed) + 7));
                    assert!(!completion.content.is_empty());
                    completed += 1;
                }
                Event::Closed | Event::Refused { .. } | Event::Failed { .. } | Event::Cancelled { .. } => {
                    panic!("unexpected terminal: {event:?}");
                }
            }
        }
        if let Some(request) = io.pop() {
            match request {
                IoRequest::Connect { owner: token, .. } => {
                    assert!(owner.replace(token).is_none());
                    component.up(&env, IoEvent::Connecting { owner: token, socket: Token::new(100) }, &mut up, &mut io);
                    component.up(&env, IoEvent::Connected { owner: token }, &mut up, &mut io);
                }
                IoRequest::Stream { stream, down } => {
                    assert_eq!(stream, Token::new(100));
                    wire.take(down);
                }
                IoRequest::Close { entity } | IoRequest::Abort { entity } => {
                    assert_eq!(entity, Token::new(100));
                    component.up(&env, IoEvent::Closed { owner: owner.expect("connected owner") }, &mut up, &mut io);
                }
                other @ (IoRequest::Listen { .. }
                | IoRequest::Bind { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Usage { .. }
                | IoRequest::Signal { .. }) => panic!("unexpected io request: {other:?}"),
            }
        }
        let requests = complete_requests(&wire.received);
        if requests > responded {
            wire.write(&response);
            responded += 1;
        }
        if let Some(answer) = wire.answer() {
            component.up(
                &env,
                IoEvent::Stream { owner: owner.expect("connected owner"), up: answer },
                &mut up,
                &mut io,
            );
        }
        if component.has_work() {
            component.fire(&env, &mut up, &mut io);
        }
        if completed == 1 && !component.has_work() && io.is_empty() && up.is_empty() && !second_started {
            let second = skein_llm_world::call(8);
            component.down(
                &env,
                Request::Start {
                    drop_reasoning: false,
                    call: Token::new(8),
                    endpoint: 0,
                    prompt: second.prompt,
                    credential: second.credential,
                    deadlines: Deadlines::none(),
                },
                &mut up,
                &mut io,
            );
            component.down(&env, Request::Next { call: Token::new(8) }, &mut up, &mut io);
            second_started = true;
        }
        if completed == 2 && !component.has_work() && io.is_empty() && up.is_empty() {
            break;
        }
    }
    assert_eq!(responded, 2, "the server saw both requests");
    assert_eq!(completed, 2);
    assert!(fragments > 0);

    component.down(&env, Request::Cancel { call: Token::new(8) }, &mut up, &mut io);
    assert!(up.is_empty() && io.is_empty(), "a cancel after completion is inert");

    let later = Env { now: Time::from_nanos(11_000_000_000), wall: Wall::EPOCH, limits: limits() };
    component.fire(&later, &mut up, &mut io);
    let mut closed = false;
    for _ in 0_u32..1000 {
        if let Some(request) = io.pop() {
            match request {
                IoRequest::Stream { down, .. } => wire.take(down),
                IoRequest::Close { entity } => {
                    assert_eq!(entity, Token::new(100));
                    component.up(&later, IoEvent::Closed { owner: owner.expect("connected owner") }, &mut up, &mut io);
                    component.reclaim();
                    closed = true;
                    break;
                }
                other @ (IoRequest::Listen { .. }
                | IoRequest::Connect { .. }
                | IoRequest::Bind { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Usage { .. }
                | IoRequest::Signal { .. }
                | IoRequest::Abort { .. }) => panic!("unexpected idle close request: {other:?}"),
            }
        }
        if let Some(answer) = wire.answer() {
            component.up(
                &later,
                IoEvent::Stream { owner: owner.expect("connected owner"), up: answer },
                &mut up,
                &mut io,
            );
        }
        if component.has_work() {
            component.fire(&later, &mut up, &mut io);
        }
    }
    assert!(closed, "the idle connection closed after its keep time");
    (wire.trace, (completed, fragments, closed))
}

#[test]
fn codex_calls_complete_reuse_and_replay_one_plaintext_connection() {
    assert_replays(7, 8, run);
}

#[expect(clippy::too_many_lines, reason = "the story drives three model policies through one actual connection")]
fn per_call_reasoning_policy(endpoint_default: bool) {
    let mut client_limits = skein_llm_world::limits();
    client_limits.drop_reasoning = endpoint_default;
    client_limits.dialect.opaque_bytes = 24;
    let mut endpoints = List::with_capacity(1);
    endpoints
        .push(Endpoint {
            address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
            transport: skein_llm_connection::Transport::Plaintext,
            llm: skein_llm::Endpoint::codex(),
            limits: client_limits,
            credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
        })
        .expect("one shared endpoint");
    let mut component = Component::new(endpoints, &limits()).expect("configured endpoint");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let mut wire = Wire::new(37);
    let documents = [
        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"r","type":"reasoning"}}"#,
        r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"r","type":"reasoning","encrypted_content":"signed","summary":[]}}"#,
        r#"{"type":"response.completed","response":{"status":"completed","usage":{}}}"#,
    ];
    let response = skein_llm_world::response(
        200,
        "Content-Type: text/event-stream\r\n",
        &skein_llm_world::events(&documents),
        false,
    );
    let mut started = 0_u64;
    let mut completed = 0_u32;
    let mut failed = 0_u32;
    let mut dropped = 0_u32;
    let mut connected = 0_u32;
    let mut responded = 0_usize;
    let mut owner = None;
    let mut closing = false;
    let mut closed = false;
    for _ in 0..40_000 {
        if started < 3 && started == u64::from(completed + failed) {
            let mut input = skein_llm_world::call(started + 7);
            input.prompt.model = format!("model-{started}").into_bytes().into_boxed_slice();
            component.down(
                &env,
                Request::Start {
                    call: input.owner,
                    endpoint: 0,
                    prompt: input.prompt,
                    credential: input.credential,
                    deadlines: Deadlines::none(),
                    drop_reasoning: started < 2,
                },
                &mut up,
                &mut io,
            );
            component.down(&env, Request::Next { call: input.owner }, &mut up, &mut io);
            started += 1;
        }
        if let Some(event) = up.pop() {
            match event {
                Event::Delta { call, .. } => component.down(&env, Request::Next { call }, &mut up, &mut io),
                Event::Block { call, block } => {
                    assert!(matches!(block, skein_llm::Block::Dropped { bytes } if bytes > 24));
                    dropped += 1;
                    component.down(&env, Request::Next { call }, &mut up, &mut io);
                }
                Event::Completed { completion, .. } => {
                    assert!(matches!(completion.content.as_ref(), [skein_llm::Block::Dropped { .. }]));
                    completed += 1;
                }
                Event::Failed { call, failure, .. } => {
                    assert_eq!(call, Token::new(9));
                    assert_eq!(failure, skein_llm::Failure::Limit { which: skein_llm::Cap::Opaque, bound: 24 });
                    failed += 1;
                }
                Event::Closed => closed = true,
                Event::Refused { .. } | Event::Cancelled { .. } => panic!("unexpected policy admission/terminal"),
            }
        }
        if let Some(request) = io.pop() {
            match request {
                IoRequest::Connect { owner: token, .. } => {
                    assert!(owner.replace(token).is_none(), "all model policies reuse one connection");
                    connected += 1;
                    component.up(&env, IoEvent::Connecting { owner: token, socket: Token::new(100) }, &mut up, &mut io);
                    component.up(&env, IoEvent::Connected { owner: token }, &mut up, &mut io);
                }
                IoRequest::Stream { down, .. } => wire.take(down),
                IoRequest::Close { .. } | IoRequest::Abort { .. } => {
                    component.up(&env, IoEvent::Closed { owner: owner.expect("connected owner") }, &mut up, &mut io);
                    component.reclaim();
                }
                IoRequest::Listen { .. }
                | IoRequest::Bind { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Usage { .. }
                | IoRequest::Signal { .. } => panic!("unexpected IO"),
            }
        }
        if complete_requests(&wire.received) > responded {
            wire.write(&response);
            responded += 1;
        }
        if let Some(answer) = wire.answer() {
            component.up(
                &env,
                IoEvent::Stream { owner: owner.expect("connected owner"), up: answer },
                &mut up,
                &mut io,
            );
        }
        if completed + failed == 3 && !closing {
            component.down(&env, Request::Close, &mut up, &mut io);
            closing = true;
        }
        if component.has_work() {
            component.fire(&env, &mut up, &mut io);
        }
        if closed && up.is_empty() && io.is_empty() && !component.has_work() {
            break;
        }
    }
    assert_eq!((started, completed, failed, dropped, connected, responded, closed), (3, 2, 1, 2, 1, 3, true));
    for model in [b"model-0", b"model-1", b"model-2"] {
        assert!(wire.received.windows(model.len()).any(|bytes| bytes == model));
    }
}

#[test]
fn model_reasoning_policy_overrides_both_defaults_and_does_not_leak_on_reuse() {
    for endpoint_default in [false, true] {
        per_call_reasoning_policy(endpoint_default);
    }
}
