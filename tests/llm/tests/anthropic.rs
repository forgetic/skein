//! Handwritten synthetic Anthropic Messages fixtures. These exercise the
//! complete HTTP/SSE/client stack independently of the product encoders;
//! they are not captured OAuth subscription traffic.
use skein_lib::{Duration, Token};
use skein_llm::{
    Block, Completion, Credential, Delta, Endpoint, Error, Failure, Message, Provider, Role, Stop, client,
};
use skein_llm_world::{World, call, limits, response};

const START: &str = r#"{"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"fixture-model","content":[],"stop_reason":null,"usage":{"input_tokens":7,"cache_read_input_tokens":11,"cache_creation_input_tokens":13,"output_tokens":1}}}"#;
const TEXT_START: &str = r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#;
const TEXT_DELTA: &str = r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello 🌍"}}"#;
const BLOCK_STOP: &str = r#"{"type":"content_block_stop","index":0}"#;
const END_TURN: &str =
    r#"{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":9}}"#;
const STOP: &str = r#"{"type":"message_stop"}"#;

fn anthropic_call(owner: u64) -> skein_llm::Call {
    let mut input = call(owner);
    input.endpoint = Endpoint::anthropic();
    input.credential = Credential::anthropic(b"synthetic-oauth-token".to_vec().into());
    input.prompt.cache_key = None;
    input.prompt.max_output_tokens = Some(1024);
    input
}

fn named_events(documents: &[&str]) -> Vec<u8> {
    // Event names and JSON types agree, as in the Messages stream protocol.
    documents
        .iter()
        .flat_map(|document| {
            let kind = document
                .strip_prefix(r#"{"type":""#)
                .expect("synthetic event begins with a type")
                .split('"')
                .next()
                .expect("synthetic event has a type name");
            format!("event: {kind}\ndata: {document}\n\n").into_bytes()
        })
        .collect()
}

fn stream(documents: &[&str], chunked: bool) -> Vec<u8> {
    response(200, "Content-Type: text/event-stream\r\n", &named_events(documents), chunked)
}

fn text_response(chunked: bool) -> Vec<u8> {
    stream(&[START, TEXT_START, TEXT_DELTA, BLOCK_STOP, END_TURN, STOP], chunked)
}

fn completion(world: &World) -> &Completion {
    world
        .seen
        .iter()
        .find_map(|event| if let client::Event::Completed { completion, .. } = event { Some(completion) } else { None })
        .expect("stream completed")
}

#[test]
fn anthropic_requires_an_explicit_event_stream_content_type() {
    let body = named_events(&[START, TEXT_START, TEXT_DELTA, BLOCK_STOP, END_TURN, STOP]);
    let mut world = World::new(anthropic_call(1), limits(), response(200, "", &body, true), 23);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|event| matches!(
        event,
        client::Event::Failed { failure: Failure::Protocol, detail, .. }
            if detail.as_ref() == b"response is not an event stream"
    )));
}

#[test]
fn oauth_request_text_usage_and_http_fragmentation() {
    for chunked in [false, true] {
        for fragment in [1, 257] {
            let mut world = World::new(anthropic_call(7), limits(), text_response(chunked), 2);
            world.fragmentation(fragment, 3);
            world.request(client::Request::Next);
            world.request(client::Request::Start);
            world.run();
            world.assert_once();
            assert_eq!(world.machine.waiting(), client::Waiting::Idle);
            let sent = String::from_utf8(world.sent.clone()).unwrap();
            let (head, body) = sent.split_once("\r\n\r\n").unwrap();
            assert!(head.starts_with("POST /v1/messages HTTP/1.1\r\n"));
            let lower = head.to_ascii_lowercase();
            for expected in [
                "host: api.anthropic.com",
                "authorization: bearer synthetic-oauth-token",
                "anthropic-version: 2023-06-01",
                "oauth-2025-04-20",
            ] {
                assert!(lower.contains(expected), "{head}");
            }
            assert!(!lower.contains("chatgpt-account-id"), "{head}");
            assert!(lower.contains(&format!("content-length: {}", body.len())));
            for expected in
                ["\"model\":\"fixture-model\"", "\"stream\":true", "\"max_tokens\":1024", "Be brief.", "Hello"]
            {
                assert!(body.contains(expected), "{body}");
            }
            assert!(!body.contains("You are Claude Code"), "{body}");
            assert!(world.seen.iter().any(|event| matches!(event, client::Event::Delta { owner, delta: Delta::Text { index: 0, text, .. } } if *owner == Token::new(7) && text.as_ref() == "Hello 🌍".as_bytes())));
            let answer = completion(&world);
            assert_eq!(answer.stop, Stop::EndTurn);
            assert_eq!(answer.usage.input_tokens, 7);
            assert_eq!(answer.usage.cache_read_tokens, 11);
            assert_eq!(answer.usage.cache_write_tokens, 13);
            assert_eq!(answer.usage.output_tokens, 9);
            assert!(matches!(&answer.content[0], Block::Text { text, .. } if text.as_ref() == "Hello 🌍".as_bytes()));
        }
    }
}

#[test]
fn tool_arguments_and_native_tool_result_replay_on_followup() {
    let documents = [
        START,
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_native","name":"read","input":{}}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"a\"}"}}"#,
        BLOCK_STOP,
        r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":4}}"#,
        STOP,
    ];
    let mut world = World::new(anthropic_call(1), limits(), stream(&documents, true), 9);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|event| matches!(event, client::Event::Delta { delta: Delta::ToolArguments { index: 0, delta }, .. } if delta.as_ref() == b"{\"path\":")));
    let answer = completion(&world);
    assert_eq!(answer.stop, Stop::ToolUse);
    assert!(
        matches!(&answer.content[0], Block::ToolCall { id, name, arguments, .. } if id.as_ref() == b"tool_native" && name.as_ref() == b"read" && arguments.as_ref() == b"{\"path\":\"a\"}")
    );
    let mut followup = anthropic_call(2);
    followup.prompt.messages = Box::new([
        Message { role: Role::Assistant, content: answer.content.clone() },
        Message {
            role: Role::User,
            content: Box::new([Block::ToolResult {
                id: b"tool_native".to_vec().into(),
                text: b"missing".to_vec().into(),
                is_error: true,
            }]),
        },
    ]);
    let mut next = World::new(followup, limits(), text_response(false), 4);
    next.request(client::Request::Start);
    next.run();
    next.assert_once();
    let wire = String::from_utf8(next.sent).unwrap();
    for expected in [
        "\"id\":\"tool_native\"",
        "\"input\":{\"path\":\"a\"}",
        "\"tool_use_id\":\"tool_native\"",
        "\"is_error\":true",
        "\"type\":\"tool_result\"",
    ] {
        assert!(wire.contains(expected), "{wire}");
    }
}

#[test]
fn unusual_received_tool_id_stays_literal_and_is_rejected_for_replay() {
    let documents = [
        START,
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool|literal","name":"read","input":{}}}"#,
        BLOCK_STOP,
        r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":4}}"#,
        STOP,
    ];
    let mut world = World::new(anthropic_call(1), limits(), stream(&documents, false), 9);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    let answer = completion(&world);
    assert!(matches!(&answer.content[0], Block::ToolCall { id, .. } if id.as_ref() == b"tool|literal"));
    let mut followup = anthropic_call(2);
    followup.prompt.messages = Box::new([Message { role: Role::Assistant, content: answer.content.clone() }]);
    assert!(matches!(client::Client::prepare(followup, &limits()), Err(Error::Invalid)));
}

#[test]
fn signed_and_redacted_thinking_replay_preserves_bytes_and_order() {
    let documents = [
        START,
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Plan carefully"}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-"}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque"}}"#,
        BLOCK_STOP,
        r#"{"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"hidden-opaque"}}"#,
        r#"{"type":"content_block_stop","index":1}"#,
        r#"{"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}"#,
        r#"{"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"Answer"}}"#,
        r#"{"type":"content_block_stop","index":2}"#,
        END_TURN,
        STOP,
    ];
    let mut world = World::new(anthropic_call(1), limits(), stream(&documents, true), 18);
    world.fragmentation(1, 2);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|event| matches!(event, client::Event::Delta { delta: Delta::Reasoning { index: 0, text, .. }, .. } if text.as_ref() == b"Plan carefully")));
    let answer = completion(&world);
    assert_eq!(answer.content.len(), 3);
    assert!(matches!(&answer.content[0], Block::Reasoning { replay } if replay.provider == Provider::Anthropic));
    assert!(matches!(&answer.content[1], Block::Reasoning { replay } if replay.provider == Provider::Anthropic));
    let mut followup = anthropic_call(2);
    followup.prompt.messages = Box::new([Message { role: Role::Assistant, content: answer.content.clone() }]);
    let mut next = World::new(followup, limits(), text_response(false), 4);
    next.request(client::Request::Start);
    next.run();
    next.assert_once();
    let wire = String::from_utf8(next.sent).unwrap();
    for expected in [
        "\"thinking\":\"Plan carefully\"",
        "\"signature\":\"signed-opaque\"",
        "\"type\":\"redacted_thinking\"",
        "\"data\":\"hidden-opaque\"",
    ] {
        assert!(wire.contains(expected), "{wire}");
    }
    assert!(wire.find("signed-opaque").unwrap() < wire.find("hidden-opaque").unwrap());
    assert!(wire.find("hidden-opaque").unwrap() < wire.find("Answer").unwrap());
    let mut other_provider = call(3);
    other_provider.prompt.messages = Box::new([Message { role: Role::Assistant, content: answer.content.clone() }]);
    assert!(matches!(client::Client::prepare(other_provider, &limits()), Err(Error::Unsupported)));
}

#[test]
fn usage_updates_are_snapshots_and_token_limit_is_successful() {
    let documents = [
        START,
        r#"{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":5}}"#,
        r#"{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":9}}"#,
        STOP,
    ];
    let mut world = World::new(anthropic_call(1), limits(), stream(&documents, false), 4);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    let answer = completion(&world);
    assert_eq!(answer.stop, Stop::MaxTokens);
    assert_eq!(answer.usage.output_tokens, 9);
    assert_eq!(answer.usage.input_tokens, 7);
    assert_eq!(answer.usage.cache_read_tokens, 11);
    assert_eq!(answer.usage.cache_write_tokens, 13);
}

#[test]
fn malformed_truncated_and_out_of_order_streams_fail_once() {
    let cases: &[&[&str]] = &[
        &["not-json"],
        &[START],
        &[START, TEXT_START, TEXT_DELTA, END_TURN, STOP],
        &[TEXT_START, START, END_TURN, STOP],
        &[START, TEXT_DELTA, END_TURN, STOP],
        &[START, TEXT_START, TEXT_START, BLOCK_STOP, END_TURN, STOP],
        &[
            START,
            TEXT_START,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}"#,
            BLOCK_STOP,
            END_TURN,
            STOP,
        ],
        &[START, BLOCK_STOP, END_TURN, STOP],
    ];
    for (index, documents) in cases.iter().enumerate() {
        let body = if index == 0 { b"data: not-json\n\n".to_vec() } else { named_events(documents) };
        let mut world = World::new(
            anthropic_call(1),
            limits(),
            response(200, "Content-Type: text/event-stream\r\n", &body, index % 2 == 0),
            2,
        );
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(
            world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: Failure::Protocol, .. })),
            "case {index}: {:?}",
            world.seen
        );
        world.settle();
        world.assert_once();
    }
}

#[test]
fn nested_provider_errors_classify_http_and_sse_with_retry_headers() {
    for (status, body, headers, expected) in [
        (
            401,
            r#"{"type":"error","error":{"type":"authentication_error","message":"expired token"}}"#,
            "",
            Failure::Unauthorized,
        ),
        (
            429,
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#,
            "Retry-After: 13\r\n",
            Failure::RateLimited { retry_after: Duration::from_secs(13) },
        ),
        (
            429,
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"subscription spent"}}"#,
            "Retry-After: 17\r\nanthropic-ratelimit-unified-status: rejected\r\n",
            Failure::Exhausted { retry_after: Duration::from_secs(17) },
        ),
        (529, r#"{"type":"error","error":{"type":"overloaded_error","message":"busy"}}"#, "", Failure::Overloaded),
        (200, r#"{"type":"error","error":{"type":"overloaded_error","message":"busy"}}"#, "", Failure::Overloaded),
        (
            200,
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#,
            "Retry-After: 19\r\n",
            Failure::RateLimited { retry_after: Duration::from_secs(19) },
        ),
    ] {
        let body = if status == 200 { named_events(&[body]) } else { body.as_bytes().to_vec() };
        let content_type = if status == 200 { "text/event-stream" } else { "application/json" };
        let headers = format!("Content-Type: {content_type}\r\n{headers}");
        let mut world = World::new(anthropic_call(1), limits(), response(status, &headers, &body, true), 10);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure, evidence: client::Evidence::Response, .. } if *failure == expected)), "{:?}", world.seen);
        world.settle();
        world.settle();
        world.assert_once();
    }
}

#[test]
fn next_demands_one_event_and_duplicate_next_is_inert() {
    let mut world = World::new(anthropic_call(1), limits(), text_response(true), 1);
    world.request(client::Request::Start);
    for _ in 0..1000 {
        world.tick(false);
        if world.machine.waiting() == client::Waiting::Next {
            break;
        }
    }
    assert!(world.seen.is_empty());
    assert_eq!(world.machine.waiting(), client::Waiting::Next);
    world.request(client::Request::Next);
    world.request(client::Request::Next);
    for _ in 0..1000 {
        world.tick(false);
        if world.machine.waiting() == client::Waiting::Next {
            break;
        }
    }
    assert_eq!(world.seen.len(), 1, "{:?}", world.seen);
    assert!(matches!(&world.seen[0], client::Event::Delta { .. }));
    for _ in 0..20 {
        world.tick(false);
    }
    assert_eq!(world.seen.len(), 1);
    world.run();
    world.assert_once();
}

#[test]
fn cancellation_settles_once_and_reuse_keeps_provider_binding() {
    for stage in 0..3 {
        let mut world = World::new(anthropic_call(19), limits(), text_response(false), 1);
        if stage > 0 {
            world.request(client::Request::Start);
        }
        if stage == 2 {
            for _ in 0..1000 {
                world.tick(false);
                if world.machine.waiting() == client::Waiting::Next {
                    break;
                }
            }
            world.request(client::Request::Next);
        }
        world.request(client::Request::Cancel);
        world.request(client::Request::Cancel);
        assert_eq!(world.terminals(), 0);
        assert_eq!(world.machine.waiting(), client::Waiting::Closing);
        world.settle();
        world.settle();
        world.assert_once();
        assert!(
            world
                .seen
                .iter()
                .any(|event| matches!(event, client::Event::Cancelled { owner } if *owner == Token::new(19)))
        );
    }
    let mut wire = text_response(true);
    wire.extend(text_response(false));
    let mut world = World::new(anthropic_call(1), limits(), wire, 8);
    world.request(client::Request::Start);
    world.run();
    assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    // Match the authority deliberately: provider is independently part of the binding.
    let mut mismatched = call(2);
    mismatched.endpoint.authority = Endpoint::anthropic().authority;
    let rejected = world.machine.next_call(client::Client::prepare(mismatched, &limits()).unwrap()).err().unwrap();
    assert_eq!(rejected.owner(), Token::new(2));
    assert!(world.machine.next_call(client::Client::prepare(anthropic_call(3), &limits()).unwrap()).is_ok());
    world.request(client::Request::Start);
    world.run();
    assert_eq!(world.terminals(), 2);
    assert!(
        world
            .seen
            .iter()
            .any(|event| matches!(event, client::Event::Completed { owner, .. } if *owner == Token::new(3)))
    );
}

#[test]
fn bounded_arguments_and_truncated_http_fail_once() {
    let mut bounded = limits();
    bounded.dialect.input_bytes = 2;
    let documents = [
        START,
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool","name":"read","input":{}}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"a\"}"}}"#,
        BLOCK_STOP,
        END_TURN,
        STOP,
    ];
    let mut world = World::new(anthropic_call(1), bounded, stream(&documents, true), 8);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(
        world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: Failure::Limit, .. })),
        "{:?}",
        world.seen
    );
    let wire = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 900\r\n\r\nevent: message_start\ndata: {".to_vec();
    let mut world = World::new(anthropic_call(1), limits(), wire, 2);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: Failure::Protocol, .. })));
}
