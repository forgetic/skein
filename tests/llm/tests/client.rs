use skein_lib::{Duration, Token};
use skein_llm::{Block, Delta, Error, Failure, Stop, client};
use skein_llm_world::{TERMINAL, World, call, events, limits, response, text_response};

#[test]
fn wire_request_and_streamed_answer() {
    for chunked in [false, true] {
        let mut world = World::new(call(7), limits(), text_response(chunked), 2);
        world.fragmentation(1, 3);
        world.request(client::Request::Next);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        let sent = String::from_utf8(world.sent.clone()).unwrap();
        let (head, body) = sent.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("POST /backend-api/codex/responses HTTP/1.1\r\n"));
        for header in [
            "Host: chatgpt.com",
            "Authorization: Bearer secret-test-token",
            "chatgpt-account-id: account-test",
            "originator: skein-world",
        ] {
            assert!(head.to_ascii_lowercase().contains(&header.to_ascii_lowercase()), "{head}");
        }
        assert!(head.to_ascii_lowercase().contains(&format!("content-length: {}", body.len())));
        for property in [
            "\"model\":\"fixture-model\"",
            "\"stream\":true",
            "\"store\":false",
            "\"prompt_cache_key\":\"42424242-4242-4242-4242-424242424242\"",
            "reasoning.encrypted_content",
            "Hello",
        ] {
            assert!(body.contains(property), "{body}");
        }
        assert!(world.seen.iter().any(|e| matches!(e, client::Event::Delta { owner, delta: Delta::Text { text, .. }} if *owner == Token::new(7) && text.as_ref() == b"Hello")));
        let completion = world
            .seen
            .iter()
            .find_map(|e| {
                if let client::Event::Completed { owner, completion } = e {
                    assert_eq!(*owner, Token::new(7));
                    Some(completion)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(completion.stop, Stop::EndTurn);
        assert_eq!(completion.usage.input, Some(8));
        assert_eq!(completion.usage.cache_read, Some(4));
        assert!(
            completion
                .content
                .iter()
                .any(|b| matches!(b, Block::Text { text, replay: Some(_) } if text.as_ref() == b"Hello"))
        );
        assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    }
}

#[test]
fn next_is_one_event_and_duplicate_demand_is_inert() {
    let mut world = World::new(call(1), limits(), text_response(false), 1);
    world.request(client::Request::Start);
    for _ in 0..200 {
        if !world.tick(false) {
            break;
        }
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
    assert_eq!(
        world
            .seen
            .iter()
            .filter(|e| matches!(
                e,
                client::Event::Delta { .. } | client::Event::Block { .. } | client::Event::Completed { .. }
            ))
            .count(),
        1
    );
    for _ in 0..20 {
        world.tick(false);
    }
    assert_eq!(world.seen.len(), 1);
    world.run();
    world.assert_once();
}

#[test]
fn reasoning_and_parallel_tools_preserve_replay_and_order() {
    let docs = [
        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning"}}"#,
        r#"{"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":"Plan"}"#,
        r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call"}}"#,
        r#"{"type":"response.function_call_arguments.delta","output_index":1,"delta":"{\"x\":1}"}"#,
        r#"{"type":"response.output_item.added","output_index":2,"item":{"id":"fc_2","type":"function_call"}}"#,
        r#"{"type":"response.output_item.done","output_index":2,"item":{"id":"fc_2","type":"function_call","call_id":"call_2","name":"b","arguments":"{}"}}"#,
        r#"{"type":"response.output_item.done","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"a","arguments":"{\"x\":1}"}}"#,
        r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","encrypted_content":"opaque-test","summary":[]}}"#,
        TERMINAL,
    ];
    let mut world =
        World::new(call(1), limits(), response(200, "Content-Type: text/event-stream\r\n", &events(&docs), true), 9);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(
        |e| matches!(e, client::Event::Delta { delta: Delta::Reasoning { text, .. }, .. } if text.as_ref() == b"Plan")
    ));
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Delta { delta: Delta::ToolArguments { delta, .. }, .. } if delta.as_ref() == b"{\"x\":1}")));
    let completion = world
        .seen
        .iter()
        .find_map(|e| if let client::Event::Completed { completion, .. } = e { Some(completion) } else { None })
        .unwrap();
    assert_eq!(completion.stop, Stop::ToolUse);
    assert!(matches!(&completion.content[0], Block::Reasoning { .. }));
    assert!(
        matches!(&completion.content[1], Block::ToolCall { id, arguments, replay: Some(_), .. } if id.as_ref() == b"call_1" && arguments.as_ref() == b"{\"x\":1}")
    );
    assert!(matches!(&completion.content[2], Block::ToolCall { id, .. } if id.as_ref() == b"call_2"));
}

#[test]
fn provider_error_body_end_and_http_done_share_one_terminal() {
    for (status, body, expected) in [
        (401, r#"{"error":{"code":"invalid_api_key","message":"bad token"}}"#, Failure::Unauthorized),
        (
            429,
            r#"{"error":{"code":"rate_limit_exceeded","message":"slow down"}}"#,
            Failure::RateLimited { retry_after: Duration::from_secs(13) },
        ),
        (503, r#"{"error":{"code":"server_error","message":"offline"}}"#, Failure::Unavailable),
    ] {
        let mut world = World::new(
            call(1),
            limits(),
            response(status, "Content-Type: application/json\r\nRetry-After: 13\r\n", body.as_bytes(), false),
            1,
        );
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(world.seen.iter().any(|e| matches!(e, client::Event::Failed { failure, evidence: client::Evidence::Response { status: actual }, .. } if *failure == expected && *actual == status)), "{:?}", world.seen);
        world.settle();
        world.settle();
        world.assert_once();
    }
}

#[test]
fn cancellation_waits_for_actual_settlement() {
    for stage in 0..3 {
        let mut world = World::new(call(19), limits(), text_response(false), 1);
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
        }
        world.request(client::Request::Cancel);
        world.request(client::Request::Cancel);
        assert_eq!(world.terminals(), 0);
        assert_eq!(world.machine.waiting(), client::Waiting::Closing);
        world.settle();
        world.settle();
        world.request(client::Request::Close);
        world.assert_once();
        assert!(world.seen.iter().any(|e| matches!(e, client::Event::Cancelled { owner } if *owner == Token::new(19))));
        assert_eq!(world.seen.iter().filter(|e| matches!(e, client::Event::Close)).count(), 1);
        assert_eq!(world.seen.iter().filter(|e| matches!(e, client::Event::Closed)).count(), 1);
    }
}

#[test]
fn codex_accepts_sse_without_content_type_but_rejects_explicit_wrong_or_duplicate_types() {
    let body =
        events(&[skein_llm_world::TEXT_ADDED, skein_llm_world::TEXT_DELTA, skein_llm_world::TEXT_DONE, TERMINAL]);
    for chunked in [false, true] {
        let mut world = World::new(call(1), limits(), response(200, "", &body, chunked), 23);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(world.seen.iter().any(|event| matches!(event, client::Event::Completed { .. })));
        assert!(world.seen.iter().any(|event| matches!(event, client::Event::Reusable)));
    }
    for headers in [
        "Content-Type: application/json\r\n",
        "Content-Type: text/event-stream\r\ncontent-type: text/event-stream\r\n",
        "Content-Type: text/event-stream\r\ncontent-type: application/json\r\n",
    ] {
        let mut world = World::new(call(1), limits(), response(200, headers, &body, true), 23);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(world.seen.iter().any(|event| matches!(
            event,
            client::Event::Failed { failure: Failure::Protocol, detail, .. }
                if detail.as_ref() == b"response is not an event stream"
        )));
    }
}

#[test]
fn malformed_truncated_and_oversized_streams_fail_once() {
    let mut small = limits();
    small.skip = 32;
    for (wire, bounds) in [
        (response(200, "Content-Type: text/event-stream\r\n", b"data: not-json\n\n", false), limits()),
        (
            response(200, "Content-Type: text/event-stream\r\n", &events(&[skein_llm_world::TEXT_ADDED]), false),
            limits(),
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 900\r\n\r\ndata: {".to_vec(),
            limits(),
        ),
        (text_response(false), small),
        (response(200, "Content-Type: application/json\r\n", b"{}", false), limits()),
        (
            response(200, "Content-Type: text/event-stream\r\nContent-Encoding: gzip\r\n", &events(&[TERMINAL]), false),
            limits(),
        ),
    ] {
        let mut world = World::new(call(1), bounds, wire, 2);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(world.seen.iter().any(|e| matches!(e, client::Event::Failed { .. })), "{:?}", world.seen);
        world.settle();
        world.assert_once();
    }
}

#[test]
fn reuse_preserves_binding_and_rejects_other_authority() {
    let mut wire = text_response(true);
    wire.extend(text_response(false));
    let mut world = World::new(call(1), limits(), wire, 8);
    world.request(client::Request::Start);
    world.run();
    assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    let mut changed = call(2);
    changed.endpoint.authority = b"another.example".to_vec().into();
    let rejected = world.machine.next_call(client::Client::prepare(changed, &limits()).unwrap()).err().unwrap();
    assert_eq!(rejected.owner(), Token::new(2));
    assert!(world.machine.next_call(client::Client::prepare(call(3), &limits()).unwrap()).is_ok());
    world.request(client::Request::Start);
    world.run();
    assert_eq!(world.terminals(), 2);
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Completed { owner, .. } if *owner == Token::new(3))));
}

#[test]
fn hostile_request_is_refused_before_connection() {
    for field in 0..4 {
        let mut request = call(1);
        match field {
            0 => request.credential.access_token = b"bad\r\ninjected".to_vec().into(),
            1 => request.credential.account_id = Box::new([]),
            2 => request.endpoint.target = b"/bad target".to_vec().into(),
            3 => request.endpoint.authority = b"bad/name".to_vec().into(),
            _ => unreachable!(),
        }
        assert!(matches!(client::Client::prepare(request, &limits()), Err(Error::Invalid)));
    }
}

#[test]
fn terminal_only_unknown_events_refusal_and_token_limit() {
    let empty_terminal = TERMINAL.to_owned();
    let refusal_done = r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","content":[{"type":"refusal","refusal":"Cannot comply"}]}}"#.to_owned();
    let incomplete = r#"{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":0,"output_tokens":1}}}"#.to_owned();
    for (documents, stop) in [
        (vec![r#"{"type":"future.event","extra":[1,2]}"#.to_owned(), empty_terminal.clone()], Stop::EndTurn),
        (vec![skein_llm_world::TEXT_ADDED.to_owned(), refusal_done, empty_terminal], Stop::Refusal),
        (vec![incomplete], Stop::MaxTokens),
    ] {
        let refs: Vec<_> = documents.iter().map(String::as_str).collect();
        let mut world = World::new(
            call(1),
            limits(),
            response(200, "Content-Type: text/event-stream\r\n", &events(&refs), false),
            4,
        );
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(
            world
                .seen
                .iter()
                .any(|e| matches!(e, client::Event::Completed { completion, .. } if completion.stop == stop)),
            "{:?}",
            world.seen
        );
    }
}

#[test]
fn error_within_sse_stream_is_terminal_and_uses_headers() {
    let body = events(&[r#"{"type":"error","code":"usage_limit_reached","message":"account spent"}"#]);
    let mut world = World::new(
        call(1),
        limits(),
        response(200, "Content-Type: text/event-stream\r\nRetry-After: 17\r\n", &body, true),
        10,
    );
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Failed { failure: Failure::Exhausted { retry_after }, .. } if *retry_after == Duration::from_secs(17))), "{:?}", world.seen);
    world.settle();
    world.assert_once();
}

#[test]
fn lower_close_before_start_is_unsent_and_cancellation_during_read_settles() {
    let mut world = World::new(call(1), limits(), Vec::new(), 1);
    world.settle();
    world.settle();
    world.assert_once();
    assert!(world.sent.is_empty());
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Failed { evidence: client::Evidence::Unsent, .. })));
    let mut world = World::new(call(2), limits(), text_response(true), 8);
    world.request(client::Request::Start);
    for _ in 0..1000 {
        world.tick(false);
        if world.machine.waiting() == client::Waiting::Next {
            break;
        }
    }
    world.request(client::Request::Next);
    assert!(world.demand.is_some(), "reading SSE creates a lower demand");
    world.request(client::Request::Cancel);
    assert_eq!(world.terminals(), 0);
    world.settle();
    world.assert_once();
}

#[test]
fn completed_messages_replay_reasoning_message_ids_and_tool_ids_on_next_turn() {
    let docs = [
        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning"}}"#,
        r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","encrypted_content":"exact-opaque","summary":[]}}"#,
        r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"msg_1","type":"message"}}"#,
        r#"{"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","phase":"commentary","content":[{"type":"output_text","text":"Reading"}]}}"#,
        r#"{"type":"response.output_item.added","output_index":2,"item":{"id":"fc_1","type":"function_call"}}"#,
        r#"{"type":"response.output_item.done","output_index":2,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"read","arguments":"{\"path\":\"a\"}"}}"#,
        TERMINAL,
    ];
    let mut world =
        World::new(call(1), limits(), response(200, "Content-Type: text/event-stream\r\n", &events(&docs), false), 18);
    world.request(client::Request::Start);
    world.run();
    let completed = world
        .seen
        .iter()
        .find_map(|e| if let client::Event::Completed { completion, .. } = e { Some(completion) } else { None })
        .unwrap();
    let mut followup = call(2);
    followup.prompt.messages = vec![
        skein_llm::Message { role: skein_llm::Role::Assistant, content: completed.content.clone() },
        skein_llm::Message {
            role: skein_llm::Role::User,
            content: Box::new([Block::ToolResult {
                id: b"call_1".to_vec().into(),
                text: b"file contents".to_vec().into(),
                is_error: false,
            }]),
        },
    ]
    .into();
    let mut next = World::new(followup, limits(), text_response(false), 4);
    next.request(client::Request::Start);
    next.run();
    next.assert_once();
    let wire = String::from_utf8(next.sent).unwrap();
    for value in [
        "\"encrypted_content\":\"exact-opaque\"",
        "\"id\":\"msg_1\"",
        "\"phase\":\"commentary\"",
        "\"id\":\"fc_1\"",
        "\"call_id\":\"call_1\"",
        "\"type\":\"function_call_output\"",
    ] {
        assert!(wire.contains(value), "{wire}");
    }
}

#[test]
fn independent_caps_bound_requests_errors_deltas_and_tool_arguments() {
    let mut tiny_request = limits();
    tiny_request.request = 32;
    assert!(matches!(client::Client::prepare(call(1), &tiny_request), Err(Error::Limit { .. })));
    let mut token_cap = limits();
    token_cap.tokens = 2;
    let mut world = World::new(call(1), token_cap, text_response(false), 3);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Failed { failure: Failure::Limit { .. }, .. })));
    let mut short_errors = limits();
    short_errors.error_bytes = 16;
    let mut world = World::new(
        call(1),
        short_errors,
        response(429, "Content-Type: application/json\r\nRetry-After: 4\r\n", &[b'x'; 512], false),
        1,
    );
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|e| matches!(
        e,
        client::Event::Failed {
            failure: Failure::RateLimited { retry_after },
            evidence: client::Evidence::Response { status: 429 },
            ..
        } if *retry_after == Duration::from_secs(4)
    )));
    world.settle();
    world.assert_once();

    let mut bounded = limits();
    bounded.input = 2;
    let tool_documents = [
        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"fc_1","type":"function_call"}}"#,
        r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"read","arguments":"{\"x\":1}"}}"#,
        TERMINAL,
    ];
    let mut world = World::new(
        call(1),
        bounded,
        response(200, "Content-Type: text/event-stream\r\n", &events(&tool_documents), true),
        8,
    );
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(
        world.seen.iter().any(|event| matches!(event, client::Event::Completed { completion, .. }
        if matches!(completion.content.as_ref(), [Block::Oversize { bytes: 7, .. }]))),
        "{:?}",
        world.seen
    );

    let mut bounded = limits();
    bounded.answer = 1;
    // Admission checks the prompt against the same answer cap, so use an
    // empty prompt while the incoming text delta exceeds it.
    let mut request = call(1);
    request.prompt.messages = Box::new([]);
    request.prompt.instructions = Box::new([]);
    let mut world = World::new(request, bounded, text_response(false), 2);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(
        world.seen.iter().any(|e| matches!(e, client::Event::Failed { failure: Failure::Limit { .. }, .. })),
        "{:?}",
        world.seen
    );
}

#[test]
fn exact_request_head_cap_is_checked_before_transport_binding() {
    let mut world = World::new(call(1), limits(), text_response(false), 2);
    world.request(client::Request::Start);
    world.run();
    let head_end = world.sent.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let mut exact = limits();
    exact.http.request = u32::try_from(head_end).unwrap();
    assert!(client::Client::prepare(call(1), &exact).is_ok());
    let wire_body = world.sent.len() - head_end;
    let input = call(1);
    let measured = client::request_head(
        &input.endpoint,
        &client::CredentialLimits {
            access_token: u32::try_from(input.credential.access_token.len()).unwrap(),
            account_id: u32::try_from(input.credential.account_id.len()).unwrap(),
        },
        &limits(),
    )
    .unwrap();
    let content_length_extra = limits().request.to_string().len() - wire_body.to_string().len();
    assert_eq!(
        usize::try_from(measured).unwrap(),
        head_end + content_length_extra,
        "standalone endpoint maximum prices both fixed affinity headers and maximum body length digits"
    );
    exact.http.request -= 1;
    assert!(matches!(
        client::Client::prepare(call(1), &exact),
        Err(Error::Limit { which: skein_llm::Cap::RequestHead, .. })
    ));
}

#[test]
fn early_final_response_stops_the_unfinished_upload() {
    let mut input = call(1);
    input.prompt.instructions = vec![b'a'; 4096].into();
    let mut world = World::new(input, limits(), text_response(false), 3);
    world.fragmentation(256, 200);
    world.allow_early_response();
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.sent.len() < 4096, "provider answered before the long request was sent");
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Completed { .. })), "{:?}", world.seen);
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Close)));
    assert!(!world.seen.iter().any(|e| matches!(e, client::Event::Reusable)));
    world.settle();
    world.assert_once();
}

#[test]
fn transport_failures_are_unsolicited_and_keep_the_terminal_unique() {
    let mut world = World::new(call(1), limits(), text_response(false), 5);
    world.request(client::Request::Start);
    for _ in 0..1000 {
        world.tick(false);
        if world.machine.waiting() == client::Waiting::Next {
            break;
        }
    }
    assert_eq!(world.terminals(), 0);
    world.transport_failed();
    world.assert_once();
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Failed { failure: Failure::Unavailable, .. })));
    world.settle();
    world.assert_once();
}

#[test]
fn lower_failure_with_a_buffered_provider_terminal_closes_without_another_next() {
    let documents = [
        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_0","type":"reasoning"}}"#,
        r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"msg_1","type":"message"}}"#,
        r#"{"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"partial"}]}}"#,
        r#"{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":1,"output_tokens":1}}}"#,
        r#"{"type":"trailing.unread.event"}"#,
    ];
    let mut world = World::new(
        call(1),
        limits(),
        response(200, "Content-Type: text/event-stream\r\n", &events(&documents), false),
        9,
    );
    world.request(client::Request::Start);
    world.request(client::Request::Next);
    for _ in 0..1000 {
        world.tick(false);
        if world.machine.waiting() == client::Waiting::Next {
            break;
        }
    }
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Block { .. })));
    assert_eq!(world.terminals(), 0, "the final completion is buffered behind the demanded block");
    world.transport_failed();
    assert!(
        world.seen.iter().any(|e| matches!(e, client::Event::Close)),
        "a dead stream closes without another data demand: {:?}",
        world.seen
    );
    assert!(
        world.seen.iter().any(|e| matches!(e, client::Event::Failed { failure: Failure::Unavailable, .. })),
        "transport failure is terminal without another Next"
    );
    world.settle();
    world.assert_once();
}

#[test]
fn lower_eof_with_a_buffered_provider_terminal_closes_without_another_next() {
    let documents = [
        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_0","type":"reasoning"}}"#,
        r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"msg_1","type":"message"}}"#,
        r#"{"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"partial"}]}}"#,
        r#"{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":1,"output_tokens":1}}}"#,
        r#"{"type":"trailing.unread.event"}"#,
    ];
    let mut world = World::new(
        call(1),
        limits(),
        response(200, "Content-Type: text/event-stream\r\n", &events(&documents), false),
        9,
    );
    world.request(client::Request::Start);
    world.request(client::Request::Next);
    for _ in 0..1000 {
        world.tick(false);
        if world.machine.waiting() == client::Waiting::Next {
            break;
        }
    }
    assert!(world.seen.iter().any(|e| matches!(e, client::Event::Block { .. })));
    assert_eq!(world.terminals(), 0, "the final completion is buffered behind the demanded block");
    world.eof();
    assert!(
        world.seen.iter().any(|e| matches!(e, client::Event::Close)),
        "a truncated stream closes without another data demand: {:?}",
        world.seen
    );
    assert!(
        world.seen.iter().any(|e| matches!(e, client::Event::Failed { failure: Failure::Protocol, .. })),
        "truncation is terminal without another Next"
    );
    world.settle();
    world.assert_once();
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "focused controls inspect failures and their selected caps")]
fn non_json_errors_keep_their_status_and_exact_error_body_bound() {
    for status in [401, 429, 503] {
        let body = b"upstream failed";
        for bound in [body.len(), body.len() - 1] {
            let mut bounds = limits();
            bounds.error_bytes = u32::try_from(bound).unwrap();
            let mut world = World::new(call(9), bounds, response(status, "", body, false), 9);
            world.fragmentation(1, 1);
            world.request(client::Request::Start);
            world.run();
            world.settle();
            world.assert_once();
            let failed: Vec<_> = world
                .seen
                .iter()
                .filter_map(|event| match event {
                    client::Event::Failed { failure, evidence, detail, .. } => Some((*failure, *evidence, detail)),
                    _ => None,
                })
                .collect();
            let [(failure, evidence, detail)] = failed.as_slice() else { panic!("one failed terminal") };
            assert_eq!(*evidence, client::Evidence::Response { status });
            let expected = match status {
                401 => Failure::Unauthorized,
                429 => Failure::RateLimited { retry_after: Duration::ZERO },
                503 => Failure::Unavailable,
                _ => unreachable!(),
            };
            assert_eq!(*failure, expected);
            if bound < body.len() {
                let expected_detail = [b"error body cut: ".as_slice(), &body[..bound]].concat();
                assert_eq!(detail.as_ref(), expected_detail);
            } else {
                assert_eq!(detail.as_ref(), format!("provider HTTP error {status}").as_bytes());
            }
        }
    }
}

#[test]
fn an_oversized_error_body_keeps_unparsed_detail_and_closes_without_draining() {
    let json = br#"{"error":{"code":"usage_limit_reached","message":"unparsed prefix"}}"#;
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        for (status, expected) in
            [(503, Failure::Unavailable), (429, Failure::RateLimited { retry_after: Duration::from_secs(4) })]
        {
            for chunked in [false, true] {
                let mut bounds = skein_llm_world::limits_for(provider);
                bounds.error_bytes = u32::try_from(json.len()).unwrap();
                bounds.detail_bytes = 48;
                let mut body = json.to_vec();
                body.extend_from_slice(&[b' '; 2048]);
                let wire = response(status, "Retry-After: 4\r\n", &body, chunked);
                let wire_length = wire.len();
                let mut input = call(11);
                if provider == skein_llm::Provider::Anthropic {
                    input.endpoint = skein_llm::Endpoint::anthropic();
                    input.credential = skein_llm::Credential::anthropic(b"fixture".as_slice().into());
                    input.prompt.affinity = None;
                }
                let mut world = World::new(input, bounds, wire, 11);
                world.fragmentation(1, 1);
                world.request(client::Request::Start);
                world.run();
                world.settle();
                world.assert_once();
                assert!(world.source_at < wire_length, "the remaining page is not drained");
                assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure, evidence: client::Evidence::Response { status: observed }, detail, .. } if *failure == expected && *observed == status && detail.len() == 48 && detail.starts_with(b"error body cut: {\"error\":{\"code\":\"usage_limit"))));
                assert!(world.seen.iter().any(|event| matches!(event, client::Event::Close)));
                assert!(!world.seen.iter().any(|event| matches!(event, client::Event::Reusable)));
            }
        }
    }
}

#[test]
fn a_zero_error_body_policy_is_still_named_at_admission() {
    let mut bounds = limits();
    bounds.error_bytes = 0;
    assert!(matches!(
        client::Client::prepare(call(11), &bounds),
        Err(Error::Limit { which: skein_llm::Cap::ErrorBody, bound: 0 })
    ));
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "focused controls inspect failures and their selected caps")]
fn receiving_caps_admit_the_edge_and_refuse_one_over_by_name() {
    use skein_llm::{Cap, Json};
    let documents = [skein_llm_world::TEXT_ADDED, skein_llm_world::TEXT_DELTA, skein_llm_world::TEXT_DONE, TERMINAL];
    let wire = text_response(false);
    let head = u32::try_from(wire.windows(4).position(|part| part == b"\r\n\r\n").unwrap() + 4).unwrap();
    let document = u32::try_from(documents.iter().map(|document| document.len()).max().unwrap()).unwrap();
    let retained = values_text_edge(&documents);
    let values: Vec<_> = documents
        .iter()
        .map(|text| Json::from_bytes(text.as_bytes(), &(limits().native()).document()).unwrap())
        .collect();
    let tokens = values.iter().map(|value| value.document().len()).max().unwrap();
    let string = values
        .iter()
        .flat_map(|value| {
            let document = value.document();
            (0..document.len()).filter_map(move |index| {
                let record = document.token(index).unwrap();
                match record.kind {
                    skein_json::Kind::Key | skein_json::Kind::String => Some(record.len),
                    _ => None,
                }
            })
        })
        .max()
        .unwrap();
    let opaque = u32::try_from(br#"{"id":"msg_1","phase":"final_answer"}"#.len()).unwrap();
    for (which, edge) in [
        (Cap::ResponseHead, head),
        (Cap::Retained, retained),
        (Cap::Tokens, tokens),
        (Cap::Strings, string),
        (Cap::Depth, 4),
        (Cap::Skip, document + 8),
        (Cap::OutputItems, 1),
        (Cap::Answer, 31),
        (Cap::Metadata, opaque),
    ] {
        for bound in [edge, edge - 1] {
            let mut bounds = limits();
            match which {
                Cap::ResponseHead => bounds.http.head = bound,
                Cap::Retained => bounds.retained = bound,
                Cap::Tokens => bounds.tokens = bound,
                Cap::Strings => bounds.strings = bound,
                Cap::Depth => bounds.depth = bound,
                Cap::Skip => bounds.skip = bound,
                Cap::OutputItems => {
                    if bound == 0 {
                        continue;
                    }
                    bounds.output_items = bound;
                }
                Cap::Answer => bounds.answer = bound,
                Cap::Metadata => bounds.metadata = bound,
                _ => unreachable!(),
            }
            let mut input = call(10);
            input.prompt.affinity = None;
            input.prompt.messages = Box::new([]);
            let mut world = World::new(input, bounds, wire.clone(), 10);
            world.fragmentation(1, 1);
            world.request(client::Request::Start);
            world.run();
            world.settle();
            world.assert_once();
            if bound == edge {
                assert!(
                    world.seen.iter().any(|event| matches!(event, client::Event::Completed { .. })),
                    "{which:?} {bound}: {:?}",
                    world.seen
                );
            } else {
                assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: Failure::Limit { which: actual, bound: actual_bound }, .. } if *actual == which && *actual_bound == u64::from(bound))), "{which:?} {bound}: {:?}", world.seen);
            }
        }
    }
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "focused controls inspect failures and their selected caps")]
fn header_fields_and_sse_fields_keep_their_own_exact_bounds() {
    use skein_llm::Cap;
    let document = TERMINAL;
    for (which, edge) in [(Cap::ResponseFields, 8), (Cap::Field, 7)] {
        for bound in [edge, edge - 1] {
            let mut bounds = limits();
            let wire = match which {
                Cap::ResponseFields => {
                    bounds.http.headers = bound;
                    response(
                        200,
                        "Content-Type: text/event-stream\r\nA: a\r\nB: b\r\nC: c\r\nD: d\r\nE: e\r\nF: f\r\n",
                        &events(&[document]),
                        false,
                    )
                }
                Cap::Field => {
                    bounds.sse.field = bound;
                    response(
                        200,
                        "Content-Type: text/event-stream\r\n",
                        format!("event: example\ndata: {document}\n\n").as_bytes(),
                        false,
                    )
                }
                _ => unreachable!(),
            };
            let mut input = call(11);
            input.prompt.affinity = None;
            let mut world = World::new(input, bounds, wire, 11);
            world.fragmentation(1, 1);
            world.request(client::Request::Start);
            world.run();
            world.settle();
            world.assert_once();
            if bound == edge {
                assert!(
                    world.seen.iter().any(|event| matches!(event, client::Event::Completed { .. })),
                    "{which:?}: {:?}",
                    world.seen
                );
            } else {
                assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: Failure::Limit { which: actual, bound: actual_bound }, .. } if *actual == which && *actual_bound == u64::from(bound))), "{which:?}: {:?}", world.seen);
            }
        }
    }
}

#[test]
fn receiving_token_limits_do_not_change_an_error_body_class() {
    let mut bounds = limits();
    bounds.tokens = 2;
    let wire = response(
        401,
        "Content-Type: application/json\r\n",
        br#"{"error":{"code":"invalid_api_key","message":"denied"}}"#,
        false,
    );
    let mut world = World::new(call(12), bounds, wire, 12);
    world.request(client::Request::Start);
    world.run();
    world.settle();
    world.assert_once();
    assert!(world.seen.iter().any(|event| matches!(
        event,
        client::Event::Failed {
            failure: Failure::Unauthorized,
            evidence: client::Evidence::Response { status: 401 },
            ..
        }
    )));
}

#[test]
fn inconsistent_usage_on_the_wire_preserves_text_and_completed_terminal() {
    for cache_parts in [r#"{"cached_tokens":3}"#, r#"{"cached_tokens":2,"cache_write_tokens":2}"#] {
        let terminal = format!(
            r#"{{"type":"response.completed","response":{{"status":"completed","usage":{{"input_tokens":1,"input_tokens_details":{cache_parts},"output_tokens":7,"output_tokens_details":{{"reasoning_tokens":2}}}}}}}}"#
        );
        let documents = [
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"m","type":"message"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"m","type":"message","content":[{"type":"output_text","text":"kept"}]}}"#,
            terminal.as_str(),
        ];
        let mut world = World::new(
            call(1),
            limits(),
            response(200, "Content-Type: text/event-stream\r\n", &events(&documents), false),
            9,
        );
        world.fragmentation(1, 3);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        let completion = world
            .seen
            .iter()
            .find_map(|event| match event {
                client::Event::Completed { completion, .. } => Some(completion),
                client::Event::Delta { .. }
                | client::Event::Block { .. }
                | client::Event::Failed { .. }
                | client::Event::Cancelled { .. }
                | client::Event::Reusable
                | client::Event::Close
                | client::Event::Closed => None,
            })
            .expect("usage cannot fail a completion");
        assert_eq!(completion.usage.input, None);
        assert!(completion.usage.cache_read.is_some());
        assert_eq!(completion.usage.output, Some(7));
        assert_eq!(completion.usage.reasoning, Some(2));
        assert!(matches!(&*completion.content, [Block::Text { text, .. }] if text.as_ref() == b"kept"));
    }
}

#[test]
fn owner_timeouts_keep_every_phase_and_clip_the_detail_without_repeating_a_terminal() {
    for (phase, expected) in [
        (skein_llm::Phase::Connect, b"timed out connecting the socket".as_slice()),
        (skein_llm::Phase::Handshake, b"timed out completing the TLS handshake".as_slice()),
        (skein_llm::Phase::Head, b"timed out waiting for the response head".as_slice()),
        (skein_llm::Phase::Idle, b"timed out waiting for a response event".as_slice()),
        (skein_llm::Phase::Whole, b"timed out waiting for the whole call".as_slice()),
    ] {
        for bound in [7, 128] {
            let mut caps = limits();
            caps.detail_bytes = bound;
            let mut world = World::new(call(1), caps, text_response(false), 41);
            let failure = Failure::TimedOut { phase };
            world.abort(failure);
            world.assert_once();
            let [client::Event::Failed { failure: got, evidence, detail, .. }, ..] = world.seen.as_slice() else {
                panic!("one owner timeout: {:?}", world.seen);
            };
            assert_eq!(*got, failure);
            assert_eq!(*evidence, client::Evidence::Unsent);
            assert_eq!(detail.as_ref(), &expected[..expected.len().min(usize::try_from(bound).unwrap())]);
            world.abort(failure);
            world.settle();
            world.assert_once();
        }
    }
}

fn values_text_edge(documents: &[&str]) -> u32 {
    documents
        .iter()
        .map(|text| {
            let value = skein_llm::Json::from_bytes(text.as_bytes(), &(limits().native()).document())
                .expect("handwritten event");
            value.document().text_len()
        })
        .max()
        .expect("nonempty event sequence")
}
