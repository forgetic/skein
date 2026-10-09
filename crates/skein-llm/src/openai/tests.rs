#![expect(clippy::match_like_matches_macro, reason = "workspace bans matches so tests use explicit match assertions")]
#![expect(clippy::wildcard_enum_match_arm, reason = "ordinary tests make focused partial-variant assertions")]
//! Synthetic scenarios and separately identified archived provider traffic.
#![expect(clippy::disallowed_types, reason = "ordinary test code collects provider outputs in Vec")]
#![expect(clippy::disallowed_methods, reason = "tests inspect fixture text and collect outputs")]
#![expect(clippy::arithmetic_side_effects, reason = "the test's trusted archive cursor uses ordinary arithmetic")]
use crate::openai::{
    DecodeError, Event, Failure, Input, Item, Json, Limits, Output, Part, ProviderError, RateLimit, Request, Role,
    Stop, StreamDecoder, Tool, Usage, classify, decode_error, decode_event, decode_request, encode_error, encode_event,
    encode_request, json, worst_case,
};
use alloc::boxed::Box;
use alloc::vec::Vec;
use skein_json::Token;
use skein_lib::{Duration, Queue, Wall, bytes};

const LIMITS: Limits = Limits {
    request_bytes: 65536,
    document_bytes: 65536,
    string_bytes: 16384,
    depth: 32,
    tokens: 4096,
    parts: 32,
    input_bytes: 8192,
    opaque_bytes: 8192,
    answer_bytes: 32768,
    detail_bytes: 256,
};
fn owned(b: &[u8]) -> Box<[u8]> {
    bytes::copy_of(b)
}
fn value(b: &[u8]) -> Json {
    Json::from_bytes(b, &LIMITS).unwrap()
}
fn request() -> Request {
    Request {
        model: owned(b"gpt-test"),
        instructions: owned(b"line\n\"quoted\""),
        tools: Box::new([Tool {
            name: owned(b"read"),
            description: owned(b"Read a file"),
            schema: value(br#"{"type":"object","properties":{"path":{"type":"string"}}}"#),
        }]),
        input: Box::new([
            Input::Message { role: Role::User, text: owned(b"hello"), id: None, phase: None, refusal: false },
            Input::Opaque {
                value: value(br#"{"type":"reasoning","id":"r","encrypted_content":"ciphertext","summary":[]}"#),
            },
            Input::Message {
                role: Role::Assistant,
                text: owned(b"reply"),
                id: Some(owned(b"message")),
                phase: Some(owned(b"commentary")),
                refusal: false,
            },
            Input::FunctionCall {
                call_id: owned(b"call"),
                item_id: Some(owned(b"item")),
                name: owned(b"read"),
                arguments: owned(br#"{"path":"a"}"#),
            },
            Input::FunctionOutput { call_id: owned(b"call"), output: owned(b"ok") },
        ]),
        effort: Some(owned(b"high")),
        prompt_cache_key: Some(owned(b"conversation")),
        choice: crate::ToolChoice::Auto,
    }
}
fn drain(queue: &mut Queue<Output>, trace: &mut Vec<Output>) {
    while let Some(output) = queue.pop() {
        trace.push(output);
    }
}
fn stream(events: &[Event], limits: &Limits) -> Vec<Output> {
    let mut decoder = StreamDecoder::new(limits);
    let mut out = Queue::with_capacity(crate::openai::MAX_OUT);
    let mut trace = Vec::new();
    for event in events {
        decoder.event(event.clone(), limits, Wall::EPOCH, &mut out);
        drain(&mut out, &mut trace);
        while decoder.has_ready() {
            decoder.ready(&mut out);
            drain(&mut out, &mut trace);
        }
    }
    decoder.end(&mut out);
    drain(&mut out, &mut trace);
    trace
}
fn fixture(scenario: &str, file: &str) -> Vec<u8> {
    let root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    std::fs::read(std::path::Path::new(&root).join("src/openai/fixtures").join(scenario).join(file)).unwrap()
}
fn dechunk(input: &[u8]) -> Vec<u8> {
    let mut at: usize = 0;
    let mut out = Vec::new();
    while at < input.len() {
        let end = bytes::find(&input[at..], b"\r\n").unwrap();
        let size = usize::from_str_radix(core::str::from_utf8(&input[at..at + end]).unwrap(), 16).unwrap();
        at += end + 2;
        if size == 0 {
            assert_eq!(&input[at..], b"\r\n");
            return out;
        }
        out.extend_from_slice(&input[at..at + size]);
        at += size;
        assert_eq!(&input[at..at + 2], b"\r\n");
        at += 2;
    }
    panic!("archive misses terminal chunk")
}

#[test]
fn measured_request_replays_all_input_kinds_and_bounded_raw_arguments() {
    let request = request();
    let body = encode_request(&request, &LIMITS).unwrap();
    let document = Json::from_bytes(&body, &LIMITS).unwrap();
    assert_eq!(decode_request(&document, &LIMITS).unwrap(), request);
    assert_eq!(body.as_ref(), document.to_bytes(&LIMITS).unwrap().as_ref());
    assert!(bytes::find(&body, b"max_output_tokens").is_none());
    assert_eq!(
        encode_request(&request, &Limits { request_bytes: u32::try_from(body.len()).unwrap() - 1, ..LIMITS }),
        Err(DecodeError::limit(crate::Cap::Request, u32::try_from(body.len()).unwrap() - 1))
    );
    for (arguments, encoded) in [
        (b"[]".as_slice(), br#""arguments":"[]""#.as_slice()),
        (b"{broken\n\"\\".as_slice(), br#""arguments":"{broken\n\"\\""#.as_slice()),
    ] {
        let mut raw = request.clone();
        raw.tools = Box::new([]);
        raw.input = Box::new([Input::FunctionCall {
            call_id: owned(b"call"),
            item_id: Some(owned(b"item")),
            name: owned(b"read"),
            arguments: owned(arguments),
        }]);
        let bounded = Limits { string_bytes: u32::try_from(arguments.len()).unwrap(), ..LIMITS };
        let body = encode_request(&raw, &bounded).unwrap();
        assert!(bytes::find(&body, encoded).is_some(), "exact native argument string, without inner parsing");
        let document = Json::from_bytes(&body, &LIMITS).unwrap();
        assert_eq!(decode_request(&document, &bounded).unwrap(), raw);
        assert_eq!(
            encode_request(&raw, &Limits { string_bytes: bounded.string_bytes - 1, ..LIMITS }),
            Err(DecodeError::limit(crate::Cap::String, bounded.string_bytes - 1))
        );
    }
    let mut invalid = request.clone();
    invalid.input = Box::new([Input::FunctionCall {
        call_id: owned(b"call"),
        item_id: Some(owned(b"item")),
        name: owned(b"read"),
        arguments: owned(&[0xff]),
    }]);
    assert_eq!(encode_request(&invalid, &LIMITS), Err(DecodeError::Malformed));
    invalid = request;
    invalid.model = owned(&[0xff]);
    assert_eq!(encode_request(&invalid, &LIMITS), Err(DecodeError::Malformed));
}

#[test]
fn function_call_without_provider_item_id_omits_the_optional_field() {
    let mut request = request();
    request.input = Box::new([Input::FunctionCall {
        call_id: owned(b"call"),
        item_id: None,
        name: owned(b"read"),
        arguments: owned(br#"{"path":"a"}"#),
    }]);
    let body = encode_request(&request, &LIMITS).unwrap();
    assert!(bytes::find(&body, br#""id":"#).is_none());
    let document = Json::from_bytes(&body, &LIMITS).unwrap();
    assert_eq!(decode_request(&document, &LIMITS).unwrap(), request);
}

#[test]
fn synthetic_parallel_tools_reasoning_and_message_heads_keep_order() {
    let reasoning = value(br#"{"type":"reasoning","id":"r","encrypted_content":"opaque","summary":[]}"#);
    let events = [
        Event::Created { echo: Some(request()) },
        Event::Added { index: 0, id: owned(b"r"), kind: owned(b"reasoning") },
        Event::Done { index: 0, item: Item::Opaque { value: reasoning.clone() } },
        Event::Added { index: 1, id: owned(b"m"), kind: owned(b"message") },
        Event::Done {
            index: 1,
            item: Item::Message {
                id: owned(b"m"),
                phase: Some(owned(b"commentary")),
                text: owned(b"hello"),
                refusal: false,
            },
        },
        Event::Added { index: 2, id: owned(b"f1"), kind: owned(b"function_call") },
        Event::Added { index: 3, id: owned(b"f2"), kind: owned(b"function_call") },
        Event::Done {
            index: 2,
            item: Item::FunctionCall {
                id: owned(b"f1"),
                call_id: owned(b"c1"),
                name: owned(b"read"),
                arguments: owned(br#"{"path":"a"}"#),
            },
        },
        Event::Done {
            index: 3,
            item: Item::FunctionCall {
                id: owned(b"f2"),
                call_id: owned(b"c2"),
                name: owned(b"read"),
                arguments: owned(br#"{"path":"b"}"#),
            },
        },
        Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO },
    ];
    let trace = stream(&events, &LIMITS);
    let parts: Vec<&Part> = trace
        .iter()
        .filter_map(|output| match output {
            Output::Part(part) => Some(part),
            Output::Completed { .. }
            | Output::Failed { .. }
            | Output::Progress
            | Output::TextDelta { .. }
            | Output::ArgumentsDelta { .. }
            | Output::ReasoningDelta { .. } => None,
        })
        .collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], &Part::Opaque { bytes: reasoning.to_bytes(&LIMITS).unwrap() });
    assert_eq!(
        parts[1],
        &Part::Text { id: owned(b"m"), phase: Some(owned(b"commentary")), text: owned(b"hello"), refusal: false }
    );
    assert!(match parts[2] {
        Part::ToolCall { call_id, item_id, .. } if call_id.as_ref() == b"c1" && item_id.as_ref() == b"f1" => true,
        _ => false,
    });
    assert!(match parts[3] {
        Part::ToolCall { call_id, item_id, .. } if call_id.as_ref() == b"c2" && item_id.as_ref() == b"f2" => true,
        _ => false,
    });
    assert!(match trace.last() {
        Some(Output::Completed { stop: Stop::ToolUse, .. }) => true,
        _ => false,
    });
}

#[test]
fn input_cap_discards_input_and_answer_limit_emits_one_failed_terminal() {
    let events = [
        Event::Added { index: 0, id: owned(b"f"), kind: owned(b"function_call") },
        Event::Done {
            index: 0,
            item: Item::FunctionCall {
                id: owned(b"f"),
                call_id: owned(b"c"),
                name: owned(b"read"),
                arguments: owned(br#"{"path":"a"}"#),
            },
        },
        Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO },
    ];
    let trace = stream(&events, &Limits { input_bytes: 3, ..LIMITS });
    assert!(trace.iter().any(|out| match out {
        Output::Part(Part::ToolCall { input, too_large: true, bytes: 12, cut: false, .. }) if input.is_empty() => true,
        _ => false,
    }));
    assert!(match trace.last() {
        Some(Output::Completed { stop: Stop::ToolUse, .. }) => true,
        _ => false,
    });
    let events = [
        Event::Added { index: 0, id: owned(b"m"), kind: owned(b"message") },
        Event::Done {
            index: 0,
            item: Item::Message { id: owned(b"m"), phase: None, text: owned(b"hello"), refusal: false },
        },
        Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO },
    ];
    let trace = stream(&events, &Limits { answer_bytes: 4, ..LIMITS });
    assert!(match trace.last() {
        Some(Output::Failed { failure: Failure::Limit { .. }, .. }) => true,
        _ => false,
    });
    assert_eq!(
        trace
            .iter()
            .filter(|out| match out {
                Output::Failed { .. } => true,
                _ => false,
            })
            .count(),
        1
    );
    assert!(!trace.iter().any(|out| match out {
        Output::Part(_) => true,
        _ => false,
    }));
}

#[test]
fn mismatched_indices_ids_kinds_and_early_end_fail_once() {
    let added = Event::Added { index: 0, id: owned(b"m"), kind: owned(b"message") };
    let scenarios = [
        Vec::from([Event::Added { index: 1, id: owned(b"m"), kind: owned(b"message") }]),
        Vec::from([
            added.clone(),
            Event::Done {
                index: 0,
                item: Item::Message { id: owned(b"other"), phase: None, text: owned(b"hello"), refusal: false },
            },
        ]),
        Vec::from([
            added.clone(),
            Event::Done {
                index: 0,
                item: Item::FunctionCall {
                    id: owned(b"m"),
                    call_id: owned(b"c"),
                    name: owned(b"read"),
                    arguments: owned(b"{}"),
                },
            },
        ]),
        Vec::from([added, Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO }]),
        Vec::from([Event::Progress]),
    ];
    for events in scenarios {
        let trace = stream(&events, &LIMITS);
        assert_eq!(
            trace
                .iter()
                .filter(|out| match out {
                    Output::Failed { .. } => true,
                    _ => false,
                })
                .count(),
            1
        );
        assert_eq!(
            trace
                .iter()
                .filter(|out| match out {
                    Output::Completed { .. } => true,
                    _ => false,
                })
                .count(),
            0
        );
    }
}

#[test]
fn server_events_roundtrip_echoes_skipped_and_errors_keep_reset_priority() {
    let events = [
        Event::Created { echo: None },
        Event::InProgress { echo: None },
        Event::Added { index: 0, id: owned(b"m"), kind: owned(b"message") },
        Event::Done {
            index: 0,
            item: Item::Message {
                id: owned(b"m"),
                phase: Some(owned(b"final_answer")),
                text: owned(b"hello"),
                refusal: false,
            },
        },
        Event::Completed {
            stop: Stop::MaxTokens,
            usage: Usage { input_tokens: 12, output_tokens: 3, cache_read_tokens: 4, cache_write_tokens: 0 },
        },
        Event::Progress,
        Event::Unknown,
    ];
    for event in events {
        let encoded = encode_event(&event, &LIMITS).unwrap();
        assert_eq!(decode_event(&value(&encoded), &LIMITS).unwrap(), event);
    }
    let error = ProviderError {
        kind: owned(b"usage_limit_reached"),
        message: owned(b"spent"),
        resets_in_seconds: Some(60),
        resets_at: Some(900),
    };
    let encoded = encode_error(&error, &LIMITS).unwrap();
    assert_eq!(decode_error(&value(&encoded), &LIMITS).unwrap(), error);
    let wall = Wall::from_nanos(100_000_000_000);
    assert_eq!(
        classify(429, Some(&error), RateLimit::NONE, wall),
        Failure::Exhausted { retry_after: Duration::from_secs(60) }
    );
    let mut rate = RateLimit::NONE;
    rate.observe(b"Retry-After", b"2");
    assert_eq!(classify(429, Some(&error), rate, wall), Failure::Exhausted { retry_after: Duration::from_secs(2) });
    let echo = encode_event(&Event::Created { echo: Some(request()) }, &LIMITS).unwrap();
    assert!(bytes::find(&echo, b"instructions").is_some());
    assert!(bytes::find(&echo, b"parameters").is_some());
    assert_eq!(decode_event(&value(&echo), &LIMITS), Ok(Event::Created { echo: None }));
}

#[test]
fn completed_usage_rejects_cached_over_total_and_done_honors_incomplete_status() {
    let valid = value(br#"{"type":"response.done","response":{"status":"incomplete","incomplete_details":{"reason":"content_filter"},"usage":{"input_tokens":8,"input_tokens_details":{"cached_tokens":3},"output_tokens":2},"output":[{"ignored":true}]}}"#);
    assert_eq!(
        decode_event(&valid, &LIMITS),
        Ok(Event::Completed {
            stop: Stop::Refusal,
            usage: Usage { input_tokens: 5, output_tokens: 2, cache_read_tokens: 3, cache_write_tokens: 0 }
        })
    );
    let invalid = value(br#"{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":2,"input_tokens_details":{"cached_tokens":3},"output_tokens":2}}}"#);
    assert_eq!(decode_event(&invalid, &LIMITS), Err(DecodeError::Malformed));
    assert_eq!(
        decode_event(
            &value(br#"{"type":"response.completed","response":{"status":"in_progress","usage":null}}"#),
            &LIMITS
        ),
        Err(DecodeError::Malformed)
    );
    let error = decode_error(
        &value(br#"{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"too long"}}"#),
        &LIMITS,
    )
    .unwrap();
    assert_eq!(classify(400, Some(&error), RateLimit::NONE, Wall::EPOCH), Failure::ContextTooLong);
}

#[test]
fn malformed_tokens_duplicates_and_every_document_limit_are_refused() {
    for bytes in [b"{]".as_slice(), b"[1,]", b"{}{}", b"\"\\ud800\"", b"{\"a\":}"] {
        assert_eq!(Json::from_bytes(bytes, &LIMITS).unwrap_err(), DecodeError::Malformed);
    }
    let tokens = [Token::ObjectStart, Token::Key(owned(b"a")), Token::ObjectEnd];
    assert_eq!(Json::from_tokens(&tokens, &LIMITS), Err(DecodeError::Malformed));
    assert_eq!(
        Json::from_bytes(b"{}", &Limits { tokens: 1, ..LIMITS }),
        Err(DecodeError::limit(crate::Cap::Tokens, 1))
    );
    assert_eq!(
        Json::from_bytes(br#"{"deep":[{}]}"#, &Limits { depth: 2, ..LIMITS }).unwrap_err(),
        DecodeError::limit(crate::Cap::Depth, 2)
    );
    assert_eq!(
        Json::from_bytes(b"\"long\"", &Limits { string_bytes: 3, ..LIMITS }).unwrap_err(),
        DecodeError::limit(crate::Cap::String, 3)
    );
    assert_eq!(
        decode_event(&value(br#"{"type":"response.created","type":"error"}"#), &LIMITS),
        Err(DecodeError::Malformed)
    );
    assert!(worst_case(&LIMITS).unwrap() > u64::from(LIMITS.answer_bytes));
    let oversized = Limits { parts: u32::MAX, ..LIMITS };
    assert!(
        worst_case(&oversized).unwrap() > worst_case(&LIMITS).unwrap(),
        "large finite bounds must be reported to the caller"
    );
    assert_eq!(worst_case(&Limits { parts: u32::MAX, tokens: u32::MAX, ..LIMITS }), None);
}

fn assert_archive_deltas(trace: &[Output]) {
    let mut text_delta = Vec::new();
    let mut completed_text = Vec::new();
    let mut argument_delta = Vec::new();
    let mut completed_argument = Vec::new();
    for output in trace {
        match output {
            Output::TextDelta { text, .. } => text_delta.extend_from_slice(text),
            Output::ArgumentsDelta { delta, .. } => argument_delta.extend_from_slice(delta),
            Output::Part(Part::Text { text, .. }) => completed_text.extend_from_slice(text),
            Output::Part(Part::ToolCall { input, .. }) => completed_argument.extend_from_slice(input),
            _ => {}
        }
    }
    assert_eq!(text_delta, completed_text, "archived text deltas reconstruct the completed message");
    assert_eq!(argument_delta, completed_argument, "archived argument deltas reconstruct the completed call");
}

#[test]
fn archived_real_provider_requests_and_answers_match_known_completions() {
    for (scenario, input, output, tool_count, opaque_count) in [
        ("single-text", 27_u64, 17_u64, 0_usize, 1_usize),
        ("tool-call", 76, 18, 1, 0),
        ("tool-result-final", 116, 12, 0, 0),
    ] {
        let wrapper = Json::from_bytes(&fixture(scenario, "request.json"), &LIMITS).unwrap();
        let body = json::text_ref(
            json::value_at(wrapper.as_tokens(), json::required(wrapper.as_tokens(), b"body").unwrap()).unwrap(),
        )
        .unwrap();
        let request = decode_request(&value(body), &LIMITS).unwrap();
        assert!(!request.input.is_empty());
        let captured = dechunk(&fixture(scenario, "response.sse"));
        let mut decoder = StreamDecoder::new(&LIMITS);
        let mut out = Queue::with_capacity(crate::openai::MAX_OUT);
        let mut trace = Vec::new();
        for line in core::str::from_utf8(&captured).unwrap().lines() {
            if let Some(data) = line.strip_prefix("data: ") {
                decoder.event(decode_event(&value(data.as_bytes()), &LIMITS).unwrap(), &LIMITS, Wall::EPOCH, &mut out);
                drain(&mut out, &mut trace);
            }
        }
        decoder.end(&mut out);
        drain(&mut out, &mut trace);
        assert_archive_deltas(&trace);
        assert_eq!(
            trace
                .iter()
                .filter(|out| match out {
                    Output::Part(Part::ToolCall { .. }) => true,
                    _ => false,
                })
                .count(),
            tool_count
        );
        assert_eq!(
            trace
                .iter()
                .filter(|out| match out {
                    Output::Part(Part::Opaque { .. }) => true,
                    _ => false,
                })
                .count(),
            opaque_count
        );
        assert_eq!(
            trace.last(),
            Some(&Output::Completed {
                stop: if tool_count == 0 { Stop::EndTurn } else { Stop::ToolUse },
                usage: Usage {
                    input_tokens: input,
                    output_tokens: output,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0
                }
            })
        );
        if scenario == "single-text" {
            assert!(trace.iter().any(|out| match out {
                Output::Part(Part::Text { text, .. }) if text.as_ref() == b"hello" => true,
                _ => false,
            }));
            assert!(trace.iter().any(|out| match out {
                Output::Part(Part::Opaque { bytes }) if bytes::find(bytes, b"encrypted_content").is_some() => true,
                _ => false,
            }));
        }
        if scenario == "tool-call" {
            assert!(trace.iter().any(|out| match out {
                Output::Part(Part::ToolCall {
                    name,
                    input,
                    call_id,
                    item_id,
                    too_large: false,
                    bytes: _,
                    cut: false,
                }) if name.as_ref() == b"get_weather"
                    && input.as_ref() == br#"{"city":"Paris"}"#
                    && !call_id.is_empty()
                    && !item_id.is_empty() =>
                    true,
                _ => false,
            }));
        }
    }
}

#[test]
fn out_of_order_done_waits_for_order_and_terminal_waits_for_ready_parts() {
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(crate::openai::MAX_OUT);
    let mut trace = Vec::new();
    for index in 0..3_u32 {
        decoder.event(
            Event::Added {
                index,
                id: owned(&[b'a'.wrapping_add(u8::try_from(index).unwrap())]),
                kind: owned(b"function_call"),
            },
            &LIMITS,
            Wall::EPOCH,
            &mut out,
        );
        drain(&mut out, &mut trace);
    }
    for index in [2_u32, 1, 0] {
        let id = owned(&[b'a'.wrapping_add(u8::try_from(index).unwrap())]);
        decoder.event(
            Event::Done {
                index,
                item: Item::FunctionCall {
                    id,
                    call_id: owned(&[b'x'.wrapping_add(u8::try_from(index).unwrap())]),
                    name: owned(b"read"),
                    arguments: owned(b"{}"),
                },
            },
            &LIMITS,
            Wall::EPOCH,
            &mut out,
        );
        drain(&mut out, &mut trace);
    }
    assert!(decoder.has_ready());
    decoder.event(Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO }, &LIMITS, Wall::EPOCH, &mut out);
    drain(&mut out, &mut trace);
    assert!(decoder.has_ready());
    assert!(!decoder.is_complete());
    decoder.end(&mut out);
    drain(&mut out, &mut trace);
    assert!(decoder.is_complete());
    assert!(!decoder.has_ready());
    let ids: Vec<_> = trace
        .iter()
        .filter_map(|out| match out {
            Output::Part(Part::ToolCall { call_id, item_id, .. }) => Some((call_id.clone(), item_id.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(ids, Vec::from([(owned(b"x"), owned(b"a")), (owned(b"y"), owned(b"b")), (owned(b"z"), owned(b"c"))]));
    assert_eq!(trace.last(), Some(&Output::Completed { stop: Stop::ToolUse, usage: Usage::ZERO }));
}

#[test]
fn incomplete_and_refused_terminals_override_tools_and_absolute_reset_uses_wall() {
    for stop in [Stop::MaxTokens, Stop::Refusal] {
        let events = [
            Event::Added { index: 0, id: owned(b"f"), kind: owned(b"function_call") },
            Event::Done {
                index: 0,
                item: Item::FunctionCall {
                    id: owned(b"f"),
                    call_id: owned(b"c"),
                    name: owned(b"read"),
                    arguments: owned(b"{}"),
                },
            },
            Event::Completed { stop, usage: Usage::ZERO },
        ];
        assert_eq!(stream(&events, &LIMITS).last(), Some(&Output::Completed { stop, usage: Usage::ZERO }));
    }
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(crate::openai::MAX_OUT);
    decoder.event(
        Event::Failed {
            error: ProviderError {
                kind: owned(b"usage_limit_reached"),
                message: owned(b"spent"),
                resets_in_seconds: None,
                resets_at: Some(180),
            },
        },
        &LIMITS,
        Wall::from_nanos(100_000_000_000),
        &mut out,
    );
    assert_eq!(
        out.pop(),
        Some(Output::Failed {
            failure: Failure::Exhausted { retry_after: Duration::from_secs(80) },
            detail: owned(b"spent")
        })
    );
    assert_eq!(
        stream(&[Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO }], &LIMITS),
        Vec::from([Output::Completed { stop: Stop::EndTurn, usage: Usage::ZERO }])
    );
}

#[test]
fn error_detail_truncation_preserves_utf8_boundaries() {
    let limits = Limits { detail_bytes: 3, ..LIMITS };
    let message = value("{\"error\":{\"code\":\"server_error\",\"message\":\"éé\"}}".as_bytes());
    let error = decode_error(&message, &limits).unwrap();
    assert_eq!(error.message.as_ref(), "é".as_bytes());
    assert_eq!(decode_error(&value(&encode_error(&error, &LIMITS).unwrap()), &LIMITS).unwrap(), error);
}

#[test]
fn server_refuses_request_limit_even_when_document_limit_is_larger() {
    let body = encode_request(&request(), &LIMITS).unwrap();
    let document = value(&body);
    assert_eq!(
        decode_request(&document, &Limits { request_bytes: u32::try_from(body.len()).unwrap() - 1, ..LIMITS }),
        Err(DecodeError::limit(crate::Cap::Request, u32::try_from(body.len()).unwrap() - 1))
    );
}

#[test]
fn fake_completion_echo_exercises_large_event_without_keeping_request() {
    let bytes = crate::openai::encode_completion(Stop::EndTurn, Usage::ZERO, &request(), &LIMITS).unwrap();
    assert!(bytes::find(&bytes, b"instructions").is_some());
    assert!(bytes::find(&bytes, b"parameters").is_some());
    assert_eq!(
        decode_event(&value(&bytes), &LIMITS).unwrap(),
        Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO }
    );
}

#[test]
fn deltas_decode_roundtrip_and_forward_before_completed_blocks() {
    let events = [
        Event::Added { index: 0, id: owned(b"m"), kind: owned(b"message") },
        Event::Added { index: 1, id: owned(b"f"), kind: owned(b"function_call") },
        Event::Added { index: 2, id: owned(b"r"), kind: owned(b"reasoning") },
        Event::TextDelta { index: 0, content_index: 0, text: owned(b"hello") },
        Event::ArgumentsDelta { index: 1, delta: owned(b"{\"path\":") },
        Event::ReasoningDelta { index: 2, summary_index: 0, text: owned(b"checking") },
        Event::ArgumentsDelta { index: 1, delta: owned(b"\"a\"}") },
        Event::Done { index: 2, item: Item::Opaque { value: value(br#"{"id":"r","type":"reasoning","summary":[{"type":"summary_text","text":"checking"}],"encrypted_content":"secret"}"#) } },
        Event::Done { index: 1, item: Item::FunctionCall { id: owned(b"f"), call_id: owned(b"c"), name: owned(b"read"), arguments: owned(br#"{"path":"a"}"#) } },
        Event::Done { index: 0, item: Item::Message { id: owned(b"m"), phase: Some(owned(b"final_answer")), text: owned(b"hello"), refusal: false } },
        Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO },
    ];
    for event in &events {
        assert_eq!(decode_event(&value(&encode_event(event, &LIMITS).unwrap()), &LIMITS).unwrap(), *event);
    }
    let trace = stream(&events, &LIMITS);
    assert_eq!(trace[3], Output::TextDelta { index: 0, content_index: 0, text: owned(b"hello") });
    assert_eq!(trace[4], Output::ArgumentsDelta { index: 1, delta: owned(b"{\"path\":") });
    assert_eq!(trace[5], Output::ReasoningDelta { index: 2, summary_index: 0, text: owned(b"checking") });
    let parts: Vec<_> = trace
        .iter()
        .filter_map(|out| match out {
            Output::Part(part) => Some(part),
            _ => None,
        })
        .collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(
        parts[0],
        &Part::Text { id: owned(b"m"), phase: Some(owned(b"final_answer")), text: owned(b"hello"), refusal: false }
    );
    assert!(match parts[1] {
        Part::ToolCall { call_id, item_id, .. } => call_id.as_ref() == b"c" && item_id.as_ref() == b"f",
        _ => false,
    });
    assert!(match parts[2] {
        Part::Opaque { .. } => true,
        _ => false,
    });
    assert_eq!(trace.last(), Some(&Output::Completed { stop: Stop::ToolUse, usage: Usage::ZERO }));
}

#[test]
fn delta_references_and_cumulative_size_are_checked_and_failure_is_terminal() {
    for event in [
        Event::TextDelta { index: 1, content_index: 0, text: owned(b"a") },
        Event::ArgumentsDelta { index: 0, delta: owned(b"a") },
        Event::ReasoningDelta { index: 0, summary_index: 0, text: owned(b"a") },
    ] {
        let trace = stream(&[Event::Added { index: 0, id: owned(b"m"), kind: owned(b"message") }, event], &LIMITS);
        assert!(match trace.last() {
            Some(Output::Failed { failure: Failure::Protocol, .. }) => true,
            _ => false,
        });
    }
    let trace = stream(
        &[
            Event::Added { index: 0, id: owned(b"m"), kind: owned(b"message") },
            Event::TextDelta { index: 0, content_index: 0, text: owned(b"ab") },
            Event::TextDelta { index: 0, content_index: 0, text: owned(b"cd") },
            Event::TextDelta { index: 0, content_index: 0, text: owned(b"ignored") },
            Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO },
        ],
        &Limits { answer_bytes: 3, ..LIMITS },
    );
    assert_eq!(trace.len(), 3);
    assert_eq!(trace[1], Output::TextDelta { index: 0, content_index: 0, text: owned(b"ab") });
    assert!(match trace[2] {
        Output::Failed { failure: Failure::Limit { .. }, .. } => true,
        _ => false,
    });
    for bytes in [
        br#"{"type":"response.output_text.delta","output_index":0,"content_index":0}"#.as_slice(),
        br#"{"type":"response.function_call_arguments.delta","output_index":0,"delta":null}"#,
        br#"{"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":-1,"delta":"x"}"#,
    ] {
        let _error = decode_event(&value(bytes), &LIMITS).unwrap_err();
    }
}

#[test]
fn refusal_replays_with_provider_content_type_and_user_refusal_is_rejected() {
    let mut request = request();
    request.input = Box::new([Input::Message {
        role: Role::Assistant,
        text: owned(b"cannot"),
        id: Some(owned(b"m")),
        phase: None,
        refusal: true,
    }]);
    let encoded = encode_request(&request, &LIMITS).unwrap();
    assert!(bytes::find(&encoded, br#""type":"refusal","refusal":"cannot""#).is_some());
    assert_eq!(decode_request(&value(&encoded), &LIMITS).unwrap(), request);
    request.input =
        Box::new([Input::Message { role: Role::User, text: owned(b"cannot"), id: None, phase: None, refusal: true }]);
    assert_eq!(encode_request(&request, &LIMITS), Err(DecodeError::WrongType));
}

#[test]
fn zero_capacity_limits_refuse_without_panicking_and_diagnostics_obey_the_cap() {
    assert_eq!(
        encode_request(&request(), &Limits { request_bytes: 0, ..LIMITS }),
        Err(DecodeError::limit(crate::Cap::Request, 0))
    );
    for limits in [
        Limits { document_bytes: 0, ..LIMITS },
        Limits { string_bytes: 0, ..LIMITS },
        Limits { depth: 0, ..LIMITS },
        Limits { tokens: 0, ..LIMITS },
    ] {
        let error = Json::from_bytes(br#"{"a":[]}"#, &limits).unwrap_err();
        assert!(match error {
            DecodeError::TooLarge { bound: 0, .. } => true,
            _ => false,
        });
    }
    let trace = stream(
        &[Event::Added { index: 0, id: owned(b"m"), kind: owned(b"message") }],
        &Limits { parts: 0, detail_bytes: 2, ..LIMITS },
    );
    assert_eq!(
        trace,
        Vec::from([Output::Failed {
            failure: Failure::Limit { which: crate::Cap::Parts, bound: 0 },
            detail: owned(b"co")
        }])
    );
    let trace = stream(&[Event::Progress], &Limits { detail_bytes: 0, ..LIMITS });
    assert_eq!(trace.last(), Some(&Output::Failed { failure: Failure::Protocol, detail: owned(b"") }));
    let limits = Limits {
        request_bytes: 0,
        document_bytes: 0,
        string_bytes: 0,
        depth: 0,
        tokens: 0,
        parts: 0,
        input_bytes: 0,
        opaque_bytes: 0,
        answer_bytes: 0,
        detail_bytes: 0,
    };
    assert!(worst_case(&limits).is_some(), "zero capacities still have a finite scratch bound");
}

#[test]
fn unknown_and_mixed_message_content_are_explicitly_refused() {
    for bytes in [
        br#"{"type":"response.output_item.done","output_index":0,"item":{"id":"m","type":"message","content":[{"type":"output_text","text":"hello"},{"type":"refusal","refusal":"no"}]}}"#.as_slice(),
        br#"{"type":"response.output_item.done","output_index":0,"item":{"id":"m","type":"message","content":[{"type":"output_image","data":"image"}]}}"#,
    ] {
        assert_eq!(decode_event(&value(bytes), &LIMITS), Err(DecodeError::WrongType));
    }
    let message = value(br#"{"stream":true,"store":false,"model":"gpt-test","instructions":"","input":[{"role":"assistant","content":[{"type":"output_text","text":"hello"},{"type":"refusal","refusal":"no"}]}]}"#);
    assert_eq!(decode_request(&message, &LIMITS), Err(DecodeError::WrongType));
}

#[test]
fn separate_opaque_tool_ids_each_obey_string_and_joint_answer_caps() {
    let events = [
        Event::Added { index: 0, id: owned(b"item|id"), kind: owned(b"function_call") },
        Event::Done {
            index: 0,
            item: Item::FunctionCall {
                id: owned(b"item|id"),
                call_id: owned(b"call|id"),
                name: owned(b"read"),
                arguments: owned(b"{}"),
            },
        },
        Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO },
    ];
    let exact = Limits { string_bytes: 13, answer_bytes: 20, ..LIMITS };
    let trace = stream(&events, &exact);
    assert_eq!(
        trace[1],
        Output::Part(Part::ToolCall {
            call_id: owned(b"call|id"),
            item_id: owned(b"item|id"),
            name: owned(b"read"),
            input: owned(b"{}"),
            too_large: false,
            bytes: u64::try_from(owned(b"{}").len()).expect("slice length fits u64"),
            cut: false,
        })
    );
    assert_eq!(trace.last(), Some(&Output::Completed { stop: Stop::ToolUse, usage: Usage::ZERO }));
    let trace = stream(&events, &Limits { answer_bytes: 19, ..exact });
    assert!(match trace.last() {
        Some(Output::Failed { failure: Failure::Limit { .. }, .. }) => true,
        _ => false,
    });
    for item_long in [false, true] {
        let events = [
            Event::Added {
                index: 0,
                id: owned(if item_long { b"item|012345678" } else { b"item|id" }),
                kind: owned(b"function_call"),
            },
            Event::Done {
                index: 0,
                item: Item::FunctionCall {
                    id: owned(if item_long { b"item|012345678" } else { b"item|id" }),
                    call_id: owned(if item_long { b"call|id" } else { b"call|012345678" }),
                    name: owned(b"read"),
                    arguments: owned(b"{}"),
                },
            },
            Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO },
        ];
        assert_eq!(
            stream(&events, &Limits { string_bytes: 14, answer_bytes: 27, ..exact }).last(),
            Some(&Output::Completed { stop: Stop::ToolUse, usage: Usage::ZERO })
        );
        let trace = stream(&events, &Limits { string_bytes: 13, answer_bytes: 27, ..exact });
        assert!(match trace.last() {
            Some(Output::Failed { failure: Failure::Limit { .. }, .. }) => true,
            _ => false,
        });
    }
}

#[test]
fn numerals_keep_the_fixed_tokenizer_cap_distinct_from_strings() {
    let limits = Limits { string_bytes: 64, ..LIMITS };
    Json::from_bytes(&[b'1'; 32], &limits).unwrap();
    assert_eq!(Json::from_bytes(&[b'1'; 33], &limits), Err(DecodeError::limit(crate::Cap::Number, 32)));
}

#[test]
fn tool_choice_encodes_auto_none_and_only_without_filtering() {
    for choice in
        [crate::ToolChoice::Auto, crate::ToolChoice::None, crate::ToolChoice::Only(Box::new([owned(b"read")]))]
    {
        let mut request = request();
        request.choice = choice.clone();
        let wire = encode_request(&request, &LIMITS).expect("bounded choice");
        let value = Json::from_bytes(&wire, &LIMITS).expect("request object");
        let tokens = value.as_tokens();
        let encoded =
            json::text_ref(json::value_at(tokens, json::required(tokens, b"tool_choice").unwrap()).unwrap()).unwrap();
        let expected = match choice {
            crate::ToolChoice::Auto | crate::ToolChoice::Only(_) => b"auto".as_slice(),
            crate::ToolChoice::None => b"none".as_slice(),
        };
        assert_eq!(encoded, expected);
        assert_eq!(
            json::value_at(tokens, json::required(tokens, b"parallel_tool_calls").unwrap()).unwrap(),
            [Token::True]
        );
        let decoded = decode_request(&value, &LIMITS).unwrap();
        assert_eq!(decoded.tools, request.tools);
        assert_eq!(decoded.choice, if expected == b"none" { crate::ToolChoice::None } else { crate::ToolChoice::Auto });
    }
}

#[test]
fn unfinished_native_call_keeps_its_identity_and_arguments_at_the_output_cap() {
    let events = [
        Event::ToolAdded {
            index: 0,
            id: owned(b"i"),
            call_id: owned(b"c"),
            name: owned(b"read"),
            arguments: Box::new([]),
        },
        Event::ArgumentsDelta { index: 0, delta: owned(br#"{"x":"#) },
        Event::Completed { stop: Stop::MaxTokens, usage: Usage::ZERO },
    ];
    for event in &events {
        let wire = encode_event(event, &LIMITS).expect("native event");
        assert_eq!(decode_event(&Json::from_bytes(&wire, &LIMITS).unwrap(), &LIMITS).unwrap(), *event);
    }
    let trace = stream(&events, &LIMITS);
    assert!(trace.iter().any(|output| match output {
        Output::Part(Part::ToolCall { call_id, name, input, cut: true, bytes: 5, too_large: false, .. }) =>
            call_id.as_ref() == b"c" && name.as_ref() == b"read" && input.as_ref() == br#"{"x":"#,
        _ => false,
    }));
    assert_eq!(trace.last(), Some(&Output::Completed { stop: Stop::MaxTokens, usage: Usage::ZERO }));
}

#[test]
fn escaped_argument_text_is_counted_at_the_input_edge_and_one_over() {
    let arguments = br#"{"x":"\u0000\u0000"}"#;
    let events = [
        Event::ToolAdded {
            index: 0,
            id: owned(b"i"),
            call_id: owned(b"c"),
            name: owned(b"read"),
            arguments: Box::new([]),
        },
        Event::Done {
            index: 0,
            item: Item::FunctionCall {
                id: owned(b"i"),
                call_id: owned(b"c"),
                name: owned(b"read"),
                arguments: owned(arguments),
            },
        },
        Event::Completed { stop: Stop::ToolUse, usage: Usage::ZERO },
    ];
    for below in [false, true] {
        let input = u32::try_from(arguments.len()).unwrap() - u32::from(below);
        let limits = Limits { input_bytes: input, ..LIMITS };
        let trace = stream(&events, &limits);
        let part = trace
            .iter()
            .find_map(|output| match output {
                Output::Part(part) => Some(part),
                _ => None,
            })
            .expect("one ordered block");
        match part {
            Part::ToolCall { input, too_large, bytes, .. } => {
                assert_eq!(*too_large, below);
                assert_eq!(*bytes, u64::try_from(arguments.len()).unwrap());
                assert_eq!(input.as_ref(), if below { b"".as_slice() } else { arguments.as_slice() });
            }
            Part::Text { .. } | Part::Opaque { .. } => unreachable!("one function call outcome"),
        }
        assert_eq!(trace.last(), Some(&Output::Completed { stop: Stop::ToolUse, usage: Usage::ZERO }));
    }
}

#[test]
fn a_native_done_item_marked_incomplete_is_a_cut_and_requires_the_output_cap() {
    let events = [
        Event::ToolAdded {
            index: 0,
            id: owned(b"i"),
            call_id: owned(b"c"),
            name: owned(b"read"),
            arguments: Box::new([]),
        },
        Event::Done {
            index: 0,
            item: Item::CutCall {
                id: owned(b"i"),
                call_id: owned(b"c"),
                name: owned(b"read"),
                arguments: owned(b"{broken"),
            },
        },
        Event::Completed { stop: Stop::MaxTokens, usage: Usage::ZERO },
    ];
    let wire = encode_event(&events[1], &LIMITS).expect("incomplete provider item");
    assert_eq!(decode_event(&Json::from_bytes(&wire, &LIMITS).unwrap(), &LIMITS).unwrap(), events[1]);
    let trace = stream(&events, &LIMITS);
    assert!(trace.iter().any(|output| match output {
        Output::Part(Part::ToolCall { input, cut: true, .. }) => input.as_ref() == b"{broken",
        _ => false,
    }));
    assert_eq!(trace.last(), Some(&Output::Completed { stop: Stop::MaxTokens, usage: Usage::ZERO }));
    let mut events = events;
    events[2] = Event::Completed { stop: Stop::EndTurn, usage: Usage::ZERO };
    let trace = stream(&events, &LIMITS);
    assert!(match trace.last() {
        Some(Output::Failed { failure: Failure::Protocol, .. }) => true,
        _ => false,
    });
}
