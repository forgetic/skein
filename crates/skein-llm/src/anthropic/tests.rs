#![expect(clippy::disallowed_macros, reason = "ordinary tests format synthetic provider documents")]
use super::{DecodeError, Event, Failure, Json, Limits, MAX_OUT, Output, Part, Stop, StreamDecoder, Usage};
use alloc::format;
use skein_lib::{Queue, Wall, bytes};

const LIMITS: Limits = Limits {
    request_bytes: 8192,
    document_bytes: 8192,
    string_bytes: 2048,
    depth: 16,
    tokens: 1024,
    parts: 16,
    input_bytes: 2048,
    opaque_bytes: 2048,
    answer_bytes: 4096,
    detail_bytes: 256,
};
const START: &[u8] = br#"{"type":"message_start","message":{"id":"msg","type":"message","role":"assistant","content":[],"usage":{"input_tokens":10,"output_tokens":1,"cache_creation_input_tokens":7,"cache_read_input_tokens":5}}}"#;
const TEXT_START: &[u8] = br#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#;
const TEXT_STOP: &[u8] = br#"{"type":"content_block_stop","index":0}"#;
const END: &[u8] = br#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":9}}"#;
const STOP: &[u8] = br#"{"type":"message_stop"}"#;

fn event(value: &[u8], limits: &Limits) -> Result<Event, DecodeError> {
    super::decode_event(&Json::from_bytes(value, limits)?, limits)
}
fn feed(decoder: &mut StreamDecoder, value: &[u8], limits: &Limits, out: &mut Queue<Output>) {
    let event = event(value, limits).expect("valid bounded event");
    decoder.event(event, limits, Wall::EPOCH, out);
}
fn progress(decoder: &mut StreamDecoder, value: &[u8], limits: &Limits, out: &mut Queue<Output>) {
    feed(decoder, value, limits, out);
    assert_eq!(out.pop(), Some(Output::Progress));
    assert_eq!(out.len(), 0);
}
fn failure(out: &mut Queue<Output>, expected: Failure) {
    match out.pop().expect("terminal error") {
        Output::Failed { failure, .. } => assert_eq!(failure, expected),
        Output::Part(_)
        | Output::TextDelta { .. }
        | Output::ArgumentsDelta { .. }
        | Output::ReasoningDelta { .. }
        | Output::Completed { .. }
        | Output::Progress => unreachable!("the terminal must be a failure"),
    }
    assert_eq!(out.len(), 0);
}

#[test]
fn text_stream_preserves_initial_text_deltas_and_cumulative_usage() {
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &LIMITS, &mut out);
    progress(
        &mut decoder,
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"hello "}}"#,
        &LIMITS,
        &mut out,
    );
    feed(
        &mut decoder,
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"world"}}"#,
        &LIMITS,
        &mut out,
    );
    assert_eq!(out.pop(), Some(Output::TextDelta { index: 0, content_index: 0, text: bytes::copy_of(b"world") }));
    feed(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
    assert_eq!(out.pop(), Some(Output::Part(Part::Text { text: bytes::copy_of(b"hello world") })));
    progress(
        &mut decoder,
        br#"{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":3}}"#,
        &LIMITS,
        &mut out,
    );
    progress(&mut decoder, END, &LIMITS, &mut out);
    progress(
        &mut decoder,
        br#"{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":11,"cache_read_input_tokens":8}}"#,
        &LIMITS,
        &mut out,
    );
    feed(&mut decoder, STOP, &LIMITS, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Completed {
            stop: Stop::EndTurn,
            usage: Usage {
                input: Some(10),
                output: Some(11),
                cache_read: Some(8),
                cache_write: Some(7),
                reasoning: None
            },
        })
    );
    assert!(decoder.is_complete(), "message_stop terminates");
    assert!(!decoder.has_ready(), "Anthropic blocks emit at stop");
    decoder.end(&mut out);
    feed(&mut decoder, START, &LIMITS, &mut out);
    assert_eq!(out.len(), 0);
}

#[test]
fn signed_and_redacted_thinking_preserve_replay_material() {
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &LIMITS, &mut out);
    progress(
        &mut decoder,
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"first ","signature":""}}"#,
        &LIMITS,
        &mut out,
    );
    feed(
        &mut decoder,
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"then"}}"#,
        &LIMITS,
        &mut out,
    );
    assert_eq!(out.pop(), Some(Output::ReasoningDelta { index: 0, summary_index: 0, text: bytes::copy_of(b"then") }));
    for wire in [
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-"}}"#
            .as_slice(),
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque"}}"#
            .as_slice(),
    ] {
        progress(&mut decoder, wire, &LIMITS, &mut out);
    }
    feed(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
    let signed = br#"{"type":"thinking","thinking":"first then","signature":"signed-opaque"}"#;
    assert_eq!(out.pop(), Some(Output::Part(Part::Opaque { bytes: bytes::copy_of(signed) })));
    progress(
        &mut decoder,
        br#"{"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"opaque encrypted"}}"#,
        &LIMITS,
        &mut out,
    );
    feed(&mut decoder, br#"{"type":"content_block_stop","index":1}"#, &LIMITS, &mut out);
    let redacted = br#"{"type":"redacted_thinking","data":"opaque encrypted"}"#;
    assert_eq!(out.pop(), Some(Output::Part(Part::Opaque { bytes: bytes::copy_of(redacted) })));
}

#[test]
fn tool_fragments_replace_start_placeholder_and_keep_malformed_json() {
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &LIMITS, &mut out);
    progress(
        &mut decoder,
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"Read","input":{}}}"#,
        &LIMITS,
        &mut out,
    );
    for (wire, fragment) in [
        (
            br#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{broken"}}"#
                .as_slice(),
            b"{broken".as_slice(),
        ),
        (
            br#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"}"}}"#
                .as_slice(),
            b"}".as_slice(),
        ),
    ] {
        feed(&mut decoder, wire, &LIMITS, &mut out);
        assert_eq!(out.pop(), Some(Output::ArgumentsDelta { index: 0, delta: bytes::copy_of(fragment) }));
    }
    progress(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
    feed(&mut decoder,
        br#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call2","name":"Write","input":{"x":1}}}"#,
        &LIMITS, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Part(Part::ToolCall {
            id: bytes::copy_of(b"call"),
            name: bytes::copy_of(b"Read"),
            input: bytes::copy_of(b"{broken}"),
            too_large: false,
            bytes: 8,
            cut: false,
        }))
    );
    progress(&mut decoder, br#"{"type":"content_block_stop","index":1}"#, &LIMITS, &mut out);
    feed(&mut decoder, br#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}"#, &LIMITS, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Part(Part::ToolCall {
            id: bytes::copy_of(b"call2"),
            name: bytes::copy_of(b"Write"),
            input: bytes::copy_of(br#"{"x":1}"#),
            too_large: false,
            bytes: 7,
            cut: false,
        }))
    );
}

#[test]
fn oversized_tool_input_is_signaled_without_retaining_fragments() {
    let mut limits = LIMITS;
    limits.input_bytes = 3;
    let mut decoder = StreamDecoder::new(&limits);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &limits, &mut out);
    progress(
        &mut decoder,
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"Read","input":{}}}"#,
        &limits,
        &mut out,
    );
    feed(
        &mut decoder,
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"long"}}"#,
        &limits,
        &mut out,
    );
    assert_eq!(out.pop(), Some(Output::ArgumentsDelta { index: 0, delta: bytes::copy_of(b"long") }));
    progress(&mut decoder, TEXT_STOP, &limits, &mut out);
    feed(&mut decoder, br#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}"#, &limits, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Part(Part::ToolCall {
            id: bytes::copy_of(b"call"),
            name: bytes::copy_of(b"Read"),
            input: bytes::copy_of(b""),
            too_large: true,
            bytes: 4,
            cut: false,
        }))
    );
}

#[test]
fn malformed_stream_order_and_delta_types_fail_once() {
    for bad in [
        START,
        TEXT_START,
        br#"{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#.as_slice(),
        br#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"wrong index"}}"#.as_slice(),
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"wrong kind"}}"#
            .as_slice(),
        END,
        STOP,
    ] {
        let mut decoder = StreamDecoder::new(&LIMITS);
        let mut out = Queue::with_capacity(MAX_OUT);
        progress(&mut decoder, START, &LIMITS, &mut out);
        progress(&mut decoder, TEXT_START, &LIMITS, &mut out);
        feed(&mut decoder, bad, &LIMITS, &mut out);
        failure(&mut out, Failure::Protocol);
        decoder.end(&mut out);
        feed(&mut decoder, STOP, &LIMITS, &mut out);
        assert_eq!(out.len(), 0);
    }
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    feed(&mut decoder, TEXT_START, &LIMITS, &mut out);
    failure(&mut out, Failure::Protocol);
}

#[test]
fn conflicting_stop_and_missing_terminal_fail() {
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &LIMITS, &mut out);
    progress(&mut decoder, END, &LIMITS, &mut out);
    feed(
        &mut decoder,
        br#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}"#,
        &LIMITS,
        &mut out,
    );
    failure(&mut out, Failure::Protocol);
    let mut decoder = StreamDecoder::new(&LIMITS);
    progress(&mut decoder, START, &LIMITS, &mut out);
    decoder.end(&mut out);
    failure(&mut out, Failure::Protocol);
    let mut decoder = StreamDecoder::new(&LIMITS);
    progress(&mut decoder, START, &LIMITS, &mut out);
    feed(&mut decoder, STOP, &LIMITS, &mut out);
    failure(&mut out, Failure::Protocol);
}

#[test]
fn unsigned_thinking_and_invalid_redacted_metadata_reject() {
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &LIMITS, &mut out);
    progress(
        &mut decoder,
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"text"}}"#,
        &LIMITS,
        &mut out,
    );
    feed(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
    failure(&mut out, Failure::Protocol);
    for wire in [
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":""}}"#.as_slice(),
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"one","data":"two"}}"#.as_slice(),
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"one","text":"two"}}"#.as_slice(),
    ] {
        match event(wire, &LIMITS) {
            Err(DecodeError::Malformed | DecodeError::WrongType) => {}
            Ok(_) | Err(DecodeError::Missing | DecodeError::TooLarge { .. }) => unreachable!("invalid metadata rejects"),
        }
    }
}

#[test]
fn answer_and_part_budgets_bound_aggregate_stream() {
    let mut limits = LIMITS;
    limits.answer_bytes = 3;
    let mut decoder = StreamDecoder::new(&limits);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &limits, &mut out);
    progress(&mut decoder, TEXT_START, &limits, &mut out);
    feed(
        &mut decoder,
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"1234"}}"#,
        &limits,
        &mut out,
    );
    failure(&mut out, Failure::Limit { which: crate::Cap::Answer, bound: 3 });
    let mut limits = LIMITS;
    limits.parts = 0;
    let mut decoder = StreamDecoder::new(&limits);
    progress(&mut decoder, START, &limits, &mut out);
    feed(&mut decoder, TEXT_START, &limits, &mut out);
    failure(&mut out, Failure::Limit { which: crate::Cap::Parts, bound: 0 });
}

#[test]
fn provider_error_classification_and_detail_truncation() {
    let mut limits = LIMITS;
    limits.detail_bytes = 3;
    let value =
        Json::from_bytes(r#"{"type":"error","error":{"type":"overloaded_error","message":"ééé"}}"#.as_bytes(), &limits)
            .expect("provider error document");
    let error = super::decode_error(&value, &limits).expect("provider error schema");
    assert_eq!(error.message.as_ref(), "é".as_bytes());
    let mut decoder = StreamDecoder::new(&limits);
    let mut out = Queue::with_capacity(MAX_OUT);
    decoder.event(Event::Failed { error }, &limits, Wall::EPOCH, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Failed { failure: Failure::Overloaded, detail: bytes::copy_of("é".as_bytes()) })
    );
}

#[test]
fn signed_thinking_extension_head_survives_fragments_and_terminal() {
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &LIMITS, &mut out);
    progress(&mut decoder, br#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":"","provider_hint":{"signed":true}}}"#, &LIMITS, &mut out);
    progress(
        &mut decoder,
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"proof"}}"#,
        &LIMITS,
        &mut out,
    );
    feed(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Part(Part::Opaque {
            bytes: bytes::copy_of(
                br#"{"type":"thinking","provider_hint":{"signed":true},"thinking":"","signature":"proof"}"#
            )
        }))
    );
    progress(&mut decoder, END, &LIMITS, &mut out);
    feed(&mut decoder, STOP, &LIMITS, &mut out);
    match out.pop() {
        Some(Output::Completed { .. }) => {}
        Some(
            Output::Part(_)
            | Output::TextDelta { .. }
            | Output::ArgumentsDelta { .. }
            | Output::ReasoningDelta { .. }
            | Output::Failed { .. }
            | Output::Progress,
        )
        | None => unreachable!("actual message stop completes"),
    }
    assert!(decoder.is_complete(), "actual message stop terminates");
}

#[test]
fn synthetic_thinking_heads_are_admitted_before_corrupted_controls() {
    for wire in [
        br#"{"type":"thinking","thinking":"visible","signature":"proof","extension":{"signed":true}}"#.as_slice(),
        br#"{"type":"thinking","thinking":"visible","extension":[null,true]}"#,
    ] {
        let head = Json::from_bytes(wire, &LIMITS).expect("complete positive head");
        let signature = if bytes::find(wire, b"\"signature\"").is_some() { b"proof".as_slice() } else { b"" };
        let event = Event::Added {
            index: 0,
            block: super::BlockStart::Thinking {
                text: bytes::copy_of(b"visible"),
                signature: bytes::copy_of(signature),
                head,
            },
        };
        let encoded = super::encode_event(&event, &LIMITS).expect("positive synthetic entrance");
        let decoded = super::decode_event(&Json::from_bytes(&encoded, &LIMITS).expect("whole wire"), &LIMITS)
            .expect("actual native parser");
        let Event::Added { block: super::BlockStart::Thinking { head, text, signature: actual }, .. } = decoded else {
            panic!("thinking native event");
        };
        assert_eq!(text.as_ref(), b"visible");
        assert_eq!(actual.as_ref(), signature);
        assert!(bytes::find(&head.to_bytes(&LIMITS).expect("whole head"), b"\"extension\"").is_some());
    }
    for head in [
        b"[1]".as_slice(),
        b"null",
        br#"{"thinking":"visible"}"#,
        br#"{"type":"text","thinking":"visible"}"#,
        br#"{"type":"thinking"}"#,
        br#"{"type":"thinking","thinking":1}"#,
        br#"{"type":"thinking","thinking":"different"}"#,
        br#"{"type":"thinking","thinking":"visible","signature":"different"}"#,
        br#"{"type":"thinking","thinking":"visible","signature":false}"#,
    ] {
        let event = Event::Added {
            index: 0,
            block: super::BlockStart::Thinking {
                text: bytes::copy_of(b"visible"),
                signature: Box::new([]),
                head: Json::from_bytes(head, &LIMITS).expect("syntactically valid negative head"),
            },
        };
        assert!(super::encode_event(&event, &LIMITS).is_err(), "malformed head rejected before encoder traversal");
    }
}

#[test]
fn unknown_opaque_native_head_keeps_nested_proof_before_kind_cap_and_delta_negatives() {
    let raw = br#"{"type":"future_block","proof":{"a":[1,2]},"data":"opaque"}"#;
    let head = Json::from_bytes(raw, &LIMITS).expect("whole positive head");
    let event = Event::Added { index: 0, block: super::BlockStart::Opaque { value: head.clone() } };
    let encoded = super::encode_event(&event, &LIMITS).expect("positive opaque entrance");
    let parsed = super::decode_event(&Json::from_bytes(&encoded, &LIMITS).expect("whole event"), &LIMITS)
        .expect("actual opaque native parser");
    assert_eq!(parsed, event);
    let mut decoder = StreamDecoder::new(&LIMITS);
    let mut out = Queue::with_capacity(MAX_OUT);
    progress(&mut decoder, START, &LIMITS, &mut out);
    decoder.event(parsed, &LIMITS, Wall::EPOCH, &mut out);
    assert_eq!(out.pop(), Some(Output::Progress));
    feed(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
    assert_eq!(out.pop(), Some(Output::Part(Part::Opaque { bytes: bytes::copy_of(raw) })));
    let tight = Limits { opaque_bytes: u32::try_from(raw.len()).expect("bounded head") - 1, ..LIMITS };
    assert_eq!(super::encode_event(&event, &tight), Err(DecodeError::limit(crate::Cap::Opaque, tight.opaque_bytes)));
    for raw in [
        b"[1]".as_slice(),
        br#"{"type":""}"#,
        br#"{"type":"text","text":"not opaque"}"#,
        br#"{"type":"tool_use","id":"a","name":"tool","input":{}}"#,
        br#"{"type":"tool_result","tool_use_id":"a","content":"x"}"#,
        br#"{"type":"thinking","thinking":"x","signature":"s"}"#,
    ] {
        let event = Event::Added {
            index: 0,
            block: super::BlockStart::Opaque { value: Json::from_bytes(raw, &LIMITS).expect("negative syntax") },
        };
        assert!(
            super::encode_event(&event, &LIMITS).is_err(),
            "known/empty/nonobject kind cannot disguise its semantics"
        );
    }
    let mut decoder = StreamDecoder::new(&LIMITS);
    progress(&mut decoder, START, &LIMITS, &mut out);
    decoder.event(event, &LIMITS, Wall::EPOCH, &mut out);
    assert_eq!(out.pop(), Some(Output::Progress));
    decoder.event(
        Event::Delta { index: 0, delta: super::Delta::Text { text: bytes::copy_of(b"rewrite") } },
        &LIMITS,
        Wall::EPOCH,
        &mut out,
    );
    failure(&mut out, Failure::Protocol);
}

#[test]
fn native_array_text_is_exactly_measured_for_system_and_tool_results() {
    let template = r#"{"model":"model","stream":true,"max_tokens":32,"system":ARRAY,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":ARRAY}]}]}"#;
    for (array, expected) in [
        ("[]", b"".as_slice()),
        (r#"[{"type":"text","text":""}]"#, b"".as_slice()),
        (r#"[{"type":"text","text":""},{"type":"text","text":""}]"#, b"\n".as_slice()),
        (r#"[{"type":"text","text":"a"}]"#, b"a".as_slice()),
        (r#"[{"type":"text","text":"aaaaa"},{"type":"text","text":"bbbbbb"}]"#, b"aaaaa\nbbbbbb".as_slice()),
    ] {
        let wire = template.replace("ARRAY", array);
        let value = Json::from_bytes(wire.as_bytes(), &LIMITS).expect("whole native positive");
        let exact = Limits { string_bytes: 12, ..LIMITS };
        let request = super::decode_request(&value, &exact).expect("tiny/empty/exact-cap arrays admitted");
        assert_eq!(request.instructions.as_ref(), expected);
        let [crate::Block::ToolResult { text, .. }] = &*request.messages[0].content else {
            unreachable!("literal result-array test");
        };
        assert_eq!(text.as_ref(), expected);
    }
    for array in
        [r#"[{"type":"future","text":"a"}]"#, r#"[{"type":"text","text":"aaaaaa"},{"type":"text","text":"bbbbbb"}]"#]
    {
        for wire in [
            template.replace("\"content\":ARRAY", "\"content\":[]").replace("ARRAY", array),
            template.replace("\"system\":ARRAY,", "").replace("ARRAY", array),
        ] {
            let value = Json::from_bytes(wire.as_bytes(), &LIMITS).expect("whole negative array syntax");
            let exact = Limits { string_bytes: 12, ..LIMITS };
            assert!(super::decode_request(&value, &exact).is_err(), "wrong kind/one-over joining bytes rejected");
        }
    }
}

#[test]
fn output_cap_cuts_the_last_tool_block_with_or_without_block_stop() {
    for closed in [false, true] {
        let mut decoder = StreamDecoder::new(&LIMITS);
        let mut out = Queue::with_capacity(MAX_OUT);
        progress(&mut decoder, START, &LIMITS, &mut out);
        progress(&mut decoder, br#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"c","name":"read","input":{}}}"#, &LIMITS, &mut out);
        feed(
            &mut decoder,
            br#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{broken"}}"#,
            &LIMITS,
            &mut out,
        );
        assert_eq!(out.pop(), Some(Output::ArgumentsDelta { index: 0, delta: bytes::copy_of(b"{broken") }));
        if closed {
            progress(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
        }
        if closed {
            progress(
                &mut decoder,
                br#"{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":2}}"#,
                &LIMITS,
                &mut out,
            );
        }
        feed(
            &mut decoder,
            br#"{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{}}"#,
            &LIMITS,
            &mut out,
        );
        assert_eq!(
            out.pop(),
            Some(Output::Part(Part::ToolCall {
                id: bytes::copy_of(b"c"),
                name: bytes::copy_of(b"read"),
                input: bytes::copy_of(b"{broken"),
                too_large: false,
                bytes: 7,
                cut: true,
            }))
        );
        feed(&mut decoder, STOP, &LIMITS, &mut out);
        match out.pop() {
            Some(Output::Completed { stop, .. }) => assert_eq!(stop, Stop::MaxTokens),
            Some(
                Output::Part(_)
                | Output::TextDelta { .. }
                | Output::ArgumentsDelta { .. }
                | Output::ReasoningDelta { .. }
                | Output::Failed { .. }
                | Output::Progress,
            )
            | None => unreachable!("output-cap terminal"),
        }
    }
}

#[test]
fn anthropic_usage_preserves_omission_zero_and_cumulative_reports() {
    for amount in [0, 7] {
        for (field, expected) in [
            ("input_tokens", Usage { input: Some(amount), ..Usage::NONE }),
            ("cache_read_input_tokens", Usage { cache_read: Some(amount), ..Usage::NONE }),
            ("cache_creation_input_tokens", Usage { cache_write: Some(amount), ..Usage::NONE }),
            ("output_tokens", Usage { output: Some(amount), ..Usage::NONE }),
        ] {
            let mut decoder = StreamDecoder::new(&LIMITS);
            let mut out = Queue::with_capacity(MAX_OUT);
            progress(
                &mut decoder,
                br#"{"type":"message_start","message":{"type":"message","role":"assistant","content":[]}}"#,
                &LIMITS,
                &mut out,
            );
            let document =
                format!(r#"{{"type":"message_delta","delta":{{"stop_reason":null}},"usage":{{"{field}":{amount}}}}}"#);
            progress(&mut decoder, document.as_bytes(), &LIMITS, &mut out);
            progress(&mut decoder, br#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":"bad","output_tokens":null,"cache_read_input_tokens":-1,"reasoning_tokens":88}}"#, &LIMITS, &mut out);
            feed(&mut decoder, STOP, &LIMITS, &mut out);
            assert_eq!(out.pop(), Some(Output::Completed { stop: Stop::EndTurn, usage: expected }));
        }
    }
    for usage in ["", r#", "usage":null"#, r#", "usage":{}"#] {
        let mut decoder = StreamDecoder::new(&LIMITS);
        let mut out = Queue::with_capacity(MAX_OUT);
        let document = format!(
            r#"{{"type":"message_start","message":{{"type":"message","role":"assistant","content":[]{usage}}}}}"#
        );
        progress(&mut decoder, document.as_bytes(), &LIMITS, &mut out);
        progress(&mut decoder, br#"{"type":"message_delta","delta":{"stop_reason":"end_turn"}}"#, &LIMITS, &mut out);
        feed(&mut decoder, STOP, &LIMITS, &mut out);
        assert_eq!(out.pop(), Some(Output::Completed { stop: Stop::EndTurn, usage: Usage::NONE }));
    }
}
