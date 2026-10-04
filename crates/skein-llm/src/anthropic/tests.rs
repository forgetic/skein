use super::{DecodeError, Event, Failure, Json, Limits, MAX_OUT, Output, Part, Stop, StreamDecoder, Usage};
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
            usage: Usage { input_tokens: 10, output_tokens: 11, cache_read_tokens: 8, cache_write_tokens: 7 },
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
    feed(&mut decoder, TEXT_STOP, &LIMITS, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Part(Part::ToolCall {
            id: bytes::copy_of(b"call"),
            name: bytes::copy_of(b"Read"),
            input: bytes::copy_of(b"{broken}"),
            too_large: false,
        }))
    );
    progress(
        &mut decoder,
        br#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call2","name":"Write","input":{"x":1}}}"#,
        &LIMITS,
        &mut out,
    );
    feed(&mut decoder, br#"{"type":"content_block_stop","index":1}"#, &LIMITS, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Part(Part::ToolCall {
            id: bytes::copy_of(b"call2"),
            name: bytes::copy_of(b"Write"),
            input: bytes::copy_of(br#"{"x":1}"#),
            too_large: false,
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
    feed(&mut decoder, TEXT_STOP, &limits, &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Part(Part::ToolCall {
            id: bytes::copy_of(b"call"),
            name: bytes::copy_of(b"Read"),
            input: bytes::copy_of(b""),
            too_large: true,
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
        br#"{"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"one","extra":1}}"#.as_slice(),
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"one","text":"two"}}"#.as_slice(),
    ] {
        match event(wire, &LIMITS) {
            Err(DecodeError::Malformed | DecodeError::WrongType) => {}
            Ok(_) | Err(DecodeError::Missing | DecodeError::TooLarge) => unreachable!("invalid metadata rejects"),
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
    failure(&mut out, Failure::Limit);
    let mut limits = LIMITS;
    limits.parts = 0;
    let mut decoder = StreamDecoder::new(&limits);
    progress(&mut decoder, START, &limits, &mut out);
    feed(&mut decoder, TEXT_START, &limits, &mut out);
    failure(&mut out, Failure::Limit);
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
