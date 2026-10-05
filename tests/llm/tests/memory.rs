//! Counting allocator verifies preparation and every driven call. The world
//! retains emitted values, so its measured total also conservatively includes
//! request/response bytes and the test's trace instead of subtracting them.
use skein_heap::{Counting, Meter, Span};
use skein_llm::client;
use skein_llm_world::{World, call, limits, text_response};

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn preparation_and_streaming_stay_within_the_declared_bound() {
    for chunked in [false, true] {
        let bounds = limits();
        let bound = client::worst_case(&bounds).unwrap();
        let input = call(1);
        let span = Span::start();
        let prepared = client::Client::prepare(input, &bounds).ok().unwrap();
        let grown = span.end();
        assert!(grown.peak <= i64::try_from(bound).unwrap(), "preparation peak {} exceeds {bound}", grown.peak);
        drop(prepared);

        let meter = Meter::new();
        let wire = text_response(chunked);
        let mut world = World::new(call(1), bounds, wire, 37);
        world.fragmentation(1, 5);
        world.request(client::Request::Start);
        for _ in 0..100_000 {
            meter.start();
            let progress = world.tick(true);
            let measured = meter.end();
            assert!(measured.peak() <= bound, "total peak {} exceeds {bound}", measured.peak());
            assert!(
                meter.held() <= bound,
                "including the world's small retained trace, live heap stays within {bound}"
            );
            if !progress {
                break;
            }
        }
        world.assert_once();
        world.request(client::Request::Close);
        world.settle();
        world.settle();
        world.assert_once();
        drop(world);
        assert_eq!(meter.held(), 0, "all client/world allocations released after settlement");
    }
}

#[test]
fn cancellation_releases_owned_buffers() {
    for stage in 0..3 {
        let bounds = limits();
        let bound = client::worst_case(&bounds).unwrap();
        let meter = Meter::new();
        let mut world = World::new(call(1), bounds, text_response(true), 19);
        if stage > 0 {
            world.request(client::Request::Start);
        }
        if stage == 2 {
            for _ in 0..50 {
                world.tick(true);
            }
        }
        world.request(client::Request::Cancel);
        world.settle();
        assert!(meter.held() <= bound);
        drop(world);
        assert_eq!(meter.held(), 0);
    }
}

#[test]
fn bounded_error_body_and_protocol_failure_release_all_storage() {
    let bounds = limits();
    let bound = client::worst_case(&bounds).unwrap();
    for body in [br#"{"error":{"code":"rate_limit_exceeded","message":"later"}}"#.as_slice(), b"not json".as_slice()] {
        let meter = Meter::new();
        let wire = skein_llm_world::response(429, "Content-Type: application/json\r\nRetry-After: 9\r\n", body, false);
        let mut world = World::new(call(1), bounds, wire, 13);
        world.request(client::Request::Start);
        for _ in 0..100_000 {
            meter.start();
            let progress = world.tick(true);
            let measured = meter.end();
            assert!(measured.peak() <= bound);
            assert!(meter.held() <= bound);
            if !progress {
                break;
            }
        }
        world.settle();
        world.assert_once();
        drop(world);
        assert_eq!(meter.held(), 0);
    }
}

#[test]
fn large_schema_request_reasoning_and_answer_fit_the_same_bound() {
    let mut bounds = limits();
    bounds.dialect.parts = 4;
    bounds.dialect.opaque_bytes = 4096;
    let bound = client::worst_case(&bounds).unwrap();
    let meter = Meter::new();
    let mut request = call(1);
    // Request escaping nearly doubles these instructions; a large schema
    // then takes the encoded request close to its body cap.
    request.prompt.instructions = vec![b'"'; 1400].into();
    let schema_wire =
        format!(r#"{{"type":"object","properties":{{"x":{{"type":"string","description":"{}"}}}}}}"#, "s".repeat(3000));
    request.prompt.tools = Box::new([skein_llm::Tool {
        name: b"large_tool".to_vec().into(),
        description: vec![b'd'; 128].into(),
        schema: skein_llm::openai::Json::from_bytes(schema_wire.as_bytes(), &bounds.dialect).unwrap(),
    }]);
    let reasoning = format!(
        r#"{{"type":"response.output_item.done","output_index":0,"item":{{"id":"rs_1","type":"reasoning","encrypted_content":"{}","summary":[]}}}}"#,
        "r".repeat(3500)
    );
    let text = format!(
        r#"{{"type":"response.output_item.done","output_index":1,"item":{{"id":"msg_1","type":"message","content":[{{"type":"output_text","text":"{}"}}]}}}}"#,
        "t".repeat(3500)
    );
    let docs = [
        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning"}}"#,
        &reasoning,
        r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"msg_1","type":"message"}}"#,
        &text,
        skein_llm_world::TERMINAL,
    ];
    let wire =
        skein_llm_world::response(200, "Content-Type: text/event-stream\r\n", &skein_llm_world::events(&docs), true);
    let mut world = World::new(request, bounds, wire, 17);
    world.fragmentation(127, 4);
    world.request(client::Request::Start);
    for _ in 0..100_000 {
        meter.start();
        let progress = world.tick(true);
        let measured = meter.end();
        assert!(measured.peak() <= bound);
        assert!(meter.held() <= bound);
        if !progress {
            break;
        }
    }
    world.assert_once();
    assert!(
        world
            .seen
            .iter()
            .any(|e| matches!(e, client::Event::Completed { completion, .. } if completion.content.len() == 2)),
        "{:?}",
        world.seen
    );
    let head = world.sent.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    assert!(world.sent.len() - head > 6000, "the request is close to its 8192-byte cap");
    world.request(client::Request::Close);
    world.settle();
    drop(world);
    drop(schema_wire);
    drop(reasoning);
    drop(text);
    assert_eq!(meter.held(), 0);
}

#[test]
fn almost_full_error_buffer_and_partial_request_are_bounded() {
    let bounds = limits();
    let bound = client::worst_case(&bounds).unwrap();
    let meter = Meter::new();
    let detail = "x".repeat(3800);
    let body = format!(r#"{{"error":{{"code":"rate_limit_exceeded","message":"{detail}"}}}}"#);
    let wire = skein_llm_world::response(429, "Content-Type: application/json\r\n", body.as_bytes(), true);
    let mut world = World::new(call(1), bounds, wire, 17);
    world.fragmentation(31, 3);
    world.request(client::Request::Start);
    for _ in 0..100_000 {
        meter.start();
        let progress = world.tick(true);
        let measured = meter.end();
        assert!(measured.peak() <= bound);
        assert!(meter.held() <= bound);
        if !progress {
            break;
        }
    }
    world.assert_once();
    world.settle();
    drop(world);
    drop(body);
    drop(detail);
    assert_eq!(meter.held(), 0);
}

#[test]
fn multiple_reasoning_items_with_many_empty_tokens_fit_the_bound() {
    let mut bounds = limits();
    bounds.dialect.parts = 4;
    bounds.dialect.opaque_bytes = 4096;
    let bound = client::worst_case(&bounds).unwrap();
    let meter = Meter::new();
    // Near 1024 tokens per event and near 8192 bytes across the answer, while
    // empty strings make owned token wrappers much larger than their bytes.
    let tiny_values = vec![r#""""#; 850].join(",");
    let mut documents = Vec::new();
    for index in 0..3 {
        documents.push(format!(r#"{{"type":"response.output_item.added","output_index":{index},"item":{{"id":"rs_{index}","type":"reasoning"}}}}"#));
        documents.push(format!(r#"{{"type":"response.output_item.done","output_index":{index},"item":{{"id":"rs_{index}","type":"reasoning","encrypted_content":"opaque","future":[{tiny_values}],"summary":[]}}}}"#));
    }
    documents.push(skein_llm_world::TERMINAL.to_owned());
    let refs: Vec<_> = documents.iter().map(String::as_str).collect();
    let wire =
        skein_llm_world::response(200, "Content-Type: text/event-stream\r\n", &skein_llm_world::events(&refs), true);
    let mut world = World::new(call(1), bounds, wire, 3);
    world.fragmentation(31, 3);
    world.request(client::Request::Start);
    for _ in 0..100_000 {
        meter.start();
        let progress = world.tick(true);
        let measured = meter.end();
        assert!(measured.peak() <= bound);
        assert!(meter.held() <= bound);
        if !progress {
            break;
        }
    }
    world.assert_once();
    let completed = world
        .seen
        .iter()
        .find_map(|e| if let client::Event::Completed { completion, .. } = e { Some(completion) } else { None })
        .unwrap();
    assert_eq!(completed.content.len(), 3);
    for block in &completed.content {
        let skein_llm::Block::Reasoning { replay } = block else { panic!("fixture only contains reasoning") };
        assert!(replay.value.as_tokens().len() > 850, "the small-token wrapper storage is exercised");
    }
    world.request(client::Request::Close);
    world.settle();
    drop(world);
    drop(refs);
    drop(documents);
    drop(tiny_values);
    assert_eq!(meter.held(), 0);
}

#[test]
fn thirty_two_reasoning_arrays_exercise_the_token_wrapper_bound() {
    let mut bounds = limits();
    bounds.dialect.parts = 32;
    bounds.dialect.tokens = 2048;
    bounds.dialect.document_bytes = 4096;
    bounds.dialect.opaque_bytes = 4096;
    bounds.dialect.string_bytes = 64;
    bounds.dialect.answer_bytes = 140_000;
    bounds.dialect.request_bytes = 1024;
    let bound = client::worst_case(&bounds).unwrap();
    let meter = Meter::new();
    let tiny_values = vec!["0"; 1900].join(",");
    let mut documents = Vec::new();
    for index in 0..32 {
        documents.push(format!(r#"{{"type":"response.output_item.added","output_index":{index},"item":{{"id":"rs_{index}","type":"reasoning"}}}}"#));
        documents.push(format!(r#"{{"type":"response.output_item.done","output_index":{index},"item":{{"id":"rs_{index}","type":"reasoning","encrypted_content":"opaque","future":[{tiny_values}],"summary":[]}}}}"#));
    }
    documents.push(skein_llm_world::TERMINAL.to_owned());
    let refs: Vec<_> = documents.iter().map(String::as_str).collect();
    let wire =
        skein_llm_world::response(200, "Content-Type: text/event-stream\r\n", &skein_llm_world::events(&refs), true);
    let mut world = World::new(call(1), bounds, wire, 33);
    world.fragmentation(251, 1);
    world.request(client::Request::Start);
    let mut most = 0;
    for _ in 0..100_000 {
        meter.start();
        let progress = world.tick(true);
        let measured = meter.end();
        most = most.max(measured.peak());
        assert!(measured.peak() <= bound, "total peak {} exceeds {bound}", measured.peak());
        if !progress {
            break;
        }
    }
    world.assert_once();
    let completed = world
        .seen
        .iter()
        .find_map(|e| if let client::Event::Completed { completion, .. } = e { Some(completion) } else { None })
        .unwrap();
    assert_eq!(completed.content.len(), 32);
    for block in &completed.content {
        let skein_llm::Block::Reasoning { replay } = block else { panic!("fixture only contains reasoning") };
        assert!(replay.value.as_tokens().len() > 1900);
    }
    assert!(most > 2_000_000, "many short values exercise substantial owned token storage: {most}");
    world.request(client::Request::Close);
    world.settle();
    drop(world);
    drop(refs);
    drop(documents);
    drop(tiny_values);
    assert_eq!(meter.held(), 0);
}

#[test]
fn anthropic_signed_thinking_tool_input_and_replay_fit_the_declared_bound() {
    // Synthetic Messages documents exercise accumulation into signed replay
    // and tool arguments; they are not captured subscription traffic.
    for chunked in [false, true] {
        for fragment in [1, 251] {
            let mut bounds = limits();
            bounds.dialect.parts = 4;
            bounds.dialect.opaque_bytes = 4096;
            let bound = client::worst_case(&bounds).unwrap();
            let input = memory_anthropic_call(1);
            let span = Span::start();
            let prepared = client::Client::prepare(input, &bounds).ok().unwrap();
            let grown = span.end();
            assert!(grown.peak <= i64::try_from(bound).unwrap(), "Anthropic preparation exceeds {bound}");
            drop(prepared);

            let meter = Meter::new();
            let thinking = format!(
                r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"thinking_delta","thinking":"{}"}}}}"#,
                "r".repeat(1500)
            );
            let signature = format!(
                r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"signature_delta","signature":"{}"}}}}"#,
                "s".repeat(1500)
            );
            let tool_input = format!(
                r#"{{"type":"content_block_delta","index":1,"delta":{{"type":"input_json_delta","partial_json":"{{\"value\":\"{}\"}}"}}}}"#,
                "a".repeat(1800)
            );
            let text = format!(
                r#"{{"type":"content_block_delta","index":2,"delta":{{"type":"text_delta","text":"{}"}}}}"#,
                "t".repeat(1500)
            );
            let documents = [
                r#"{"type":"message_start","message":{"type":"message","role":"assistant","content":[],"usage":{"input_tokens":7,"output_tokens":1}}}"#,
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
                &thinking,
                &signature,
                r#"{"type":"content_block_stop","index":0}"#,
                r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool_1","name":"read","input":{}}}"#,
                &tool_input,
                r#"{"type":"content_block_stop","index":1}"#,
                r#"{"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}"#,
                &text,
                r#"{"type":"content_block_stop","index":2}"#,
                r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":100}}"#,
                r#"{"type":"message_stop"}"#,
            ];
            let wire = skein_llm_world::response(
                200,
                "Content-Type: text/event-stream\r\n",
                &skein_llm_world::events(&documents),
                chunked,
            );
            let input = memory_anthropic_call(1);
            let mut world = World::new(input, bounds, wire, 37);
            world.fragmentation(fragment, 5);
            world.request(client::Request::Start);
            drive_anthropic_memory(&mut world, &meter, bound);
            world.assert_once();
            assert!(
                !world.seen.iter().any(|event| matches!(event, client::Event::Failed { .. })),
                "{:?}",
                world.seen.iter().find(|event| matches!(event, client::Event::Failed { .. }))
            );
            let completed =
                world
                    .seen
                    .iter()
                    .find_map(|event| {
                        if let client::Event::Completed { completion, .. } = event { Some(completion) } else { None }
                    })
                    .unwrap();
            assert_eq!(completed.content.len(), 3);
            assert!(matches!(&completed.content[0], skein_llm::Block::Reasoning { .. }));
            assert!(
                matches!(&completed.content[1], skein_llm::Block::ToolCall { arguments, .. } if arguments.len() > 1800)
            );
            let content = completed.content.clone();
            world.request(client::Request::Close);
            world.settle();
            drop(world);

            let mut replay = memory_anthropic_call(2);
            replay.prompt.messages = Box::new([skein_llm::Message { role: skein_llm::Role::Assistant, content }]);
            let span = Span::start();
            let prepared = client::Client::prepare(replay, &bounds).ok().unwrap();
            let grown = span.end();
            assert!(grown.peak <= i64::try_from(bound).unwrap(), "signed replay preparation exceeds {bound}");
            assert!(meter.held() <= bound, "replay and fixture storage stays within {bound}");
            drop(prepared);
            drop(thinking);
            drop(signature);
            drop(tool_input);
            drop(text);
            assert_eq!(meter.held(), 0, "Anthropic storage released after settlement and replay");
        }
    }
}

fn drive_anthropic_memory(world: &mut World, meter: &Meter, bound: u64) {
    for _ in 0..100_000 {
        meter.start();
        let progress = world.tick(true);
        let measured = meter.end();
        assert!(measured.peak() <= bound, "Anthropic total peak {} exceeds {bound}", measured.peak());
        assert!(meter.held() <= bound, "Anthropic live storage exceeds {bound}");
        if !progress {
            return;
        }
    }
    panic!("bounded Anthropic memory world stalled");
}

fn memory_anthropic_call(owner: u64) -> skein_llm::Call {
    let mut input = call(owner);
    input.endpoint = skein_llm::Endpoint::anthropic();
    input.credential = skein_llm::Credential::anthropic(b"synthetic-oauth-token".to_vec().into());
    input.prompt.cache_key = None;
    input.prompt.max_output_tokens = Some(1024);
    input
}

#[test]
fn opaque_envelope_and_extended_thinking_transit_fit_counted_bounds() {
    for bytes in [32_u32, 2048] {
        let mut bounds = limits();
        bounds.dialect.opaque_bytes = bytes;
        bounds.dialect.string_bytes = bytes.max(4096);
        bounds.dialect.document_bytes = 8192;
        let metadata = format!(
            r#"{{"type":"thinking","thinking":"{}","signature":"s","extension":{{"signed":true}}}}"#,
            "x".repeat(usize::try_from(bytes.saturating_sub(96)).expect("bounded payload"))
        );
        let replay = skein_llm::Replay {
            provider: skein_llm::Provider::Anthropic,
            value: skein_llm::Json::from_bytes(metadata.as_bytes(), &bounds.dialect)
                .expect("bounded extended opaque value"),
        };
        let bound = skein_llm::replay_worst_case(&bounds.dialect).expect("checked replay transit bound");
        let span = Span::start();
        let encoded = replay.to_bytes(&bounds.dialect);
        match encoded {
            Ok(encoded) => {
                let decoded =
                    skein_llm::Replay::from_bytes(&encoded, &bounds.dialect).expect("complete envelope restores");
                assert_eq!(decoded, replay, "counted transit preserves every extension");
                let grown = span.end();
                assert!(
                    grown.peak <= i64::try_from(bound).expect("bounded signed comparison"),
                    "replay transit peak {} exceeds {bound}",
                    grown.peak
                );
                drop(decoded);
                drop(encoded);
            }
            Err(skein_llm::Error::Limit) => {
                let grown = span.end();
                assert!(
                    grown.peak <= i64::try_from(bound).expect("bounded signed comparison"),
                    "refused envelope peak {} exceeds {bound}",
                    grown.peak
                );
            }
            Err(skein_llm::Error::Invalid | skein_llm::Error::Unsupported) => panic!("fixture metadata is valid"),
        }
    }
}

#[test]
fn actual_scripted_byte_peer_and_client_fit_the_composed_heap_envelope() {
    use skein_fake_llm_domain::api::{Finish, Line, Script, Turn};
    let mut bounds = limits();
    // A string at its 4096-byte cap is emitted inside a larger JSON event and
    // a `data: ` line. Admit the entire document plus its six framing bytes.
    bounds.sse.line = bounds.dialect.document_bytes.checked_add(6).expect("bounded data-line framing");
    let peer = skein_llm_world::fake::limits(&bounds);
    let config = skein_llm_world::fake::config();
    let core = client::worst_case(&bounds)
        .expect("client bound")
        .checked_add(skein_fake_llm_protocol::provider::worst_case(&peer).expect("peer bound"))
        .expect("composed peer bound")
        .checked_add(skein_fake_llm_domain::worst_case(&config).expect("script domain bound"))
        .expect("composed domain bound");
    // The independent world owns two 32-KiB intakes, exact wire tapes and
    // observation copies. They are separate from protocol-owned allocations.
    let external = 4_u64 * 32768 + 8 * u64::from(bounds.dialect.document_bytes) + 64 * 1024;
    let mut input = call(1);
    input.prompt.instructions = b"maximum-script".as_slice().into();
    let scripts = Box::new([Script {
        cue: b"maximum-script".as_slice().into(),
        turns: Box::new([Turn {
            lines: Box::new([Line::Text {
                text: vec![b'x'; usize::try_from(bounds.dialect.string_bytes).expect("bounded maximum answer")].into(),
            }]),
            finish: Finish::Stop,
            tokens: 1000,
        }]),
    }]);
    let span = Span::start();
    let mut world = skein_llm_world::fake::Exchange::new(input, bounds, scripts);
    world.start();
    world.run();
    let grown = span.end();
    let bound = core.checked_add(external).expect("checked world-owned envelope");
    assert!(
        grown.peak <= i64::try_from(bound).expect("bounded comparison"),
        "composed actual Client/peer peak {} exceeds {bound}",
        grown.peak
    );
    assert_eq!(world.queries.len(), 1, "maximum payload reached actual peer");
    assert!(
        world.seen.iter().any(|event| matches!(event, client::Event::Completed { .. })),
        "maximum payload reached actual Client terminal: {:?}",
        world.seen
    );
    let actual: Vec<_> = world
        .seen
        .iter()
        .filter_map(|event| match event {
            client::Event::Completed { completion, .. } => Some(completion),
            client::Event::Block { .. }
            | client::Event::Delta { .. }
            | client::Event::Failed { .. }
            | client::Event::Cancelled { .. }
            | client::Event::Reusable
            | client::Event::Close
            | client::Event::Closed => None,
        })
        .collect();
    let [completion] = actual.as_slice() else {
        panic!("exactly one maximum-payload completion");
    };
    let [skein_llm::Block::Text { text, .. }] = &*completion.content else {
        panic!("whole maximum scripted text");
    };
    assert_eq!(text.len(), usize::try_from(bounds.dialect.string_bytes).expect("same maximum text cap"));
    assert!(text.iter().all(|byte| *byte == b'x'), "whole maximum payload was conveyed");
}
