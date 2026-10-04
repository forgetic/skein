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
