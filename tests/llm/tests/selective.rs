use skein_llm::{Block, Failure, client};
use skein_llm_world::{TERMINAL, World, call, events, limits, response};

fn tiny() -> client::Limits {
    let mut bounds = limits();
    bounds.dialect.tokens = 64;
    bounds.dialect.document_bytes = 512;
    bounds.dialect.string_bytes = 64;
    bounds.dialect.input_bytes = 3;
    bounds.dialect.opaque_bytes = 128;
    bounds.sse.line = 2_000_000;
    bounds.sse.event = 2_000_000;
    bounds
}

#[test]
fn a_captured_completion_decodes_under_tiny_retained_limits() {
    let capture = include_str!("../../../crates/skein-llm/src/openai/fixtures/provider-completed.json");
    let mut world =
        World::new(call(1), tiny(), response(200, "Content-Type: text/event-stream\r\n", &events(&[capture]), true), 4);
    world.fragmentation(1, 3);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(world.seen.iter().any(|event| matches!(event, client::Event::Completed { completion, .. }
        if completion.usage.input == Some(5256) && completion.usage.output == Some(36))));
}

#[test]
fn an_oversized_native_call_is_scanned_and_completes_without_its_arguments() {
    let arguments = "x".repeat(1_048_576);
    let done = format!(
        r#"{{"type":"response.output_item.done","output_index":0,"item":{{"arguments":"{arguments}","id":"i","call_id":"c","name":"read","type":"function_call"}}}}"#
    );
    let added = r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"i","call_id":"c","name":"read","type":"function_call","arguments":""}}"#;
    let wire = response(200, "Content-Type: text/event-stream\r\n", &events(&[added, &done, TERMINAL]), true);
    let mut world = World::new(call(2), tiny(), wire, 7);
    world.fragmentation(251, 3);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(
        world.seen.iter().any(|event| matches!(event, client::Event::Completed { completion, .. }
        if matches!(completion.content.as_ref(), [Block::Oversize { bytes: 1_048_576, .. }]))),
        "{:?}",
        world.seen
    );
}

#[test]
fn reasoning_one_byte_over_is_dropped_only_by_the_explicit_policy() {
    let content = "x".repeat(129);
    let added = r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"r","type":"reasoning"}}"#;
    let done = format!(
        r#"{{"type":"response.output_item.done","output_index":0,"item":{{"encrypted_content":"{content}","summary":[],"id":"r","type":"reasoning"}}}}"#
    );
    for enabled in [false, true] {
        let mut bounds = tiny();
        bounds.drop_reasoning = enabled;
        let wire = response(200, "Content-Type: text/event-stream\r\n", &events(&[added, &done, TERMINAL]), false);
        let mut world = World::new(call(3), bounds, wire, 9);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        if enabled {
            assert!(
                world.seen.iter().any(|event| matches!(event, client::Event::Completed { completion, .. }
                if matches!(completion.content.as_ref(), [Block::Dropped { bytes: 129 }]))),
                "{:?}",
                world.seen
            );
        } else {
            assert!(
                world.seen.iter().any(|event| matches!(
                    event,
                    client::Event::Failed { failure: Failure::Limit { which: skein_llm::Cap::Opaque, bound: 128 }, .. }
                )),
                "{:?}",
                world.seen
            );
        }
    }
}

#[test]
fn malformed_ignored_extensions_still_fail_the_event() {
    for extension in [r"[1,]", r#""bad\x""#, r#"{"a" 1}"#] {
        let document =
            format!(r#"{{"type":"response.completed","response":{{"status":"completed","output":{extension}}}}}"#);
        let mut world = World::new(
            call(4),
            tiny(),
            response(200, "Content-Type: text/event-stream\r\n", &events(&[&document]), false),
            10,
        );
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(
            world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: Failure::Protocol, .. }))
        );
    }
}

#[test]
fn a_long_messages_argument_delta_counts_the_whole_scanned_fragment() {
    let arguments = "x".repeat(65_536);
    let delta = format!(
        r#"{{"type":"content_block_delta","index":0,"delta":{{"partial_json":"{arguments}","type":"input_json_delta"}}}}"#
    );
    let documents = [
        r#"{"type":"message_start","message":{"type":"message","role":"assistant","content":[],"usage":{"input_tokens":1}}}"#,
        r#"{"type":"content_block_start","index":0,"content_block":{"input":{},"id":"c","name":"read","type":"tool_use"}}"#,
        &delta,
        r#"{"type":"content_block_stop","index":0}"#,
        r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}"#,
        r#"{"type":"message_stop"}"#,
    ];
    let mut input = call(5);
    input.endpoint = skein_llm::Endpoint::anthropic();
    input.credential = skein_llm::Credential::anthropic(b"test-oauth".as_slice().into());
    input.prompt.affinity = None;
    let wire = response(200, "Content-Type: text/event-stream\r\n", &events(&documents), true);
    let mut world = World::new(input, tiny(), wire, 11);
    world.fragmentation(251, 3);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(
        world.seen.iter().any(|event| matches!(event, client::Event::Completed { completion, .. }
        if matches!(completion.content.as_ref(), [Block::Oversize { bytes: 65_536, .. }]))),
        "{:?}",
        world.seen
    );
}
