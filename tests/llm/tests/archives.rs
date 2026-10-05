//! Historical redacted Tongs captures, distinct from synthetic byte-peer stories.
//! Provenance travels with fixtures; these tests establish no live admission.

use skein_llm::{Block, Completion, Stop, client};
use skein_llm_world::{World, call, limits};

fn archive(scenario: &str, file: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/skein-llm/src/anthropic/fixtures")
            .join(scenario)
            .join(file),
    )
    .expect("preserved historical archive")
}

#[test]
fn historical_messages_captures_flow_through_the_actual_shared_client() {
    for (scenario, input_tokens, output_tokens, calls) in [
        ("single-text", 39_u64, 4_u64, 0_usize),
        ("tool-call", 603, 53, 1),
        ("parallel-tool-calls", 617, 89, 2),
        ("tool-result-final", 679, 15, 0),
    ] {
        let dialect = skein_llm::DocumentLimits {
            request_bytes: 65536,
            document_bytes: 65536,
            string_bytes: 32768,
            tokens: 4096,
            parts: 64,
            ..limits().dialect
        };
        let captured_request = archive(scenario, "request.json");
        let wrapper = skein_llm::Json::from_bytes(&captured_request, &dialect).expect("historical request wrapper");
        let bodies: Vec<_> = wrapper
            .as_tokens()
            .windows(2)
            .filter_map(|tokens| match tokens {
                [skein_json::Token::Key(name), skein_json::Token::String(body)] if name.as_ref() == b"body" => {
                    Some(body)
                }
                _ => None,
            })
            .collect();
        let [body] = bodies.as_slice() else {
            panic!("one preserved captured body");
        };
        let request = skein_llm::anthropic::decode_request(
            &skein_llm::Json::from_bytes(body, &dialect).expect("historical body"),
            &dialect,
        )
        .expect("shared peer grammar admits captured request");
        assert!(!request.messages.is_empty(), "actual historical conversation is preserved");
        assert!(request.max_output_tokens.is_some(), "historical Messages cap remains explicit");
        let mut input = call(51);
        input.endpoint = skein_llm::Endpoint::anthropic();
        input.credential = skein_llm::Credential::anthropic(b"fake-archive-token".as_slice().into());
        input.prompt.cache_key = None;
        input.prompt.max_output_tokens = Some(4096);
        let mut wire = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
        wire.extend_from_slice(&archive(scenario, "response.sse"));
        let mut world = World::new(input, limits(), wire, 7);
        world.fragmentation(23, 1);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        let completions: Vec<&Completion> = world
            .seen
            .iter()
            .filter_map(|event| match event {
                client::Event::Completed { completion, .. } => Some(completion),
                client::Event::Delta { .. }
                | client::Event::Block { .. }
                | client::Event::Failed { .. }
                | client::Event::Cancelled { .. }
                | client::Event::Reusable
                | client::Event::Close
                | client::Event::Closed => None,
            })
            .collect();
        let [completion] = completions.as_slice() else {
            panic!("archive must complete once: {:?}", world.seen);
        };
        assert_eq!(completion.usage.input_tokens, input_tokens, "known captured input accounting");
        assert_eq!(completion.usage.output_tokens, output_tokens, "known captured output accounting");
        assert_eq!(completion.usage.cache_read_tokens, 0);
        assert_eq!(completion.usage.cache_write_tokens, 0);
        assert_eq!(completion.stop, if calls == 0 { Stop::EndTurn } else { Stop::ToolUse });
        let actual_calls: Vec<_> = completion
            .content
            .iter()
            .filter_map(|block| match block {
                Block::ToolCall { name, arguments, .. } => Some((name, arguments)),
                Block::Text { .. } | Block::Refusal { .. } | Block::ToolResult { .. } | Block::Reasoning { .. } => None,
            })
            .collect();
        assert_eq!(actual_calls.len(), calls);
        for (name, arguments) in actual_calls {
            assert_eq!(name.as_ref(), b"get_weather");
            assert!(
                arguments.windows(5).any(|text| text == b"Paris") || arguments.windows(6).any(|text| text == b"London")
            );
        }
        if scenario == "single-text" {
            assert!(completion.content.iter().any(|block| match block {
                Block::Text { text, .. } => text.as_ref() == b"hello",
                Block::Refusal { .. } | Block::ToolCall { .. } | Block::ToolResult { .. } | Block::Reasoning { .. } =>
                    false,
            }));
        }
    }
}
