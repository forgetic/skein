use skein_llm::{Block, Cap, Error, Failure, Json, Message, Role, Tool, client};
use skein_llm_world::{TERMINAL, World, call, events, limits, response};

#[test]
fn reasoning_and_other_metadata_have_independent_exact_caps() {
    let metadata = u32::try_from(br#"{"id":"m"}"#.len()).unwrap();
    for (reasoning, metadata_cap) in [(128, metadata), (129, metadata), (128, metadata - 1)] {
        let encrypted = "x".repeat(reasoning);
        let reasoning_done = format!(
            r#"{{"type":"response.output_item.done","output_index":0,"item":{{"type":"reasoning","id":"r","encrypted_content":"{encrypted}"}}}}"#
        );
        let documents = [
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"r","type":"reasoning"}}"#,
            &reasoning_done,
            r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"m","type":"message"}}"#,
            r#"{"type":"response.output_item.done","output_index":1,"item":{"id":"m","type":"message","content":[{"type":"output_text","text":"hello"}]}}"#,
            TERMINAL,
        ];
        let mut bounds = limits();
        bounds.reasoning = 128;
        bounds.metadata = metadata_cap;
        let wire = response(200, "Content-Type: text/event-stream\r\n", &events(&documents), true);
        let mut world = World::new(call(1), bounds, wire, 91);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        if reasoning == 128 && metadata_cap == metadata {
            assert!(
                world.seen.iter().any(|event| matches!(event, client::Event::Completed { completion, .. }
                if matches!(completion.content.as_ref(), [Block::Reasoning { .. }, Block::Text { .. }]))),
                "{:?}",
                world.seen
            );
        } else {
            let (which, bound) =
                if reasoning == 129 { (Cap::Reasoning, 128) } else { (Cap::Metadata, u64::from(metadata_cap)) };
            assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: Failure::Limit { which: actual, bound: actual_bound }, .. } if *actual == which && *actual_bound == bound)), "{:?}", world.seen);
        }
    }
}

#[test]
fn tools_and_history_counts_name_their_own_admission_bound() {
    for tools in [true, false] {
        let which = if tools { Cap::Tools } else { Cap::HistoryItems };
        for maximum in [2, 1] {
            let mut bounds = limits();
            let mut input = call(2);
            if tools {
                bounds.tools = maximum;
                let schema = Json::from_bytes(b"{}", &bounds.native().request_document()).unwrap();
                input.prompt.tools = Box::new([
                    Tool { name: b"a".as_slice().into(), description: Box::new([]), schema: schema.clone() },
                    Tool { name: b"b".as_slice().into(), description: Box::new([]), schema },
                ]);
            } else {
                bounds.history_items = maximum;
                input.prompt.messages = Box::new([
                    Message {
                        role: Role::User,
                        content: Box::new([Block::Text { text: b"a".as_slice().into(), replay: None }]),
                    },
                    Message {
                        role: Role::User,
                        content: Box::new([Block::Text { text: b"b".as_slice().into(), replay: None }]),
                    },
                ]);
            }
            let prepared = client::Client::prepare(input, &bounds);
            if maximum == 2 {
                assert!(prepared.is_ok());
            } else {
                let Err(error) = prepared else { panic!("one over the named count is refused") };
                assert_eq!(error, Error::Limit { which, bound: 1 });
            }
        }
    }
}

#[test]
fn sent_text_uses_the_request_budget_beside_tiny_receiving_strings() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let mut bounds = limits();
        bounds.strings = 4;
        let mut input = call(3);
        input.prompt.instructions = vec![b'x'; 512].into();
        if provider == skein_llm::Provider::Anthropic {
            input.endpoint = skein_llm::Endpoint::anthropic();
            input.credential = skein_llm::Credential::anthropic(b"test".as_slice().into());
            input.prompt.affinity = None;
        }
        assert!(client::Client::prepare(input, &bounds).is_ok());
    }
}
