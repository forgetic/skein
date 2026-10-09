//! Seeded fragmentation, delayed grants, premature lower close and stalls.
use skein_llm::{Failure, client};
use skein_llm_world::{World, call, limits, text_response};

#[test]
fn fragmentation_and_delayed_room_keep_the_same_completion() {
    for seed in 1..=128 {
        let mut world = World::new(call(seed), limits(), text_response(seed % 2 == 0), seed);
        world.fragmentation(u32::try_from(seed % 73 + 1).unwrap(), u32::try_from(seed % 7).unwrap());
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        assert!(
            world.seen.iter().any(|e| matches!(e, client::Event::Completed { .. })),
            "seed {seed}: {:?}",
            world.seen
        );
    }
}

#[test]
fn closing_and_stalling_at_seeded_turns_never_duplicates_outcomes() {
    for seed in 1..=128 {
        let wire = text_response(seed % 2 == 0);
        let cut = usize::try_from(seed * 17).unwrap() % wire.len();
        let mut world = World::new(call(seed), limits(), wire[..cut].to_vec(), seed);
        world.fragmentation(u32::try_from(seed % 19 + 1).unwrap(), 2);
        world.request(client::Request::Start);
        for _ in 0..seed % 71 {
            if !world.tick(true) {
                break;
            }
        }
        match seed % 3 {
            0 => world.request(client::Request::Cancel),
            1 => world.abort(Failure::TimedOut { phase: skein_llm::Phase::Whole }),
            2 => world.settle(),
            _ => unreachable!(),
        }
        world.settle();
        world.settle();
        world.request(client::Request::Cancel);
        world.assert_once();
    }
}

#[test]
fn reset_between_every_routing_turn_preserves_one_terminal() {
    // This includes resets while HTTP/SSE/dialect local work is buffered;
    // the owner is allowed to learn that its lower stream died at any turn.
    for cut in 0..256 {
        let mut world = World::new(call(cut + 1), limits(), text_response(false), 7);
        world.fragmentation(19, 2);
        world.request(client::Request::Start);
        for _ in 0..cut {
            if !world.tick(true) {
                break;
            }
        }
        world.transport_failed();
        world.settle();
        world.settle();
        world.assert_once();
    }
}

#[test]
fn anthropic_seeded_fragmentation_and_grants_preserve_completion() {
    // Handwritten synthetic Messages stream, including split UTF-8 and signed
    // thinking replay, independent of the product request/event encoders.
    let documents = [
        r#"{"type":"message_start","message":{"type":"message","role":"assistant","content":[],"usage":{"input_tokens":7,"output_tokens":1}}}"#,
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Plan 🌍"}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-opaque"}}"#,
        r#"{"type":"content_block_stop","index":0}"#,
        r#"{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#,
        r#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Hello 🌍"}}"#,
        r#"{"type":"content_block_stop","index":1}"#,
        r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":9}}"#,
        r#"{"type":"message_stop"}"#,
    ];
    for seed in 1..=128 {
        let mut input = call(seed);
        input.endpoint = skein_llm::Endpoint::anthropic();
        input.credential = skein_llm::Credential::anthropic(b"synthetic-oauth-token".to_vec().into());
        input.prompt.affinity = None;
        input.prompt.max_output_tokens = Some(1024);
        let wire = skein_llm_world::response(
            200,
            "Content-Type: text/event-stream\r\n",
            &skein_llm_world::events(&documents),
            seed % 2 == 0,
        );
        let mut world = World::new(input, limits(), wire, seed);
        world.fragmentation(u32::try_from(seed % 73 + 1).unwrap(), u32::try_from(seed % 7).unwrap());
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        let answer = world
            .seen
            .iter()
            .find_map(
                |event| {
                    if let client::Event::Completed { completion, .. } = event { Some(completion) } else { None }
                },
            )
            .unwrap();
        assert_eq!(answer.stop, skein_llm::Stop::EndTurn, "seed {seed}");
        assert_eq!(answer.usage.input, Some(7));
        assert_eq!(answer.usage.output, Some(9));
        assert_eq!(answer.content.len(), 2);
        assert!(matches!(&answer.content[0], skein_llm::Block::Reasoning { .. }));
        assert!(
            matches!(&answer.content[1], skein_llm::Block::Text { text, .. } if text.as_ref() == "Hello 🌍".as_bytes())
        );
        assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    }
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "the referee extracts only failure terminals")]
fn randomized_receiving_limits_name_the_cap_and_the_bound_that_was_passed() {
    use skein_llm::Cap;
    for seed in 1..=96 {
        let mut bounds = limits();
        let bound = u32::try_from(seed % 7 + 1).unwrap();
        let which = match seed % 6 {
            0 => {
                bounds.tokens = bound;
                Cap::Tokens
            }
            1 => {
                bounds.retained = bound;
                Cap::Retained
            }
            2 => {
                bounds.answer = bound;
                Cap::Answer
            }
            3 | 4 => {
                bounds.skip = bound;
                Cap::Skip
            }
            5 => {
                bounds.output_items = 1;
                Cap::OutputItems
            }
            _ => unreachable!(),
        };
        let expected_bound = if which == Cap::OutputItems { 1 } else { u64::from(bound) };
        let mut input = call(seed);
        input.prompt.messages = Box::new([]);
        let wire = if which == Cap::OutputItems {
            let documents = [
                skein_llm_world::TEXT_ADDED,
                skein_llm_world::TEXT_DONE,
                r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"second","type":"message"}}"#,
                r#"{"type":"response.output_item.done","output_index":1,"item":{"id":"second","type":"message","content":[{"type":"output_text","text":"more"}]}}"#,
                skein_llm_world::TERMINAL,
            ];
            skein_llm_world::response(
                200,
                "Content-Type: text/event-stream\r\n",
                &skein_llm_world::events(&documents),
                false,
            )
        } else {
            text_response(seed % 2 == 0)
        };
        let mut world = World::new(input, bounds, wire, seed);
        world.fragmentation(u32::try_from(seed % 19 + 1).unwrap(), 1);
        world.request(client::Request::Start);
        world.run();
        world.settle();
        world.assert_once();
        let failures: Vec<_> = world
            .seen
            .iter()
            .filter_map(|event| match event {
                client::Event::Failed { failure, evidence, .. } => Some((*failure, *evidence)),
                _ => None,
            })
            .collect();
        assert_eq!(
            failures,
            [(Failure::Limit { which, bound: expected_bound }, client::Evidence::Response { status: 200 })],
            "seed {seed}"
        );
        // The independently written tape exceeds each tiny cap: its answer is
        // Hello plus native identity, its first document has more than seven
        // tokens/bytes, and its SSE framing has more than seven bytes. An item
        // cap of one cannot accept its second output block.
        assert!(expected_bound < 8);
    }
}

#[test]
fn selective_known_events_ignore_seeded_extensions_and_field_order() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        for seed in 1_u64..=256 {
            let mut rng = skein_lib::Rng::new(seed);
            let noise =
                "x".repeat(usize::try_from(rng.next_u64() % 2048).expect("seeded value fits the configured bound"));
            let extra = format!(r#""extension_{seed}":{{"future":[null,1,{{"wire":"{noise}"}}]}}"#);
            let documents = match provider {
                skein_llm::Provider::OpenAiCodex => {
                    let item = if seed % 2 == 0 {
                        format!(
                            r#"{{{extra},"id":"m","content":[{{{extra},"text":"hello","type":"output_text"}}],"type":"message"}}"#
                        )
                    } else {
                        format!(
                            r#"{{"type":"message","id":"m","content":[{{"type":"output_text","text":"hello",{extra}}}],{extra}}}"#
                        )
                    };
                    vec![
                        format!(
                            r#"{{{extra},"type":"response.output_item.added","output_index":0,"item":{{"id":"m","type":"message"}}}}"#
                        ),
                        format!(r#"{{"type":"response.output_item.done","output_index":0,"item":{item},{extra}}}"#),
                        format!(
                            r#"{{"type":"response.completed","response":{{{extra},"output":["{noise}"],"status":"completed","usage":{{{extra},"input_tokens":3,"output_tokens":2}}}},{extra}}}"#
                        ),
                    ]
                }
                skein_llm::Provider::Anthropic => vec![
                    format!(
                        r#"{{{extra},"type":"message_start","message":{{{extra},"type":"message","role":"assistant","content":[],"usage":{{"input_tokens":3,{extra}}}}}}}"#
                    ),
                    format!(
                        r#"{{"type":"content_block_start","index":0,"content_block":{{{extra},"text":"hello","type":"text"}}}}"#
                    ),
                    format!(r#"{{{extra},"type":"content_block_stop","index":0}}"#),
                    format!(
                        r#"{{"type":"message_delta","delta":{{"stop_reason":"end_turn"}},"usage":{{{extra},"output_tokens":2}}}}"#
                    ),
                    format!(r#"{{{extra},"type":"message_stop"}}"#),
                ],
            };
            let refs: Vec<_> = documents.iter().map(String::as_str).collect();
            let wire = skein_llm_world::response(
                200,
                "Content-Type: text/event-stream\r\n",
                &skein_llm_world::events(&refs),
                seed % 2 == 0,
            );
            let mut bounds = limits();
            bounds.tokens = 64;
            bounds.retained = 512;
            bounds.strings = 64;
            bounds.input = 64;
            bounds.reasoning = 128;
            bounds.skip = 16_384;
            let mut input = call(seed);
            if provider == skein_llm::Provider::Anthropic {
                input.endpoint = skein_llm::Endpoint::anthropic();
                input.credential = skein_llm::Credential::anthropic(b"test-oauth".to_vec().into());
                input.prompt.affinity = None;
            }
            let mut world = World::new(input, bounds, wire, seed);
            world.fragmentation(
                u32::try_from(rng.next_u64() % 73 + 1).expect("seeded value fits the configured bound"),
                3,
            );
            world.request(client::Request::Start);
            world.run();
            world.assert_once();
            assert!(world.seen.iter().any(|event| matches!(event, client::Event::Completed { completion, .. }
                if completion.usage.input == Some(3) && completion.usage.output == Some(2)
                && matches!(completion.content.as_ref(), [skein_llm::Block::Text { text, .. }] if text.as_ref() == b"hello"))), "{provider:?} seed {seed}: {:?}", world.seen);
        }
    }
}
