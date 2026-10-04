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
            1 => world.abort(Failure::TimedOut),
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
        input.prompt.cache_key = None;
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
        assert_eq!(answer.usage.input_tokens, 7);
        assert_eq!(answer.usage.output_tokens, 9);
        assert_eq!(answer.content.len(), 2);
        assert!(matches!(&answer.content[0], skein_llm::Block::Reasoning { .. }));
        assert!(
            matches!(&answer.content[1], skein_llm::Block::Text { text, .. } if text.as_ref() == "Hello 🌍".as_bytes())
        );
        assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    }
}
