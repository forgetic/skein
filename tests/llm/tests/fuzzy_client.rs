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
