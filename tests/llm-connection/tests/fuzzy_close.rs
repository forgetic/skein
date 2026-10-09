//! Draw owner close/abort turns across racing byte-peer and call terminals.

use skein_lib::Rng;
use skein_llm_connection::Request;
use skein_llm_connection_world::world::World;

#[test]
fn close_and_abort_cross_racing_call_terminals_without_spin_or_repeated_events() {
    for seed in 0..48 {
        let mut rng = Rng::new(seed);
        let turn = rng.between(0, 1200);
        let abort = rng.chance(500);
        let per_endpoint = u32::try_from(rng.between(1, 3)).expect("draw fits");
        let run = |seed| {
            let mut world = World::configured(seed, 3, per_endpoint, 1);
            for _ in 0..turn {
                world.tick();
            }
            world.request(if abort { Request::Abort } else { Request::Close });
            world.finish();
            assert_eq!(world.judge.completed + world.judge.cancelled, 3);
            (world.trace, world.events)
        };
        assert_eq!(run(seed), run(seed), "the close and terminal races replay");
    }
}
