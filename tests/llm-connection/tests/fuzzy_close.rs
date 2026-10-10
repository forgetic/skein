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
            let mut input = skein_llm_world::call(7);
            input.endpoint = skein_llm::Endpoint::codex();
            input.prompt.instructions = b"close-world".as_slice().into();
            let mut bounds = skein_llm_world::limits();
            bounds.http.request = skein_llm::client::request_head(
                &input.endpoint,
                &skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
                &bounds,
            )
            .unwrap();
            let measured =
                skein_llm::client::measure(&input.prompt, &input.credential, &input.endpoint, &bounds).unwrap();
            let memory = 2 * skein_llm::client::reservation(measured, &bounds).unwrap();
            let mut world = World::empty_with_memory(seed, 3, per_endpoint, 1, memory);
            for token in 7..10 {
                world.start(token);
            }
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
