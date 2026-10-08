//! Hosted process interleavings, replay and ownership over deterministic seeds.

use skein_heap::Counting;
use skein_world_tests::hosted::{Story, check_story, story};

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn every_hosted_story_settles_replays_and_frees_its_own_heap_over_seeds() {
    for seed in 0..100 {
        for scenario in [Story::Exchange, Story::Kill, Story::EarlyExit, Story::Terminate, Story::Memory] {
            let first = story(seed, scenario);
            let replay = story(seed, scenario);
            check_story(scenario, &first);
            check_story(scenario, &replay);
            assert_eq!(first.trace, replay.trace, "seed {seed} replays its hosted records");
            assert_eq!(first.heap, replay.heap, "seed {seed} replays its metered heaps");
        }
    }
}
