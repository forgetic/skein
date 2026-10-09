//! All process-group stories under delayed, reordered kernel completions.
use skein_io_world::processes::{Story, chaos, run};
use skein_world::Memory;

#[test]
fn process_groups_settle_over_every_seed() {
    for seed in 0..200 {
        for story in [Story::SignalRunning, Story::CloseExited, Story::SignalExited] {
            drop(run(seed, chaos(), story, Memory::Unchecked));
        }
    }
}
