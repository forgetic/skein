//! The echo's scenarios over many seeds (testing-strategy.md, 3 and 8): calm,
//! each held to everything it expects, and chaotic, every fault of the
//! simulator on. A fault configured that never fell tests nothing, so the
//! chaotic sweep asserts that each fell. Every world checks memory at every
//! iteration, under the counting allocator.

use std::collections::BTreeSet;

use skein_echo_world::census::{FAULTS, faults};
use skein_echo_world::scenarios::SCENARIOS;
use skein_heap::Counting;
use skein_sim::Config;

#[global_allocator]
static HEAP: Counting = Counting;

/// Seeds per scenario, calm and chaotic.
const SEEDS: u64 = 300;

#[test]
fn every_scenario_holds_calm_over_many_seeds() {
    for (name, scenario) in SCENARIOS {
        for seed in 0..SEEDS {
            let outcome = scenario(seed, Config::calm()).run();
            assert!(faults(&outcome.trace).is_empty(), "{name}, seed {seed}: a calm world injects no fault");
        }
    }
}

#[test]
fn every_scenario_settles_under_chaos_and_every_fault_falls() {
    let mut fell = BTreeSet::new();
    for (_name, scenario) in SCENARIOS {
        for seed in 0..SEEDS {
            let outcome = scenario(seed, Config::chaos()).run();
            fell.extend(faults(&outcome.trace));
        }
    }
    for fault in FAULTS {
        assert!(fell.contains(&fault), "{fault:?} fell in some seed: {fell:?}");
    }
}
