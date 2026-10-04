//! io's scenarios over many seeds (testing-strategy.md, 3 and 8): calm, each
//! held to everything it expects, and chaotic, every fault of the simulator
//! on. A fault configured that never fell tests nothing, so the chaotic
//! sweep asserts that each fell, and that every outcome a cancel may have
//! (kernel.md, 5) came of a cancel of each operation io cancels.

use std::collections::BTreeSet;

use skein_io_world::census::{Answer, Target, cancels, faults};
use skein_io_world::scenarios::SCENARIOS;
use skein_sim::{Config, Fault};

/// Seeds per scenario, calm and chaotic.
const SEEDS: u64 = 150;

#[test]
fn every_scenario_settles_calm_over_many_seeds() {
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
    let mut answered = BTreeSet::new();
    for (_name, scenario) in SCENARIOS {
        for seed in 0..SEEDS {
            let outcome = scenario(seed, Config::chaos()).run();
            fell.extend(faults(&outcome.trace));
            answered.extend(cancels(&outcome.trace));
        }
    }
    for fault in [
        Fault::Latency,
        Fault::ShortRecv,
        Fault::ShortSend,
        Fault::Reset,
        Fault::Refuse,
        Fault::NoBuffer,
        Fault::TimedOut,
        Fault::CancelRace,
        Fault::CancelUnsubmitted,
        Fault::LateReset,
    ] {
        assert!(fell.contains(&fault), "{fault:?} fell in some seed: {fell:?}");
    }
    for target in [Target::Accept, Target::Connect, Target::Recv, Target::Send] {
        for answer in [Answer::Stopped, Answer::TooLate, Answer::Unsubmitted] {
            assert!(answered.contains(&(target, answer)), "a cancel of a {target:?} was {answer:?}: {answered:?}");
        }
    }
}
