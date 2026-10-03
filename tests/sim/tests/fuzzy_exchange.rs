//! A client and a server exchanging bytes under chaos, over many seeds
//! (simulator.md, 6): every invariant kept, and every fault fell.

use std::collections::BTreeMap;

use skein_io::kernel::Error;
use skein_sim::{Config, Entry, Event, Fault};
use skein_sim_tests::exchange::run;

/// Whether a process reaped a completion of an operation submitted before
/// one it had already reaped.
fn reordered(trace: &[Entry]) -> bool {
    let mut submitted = BTreeMap::new();
    let mut latest = BTreeMap::new();
    for (index, entry) in trace.iter().enumerate() {
        match entry.event {
            Event::Submit { op, .. } => {
                submitted.insert((entry.pid, op), index);
            }
            Event::Complete { op, .. } => {
                let at = submitted[&(entry.pid, op)];
                let last = latest.entry(entry.pid).or_insert(at);
                if at < *last {
                    return true;
                }
                *last = at;
            }
            Event::Fault(_) => {}
        }
    }
    false
}

#[test]
fn a_chaotic_exchange_keeps_every_invariant_for_many_seeds() {
    let mut broken = 0_u32;
    let mut reorders = 0_u32;
    let mut faults = BTreeMap::new();
    let mut errors = BTreeMap::new();
    for seed in 0..200_u64 {
        let outcome = run(seed, Config::chaos());
        if outcome.broken {
            broken = broken.checked_add(1).unwrap();
        }
        if reordered(&outcome.trace) {
            reorders = reorders.checked_add(1).unwrap();
        }
        for entry in &outcome.trace {
            match entry.event {
                Event::Fault(fault) => *faults.entry(fault).or_insert(0_u32) += 1,
                Event::Complete { result: Err(error), .. } => *errors.entry(error).or_insert(0_u32) += 1,
                Event::Submit { .. } | Event::Complete { .. } => {}
            }
        }
    }
    assert!(broken > 0, "chaos breaks some connections");
    assert!(broken < 100, "and most of them deliver everything: {broken}");
    assert!(reorders > 0, "completions arrive out of order");
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
    ] {
        assert!(faults.contains_key(&fault), "{fault:?} fell in some seed: {faults:?}");
    }
    for error in [Error::Reset, Error::Refused, Error::NoBufferSpace, Error::TimedOut, Error::Cancelled] {
        assert!(errors.contains_key(&error), "some operation failed with {error:?}: {errors:?}");
    }
}
