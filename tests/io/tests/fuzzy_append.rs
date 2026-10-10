//! File prefixes, terminal ordering, memory and replay under every append fault.

use skein_io_world::append::{CONTENTS, Story, simulated};
use skein_sim::{Config, Event, Fault};
use skein_world::Memory;
use std::collections::BTreeSet;

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn appends_under_chaos_settle_replay_and_every_applicable_fault_falls() {
    let required = BTreeSet::from([
        Fault::Latency,
        Fault::ShortWrite,
        Fault::NoBuffer,
        Fault::NoSpace,
        Fault::ReadOnly,
        Fault::IoError,
        Fault::Hung,
        Fault::CancelRace,
        Fault::CancelUnsubmitted,
    ]);
    let mut seen = BTreeSet::new();
    for seed in 0..4096 {
        for story in [Story::Finish, Story::Close, Story::Abort] {
            let mut first = simulated(seed, Config::chaos(), story, Memory::Checked);
            let mut second = simulated(seed, Config::chaos(), story, Memory::Checked);
            assert_eq!(first.trace, second.trace, "seed {seed}: exact replay");
            assert_eq!(first.procs[0].events, second.procs[0].events);
            let bytes = first.machine.bytes();
            assert_eq!(bytes, second.machine.bytes());
            assert!(CONTENTS.starts_with(&bytes), "seed {seed}: only an ordered prefix reached the file: {bytes:?}");
            for entry in &first.trace {
                if let Event::Fault(fault) = entry.event {
                    seen.insert(fault);
                }
            }
            first.machine.finish();
            second.machine.finish();
        }
        if seed >= 256 && required.is_subset(&seen) {
            eprintln!("append census: {} seeds, all nine applicable faults covered", seed + 1);
            break;
        }
    }
    assert!(
        required.is_subset(&seen),
        "append faults not yet covered: {:?}",
        required.difference(&seen).collect::<Vec<_>>()
    );
}
