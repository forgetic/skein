//! Whole-file replacements and loads replay under every applicable disk fault.

use skein_io_world::files::{NEW, OLD, Story, world};
use skein_sim::{Config, Event, Fault};
use std::collections::BTreeSet;

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn stores_and_loads_under_chaos_settle_replay_and_every_disk_fault_falls() {
    let required = BTreeSet::from([
        Fault::Latency,
        Fault::ShortWrite,
        Fault::ShortRead,
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
        let mut first = world(seed, Config::chaos(), Story::Replace, None, 3).run();
        let mut second = world(seed, Config::chaos(), Story::Replace, None, 3).run();
        assert_eq!(first.trace, second.trace, "seed {seed}: exact replay");
        assert_eq!(first.procs[0].events, second.procs[0].events, "seed {seed}: terminal replay");
        let bytes = first.machine.bytes(b"record");
        assert_eq!(bytes, second.machine.bytes(b"record"));
        assert!(bytes == OLD || bytes == NEW, "seed {seed}: whole old or new target");
        let mut names = vec![b"record".to_vec()];
        if let skein_io::file::Event::Failed { residue: Some(residue), committed, .. } = &first.procs[0].events[0] {
            assert!(!committed, "seed {seed}: a committed store has no temporary residue");
            assert_eq!(bytes, OLD, "seed {seed}: cleanup refusal preserves old bytes");
            assert!(residue.name.starts_with(b"record.skein-"));
            names.push(residue.name.to_vec());
        }
        assert_eq!(first.machine.names(), names, "seed {seed}: all temporary residue is reported");
        assert_eq!(second.machine.names(), names, "seed {seed}: residue replay");
        for entry in &first.trace {
            if let Event::Fault(fault) = entry.event {
                seen.insert(fault);
            }
        }
        first.machine.finish();
        second.machine.finish();
        if seed >= 256 && required.is_subset(&seen) {
            eprintln!("files census: {} seeds, all ten applicable faults covered", seed + 1);
            break;
        }
    }
    assert!(required.is_subset(&seen), "file faults missing: {:?}", required.difference(&seen).collect::<Vec<_>>());
}
