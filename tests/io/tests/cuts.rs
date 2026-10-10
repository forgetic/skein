//! Whole replacements recover through the shared simulator cut and world restart.
use skein_io_world::cuts::{NEW, OLD, world};
use skein_sim::Config;
use skein_world::Cut;

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn a_store_cut_at_each_sync_and_rename_recovers_old_or_new_whole() {
    let mut seen_old = false;
    let mut seen_new = false;
    let mut runs = 0;
    for private in [false, true] {
        let uncut = world(0, Config::calm(), private, false).run();
        let count = uncut.submissions[0];
        assert_eq!(uncut.procs[0].recovered.as_deref(), Some(NEW));
        for before in 0..count {
            for seed in [0, 1, 17] {
                for cut in [Cut::Kill, Cut::PowerLoss] {
                    let mut scenario = world(seed, Config::calm(), private, false);
                    scenario.cut(0, before, cut);
                    let mut outcome = scenario.run();
                    let recovered = outcome.procs[0].recovered.as_deref().expect("recovery loaded a record");
                    seen_old |= recovered == OLD;
                    seen_new |= recovered == NEW;
                    assert_eq!(outcome.machine.cuts, 1);
                    assert_eq!(outcome.machine.opened, [2, 0]);
                    assert_eq!(outcome.machine.files.open_handles(), 0);
                    if private {
                        outcome.machine.check_private();
                    }
                    runs += 1;
                }
            }
        }
    }
    assert!(seen_old && seen_new, "cuts exercise both recovery outcomes");
    eprintln!("file cuts: {runs} memory-checked runs");
}

#[test]
fn kill_keeps_the_peer_running_and_power_loss_restarts_every_root() {
    for cut in [Cut::Kill, Cut::PowerLoss] {
        let mut scenario = world(17, Config::calm(), true, true);
        scenario.cut(0, 5, cut);
        let outcome = scenario.run();
        assert_eq!(outcome.machine.opened, if cut == Cut::Kill { [2, 1] } else { [2, 2] });
        assert!(outcome.procs.iter().all(|proc| proc.recovered.is_some()));
        assert_eq!(outcome.machine.files.open_handles(), 0);
    }
}
