//! Seeded completion reordering for shared startup-root ownership controls.

use skein_io::kernel::{Error, Exit};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn startup_roots_settle_over_reordered_normal_failed_and_killed_launches() {
    for seed in 0..100 {
        for (kill, missing) in [(false, false), (true, false), (false, true)] {
            let mut outcome = skein_world_tests::roots::simulated(seed, kill, missing);
            if missing {
                assert!(outcome.procs[0].results.contains(&Err(Error::NotFound)));
                assert_eq!(outcome.procs.len(), 1);
            } else if kill {
                assert_eq!(outcome.procs[0].child_exit, Some(Exit::Signal(9)));
                assert_eq!(outcome.killed.len(), 1);
            } else {
                assert_eq!(outcome.procs[1].received, b"gofrom child");
                assert_eq!(outcome.procs[0].child_exit, Some(Exit::Code(0)));
            }
            outcome.machine.finish();
        }
    }
}
