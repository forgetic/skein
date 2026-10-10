//! Seeded crash choices and every submission cut replay through one scheduler.
use skein_io_world::cuts::world;
use skein_lib::Duration;
use skein_sim::{Config, Faults};
use skein_world::Cut;

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn every_file_cut_replays_across_seeded_power_losses() {
    let mut runs = 0;
    for private in [false, true] {
        for seed in 0..64 {
            let config = Config {
                faults: Faults {
                    latency: 600,
                    latency_max: Duration::from_millis(1),
                    short_write: 700,
                    short_read: 700,
                    ..Faults::NONE
                },
                ..Config::calm()
            };
            let count = world(seed, config, private, false).run().submissions[0];
            for before in 0..count {
                for cut in [Cut::Kill, Cut::PowerLoss] {
                    let mut first = world(seed, config, private, false);
                    first.cut(0, before, cut);
                    let first = first.run();
                    let mut second = world(seed, config, private, false);
                    second.cut(0, before, cut);
                    let second = second.run();
                    assert_eq!(
                        first.trace, second.trace,
                        "seed {seed}, private {private}, before {before}, cut {cut:?}"
                    );
                    assert_eq!(first.procs[0].recovered, second.procs[0].recovered);
                    assert_eq!(first.submissions, second.submissions);
                    assert_eq!(first.machine.files.open_handles(), 0);
                    assert_eq!(second.machine.files.open_handles(), 0);
                    runs += 2;
                }
            }
        }
    }
    eprintln!("file cut replay: {runs} memory-checked runs, 64 crash seeds");
}
