//! A bounded seed sweep of native output with real IO/kernel settlement.

use skein_io_world::output::conversation;
use skein_lib::Duration;
use skein_sim::{Config, Faults};

#[test]
fn independent_output_and_unanswered_reads_progress_across_reordered_seeds() {
    let config = Config {
        buffer: 1,
        faults: Faults {
            latency: 700,
            latency_max: Duration::from_millis(3),
            short_send: 700,
            short_recv: 700,
            ..Faults::NONE
        },
        ..Config::calm()
    };
    for seed in 0..64 {
        let first = conversation(seed, config);
        assert_eq!(first, conversation(seed, config), "native seed {seed} replays");
    }
}
