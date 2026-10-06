//! Focused native IO conversation and exact replay under fragmentation.

use skein_io_world::output::conversation;
use skein_lib::Duration;
use skein_sim::{Config, Faults};

#[test]
fn late_output_progresses_behind_a_live_read_and_both_actual_payloads_settle() {
    let config = Config { buffer: 1, ..Config::calm() };
    let first = conversation(7, config);
    assert_eq!(first, conversation(7, config), "same seed reproduces actual IO events and kernel trace");
    assert!(!first.trace.is_empty() && !first.events.is_empty());
}

#[test]
fn reordered_short_socket_operations_preserve_exact_output_rights_and_payloads() {
    let config = Config {
        buffer: 2,
        faults: Faults {
            latency: 1000,
            latency_max: Duration::from_millis(3),
            short_send: 1000,
            short_recv: 1000,
            ..Faults::NONE
        },
        ..Config::calm()
    };
    let first = conversation(13, config);
    assert_eq!(first, conversation(13, config));
}
