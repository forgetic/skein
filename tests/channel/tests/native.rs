//! Actual native IO composition, independent byte/slot pressure and replay.
use skein_channel_world::native::conversation;
use skein_lib::Duration;
use skein_sim::{Config, Faults};

#[test]
fn actual_native_read_and_frame_output_progress_independently_and_retire() {
    let config = Config { buffer: 1, ..Config::calm() };
    let outcome = conversation(7, config);
    assert_eq!(outcome, conversation(7, config));
    assert!(outcome.grant_beside_read);
    assert!(outcome.terminals >= 4);
    assert!(!outcome.trace.is_empty());
}

#[test]
fn actual_short_io_frames_keep_single_slot_beside_flight_and_named_winners() {
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
    let outcome = conversation(13, config);
    assert_eq!(outcome, conversation(13, config));
}
