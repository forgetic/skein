//! Exact/one-short byte room and queue-slot headroom through actual IO+Sim.
use skein_channel_world::capacity::{Observation, Outcome, Pressure, pressure};
use skein_io::kernel::Done;
use skein_sim::Summary;

#[test]
fn real_frame_gives_exact_c_or_one_short_with_actual_send_slot_available() {
    let exact = pressure(7, Pressure::BytesExact, false);
    let one_short = pressure(7, Pressure::BytesOneShort, false);
    assert_eq!(exact.retained_bytes, 72);
    assert_eq!(one_short.retained_bytes, 72);
    assert_eq!(exact.free_bytes, 1024);
    assert_eq!(one_short.free_bytes, 1023);
    assert_eq!(exact.retained_frames, 1);
    assert_eq!(one_short.retained_frames, 1);
    assert_eq!(exact.matching_grants, 1);
    assert_eq!(one_short.matching_grants, 1);
    assert!(exact.read_live && one_short.read_live);
    for outcome in [&exact, &one_short] {
        assert!(
            outcome.observations.iter().any(|event| matches!(
                event,
                Observation::Reaped {
                    summary: Summary::Send { len: 72, from: 71, .. },
                    result: Ok(Done::Count(1)),
                    ..
                }
            )),
            "actual fragmented final write winner exists"
        );
    }
    let exact_grant = matching_grant(&exact);
    let short_grant = matching_grant(&one_short);
    assert!(exact_grant < first_retirement(&exact), "exact C is admitted beside owning flight");
    assert!(short_grant > first_retirement(&one_short), "C-1 cannot admit before whole-box retirement");
}

#[test]
fn final_actual_fragment_retires_full_owning_box_before_fixed_c_room_granted() {
    let first = pressure(13, Pressure::Bytes, false);
    assert_eq!(first.unretired_write_bytes, 1);
    assert_eq!(first.retained_bytes, 72);
    assert_eq!(first.free_bytes, 952);
    assert_eq!(first.matching_grants, 1);
    assert_eq!(first, pressure(13, Pressure::Bytes, false), "actual trace and completion-held chronology replay");
}

#[test]
fn actual_queued_slot_blocks_whole_c_despite_three_c_byte_headroom() {
    let first = pressure(17, Pressure::Slots, false);
    assert_eq!(first.retained_frames, 2, "one IO flight plus one real queued Send");
    assert_eq!(first.retained_bytes, 112);
    assert!(first.free_bytes >= 1024);
    assert_eq!(first.matching_grants, 1);
    assert!(first.read_live);
    assert_eq!(first, pressure(17, Pressure::Slots, false));
}

#[test]
fn actual_queued_winner_crosses_logical_close_and_releases_before_resource_closed() {
    for scenario in [Pressure::BytesOneShort, Pressure::Slots] {
        let outcome = pressure(19, scenario, true);
        assert!(outcome.late_winner_retired);
        assert_eq!(outcome.matching_grants, 1);
        let logical = outcome
            .observations
            .iter()
            .position(|event| matches!(event, Observation::LogicalClosed))
            .expect("actual logical core closure");
        let release = outcome
            .observations
            .iter()
            .position(|event| matches!(event, Observation::Release { .. }))
            .expect("actual late winner Release");
        let physical = outcome
            .observations
            .iter()
            .position(|event| matches!(event, Observation::ResourceClosed))
            .expect("actual lower resource closure");
        assert!(logical < release && release < physical, "real queued winner must retire before resourceClosed");
        assert_eq!(outcome.observations.iter().filter(|event| matches!(event, Observation::LogicalClosed)).count(), 1);
    }
}

fn matching_grant(outcome: &Outcome) -> usize {
    outcome
        .observations
        .iter()
        .position(|event| matches!(event, Observation::Granted { held: true, .. }))
        .expect("one genuine held native Granted")
}

fn first_retirement(outcome: &Outcome) -> usize {
    outcome
        .observations
        .iter()
        .position(|event| matches!(event, Observation::Retired { kind: 257 }))
        .expect("genuine full owning frame retirement")
}
