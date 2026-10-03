//! The meter checks a step at the most it held of its own: what it allocated
//! and freed within the step counts, and what it handed out and the test
//! dropped does not, from the moment it was allocated.

use std::hint::black_box;

use skein_heap::{Counting, Measured, Meter};

#[global_allocator]
static HEAP: Counting = Counting;

/// A step that keeps 100 bytes, hands out 50, then uses 1000 for a while: at
/// its peak, it holds 1100 of its own.
fn handing_first(meter: &Meter) -> (Measured, Vec<u8>, Vec<u8>) {
    meter.start();
    let kept = black_box(vec![0_u8; 100]);
    let handed = black_box(vec![0_u8; 50]);
    drop(black_box(vec![0_u8; 1000]));
    (meter.end(), kept, handed)
}

/// A step that keeps 100 bytes, uses 1000 for a while, then hands out
/// `handed`: whatever it hands out, it held 1100 of its own before.
fn handing_last(meter: &Meter, handed: usize) -> (Measured, Vec<u8>, Vec<u8>) {
    meter.start();
    let kept = black_box(vec![0_u8; 100]);
    drop(black_box(vec![0_u8; 1000]));
    let handed = black_box(vec![0_u8; handed]);
    (meter.end(), kept, handed)
}

#[test]
fn a_step_is_measured_at_its_peak_less_what_it_had_handed_out_by_then() {
    let meter = Meter::new();
    let (measured, kept, handed) = handing_first(&meter);
    assert_eq!((measured.peak(), measured.held()), (1150, 150));
    drop(handed);
    assert_eq!(meter.check(measured, 1100, &"a step"), 1100);
    assert_eq!(meter.held(), 100);
    drop(kept);
}

#[test]
#[should_panic(expected = "1100 bytes held at the peak of a step, more than the worst case of 1099")]
fn a_step_whose_peak_passes_the_worst_case_fails_its_check() {
    let meter = Meter::new();
    let (measured, _kept, handed) = handing_first(&meter);
    drop(handed);
    meter.check(measured, 1099, &"a step");
}

#[test]
#[should_panic(expected = "1100 bytes held at the peak of a step, more than the worst case of 1050")]
fn what_a_step_hands_out_after_its_peak_does_not_count_against_it() {
    let meter = Meter::new();
    let (measured, _kept, handed) = handing_last(&meter, 50);
    assert_eq!((measured.peak(), measured.held()), (1100, 150));
    drop(handed);
    meter.check(measured, 1050, &"a step");
}

#[test]
#[should_panic(expected = "1100 bytes held at the peak of a step, more than the worst case of 1099")]
fn a_step_is_checked_at_the_most_it_held_of_its_own_not_at_its_peak() {
    // Its peak, 1150 bytes, is mostly what it handed out; before, it held
    // 1100 of its own.
    let meter = Meter::new();
    let (measured, _kept, handed) = handing_last(&meter, 1050);
    assert_eq!((measured.peak(), measured.held()), (1150, 1150));
    drop(handed);
    meter.check(measured, 1099, &"a step");
}

/// A step that hands out 1000 blocks of 10 bytes, each a new high, more than
/// the meter keeps, then uses 500 for a while: at most, it held 500 of its
/// own at once.
fn handing_many(meter: &Meter) -> (Measured, Vec<Box<[u8]>>) {
    meter.start();
    let mut handed = black_box(Vec::with_capacity(1000));
    for _ in 0..1000 {
        handed.push(black_box(vec![0_u8; 10].into_boxed_slice()));
    }
    drop(black_box(vec![0_u8; 500]));
    (meter.end(), handed)
}

#[test]
fn a_step_with_more_highs_than_the_meter_keeps_is_checked_close() {
    let meter = Meter::new();
    let (measured, handed) = handing_many(&meter);
    drop(handed);
    assert_eq!(meter.check(measured, 500, &"a step"), 500);
}

#[test]
#[should_panic(expected = "500 bytes held at the peak of a step, more than the worst case of 499")]
fn a_step_with_more_highs_than_the_meter_keeps_is_checked_soundly() {
    let meter = Meter::new();
    let (measured, handed) = handing_many(&meter);
    drop(handed);
    meter.check(measured, 499, &"a step");
}
