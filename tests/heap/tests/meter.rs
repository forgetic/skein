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

#[test]
#[should_panic(expected = "a step is checked once it has ended, before the next starts")]
fn a_step_that_allocates_nothing_is_not_checked_after_the_next() {
    // Both start at the same allocation; the step counter tells them apart.
    let meter = Meter::new();
    meter.start();
    let first = meter.end();
    meter.start();
    let second = meter.end();
    assert_eq!(meter.check(second, 0, &"the second step"), 0);
    meter.check(first, 0, &"the first step");
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

/// A step whose peak, 101 bytes of its own, is a byte allocated right after
/// 100: the narrowest span between two of its highs, so the first two the
/// meter merges. It frees both, then hands out 400 blocks of 2 bytes, each a
/// new high once past its peak: more than the meter keeps.
fn narrow_peak(meter: &Meter) -> (Measured, Vec<Box<[u8]>>) {
    let mut handed = black_box(Vec::with_capacity(400));
    meter.start();
    let hundred = black_box(vec![0_u8; 100]);
    let one = black_box(Box::new(0_u8));
    drop(one);
    drop(hundred);
    for _ in 0..400 {
        handed.push(black_box(vec![0_u8; 2].into_boxed_slice()));
    }
    (meter.end(), handed)
}

#[test]
#[should_panic(expected = "bytes held at the peak of a step, more than the worst case of 100")]
fn a_narrow_peak_outlasts_the_merging_of_highs() {
    let meter = Meter::new();
    let (measured, handed) = narrow_peak(&meter);
    drop(handed);
    meter.check(measured, 100, &"a step");
}

#[test]
fn a_narrow_peak_merged_is_checked_close() {
    // Its high merges with the next, a block of 2 bytes later: the check errs
    // by that block, and no more.
    let meter = Meter::new();
    let (measured, handed) = narrow_peak(&meter);
    drop(handed);
    assert_eq!(meter.check(measured, 102, &"a step"), 102);
}

#[test]
fn a_realloc_holds_the_old_block_and_the_new_at_once() {
    let meter = Meter::new();
    let mut grown = black_box(Vec::<u8>::with_capacity(100));
    meter.start();
    grown.reserve_exact(200);
    let measured = meter.end();
    assert_eq!((measured.peak(), measured.held()), (300, 200));
    assert_eq!(meter.check(measured, 300, &"a step"), 300);
}
