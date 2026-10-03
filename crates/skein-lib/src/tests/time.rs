//! Time, deadlines and randomness (lib.md, 8).

use crate::{Deadlines, Duration, Rng, Time, Wall};

#[test]
fn arithmetic_is_checked_or_saturating() {
    let end = Time::from_nanos(u64::MAX);
    assert_eq!(end.checked_add(Duration::from_nanos(1)), None);
    assert_eq!(end.saturating_add(Duration::from_secs(1)), end);
    assert_eq!(Time::ZERO.saturating_since(end), Duration::ZERO);
    assert_eq!(Duration::from_secs(u64::MAX).as_nanos(), u64::MAX);
    assert_eq!(Duration::from_millis(3).as_nanos(), 3_000_000);
}

#[test]
fn wall_time_counts_from_the_epoch() {
    assert_eq!(Wall::EPOCH.as_nanos(), 0);
    let wall = Wall::from_nanos(1_700_000_000_999_999_999);
    assert_eq!(wall.as_secs(), 1_700_000_000);
    assert_eq!(wall.as_nanos(), 1_700_000_000_999_999_999);
    assert!(Wall::EPOCH < wall);
}

fn at(nanos: u64) -> Time {
    Time::from_nanos(nanos)
}

#[test]
fn timers_fire_in_order_once_due() {
    let mut timers = Deadlines::with_capacity(3);
    timers.arm('b', at(20)).expect("room");
    timers.arm('a', at(10)).expect("room");
    timers.arm('c', at(10)).expect("room");
    assert_eq!(timers.next(), Some(at(10)));
    assert_eq!(timers.expire(at(9)), None);
    assert_eq!(timers.expire(at(15)), Some('a'));
    assert_eq!(timers.expire(at(15)), Some('c'));
    assert_eq!(timers.expire(at(15)), None);
    assert_eq!(timers.expire(at(20)), Some('b'));
    assert!(timers.is_empty());
}

#[test]
fn rearming_moves_a_timer_and_cancelling_is_idempotent() {
    let mut timers = Deadlines::with_capacity(1);
    timers.arm('a', at(10)).expect("room");
    timers.arm('a', at(30)).expect("re-arming takes no new room");
    assert_eq!(timers.arm('b', at(5)), Err('b'));
    assert_eq!(timers.expire(at(10)), None);
    timers.cancel('a');
    timers.cancel('a');
    assert_eq!(timers.next(), None);
}

#[test]
fn the_same_seed_gives_the_same_stream() {
    let mut a = Rng::new(42);
    let mut b = Rng::new(42);
    for _ in 0_u32..100 {
        assert_eq!(a.next_u64(), b.next_u64());
    }
}

#[test]
fn draws_stay_in_range() {
    let mut rng = Rng::new(7);
    for _ in 0_u32..1000 {
        assert!(rng.below(10) < 10);
        let n = rng.between(5, 9);
        assert!((5_u64..=9).contains(&n));
    }
    assert_eq!(rng.below(0), 0);
    assert_eq!(rng.between(9, 5), 9_u64);
    let _: u64 = rng.between(0, u64::MAX);
    assert!(!rng.chance(0));
    assert!(rng.chance(1000));
}
