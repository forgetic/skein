//! Contract regressions for the generic domain-world utilities extracted
//! when smith became temper's second harness consumer (testing-strategy.md, 6).

use skein_lib::{Duration, Rng, Time};
use skein_world::domain::{Ledger, Schedule, Span, Stage, Trace, assert_replays};

fn at(milliseconds: u64) -> Time {
    Time::ZERO.saturating_add(Duration::from_millis(milliseconds))
}

#[test]
fn same_time_deliveries_keep_send_order_and_a_withdrawn_key_cannot_cancel_a_later_delivery() {
    let mut schedule = Schedule::new();
    let first = schedule.send(at(4), "first");
    let cancelled = schedule.send(at(2), "cancelled");
    let last = schedule.send(at(4), "last");
    assert!(first.serial < cancelled.serial && cancelled.serial < last.serial);
    assert_eq!(schedule.withdraw(cancelled), Some("cancelled"));
    assert_eq!(schedule.next_time(), Some(at(4)));
    assert_eq!(schedule.next(at(3)), None);
    assert_eq!(schedule.next(at(4)), Some("first"));
    let later = schedule.send(at(5), "later");
    assert_ne!(later.serial, cancelled.serial);
    assert_eq!(schedule.withdraw(cancelled), None);
    assert_eq!(schedule.next(at(9)), Some("last"));
    assert_eq!(schedule.next(at(9)), Some("later"));
    assert!(schedule.is_empty());
}

#[test]
fn a_slow_output_consumer_keeps_input_waiting_until_one_whole_step_fits() {
    let mut stage = Stage::<u32, u64, u64>::new(7, 2, 3);
    stage.tick(at(2));
    stage.push(10);
    stage.push(20);
    assert_eq!(stage.next_event(), Some(10));
    stage.out.push(11);
    stage.out.push(12);
    assert_eq!(stage.next_event(), None, "a second step could emit two requests");
    assert!(stage.has_events());
    assert_eq!(stage.out.pop(), Some(11));
    assert_eq!(stage.next_event(), Some(20));
    assert_eq!(stage.env.now, at(2));
    assert_eq!(stage.env.limits, 7, "output pressure does not change domain configuration");
}

#[test]
fn terminal_races_remove_each_open_obligation_once() {
    let mut ledger = Ledger::new("domain effect");
    ledger.open(1_u64, "read");
    ledger.open(2_u64, "cancelled");
    assert_eq!(ledger.end(1), "read");
    assert_eq!(ledger.take(1), None, "a losing terminal race changes nothing");
    assert_eq!(ledger.take(2), Some("cancelled"));
    ledger.assert_settled();
}

#[test]
#[should_panic(expected = "a domain effect ends once, while in flight: 1")]
fn a_duplicate_terminal_is_a_boundary_failure() {
    let mut ledger = Ledger::new("domain effect");
    ledger.open(1_u64, ());
    ledger.end(1);
    ledger.end(1);
}

#[test]
fn replay_compares_boundary_order_and_the_scenario_outcome() {
    let lines = assert_replays(7, 8, |seed| {
        let mut schedule = Schedule::new();
        let mut trace = Trace::default();
        schedule.send(at(seed), "reply");
        let reply = schedule.next(at(seed)).expect("the scheduled reply is due");
        trace.log(at(seed), reply);
        (trace.lines().to_vec(), schedule.is_empty())
    });
    assert_eq!(lines.len(), 1);
    assert!(lines[0].ends_with("reply"));
}

#[test]
fn injected_random_latencies_stay_inside_the_declared_inclusive_span() {
    let span = Span::millis(2, 5);
    let mut rng = Rng::new(19);
    for _ in 0..32 {
        let delay = span.draw(&mut rng);
        assert!(span.min <= delay && delay <= span.max);
    }
    assert_eq!(Span::millis(4, 4).draw(&mut rng), Duration::from_millis(4));
}
