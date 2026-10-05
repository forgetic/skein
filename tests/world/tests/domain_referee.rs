//! The referee holds a world to a scenario's expectations: safety on every
//! observation, liveness as deadlines of its own, stimuli at their moments,
//! and a verdict once it has seen enough.

use skein_lib::{Duration, Time};
use skein_world::domain::{Expectations, Judge, Referee, Verdict};

/// What a toy scenario's referee observes: a request sent, its reply, and a
/// change landing, with or without green checks; a request cancelled, or
/// sent again; a write, after which the scenario drops the network at once;
/// the scenario's goal reached.
enum Seen {
    Sent(u64),
    Replied(u64),
    Landed { green: bool },
    Cancelled(u64),
    Resent(u64),
    Wrote,
    Goal,
}

/// What it injects: the network dropping.
#[derive(PartialEq, Eq, Debug)]
enum Stimulus {
    Drop(u32),
}

/// What it expects to happen: a request's reply.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Name {
    Reply(u64),
}

/// Every request replied to within five seconds, and nothing landed without
/// green checks.
#[derive(Default, Debug)]
struct Toy {
    landed: u32,
}

impl Expectations for Toy {
    type Seen = Seen;
    type Name = Name;
    type Stimulus = Stimulus;

    fn observe(&mut self, seen: Seen, judge: &mut Judge<Name, Stimulus>) {
        match seen {
            Seen::Sent(request) => judge.expect(Name::Reply(request), Duration::from_secs(5)),
            Seen::Replied(request) => {
                let pending = judge.meet(&Name::Reply(request));
                judge.check(pending, "a reply answers a request");
            }
            Seen::Landed { green } => {
                judge.check(green, "nothing lands without green checks");
                self.landed += 1;
            }
            Seen::Cancelled(request) => {
                judge.withdraw(&Name::Reply(request));
            }
            Seen::Resent(request) => judge.rearm(Name::Reply(request), Duration::from_secs(5)),
            Seen::Wrote => judge.inject_now(Stimulus::Drop(0)),
            Seen::Goal => judge.stop(),
        }
    }
}

fn at(secs: u64) -> Time {
    Time::ZERO.saturating_add(Duration::from_secs(secs))
}

/// The referee observes `seen` at `secs`, and nothing is injected at once.
fn see(referee: &mut Referee<Toy>, secs: u64, seen: Seen) {
    let mut out = Vec::new();
    referee.observe(at(secs), seen, &mut out);
    assert!(out.is_empty(), "nothing injected at once");
}

#[test]
fn a_test_passes_once_every_expectation_is_met_and_nothing_is_left_to_inject() {
    let mut referee = Referee::new(Toy::default());
    assert_eq!(referee.verdict(), Verdict::Passed, "nothing expected yet");
    see(&mut referee, 1, Seen::Sent(7));
    see(&mut referee, 2, Seen::Sent(8));
    assert_eq!(referee.next_deadline(), Some(at(6)), "the earliest deadline is the first request's");
    let Verdict::Open { pending } = referee.verdict() else { panic!("two replies are pending") };
    assert_eq!(pending, ["Reply(7) by 6.000000000s", "Reply(8) by 7.000000000s"]);
    see(&mut referee, 3, Seen::Replied(7));
    assert_eq!(referee.next_deadline(), Some(at(7)), "a reply disarms its deadline");
    see(&mut referee, 4, Seen::Landed { green: true });
    see(&mut referee, 5, Seen::Replied(8));
    assert_eq!(referee.next_deadline(), None);
    assert_eq!(referee.verdict(), Verdict::Passed);
    assert_eq!(referee.judged(), (3, 2), "three checks made, two expectations met");
    assert_eq!(referee.expectations().landed, 1);
    referee.assert_passed(0);
}

#[test]
fn an_observation_that_breaks_a_safety_expectation_fails_the_test_with_why() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    see(&mut referee, 2, Seen::Landed { green: false });
    let Verdict::Failed(failure) = referee.verdict() else { panic!("the test failed") };
    assert_eq!(failure.at, at(2));
    assert_eq!(failure.why, "nothing lands without green checks");
    assert_eq!(failure.pending, ["Reply(7) by 6.000000000s"], "what was pending when it failed");
    assert_eq!(
        failure.to_string(),
        "at 2.000000000s, nothing lands without green checks; still pending: Reply(7) by 6.000000000s"
    );
    // The first failure is the one kept.
    see(&mut referee, 3, Seen::Replied(9));
    let Verdict::Failed(again) = referee.verdict() else { panic!("the test stays failed") };
    assert_eq!(again, failure);
}

#[test]
fn a_liveness_deadline_that_passes_fails_the_test_listing_what_is_pending() {
    let mut referee = Referee::new(Toy::default());
    let mut out = Vec::new();
    see(&mut referee, 1, Seen::Sent(7));
    see(&mut referee, 3, Seen::Sent(8));
    assert!(!referee.is_due(at(5)));
    referee.fire(at(5), &mut out);
    let Verdict::Open { .. } = referee.verdict() else { panic!("nothing is due before the deadline") };
    assert!(referee.is_due(at(6)));
    referee.fire(at(6), &mut out);
    let Verdict::Failed(failure) = referee.verdict() else { panic!("the deadline fired") };
    assert_eq!(failure.at, at(6));
    assert_eq!(failure.why, "Reply(7) was not met by 6.000000000s");
    assert_eq!(failure.pending, ["Reply(8) by 8.000000000s"]);
    assert!(out.is_empty());
}

#[test]
fn stimuli_come_out_at_their_moments_in_the_order_they_were_injected() {
    let mut referee = Referee::new(Toy::default());
    let mut out = Vec::new();
    referee.inject(at(4), Stimulus::Drop(2));
    referee.inject(at(2), Stimulus::Drop(1));
    referee.inject(at(4), Stimulus::Drop(3));
    let Verdict::Open { pending } = referee.verdict() else { panic!("stimuli are still to come") };
    assert!(pending.is_empty(), "no liveness expectation is pending");
    assert_eq!(referee.next_deadline(), Some(at(2)));
    referee.fire(at(3), &mut out);
    assert_eq!(out, [Stimulus::Drop(1)]);
    assert_eq!(referee.next_deadline(), Some(at(4)));
    referee.fire(at(4), &mut out);
    assert_eq!(out, [Stimulus::Drop(1), Stimulus::Drop(2), Stimulus::Drop(3)]);
    assert_eq!(referee.verdict(), Verdict::Passed);
}

#[test]
#[should_panic(expected = "seed 42: at 6.000000000s, Reply(7) was not met by 6.000000000s")]
fn a_failed_test_ends_with_its_seed() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    referee.fire(at(6), &mut Vec::new());
    referee.assert_holding(42);
}

#[test]
#[should_panic(expected = "seed 42: the referee still expects Reply(7) by 6.000000000s")]
fn a_test_that_ends_with_expectations_pending_has_not_passed() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    referee.assert_holding(42);
    referee.assert_passed(42);
}

#[test]
#[should_panic(expected = "each expectation pending has a name of its own: Reply(7)")]
fn an_expectation_pending_twice_is_the_scenario_s_mistake() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    see(&mut referee, 2, Seen::Sent(7));
}

#[test]
fn a_stimulus_called_for_by_an_observation_comes_out_of_it_at_once() {
    let mut referee = Referee::new(Toy::default());
    referee.inject(at(9), Stimulus::Drop(9));
    let mut out = Vec::new();
    referee.observe(at(1), Seen::Wrote, &mut out);
    assert_eq!(out, [Stimulus::Drop(0)], "between this write and the next, not at the next fire");
    referee.observe(at(2), Seen::Sent(1), &mut out);
    assert_eq!(out, [Stimulus::Drop(0)], "a stimulus at a later moment waits for it");
}

#[test]
fn an_expectation_whose_deadline_passed_is_no_longer_pending_nor_met_later() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    referee.fire(at(6), &mut Vec::new());
    see(&mut referee, 7, Seen::Replied(7));
    let Verdict::Failed(failure) = referee.verdict() else { panic!("failed") };
    assert_eq!(failure.why, "Reply(7) was not met by 6.000000000s", "the first failure is kept");
    assert_eq!(referee.judged(), (1, 0), "the late reply met nothing");
}

#[test]
fn an_expectation_withdrawn_is_neither_met_nor_failed() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    see(&mut referee, 2, Seen::Cancelled(7));
    assert_eq!(referee.next_deadline(), None);
    referee.fire(at(60), &mut Vec::new());
    assert_eq!(referee.verdict(), Verdict::Passed);
    assert_eq!(referee.judged(), (0, 0));
}

#[test]
fn an_expectation_rearmed_has_its_deadline_from_then() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    see(&mut referee, 4, Seen::Resent(7));
    assert_eq!(referee.next_deadline(), Some(at(9)));
    referee.fire(at(6), &mut Vec::new());
    see(&mut referee, 8, Seen::Replied(7));
    assert_eq!(referee.verdict(), Verdict::Passed);
    see(&mut referee, 9, Seen::Resent(8));
    assert_eq!(referee.next_deadline(), Some(at(14)), "one not pending is armed");
}

#[test]
fn a_scenario_stops_the_test_early_listing_what_is_pending() {
    let mut referee = Referee::new(Toy::default());
    see(&mut referee, 1, Seen::Sent(7));
    assert!(!referee.is_stopped());
    see(&mut referee, 2, Seen::Goal);
    assert!(referee.is_stopped());
    assert_eq!(referee.verdict(), Verdict::Stopped { pending: vec!["Reply(7) by 6.000000000s".to_owned()] });
    referee.assert_passed(0);
    see(&mut referee, 3, Seen::Landed { green: false });
    assert!(!referee.is_stopped(), "a failure still fails it");
    let Verdict::Failed(_) = referee.verdict() else { panic!("failed") };
}
