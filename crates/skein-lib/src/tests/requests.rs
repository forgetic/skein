use crate::List;
use crate::{Duration, RequestLimits, RequestOut, RequestRecord, RequestTable, Time, Token, Wall};
use alloc::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    First(Token),
    Second(Token),
    Restored(crate::RequestKey),
}

fn owner(raw: u64) -> Owner {
    if raw.is_multiple_of(2) { Owner::First(Token::new(raw)) } else { Owner::Second(Token::new(raw)) }
}

fn limits() -> RequestLimits {
    RequestLimits {
        requests: 3,
        bytes: 10,
        out: 3,
        first: Duration::from_secs(1),
        most: Duration::from_secs(8),
        margin: Duration::from_secs(1),
    }
}

#[test]
fn seeded_keys_are_unique_across_thousands_of_asks() {
    let limits = RequestLimits { requests: 5_000, bytes: 5_000, ..limits() };
    let mut table = RequestTable::<Owner, u64>::new(&limits, 0x2345);
    let mut other = RequestTable::<Owner, u64>::new(&limits, 0x2345);
    let mut seen = BTreeSet::new();
    for n in 0..5_000 {
        let key = table.ask(owner(n), 7, 1, n).expect("fits");
        assert!(seen.insert(key));
        assert_eq!(other.ask(owner(n), 7, 1, n), Ok(key));
    }
    assert_eq!(table.len(), 5_000);
}

#[test]
fn count_and_byte_refusals_return_ownership_and_change_nothing() {
    for limits in [RequestLimits { requests: 1, ..limits() }, RequestLimits { bytes: 3, ..limits() }] {
        let mut table = RequestTable::<Owner, u64>::new(&limits, 5);
        let mut control = RequestTable::<Owner, u64>::new(&limits, 5);
        let key = table.ask(owner(1), 7, 3, 10).expect("fits");
        assert_eq!(control.ask(owner(1), 7, 3, 10), Ok(key));
        assert_eq!(table.ask(owner(2), 7, 1, 20), Err(20));
        assert_eq!(table.len(), control.len());
        assert_eq!(table.bytes(), control.bytes());
        assert_eq!(table.owner(key), Some(owner(1)));
        assert_eq!(table.declared_size(key), Some(3));
        let mut outputs = table.take(Time::ZERO, Wall::EPOCH);
        match outputs.next_out().expect("record") {
            RequestOut::Save(record) => assert_eq!(record.key, key),
            RequestOut::Erase(_) | RequestOut::Send { .. } => unreachable!("link starts down"),
        }
        assert!(outputs.next_out().is_none());
    }
}

#[test]
fn byte_overflow_and_zero_capacity_refuse() {
    let mut table = RequestTable::<Owner, u64>::new(&RequestLimits { bytes: u64::MAX, ..limits() }, 1);
    table.ask(owner(1), 7, u64::MAX, 1).expect("fits");
    assert_eq!(table.ask(owner(2), 7, 1, 2), Err(2));
    assert_eq!(table.bytes(), u64::MAX);
    let mut empty = RequestTable::<Owner, u64>::new(&RequestLimits { requests: 0, ..limits() }, 1);
    assert_eq!(empty.ask(owner(1), 7, 0, 1), Err(1));
    assert!(empty.is_empty());
}

#[test]
fn saves_precede_sends_across_bounded_visits() {
    for out in 0..=7 {
        let mut table = RequestTable::<Owner, u64>::new(&RequestLimits { out, ..limits() }, 7);
        table.link(true);
        table.confirmed(Some(4));
        let mut keys = List::with_capacity(3);
        for n in 0..3 {
            keys.push(table.ask(owner(n), 4, 1, n).expect("fits")).expect("three keys");
        }
        let mut saved = BTreeSet::new();
        let mut sent = List::with_capacity(3);
        let mut attempts = BTreeSet::new();
        for _ in 0_u32..7 {
            let mut visit = table.take(Time::ZERO, Wall::from_nanos(99));
            let mut count = 0;
            let mut sending = false;
            while let Some(output) = visit.next_out() {
                count += 1;
                match output {
                    RequestOut::Save(record) => {
                        assert!(!sending);
                        assert_eq!(record.first_sent, Some(Wall::from_nanos(99)));
                        saved.insert(record.key);
                    }
                    RequestOut::Erase(_) => unreachable!("no request retired"),
                    RequestOut::Send { attempt, key, request } => {
                        sending = true;
                        assert!(saved.contains(&key));
                        assert!(attempts.insert(attempt));
                        assert_eq!(*request, u64::from(sent.len()));
                        sent.push(key).expect("three sends");
                    }
                }
            }
            assert!(count <= out);
        }
        if out == 0 {
            assert!(saved.is_empty());
            assert!(sent.is_empty());
        } else {
            assert_eq!(sent.as_slice(), keys.as_slice());
            assert!(!table.pending());
        }
    }
}

#[test]
fn dropped_visits_leave_untaken_work_held() {
    let mut table = RequestTable::<Owner, u64>::new(&limits(), 1);
    let key = table.ask(owner(1), 7, 1, 10).expect("fits");
    {
        let _visit = table.take(Time::ZERO, Wall::EPOCH);
    }
    assert!(table.pending());
    let mut visit = table.take(Time::ZERO, Wall::EPOCH);
    match visit.next_out().expect("saved") {
        RequestOut::Save(record) => assert_eq!(record.key, key),
        RequestOut::Erase(_) | RequestOut::Send { .. } => unreachable!("parked"),
    }
    assert!(visit.next_out().is_none());
}

#[test]
fn parked_asks_are_saved_then_first_send_updates_the_record() {
    let mut table = RequestTable::<Owner, u64>::new(&limits(), 1);
    table.ask(owner(1), 7, 1, 10).expect("fits");
    {
        let mut visit = table.take(Time::ZERO, Wall::EPOCH);
        match visit.next_out().expect("record") {
            RequestOut::Save(record) => assert_eq!(record.first_sent, None),
            RequestOut::Erase(_) | RequestOut::Send { .. } => unreachable!("parked"),
        }
    }
    table.link(true);
    table.confirmed(Some(7));
    let mut visit = table.take(Time::ZERO, Wall::from_nanos(10));
    match visit.next_out().expect("first sending's record") {
        RequestOut::Save(record) => assert_eq!(record.first_sent, Some(Wall::from_nanos(10))),
        RequestOut::Erase(_) | RequestOut::Send { .. } => unreachable!("record first"),
    }
    match visit.next_out().expect("send") {
        RequestOut::Send { .. } => {}
        RequestOut::Erase(_) | RequestOut::Save(_) => unreachable!("record already taken"),
    }
    assert!(visit.next_out().is_none());
}

#[test]
fn restoration_keeps_keys_and_waits_for_link_scope_and_retention() {
    let mut table = RequestTable::<Owner, u64>::new(&limits(), 1);
    let record = RequestRecord { key: [42; 16], scope: 7, first_sent: Some(Wall::from_nanos(10)), request: 5 };
    assert_eq!(table.restore(owner(9), 1, record.clone()), Ok(()));
    assert_eq!(table.restore(owner(8), 1, record.clone()), Err(record.clone()));
    assert!(!table.pending());
    table.link(true);
    table.confirmed(Some(6));
    table.retention(Duration::from_secs(100));
    assert!(table.take(Time::ZERO, Wall::from_nanos(11)).next_out().is_none());
    table.confirmed(Some(7));
    match table.take(Time::ZERO, Wall::from_nanos(11)).next_out().expect("restored send") {
        RequestOut::Send { key, request, .. } => {
            assert_eq!(key, record.key);
            assert_eq!(*request, 5);
        }
        RequestOut::Erase(_) | RequestOut::Save(_) => unreachable!("unchanged record already saved"),
    }
    assert_eq!(table.owner(record.key), Some(owner(9)));
}

#[test]
fn restored_work_waits_for_retention_and_new_keys_skip_held_collisions() {
    let mut original = RequestTable::<Owner, u64>::new(&limits(), 8);
    let key = original.ask(owner(1), 7, 1, 10).expect("fits");
    let record = RequestRecord { key, scope: 7, first_sent: None, request: 10 };
    let mut table = RequestTable::<Owner, u64>::new(&limits(), 8);
    table.restore(owner(1), 1, record).expect("fits");
    table.link(true);
    table.confirmed(Some(7));
    assert!(table.take(Time::ZERO, Wall::EPOCH).next_out().is_none());
    let fresh = table.ask(owner(2), 7, 1, 11).expect("fits");
    assert_ne!(fresh, key);
}

use crate::{RequestAnswered, RequestEnvelope, RequestProgress};

fn live() -> RequestTable<Owner, u64> {
    let mut table = RequestTable::new(&limits(), 42);
    table.link(true);
    table.confirmed(Some(7));
    table.retention(Duration::from_secs(3_600));
    table
}

fn sent(table: &mut RequestTable<Owner, u64>, now: Time, wall: Wall) -> Option<Token> {
    let mut visit = table.take(now, wall);
    let mut attempt = None;
    while let Some(out) = visit.next_out() {
        match out {
            RequestOut::Send { attempt: token, .. } => {
                assert!(attempt.is_none(), "helper expects one send");
                attempt = Some(token);
            }
            RequestOut::Save(_) | RequestOut::Erase(_) => {}
        }
    }
    attempt
}

#[test]
fn final_hands_back_owner_once_and_erases_before_reclaiming() {
    let mut table = live();
    let key = table.ask(owner(99), 7, 10, 5).expect("fits");
    let attempt = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("send");
    assert_eq!(table.progress(), Some((owner(99), RequestProgress::InFlight)));
    assert_eq!(table.answered(Time::ZERO, attempt, RequestEnvelope::Final), RequestAnswered::Final(owner(99)));
    assert_eq!(table.answered(Time::ZERO, attempt, RequestEnvelope::Final), RequestAnswered::Stale);
    table.reclaim();
    assert_eq!(table.len(), 1);
    assert_eq!(table.ask(owner(2), 7, 1, 6), Err(6));
    match table.take(Time::ZERO, Wall::EPOCH).next_out().expect("erase") {
        RequestOut::Erase(erased) => assert_eq!(erased, key),
        RequestOut::Save(_) | RequestOut::Send { .. } => unreachable!("retired"),
    }
    assert!(table.progress().is_none());
    table.reclaim();
    assert!(table.is_empty());
    assert_eq!(table.bytes(), 0);
    assert_eq!(table.owner(key), None);
    assert_ne!(table.ask(owner(2), 7, 10, 6).expect("room freed"), key);
}

#[test]
fn again_and_live_lost_back_off_with_fresh_attempts_and_one_key() {
    for envelope in [RequestEnvelope::Again, RequestEnvelope::Lost] {
        let mut table = live();
        let key = table.ask(owner(1), 7, 1, 8).expect("fits");
        let mut now = Time::ZERO;
        let mut attempt = sent(&mut table, now, Wall::EPOCH).expect("send");
        let mut span = Duration::from_secs(1);
        for _ in 0_u32..6 {
            assert_eq!(table.progress(), Some((owner(1), RequestProgress::InFlight)));
            assert_eq!(table.answered(now, attempt, envelope), RequestAnswered::Pending);
            assert_eq!(table.progress(), Some((owner(1), RequestProgress::Retrying)));
            let deadline = table.next_deadline().expect("retry deadline");
            let delay = deadline.saturating_since(now);
            assert!(delay.as_nanos() >= span.as_nanos().div_euclid(2));
            assert!(delay <= span);
            assert_eq!(sent(&mut table, now, Wall::EPOCH), None);
            let before = Time::from_nanos(deadline.as_nanos().checked_sub(1).expect("positive delay"));
            assert_eq!(sent(&mut table, before, Wall::EPOCH), None);
            let mut visit = table.take(deadline, Wall::EPOCH);
            let next = match visit.next_out().expect("retry") {
                RequestOut::Send { attempt: next, key: retry_key, .. } => {
                    assert_eq!(retry_key, key);
                    assert_ne!(next, attempt);
                    next
                }
                RequestOut::Save(_) | RequestOut::Erase(_) => unreachable!("record unchanged"),
            };
            assert!(visit.next_out().is_none());
            now = deadline;
            assert_eq!(table.answered(now, attempt, RequestEnvelope::Final), RequestAnswered::Stale);
            attempt = next;
            span = span.saturating_mul(2).min(Duration::from_secs(8));
        }
    }
}

#[test]
fn a_down_link_parks_invalidates_attempts_and_resumes_under_new_tokens() {
    let mut table = live();
    let key = table.ask(owner(1), 7, 1, 5).expect("fits");
    let attempt = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("send");
    assert!(table.progress().is_some());
    table.link(false);
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::Parked)));
    assert_eq!(table.answered(Time::ZERO, attempt, RequestEnvelope::Lost), RequestAnswered::Stale);
    assert_eq!(sent(&mut table, Time::ZERO, Wall::EPOCH), None);
    table.link(true);
    let next = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("resumed");
    assert_ne!(next, attempt);
    assert_eq!(table.owner(key), Some(owner(1)));
    assert_eq!(sent(&mut table, Time::ZERO, Wall::EPOCH), None);
}

#[test]
fn signed_out_parks_the_scope_until_explicitly_confirmed_again() {
    let mut table = live();
    table.ask(owner(1), 7, 1, 5).expect("fits");
    let attempt = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("send");
    assert!(table.progress().is_some());
    assert_eq!(table.answered(Time::ZERO, attempt, RequestEnvelope::SignedOut), RequestAnswered::Pending);
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::Parked)));
    table.link(true);
    assert_eq!(sent(&mut table, Time::ZERO, Wall::EPOCH), None);
    table.confirmed(Some(8));
    assert_eq!(sent(&mut table, Time::ZERO, Wall::EPOCH), None);
    table.confirmed(Some(7));
    let next = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("signed in again");
    assert_ne!(next, attempt);
}

#[test]
fn only_the_confirmed_scope_sends_and_changing_it_supersedes_attempts() {
    let mut table = live();
    table.ask(owner(1), 7, 1, 5).expect("fits");
    table.ask(owner(2), 8, 1, 6).expect("fits");
    let old = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("only scope seven");
    table.confirmed(Some(8));
    let next = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("only scope eight");
    assert_eq!(table.answered(Time::ZERO, old, RequestEnvelope::Final), RequestAnswered::Stale);
    assert_eq!(table.answered(Time::ZERO, next, RequestEnvelope::Final), RequestAnswered::Final(owner(2)));
    table.confirmed(None);
    assert_eq!(sent(&mut table, Time::ZERO, Wall::EPOCH), None);
    table.confirmed(Some(7));
    assert!(sent(&mut table, Time::ZERO, Wall::EPOCH).is_some());
}

#[test]
fn retention_is_strict_and_unknown_is_terminal_even_while_parked() {
    let mut table = RequestTable::<Owner, u64>::new(&RequestLimits { margin: Duration::from_nanos(1), ..limits() }, 1);
    table.retention(Duration::from_nanos(11));
    table.confirmed(Some(7));
    table.link(true);
    let key = table.ask(owner(1), 7, 1, 5).expect("fits");
    let attempt = sent(&mut table, Time::ZERO, Wall::from_nanos(100)).expect("send");
    assert_eq!(table.next_deadline(), Some(Time::from_nanos(11)));
    table.link(false);
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::Parked)));
    table.fire(Time::from_nanos(10), Wall::from_nanos(u64::MAX));
    assert!(table.progress().is_none());
    table.fire(Time::from_nanos(11), Wall::EPOCH);
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::Unknown)));
    assert!(table.progress().is_none());
    assert_eq!(table.next_deadline(), None);
    assert_eq!(table.answered(Time::from_nanos(11), attempt, RequestEnvelope::Final), RequestAnswered::Stale);
    match table.take(Time::from_nanos(11), Wall::EPOCH).next_out().expect("erase") {
        RequestOut::Erase(erased) => assert_eq!(erased, key),
        RequestOut::Save(_) | RequestOut::Send { .. } => unreachable!("unknown is retired"),
    }
    table.reclaim();
    assert!(table.is_empty());
}

#[test]
fn restored_age_becomes_monotonic_and_does_not_follow_wall_jumps() {
    let mut table = RequestTable::<Owner, u64>::new(&RequestLimits { margin: Duration::ZERO, ..limits() }, 1);
    let record = RequestRecord { key: [1; 16], scope: 7, first_sent: Some(Wall::from_nanos(10)), request: 5 };
    table.restore(owner(1), 1, record).expect("fits");
    table.fire(Time::from_nanos(100), Wall::from_nanos(15));
    table.retention(Duration::from_nanos(10));
    assert_eq!(table.next_deadline(), Some(Time::from_nanos(106)));
    table.confirmed(Some(7));
    table.link(true);
    assert!(sent(&mut table, Time::from_nanos(105), Wall::EPOCH).is_some());
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::InFlight)));
    table.fire(Time::from_nanos(106), Wall::EPOCH);
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::Unknown)));
}

#[test]
fn expired_restoration_erases_without_sending_and_holds_unknown_until_taken() {
    let mut table = live();
    table.retention(Duration::from_secs(5));
    let record = RequestRecord { key: [1; 16], scope: 7, first_sent: Some(Wall::EPOCH), request: 5 };
    table.restore(owner(1), 1, record).expect("fits");
    assert_eq!(sent(&mut table, Time::ZERO, Wall::from_nanos(9_000_000_000)), None);
    table.reclaim();
    assert_eq!(table.len(), 1);
    assert!(table.progress_pending());
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::Unknown)));
    table.reclaim();
    assert!(table.is_empty());
}

#[test]
fn final_answer_before_firing_in_an_iteration_wins_the_cutoff() {
    let mut table = live();
    table.retention(Duration::ZERO);
    table.ask(owner(1), 7, 1, 5).expect("fits");
    let attempt = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("first send");
    assert_eq!(table.next_deadline(), Some(Time::from_nanos(1)));
    assert_eq!(table.answered(Time::from_nanos(1), attempt, RequestEnvelope::Final), RequestAnswered::Final(owner(1)));
    table.fire(Time::from_nanos(1), Wall::EPOCH);
    assert_eq!(table.progress(), None);
}

#[test]
fn zero_output_or_abandoned_visits_do_not_start_the_retention_clock() {
    let mut table = RequestTable::<Owner, u64>::new(&RequestLimits { out: 0, ..limits() }, 1);
    table.retention(Duration::ZERO);
    table.confirmed(Some(7));
    table.link(true);
    table.ask(owner(1), 7, 1, 5).expect("fits");
    assert!(table.take(Time::ZERO, Wall::EPOCH).next_out().is_none());
    table.fire(Time::from_nanos(1), Wall::EPOCH);
    assert_eq!(table.next_deadline(), None);
    assert_eq!(table.progress(), None);
    let mut table = live();
    table.retention(Duration::ZERO);
    table.ask(owner(1), 7, 1, 5).expect("fits");
    {
        let _visit = table.take(Time::ZERO, Wall::EPOCH);
    }
    assert_eq!(table.next_deadline(), None);
    assert!(sent(&mut table, Time::from_nanos(1), Wall::EPOCH).is_some());
}

#[test]
fn unused_keys_are_never_reused_after_thousands_of_retirements() {
    let mut table = RequestTable::<Owner, u64>::new(&RequestLimits { requests: 1, ..limits() }, 1);
    table.confirmed(Some(7));
    table.link(true);
    let mut keys = BTreeSet::new();
    let mut attempts = BTreeSet::new();
    for n in 0..5_000 {
        let key = table.ask(owner(n), 7, 1, n).expect("room reclaimed");
        assert!(keys.insert(key));
        let attempt = sent(&mut table, Time::ZERO, Wall::EPOCH).expect("send");
        assert!(attempts.insert(attempt));
        assert_eq!(table.answered(Time::ZERO, attempt, RequestEnvelope::Final), RequestAnswered::Final(owner(n)));
        assert_eq!(sent(&mut table, Time::ZERO, Wall::EPOCH), None);
        table.reclaim();
        assert!(table.is_empty());
    }
}

#[test]
fn retention_change_uses_the_existing_monotonic_age() {
    let mut table = RequestTable::<Owner, u64>::new(&RequestLimits { margin: Duration::ZERO, ..limits() }, 1);
    table.confirmed(Some(7));
    table.link(true);
    table.ask(owner(1), 7, 1, 5).expect("fits");
    assert!(sent(&mut table, Time::from_nanos(100), Wall::from_nanos(10)).is_some());
    table.retention(Duration::from_nanos(10));
    assert_eq!(table.next_deadline(), Some(Time::from_nanos(111)));
    table.retention(Duration::from_nanos(5));
    assert_eq!(table.next_deadline(), Some(Time::from_nanos(106)));
    table.fire(Time::from_nanos(106), Wall::from_nanos(10));
    assert_eq!(table.progress(), Some((owner(1), RequestProgress::Unknown)));
}

#[test]
fn time_overflow_and_zero_backoff_stay_bounded() {
    let mut table = RequestTable::<Owner, u64>::new(
        &RequestLimits { first: Duration::ZERO, most: Duration::ZERO, margin: Duration::ZERO, ..limits() },
        1,
    );
    table.confirmed(Some(7));
    table.link(true);
    table.retention(Duration::from_nanos(u64::MAX));
    table.ask(owner(1), 7, 1, 5).expect("fits");
    let now = Time::from_nanos(u64::MAX);
    let attempt = sent(&mut table, now, Wall::EPOCH).expect("send");
    assert_eq!(table.next_deadline(), None);
    assert_eq!(table.answered(now, attempt, RequestEnvelope::Again), RequestAnswered::Pending);
    assert_eq!(table.next_deadline(), Some(now));
    assert!(sent(&mut table, now, Wall::EPOCH).is_some());
}

#[test]
fn progress_and_final_answers_reach_each_owner_across_restores() {
    let owners = [Owner::First(Token::new(1)), Owner::Second(Token::new(1)), Owner::Restored([42; 16])];
    for restored in [false, true] {
        let mut table = RequestTable::new(&RequestLimits { out: 6, ..limits() }, 7);
        let mut keys = List::with_capacity(3);
        for (n, owner) in owners.iter().enumerate() {
            let request = u64::try_from(n).expect("three owners");
            let key = if restored {
                let key = [u8::try_from(n).expect("three keys"); 16];
                let record = RequestRecord { key, scope: 7, first_sent: Some(Wall::EPOCH), request };
                table.restore(*owner, 1, record).expect("room for three restored requests");
                key
            } else {
                table.ask(*owner, 7, 1, request).expect("room for three requests")
            };
            keys.push(key).expect("three keys");
            assert_eq!(table.owner(key), Some(*owner));
            assert_eq!(table.progress(), Some((*owner, RequestProgress::Parked)));
        }
        table.retention(Duration::from_secs(100));
        table.confirmed(Some(7));
        table.link(true);
        let mut attempts = List::with_capacity(3);
        {
            let mut visit = table.take(Time::ZERO, Wall::EPOCH);
            while let Some(output) = visit.next_out() {
                match output {
                    RequestOut::Send { attempt, .. } => attempts.push(attempt).expect("three sends"),
                    RequestOut::Save(_) => {}
                    RequestOut::Erase(_) => unreachable!("nothing retired"),
                }
            }
        }
        assert_eq!(attempts.len(), 3);
        for (owner, attempt) in owners.iter().zip(attempts.as_slice()) {
            assert_eq!(table.progress(), Some((*owner, RequestProgress::InFlight)));
            assert_eq!(table.answered(Time::ZERO, *attempt, RequestEnvelope::Final), RequestAnswered::Final(*owner));
            assert_eq!(table.answered(Time::ZERO, *attempt, RequestEnvelope::Final), RequestAnswered::Stale);
        }
        {
            let mut visit = table.take(Time::ZERO, Wall::EPOCH);
            for key in keys.as_slice() {
                match visit.next_out().expect("one erase per owner") {
                    RequestOut::Erase(erased) => assert_eq!(erased, *key),
                    RequestOut::Save(_) | RequestOut::Send { .. } => unreachable!("all requests retired"),
                }
            }
            assert!(visit.next_out().is_none());
        }
        table.reclaim();
        assert!(table.is_empty());
    }
}

#[test]
fn a_saved_request_can_return_to_a_different_owner_after_reload() {
    let mut before = live();
    let key = before.ask(Owner::First(Token::new(1)), 7, 1, 5).expect("fits");
    let record = match before.take(Time::ZERO, Wall::EPOCH).next_out().expect("saved before sent") {
        RequestOut::Save(record) => record.clone(),
        RequestOut::Erase(_) | RequestOut::Send { .. } => unreachable!("record comes first"),
    };
    let owner = Owner::Restored(key);
    let mut after = live();
    after.restore(owner, 1, record).expect("fits");
    assert_eq!(after.owner(key), Some(owner));
    let attempt = sent(&mut after, Time::ZERO, Wall::EPOCH).expect("restored send");
    assert_eq!(after.progress(), Some((owner, RequestProgress::InFlight)));
    assert_eq!(after.answered(Time::ZERO, attempt, RequestEnvelope::Final), RequestAnswered::Final(owner));
}

#[test]
fn unknown_outcomes_reach_each_owner_without_interpreting_its_value() {
    let owners = [Owner::First(Token::new(1)), Owner::Second(Token::new(1)), Owner::Restored([42; 16])];
    let mut table = RequestTable::<Owner, u64>::new(&limits(), 7);
    table.retention(Duration::ZERO);
    for (n, owner) in owners.iter().enumerate() {
        let record = RequestRecord {
            key: [u8::try_from(n).expect("three keys"); 16],
            scope: 7,
            first_sent: Some(Wall::EPOCH),
            request: 5,
        };
        table.restore(*owner, 1, record).expect("fits");
    }
    table.fire(Time::ZERO, Wall::from_nanos(1));
    for owner in owners {
        assert_eq!(table.progress(), Some((owner, RequestProgress::Unknown)));
    }
    assert!(table.progress().is_none());
}
