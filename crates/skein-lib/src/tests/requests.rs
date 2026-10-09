use crate::List;
use crate::{Duration, RequestLimits, RequestOut, RequestRecord, RequestTable, Time, Token, Wall};
use alloc::collections::BTreeSet;

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
    let mut table = RequestTable::<u64>::new(&limits, 0x2345);
    let mut other = RequestTable::<u64>::new(&limits, 0x2345);
    let mut seen = BTreeSet::new();
    for n in 0..5_000 {
        let key = table.ask(Token::new(n), 7, 1, n).expect("fits");
        assert!(seen.insert(key));
        assert_eq!(other.ask(Token::new(n), 7, 1, n), Ok(key));
    }
    assert_eq!(table.len(), 5_000);
}

#[test]
fn count_and_byte_refusals_return_ownership_and_change_nothing() {
    for limits in [RequestLimits { requests: 1, ..limits() }, RequestLimits { bytes: 3, ..limits() }] {
        let mut table = RequestTable::<u64>::new(&limits, 5);
        let mut control = RequestTable::<u64>::new(&limits, 5);
        let key = table.ask(Token::new(1), 7, 3, 10).expect("fits");
        assert_eq!(control.ask(Token::new(1), 7, 3, 10), Ok(key));
        assert_eq!(table.ask(Token::new(2), 7, 1, 20), Err(20));
        assert_eq!(table.len(), control.len());
        assert_eq!(table.bytes(), control.bytes());
        assert_eq!(table.owner(key), Some(Token::new(1)));
        assert_eq!(table.declared_size(key), Some(3));
        let mut outputs = table.take(Time::ZERO, Wall::EPOCH);
        match outputs.next_out().expect("record") {
            RequestOut::Save(record) => assert_eq!(record.key, key),
            RequestOut::Send { .. } => unreachable!("link starts down"),
        }
        assert!(outputs.next_out().is_none());
    }
}

#[test]
fn byte_overflow_and_zero_capacity_refuse() {
    let mut table = RequestTable::<u64>::new(&RequestLimits { bytes: u64::MAX, ..limits() }, 1);
    table.ask(Token::new(1), 7, u64::MAX, 1).expect("fits");
    assert_eq!(table.ask(Token::new(2), 7, 1, 2), Err(2));
    assert_eq!(table.bytes(), u64::MAX);
    let mut empty = RequestTable::<u64>::new(&RequestLimits { requests: 0, ..limits() }, 1);
    assert_eq!(empty.ask(Token::new(1), 7, 0, 1), Err(1));
    assert!(empty.is_empty());
}

#[test]
fn saves_precede_sends_across_bounded_visits() {
    for out in 0..=7 {
        let mut table = RequestTable::<u64>::new(&RequestLimits { out, ..limits() }, 7);
        table.link(true);
        table.confirmed(Some(4));
        let mut keys = List::with_capacity(3);
        for n in 0..3 {
            keys.push(table.ask(Token::new(n), 4, 1, n).expect("fits")).expect("three keys");
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
    let mut table = RequestTable::<u64>::new(&limits(), 1);
    let key = table.ask(Token::new(1), 7, 1, 10).expect("fits");
    {
        let _visit = table.take(Time::ZERO, Wall::EPOCH);
    }
    assert!(table.pending());
    let mut visit = table.take(Time::ZERO, Wall::EPOCH);
    match visit.next_out().expect("saved") {
        RequestOut::Save(record) => assert_eq!(record.key, key),
        RequestOut::Send { .. } => unreachable!("parked"),
    }
    assert!(visit.next_out().is_none());
}

#[test]
fn parked_asks_are_saved_then_first_send_updates_the_record() {
    let mut table = RequestTable::<u64>::new(&limits(), 1);
    table.ask(Token::new(1), 7, 1, 10).expect("fits");
    {
        let mut visit = table.take(Time::ZERO, Wall::EPOCH);
        match visit.next_out().expect("record") {
            RequestOut::Save(record) => assert_eq!(record.first_sent, None),
            RequestOut::Send { .. } => unreachable!("parked"),
        }
    }
    table.link(true);
    table.confirmed(Some(7));
    let mut visit = table.take(Time::ZERO, Wall::from_nanos(10));
    match visit.next_out().expect("first sending's record") {
        RequestOut::Save(record) => assert_eq!(record.first_sent, Some(Wall::from_nanos(10))),
        RequestOut::Send { .. } => unreachable!("record first"),
    }
    match visit.next_out().expect("send") {
        RequestOut::Send { .. } => {}
        RequestOut::Save(_) => unreachable!("record already taken"),
    }
    assert!(visit.next_out().is_none());
}

#[test]
fn restoration_keeps_keys_and_waits_for_link_scope_and_retention() {
    let mut table = RequestTable::<u64>::new(&limits(), 1);
    let record = RequestRecord { key: [42; 16], scope: 7, first_sent: Some(Wall::from_nanos(10)), request: 5 };
    assert_eq!(table.restore(Token::new(9), 1, record.clone()), Ok(()));
    assert_eq!(table.restore(Token::new(8), 1, record.clone()), Err(record.clone()));
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
        RequestOut::Save(_) => unreachable!("unchanged record already saved"),
    }
    assert_eq!(table.owner(record.key), Some(Token::new(9)));
}

#[test]
fn restored_work_waits_for_retention_and_new_keys_skip_held_collisions() {
    let mut original = RequestTable::<u64>::new(&limits(), 8);
    let key = original.ask(Token::new(1), 7, 1, 10).expect("fits");
    let record = RequestRecord { key, scope: 7, first_sent: None, request: 10 };
    let mut table = RequestTable::<u64>::new(&limits(), 8);
    table.restore(Token::new(1), 1, record).expect("fits");
    table.link(true);
    table.confirmed(Some(7));
    assert!(table.take(Time::ZERO, Wall::EPOCH).next_out().is_none());
    let fresh = table.ask(Token::new(2), 7, 1, 11).expect("fits");
    assert_ne!(fresh, key);
}
