use crate::{Journal, JournalLimits, JournalRoom};

fn limits() -> JournalLimits {
    JournalLimits { commits: 2, writes: 2, held: 2, now: 2, release: 2 }
}

#[test]
fn admission_refuses_without_changing_room() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let too_many = JournalRoom { writes: 3, held: 0 };
    let fits = JournalRoom { writes: 2, held: 2 };
    assert!(!journal.takes(&too_many));
    assert!(journal.decision(&too_many).is_none());
    assert!(journal.takes(&fits));
    let decision = journal.decision(&fits).expect("fits");
    assert!(!journal.takes(&fits));
    journal.accept(decision);
    assert!(journal.takes(&fits));
}

#[test]
fn decision_reserves_and_returns_unused_room() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let room = JournalRoom { writes: 2, held: 2 };
    let mut decision = journal.decision(&room).expect("fits");
    assert_eq!(decision.write(10), Ok(()));
    journal.accept(decision);
    assert!(journal.takes(&room));
    let mut next = journal.decision(&room).expect("unused room was returned");
    assert_eq!(next.write(11), Ok(()));
    journal.accept(next);
    assert!(!journal.takes(&JournalRoom { writes: 1, held: 0 }));
}

#[test]
fn one_numbered_commit_per_writing_decision() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let room = JournalRoom { writes: 2, held: 0 };
    let mut first = journal.decision(&room).expect("fits");
    assert_eq!(first.write(3), Ok(()));
    assert_eq!(first.write(4), Ok(()));
    journal.accept(first);
    let empty = journal.decision(&room).expect("fits");
    journal.accept(empty);
    let mut second = journal.decision(&room).expect("fits");
    assert_eq!(second.write(5), Ok(()));
    journal.accept(second);
    let mut first_commit = journal.commit().expect("first commit");
    assert_eq!(first_commit.number, 1);
    assert_eq!(first_commit.writes.pop(), Some(3));
    assert_eq!(first_commit.writes.pop(), Some(4));
    let mut second_commit = journal.commit().expect("second commit");
    assert_eq!(second_commit.number, 2);
    assert_eq!(second_commit.writes.pop(), Some(5));
    assert!(journal.commit().is_none());
}

#[test]
fn overrun_returns_ownership_and_stops_on_accept() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let room = JournalRoom { writes: 0, held: 1 };
    let mut decision = journal.decision(&room).expect("fits");
    assert_eq!(decision.write(7), Err(7));
    assert_eq!(decision.hold(8), Ok(()));
    assert_eq!(decision.hold(9), Err(9));
    assert!(!journal.stopped());
    journal.accept(decision);
    assert!(journal.stopped());
    assert!(!journal.takes(&room));
    assert!(journal.commit().is_none());
}
