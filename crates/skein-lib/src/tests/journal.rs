use crate::{Journal, JournalLimits, JournalRoom, Queue, Released};

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

#[test]
fn held_outputs_wait_for_their_commit_and_leave_in_order() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let mut out = Queue::with_capacity(4);
    let mut first = journal.decision(&JournalRoom { writes: 1, held: 1 }).expect("fits");
    assert_eq!(first.write(1), Ok(()));
    assert_eq!(first.hold(10), Ok(()));
    journal.accept(first);
    let mut second = journal.decision(&JournalRoom { writes: 1, held: 1 }).expect("fits");
    assert_eq!(second.write(2), Ok(()));
    assert_eq!(second.hold(20), Ok(()));
    journal.accept(second);
    assert_eq!(journal.release(&mut out), Released::None);
    assert_eq!(journal.commit().expect("first").number, 1);
    assert_eq!(journal.commit().expect("second").number, 2);
    journal.committed(1);
    assert_eq!(journal.release(&mut out), Released::Some);
    assert_eq!(out.pop(), Some(10));
    assert_eq!(out.pop(), None);
    journal.committed(2);
    assert_eq!(journal.release(&mut out), Released::Some);
    assert_eq!(out.pop(), Some(20));
    assert!(!journal.stopped());
}

#[test]
fn no_write_follows_last_commit_or_is_ready_at_zero() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let mut out = Queue::with_capacity(3);
    let mut initial = journal.decision(&JournalRoom { writes: 0, held: 1 }).expect("fits");
    assert_eq!(initial.hold(1), Ok(()));
    journal.accept(initial);
    assert_eq!(journal.release(&mut out), Released::Some);
    assert_eq!(out.pop(), Some(1));
    let mut writing = journal.decision(&JournalRoom { writes: 1, held: 0 }).expect("fits");
    assert_eq!(writing.write(7), Ok(()));
    journal.accept(writing);
    let mut following = journal.decision(&JournalRoom { writes: 0, held: 1 }).expect("fits");
    assert_eq!(following.hold(2), Ok(()));
    journal.accept(following);
    assert_eq!(journal.release(&mut out), Released::None);
    assert_eq!(journal.commit().expect("commit").number, 1);
    journal.committed(1);
    assert_eq!(journal.release(&mut out), Released::Some);
    assert_eq!(out.pop(), Some(2));
}

#[test]
fn door_is_bounded_and_release_is_bounded_per_call() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let mut out = Queue::with_capacity(4);
    assert_eq!(journal.now(1), Ok(()));
    assert_eq!(journal.now(2), Ok(()));
    assert_eq!(journal.now(3), Err(3));
    assert_eq!(journal.release(&mut out), Released::Some);
    assert_eq!(out.pop(), Some(1));
    assert_eq!(out.pop(), Some(2));
    let mut decision = journal.decision(&JournalRoom { writes: 0, held: 2 }).expect("fits");
    assert_eq!(decision.hold(4), Ok(()));
    assert_eq!(decision.hold(5), Ok(()));
    journal.accept(decision);
    assert_eq!(journal.now(3), Ok(()));
    assert_eq!(journal.release(&mut out), Released::Some);
    assert_eq!(out.pop(), Some(3));
    assert_eq!(out.pop(), Some(4));
    assert_eq!(journal.release(&mut out), Released::Some);
    assert_eq!(out.pop(), Some(5));
}

#[test]
fn cumulative_answer_frees_commit_room() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let room = JournalRoom { writes: 1, held: 0 };
    for write in [1, 2] {
        let mut decision = journal.decision(&room).expect("fits");
        assert_eq!(decision.write(write), Ok(()));
        journal.accept(decision);
    }
    assert!(!journal.takes(&room));
    assert_eq!(journal.commit().expect("first").number, 1);
    assert_eq!(journal.commit().expect("second").number, 2);
    journal.committed(2);
    assert!(journal.takes(&room));
}

#[test]
fn failure_stops_release_and_admission() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let mut out = Queue::with_capacity(2);
    let room = JournalRoom { writes: 1, held: 1 };
    let mut decision = journal.decision(&room).expect("fits");
    assert_eq!(decision.write(1), Ok(()));
    assert_eq!(decision.hold(10), Ok(()));
    journal.accept(decision);
    assert_eq!(journal.commit().expect("commit").number, 1);
    journal.failed(1);
    assert!(journal.stopped());
    assert_eq!(journal.release(&mut out), Released::Stopped);
    assert_eq!(out.pop(), None);
    assert!(!journal.takes(&room));
    assert_eq!(journal.now(2), Err(2));
}

#[test]
fn answer_for_unsent_or_old_commit_stops() {
    let mut journal = Journal::<u8, u8>::new(&limits());
    let mut decision = journal.decision(&JournalRoom { writes: 1, held: 0 }).expect("fits");
    assert_eq!(decision.write(1), Ok(()));
    journal.accept(decision);
    journal.committed(1);
    assert!(journal.stopped());

    let mut another = Journal::<u8, u8>::new(&limits());
    let mut decision = another.decision(&JournalRoom { writes: 1, held: 0 }).expect("fits");
    assert_eq!(decision.write(1), Ok(()));
    another.accept(decision);
    assert_eq!(another.commit().expect("commit").number, 1);
    another.committed(1);
    another.committed(1);
    assert!(another.stopped());
}
