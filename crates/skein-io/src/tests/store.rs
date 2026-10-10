//! Conditional whole-file store transitions (io.md, section 5).

use alloc::boxed::Box;

use skein_lib::{Duration, Queue, Time, Token};

use crate::digest::digest;
use crate::file::{Event, Expect, Request};
use crate::file_layer::{self, FileIo};
use crate::kernel::{Complete, Done, Error, Fd, Kind, Op, OpenHow, Stat, Submit};

const OWNER: Token = Token::new(11);
const ROOT: Fd = Fd::new(50);
const PARENT: Fd = Fd::new(51);
const TEMP: Fd = Fd::new(52);
const CHECK: Fd = Fd::new(53);

struct Rig {
    io: FileIo,
    events: Queue<Event>,
    subs: Queue<Submit>,
    root: Token,
}

impl Rig {
    fn new() -> Rig {
        let mut io = FileIo::with_whole_limit(4, 2, 4, 16, Duration::from_secs(1));
        io.seed_randomness(123);
        let root = io.adopt_root(ROOT).expect("room for root");
        Rig { io, events: Queue::with_capacity(2), subs: Queue::with_capacity(2), root }
    }

    fn store(&mut self, expected: Expect) {
        file_layer::down(
            &mut self.io,
            Time::ZERO,
            Request::Store {
                owner: OWNER,
                root: self.root,
                path: Box::from(&b"dir/file"[..]),
                bytes: Box::from(&b"new"[..]),
                expected,
                no_follow: true,
            },
            &mut self.events,
            &mut self.subs,
        );
    }

    fn take(&mut self) -> Submit {
        self.subs.pop().expect("one kernel operation")
    }

    fn complete(&mut self, submit: Submit, result: Result<Done, Error>) {
        file_layer::up(
            &mut self.io,
            Complete { op: submit.op, kind: submit.kind, result },
            &mut self.events,
            &mut self.subs,
        );
    }

    fn read(&mut self, submit: Submit, bytes: &[u8]) {
        let Op::Read { fd, mut buf, at } = submit.kind else { panic!("a read") };
        for (slot, byte) in buf.get_mut(..bytes.len()).expect("read fits").iter_mut().zip(bytes) {
            *slot = *byte;
        }
        let count = u32::try_from(bytes.len()).expect("small test data");
        file_layer::up(
            &mut self.io,
            Complete { op: submit.op, kind: Op::Read { fd, buf, at }, result: Ok(Done::Count(count)) },
            &mut self.events,
            &mut self.subs,
        );
    }

    fn parent_and_absent_old(&mut self) {
        let parent = self.take();
        let Op::Open { root, path, how } = &parent.kind else { panic!("parent open") };
        assert_eq!((*root, path.as_ref(), *how), (ROOT, b"dir".as_slice(), OpenHow::DirectoryNoFollow));
        self.complete(parent, Ok(Done::Fd(PARENT)));
        let old = self.take();
        let Op::Open { root, path, how } = &old.kind else { panic!("old open") };
        assert_eq!((*root, path.as_ref(), *how), (PARENT, b"file".as_slice(), OpenHow::ReadNoFollow));
        self.complete(old, Err(Error::NotFound));
    }

    fn make_and_sync_temp(&mut self) -> Box<[u8]> {
        let first = self.take();
        let Op::Open { root, path, how } = &first.kind else { panic!("temporary create") };
        assert_eq!((*root, *how), (PARENT, OpenHow::CreateNoFollow { mode: Some(0o666) }));
        assert!(path.starts_with(b"file.skein-"));
        let name = path.clone();
        self.complete(first, Ok(Done::Fd(TEMP)));
        let write = self.take();
        let Op::Write { fd, bytes, from, at } = &write.kind else { panic!("temporary write") };
        assert_eq!((*fd, bytes.as_ref(), *from, *at), (TEMP, b"new".as_slice(), 0, 0));
        self.complete(write, Ok(Done::Count(3)));
        let sync = self.take();
        assert_eq!(sync.kind, Op::Sync { fd: TEMP });
        self.complete(sync, Ok(Done::Nothing));
        let close = self.take();
        assert_eq!(close.kind, Op::Close { fd: TEMP });
        self.complete(close, Ok(Done::Nothing));
        name
    }
}

#[test]
fn a_new_file_is_written_synced_rechecked_renamed_and_synced() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    let temporary = rig.make_and_sync_temp();
    let check = rig.take();
    let Op::Open { how, .. } = &check.kind else { panic!("version open") };
    assert_eq!(*how, OpenHow::ReadNoFollow);
    rig.complete(check, Err(Error::NotFound));
    let rename = rig.take();
    let Op::Rename { from_dir, from, to_dir, to } = &rename.kind else { panic!("rename") };
    assert_eq!(
        (*from_dir, from.as_ref(), *to_dir, to.as_ref()),
        (PARENT, temporary.as_ref(), PARENT, b"file".as_slice())
    );
    rig.complete(rename, Ok(Done::Nothing));
    let sync = rig.take();
    assert_eq!(sync.kind, Op::Sync { fd: PARENT });
    rig.complete(sync, Ok(Done::Nothing));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Stored { owner: OWNER, digest: digest(b"new") }));
    assert!(rig.events.is_empty() && rig.subs.is_empty());
    assert_eq!(rig.io.open_files(), 1);
}

#[test]
fn a_changed_file_refuses_the_store_and_removes_its_temporary() {
    let mut rig = Rig::new();
    rig.store(Expect::Digest(digest(b"old")));
    rig.parent_and_absent_old();
    let temporary = rig.make_and_sync_temp();
    let check = rig.take();
    rig.complete(check, Ok(Done::Fd(CHECK)));
    let stat = rig.take();
    assert_eq!(stat.kind, Op::Stat { fd: CHECK });
    rig.complete(stat, Ok(Done::Stat(Stat { kind: Kind::File, size: 3, mode: 0o644, owner: 1000, links: 1 })));
    let first = rig.take();
    rig.read(first, b"ba");
    let second = rig.take();
    rig.read(second, b"d");
    let end = rig.take();
    rig.read(end, b"");
    assert!(rig.events.is_empty());
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: CHECK });
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    let Op::Remove { dir, name, directory } = &remove.kind else { panic!("remove temporary") };
    assert_eq!((*dir, name.as_ref(), *directory), (PARENT, temporary.as_ref(), false));
    rig.complete(remove, Ok(Done::Nothing));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Conflict { owner: OWNER, now: Some(digest(b"bad")) }));
    assert!(rig.events.is_empty() && rig.subs.is_empty());
}

#[test]
fn an_existing_file_conflicts_with_expected_absence_without_reading() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    rig.make_and_sync_temp();
    let check = rig.take();
    rig.complete(check, Ok(Done::Fd(CHECK)));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: CHECK });
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    let Op::Remove { .. } = &remove.kind else {
        panic!("remove temporary");
    };
    rig.complete(remove, Ok(Done::Nothing));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Conflict { owner: OWNER, now: None }));
    assert!(rig.events.is_empty() && rig.subs.is_empty());
}

#[test]
fn a_link_in_a_parent_path_is_refused_before_any_write() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    let parent = rig.take();
    let Op::Open { how, .. } = &parent.kind else { panic!("parent open") };
    assert_eq!(*how, OpenHow::DirectoryNoFollow);
    rig.complete(parent, Err(Error::TooManyLinks));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::TooManyLinks, committed: false, residue: None })
    );
    assert!(rig.subs.is_empty());
}

#[test]
fn an_existing_file_keeps_its_mode_and_matching_content_version() {
    let mut rig = Rig::new();
    rig.store(Expect::Digest(digest(b"old")));
    let parent = rig.take();
    rig.complete(parent, Ok(Done::Fd(PARENT)));
    let old = rig.take();
    rig.complete(old, Ok(Done::Fd(CHECK)));
    let stat = rig.take();
    assert_eq!(stat.kind, Op::Stat { fd: CHECK });
    rig.complete(stat, Ok(Done::Stat(Stat { kind: Kind::File, size: 3, mode: 0o640, owner: 1000, links: 1 })));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: CHECK });
    rig.complete(close, Ok(Done::Nothing));
    let create = rig.take();
    let Op::Open { how, .. } = &create.kind else { panic!("temporary create") };
    assert_eq!(*how, OpenHow::CreateNoFollow { mode: Some(0o640) });
    rig.complete(create, Ok(Done::Fd(TEMP)));
    let write = rig.take();
    rig.complete(write, Ok(Done::Count(3)));
    let sync = rig.take();
    rig.complete(sync, Ok(Done::Nothing));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    let check = rig.take();
    rig.complete(check, Ok(Done::Fd(CHECK)));
    let stat = rig.take();
    rig.complete(stat, Ok(Done::Stat(Stat { kind: Kind::File, size: 3, mode: 0o640, owner: 1000, links: 1 })));
    let read = rig.take();
    rig.read(read, b"ol");
    let read = rig.take();
    rig.read(read, b"d");
    let read = rig.take();
    rig.read(read, b"");
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    let rename = rig.take();
    let Op::Rename { .. } = &rename.kind else { panic!("matching version renames") };
    rig.complete(rename, Ok(Done::Nothing));
    let sync = rig.take();
    rig.complete(sync, Ok(Done::Nothing));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Stored { owner: OWNER, digest: digest(b"new") }));
    assert!(rig.events.is_empty() && rig.subs.is_empty());
}

#[test]
fn a_taken_temporary_name_draws_another_suffix() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    let first = rig.take();
    let Op::Open { path: first_name, .. } = &first.kind else { panic!("first temporary") };
    let first_name = first_name.clone();
    rig.complete(first, Err(Error::Exists));
    let second = rig.take();
    let Op::Open { path: second_name, .. } = &second.kind else { panic!("second temporary") };
    assert_ne!(first_name, *second_name);
    rig.complete(second, Err(Error::Permission));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::Permission, committed: false, residue: None })
    );
    assert!(rig.events.is_empty() && rig.subs.is_empty());
}

#[test]
fn a_rename_settled_at_the_deadline_does_not_remove_the_new_file() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    rig.make_and_sync_temp();
    let check = rig.take();
    rig.complete(check, Err(Error::NotFound));
    let rename = rig.take();
    let Op::Rename { .. } = &rename.kind else { panic!("ready to rename") };
    file_layer::expire(&mut rig.io, Time::from_nanos(1_000_000_000), &mut rig.subs);
    assert!(rig.subs.is_empty());
    rig.complete(rename, Ok(Done::Nothing));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::TimedOut, committed: true, residue: None })
    );
    assert!(rig.events.is_empty() && rig.subs.is_empty());
}

#[test]
fn cancelling_a_store_before_rename_removes_its_temporary_and_reports_cancelled() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    let create = rig.take();
    let Op::Open { path: temp_name, .. } = &create.kind else { panic!("temporary create") };
    let temp_name = temp_name.clone();
    rig.complete(create, Ok(Done::Fd(TEMP)));
    let write = rig.take();
    file_layer::cancel(&mut rig.io, OWNER, &mut rig.subs);
    let cancel = rig.take();
    assert_eq!(cancel.kind, Op::Cancel { target: write.op });
    rig.complete(cancel, Ok(Done::Nothing));
    rig.complete(write, Err(Error::Cancelled));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: TEMP });
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    let Op::Remove { dir, name, directory } = &remove.kind else { panic!("remove temporary") };
    assert_eq!((*dir, name.as_ref(), *directory), (PARENT, temp_name.as_ref(), false));
    rig.complete(remove, Ok(Done::Nothing));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Cancelled { owner: OWNER }));
    assert!(rig.io.takes() && rig.events.is_empty() && rig.subs.is_empty());
}

// A complete old-file path before the commit boundary. The rig checks only
// submitted records and terminals, including the descriptors it must release.
fn old_file_step(rig: &mut Rig, submit: Submit, opened: &mut alloc::collections::BTreeSet<Fd>, temp: &mut bool) {
    match &submit.kind {
        Op::Open { root, how, .. } => {
            let fd = if *root == ROOT {
                PARENT
            } else if let OpenHow::CreateNoFollow { .. } = how {
                TEMP
            } else {
                CHECK
            };
            assert!(opened.insert(fd), "each descriptor opens once before its close");
            if fd == TEMP {
                *temp = true;
            }
            rig.complete(submit, Ok(Done::Fd(fd)));
        }
        Op::Stat { .. } => {
            rig.complete(
                submit,
                Ok(Done::Stat(Stat { kind: Kind::File, size: 3, mode: 0o644, owner: 1000, links: 1 })),
            );
        }
        Op::Write { bytes, from, .. } => {
            let count = u32::try_from(bytes.len()).expect("small test store").checked_sub(*from).expect("valid offset");
            rig.complete(submit, Ok(Done::Count(count)));
        }
        Op::Read { buf, at, .. } => {
            let from = usize::try_from(*at).expect("small old file offset");
            let bytes = b"old".get(from..).expect("read stays through EOF");
            let bytes = bytes.get(..bytes.len().min(buf.len())).expect("read fits buffer");
            rig.read(submit, bytes);
        }
        Op::Close { fd } => {
            assert!(opened.remove(fd), "every close owns an open descriptor");
            rig.complete(submit, Ok(Done::Nothing));
        }
        Op::Remove { .. } => {
            assert!(*temp, "cleanup removes a created temporary");
            *temp = false;
            rig.complete(submit, Ok(Done::Nothing));
        }
        Op::Sync { .. } => rig.complete(submit, Ok(Done::Nothing)),
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Append { .. }
        | Op::Rename { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Cancel { .. } => panic!("the pre-commit rig completes only its known file records"),
    }
}

#[test]
fn every_step_failed_before_or_at_rename_cleans_up_before_one_terminal() {
    for failed_at in 0_u32..15 {
        let mut rig = Rig::new();
        rig.store(Expect::Digest(digest(b"old")));
        let mut opened = alloc::collections::BTreeSet::new();
        let mut temp = false;
        let mut failed = false;
        for index in 0_u32..32 {
            let submit = rig.take();
            if index == failed_at {
                assert!(!failed, "one injected failure per run");
                failed = true;
                if let Op::Close { fd } = submit.kind {
                    assert!(opened.remove(&fd), "a failed Linux close still releases its descriptor");
                }
                rig.complete(submit, Err(Error::Other(5)));
            } else {
                if let Op::Rename { .. } = &submit.kind {
                    panic!("the failure happens before any successful rename");
                }
                old_file_step(&mut rig, submit, &mut opened, &mut temp);
            }
            if !rig.events.is_empty() {
                break;
            }
        }
        assert!(failed && opened.is_empty() && !temp, "step {failed_at}: cleanup precedes the terminal");
        assert_eq!(
            rig.events.pop(),
            Some(Event::Failed { owner: OWNER, error: Error::Other(5), committed: false, residue: None })
        );
        assert!(rig.events.is_empty() && rig.subs.is_empty() && rig.io.takes());
        assert_eq!(rig.io.open_files(), 1, "only the inherited root remains");
        assert_eq!(rig.io.next_deadline(), None);
    }
}

#[test]
fn an_unsubmitted_store_cancel_is_retried_while_the_stalled_write_remains_owned() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    let create = rig.take();
    rig.complete(create, Ok(Done::Fd(TEMP)));
    let write = rig.take();
    file_layer::expire(&mut rig.io, Time::from_nanos(1_000_000_000), &mut rig.subs);
    let cancel = rig.take();
    assert_eq!(cancel.kind, Op::Cancel { target: write.op });
    rig.complete(cancel, Err(Error::Other(12)));
    assert!(rig.events.is_empty() && !rig.io.takes(), "the original write remains outstanding");
    let retry = rig.take();
    assert_eq!(retry.kind, Op::Cancel { target: write.op });
    rig.complete(retry, Ok(Done::Nothing));
    rig.complete(write, Err(Error::Cancelled));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: TEMP });
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    let Op::Remove { .. } = &remove.kind else {
        panic!("temporary removal");
    };
    rig.complete(remove, Ok(Done::Nothing));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::TimedOut, committed: false, residue: None })
    );
    assert!(rig.events.is_empty() && rig.subs.is_empty() && rig.io.takes());
}

#[test]
fn a_late_unsubmitted_store_cancel_never_cancels_the_cleanup_close() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    let create = rig.take();
    rig.complete(create, Ok(Done::Fd(TEMP)));
    let write = rig.take();
    file_layer::expire(&mut rig.io, Time::from_nanos(1_000_000_000), &mut rig.subs);
    let cancel = rig.take();
    rig.complete(write, Err(Error::Cancelled));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: TEMP });
    rig.complete(cancel, Err(Error::Other(12)));
    assert!(rig.subs.is_empty(), "the old cancel never targets a new cleanup operation");
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    rig.complete(remove, Ok(Done::Nothing));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::TimedOut, committed: false, residue: None })
    );
    assert!(rig.events.is_empty() && rig.subs.is_empty() && rig.io.takes());
}

fn renamed(rig: &mut Rig) -> Submit {
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    rig.make_and_sync_temp();
    let check = rig.take();
    rig.complete(check, Err(Error::NotFound));
    let rename = rig.take();
    let Op::Rename { .. } = &rename.kind else {
        panic!("rename");
    };
    rig.complete(rename, Ok(Done::Nothing));
    let sync = rig.take();
    assert_eq!(sync.kind, Op::Sync { fd: PARENT });
    sync
}

#[test]
fn failed_directory_sync_reports_a_committed_replace_after_parent_close() {
    let mut rig = Rig::new();
    let sync = renamed(&mut rig);
    rig.complete(sync, Err(Error::Other(5)));
    assert!(rig.events.is_empty());
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::Other(5), committed: true, residue: None })
    );
    assert!(rig.subs.is_empty() && rig.events.is_empty() && rig.io.takes());
}

#[test]
fn a_hung_directory_sync_reports_a_committed_deadline_after_settlement() {
    let mut rig = Rig::new();
    let sync = renamed(&mut rig);
    file_layer::expire(&mut rig.io, Time::from_nanos(1_000_000_000), &mut rig.subs);
    let cancel = rig.take();
    assert_eq!(cancel.kind, Op::Cancel { target: sync.op });
    rig.complete(cancel, Ok(Done::Nothing));
    rig.complete(sync, Err(Error::Cancelled));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::TimedOut, committed: true, residue: None })
    );
    assert!(rig.subs.is_empty() && rig.events.is_empty() && rig.io.takes());
}

#[test]
fn cancellation_after_rename_waits_for_sync_and_stored() {
    let mut rig = Rig::new();
    let sync = renamed(&mut rig);
    file_layer::cancel(&mut rig.io, OWNER, &mut rig.subs);
    assert!(rig.subs.is_empty());
    rig.complete(sync, Ok(Done::Nothing));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Stored { owner: OWNER, digest: digest(b"new") }));
    assert!(rig.subs.is_empty() && rig.events.is_empty() && rig.io.takes());
}

#[test]
fn a_failed_parent_close_keeps_the_commit_evidence() {
    let mut rig = Rig::new();
    let sync = renamed(&mut rig);
    rig.complete(sync, Ok(Done::Nothing));
    let close = rig.take();
    rig.complete(close, Err(Error::Other(5)));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::Other(5), committed: true, residue: None })
    );
    assert!(rig.subs.is_empty() && rig.events.is_empty() && rig.io.takes());
}

#[test]
fn each_cancellable_pre_rename_phase_settles_at_the_owners_deadline() {
    for stopped_at in [0_u32, 1, 4, 5, 6, 8, 10, 11, 12] {
        let mut rig = Rig::new();
        let mut opened = alloc::collections::BTreeSet::new();
        let mut temp = false;
        rig.store(Expect::Digest(digest(b"old")));
        for _ in 0_u32..stopped_at {
            let submit = rig.take();
            old_file_step(&mut rig, submit, &mut opened, &mut temp);
        }
        let stalled = rig.take();
        let (Op::Open { .. } | Op::Write { .. } | Op::Read { .. } | Op::Sync { .. }) = &stalled.kind else {
            panic!("cancellable store phase");
        };
        file_layer::expire(&mut rig.io, Time::from_nanos(1_000_000_000), &mut rig.subs);
        let cancel = rig.take();
        assert_eq!(cancel.kind, Op::Cancel { target: stalled.op });
        rig.complete(cancel, Ok(Done::Nothing));
        rig.complete(stalled, Err(Error::Cancelled));
        while !rig.subs.is_empty() {
            let submit = rig.take();
            old_file_step(&mut rig, submit, &mut opened, &mut temp);
        }
        assert_eq!(
            rig.events.pop(),
            Some(Event::Failed { owner: OWNER, error: Error::TimedOut, committed: false, residue: None })
        );
        assert!(opened.is_empty() && !temp && rig.events.is_empty() && rig.io.takes());
        assert_eq!(rig.io.open_files(), 1);
    }
}

#[test]
fn an_unconditional_store_uses_default_mode_without_reading_unreadable_or_linked_targets() {
    for error in [Error::Permission, Error::TooManyLinks, Error::Escape, Error::NotAFile] {
        let mut rig = Rig::new();
        rig.store(Expect::Any);
        let parent = rig.take();
        rig.complete(parent, Ok(Done::Fd(PARENT)));
        let old = rig.take();
        rig.complete(old, Err(error));
        rig.make_and_sync_temp();
        let rename = rig.take();
        let Op::Rename { .. } = &rename.kind else {
            panic!("Any proceeds to rename without a recheck");
        };
        rig.complete(rename, Ok(Done::Nothing));
        let sync = rig.take();
        rig.complete(sync, Ok(Done::Nothing));
        let close = rig.take();
        rig.complete(close, Ok(Done::Nothing));
        assert_eq!(rig.events.pop(), Some(Event::Stored { owner: OWNER, digest: digest(b"new") }));
        assert!(rig.events.is_empty() && rig.subs.is_empty() && rig.io.takes());
    }
}

#[test]
fn a_failed_remove_names_the_temporary_even_when_parent_close_also_fails() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    let create = rig.take();
    let Op::Open { path, .. } = &create.kind else {
        panic!("temporary create");
    };
    let name = path.clone();
    rig.complete(create, Ok(Done::Fd(TEMP)));
    let write = rig.take();
    rig.complete(write, Err(Error::Other(5)));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    rig.complete(remove, Err(Error::ReadOnly));
    let close = rig.take();
    assert_eq!(close.kind, Op::Close { fd: PARENT });
    rig.complete(close, Err(Error::Other(5)));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed {
            owner: OWNER,
            error: Error::Other(5),
            committed: false,
            residue: Some(crate::file::Residue { name, error: Error::ReadOnly }),
        })
    );
    assert!(rig.events.is_empty() && rig.subs.is_empty() && rig.io.takes());
}

#[test]
fn a_temporary_already_removed_by_another_owner_has_no_residue() {
    let mut rig = Rig::new();
    rig.store(Expect::Absent);
    rig.parent_and_absent_old();
    let create = rig.take();
    rig.complete(create, Ok(Done::Fd(TEMP)));
    let write = rig.take();
    rig.complete(write, Err(Error::Other(5)));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    rig.complete(remove, Err(Error::NotFound));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(
        rig.events.pop(),
        Some(Event::Failed { owner: OWNER, error: Error::Other(5), committed: false, residue: None })
    );
    assert!(rig.events.is_empty() && rig.subs.is_empty() && rig.io.takes());
}
