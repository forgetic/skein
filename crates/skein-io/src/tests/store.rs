//! Conditional whole-file store transitions (io.md, section 5).

use alloc::boxed::Box;

use skein_lib::{Duration, Queue, Time, Token};

use crate::digest::digest;
use crate::file::{Event, Request};
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

    fn store(&mut self, expected: Option<crate::digest::Digest>) {
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
    rig.store(None);
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
    rig.store(Some(digest(b"old")));
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
fn an_existing_large_file_conflicts_with_expected_absence_and_reports_its_digest() {
    let mut rig = Rig::new();
    rig.store(None);
    rig.parent_and_absent_old();
    rig.make_and_sync_temp();
    let check = rig.take();
    rig.complete(check, Ok(Done::Fd(CHECK)));
    let stat = rig.take();
    rig.complete(stat, Ok(Done::Stat(Stat { kind: Kind::File, size: 17, mode: 0o644, owner: 1000, links: 1 })));
    let content = b"abcdefghijklmnopq";
    for chunk in content.chunks(2) {
        let read = rig.take();
        rig.read(read, chunk);
    }
    let end = rig.take();
    rig.read(end, b"");
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    let remove = rig.take();
    let Op::Remove { .. } = remove.kind else { panic!("remove temporary") };
    rig.complete(remove, Ok(Done::Nothing));
    let close = rig.take();
    rig.complete(close, Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Conflict { owner: OWNER, now: Some(digest(content)) }));
}

#[test]
fn a_link_in_a_parent_path_is_refused_before_any_write() {
    let mut rig = Rig::new();
    rig.store(None);
    let parent = rig.take();
    let Op::Open { how, .. } = &parent.kind else { panic!("parent open") };
    assert_eq!(*how, OpenHow::DirectoryNoFollow);
    rig.complete(parent, Err(Error::TooManyLinks));
    assert_eq!(rig.events.pop(), Some(Event::Failed { owner: OWNER, error: Error::TooManyLinks }));
    assert!(rig.subs.is_empty());
}

#[test]
fn an_existing_file_keeps_its_mode_and_matching_content_version() {
    let mut rig = Rig::new();
    rig.store(Some(digest(b"old")));
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
    rig.store(None);
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
    assert_eq!(rig.events.pop(), Some(Event::Failed { owner: OWNER, error: Error::Permission }));
    assert!(rig.events.is_empty() && rig.subs.is_empty());
}

#[test]
fn a_rename_settled_at_the_deadline_does_not_remove_the_new_file() {
    let mut rig = Rig::new();
    rig.store(None);
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
    assert_eq!(rig.events.pop(), Some(Event::Failed { owner: OWNER, error: Error::TimedOut }));
    assert!(rig.events.is_empty() && rig.subs.is_empty());
}

#[test]
fn cancelling_a_store_before_rename_removes_its_temporary_and_reports_cancelled() {
    let mut rig = Rig::new();
    rig.store(None);
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
