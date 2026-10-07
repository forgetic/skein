//! Root admission and file metadata through the file driver (io.md, section 5).

#![expect(clippy::wildcard_enum_match_arm, reason = "unexpected test events are reported with their values")]

use alloc::boxed::Box;

use skein_lib::{Duration, Queue, Time, Token};

use crate::file::{Event, Request};
use crate::file_layer::{self, FileIo};
use crate::kernel::{Complete, Done, Fd, Kind, Op, OpenHow, Stat, Submit};

const OWNER: Token = Token::new(7);
const ROOT: Fd = Fd::new(41);
const CHILD: Fd = Fd::new(42);

fn io() -> FileIo {
    FileIo::new(3, 16, 4, Duration::from_secs(1))
}

#[test]
fn file_driver_worst_case_counts_each_configured_limit() {
    let base = FileIo::worst_case(1, 8, 1, 8).expect("small file limits fit");
    assert!(FileIo::worst_case(32, 8, 1, 8).expect("more slots fit") > base);
    assert!(FileIo::worst_case(1, 9, 1, 8).expect("larger reads fit") > base);
    assert!(FileIo::worst_case(1, 8, 2, 8).expect("more entries fit") > base);
    assert!(FileIo::worst_case(1, 8, 1, 9).expect("larger files fit") > base);
    assert_eq!(FileIo::worst_case(0, 8, 1, 8), None);
    assert_eq!(FileIo::worst_case(1, 0, 1, 8), None);
    assert_eq!(FileIo::worst_case(1, 8, 0, 8), None);
    assert_eq!(FileIo::worst_case(1, 8, 1, 0), None);
}

fn complete(
    io: &mut FileIo,
    submit: Submit,
    result: Result<Done, crate::kernel::Error>,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    file_layer::up(io, Complete { op: submit.op, kind: submit.kind, result }, events, subs);
}

fn read(io: &mut FileIo, submit: Submit, bytes: &[u8], events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let Op::Read { fd, mut buf, at } = submit.kind else { panic!("a read") };
    for (slot, byte) in buf.get_mut(..bytes.len()).expect("read fits the buffer").iter_mut().zip(bytes) {
        *slot = *byte;
    }
    let kind = Op::Read { fd, buf, at };
    file_layer::up(
        io,
        Complete {
            op: submit.op,
            kind,
            result: Ok(Done::Count(u32::try_from(bytes.len()).expect("short test input"))),
        },
        events,
        subs,
    );
}

fn listed(
    io: &mut FileIo,
    submit: Submit,
    names_to_list: &[&[u8]],
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let Op::List { fd, mut entries, mut names } = submit.kind else { panic!("a list") };
    let mut offset = 0_usize;
    for (index, name) in names_to_list.iter().enumerate() {
        let end = offset.checked_add(name.len()).expect("test names fit");
        for (position, byte) in name.iter().enumerate() {
            let at = offset.checked_add(position).expect("test name position fits");
            names[at] = *byte;
        }
        entries[index] = crate::kernel::Entry {
            kind: Kind::File,
            start: u32::try_from(offset).expect("small test offset"),
            len: u32::try_from(name.len()).expect("small test name"),
        };
        offset = end;
    }
    file_layer::up(
        io,
        Complete {
            op: submit.op,
            kind: Op::List { fd, entries, names },
            result: Ok(Done::Count(u32::try_from(names_to_list.len()).expect("small test batch"))),
        },
        events,
        subs,
    );
}

#[test]
fn adopted_root_and_open_directory_keep_tokens_inside_io() {
    let mut io = io();
    let root = io.adopt_root(ROOT).expect("room for a root");
    assert_eq!(io.descriptor(root), Some(ROOT));

    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::OpenDirectory { owner: OWNER, root: ROOT, name: Box::from(&b"child"[..]), no_follow: true },
        &mut events,
        &mut subs,
    );
    let submit = subs.pop().expect("one open");
    let Op::Open { how, .. } = &submit.kind else { panic!("an open") };
    assert_eq!(*how, OpenHow::DirectoryNoFollow);
    file_layer::up(
        &mut io,
        Complete { op: submit.op, kind: submit.kind, result: Ok(Done::Fd(CHILD)) },
        &mut events,
        &mut subs,
    );
    let child = match events.pop().expect("one terminal") {
        Event::Opened { owner: OWNER, file, len: 0 } => file,
        other => panic!("unexpected event: {other:?}"),
    };
    assert_eq!(io.descriptor(child), Some(CHILD));

    file_layer::down(&mut io, Time::ZERO, Request::Stat { owner: OWNER, file: child }, &mut events, &mut subs);
    let submit = subs.pop().expect("one stat");
    let Op::Stat { fd } = &submit.kind else { panic!("a stat") };
    assert_eq!(*fd, CHILD);
    let stat = Stat { kind: Kind::Directory, size: 0, mode: 0o755 };
    file_layer::up(
        &mut io,
        Complete { op: submit.op, kind: submit.kind, result: Ok(Done::Stat(stat)) },
        &mut events,
        &mut subs,
    );
    assert_eq!(events.pop(), Some(Event::Stated { owner: OWNER, stat }));
}

#[test]
fn read_and_create_no_follow_use_the_safe_kernel_modes() {
    let mut io = io();
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::OpenReadNoFollow { owner: OWNER, root: ROOT, name: Box::from(&b"file"[..]) },
        &mut events,
        &mut subs,
    );
    let submit = subs.pop().expect("one open");
    let Op::Open { how, .. } = &submit.kind else { panic!("an open") };
    assert_eq!(*how, OpenHow::ReadNoFollow);
    file_layer::up(
        &mut io,
        Complete { op: submit.op, kind: submit.kind, result: Err(crate::kernel::Error::NotFound) },
        &mut events,
        &mut subs,
    );
    assert_eq!(events.pop(), Some(Event::Failed { owner: OWNER, error: crate::kernel::Error::NotFound }));

    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::CreateNoFollow { owner: OWNER, root: ROOT, name: Box::from(&b"file"[..]), mode: 0o600 },
        &mut events,
        &mut subs,
    );
    let submit: Submit = subs.pop().expect("one create");
    let Op::Open { how, .. } = submit.kind else { panic!("an open") };
    assert_eq!(how, OpenHow::CreateNoFollow { mode: Some(0o600) });
}

#[test]
fn a_whole_load_continues_short_reads_and_closes_before_its_terminal() {
    let mut io = FileIo::with_whole_limit(3, 2, 4, 8, Duration::from_secs(1));
    let root = io.adopt_root(ROOT).expect("room for the root");
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::Load { owner: OWNER, root, path: Box::from(&b"f"[..]), max: 4, no_follow: true },
        &mut events,
        &mut subs,
    );
    let open = subs.pop().expect("open");
    let Op::Open { how, .. } = &open.kind else { panic!("an open") };
    assert_eq!(*how, OpenHow::ReadNoFollow);
    complete(&mut io, open, Ok(Done::Fd(CHILD)), &mut events, &mut subs);
    let stat = subs.pop().expect("stat");
    complete(&mut io, stat, Ok(Done::Stat(Stat { kind: Kind::File, size: 3, mode: 0o644 })), &mut events, &mut subs);
    read(&mut io, subs.pop().expect("first read"), b"ab", &mut events, &mut subs);
    read(&mut io, subs.pop().expect("second read"), b"c", &mut events, &mut subs);
    read(&mut io, subs.pop().expect("end read"), b"", &mut events, &mut subs);
    assert!(events.is_empty(), "the close must finish first");
    let close = subs.pop().expect("close");
    complete(&mut io, close, Ok(Done::Nothing), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::Loaded { owner: OWNER, bytes: Box::from(&b"abc"[..]) }));
    assert!(io.takes());
    assert_eq!(io.open_files(), 1, "only the root stays open");
}

#[test]
fn a_whole_load_refuses_a_file_over_its_bound_after_closing() {
    let mut io = FileIo::with_whole_limit(3, 2, 4, 8, Duration::from_secs(1));
    let root = io.adopt_root(ROOT).expect("room for the root");
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::Load { owner: OWNER, root, path: Box::from(&b"f"[..]), max: 3, no_follow: false },
        &mut events,
        &mut subs,
    );
    complete(&mut io, subs.pop().expect("open"), Ok(Done::Fd(CHILD)), &mut events, &mut subs);
    complete(
        &mut io,
        subs.pop().expect("stat"),
        Ok(Done::Stat(Stat { kind: Kind::File, size: 4, mode: 0o644 })),
        &mut events,
        &mut subs,
    );
    assert!(events.is_empty());
    complete(&mut io, subs.pop().expect("close"), Ok(Done::Nothing), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::TooLarge { owner: OWNER, size: 4 }));
}

#[test]
fn a_whole_scan_returns_entries_or_a_bound_refusal_after_close() {
    for max in [0, 1] {
        let mut io = io();
        let root = io.adopt_root(ROOT).expect("room for the root");
        let mut events = Queue::with_capacity(2);
        let mut subs = Queue::with_capacity(2);
        file_layer::down(
            &mut io,
            Time::ZERO,
            Request::Scan { owner: OWNER, root, path: Box::from(&b"dir"[..]), max, max_bytes: 100, no_follow: true },
            &mut events,
            &mut subs,
        );
        let open = subs.pop().expect("open");
        let Op::Open { how, .. } = &open.kind else { panic!("an open") };
        assert_eq!(*how, OpenHow::DirectoryNoFollow);
        complete(&mut io, open, Ok(Done::Fd(CHILD)), &mut events, &mut subs);
        let list = subs.pop().expect("list");
        let Op::List { fd, mut entries, mut names } = list.kind else { panic!("a list") };
        entries[0] = crate::kernel::Entry { kind: Kind::File, start: 0, len: 1 };
        names[0] = b'f';
        file_layer::up(
            &mut io,
            Complete { op: list.op, kind: Op::List { fd, entries, names }, result: Ok(Done::Count(1)) },
            &mut events,
            &mut subs,
        );
        complete(&mut io, subs.pop().expect("end listing"), Ok(Done::Count(0)), &mut events, &mut subs);
        assert!(events.is_empty());
        complete(&mut io, subs.pop().expect("close"), Ok(Done::Nothing), &mut events, &mut subs);
        let entries = if max == 0 {
            Box::from([])
        } else {
            Box::from([crate::file::Entry { name: Box::from(&b"f"[..]), kind: Kind::File }])
        };
        assert_eq!(events.pop(), Some(Event::Scanned { owner: OWNER, entries, more: u64::from(max == 0) }));
    }
}

#[test]
fn a_scan_keeps_the_name_order_prefix_across_batches_and_counts_omissions() {
    let mut io = io();
    let root = io.adopt_root(ROOT).expect("room for root");
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    let entry_bytes = u64::try_from(size_of::<crate::file::Entry>()).expect("entry cell fits u64");
    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::Scan {
            owner: OWNER,
            root,
            path: Box::from(&b"dir"[..]),
            max: 3,
            max_bytes: entry_bytes.checked_mul(2).expect("two cells fit").checked_add(2).expect("two names fit"),
            no_follow: true,
        },
        &mut events,
        &mut subs,
    );
    complete(&mut io, subs.pop().expect("open"), Ok(Done::Fd(CHILD)), &mut events, &mut subs);
    listed(&mut io, subs.pop().expect("first batch"), &[b"z", b"c"], &mut events, &mut subs);
    listed(&mut io, subs.pop().expect("second batch"), &[b"b", b"a"], &mut events, &mut subs);
    listed(&mut io, subs.pop().expect("end"), &[], &mut events, &mut subs);
    assert!(events.is_empty());
    complete(&mut io, subs.pop().expect("close"), Ok(Done::Nothing), &mut events, &mut subs);
    assert_eq!(
        events.pop(),
        Some(Event::Scanned {
            owner: OWNER,
            entries: Box::from([
                crate::file::Entry { name: Box::from(&b"a"[..]), kind: Kind::File },
                crate::file::Entry { name: Box::from(&b"b"[..]), kind: Kind::File },
            ]),
            more: 2,
        })
    );
}

#[test]
fn an_unfit_first_scan_entry_makes_the_prefix_empty() {
    let mut io = io();
    let root = io.adopt_root(ROOT).expect("room for root");
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    let entry_bytes = u64::try_from(size_of::<crate::file::Entry>()).expect("entry cell fits u64");
    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::Scan {
            owner: OWNER,
            root,
            path: Box::from(&b"dir"[..]),
            max: 2,
            max_bytes: entry_bytes + 1,
            no_follow: true,
        },
        &mut events,
        &mut subs,
    );
    complete(&mut io, subs.pop().expect("open"), Ok(Done::Fd(CHILD)), &mut events, &mut subs);
    listed(&mut io, subs.pop().expect("batch"), &[b"b", b"aa"], &mut events, &mut subs);
    listed(&mut io, subs.pop().expect("end"), &[], &mut events, &mut subs);
    complete(&mut io, subs.pop().expect("close"), Ok(Done::Nothing), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::Scanned { owner: OWNER, entries: Box::from([]), more: 2 }));
}

#[test]
fn a_deadline_waits_for_uncancellable_stat_then_closes_the_file() {
    let mut io = FileIo::with_whole_limit(3, 2, 4, 8, Duration::from_secs(1));
    let root = io.adopt_root(ROOT).expect("room for the root");
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down(
        &mut io,
        Time::ZERO,
        Request::Load { owner: OWNER, root, path: Box::from(&b"f"[..]), max: 4, no_follow: false },
        &mut events,
        &mut subs,
    );
    complete(&mut io, subs.pop().expect("open"), Ok(Done::Fd(CHILD)), &mut events, &mut subs);
    let stat = subs.pop().expect("stat");
    file_layer::expire(&mut io, Time::ZERO.saturating_add(Duration::from_secs(1)), &mut subs);
    assert!(subs.is_empty(), "stat is not cancellable");
    complete(&mut io, stat, Ok(Done::Stat(Stat { kind: Kind::File, size: 1, mode: 0o644 })), &mut events, &mut subs);
    assert!(events.is_empty());
    complete(&mut io, subs.pop().expect("cleanup close"), Ok(Done::Nothing), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::Failed { owner: OWNER, error: crate::kernel::Error::TimedOut }));
}

#[test]
fn each_file_request_uses_its_own_absolute_deadline() {
    let mut io = io();
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    let deadline = Time::from_nanos(7_000_000_000);
    file_layer::down_until(
        &mut io,
        deadline,
        Request::OpenRead { owner: OWNER, root: ROOT, name: Box::from(&b"file"[..]) },
        &mut events,
        &mut subs,
    );
    assert_eq!(io.next_deadline(), Some(deadline));
    let open = subs.pop().expect("open in flight");
    file_layer::expire(&mut io, Time::from_nanos(1_000_000_000), &mut subs);
    assert!(subs.is_empty());
    file_layer::expire(&mut io, deadline, &mut subs);
    let cancel = subs.pop().expect("deadline cancels open");
    assert_eq!(cancel.kind, Op::Cancel { target: open.op });
    complete(&mut io, cancel, Ok(Done::Nothing), &mut events, &mut subs);
    complete(&mut io, open, Err(crate::kernel::Error::Cancelled), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::Failed { owner: OWNER, error: crate::kernel::Error::TimedOut }));
    assert!(io.takes());
}

#[test]
fn an_owner_cancel_waits_for_the_target_and_reports_when_it_stopped() {
    let mut io = io();
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down_until(
        &mut io,
        Time::from_nanos(9_000_000_000),
        Request::OpenRead { owner: OWNER, root: ROOT, name: Box::from(&b"file"[..]) },
        &mut events,
        &mut subs,
    );
    let open = subs.pop().expect("open in flight");
    file_layer::cancel(&mut io, Token::new(99), &mut subs);
    assert!(subs.is_empty(), "another owner cannot cancel the request");
    file_layer::cancel(&mut io, OWNER, &mut subs);
    file_layer::cancel(&mut io, OWNER, &mut subs);
    let cancel = subs.pop().expect("one cancel");
    assert_eq!(cancel.kind, Op::Cancel { target: open.op });
    complete(&mut io, open, Err(crate::kernel::Error::Cancelled), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::Cancelled { owner: OWNER }));
    assert!(!io.takes(), "the cancel completion still holds its slot");
    complete(&mut io, cancel, Ok(Done::Nothing), &mut events, &mut subs);
    assert!(io.takes());
    assert!(events.is_empty());
}

#[test]
fn a_successful_direct_write_reports_written_when_cancel_was_too_late() {
    let mut io = io();
    let file = io.adopt_root(ROOT).expect("room for file");
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down_until(
        &mut io,
        Time::from_nanos(9_000_000_000),
        Request::WriteAt { owner: OWNER, file, offset: 0, bytes: Box::from(&b"hi"[..]) },
        &mut events,
        &mut subs,
    );
    let write = subs.pop().expect("write in flight");
    file_layer::cancel(&mut io, OWNER, &mut subs);
    let cancel = subs.pop().expect("cancel submitted");
    complete(&mut io, cancel, Err(crate::kernel::Error::TooLate), &mut events, &mut subs);
    complete(&mut io, write, Ok(Done::Count(2)), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::Written { owner: OWNER }));
    assert!(io.takes() && events.is_empty());
}

#[test]
fn a_short_write_can_be_cancelled_before_its_remaining_bytes() {
    let mut io = io();
    let file = io.adopt_root(ROOT).expect("room for file");
    let mut events = Queue::with_capacity(2);
    let mut subs = Queue::with_capacity(2);
    file_layer::down_until(
        &mut io,
        Time::from_nanos(9_000_000_000),
        Request::WriteAt { owner: OWNER, file, offset: 0, bytes: Box::from(&b"hi"[..]) },
        &mut events,
        &mut subs,
    );
    let write = subs.pop().expect("write in flight");
    file_layer::cancel(&mut io, OWNER, &mut subs);
    let cancel = subs.pop().expect("cancel submitted");
    complete(&mut io, write, Ok(Done::Count(1)), &mut events, &mut subs);
    assert_eq!(events.pop(), Some(Event::Cancelled { owner: OWNER }));
    assert!(subs.is_empty(), "the remainder is not written");
    complete(&mut io, cancel, Err(crate::kernel::Error::TooLate), &mut events, &mut subs);
    assert!(io.takes());
}
