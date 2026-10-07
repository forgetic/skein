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
