//! Conditional atomic replacement beside an opened parent directory (io.md, section 5).
//!
//! A store keeps the parent and temporary descriptors, the expected digest,
//! and one kernel operation at a time. It never knows a workspace's policy.
//! `start`, `up`, and `timed_out` are called only by the file layer. A
//! terminal follows the cleanup of every descriptor and temporary entry.

#![expect(clippy::disallowed_types, reason = "the version check holds at most FileIo's whole-file limit")]

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;

use skein_lib::{Queue, Token};

use super::{FileIo, Pending};
use crate::digest::{Digest, digest};
use crate::file::Event;
use crate::kernel::{Complete, Done, Error, Fd, Kind, Op, OpenHow, Submit, is_name};

const ATTEMPTS: u8 = 8;

type SplitPath = (Box<[u8]>, Box<[u8]>);

#[derive(Clone, Copy, Debug)]
enum Phase {
    ParentOpening,
    OldOpening,
    OldStating,
    OldClosing,
    TempCreating,
    TempWriting,
    TempSyncing,
    TempClosing,
    CheckOpening,
    CheckStating,
    CheckReading,
    CheckClosing,
    Renaming,
    DirectorySyncing,
    ParentClosing,
    CleanupCheck,
    CleanupOld,
    CleanupTemp,
    CleanupRemove,
    CleanupParent,
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Kernel(Error),
    Conflict,
}

#[derive(Debug)]
pub(super) struct Store {
    owner: Token,
    phase: Phase,
    parent: Option<Fd>,
    old: Option<Fd>,
    temporary: Option<Fd>,
    check: Option<Fd>,
    target: Box<[u8]>,
    temp_name: Option<Box<[u8]>>,
    temp_exists: bool,
    bytes: Option<Box<[u8]>>,
    expected: Option<Digest>,
    produced: Digest,
    no_follow: bool,
    mode: u32,
    attempts: u8,
    seen: Vec<u8>,
    failure: Option<Failure>,
}

impl Store {
    pub(super) fn can_cancel(&self) -> bool {
        match self.phase {
            Phase::ParentOpening
            | Phase::OldOpening
            | Phase::TempCreating
            | Phase::TempWriting
            | Phase::TempSyncing
            | Phase::CheckOpening
            | Phase::CheckReading
            | Phase::DirectorySyncing => true,
            Phase::OldStating
            | Phase::OldClosing
            | Phase::TempClosing
            | Phase::CheckStating
            | Phase::CheckClosing
            | Phase::Renaming
            | Phase::ParentClosing
            | Phase::CleanupCheck
            | Phase::CleanupOld
            | Phase::CleanupTemp
            | Phase::CleanupRemove
            | Phase::CleanupParent => false,
        }
    }
}

fn split_path(path: &[u8]) -> Option<SplitPath> {
    let mut slash = None;
    for index in 0..path.len() {
        if path.get(index) == Some(&b'/') {
            slash = Some(index);
        }
    }
    match slash {
        Some(0) => None,
        Some(index) => {
            let parent = path.get(..index)?;
            let target = path.get(index.checked_add(1)?..)?;
            if is_name(target) { Some((Box::from(parent), Box::from(target))) } else { None }
        }
        None => {
            if is_name(path) {
                Some((Box::from(&b"."[..]), Box::from(path)))
            } else {
                None
            }
        }
    }
}

#[expect(clippy::too_many_arguments, reason = "the store request carries its complete bounded input")]
pub(super) fn start(
    io: &mut FileIo,
    owner: Token,
    root: Token,
    path: Box<[u8]>,
    bytes: Box<[u8]>,
    expected: Option<Digest>,
    no_follow: bool,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let Some(root_fd) = io.file(root) else {
        events.push(Event::Failed { owner, error: Error::NotFound });
        return;
    };
    if io.random.is_none()
        || bytes.len() > usize::try_from(io.max_file).expect("u32 fits usize")
        || path.len() >= 4096
        || path.contains(&0)
    {
        events.push(Event::Failed { owner, error: Error::InvalidArgument });
        return;
    }
    let Some((parent_path, target)) = split_path(&path) else {
        events.push(Event::Failed { owner, error: Error::InvalidArgument });
        return;
    };
    let produced = digest(&bytes);
    let store = Store {
        owner,
        phase: Phase::ParentOpening,
        parent: None,
        old: None,
        temporary: None,
        check: None,
        target,
        temp_name: None,
        temp_exists: false,
        bytes: Some(bytes),
        expected,
        produced,
        no_follow,
        mode: 0o666,
        attempts: 0,
        seen: Vec::new(),
        failure: None,
    };
    let how = if no_follow { OpenHow::DirectoryNoFollow } else { OpenHow::Directory };
    issue(io, store, Op::Open { root: root_fd, path: parent_path, how }, subs);
}

fn issue(io: &mut FileIo, store: Store, op: Op, subs: &mut Queue<Submit>) {
    io.pending = Some(Pending::Store(store));
    io.issue(op, subs);
}

fn parent(store: &Store) -> Fd {
    store.parent.expect("a store has opened its parent")
}

fn temp_name(io: &mut FileIo, target: &[u8]) -> Box<[u8]> {
    let random = io.random.expect("a store has a seed").wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    io.random = Some(random);
    let prefix = target.get(..target.len().min(230)).expect("bounded prefix");
    let mut name = Vec::with_capacity(prefix.len().checked_add(23).expect("temporary name fits"));
    name.extend_from_slice(prefix);
    name.extend_from_slice(b".skein-");
    for index in 0..16_u32 {
        let shift = 60_u32.checked_sub(index.checked_mul(4).expect("hex position fits")).expect("hex position fits");
        let digit = u8::try_from((random >> shift) & 0xf).expect("hex digit");
        let ascii = if digit < 10 {
            b'0'.checked_add(digit).expect("decimal digit")
        } else {
            b'a'.checked_add(digit.checked_sub(10).expect("hex letter")).expect("hex letter")
        };
        name.push(ascii);
    }
    name.into_boxed_slice()
}

fn create_temp(io: &mut FileIo, mut store: Store, subs: &mut Queue<Submit>) {
    let name = temp_name(io, &store.target);
    store.temp_name = Some(name.clone());
    store.attempts = store.attempts.checked_add(1).expect("bounded attempts");
    store.phase = Phase::TempCreating;
    let how = if store.no_follow {
        OpenHow::CreateNoFollow { mode: Some(store.mode) }
    } else {
        OpenHow::Create { mode: Some(store.mode) }
    };
    let root = parent(&store);
    issue(io, store, Op::Open { root, path: name, how }, subs);
}

fn open_old(io: &mut FileIo, mut store: Store, subs: &mut Queue<Submit>) {
    store.phase = Phase::OldOpening;
    let how = if store.no_follow { OpenHow::ReadNoFollow } else { OpenHow::Read };
    let root = parent(&store);
    let path = store.target.clone();
    issue(io, store, Op::Open { root, path, how }, subs);
}

fn open_check(io: &mut FileIo, mut store: Store, subs: &mut Queue<Submit>) {
    store.phase = Phase::CheckOpening;
    let how = if store.no_follow { OpenHow::ReadNoFollow } else { OpenHow::Read };
    let root = parent(&store);
    let path = store.target.clone();
    issue(io, store, Op::Open { root, path, how }, subs);
}

fn rename(io: &mut FileIo, mut store: Store, subs: &mut Queue<Submit>) {
    store.phase = Phase::Renaming;
    let dir = parent(&store);
    let from = store.temp_name.as_ref().expect("a temporary was created").clone();
    let to = store.target.clone();
    issue(io, store, Op::Rename { from_dir: dir, from, to_dir: dir, to }, subs);
}

fn read_check(io: &mut FileIo, mut store: Store, subs: &mut Queue<Submit>) {
    let used = u32::try_from(store.seen.len()).expect("bounded version check");
    let left = io.max_file.checked_sub(used).expect("bounded version check");
    let asked = io.max_read.min(left.saturating_add(1));
    let buffer = vec![0_u8; usize::try_from(asked).expect("u32 fits usize")].into_boxed_slice();
    let fd = store.check.expect("the checked file is open");
    store.phase = Phase::CheckReading;
    let op = Op::read(fd, buffer, u64::from(used)).expect("positive bounded read");
    issue(io, store, op, subs);
}

fn terminal(store: Store, events: &mut Queue<Event>) {
    match store.failure.expect("cleanup has a failure") {
        Failure::Kernel(error) => events.push(Event::Failed { owner: store.owner, error }),
        Failure::Conflict => events.push(Event::Conflict { owner: store.owner }),
    }
}

fn cleanup(io: &mut FileIo, mut store: Store, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    store.bytes = None;
    store.seen = Vec::new();
    if let Some(fd) = store.check {
        store.phase = Phase::CleanupCheck;
        issue(io, store, Op::Close { fd }, subs);
    } else if let Some(fd) = store.old {
        store.phase = Phase::CleanupOld;
        issue(io, store, Op::Close { fd }, subs);
    } else if let Some(fd) = store.temporary {
        store.phase = Phase::CleanupTemp;
        issue(io, store, Op::Close { fd }, subs);
    } else if store.temp_exists {
        let dir = parent(&store);
        let name = store.temp_name.as_ref().expect("an existing temporary has a name").clone();
        store.phase = Phase::CleanupRemove;
        issue(io, store, Op::Remove { dir, name, directory: false }, subs);
    } else if let Some(fd) = store.parent {
        store.phase = Phase::CleanupParent;
        issue(io, store, Op::Close { fd }, subs);
    } else {
        terminal(store, events);
    }
}

fn fail(io: &mut FileIo, mut store: Store, failure: Failure, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    store.failure = Some(failure);
    io.deadline = None;
    cleanup(io, store, events, subs);
}

fn finish_cleanup(
    io: &mut FileIo,
    mut store: Store,
    result: Result<Done, Error>,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match store.phase {
        Phase::CleanupCheck => store.check = None,
        Phase::CleanupOld => store.old = None,
        Phase::CleanupTemp => store.temporary = None,
        Phase::CleanupRemove => store.temp_exists = false,
        Phase::CleanupParent => store.parent = None,
        Phase::ParentOpening
        | Phase::OldOpening
        | Phase::OldStating
        | Phase::OldClosing
        | Phase::TempCreating
        | Phase::TempWriting
        | Phase::TempSyncing
        | Phase::TempClosing
        | Phase::CheckOpening
        | Phase::CheckStating
        | Phase::CheckReading
        | Phase::CheckClosing
        | Phase::Renaming
        | Phase::DirectorySyncing
        | Phase::ParentClosing => unreachable!("a cleanup completion has a cleanup phase"),
    }
    if let Err(error) = result {
        store.failure = Some(Failure::Kernel(error));
    }
    cleanup(io, store, events, subs);
}

pub(super) fn up(
    io: &mut FileIo,
    store: Store,
    complete: Complete,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match store.phase {
        Phase::CleanupCheck | Phase::CleanupOld | Phase::CleanupTemp | Phase::CleanupRemove | Phase::CleanupParent => {
            finish_cleanup(io, store, complete.result, events, subs);
        }
        Phase::ParentOpening
        | Phase::OldOpening
        | Phase::OldStating
        | Phase::OldClosing
        | Phase::TempCreating
        | Phase::TempWriting
        | Phase::TempSyncing
        | Phase::TempClosing
        | Phase::CheckOpening
        | Phase::CheckStating
        | Phase::CheckReading
        | Phase::CheckClosing
        | Phase::Renaming
        | Phase::DirectorySyncing
        | Phase::ParentClosing => match complete.result {
            Ok(done) => success(io, store, complete.kind, done, events, subs),
            Err(error) => failed_operation(io, store, error, events, subs),
        },
    }
}

fn failed_operation(
    io: &mut FileIo,
    mut store: Store,
    error: Error,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match store.phase {
        Phase::OldOpening => {
            if error == Error::NotFound || error == Error::Permission {
                create_temp(io, store, subs);
            } else {
                fail(io, store, Failure::Kernel(error), events, subs);
            }
        }
        Phase::TempCreating => {
            if error == Error::Exists && store.attempts < ATTEMPTS {
                create_temp(io, store, subs);
            } else {
                fail(io, store, Failure::Kernel(error), events, subs);
            }
        }
        Phase::CheckOpening => {
            if error == Error::NotFound {
                match store.expected {
                    None => rename(io, store, subs),
                    Some(_) => fail(io, store, Failure::Conflict, events, subs),
                }
            } else {
                fail(io, store, Failure::Kernel(error), events, subs);
            }
        }
        Phase::OldClosing => {
            store.old = None;
            fail(io, store, Failure::Kernel(error), events, subs);
        }
        Phase::TempClosing => {
            store.temporary = None;
            fail(io, store, Failure::Kernel(error), events, subs);
        }
        Phase::CheckClosing => {
            store.check = None;
            fail(io, store, Failure::Kernel(error), events, subs);
        }
        Phase::ParentClosing => {
            store.parent = None;
            fail(io, store, Failure::Kernel(error), events, subs);
        }
        Phase::ParentOpening
        | Phase::OldStating
        | Phase::TempWriting
        | Phase::TempSyncing
        | Phase::CheckStating
        | Phase::CheckReading
        | Phase::Renaming
        | Phase::DirectorySyncing => fail(io, store, Failure::Kernel(error), events, subs),
        Phase::CleanupCheck | Phase::CleanupOld | Phase::CleanupTemp | Phase::CleanupRemove | Phase::CleanupParent => {
            unreachable!("cleanup completion handled separately")
        }
    }
}

pub(super) fn timed_out(
    io: &mut FileIo,
    mut store: Store,
    complete: Complete,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match complete.result {
        Ok(Done::Fd(fd)) => match store.phase {
            Phase::ParentOpening => store.parent = Some(fd),
            Phase::OldOpening => store.old = Some(fd),
            Phase::TempCreating => {
                store.temporary = Some(fd);
                store.temp_exists = true;
            }
            Phase::CheckOpening => store.check = Some(fd),
            Phase::OldStating
            | Phase::OldClosing
            | Phase::TempWriting
            | Phase::TempSyncing
            | Phase::TempClosing
            | Phase::CheckStating
            | Phase::CheckReading
            | Phase::CheckClosing
            | Phase::Renaming
            | Phase::DirectorySyncing
            | Phase::ParentClosing
            | Phase::CleanupCheck
            | Phase::CleanupOld
            | Phase::CleanupTemp
            | Phase::CleanupRemove
            | Phase::CleanupParent => unreachable!("only an open returns a descriptor"),
        },
        Ok(
            Done::Nothing
            | Done::Count(_)
            | Done::Stat(_)
            | Done::Accepted { .. }
            | Done::Bound(_)
            | Done::Spawned { .. }
            | Done::Exit(_),
        )
        | Err(_) => {}
    }
    match store.phase {
        Phase::OldClosing => store.old = None,
        Phase::TempClosing => store.temporary = None,
        Phase::CheckClosing => store.check = None,
        Phase::Renaming => {
            if complete.result.is_ok() {
                store.temp_exists = false;
            }
        }
        Phase::ParentClosing => store.parent = None,
        Phase::ParentOpening
        | Phase::OldOpening
        | Phase::OldStating
        | Phase::TempCreating
        | Phase::TempWriting
        | Phase::TempSyncing
        | Phase::CheckOpening
        | Phase::CheckStating
        | Phase::CheckReading
        | Phase::DirectorySyncing
        | Phase::CleanupCheck
        | Phase::CleanupOld
        | Phase::CleanupTemp
        | Phase::CleanupRemove
        | Phase::CleanupParent => {}
    }
    fail(io, store, Failure::Kernel(Error::TimedOut), events, subs);
}

fn done_fd(done: Done) -> Fd {
    match done {
        Done::Fd(fd) => fd,
        Done::Nothing
        | Done::Count(_)
        | Done::Accepted { .. }
        | Done::Bound(_)
        | Done::Stat(_)
        | Done::Spawned { .. }
        | Done::Exit(_) => unreachable!("an open returns a descriptor"),
    }
}

fn done_stat(done: Done) -> crate::kernel::Stat {
    match done {
        Done::Stat(stat) => stat,
        Done::Nothing
        | Done::Count(_)
        | Done::Fd(_)
        | Done::Accepted { .. }
        | Done::Bound(_)
        | Done::Spawned { .. }
        | Done::Exit(_) => unreachable!("a stat returns metadata"),
    }
}

fn done_count(done: Done) -> u32 {
    match done {
        Done::Count(count) => count,
        Done::Nothing
        | Done::Fd(_)
        | Done::Accepted { .. }
        | Done::Bound(_)
        | Done::Stat(_)
        | Done::Spawned { .. }
        | Done::Exit(_) => unreachable!("a transfer returns a count"),
    }
}

fn done_nothing(done: Done) {
    match done {
        Done::Nothing => {}
        Done::Count(_)
        | Done::Fd(_)
        | Done::Accepted { .. }
        | Done::Bound(_)
        | Done::Stat(_)
        | Done::Spawned { .. }
        | Done::Exit(_) => unreachable!("this operation returns nothing"),
    }
}

fn write_parts(kind: Op) -> (Fd, Box<[u8]>, u32, u64) {
    match kind {
        Op::Write { fd, bytes, from, at } => (fd, bytes, from, at),
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Open { .. }
        | Op::Read { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Cancel { .. } => unreachable!("a store write completes a write"),
    }
}

fn read_buffer(kind: Op) -> Box<[u8]> {
    match kind {
        Op::Read { buf, .. } => buf,
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Open { .. }
        | Op::Write { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Cancel { .. } => unreachable!("a version read completes a read"),
    }
}

#[expect(clippy::too_many_lines, reason = "each store phase handles its own one kernel completion")]
fn success(
    io: &mut FileIo,
    mut store: Store,
    kind: Op,
    done: Done,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match store.phase {
        Phase::ParentOpening => {
            store.parent = Some(done_fd(done));
            open_old(io, store, subs);
        }
        Phase::OldOpening => {
            let fd = done_fd(done);
            store.old = Some(fd);
            store.phase = Phase::OldStating;
            issue(io, store, Op::Stat { fd }, subs);
        }
        Phase::OldStating => {
            let stat = done_stat(done);
            if stat.kind != Kind::File {
                fail(io, store, Failure::Kernel(Error::IsADirectory), events, subs);
                return;
            }
            store.mode = stat.mode;
            store.phase = Phase::OldClosing;
            let fd = store.old.expect("old file opened");
            issue(io, store, Op::Close { fd }, subs);
        }
        Phase::OldClosing => {
            done_nothing(done);
            store.old = None;
            create_temp(io, store, subs);
        }
        Phase::TempCreating => {
            let fd = done_fd(done);
            store.temporary = Some(fd);
            store.temp_exists = true;
            let bytes = store.bytes.take().expect("store bytes held until temporary opens");
            if bytes.is_empty() {
                store.phase = Phase::TempSyncing;
                issue(io, store, Op::Sync { fd }, subs);
            } else {
                store.phase = Phase::TempWriting;
                let op = Op::write(fd, bytes, 0, 0).expect("bounded nonempty store bytes");
                issue(io, store, op, subs);
            }
        }
        Phase::TempWriting => {
            let count = done_count(done);
            let (fd, bytes, from, at) = write_parts(kind);
            if count == 0 {
                fail(io, store, Failure::Kernel(Error::Other(5)), events, subs);
                return;
            }
            let next = from.checked_add(count).expect("write count fits");
            if usize::try_from(next).expect("u32 fits usize") == bytes.len() {
                store.phase = Phase::TempSyncing;
                issue(io, store, Op::Sync { fd }, subs);
            } else {
                let offset = at.checked_add(u64::from(count)).expect("bounded offset");
                let op = Op::write(fd, bytes, next, offset).expect("short write continues inside bytes");
                issue(io, store, op, subs);
            }
        }
        Phase::TempSyncing => {
            done_nothing(done);
            store.phase = Phase::TempClosing;
            let fd = store.temporary.expect("temporary is open");
            issue(io, store, Op::Close { fd }, subs);
        }
        Phase::TempClosing => {
            done_nothing(done);
            store.temporary = None;
            open_check(io, store, subs);
        }
        Phase::CheckOpening => {
            let fd = done_fd(done);
            store.check = Some(fd);
            if store.expected.is_none() {
                store.failure = Some(Failure::Conflict);
                io.deadline = None;
                store.phase = Phase::CheckClosing;
                issue(io, store, Op::Close { fd }, subs);
            } else {
                store.phase = Phase::CheckStating;
                issue(io, store, Op::Stat { fd }, subs);
            }
        }
        Phase::CheckStating => {
            let stat = done_stat(done);
            if stat.kind != Kind::File || stat.size > u64::from(io.max_file) {
                store.failure = Some(Failure::Conflict);
                io.deadline = None;
                store.phase = Phase::CheckClosing;
                let fd = store.check.expect("checked file is open");
                issue(io, store, Op::Close { fd }, subs);
            } else {
                let capacity =
                    usize::try_from(io.max_file).expect("u32 fits usize").checked_add(1).expect("one more byte fits");
                store.seen = Vec::with_capacity(capacity);
                read_check(io, store, subs);
            }
        }
        Phase::CheckReading => {
            let count = done_count(done);
            let buf = read_buffer(kind);
            let n = usize::try_from(count).expect("u32 fits usize");
            store.seen.extend_from_slice(buf.get(..n).expect("read count within buffer"));
            if store.seen.len() > usize::try_from(io.max_file).expect("u32 fits usize")
                || (count == 0 && Some(digest(&store.seen)) != store.expected)
            {
                store.failure = Some(Failure::Conflict);
            }
            if store.failure.is_some() || count == 0 {
                if store.failure.is_some() {
                    io.deadline = None;
                }
                store.seen = Vec::new();
                store.phase = Phase::CheckClosing;
                let fd = store.check.expect("checked file is open");
                issue(io, store, Op::Close { fd }, subs);
            } else {
                read_check(io, store, subs);
            }
        }
        Phase::CheckClosing => {
            done_nothing(done);
            store.check = None;
            match store.failure {
                Some(_) => cleanup(io, store, events, subs),
                None => rename(io, store, subs),
            }
        }
        Phase::Renaming => {
            done_nothing(done);
            store.temp_exists = false;
            store.phase = Phase::DirectorySyncing;
            let fd = parent(&store);
            issue(io, store, Op::Sync { fd }, subs);
        }
        Phase::DirectorySyncing => {
            done_nothing(done);
            store.phase = Phase::ParentClosing;
            let fd = parent(&store);
            issue(io, store, Op::Close { fd }, subs);
        }
        Phase::ParentClosing => {
            done_nothing(done);
            store.parent = None;
            events.push(Event::Stored { owner: store.owner, digest: store.produced });
        }
        Phase::CleanupCheck | Phase::CleanupOld | Phase::CleanupTemp | Phase::CleanupRemove | Phase::CleanupParent => {
            unreachable!("cleanup handled separately")
        }
    }
}
