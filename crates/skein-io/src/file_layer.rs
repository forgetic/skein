//! The positioned file entity, driven beside socket io.

#![expect(clippy::disallowed_types, reason = "read and directory buffers are bounded by FileIo's limits")]
#![expect(clippy::disallowed_macros, reason = "fixed and limit-sized kernel buffers need initialized storage")]

use crate::file::{Entry, Event, Request};
use crate::kernel::{Complete, Done, Error, Fd, Op, OpenHow, Submit};
use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
use skein_lib::{Duration, Map, Queue, Time, Token};

#[derive(Debug)]
enum Pending {
    Create { owner: Token },
    OpenRead { owner: Token },
    Stat { owner: Token, fd: Fd },
    Cleanup { owner: Token, error: Error },
    Write { owner: Token },
    Read { owner: Token, fd: Fd, offset: u64, max: u32, bytes: Vec<u8> },
    Sync { owner: Token },
    Close { owner: Token },
    Rename { owner: Token },
    Remove { owner: Token },
    ListOpening { owner: Token },
    List { owner: Token, fd: Fd, entries: Vec<Entry> },
    ListClosing { owner: Token, entries: Vec<Entry> },
}

/// A bounded file driver. It takes one request at a time. A request has one
/// terminal event, including when a short transfer takes several kernel ops.
#[derive(Debug)]
pub struct FileIo {
    files: Map<Token, Fd>,
    next_file: u64,
    next_op: u64,
    pending: Option<Pending>,
    outstanding: Option<Token>,
    max_read: u32,
    max_entries: u32,
    timeout: Duration,
    deadline: Option<Time>,
    cancel_op: Option<Token>,
    expired: bool,
}

impl FileIo {
    #[must_use]
    pub fn new(files: u32, max_read: u32, max_entries: u32, timeout: Duration) -> FileIo {
        assert!(files > 0 && max_read > 0 && max_entries > 0 && timeout.as_nanos() > 0, "file limits need room");
        FileIo {
            files: Map::with_capacity(files),
            next_file: 1,
            next_op: 1,
            pending: None,
            outstanding: None,
            max_read,
            max_entries,
            timeout,
            deadline: None,
            cancel_op: None,
            expired: false,
        }
    }

    #[must_use]
    pub const fn takes(&self) -> bool {
        self.pending.is_none() && self.cancel_op.is_none()
    }

    #[must_use]
    pub fn open_files(&self) -> u32 {
        self.files.len()
    }

    #[must_use]
    pub const fn next_deadline(&self) -> Option<Time> {
        self.deadline
    }

    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        match self.deadline {
            Some(deadline) => deadline <= now,
            None => false,
        }
    }

    fn issue(&mut self, op: Op, subs: &mut Queue<Submit>) {
        assert!(self.outstanding.is_none(), "one kernel operation per file request");
        assert!(op.is_valid(), "file request makes a valid kernel record");
        let token = Token::new(self.next_op);
        self.next_op = self.next_op.checked_add(1).expect("operation tokens do not wrap");
        self.outstanding = Some(token);
        subs.push(Submit { op: token, kind: op });
    }

    fn file(&self, token: Token) -> Option<Fd> {
        self.files.get(&token).copied()
    }

    fn insert(&mut self, fd: Fd) -> Option<Token> {
        let token = Token::new(self.next_file);
        self.next_file = self.next_file.checked_add(1).expect("file tokens do not wrap");
        self.files.insert(token, fd).ok()?;
        Some(token)
    }

    fn list(&mut self, root: Fd, subs: &mut Queue<Submit>) {
        self.issue(
            Op::List {
                fd: root,
                entries: vec![crate::kernel::Entry::BLANK; 32].into_boxed_slice(),
                names: vec![0_u8; 8192].into_boxed_slice(),
            },
            subs,
        );
    }
}

/// One file request, only while `FileIo::takes` is true.
pub fn down(io: &mut FileIo, now: Time, request: Request, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    assert!(io.takes(), "a file request completes before the next begins");
    io.deadline = Some(now.saturating_add(io.timeout));
    down_inner(io, request, events, subs);
    if io.pending.is_none() {
        io.deadline = None;
    }
}

fn down_inner(io: &mut FileIo, request: Request, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    match request {
        Request::Create { owner, root, name, mode } => {
            if name.contains(&0) || mode & !crate::kernel::PERMISSIONS != 0 {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            if io.files.len() == io.files.capacity() {
                events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
                return;
            }
            io.pending = Some(Pending::Create { owner });
            io.issue(Op::Open { root, path: name, how: OpenHow::Create { mode: Some(mode) } }, subs);
        }
        Request::OpenRead { owner, root, name } => {
            if name.contains(&0) {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            if io.files.len() == io.files.capacity() {
                events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
                return;
            }
            io.pending = Some(Pending::OpenRead { owner });
            io.issue(Op::Open { root, path: name, how: OpenHow::Read }, subs);
        }
        Request::WriteAt { owner, file, offset, bytes } => {
            let Some(fd) = io.file(file) else {
                events.push(Event::Failed { owner, error: Error::NotFound });
                return;
            };
            if bytes.is_empty() {
                events.push(Event::Written { owner });
                return;
            }
            let Ok(op) = Op::write(fd, bytes, 0, offset) else {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            };
            io.pending = Some(Pending::Write { owner });
            io.issue(op, subs);
        }
        Request::ReadAt { owner, file, offset, max } => {
            let Some(fd) = io.file(file) else {
                events.push(Event::Failed { owner, error: Error::NotFound });
                return;
            };
            if max == 0 || max > io.max_read {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            let buffer = vec![0_u8; usize::try_from(max).expect("u32 fits usize")].into_boxed_slice();
            let Ok(op) = Op::read(fd, buffer, offset) else {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            };
            io.pending = Some(Pending::Read { owner, fd, offset, max, bytes: Vec::new() });
            io.issue(op, subs);
        }
        Request::Sync { owner, file } => {
            let Some(fd) = io.file(file) else {
                events.push(Event::Failed { owner, error: Error::NotFound });
                return;
            };
            io.pending = Some(Pending::Sync { owner });
            io.issue(Op::Sync { fd }, subs);
        }
        Request::Close { owner, file } => {
            let Some(fd) = io.files.remove(&file) else {
                events.push(Event::Failed { owner, error: Error::NotFound });
                return;
            };
            io.pending = Some(Pending::Close { owner });
            io.issue(Op::Close { fd }, subs);
        }
        Request::SyncDirectory { owner, root } => {
            io.pending = Some(Pending::Sync { owner });
            io.issue(Op::Sync { fd: root }, subs);
        }
        Request::Rename { owner, root, from, to } => {
            if !crate::kernel::is_name(&from) || !crate::kernel::is_name(&to) {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            io.pending = Some(Pending::Rename { owner });
            io.issue(Op::Rename { from_dir: root, from, to_dir: root, to }, subs);
        }
        Request::Remove { owner, root, name } => {
            if !crate::kernel::is_name(&name) {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            io.pending = Some(Pending::Remove { owner });
            io.issue(Op::Remove { dir: root, name, directory: false }, subs);
        }
        Request::List { owner, root } => {
            io.pending = Some(Pending::ListOpening { owner });
            io.issue(Op::Open { root, path: Box::from(&b"."[..]), how: OpenHow::Directory }, subs);
        }
    }
}

/// Cancels the outstanding kernel operation after its request deadline. The
/// terminal failure is emitted when the target has settled, exactly once.
pub fn expire(io: &mut FileIo, now: Time, subs: &mut Queue<Submit>) {
    if !io.is_due(now) || io.expired {
        return;
    }
    let Some(target) = io.outstanding else {
        return;
    };
    io.expired = true;
    io.deadline = None;
    let op = Token::new(io.next_op);
    io.next_op = io.next_op.checked_add(1).expect("operation tokens do not wrap");
    io.cancel_op = Some(op);
    subs.push(Submit { op, kind: Op::Cancel { target } });
}

/// One kernel completion. It may submit the continuation of a short transfer.
pub fn up(io: &mut FileIo, complete: Complete, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    if io.cancel_op == Some(complete.op) {
        io.cancel_op = None;
        return;
    }
    assert!(io.outstanding == Some(complete.op), "completion names the outstanding file operation");
    io.outstanding = None;
    if io.expired {
        timed_out(io, complete, events, subs);
    } else {
        up_inner(io, complete, events, subs);
    }
    if io.pending.is_none() {
        io.deadline = None;
    }
}

fn timed_out(io: &mut FileIo, complete: Complete, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    io.expired = false;
    let pending = io.pending.take().expect("a timed out completion has a request");
    let owner = match &pending {
        Pending::Create { owner }
        | Pending::OpenRead { owner }
        | Pending::Stat { owner, .. }
        | Pending::Cleanup { owner, .. }
        | Pending::Write { owner }
        | Pending::Read { owner, .. }
        | Pending::Sync { owner }
        | Pending::Close { owner }
        | Pending::Rename { owner }
        | Pending::Remove { owner }
        | Pending::ListOpening { owner }
        | Pending::List { owner, .. }
        | Pending::ListClosing { owner, .. } => *owner,
    };
    let fd = match (pending, complete.result) {
        (Pending::Create { .. } | Pending::OpenRead { .. } | Pending::ListOpening { .. }, Ok(Done::Fd(fd)))
        | (Pending::Stat { fd, .. } | Pending::List { fd, .. }, _) => Some(fd),
        _ => None,
    };
    if let Some(fd) = fd {
        io.pending = Some(Pending::Cleanup { owner, error: Error::TimedOut });
        io.issue(Op::Close { fd }, subs);
    } else {
        events.push(Event::Failed { owner, error: Error::TimedOut });
    }
}

#[expect(clippy::too_many_lines, reason = "one exhaustive completion transition handles every file operation")]
fn up_inner(io: &mut FileIo, complete: Complete, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let pending = io.pending.take().expect("a completion has a request");
    if let Err(error) = complete.result {
        let owner = match pending {
            Pending::Stat { owner, fd } | Pending::List { owner, fd, .. } => {
                io.pending = Some(Pending::Cleanup { owner, error });
                io.issue(Op::Close { fd }, subs);
                return;
            }
            Pending::Cleanup { owner, error } => {
                events.push(Event::Failed { owner, error });
                return;
            }
            Pending::Create { owner }
            | Pending::OpenRead { owner }
            | Pending::Write { owner }
            | Pending::Read { owner, .. }
            | Pending::Sync { owner }
            | Pending::Close { owner }
            | Pending::Rename { owner }
            | Pending::Remove { owner }
            | Pending::ListOpening { owner }
            | Pending::ListClosing { owner, .. } => owner,
        };
        events.push(Event::Failed { owner, error });
        return;
    }
    match (pending, complete.kind, complete.result.expect("checked success")) {
        (Pending::Create { owner }, Op::Open { .. }, Done::Fd(fd)) => {
            if let Some(file) = io.insert(fd) {
                events.push(Event::Opened { owner, file, len: 0 });
            } else {
                events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
            }
        }
        (Pending::OpenRead { owner }, Op::Open { .. }, Done::Fd(fd)) => {
            io.pending = Some(Pending::Stat { owner, fd });
            io.issue(Op::Stat { fd }, subs);
        }
        (Pending::Stat { owner, fd }, Op::Stat { .. }, Done::Stat(stat)) => {
            if let Some(file) = io.insert(fd) {
                events.push(Event::Opened { owner, file, len: stat.size });
            } else {
                events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
            }
        }
        (Pending::Cleanup { owner, error }, Op::Close { .. }, Done::Nothing) => {
            events.push(Event::Failed { owner, error });
        }
        (Pending::ListOpening { owner }, Op::Open { .. }, Done::Fd(fd)) => {
            io.pending = Some(Pending::List { owner, fd, entries: Vec::new() });
            io.list(fd, subs);
        }
        (Pending::Write { owner }, Op::Write { fd, bytes, from, at }, Done::Count(count)) => {
            let next = from.checked_add(count).expect("write count fits");
            if count == 0 {
                events.push(Event::Failed { owner, error: Error::Other(5) });
            } else if usize::try_from(next).expect("u32 fits usize") == bytes.len() {
                events.push(Event::Written { owner });
            } else {
                let at = at.checked_add(u64::from(count)).expect("write offset fits");
                io.pending = Some(Pending::Write { owner });
                let Ok(op) = Op::write(fd, bytes, next, at) else {
                    io.pending = None;
                    events.push(Event::Failed { owner, error: Error::InvalidArgument });
                    return;
                };
                io.issue(op, subs);
            }
        }
        (Pending::Read { owner, fd, offset, max, mut bytes }, Op::Read { buf, .. }, Done::Count(count)) => {
            let n = usize::try_from(count).expect("u32 fits usize");
            bytes.extend_from_slice(buf.get(..n).expect("read count is within buffer"));
            let done = count == 0 || u32::try_from(bytes.len()).expect("read no more than max") == max;
            if done {
                events.push(Event::Read { owner, bytes: bytes.into_boxed_slice() });
            } else {
                let at = offset.checked_add(u64::try_from(bytes.len()).expect("length fits u64")).expect("offset fits");
                let left = max.checked_sub(u32::try_from(bytes.len()).expect("length fits u32")).expect("within max");
                io.pending = Some(Pending::Read { owner, fd, offset, max, bytes });
                let buffer = vec![0_u8; usize::try_from(left).expect("u32 fits usize")].into_boxed_slice();
                let Ok(op) = Op::read(fd, buffer, at) else {
                    io.pending = None;
                    events.push(Event::Failed { owner, error: Error::InvalidArgument });
                    return;
                };
                io.issue(op, subs);
            }
        }
        (Pending::Sync { owner }, Op::Sync { .. }, Done::Nothing) => events.push(Event::Synced { owner }),
        (Pending::Close { owner }, Op::Close { .. }, Done::Nothing) => events.push(Event::Closed { owner }),
        (Pending::Rename { owner }, Op::Rename { .. }, Done::Nothing) => events.push(Event::Renamed { owner }),
        (Pending::Remove { owner }, Op::Remove { .. }, Done::Nothing) => events.push(Event::Removed { owner }),
        (Pending::List { owner, fd, mut entries }, Op::List { entries: records, names, .. }, Done::Count(count)) => {
            let count = usize::try_from(count).expect("u32 fits usize");
            if count == 0 {
                io.pending = Some(Pending::ListClosing { owner, entries });
                io.issue(Op::Close { fd }, subs);
                return;
            }
            for record in records.get(..count).expect("list count is within buffer") {
                let name = record.name(&names).expect("a listed entry names bytes");
                entries.push(Entry { name: Box::from(name) });
            }
            if entries.len() > usize::try_from(io.max_entries).expect("u32 fits usize") {
                io.pending = Some(Pending::Cleanup { owner, error: Error::NoBufferSpace });
                io.issue(Op::Close { fd }, subs);
            } else {
                io.pending = Some(Pending::List { owner, fd, entries });
                io.list(fd, subs);
            }
        }
        (Pending::ListClosing { owner, entries }, Op::Close { .. }, Done::Nothing) => {
            events.push(Event::Listed { owner, entries: entries.into_boxed_slice() });
        }
        (_, _, _) => unreachable!("kernel validates completion shape"),
    }
}
