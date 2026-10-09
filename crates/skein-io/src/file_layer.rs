//! Bounded file operations over kernel records (io.md, section 5).
//!
//! `FileIo` keeps open file and root descriptors behind tokens, one request
//! in flight, its deadline, and bounded bytes or entries while a whole-file
//! operation progresses. It does not know a workspace's writable policy or
//! interpret a file's content or version. `down` accepts one request while
//! `takes` is true; `up` handles one completion; `expire` settles a deadline.
//! Each admitted request produces one terminal event after owned descriptors
//! are closed. Loads refuse excess bytes; scans retain a name-order prefix
//! within their entry and byte caps and count all omitted entries.

#![expect(clippy::disallowed_types, reason = "read and directory buffers are bounded by FileIo's limits")]
#![expect(clippy::disallowed_macros, reason = "fixed and limit-sized kernel buffers need initialized storage")]
#![expect(clippy::large_enum_variant, reason = "FileIo holds one pending store without a separate allocation")]

use crate::file::{Entry, Event, Request};
use crate::kernel::{Complete, Done, Error, Fd, Op, OpenHow, Submit};
use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
use skein_lib::{Duration, Map, Queue, Time, Token};

mod store;

#[derive(Debug)]
enum Pending {
    Create {
        owner: Token,
    },
    OpenRead {
        owner: Token,
    },
    OpenDirectory {
        owner: Token,
    },
    Stat {
        owner: Token,
        fd: Fd,
    },
    Stating {
        owner: Token,
    },
    Cleanup {
        owner: Token,
        error: Error,
    },
    Write {
        owner: Token,
    },
    Read {
        owner: Token,
        fd: Fd,
        offset: u64,
        max: u32,
        bytes: Vec<u8>,
    },
    Sync {
        owner: Token,
    },
    Close {
        owner: Token,
    },
    Rename {
        owner: Token,
    },
    Remove {
        owner: Token,
    },
    ListOpening {
        owner: Token,
    },
    List {
        owner: Token,
        fd: Fd,
        entries: Vec<Entry>,
    },
    ListClosing {
        owner: Token,
        entries: Vec<Entry>,
    },
    LoadOpening {
        owner: Token,
        max: u32,
    },
    LoadStating {
        owner: Token,
        fd: Fd,
        max: u32,
    },
    LoadReading {
        owner: Token,
        fd: Fd,
        max: u32,
        bytes: Vec<u8>,
    },
    LoadClosing {
        owner: Token,
        bytes: Vec<u8>,
    },
    LoadTooLargeClosing {
        owner: Token,
        size: u64,
    },
    ScanOpening {
        owner: Token,
        max: u32,
        max_bytes: u64,
    },
    ScanListing {
        owner: Token,
        fd: Fd,
        max: u32,
        max_bytes: u64,
        entries: Vec<Entry>,
        bytes: u64,
        cutoff: Option<Box<[u8]>>,
        total: u64,
    },
    ScanClosing {
        owner: Token,
        entries: Vec<Entry>,
        more: u64,
    },
    Store(store::Store),
}

#[derive(Clone, Copy, Debug)]
enum Stop {
    Deadline,
    Cancel,
}

impl Pending {
    fn owner(&self) -> Token {
        match self {
            Pending::Create { owner }
            | Pending::OpenRead { owner }
            | Pending::OpenDirectory { owner }
            | Pending::Stat { owner, .. }
            | Pending::Stating { owner }
            | Pending::Cleanup { owner, .. }
            | Pending::Write { owner }
            | Pending::Read { owner, .. }
            | Pending::Sync { owner }
            | Pending::Close { owner }
            | Pending::Rename { owner }
            | Pending::Remove { owner }
            | Pending::ListOpening { owner }
            | Pending::List { owner, .. }
            | Pending::ListClosing { owner, .. }
            | Pending::LoadOpening { owner, .. }
            | Pending::LoadStating { owner, .. }
            | Pending::LoadReading { owner, .. }
            | Pending::LoadClosing { owner, .. }
            | Pending::LoadTooLargeClosing { owner, .. }
            | Pending::ScanOpening { owner, .. }
            | Pending::ScanListing { owner, .. }
            | Pending::ScanClosing { owner, .. } => *owner,
            Pending::Store(store) => store.owner(),
        }
    }

    fn can_cancel(&self) -> bool {
        match self {
            Pending::Create { .. }
            | Pending::OpenRead { .. }
            | Pending::OpenDirectory { .. }
            | Pending::Write { .. }
            | Pending::Read { .. }
            | Pending::Sync { .. }
            | Pending::ListOpening { .. }
            | Pending::LoadOpening { .. }
            | Pending::LoadReading { .. }
            | Pending::ScanOpening { .. } => true,
            Pending::Stat { .. }
            | Pending::Stating { .. }
            | Pending::Cleanup { .. }
            | Pending::Close { .. }
            | Pending::Rename { .. }
            | Pending::Remove { .. }
            | Pending::List { .. }
            | Pending::ListClosing { .. }
            | Pending::LoadStating { .. }
            | Pending::LoadClosing { .. }
            | Pending::LoadTooLargeClosing { .. }
            | Pending::ScanListing { .. }
            | Pending::ScanClosing { .. } => false,
            Pending::Store(store) => store.can_cancel(),
        }
    }
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
    max_file: u32,
    max_entries: u32,
    timeout: Duration,
    deadline: Option<Time>,
    cancel_op: Option<Token>,
    stop: Option<Stop>,
    random: Option<u64>,
}

impl FileIo {
    /// The most heap retained by a file driver with these limits, including
    /// its file map, one pending request, the file buffers in its kernel
    /// records, and transient whole-file and directory buffers
    /// (programming-model.md, section 6.3; io.md, section 5).
    ///
    /// The caller accounts for buffers it supplies in requests, including
    /// `WriteAt` bytes and paths passed to simple file operations. The bound
    /// counts buffers made by the driver and the admitted `Store`, `Load`,
    /// and `Scan` requests. It returns `None` when arithmetic overflows or
    /// a limit cannot be used by the driver.
    #[must_use]
    pub fn worst_case(files: u32, max_read: u32, max_entries: u32, max_file: u32) -> Option<u64> {
        if files == 0 || max_read == 0 || max_entries == 0 || max_file == 0 {
            return None;
        }

        let file_map = Map::<Token, Fd>::worst_case(files)?;

        // A list can append a full kernel batch before rejecting excess
        // entries. A scan can insert one transient entry into a vector
        // initially reserved for max_entries + 1. Allow a doubled vector
        // capacity for either growth path, and names for all live entries.
        let entries = u64::from(max_entries).checked_add(32)?;
        let entry_cells = entries.checked_mul(2)?.checked_mul(u64::try_from(size_of::<Entry>()).ok()?)?;
        let entry_names = entries.checked_add(1)?.checked_mul(u64::try_from(crate::kernel::LONGEST_NAME).ok()?)?;

        // A whole-file load may hold max_file + 1 bytes while checking for
        // overflow, beside one max_read kernel buffer. A store can hold its
        // content in the pending request or the write record, and can also
        // keep a max_read recheck buffer. Count both envelopes together so
        // they cover every transition, including cancellation.
        let whole_files = u64::from(max_file).checked_mul(2)?.checked_add(1)?;
        let read_buffers = u64::from(max_read).checked_mul(3)?;

        // One listing record carries fixed entry and name arrays. Its
        // completion returns those same arrays; a cancel has no payload.
        let kernel_listing =
            u64::try_from(size_of::<crate::kernel::Entry>()).ok()?.checked_mul(32)?.checked_add(8192)?;

        // Whole-file paths are shorter than 4096 bytes. A store can hold
        // the target beside a parent path in the operation record, while
        // temporary and copied target names coexist during replacement.
        let paths = 4095_u64.checked_mul(4)?.checked_add(255_u64.checked_mul(4)?)?;

        file_map
            .checked_add(entry_cells)?
            .checked_add(entry_names)?
            .checked_add(whole_files)?
            .checked_add(read_buffers)?
            .checked_add(kernel_listing)?
            .checked_add(paths)?
            .checked_add(u64::try_from(size_of::<Pending>()).ok()?)?
            .checked_add(u64::try_from(size_of::<Submit>()).ok()?.checked_mul(2)?)?
            .checked_add(u64::try_from(size_of::<Complete>()).ok()?)
    }

    #[must_use]
    pub fn new(files: u32, max_read: u32, max_entries: u32, timeout: Duration) -> FileIo {
        Self::with_whole_limit(files, max_read, max_entries, max_read, timeout)
    }

    /// Creates a driver with a separate whole-file bound above its read chunk.
    #[must_use]
    pub fn with_whole_limit(files: u32, max_read: u32, max_entries: u32, max_file: u32, timeout: Duration) -> FileIo {
        assert!(
            files > 0 && max_read > 0 && max_entries > 0 && max_file > 0 && timeout.as_nanos() > 0,
            "file limits need room"
        );
        FileIo {
            files: Map::with_capacity(files),
            next_file: 1,
            next_op: 1,
            pending: None,
            outstanding: None,
            max_read,
            max_file,
            max_entries,
            timeout,
            deadline: None,
            cancel_op: None,
            stop: None,
            random: None,
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

    /// Adopts a directory descriptor opened by the shell as an io-owned root.
    /// The returned token is closed with `Request::Close`.
    pub fn adopt_root(&mut self, root: Fd) -> Option<Token> {
        self.insert(root)
    }

    /// Resolves an open file or root token within io.
    #[must_use]
    pub fn descriptor(&self, file: Token) -> Option<Fd> {
        self.file(file)
    }

    /// Supplies the random seed used to choose temporary names for stores.
    pub fn seed_randomness(&mut self, seed: u64) {
        assert!(self.takes(), "seed randomness between file requests");
        self.random = Some(seed);
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
    down_until(io, now.saturating_add(io.timeout), request, events, subs);
}

/// Starts one request with the owner's absolute deadline.
pub fn down_until(
    io: &mut FileIo,
    deadline: Time,
    request: Request,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    assert!(io.takes(), "a file request completes before the next begins");
    io.deadline = Some(deadline);
    down_inner(io, request, events, subs);
    if io.pending.is_none() {
        io.deadline = None;
    }
}

#[expect(clippy::too_many_lines, reason = "one exhaustive request match keeps file admission in one place")]
fn down_inner(io: &mut FileIo, request: Request, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    match request {
        Request::Create { owner, root, name, mode } => create(io, owner, root, name, mode, false, events, subs),
        Request::CreateNoFollow { owner, root, name, mode } => {
            create(io, owner, root, name, mode, true, events, subs);
        }
        Request::OpenRead { owner, root, name } => open_read(io, owner, root, name, false, events, subs),
        Request::OpenReadNoFollow { owner, root, name } => {
            open_read(io, owner, root, name, true, events, subs);
        }
        Request::OpenDirectory { owner, root, name, no_follow } => {
            if name.contains(&0) {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            if io.files.len() == io.files.capacity() {
                events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
                return;
            }
            io.pending = Some(Pending::OpenDirectory { owner });
            let how = if no_follow { OpenHow::DirectoryNoFollow } else { OpenHow::Directory };
            io.issue(Op::Open { root, path: name, how }, subs);
        }
        Request::Load { owner, root, path, max, no_follow } => {
            let Some(fd) = io.file(root) else {
                events.push(Event::Failed { owner, error: Error::NotFound });
                return;
            };
            if max > io.max_file || path.contains(&0) || path.len() >= 4096 {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            io.pending = Some(Pending::LoadOpening { owner, max });
            let how = if no_follow { OpenHow::ReadNoFollow } else { OpenHow::Read };
            io.issue(Op::Open { root: fd, path, how }, subs);
        }
        Request::Scan { owner, root, path, max, max_bytes, no_follow } => {
            let Some(fd) = io.file(root) else {
                events.push(Event::Failed { owner, error: Error::NotFound });
                return;
            };
            if max > io.max_entries || path.contains(&0) || path.len() >= 4096 {
                events.push(Event::Failed { owner, error: Error::InvalidArgument });
                return;
            }
            io.pending = Some(Pending::ScanOpening { owner, max, max_bytes });
            let how = if no_follow { OpenHow::DirectoryNoFollow } else { OpenHow::Directory };
            io.issue(Op::Open { root: fd, path, how }, subs);
        }
        Request::Store { owner, root, path, bytes, expected, no_follow } => {
            store::start(io, owner, root, path, bytes, expected, no_follow, events, subs);
        }
        Request::Stat { owner, file } => {
            let Some(fd) = io.file(file) else {
                events.push(Event::Failed { owner, error: Error::NotFound });
                return;
            };
            io.pending = Some(Pending::Stating { owner });
            io.issue(Op::Stat { fd }, subs);
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

#[expect(clippy::too_many_arguments, reason = "the file admission inputs and two output queues are explicit")]
fn create(
    io: &mut FileIo,
    owner: Token,
    root: Fd,
    name: Box<[u8]>,
    mode: u32,
    no_follow: bool,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    if name.contains(&0) || mode & !crate::kernel::PERMISSIONS != 0 {
        events.push(Event::Failed { owner, error: Error::InvalidArgument });
        return;
    }
    if io.files.len() == io.files.capacity() {
        events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
        return;
    }
    io.pending = Some(Pending::Create { owner });
    let how =
        if no_follow { OpenHow::CreateNoFollow { mode: Some(mode) } } else { OpenHow::Create { mode: Some(mode) } };
    io.issue(Op::Open { root, path: name, how }, subs);
}

fn open_read(
    io: &mut FileIo,
    owner: Token,
    root: Fd,
    name: Box<[u8]>,
    no_follow: bool,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    if name.contains(&0) {
        events.push(Event::Failed { owner, error: Error::InvalidArgument });
        return;
    }
    if io.files.len() == io.files.capacity() {
        events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
        return;
    }
    io.pending = Some(Pending::OpenRead { owner });
    let how = if no_follow { OpenHow::ReadNoFollow } else { OpenHow::Read };
    io.issue(Op::Open { root, path: name, how }, subs);
}

fn read_whole(io: &mut FileIo, owner: Token, fd: Fd, max: u32, bytes: Vec<u8>, subs: &mut Queue<Submit>) {
    let used = u32::try_from(bytes.len()).expect("whole load stays within its u32 bound");
    let left = max.checked_sub(used).expect("whole load stays within its bound");
    let asked = io.max_read.min(left.saturating_add(1));
    let buffer = vec![0_u8; usize::try_from(asked).expect("u32 fits usize")].into_boxed_slice();
    let offset = u64::from(used);
    io.pending = Some(Pending::LoadReading { owner, fd, max, bytes });
    let op = Op::read(fd, buffer, offset).expect("a positive bounded whole-file read");
    io.issue(op, subs);
}

/// Cancels the outstanding kernel operation after its request deadline. The
/// terminal failure is emitted when the target has settled, exactly once.
pub fn expire(io: &mut FileIo, now: Time, subs: &mut Queue<Submit>) {
    if !io.is_due(now) || io.stop.is_some() {
        return;
    }
    io.stop = Some(Stop::Deadline);
    io.deadline = None;
    submit_cancel(io, subs);
}

/// Requests cancellation of the active owner's file operation. The terminal
/// reports `Cancelled` only when the request was stopped before completion.
pub fn cancel(io: &mut FileIo, owner: Token, subs: &mut Queue<Submit>) {
    let Some(pending) = io.pending.as_ref() else {
        return;
    };
    if io.stop.is_some() || pending.owner() != owner {
        return;
    }
    match pending {
        Pending::Store(store) => {
            if !store.can_abandon() {
                return;
            }
        }
        Pending::Cleanup { .. } => return,
        Pending::Create { .. }
        | Pending::OpenRead { .. }
        | Pending::OpenDirectory { .. }
        | Pending::Stat { .. }
        | Pending::Stating { .. }
        | Pending::Write { .. }
        | Pending::Read { .. }
        | Pending::Sync { .. }
        | Pending::Close { .. }
        | Pending::Rename { .. }
        | Pending::Remove { .. }
        | Pending::ListOpening { .. }
        | Pending::List { .. }
        | Pending::ListClosing { .. }
        | Pending::LoadOpening { .. }
        | Pending::LoadStating { .. }
        | Pending::LoadReading { .. }
        | Pending::LoadClosing { .. }
        | Pending::LoadTooLargeClosing { .. }
        | Pending::ScanOpening { .. }
        | Pending::ScanListing { .. }
        | Pending::ScanClosing { .. } => {}
    }
    io.stop = Some(Stop::Cancel);
    submit_cancel(io, subs);
}

fn submit_cancel(io: &mut FileIo, subs: &mut Queue<Submit>) {
    let Some(target) = io.outstanding else {
        return;
    };
    if !io.pending.as_ref().expect("an outstanding operation has a request").can_cancel() {
        return;
    }
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
    match io.stop.take() {
        Some(Stop::Deadline) => stopped(io, complete, Error::TimedOut, events, subs),
        Some(Stop::Cancel) => cancelled(io, complete, events, subs),
        None => up_inner(io, complete, events, subs),
    }
    if io.pending.is_none() {
        io.deadline = None;
    }
}

fn cancelled(io: &mut FileIo, complete: Complete, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let pending = io.pending.as_ref().expect("a cancelled completion has a request");
    let can_abandon = match pending {
        Pending::Store(store) => store.can_abandon(),
        Pending::OpenRead { .. }
        | Pending::Stat { .. }
        | Pending::ListOpening { .. }
        | Pending::List { .. }
        | Pending::LoadOpening { .. }
        | Pending::LoadStating { .. }
        | Pending::LoadReading { .. }
        | Pending::ScanOpening { .. }
        | Pending::ScanListing { .. } => true,
        Pending::Write { .. } => !write_finished(&complete),
        Pending::Read { max, bytes, .. } => !read_finished(&complete, *max, bytes.len()),
        Pending::Create { .. }
        | Pending::OpenDirectory { .. }
        | Pending::Stating { .. }
        | Pending::Cleanup { .. }
        | Pending::Sync { .. }
        | Pending::Close { .. }
        | Pending::Rename { .. }
        | Pending::Remove { .. }
        | Pending::ListClosing { .. }
        | Pending::LoadClosing { .. }
        | Pending::LoadTooLargeClosing { .. }
        | Pending::ScanClosing { .. } => false,
    };
    let stop_won = match &complete.result {
        Err(Error::Cancelled) => match pending {
            Pending::Store(_) => can_abandon,
            Pending::Create { .. }
            | Pending::OpenRead { .. }
            | Pending::OpenDirectory { .. }
            | Pending::Stat { .. }
            | Pending::Stating { .. }
            | Pending::Cleanup { .. }
            | Pending::Write { .. }
            | Pending::Read { .. }
            | Pending::Sync { .. }
            | Pending::Close { .. }
            | Pending::Rename { .. }
            | Pending::Remove { .. }
            | Pending::ListOpening { .. }
            | Pending::List { .. }
            | Pending::ListClosing { .. }
            | Pending::LoadOpening { .. }
            | Pending::LoadStating { .. }
            | Pending::LoadReading { .. }
            | Pending::LoadClosing { .. }
            | Pending::LoadTooLargeClosing { .. }
            | Pending::ScanOpening { .. }
            | Pending::ScanListing { .. }
            | Pending::ScanClosing { .. } => true,
        },
        Ok(_) | Err(_) => can_abandon,
    };
    if stop_won {
        stopped(io, complete, Error::Cancelled, events, subs);
    } else {
        up_inner(io, complete, events, subs);
    }
}

fn write_finished(complete: &Complete) -> bool {
    match &complete.kind {
        Op::Write { bytes, from, .. } => match complete.result {
            Ok(Done::Count(count)) => {
                u64::from(*from).saturating_add(u64::from(count))
                    >= u64::try_from(bytes.len()).expect("buffer fits u64")
            }
            Ok(_) | Err(_) => false,
        },
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
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Cancel { .. } => false,
    }
}

fn read_finished(complete: &Complete, max: u32, prior: usize) -> bool {
    match complete.result {
        Ok(Done::Count(count)) => {
            count == 0
                || u64::try_from(prior).expect("bytes fit u64").saturating_add(u64::from(count)) >= u64::from(max)
        }
        Ok(_) | Err(_) => false,
    }
}

fn stopped(io: &mut FileIo, complete: Complete, reason: Error, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let pending = io.pending.take().expect("a timed out completion has a request");
    let pending = match pending {
        Pending::Store(store) => {
            store::stopped(io, store, complete, reason, events, subs);
            return;
        }
        other @ (Pending::Create { .. }
        | Pending::OpenRead { .. }
        | Pending::OpenDirectory { .. }
        | Pending::Stat { .. }
        | Pending::Stating { .. }
        | Pending::Cleanup { .. }
        | Pending::Write { .. }
        | Pending::Read { .. }
        | Pending::Sync { .. }
        | Pending::Close { .. }
        | Pending::Rename { .. }
        | Pending::Remove { .. }
        | Pending::ListOpening { .. }
        | Pending::List { .. }
        | Pending::ListClosing { .. }
        | Pending::LoadOpening { .. }
        | Pending::LoadStating { .. }
        | Pending::LoadReading { .. }
        | Pending::LoadClosing { .. }
        | Pending::LoadTooLargeClosing { .. }
        | Pending::ScanOpening { .. }
        | Pending::ScanListing { .. }
        | Pending::ScanClosing { .. }) => other,
    };
    let owner = match &pending {
        Pending::Create { owner }
        | Pending::OpenRead { owner }
        | Pending::OpenDirectory { owner }
        | Pending::Stat { owner, .. }
        | Pending::Stating { owner }
        | Pending::Cleanup { owner, .. }
        | Pending::Write { owner }
        | Pending::Read { owner, .. }
        | Pending::Sync { owner }
        | Pending::Close { owner }
        | Pending::Rename { owner }
        | Pending::Remove { owner }
        | Pending::ListOpening { owner }
        | Pending::List { owner, .. }
        | Pending::ListClosing { owner, .. }
        | Pending::LoadOpening { owner, .. }
        | Pending::LoadStating { owner, .. }
        | Pending::LoadReading { owner, .. }
        | Pending::LoadClosing { owner, .. }
        | Pending::LoadTooLargeClosing { owner, .. }
        | Pending::ScanOpening { owner, .. }
        | Pending::ScanListing { owner, .. }
        | Pending::ScanClosing { owner, .. } => *owner,
        Pending::Store(_) => unreachable!("store completion is handled separately"),
    };
    let fd = match (pending, complete.result) {
        (
            Pending::Create { .. }
            | Pending::OpenRead { .. }
            | Pending::OpenDirectory { .. }
            | Pending::ListOpening { .. }
            | Pending::LoadOpening { .. }
            | Pending::ScanOpening { .. },
            Ok(Done::Fd(fd)),
        )
        | (
            Pending::LoadStating { fd, .. }
            | Pending::LoadReading { fd, .. }
            | Pending::ScanListing { fd, .. }
            | Pending::Stat { fd, .. }
            | Pending::List { fd, .. },
            _,
        ) => Some(fd),
        _ => None,
    };
    if let Some(fd) = fd {
        io.pending = Some(Pending::Cleanup { owner, error: reason });
        io.issue(Op::Close { fd }, subs);
    } else {
        terminal_failure(events, owner, reason);
    }
}

fn terminal_failure(events: &mut Queue<Event>, owner: Token, reason: Error) {
    if reason == Error::Cancelled {
        events.push(Event::Cancelled { owner });
    } else {
        events.push(Event::Failed { owner, error: reason });
    }
}

/// Retains a name-order prefix under both scan bounds. `cutoff` is the
/// smallest omitted name seen so far; a later entry cannot cross it.
fn retain_scan_entry(
    entries: &mut Vec<Entry>,
    bytes: &mut u64,
    cutoff: &mut Option<Box<[u8]>>,
    max: u32,
    max_bytes: u64,
    name: &[u8],
    kind: crate::kernel::Kind,
) {
    match cutoff {
        Some(first_omitted) if name >= first_omitted.as_ref() => return,
        Some(_) | None => {}
    }
    let mut at = 0_usize;
    for entry in entries.iter() {
        if entry.name.as_ref() >= name {
            break;
        }
        at = at.checked_add(1).expect("bounded retained entries");
    }
    let cost = u64::try_from(size_of::<Entry>())
        .expect("entry cell fits u64")
        .checked_add(u64::try_from(name.len()).expect("kernel name fits u64"))
        .expect("entry cost fits u64");
    entries.insert(at, Entry { name: Box::from(name), kind });
    *bytes = bytes.checked_add(cost).expect("bounded scan bytes fit u64");
    for _ in 0..entries.len() {
        if entries.len() <= usize::try_from(max).expect("u32 fits usize") && *bytes <= max_bytes {
            break;
        }
        let removed = entries.pop().expect("overfull scan has a last entry");
        let removed_cost = u64::try_from(size_of::<Entry>())
            .expect("entry cell fits u64")
            .checked_add(u64::try_from(removed.name.len()).expect("kernel name fits u64"))
            .expect("entry cost fits u64");
        *bytes = bytes.checked_sub(removed_cost).expect("removed entry was counted");
        *cutoff = Some(removed.name);
    }
}

#[expect(clippy::too_many_lines, reason = "one exhaustive completion transition handles every file operation")]
fn up_inner(io: &mut FileIo, complete: Complete, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    let pending = io.pending.take().expect("a completion has a request");
    let pending = match pending {
        Pending::Store(store) => {
            store::up(io, store, complete, events, subs);
            return;
        }
        other @ (Pending::Create { .. }
        | Pending::OpenRead { .. }
        | Pending::OpenDirectory { .. }
        | Pending::Stat { .. }
        | Pending::Stating { .. }
        | Pending::Cleanup { .. }
        | Pending::Write { .. }
        | Pending::Read { .. }
        | Pending::Sync { .. }
        | Pending::Close { .. }
        | Pending::Rename { .. }
        | Pending::Remove { .. }
        | Pending::ListOpening { .. }
        | Pending::List { .. }
        | Pending::ListClosing { .. }
        | Pending::LoadOpening { .. }
        | Pending::LoadStating { .. }
        | Pending::LoadReading { .. }
        | Pending::LoadClosing { .. }
        | Pending::LoadTooLargeClosing { .. }
        | Pending::ScanOpening { .. }
        | Pending::ScanListing { .. }
        | Pending::ScanClosing { .. }) => other,
    };
    if let Err(error) = complete.result {
        let owner = match pending {
            Pending::Stat { owner, fd }
            | Pending::List { owner, fd, .. }
            | Pending::LoadStating { owner, fd, .. }
            | Pending::LoadReading { owner, fd, .. }
            | Pending::ScanListing { owner, fd, .. } => {
                io.pending = Some(Pending::Cleanup { owner, error });
                io.issue(Op::Close { fd }, subs);
                return;
            }
            Pending::Cleanup { owner, error } => {
                terminal_failure(events, owner, error);
                return;
            }
            Pending::Create { owner }
            | Pending::OpenRead { owner }
            | Pending::OpenDirectory { owner }
            | Pending::Stating { owner }
            | Pending::Write { owner }
            | Pending::Read { owner, .. }
            | Pending::Sync { owner }
            | Pending::Close { owner }
            | Pending::Rename { owner }
            | Pending::Remove { owner }
            | Pending::ListOpening { owner }
            | Pending::ListClosing { owner, .. }
            | Pending::LoadOpening { owner, .. }
            | Pending::LoadClosing { owner, .. }
            | Pending::LoadTooLargeClosing { owner, .. }
            | Pending::ScanOpening { owner, .. }
            | Pending::ScanClosing { owner, .. } => owner,
            Pending::Store(_) => unreachable!("store completion is handled separately"),
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
        (Pending::OpenDirectory { owner }, Op::Open { .. }, Done::Fd(fd)) => {
            if let Some(file) = io.insert(fd) {
                events.push(Event::Opened { owner, file, len: 0 });
            } else {
                io.pending = Some(Pending::Cleanup { owner, error: Error::TooManyOpenFiles });
                io.issue(Op::Close { fd }, subs);
            }
        }
        (Pending::Stating { owner }, Op::Stat { .. }, Done::Stat(stat)) => events.push(Event::Stated { owner, stat }),
        (Pending::Stat { owner, fd }, Op::Stat { .. }, Done::Stat(stat)) => {
            if let Some(file) = io.insert(fd) {
                events.push(Event::Opened { owner, file, len: stat.size });
            } else {
                events.push(Event::Failed { owner, error: Error::TooManyOpenFiles });
            }
        }
        (Pending::Cleanup { owner, error }, Op::Close { .. }, Done::Nothing) => {
            terminal_failure(events, owner, error);
        }
        (Pending::ListOpening { owner }, Op::Open { .. }, Done::Fd(fd)) => {
            io.pending = Some(Pending::List { owner, fd, entries: Vec::new() });
            io.list(fd, subs);
        }
        (Pending::LoadOpening { owner, max }, Op::Open { .. }, Done::Fd(fd)) => {
            io.pending = Some(Pending::LoadStating { owner, fd, max });
            io.issue(Op::Stat { fd }, subs);
        }
        (Pending::LoadStating { owner, fd, max }, Op::Stat { .. }, Done::Stat(stat)) => {
            if stat.kind != crate::kernel::Kind::File {
                io.pending = Some(Pending::Cleanup { owner, error: Error::IsADirectory });
                io.issue(Op::Close { fd }, subs);
            } else if stat.size > u64::from(max) {
                io.pending = Some(Pending::LoadTooLargeClosing { owner, size: stat.size });
                io.issue(Op::Close { fd }, subs);
            } else {
                let capacity =
                    usize::try_from(max).expect("u32 fits usize").checked_add(1).expect("one more byte fits");
                read_whole(io, owner, fd, max, Vec::with_capacity(capacity), subs);
            }
        }
        (Pending::LoadReading { owner, fd, max, mut bytes }, Op::Read { buf, .. }, Done::Count(count)) => {
            let n = usize::try_from(count).expect("u32 fits usize");
            bytes.extend_from_slice(buf.get(..n).expect("read count is within buffer"));
            if bytes.len() > usize::try_from(max).expect("u32 fits usize") {
                let size = u64::try_from(bytes.len()).expect("loaded bytes fit u64");
                io.pending = Some(Pending::LoadTooLargeClosing { owner, size });
                io.issue(Op::Close { fd }, subs);
            } else if count == 0 {
                io.pending = Some(Pending::LoadClosing { owner, bytes });
                io.issue(Op::Close { fd }, subs);
            } else {
                read_whole(io, owner, fd, max, bytes, subs);
            }
        }
        (Pending::LoadClosing { owner, bytes }, Op::Close { .. }, Done::Nothing) => {
            events.push(Event::Loaded { owner, bytes: bytes.into_boxed_slice() });
        }
        (Pending::LoadTooLargeClosing { owner, size }, Op::Close { .. }, Done::Nothing) => {
            events.push(Event::TooLarge { owner, size });
        }
        (Pending::ScanOpening { owner, max, max_bytes }, Op::Open { .. }, Done::Fd(fd)) => {
            let cell = u64::try_from(size_of::<Entry>()).expect("entry cell fits u64");
            let by_bytes = max_bytes.checked_div(cell).expect("entry cell is nonzero");
            let capacity = u64::from(max).min(by_bytes).checked_add(1).expect("one transient entry fits");
            io.pending = Some(Pending::ScanListing {
                owner,
                fd,
                max,
                max_bytes,
                entries: Vec::with_capacity(usize::try_from(capacity).expect("scan capacity fits usize")),
                bytes: 0,
                cutoff: None,
                total: 0,
            });
            io.list(fd, subs);
        }
        (
            Pending::ScanListing { owner, fd, max, max_bytes, mut entries, mut bytes, mut cutoff, mut total },
            Op::List { entries: records, names, .. },
            Done::Count(count),
        ) => {
            let count = usize::try_from(count).expect("u32 fits usize");
            if count == 0 {
                let kept = u64::try_from(entries.len()).expect("entry count fits u64");
                let more = total.checked_sub(kept).expect("retained entries were counted");
                io.pending = Some(Pending::ScanClosing { owner, entries, more });
                io.issue(Op::Close { fd }, subs);
                return;
            }
            for record in records.get(..count).expect("list count is within buffer") {
                let name = record.name(&names).expect("a listed entry names bytes");
                if name != b"." && name != b".." {
                    total = total.checked_add(1).expect("directory entry count fits u64");
                    retain_scan_entry(&mut entries, &mut bytes, &mut cutoff, max, max_bytes, name, record.kind);
                }
            }
            io.pending = Some(Pending::ScanListing { owner, fd, max, max_bytes, entries, bytes, cutoff, total });
            io.list(fd, subs);
        }
        (Pending::ScanClosing { owner, entries, more }, Op::Close { .. }, Done::Nothing) => {
            events.push(Event::Scanned { owner, entries: entries.into_boxed_slice(), more });
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
                entries.push(Entry { name: Box::from(name), kind: record.kind });
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
