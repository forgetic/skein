//! The store's transitions. At most one file request is outstanding.

#![expect(
    clippy::disallowed_types,
    reason = "commit queues, segments, and snapshot chunks are bounded by the store limits"
)]
#![expect(clippy::disallowed_methods, reason = "pure bounded admission and recovery scans have no hidden effects")]
#![expect(clippy::struct_excessive_bools, reason = "recovery flags distinguish independent durable milestones")]
#![expect(clippy::too_many_lines, reason = "file completion transitions stay in one exhaustive state match")]
#![expect(clippy::too_many_arguments, reason = "snapshot completion carries both queues and log coordinates")]

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::mem;
use skein_io::file;
use skein_io::kernel::Fd;
use skein_lib::{Env, Queue, Token};

use crate::Limits;
use crate::boundary::{Event, Failure, Op, Refusal, Request, Row};
use crate::frame;
use crate::map::KvMap;
use crate::snapshot;

const IO_OWNER: Token = Token::new(0x4b56);

#[derive(Debug)]
struct Pending {
    owner: Token,
    number: u64,
    ops: Box<[Op]>,
    frame: Box<[u8]>,
}

#[derive(Debug)]
enum Phase {
    Closed,
    Listing,
    OpeningLog { name: Box<[u8]> },
    ReadingLog { file: Token, first: u64, len: u64, at: u64, bytes: Vec<u8> },
    ClosingRecovered,
    RemovingTail,
    Creating { name: Box<[u8]> },
    Announcing { file: Token },
    Ready { file: Token, offset: u64 },
    Writing { file: Token, offset: u64 },
    Syncing { file: Token, offset: u64 },
    Closing,
    RecoveryClosing,
    RolloverClosing,
    SnapIo { log: Token, offset: u64, action: SnapAction },
    SnapshotFailureClosing { log: Token, offset: u64 },
    OpenFailureClosing { failure: Failure },
    OpeningSnapshot,
    ReadingSnapshot { file: Token, len: u64, at: u64, bytes: Vec<u8> },
    ClosingSnapshot,
    RemovingTempOnOpen,
}

#[derive(Debug)]
enum SnapAction {
    Create,
    RemoveTemp,
    Header { len: u64 },
    Chunk { last: Box<[u8]>, rows: u64, payload: u64, len: u64 },
    Trailer { len: u64 },
    Sync,
    Close,
    Rename,
    SyncDirectory,
    RemoveOld { name: Box<[u8]> },
    CleanupSyncDirectory,
}

#[derive(Debug)]
enum SnapStage {
    Create,
    Header,
    Chunk,
    Trailer,
    Sync,
    Close,
    Rename,
    SyncDirectory,
    RemoveOld,
    CleanupSyncDirectory,
}

#[derive(Debug)]
struct Snapshot {
    start: u64,
    file: Option<Token>,
    offset: u64,
    cursor: Option<Box<[u8]>>,
    rows: u64,
    payload: u64,
    stage: SnapStage,
}

impl Snapshot {
    fn new(start: u64) -> Snapshot {
        Snapshot { start, file: None, offset: 0, cursor: None, rows: 0, payload: 0, stage: SnapStage::Create }
    }
}

/// One store under one directory. It sends file requests below and events
/// above; the caller reserves `MAX_OUT` room before each step.
#[derive(Debug)]
pub struct Store {
    phase: Phase,
    root: Option<Fd>,
    open_owner: Option<Token>,
    close_owner: Option<Token>,
    last: u64,
    map: KvMap,
    queue: Queue<Pending>,
    queued_bytes: u64,
    active: Vec<Pending>,
    segments: Vec<Box<[u8]>>,
    segment_index: usize,
    tail_name: Option<Box<[u8]>>,
    recovered: bool,
    snapshot: Option<Snapshot>,
    snapshot_start: Option<u64>,
    has_snapshot: bool,
    has_temp: bool,
    log_bytes: u64,
    repairing: bool,
    repair_required: bool,
    recovering_after_failure: bool,
}

impl Store {
    #[must_use]
    pub fn new(limits: &Limits) -> Store {
        assert!(limits.is_usable(), "a store needs usable limits");
        Store {
            phase: Phase::Closed,
            root: None,
            open_owner: None,
            close_owner: None,
            last: 0,
            map: KvMap::new(limits),
            queue: Queue::with_capacity(limits.queued),
            queued_bytes: 0,
            active: Vec::new(),
            segments: Vec::new(),
            segment_index: 0,
            tail_name: None,
            recovered: false,
            snapshot: None,
            snapshot_start: None,
            has_snapshot: false,
            has_temp: false,
            log_bytes: 0,
            repairing: false,
            repair_required: false,
            recovering_after_failure: false,
        }
    }

    /// An in-memory store for protocol tests; commits are applied at once.
    #[must_use]
    pub fn memory(limits: &Limits) -> Store {
        let mut store = Store::new(limits);
        store.recovered = true;
        store
    }

    #[must_use]
    pub const fn last(&self) -> u64 {
        self.last
    }

    #[must_use]
    pub fn used(&self) -> u64 {
        self.map.used()
    }

    #[must_use]
    pub fn len(&self) -> u32 {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }

    fn root(&self) -> Fd {
        self.root.expect("file phases have a root")
    }

    fn announce_recovered(&mut self, above: &mut Queue<Event>) {
        if let Some(owner) = self.open_owner.take() {
            above.push(Event::Opened { owner, last: self.last });
        } else if self.recovering_after_failure {
            above.push(Event::Recovered { last: self.last });
        }
        self.recovering_after_failure = false;
    }

    fn available(&self) -> bool {
        self.recovered
            && match self.phase {
                Phase::Ready { .. }
                | Phase::Writing { .. }
                | Phase::Syncing { .. }
                | Phase::RolloverClosing
                | Phase::Creating { .. }
                | Phase::Announcing { .. }
                | Phase::SnapIo { .. } => true,
                Phase::Closed
                | Phase::Listing
                | Phase::OpeningLog { .. }
                | Phase::ReadingLog { .. }
                | Phase::ClosingRecovered
                | Phase::RemovingTail
                | Phase::Closing
                | Phase::RecoveryClosing
                | Phase::OpeningSnapshot
                | Phase::ReadingSnapshot { .. }
                | Phase::ClosingSnapshot
                | Phase::RemovingTempOnOpen
                | Phase::SnapshotFailureClosing { .. }
                | Phase::OpenFailureClosing { .. } => false,
            }
    }

    fn next_number(&self) -> Option<u64> {
        let active = u64::try_from(self.active.len()).ok()?;
        self.last.checked_add(active)?.checked_add(u64::from(self.queue.len()))?.checked_add(1)
    }

    fn all_ops(&self, extra: &[Op]) -> Vec<Op> {
        let mut ops = Vec::new();
        for pending in &self.active {
            ops.extend_from_slice(&pending.ops);
        }
        for pending in &self.queue {
            ops.extend_from_slice(&pending.ops);
        }
        ops.extend_from_slice(extra);
        ops
    }

    fn start_batch(&mut self, limits: &Limits, below: &mut Queue<file::Request>) {
        let Phase::Ready { file, offset } = self.phase else {
            return;
        };
        if self.queue.is_empty() {
            return;
        }
        if offset >= limits.segment {
            self.phase = Phase::RolloverClosing;
            below.push(file::Request::Close { owner: IO_OWNER, file });
            return;
        }
        while let Some(pending) = self.queue.pop() {
            self.queued_bytes = self
                .queued_bytes
                .checked_sub(u64::try_from(pending.frame.len()).expect("length fits u64"))
                .expect("queued bytes account for every frame");
            self.active.push(pending);
        }
        let mut bytes = Vec::new();
        for pending in &self.active {
            bytes.extend_from_slice(&pending.frame);
        }
        self.phase = Phase::Writing { file, offset };
        below.push(file::Request::WriteAt { owner: IO_OWNER, file, offset, bytes: bytes.into_boxed_slice() });
    }

    fn list(&mut self, below: &mut Queue<file::Request>) {
        self.phase = Phase::Listing;
        below.push(file::Request::List { owner: IO_OWNER, root: self.root() });
    }

    fn recover(&mut self, below: &mut Queue<file::Request>, limits: &Limits) {
        self.last = 0;
        self.map = KvMap::new(limits);
        self.segments.clear();
        self.segment_index = 0;
        self.tail_name = None;
        self.recovered = false;
        self.snapshot = None;
        self.snapshot_start = None;
        self.has_snapshot = false;
        self.has_temp = false;
        self.log_bytes = 0;
        self.repairing = false;
        self.list(below);
    }

    fn fail_commits(&mut self, above: &mut Queue<Event>, below: &mut Queue<file::Request>, limits: &Limits) {
        self.repair_required = true;
        self.recovering_after_failure = true;
        for pending in self.active.drain(..) {
            above.push(Event::Failed { owner: pending.owner, number: pending.number });
        }
        while let Some(pending) = self.queue.pop() {
            above.push(Event::Failed { owner: pending.owner, number: pending.number });
        }
        self.queued_bytes = 0;
        match mem::replace(&mut self.phase, Phase::Closed) {
            Phase::Ready { file, .. } | Phase::Writing { file, .. } | Phase::Syncing { file, .. } => {
                self.phase = Phase::RecoveryClosing;
                below.push(file::Request::Close { owner: IO_OWNER, file });
            }
            Phase::Closed
            | Phase::Listing
            | Phase::OpeningLog { .. }
            | Phase::ReadingLog { .. }
            | Phase::ClosingRecovered
            | Phase::RemovingTail
            | Phase::Creating { .. }
            | Phase::Announcing { .. }
            | Phase::Closing
            | Phase::RecoveryClosing
            | Phase::RolloverClosing
            | Phase::SnapIo { .. }
            | Phase::OpeningSnapshot
            | Phase::ReadingSnapshot { .. }
            | Phase::ClosingSnapshot
            | Phase::RemovingTempOnOpen
            | Phase::SnapshotFailureClosing { .. }
            | Phase::OpenFailureClosing { .. } => {
                self.recover(below, limits);
            }
        }
    }

    fn finish_recovery(&mut self, below: &mut Queue<file::Request>) {
        let next = self.last.checked_add(1).expect("commit number did not overflow");
        if self.repair_required {
            self.repairing = true;
        }
        if self.repairing {
            self.snapshot_start = Some(next);
        }
        let name = segment_name(next);
        if self.segments.iter().any(|entry| entry.as_ref() == name.as_ref()) {
            self.tail_name = Some(name);
            self.phase = Phase::RemovingTail;
            below.push(file::Request::Remove {
                owner: IO_OWNER,
                root: self.root(),
                name: self.tail_name.as_ref().expect("set").clone(),
            });
        } else {
            self.create_segment(name, below);
        }
    }

    fn create_segment(&mut self, name: Box<[u8]>, below: &mut Queue<file::Request>) {
        self.phase = Phase::Creating { name: name.clone() };
        below.push(file::Request::Create { owner: IO_OWNER, root: self.root(), name, mode: 0o600 });
    }

    fn next_log(&mut self, below: &mut Queue<file::Request>) {
        let Some(name) = self.segments.get(self.segment_index).cloned() else {
            self.finish_recovery(below);
            return;
        };
        self.segment_index = self.segment_index.checked_add(1).expect("segment index fits");
        self.phase = Phase::OpeningLog { name: name.clone() };
        below.push(file::Request::OpenRead { owner: IO_OWNER, root: self.root(), name });
    }

    fn start_recovery(&mut self, below: &mut Queue<file::Request>) {
        if self.has_snapshot {
            self.phase = Phase::OpeningSnapshot;
            below.push(file::Request::OpenRead {
                owner: IO_OWNER,
                root: self.root(),
                name: Box::from(&b"snapshot"[..]),
            });
        } else {
            self.next_log(below);
        }
    }

    fn apply_snapshot(&mut self, bytes: &[u8], limits: &Limits) -> Result<(), Failure> {
        let (start, rows) = snapshot::decode(bytes, limits.chunk).ok_or(Failure::Corrupt)?;
        self.last = start.checked_sub(1).ok_or(Failure::Corrupt)?;
        for row in rows {
            let op = Op::Put { key: row.key, value: row.value };
            let used = self.map.check(core::slice::from_ref(&op), limits).map_err(|_| Failure::Full)?;
            self.map.apply(core::slice::from_ref(&op), used);
        }
        self.segments.retain(|name| log_number(name).is_some_and(|number| number >= start));
        self.segment_index = 0;
        Ok(())
    }

    fn replay(&mut self, first: u64, bytes: &[u8], limits: &Limits) -> Result<bool, Failure> {
        if first != self.last.checked_add(1).ok_or(Failure::Corrupt)? {
            return Ok(false);
        }
        let mut at: usize = 0;
        while at < bytes.len() {
            let Some(head_end) = at.checked_add(frame::HEADER) else { return Ok(false) };
            let Some(head) = bytes.get(at..head_end) else {
                return Ok(false);
            };
            let Some(length) = frame::frame_len(head, limits.commit) else {
                return Ok(false);
            };
            let Some(end) = at.checked_add(length) else { return Ok(false) };
            let Some(encoded) = bytes.get(at..end) else {
                return Ok(false);
            };
            let Some(decoded) = frame::decode(encoded, limits.commit) else {
                return Ok(false);
            };
            if decoded.ops.len() > usize::try_from(limits.ops).expect("u32 fits usize") {
                return Ok(false);
            }
            if decoded.number != self.last.checked_add(1).ok_or(Failure::Corrupt)? {
                return Ok(false);
            }
            let used = self.map.check(&decoded.ops, limits).map_err(|_| Failure::Full)?;
            self.map.apply(&decoded.ops, used);
            self.last = decoded.number;
            at = end;
        }
        Ok(true)
    }
}

fn segment_name(number: u64) -> Box<[u8]> {
    let mut name = *b"log-00000000000000000000";
    let mut number = number;
    let mut pos = name.len();
    while pos > 4 {
        pos = pos.checked_sub(1).expect("position is positive");
        let digit = u8::try_from(number % 10).expect("decimal digit fits byte");
        *name.get_mut(pos).expect("position is inside segment name") =
            b'0'.checked_add(digit).expect("digit fits ASCII");
        number /= 10;
    }
    Box::from(name)
}

fn log_number(name: &[u8]) -> Option<u64> {
    if name.len() != 24 || name.get(..4)? != b"log-" {
        return None;
    }
    let mut number = 0_u64;
    for &byte in name.get(4..)? {
        if !byte.is_ascii_digit() {
            return None;
        }
        number = number.checked_mul(10)?.checked_add(u64::from(byte.checked_sub(b'0')?))?;
    }
    Some(number)
}

/// The owner's request.
pub fn down(
    store: &mut Store,
    env: &Env<Limits>,
    request: Request,
    above: &mut Queue<Event>,
    below: &mut Queue<file::Request>,
) {
    match request {
        Request::Open { owner, root } => {
            if store.root.is_some() {
                above.push(Event::Refused { owner, refusal: Refusal::Unavailable });
                return;
            }
            if let Phase::Closed = store.phase {
            } else {
                above.push(Event::Refused { owner, refusal: Refusal::Unavailable });
                return;
            }
            store.root = Some(root);
            store.open_owner = Some(owner);
            store.recover(below, &env.limits);
        }
        Request::Commit { owner, ops } => {
            if store.root.is_none() && store.recovered {
                commit_memory(store, &env.limits, owner, ops, above);
                return;
            }
            if !store.available() || store.close_owner.is_some() {
                above.push(Event::Refused { owner, refusal: Refusal::Unavailable });
                return;
            }
            if store.queue.room() == 0 {
                above.push(Event::Refused { owner, refusal: Refusal::Busy });
                return;
            }
            let Some(number) = store.next_number() else {
                above.push(Event::Refused { owner, refusal: Refusal::Full });
                return;
            };
            if ops.len() > usize::try_from(env.limits.ops).expect("u32 fits usize") {
                above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
                return;
            }
            if ops.iter().any(|op| {
                op.key().len() > usize::try_from(env.limits.key).expect("key limit fits usize")
                    || match op {
                        Op::Put { value, .. } => {
                            value.len() > usize::try_from(env.limits.value).expect("value limit fits usize")
                        }
                        Op::Erase { .. } => false,
                    }
            }) {
                above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
                return;
            }
            if frame::encoded_len(&ops)
                .is_none_or(|size| size > usize::try_from(env.limits.commit).expect("commit limit fits usize"))
            {
                above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
                return;
            }
            let Some(bytes) = frame::encode(number, &ops, env.limits.commit) else {
                above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
                return;
            };
            let all = store.all_ops(&ops);
            if let Err(refusal) = store.map.check(&all, &env.limits) {
                above.push(Event::Refused { owner, refusal });
                return;
            }
            let total = store.queued_bytes.checked_add(u64::try_from(bytes.len()).expect("frame length fits u64"));
            if total.is_none_or(|total| total > env.limits.queued_bytes) {
                above.push(Event::Refused { owner, refusal: Refusal::Busy });
                return;
            }
            store.queued_bytes = total.expect("checked queue bytes");
            store.queue.push(Pending { owner, number, ops, frame: bytes });
            store.start_batch(&env.limits, below);
        }
        Request::Get { owner, key } => {
            if key.len() > usize::try_from(env.limits.key).expect("key limit fits") {
                above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
                return;
            }
            if !(store.available() || store.root.is_none() && store.recovered) {
                above.push(Event::Refused { owner, refusal: Refusal::Unavailable });
                return;
            }
            above.push(Event::Got { owner, value: store.map.get(&key).map(Box::from) });
        }
        Request::Load { owner, range, max } => {
            if !(store.available() || store.root.is_none() && store.recovered) {
                above.push(Event::Refused { owner, refusal: Refusal::Unavailable });
                return;
            }
            if max.rows == 0 || max.bytes == 0 || max.rows > env.limits.page.rows || max.bytes > env.limits.page.bytes {
                above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
                return;
            }
            if range.start.len() > usize::try_from(env.limits.key).expect("key limit fits")
                || range
                    .end
                    .as_ref()
                    .is_some_and(|end| end.len() > usize::try_from(env.limits.key).expect("key limit fits"))
                || range.end.as_ref().is_some_and(|end| end.as_ref() < range.start.as_ref())
            {
                above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
                return;
            }
            match store.map.page(&range, max) {
                Ok(page) => above.push(Event::Loaded { owner, rows: page.rows, next: page.next }),
                Err(refusal) => above.push(Event::Refused { owner, refusal }),
            }
        }
        Request::Close { owner } => {
            if store.root.is_none() && store.recovered {
                store.recovered = false;
                store.phase = Phase::Closed;
                above.push(Event::Closed { owner });
            } else if store.root.is_none() || store.close_owner.is_some() {
                above.push(Event::Refused { owner, refusal: Refusal::Unavailable });
            } else {
                store.close_owner = Some(owner);
                if let Phase::Ready { file, .. } = store.phase
                    && store.snapshot.is_none()
                    && store.queue.is_empty()
                {
                    store.phase = Phase::Closing;
                    below.push(file::Request::Close { owner: IO_OWNER, file });
                }
            }
        }
    }
}

fn commit_memory(store: &mut Store, limits: &Limits, owner: Token, ops: Box<[Op]>, above: &mut Queue<Event>) {
    let Some(number) = store.last.checked_add(1) else {
        above.push(Event::Refused { owner, refusal: Refusal::Full });
        return;
    };
    if ops.iter().any(|op| {
        op.key().len() > usize::try_from(limits.key).expect("key limit fits usize")
            || match op {
                Op::Put { value, .. } => value.len() > usize::try_from(limits.value).expect("value limit fits usize"),
                Op::Erase { .. } => false,
            }
    }) || frame::encoded_len(&ops)
        .is_none_or(|size| size > usize::try_from(limits.commit).expect("commit limit fits usize"))
    {
        above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
        return;
    }
    if ops.len() > usize::try_from(limits.ops).expect("u32 fits usize") {
        above.push(Event::Refused { owner, refusal: Refusal::TooLarge });
        return;
    }
    match store.map.check(&ops, limits) {
        Ok(used) => {
            store.map.apply(&ops, used);
            store.last = number;
            above.push(Event::Committed { owner, number });
        }
        Err(refusal) => above.push(Event::Refused { owner, refusal }),
    }
}

/// One terminal event from the file layer.
pub fn up(
    store: &mut Store,
    env: &Env<Limits>,
    event: file::Event,
    above: &mut Queue<Event>,
    below: &mut Queue<file::Request>,
) {
    assert!(event.owner() == IO_OWNER, "the store receives only its own file events");
    let phase = mem::replace(&mut store.phase, Phase::Closed);
    match (phase, event) {
        (Phase::Listing, file::Event::Listed { entries, .. }) => {
            store.segments.clear();
            for entry in entries {
                if entry.name.as_ref() == b"snapshot" {
                    store.has_snapshot = true;
                }
                if entry.name.as_ref() == b"snapshot.tmp" {
                    store.has_temp = true;
                }
                if log_number(&entry.name).is_some() {
                    store.segments.push(entry.name);
                }
            }
            store.segments.sort_unstable();
            store.segment_index = 0;
            if store.has_temp {
                store.phase = Phase::RemovingTempOnOpen;
                below.push(file::Request::Remove {
                    owner: IO_OWNER,
                    root: store.root(),
                    name: Box::from(&b"snapshot.tmp"[..]),
                });
            } else {
                store.start_recovery(below);
            }
        }
        (Phase::RemovingTempOnOpen, file::Event::Removed { .. }) => store.start_recovery(below),
        (Phase::OpeningSnapshot, file::Event::Opened { file, len, .. }) => {
            let cap = env.limits.budget.saturating_mul(2).saturating_add(1024);
            if len > cap {
                open_failed_file(store, file, Failure::Full, below);
                return;
            }
            store.phase = Phase::ReadingSnapshot { file, len, at: 0, bytes: Vec::new() };
            below.push(file::Request::ReadAt { owner: IO_OWNER, file, offset: 0, max: env.limits.chunk });
        }
        (Phase::ReadingSnapshot { file, len, at, mut bytes }, file::Event::Read { bytes: read, .. }) => {
            let next = at.checked_add(u64::try_from(read.len()).expect("length fits u64"));
            if read.is_empty() || next.is_none_or(|next| next > len) {
                open_failed_file(store, file, Failure::Corrupt, below);
                return;
            }
            bytes.extend_from_slice(&read);
            let next = next.expect("checked read offset");
            if next < len {
                store.phase = Phase::ReadingSnapshot { file, len, at: next, bytes };
                below.push(file::Request::ReadAt { owner: IO_OWNER, file, offset: next, max: env.limits.chunk });
            } else {
                if let Err(failure) = store.apply_snapshot(&bytes, &env.limits) {
                    open_failed_file(store, file, failure, below);
                    return;
                }
                store.phase = Phase::ClosingSnapshot;
                below.push(file::Request::Close { owner: IO_OWNER, file });
            }
        }
        (Phase::ClosingSnapshot | Phase::ClosingRecovered, file::Event::Closed { .. }) => store.next_log(below),
        (Phase::OpeningLog { name }, file::Event::Opened { file, len, .. }) => {
            let cap = env.limits.segment.saturating_add(env.limits.queued_bytes);
            if len > cap {
                open_failed_file(store, file, Failure::Full, below);
                return;
            }
            let first = log_number(&name).expect("an opened log has a numbered name");
            store.phase = Phase::ReadingLog { file, first, len, at: 0, bytes: Vec::new() };
            if len == 0 {
                below.push(file::Request::Close { owner: IO_OWNER, file });
                store.phase = Phase::ClosingRecovered;
            } else {
                below.push(file::Request::ReadAt { owner: IO_OWNER, file, offset: 0, max: env.limits.chunk });
            }
        }
        (Phase::ReadingLog { file, first, len, at, mut bytes }, file::Event::Read { bytes: read, .. }) => {
            let next = at.checked_add(u64::try_from(read.len()).expect("length fits u64"));
            if read.is_empty() || next.is_none_or(|next| next > len) {
                open_failed_file(store, file, Failure::Corrupt, below);
                return;
            }
            bytes.extend_from_slice(&read);
            let next = next.expect("checked read offset");
            if next < len {
                store.phase = Phase::ReadingLog { file, first, len, at: next, bytes };
                below.push(file::Request::ReadAt { owner: IO_OWNER, file, offset: next, max: env.limits.chunk });
            } else {
                let replayed = store.replay(first, &bytes, &env.limits);
                match replayed {
                    Ok(whole) => {
                        if !whole {
                            store.segment_index = store.segments.len();
                            store.repairing = true;
                            store.repair_required = true;
                        }
                        store.phase = Phase::ClosingRecovered;
                        below.push(file::Request::Close { owner: IO_OWNER, file });
                    }
                    Err(failure) => open_failed_file(store, file, failure, below),
                }
            }
        }
        (Phase::RemovingTail, file::Event::Removed { .. }) => {
            let name = store.tail_name.take().expect("tail to remove");
            store.create_segment(name, below);
        }
        (Phase::Creating { name }, file::Event::Opened { file, .. }) => {
            store.segments.push(name);
            store.phase = Phase::Announcing { file };
            below.push(file::Request::SyncDirectory { owner: IO_OWNER, root: store.root() });
        }
        (Phase::Announcing { file }, file::Event::Synced { .. }) => {
            store.phase = Phase::Ready { file, offset: 0 };
            let was_recovered = store.recovered;
            store.recovered = !store.repairing;
            if let Some(start) = store.snapshot_start.take() {
                store.snapshot = Some(Snapshot::new(start));
                store.log_bytes = 0;
            }
            if !was_recovered && !store.repairing {
                store.announce_recovered(above);
            }
            if !store.queue.is_empty() {
                store.start_batch(&env.limits, below);
            } else if let Some(owner) = store.close_owner.filter(|_| store.snapshot.is_none()) {
                store.phase = Phase::Closing;
                below.push(file::Request::Close { owner: IO_OWNER, file });
                store.close_owner = Some(owner);
            }
        }
        (Phase::Writing { file, offset }, file::Event::Written { .. }) => {
            store.phase = Phase::Syncing { file, offset };
            below.push(file::Request::Sync { owner: IO_OWNER, file });
        }
        (Phase::Syncing { file, offset }, file::Event::Synced { .. }) => {
            let mut bytes: u64 = 0;
            for pending in store.active.drain(..) {
                let used = store.map.check(&pending.ops, &env.limits).expect("a queued commit passed admission");
                store.map.apply(&pending.ops, used);
                store.last = pending.number;
                bytes = bytes
                    .checked_add(u64::try_from(pending.frame.len()).expect("frame length fits u64"))
                    .expect("batch size fits u64");
                above.push(Event::Committed { owner: pending.owner, number: pending.number });
            }
            let offset = offset.checked_add(bytes).expect("segment offset fits u64");
            store.log_bytes = store.log_bytes.checked_add(bytes).expect("log bytes fit u64");
            store.phase = Phase::Ready { file, offset };
            if !store.queue.is_empty() {
                store.start_batch(&env.limits, below);
            } else if let Some(owner) = store.close_owner.filter(|_| store.snapshot.is_none()) {
                store.phase = Phase::Closing;
                below.push(file::Request::Close { owner: IO_OWNER, file });
                store.close_owner = Some(owner);
            }
        }
        (Phase::Closing, file::Event::Closed { .. } | file::Event::Failed { .. }) => {
            store.phase = Phase::Closed;
            store.root = None;
            store.recovered = false;
            if let Some(owner) = store.close_owner.take() {
                above.push(Event::Closed { owner });
            }
        }
        (Phase::RecoveryClosing, file::Event::Closed { .. } | file::Event::Failed { .. }) => {
            store.recover(below, &env.limits);
        }
        (Phase::RolloverClosing, file::Event::Closed { .. }) => {
            let next = store.last.checked_add(1).expect("commit number did not overflow");
            store.create_segment(segment_name(next), below);
        }
        (Phase::SnapIo { log, offset, action }, event) => {
            snapshot_up(store, action, event, log, offset, env, above, below);
        }
        (Phase::SnapshotFailureClosing { log, offset }, file::Event::Closed { .. } | file::Event::Failed { .. }) => {
            store.phase = Phase::Ready { file: log, offset };
            store.fail_commits(above, below, &env.limits);
        }
        (Phase::OpenFailureClosing { failure }, file::Event::Closed { .. } | file::Event::Failed { .. }) => {
            open_failed(store, failure, above);
        }
        (
            Phase::ReadingSnapshot { file, .. } | Phase::ReadingLog { file, .. } | Phase::Announcing { file },
            file::Event::Failed { .. },
        ) => open_failed_file(store, file, Failure::Io, below),
        (
            Phase::OpeningLog { .. }
            | Phase::Listing
            | Phase::RemovingTempOnOpen
            | Phase::OpeningSnapshot
            | Phase::ClosingSnapshot
            | Phase::ClosingRecovered
            | Phase::Creating { .. }
            | Phase::RemovingTail,
            file::Event::Failed { .. },
        ) => {
            open_failed(store, Failure::Io, above);
        }
        (Phase::RolloverClosing, file::Event::Failed { .. }) => {
            store.fail_commits(above, below, &env.limits);
        }
        (
            Phase::Writing { file, offset } | Phase::Syncing { file, offset } | Phase::Ready { file, offset },
            file::Event::Failed { .. },
        ) => {
            store.phase = Phase::Ready { file, offset };
            store.fail_commits(above, below, &env.limits);
        }
        (phase, event) => {
            store.phase = phase;
            drop(event);
            unreachable!("a file event must match the one outstanding request");
        }
    }
}

fn open_failed(store: &mut Store, failure: Failure, above: &mut Queue<Event>) {
    store.phase = Phase::Closed;
    store.root = None;
    store.recovered = false;
    if let Some(owner) = store.open_owner.take() {
        above.push(Event::OpenFailed { owner, failure });
    } else if store.recovering_after_failure {
        above.push(Event::RecoveryFailed { failure });
    }
    store.recovering_after_failure = false;
    if let Some(owner) = store.close_owner.take() {
        above.push(Event::Closed { owner });
    }
}

fn open_failed_file(store: &mut Store, file: Token, failure: Failure, below: &mut Queue<file::Request>) {
    store.phase = Phase::OpenFailureClosing { failure };
    below.push(file::Request::Close { owner: IO_OWNER, file });
}

/// Whether `fire` has one chunk or one replacement step ready to do.
#[must_use]
pub fn is_ready(store: &Store, limits: &Limits) -> bool {
    if let Phase::Ready { .. } = store.phase {
        store.snapshot.is_some() || store.log_bytes >= limits.snapshot_after || store.close_owner.is_some()
    } else {
        false
    }
}

/// Writes one snapshot chunk or advances its replacement sequence.
pub fn fire(store: &mut Store, env: &Env<Limits>, _above: &mut Queue<Event>, below: &mut Queue<file::Request>) {
    let Phase::Ready { file: log, offset } = store.phase else {
        return;
    };
    if !store.queue.is_empty() {
        store.start_batch(&env.limits, below);
        return;
    }
    if store.snapshot.is_none() {
        if store.close_owner.is_some() {
            store.phase = Phase::Closing;
            below.push(file::Request::Close { owner: IO_OWNER, file: log });
        } else if store.log_bytes >= env.limits.snapshot_after {
            store.snapshot_start = Some(store.last.checked_add(1).expect("commit number did not overflow"));
            store.phase = Phase::RolloverClosing;
            below.push(file::Request::Close { owner: IO_OWNER, file: log });
        }
        return;
    }
    let snap = store.snapshot.as_mut().expect("snapshot exists");
    let (action, request) = match snap.stage {
        SnapStage::Create => (
            SnapAction::Create,
            file::Request::Create {
                owner: IO_OWNER,
                root: store.root(),
                name: Box::from(&b"snapshot.tmp"[..]),
                mode: 0o600,
            },
        ),
        SnapStage::Header => {
            let file = snap.file.expect("snapshot file opened");
            let bytes = snapshot::header(snap.start);
            let len = u64::try_from(bytes.len()).expect("header length fits");
            (SnapAction::Header { len }, file::Request::WriteAt { owner: IO_OWNER, file, offset: snap.offset, bytes })
        }
        SnapStage::Chunk => {
            let mut rows: Vec<Row> = Vec::new();
            let mut size: usize = 12;
            let mut payload: u64 = 0;
            for (key, value) in store.map.after(snap.cursor.as_deref()) {
                let row_size =
                    8_usize.checked_add(key.len()).and_then(|v| v.checked_add(value.len())).expect("row size fits");
                if size
                    .checked_add(row_size)
                    .is_none_or(|v| v > usize::try_from(env.limits.chunk).expect("chunk fits usize"))
                {
                    break;
                }
                size = size.checked_add(row_size).expect("chunk size fits");
                payload = payload
                    .checked_add(
                        u64::try_from(key.len().checked_add(value.len()).expect("payload fits"))
                            .expect("length fits u64"),
                    )
                    .expect("payload total fits u64");
                rows.push(Row { key: key.clone(), value: value.clone() });
            }
            if rows.is_empty() {
                snap.stage = SnapStage::Trailer;
                let file = snap.file.expect("snapshot file opened");
                let bytes = snapshot::trailer(snap.rows, snap.payload);
                let len = u64::try_from(bytes.len()).expect("trailer length fits");
                (
                    SnapAction::Trailer { len },
                    file::Request::WriteAt { owner: IO_OWNER, file, offset: snap.offset, bytes },
                )
            } else {
                let last = rows.last().expect("a chunk has rows").key.clone();
                let count = u64::try_from(rows.len()).expect("count fits u64");
                let bytes = snapshot::chunk(&rows, env.limits.chunk).expect("limits hold one row per chunk");
                let len = u64::try_from(bytes.len()).expect("chunk length fits");
                let file = snap.file.expect("snapshot file opened");
                (
                    SnapAction::Chunk { last, rows: count, payload, len },
                    file::Request::WriteAt { owner: IO_OWNER, file, offset: snap.offset, bytes },
                )
            }
        }
        SnapStage::Trailer => {
            let file = snap.file.expect("snapshot file opened");
            let bytes = snapshot::trailer(snap.rows, snap.payload);
            let len = u64::try_from(bytes.len()).expect("trailer length fits");
            (SnapAction::Trailer { len }, file::Request::WriteAt { owner: IO_OWNER, file, offset: snap.offset, bytes })
        }
        SnapStage::Sync => {
            (SnapAction::Sync, file::Request::Sync { owner: IO_OWNER, file: snap.file.expect("snapshot file opened") })
        }
        SnapStage::Close => (
            SnapAction::Close,
            file::Request::Close { owner: IO_OWNER, file: snap.file.expect("snapshot file opened") },
        ),
        SnapStage::Rename => (
            SnapAction::Rename,
            file::Request::Rename {
                owner: IO_OWNER,
                root: store.root(),
                from: Box::from(&b"snapshot.tmp"[..]),
                to: Box::from(&b"snapshot"[..]),
            },
        ),
        SnapStage::SyncDirectory => {
            (SnapAction::SyncDirectory, file::Request::SyncDirectory { owner: IO_OWNER, root: store.root() })
        }
        SnapStage::RemoveOld => {
            let old = store.segments.iter().find(|name| log_number(name).is_some_and(|n| n < snap.start)).cloned();
            let Some(name) = old else {
                snap.stage = SnapStage::CleanupSyncDirectory;
                store.phase = Phase::SnapIo { log, offset, action: SnapAction::CleanupSyncDirectory };
                below.push(file::Request::SyncDirectory { owner: IO_OWNER, root: store.root() });
                return;
            };
            (
                SnapAction::RemoveOld { name: name.clone() },
                file::Request::Remove { owner: IO_OWNER, root: store.root(), name },
            )
        }
        SnapStage::CleanupSyncDirectory => {
            (SnapAction::CleanupSyncDirectory, file::Request::SyncDirectory { owner: IO_OWNER, root: store.root() })
        }
    };
    store.phase = Phase::SnapIo { log, offset, action };
    below.push(request);
}

fn snapshot_up(
    store: &mut Store,
    action: SnapAction,
    event: file::Event,
    log: Token,
    offset: u64,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<file::Request>,
) {
    if let file::Event::Failed { error, .. } = event {
        if let SnapAction::Create = action
            && error == skein_io::kernel::Error::Exists
        {
            store.phase = Phase::SnapIo { log, offset, action: SnapAction::RemoveTemp };
            below.push(file::Request::Remove {
                owner: IO_OWNER,
                root: store.root(),
                name: Box::from(&b"snapshot.tmp"[..]),
            });
            return;
        }
        if let Some(file) = store.snapshot.take().and_then(|snap| snap.file) {
            store.phase = Phase::SnapshotFailureClosing { log, offset };
            below.push(file::Request::Close { owner: IO_OWNER, file });
            return;
        }
        store.phase = Phase::Ready { file: log, offset };
        store.fail_commits(above, below, &env.limits);
        return;
    }
    let snap = store.snapshot.as_mut().expect("snapshot action has state");
    match (action, event) {
        (SnapAction::Create, file::Event::Opened { file, .. }) => {
            snap.file = Some(file);
            snap.stage = SnapStage::Header;
        }
        (SnapAction::RemoveTemp, file::Event::Removed { .. }) => snap.stage = SnapStage::Create,
        (SnapAction::Header { len }, file::Event::Written { .. }) => {
            snap.offset = snap.offset.checked_add(len).expect("snapshot offset fits");
            snap.stage = SnapStage::Chunk;
        }
        (SnapAction::Chunk { last, rows, payload, len }, file::Event::Written { .. }) => {
            snap.offset = snap.offset.checked_add(len).expect("snapshot offset fits");
            snap.rows = snap.rows.checked_add(rows).expect("snapshot rows fit");
            snap.payload = snap.payload.checked_add(payload).expect("snapshot payload fits");
            snap.cursor = Some(last);
            snap.stage = SnapStage::Chunk;
        }
        (SnapAction::Trailer { len }, file::Event::Written { .. }) => {
            snap.offset = snap.offset.checked_add(len).expect("snapshot offset fits");
            snap.stage = SnapStage::Sync;
        }
        (SnapAction::Sync, file::Event::Synced { .. }) => snap.stage = SnapStage::Close,
        (SnapAction::Close, file::Event::Closed { .. }) => {
            snap.file = None;
            snap.stage = SnapStage::Rename;
        }
        (SnapAction::Rename, file::Event::Renamed { .. }) => snap.stage = SnapStage::SyncDirectory,
        (SnapAction::SyncDirectory, file::Event::Synced { .. }) => {
            store.has_snapshot = true;
            snap.stage = SnapStage::RemoveOld;
            if store.repairing {
                store.repairing = false;
                store.repair_required = false;
                store.recovered = true;
                store.announce_recovered(above);
            }
        }
        (SnapAction::RemoveOld { name }, file::Event::Removed { .. }) => {
            store.segments.retain(|item| item.as_ref() != name.as_ref());
            snap.stage = SnapStage::RemoveOld;
        }
        (SnapAction::CleanupSyncDirectory, file::Event::Synced { .. }) => {
            store.snapshot = None;
        }
        (_, _) => unreachable!("snapshot event matches its request"),
    }
    store.phase = Phase::Ready { file: log, offset };
    if !store.queue.is_empty() {
        store.start_batch(&env.limits, below);
    }
}
