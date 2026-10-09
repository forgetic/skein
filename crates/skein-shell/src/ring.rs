//! The ring adapter (shell.md, 3): io's kernel records
//! (`skein_io::kernel`, whose module documentation is the contract kept
//! here) on io_uring. It maps each record onto one submission entry, keeps
//! the record in its in-flight table, and when the entry completes decodes
//! the result and hands the record back. It makes no decisions.
//!
//! # The in-flight table
//!
//! One slot per operation the [`Config`] allows, allocated once at its final
//! capacity in a `Box<[Slot]>` that is never resized or replaced while
//! anything is in flight, so a slot's address is fixed for the life of the
//! [`Kernel`]. A slot holds the [`Submit`] record and the kernel structures
//! its operation needs (a socket address and its length, an `open_how`, a
//! `statx` buffer, a `signalfd_siginfo`, and a path or two as the NUL-terminated strings the
//! kernel reads); those structures never leave this module, and a record's
//! buffers are pointed at in place, inside the record's own `Box`.
//!
//! An operation's `user_data` is its slot's index and the slot's generation,
//! which moves on every time the slot is freed, so a completion or an async
//! cancel can never name the slot's next operation. A map from token to
//! `user_data` finds a cancel's target and catches a token already in flight.
//!
//! # The `unsafe`, and why it is sound
//!
//! - **Pushing an entry** hands the kernel pointers into the slot (an
//!   address and its length, an `open_how`, a `statx` buffer, a signal
//!   record), into the
//!   slot's paths, and into the record's buffers. All stay valid and
//!   unmoved until the entry's completion is reaped: the slot is not freed
//!   or moved until then, and no Rust code reads or writes it (only its
//!   token's entry in the map is read). The structures inline in the slot
//!   sit in `UnsafeCell`s: the kernel gets the cells' raw pointers, and
//!   Rust reaches their contents through `get_mut` only before the entry is
//!   pushed and after its completion has moved the operation out of its
//!   slot. The paths are boxes the slot owns, copies of the record's with a
//!   NUL after them, made before the entry is pushed and only read by the
//!   kernel. The record's `Box`es stay in the slot; a `Recv` or `Read`
//!   buffer is written by the kernel, a `Send` or `Write` buffer only read.
//!   Reaching a free slot borrows the table's slice for a moment, which
//!   touches no byte of a busy slot.
//! - **Entering the ring** passes the count of pushed entries and, for a
//!   deadline, an argument that lives across the call.
//! - **Socket addresses** are cast between `sockaddr_storage` and the
//!   `sockaddr_in` or `sockaddr_in6` its family names: the storage is larger
//!   and at least as aligned as either.
//! - **The synchronous calls** (`setsockopt`, `getsockname`, `close`,
//!   `open`, `getdents64`, `lseek`, `fstatat`, `fstat`, `fcntl`,
//!   `clock_gettime`, `getrandom`) are given pointers to locals, to strings
//!   or to the
//!   table's listing buffer, each of which lives across the call, with their
//!   true sizes. `getdents64` is not a ring operation, so a `List` runs at
//!   its submit and completes at the next reap.
//! - **Dropping a [`Kernel`]** with operations in flight leaks the table
//!   through `Box::into_raw`, creating no reference to it, so the kernel
//!   never writes into freed memory while the ring winds down.
//!
//! The completion queue holds as many entries as the table has slots, and
//! every slot has at most one entry in the kernel (single-shot operations
//! only, each cancel in a slot of its own), so it cannot overflow. An
//! `Open` that a `..` race failed with `EAGAIN` is pushed again from its
//! slot, under the same `user_data`, once its first entry has completed:
//! still one entry a slot.

#![expect(
    unsafe_code,
    reason = "the ring adapter is the one place in skein that hands memory to the kernel (shell.md, 3; programming-model.md, 2.1)"
)]

mod process;

use std::cell::UnsafeCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{CStr, CString};
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::mem::{self, size_of};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;

use io_uring::{EnterFlags, IoUring, Probe, opcode, squeue, types};
use process::{signal_child, spawn};

/// Blocks the service's termination signals and opens their signalfd for io
/// to adopt before the loop starts (shell.md, section 6; io.md, section 7).
pub fn open_termination_signals() -> Result<Fd, i32> {
    process::open_termination_signals()
}
/// Pipe descriptors for a spawned service hosted in the real loop (examples.md, section 6).
pub use process::HostedPipes;

/// Makes a hosted spawn's real pipes and identity placeholder, without exec.
pub fn hosted_pipes(command: &mut skein_io::kernel::Spawn) -> Result<HostedPipes, Error> {
    process::hosted_pipes(command)
}

/// Starts a shipped binary on pipes or a controlling pseudo-terminal for
/// an end-to-end world; stderr is always separate (examples.md, section 6).
pub fn start_binary(command: &mut skein_io::kernel::Spawn, terminal: bool) -> Result<(Fd, Option<Fd>), Error> {
    process::start_binary(command, terminal, None)
}

/// Starts the binary in an already-open delegated cgroup, before its exec.
pub fn start_binary_in_cgroup(
    command: &mut skein_io::kernel::Spawn,
    terminal: bool,
    cgroup: Fd,
) -> Result<(Fd, Option<Fd>), Error> {
    process::start_binary(command, terminal, Some(cgroup))
}

/// Makes the test process a subreaper for its binary's descendants.
pub fn make_subreaper() -> Result<(), Error> {
    process::make_subreaper()
}

/// Opens a discovered process's pidfd for the test keeper.
pub fn open_pidfd(pid: u32) -> Result<Fd, Error> {
    process::open_pidfd(pid)
}

/// Opens a cgroup directory for the binary's atomic placement at spawn.
pub fn open_cgroup(path: &Path) -> Result<Fd, Error> {
    process::open_cgroup(path)
}

/// Polls a pidfd during observer cleanup, observing or reaping only that child.
pub fn poll_child(pidfd: Fd, reap: bool) -> Result<Option<Exit>, Error> {
    process::poll_child(pidfd, reap)
}

/// Sends a keeper's signal through the owned pidfd, including its group.
pub fn signal_kept_child(
    pidfd: Fd,
    signal: skein_io::kernel::Signal,
    to: skein_io::kernel::Target,
) -> Result<(), Error> {
    signal_child(pidfd, signal, to).map(|_| ())
}

/// Closes an exclusively owned test keeper descriptor during cleanup.
pub fn close_keeper_fd(descriptor: Fd) {
    process::close_keeper_fd(descriptor);
}

/// Waits for a keeper's cgroup event file to change, within the given milliseconds.
pub fn wait_cgroup_change(descriptor: Fd, millis: u32) -> Result<(), Error> {
    process::wait_cgroup_change(descriptor, millis)
}

/// Reaps and releases an external child abandoned by a failing test. Normal
/// end-to-end settlement uses the ring (testing-strategy.md, section 2.9).
pub fn abandon_binary(pidfd: Option<Fd>, descriptors: &[Fd]) {
    process::abandon_binary(pidfd, descriptors);
}

/// Sends a signal record to a hosted service's signal pipe (examples.md, section 6).
pub fn write_service_signal(writer: Fd, signal: ServiceSignal) -> Result<(), Error> {
    process::write_signal(writer, signal)
}

/// Opens a signal-record pipe, returning its reader and writer (examples.md, section 6).
pub fn open_signal_pipe() -> Result<(Fd, Fd), Error> {
    process::signal_pipe()
}

/// Sends to this thread's blocked termination signalfd (io.md, section 7).
/// Call only after `open_termination_signals` on the same thread.
pub fn signal_current_thread(signal: ServiceSignal) -> Result<(), Error> {
    process::signal_current_thread(signal)
}

use skein_io::kernel::{
    Addr, Complete, Done, Entry, Error, Exit, Family, Fd, Kind, Op, OpenHow, PERMISSIONS, ServiceSignal, Stat, Submit,
};
use skein_lib::{Queue, Time, Token};

/// How large a [`Kernel`] is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Config {
    /// The most operations in flight at once, cancels included, as io's
    /// operation slab counts them (kernel.md, 5). Both rings are
    /// sized from it, so the completion queue cannot overflow, and
    /// [`Kernel::submit`] takes no record past it.
    pub operations: u32,
}

/// Whether [`Kernel::submit`] blocks for a completion, and until when.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Wait {
    /// Submit and return: work is pending above.
    No,
    /// Block until a completion arrives or the deadline passes, a monotonic
    /// [`Time`] read from the [`Clock`](crate::Clock): the earliest deadline
    /// over every layer (programming-model.md, section 9).
    Until(Time),
    /// Block until a completion arrives: no deadline is armed. Asserts that
    /// something is in flight or ready, since otherwise it never returns.
    Forever,
}

/// Why [`Kernel::open`] refused: the shell does not start (shell.md, 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpenError {
    /// A config of no operations, which could never submit one.
    NoOperations,
    /// The ring could not be set up, with the error number: io_uring is
    /// missing, disabled (`io_uring_disabled`) or refused by a seccomp
    /// profile (`ENOSYS`, `EPERM`), or `EINVAL`: the config is past the
    /// ring's sizes, or the kernel is below the floor and does not know
    /// `SINGLE_ISSUER`, `DEFER_TASKRUN` or `SUBMIT_ALL`.
    Setup(i32),
    /// The kernel is below the floor, 6.12: its ring lacks this operation.
    Missing(&'static str),
    /// The kernel is below the floor, 6.12: its ring has every operation,
    /// but not the absolute wait timeouts, which came with
    /// `IORING_FEAT_MIN_TIMEOUT`.
    BelowFloor,
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::NoOperations => write!(f, "the kernel is configured for no operations"),
            OpenError::Setup(errno) => {
                write!(f, "io_uring could not be set up: {}", io::Error::from_raw_os_error(*errno))
            }
            OpenError::Missing(name) => write!(f, "the kernel is below 6.12: io_uring has no {name}"),
            OpenError::BelowFloor => write!(f, "the kernel is below 6.12: io_uring has no absolute wait timeouts"),
        }
    }
}

impl std::error::Error for OpenError {}

/// io's kernel, on io_uring: opened once, then submitted to and reaped from
/// once per iteration (programming-model.md, section 2).
pub struct Kernel {
    ring: IoUring,
    table: Table,
    /// Neither `Send` nor `Sync`: `SINGLE_ISSUER` and `DEFER_TASKRUN` bind
    /// the ring to the thread that opened it.
    thread: PhantomData<*const ()>,
}

impl fmt::Debug for Kernel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Kernel")
            .field("operations", &self.table.slots.len())
            .field("in_flight", &self.table.tokens.len())
            .field("ready", &self.table.ready.len())
            .finish_non_exhaustive()
    }
}

/// The in-flight table and what finds things in it.
struct Table {
    /// Allocated once, at its final capacity; see the module documentation.
    slots: Box<[Slot]>,
    /// The free slots, the lowest on top.
    free: Vec<u32>,
    /// Slots whose operation the adapter completed itself, oldest first.
    ready: VecDeque<u32>,
    /// Every operation in flight: its token, and its `user_data`.
    tokens: BTreeMap<Token, u64>,
    /// The operations a `Cancel` was submitted for, so an `EINTR` reads as
    /// `Cancelled` on them only.
    cancelled: BTreeSet<Token>,
    /// What `getdents64` writes, for a `List`: the call is synchronous, so
    /// one buffer serves every one.
    listing: Box<[u8]>,
    /// How many times an `Open` was submitted again after `EAGAIN`.
    resubmitted: u64,
}

/// What a reaped entry comes to: a completion to hand up, or an `Open` to
/// submit again, its slot kept.
enum Reaped {
    Complete(Complete),
    Again(u32),
}

struct Slot {
    generation: u32,
    flight: Option<Flight>,
}

/// An operation in flight: the record, and what the kernel reads or writes
/// for it beside the record's own buffers.
struct Flight {
    op: Token,
    kind: Op,
    /// The address a `Bind` or `Connect` asks for, the peer an `Accept`
    /// writes, the address a `Bind` reads back with `getsockname`.
    /// In a cell: the kernel holds a pointer to it while the operation is in
    /// flight (see the module documentation).
    addr: UnsafeCell<libc::sockaddr_storage>,
    addr_len: UnsafeCell<libc::socklen_t>,
    /// An `Open`'s path, or the name a `Rename`, `Remove` or
    /// `MakeDirectory` acts on, with a NUL after it: what the kernel reads.
    path: Box<[u8]>,
    /// A `Rename`'s new name, with a NUL after it.
    to: Box<[u8]>,
    /// How an `Open` opens, which the kernel reads; in a cell, as the
    /// address is.
    open_how: UnsafeCell<types::OpenHow>,
    /// What a `Stat` reads back, which the kernel writes; in a cell, as the
    /// address is.
    statx: UnsafeCell<libc::statx>,
    /// `WaitId` writes this while its ring entry is in flight.
    siginfo: UnsafeCell<libc::siginfo_t>,
    /// One signalfd record read by the ring, decoded before crossing the kernel boundary.
    signal_info: UnsafeCell<libc::signalfd_siginfo>,
    /// How many times an `Open` was submitted again after `EAGAIN`.
    again: u32,
    /// The result, for an operation the adapter completed itself.
    ready: Option<Result<Done, Error>>,
}

/// What a record becomes: one submission entry, or a result the adapter
/// reached itself (a synchronous operation, or a cancel whose target is not
/// in flight).
enum Prepared {
    Entry(squeue::Entry),
    Done(Result<Done, Error>),
}

/// The operations the ring adapter submits, which the kernel's ring must
/// have: all of them at the floor, 6.12.
const OPERATIONS: [(u8, &str); 19] = [
    (opcode::Socket::CODE, "IORING_OP_SOCKET"),
    (opcode::Bind::CODE, "IORING_OP_BIND"),
    (opcode::Listen::CODE, "IORING_OP_LISTEN"),
    (opcode::Accept::CODE, "IORING_OP_ACCEPT"),
    (opcode::Connect::CODE, "IORING_OP_CONNECT"),
    (opcode::Recv::CODE, "IORING_OP_RECV"),
    (opcode::Send::CODE, "IORING_OP_SEND"),
    (opcode::Shutdown::CODE, "IORING_OP_SHUTDOWN"),
    (opcode::Close::CODE, "IORING_OP_CLOSE"),
    (opcode::OpenAt2::CODE, "IORING_OP_OPENAT2"),
    (opcode::Read::CODE, "IORING_OP_READ"),
    (opcode::Write::CODE, "IORING_OP_WRITE"),
    (opcode::Fsync::CODE, "IORING_OP_FSYNC"),
    (opcode::Statx::CODE, "IORING_OP_STATX"),
    (opcode::RenameAt::CODE, "IORING_OP_RENAMEAT"),
    (opcode::UnlinkAt::CODE, "IORING_OP_UNLINKAT"),
    (opcode::MkDirAt::CODE, "IORING_OP_MKDIRAT"),
    (opcode::AsyncCancel::CODE, "IORING_OP_ASYNC_CANCEL"),
    (opcode::WaitId::CODE, "IORING_OP_WAITID"),
];

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// How many times an `Open` is submitted again after `EAGAIN` before its
/// `Other(EAGAIN)` goes up.
const AGAIN: u32 = 16;

/// The bytes of `getdents64`'s buffer: many entries a call, and room for
/// the largest one.
const LISTING: usize = 32 << 10;

/// The path a `Stat` names with `AT_EMPTY_PATH`: the descriptor itself.
const EMPTY: &CStr = c"";

/// The new file's mode a `Create` that asks for none is made with, and a
/// `MakeDirectory`'s, both less the umask (`skein_io::kernel`, backend
/// defaults).
const FILE_MODE: u64 = 0o666;
const DIRECTORY_MODE: libc::mode_t = 0o777;

impl Kernel {
    /// Sets up the ring for this thread alone: one issuer, and completions
    /// processed only when the loop enters the ring (shell.md, 3).
    /// Probes the ring for every operation it uses, and refuses to open below
    /// the floor.
    pub fn open(config: Config) -> Result<Kernel, OpenError> {
        let operations = config.operations;
        if operations == 0 {
            return Err(OpenError::NoOperations);
        }
        let built = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_submit_all()
            .setup_cqsize(operations)
            .build(operations);
        let ring: IoUring = match built {
            Ok(ring) => ring,
            Err(error) => return Err(OpenError::Setup(errno_of(&error))),
        };
        let mut probe = Probe::new();
        if let Err(error) = ring.submitter().register_probe(&mut probe) {
            return Err(OpenError::Setup(errno_of(&error)));
        }
        for (code, name) in OPERATIONS {
            if !probe.is_supported(code) {
                return Err(OpenError::Missing(name));
            }
        }
        if !ring.params().is_feature_min_timeout() {
            return Err(OpenError::BelowFloor);
        }
        assert!(
            ring.params().sq_entries() >= operations && ring.params().cq_entries() >= operations,
            "the kernel sizes each ring at least as asked"
        );
        if let Err(errno) = process::block_sigpipe() {
            return Err(OpenError::Setup(errno));
        }
        Ok(Kernel { ring, table: Table::new(operations), thread: PhantomData })
    }

    /// How many operations are in flight, those completed but not yet reaped
    /// included.
    #[must_use]
    pub fn in_flight(&self) -> u32 {
        u32::try_from(self.table.tokens.len()).expect("no more in flight than the config's u32")
    }

    /// How many more records [`submit`](Kernel::submit) would take now.
    #[must_use]
    pub fn room(&self) -> u32 {
        u32::try_from(self.table.free.len()).expect("no more slots than the config's u32")
    }

    /// Takes records from the front of `records` while a slot is free, maps
    /// each onto one submission entry (or completes it itself, at the next
    /// reap, when it is not a ring operation), submits them, and waits as
    /// `wait` says. A record that finds no free slot stays in `records`.
    ///
    /// Panics on the contract's broken invariants that the ring checks: a
    /// record that is not [`Op::is_valid`], and a token already in flight;
    /// and on [`Wait::Forever`] with nothing in flight or ready, which would
    /// never return.
    pub fn submit(&mut self, records: &mut Queue<Submit>, wait: Wait) {
        while !self.table.free.is_empty() {
            let Some(record) = records.pop() else {
                break;
            };
            self.start(record);
        }
        self.enter(wait);
    }

    /// Moves completions into `out` while it has room, each with its record
    /// handed back: first those the adapter reached itself, then the ring's.
    /// What does not fit waits for the next reap.
    pub fn reap(&mut self, out: &mut Queue<Complete>) {
        while out.room() > 0 {
            let Some(index) = self.table.ready.pop_front() else {
                break;
            };
            let (flight, _cancelled) = self.table.retire(index);
            let result = flight.ready.expect("a ready slot holds its result");
            out.push(Complete { op: flight.op, kind: flight.kind, result });
        }
        let mut again = Vec::new();
        let mut completions = self.ring.completion();
        while out.room() > 0 {
            let Some(entry) = completions.next() else {
                break;
            };
            match self.table.complete(entry.user_data(), entry.result()) {
                Reaped::Complete(complete) => out.push(complete),
                Reaped::Again(index) => again.push(index),
            }
        }
        drop(completions);
        for index in again {
            self.resubmit(index);
        }
    }

    /// How many times an `Open` was submitted again after `EAGAIN`, its `..`
    /// having raced a rename or a mount: for tests, which provoke it.
    #[must_use]
    pub const fn resubmitted(&self) -> u64 {
        self.table.resubmitted
    }

    /// Pushes an `Open` that failed with `EAGAIN` again, in its slot under the
    /// same `user_data`; the next enter submits it.
    fn resubmit(&mut self, index: u32) {
        let slot = self.table.slots.get(slot_index(index)).expect("an index in flight names a slot");
        let flight = slot.flight.as_ref().expect("an operation submitted again is in its slot");
        let Op::Open { root, .. } = flight.kind else {
            unreachable!("only an Open is submitted again");
        };
        let entry = open_entry(root, &flight.path, &flight.open_how).user_data(user_data(index, slot.generation));
        // SAFETY: as at the first push (`start`): the path and the
        // `open_how` the entry points at are this slot's, which stays in
        // place, untouched by Rust code, until the new completion is reaped;
        // the kernel had done with them, having posted the first.
        let pushed = unsafe { self.ring.submission().push(&entry) };
        pushed.expect("the submission queue has an entry for every slot");
        self.table.resubmitted = self.table.resubmitted.saturating_add(1);
    }

    fn start(&mut self, record: Submit) {
        let Submit { op, kind } = record;
        assert!(kind.is_valid(), "io submits only valid records (skein_io::kernel, broken invariants)");
        assert!(!self.table.tokens.contains_key(&op), "io never reuses a token in flight (skein_io::kernel)");
        let index = self.table.free.pop().expect("a record is taken only while a slot is free");
        let Table { slots, tokens, cancelled, ready, listing, .. } = &mut self.table;
        let slot = slots.get_mut(slot_index(index)).expect("a free index names a slot");
        assert!(slot.flight.is_none(), "a free slot holds no operation");
        let user_data = user_data(index, slot.generation);
        let flight = slot.flight.insert(Flight::new(op, kind));
        match prepare(flight, tokens, cancelled, listing) {
            Prepared::Entry(entry) => {
                let entry = entry.user_data(user_data);
                // SAFETY: every pointer in the entry points into this slot's
                // flight (its address and length, its `open_how`, its
                // `statx`), into the paths it owns, or into its record's
                // `Box` buffers. The slot's table is never resized, and the
                // flight stays in place, untouched by Rust code, until
                // `complete` takes it after the kernel posts this entry's
                // completion.
                let pushed = unsafe { self.ring.submission().push(&entry) };
                pushed.expect("the submission queue has an entry for every slot");
            }
            Prepared::Done(result) => {
                flight.ready = Some(result);
                ready.push_back(index);
            }
        }
        let previous = tokens.insert(op, user_data);
        assert!(previous.is_none(), "the token was checked not in flight");
    }

    /// Submits what was pushed, and runs the completions the kernel deferred
    /// until now (`GETEVENTS`, always), waiting as `wait` says. A completion
    /// the adapter reached itself is already waiting, so it never blocks.
    fn enter(&mut self, wait: Wait) {
        let pending = u32::try_from(self.ring.submission().len()).expect("no more entries than the config's u32");
        let blocks = self.table.ready.is_empty();
        let flags = types::SubmitArgs::new();
        let entered = match wait {
            Wait::Until(deadline) if blocks => {
                let nanos = deadline.as_nanos();
                let subsec = u32::try_from(nanos.rem_euclid(NANOS_PER_SEC)).expect("below a second");
                let timespec = types::Timespec::new().sec(nanos.div_euclid(NANOS_PER_SEC)).nsec(subsec);
                let args = flags.timespec(&timespec);
                let bits = EnterFlags::GETEVENTS | EnterFlags::EXT_ARG | EnterFlags::ABS_TIMER;
                // SAFETY: `args` is the `io_uring_getevents_arg` that `EXT_ARG`
                // announces (`SubmitArgs` is its transparent wrapper), and it
                // and the timespec it points at live across the call;
                // `pending` entries were pushed and published.
                unsafe { self.ring.submitter().enter(pending, 1, bits.bits(), Some(&args)) }
            }
            Wait::No | Wait::Until(_) => self.enter_plain(pending, 0),
            Wait::Forever => {
                assert!(
                    !blocks || !self.table.tokens.is_empty(),
                    "waiting forever with nothing in flight never returns"
                );
                self.enter_plain(pending, u32::from(blocks))
            }
        };
        let Err(error) = entered else {
            return;
        };
        match errno_of(&error) {
            // The deadline passed, or a signal came: back to the loop, which
            // decides what is due. Out of kernel memory for a submission:
            // the entries stay queued, and the next enter submits them.
            libc::ETIME | libc::EINTR | libc::EAGAIN | libc::ENOMEM => {}
            other => panic!("the ring refused an enter: {}", io::Error::from_raw_os_error(other)),
        }
    }

    fn enter_plain(&self, pending: u32, min_complete: u32) -> io::Result<usize> {
        let bits = EnterFlags::GETEVENTS.bits();
        // SAFETY: no extended argument (a null signal mask), and `pending`
        // entries were pushed and published.
        unsafe { self.ring.submitter().enter::<libc::sigset_t>(pending, min_complete, bits, None) }
    }
}

/// Dropping a kernel with operations in flight is a shutdown path: the ring
/// is closed, the kernel cancels what it still holds in its own time, and
/// the table, with every record and buffer in it, is leaked so that nothing
/// the kernel may still write is freed. Descriptors are not closed: only a
/// `Close` record closes one.
impl Drop for Kernel {
    fn drop(&mut self) {
        if self.table.tokens.is_empty() {
            return;
        }
        let slots = mem::take(&mut self.table.slots);
        let _leaked: *mut [Slot] = Box::into_raw(slots);
    }
}

impl Table {
    fn new(operations: u32) -> Table {
        let capacity = slot_index(operations);
        let mut slots = Vec::with_capacity(capacity);
        let mut free = Vec::with_capacity(capacity);
        for index in (0..operations).rev() {
            slots.push(Slot { generation: 0, flight: None });
            free.push(index);
        }
        Table {
            slots: slots.into_boxed_slice(),
            free,
            ready: VecDeque::with_capacity(capacity),
            tokens: BTreeMap::new(),
            cancelled: BTreeSet::new(),
            listing: vec![0; LISTING].into_boxed_slice(),
            resubmitted: 0,
        }
    }

    /// Takes the operation out of its slot, and frees the slot for the next
    /// one under a new generation.
    /// Also says whether a `Cancel` was submitted for it.
    fn retire(&mut self, index: u32) -> (Flight, bool) {
        let slot = self.slots.get_mut(slot_index(index)).expect("an index in flight names a slot");
        let flight = slot.flight.take().expect("a slot in flight holds its operation");
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(index);
        let removed = self.tokens.remove(&flight.op);
        assert!(removed.is_some(), "an operation in flight has its token");
        let cancelled = self.cancelled.remove(&flight.op);
        (flight, cancelled)
    }

    /// The completion of the ring entry named `user_data`, its result `res`
    /// decoded.
    fn complete(&mut self, user_data: u64, res: i32) -> Reaped {
        let (index, generation) = split(user_data);
        let slot = self.slots.get_mut(slot_index(index)).expect("a completion names a slot");
        assert!(slot.generation == generation, "every completion names an operation in flight, once");
        let flight = slot.flight.as_mut().expect("every completion names an operation in flight, once");
        // RESOLVE_BENEATH answers EAGAIN when a `..` raced a rename or a
        // mount anywhere on the system: the same Open is likely to pass.
        if let Op::Open { .. } = flight.kind
            && res == -libc::EAGAIN
            && flight.again < AGAIN
            && !self.cancelled.contains(&flight.op)
        {
            flight.again = flight.again.saturating_add(1);
            return Reaped::Again(index);
        }
        let (mut flight, cancelled) = self.retire(index);
        let result = decode(&mut flight, res, cancelled);
        Reaped::Complete(Complete { op: flight.op, kind: flight.kind, result })
    }
}

impl Flight {
    fn new(op: Token, kind: Op) -> Flight {
        Flight {
            op,
            kind,
            addr: UnsafeCell::new(zeroed_storage()),
            addr_len: UnsafeCell::new(0),
            path: Box::default(),
            to: Box::default(),
            open_how: UnsafeCell::new(types::OpenHow::new()),
            statx: UnsafeCell::new(zeroed_statx()),
            // SAFETY: siginfo_t is an integer-bearing C output structure;
            // waitid fills it before decode reads its fields.
            siginfo: UnsafeCell::new(unsafe { mem::zeroed() }),
            // SAFETY: signalfd_siginfo is a plain output structure, filled by Read.
            signal_info: UnsafeCell::new(unsafe { mem::zeroed() }),
            again: 0,
            ready: None,
        }
    }
}

/// Maps a record onto its one submission entry, with the backend defaults
/// that are not records (`skein_io::kernel`): close-on-exec on every new
/// descriptor, `SO_REUSEADDR` on a socket that binds (set here, before the
/// `Bind`), `MSG_NOSIGNAL` on every send, the modes of new files and
/// directories. A `Cancel` whose target is not in flight is too late,
/// without asking the kernel; a `List` runs here, synchronously.
#[expect(clippy::too_many_lines, reason = "one exhaustive operation table maps each record to its ring entry")]
fn prepare(
    flight: &mut Flight,
    tokens: &BTreeMap<Token, u64>,
    cancelled: &mut BTreeSet<Token>,
    listing: &mut [u8],
) -> Prepared {
    let Flight { kind, addr, addr_len, path, to: to_path, open_how, statx, siginfo, signal_info, .. } = flight;
    let entry = match kind {
        Op::Socket { family } => {
            let kind = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
            opcode::Socket::new(domain(*family), kind, libc::IPPROTO_TCP).build()
        }
        Op::Bind { fd, addr: to } => {
            let (fd, to) = (*fd, *to);
            if let Err(errno) = set_option(fd.raw(), libc::SOL_SOCKET, libc::SO_REUSEADDR) {
                return Prepared::Done(Err(error(kind, errno, false)));
            }
            let len = encode(&to, addr.get_mut());
            *addr_len.get_mut() = len;
            opcode::Bind::new(types::Fd(fd.raw()), addr.get().cast_const().cast(), len).build()
        }
        Op::Listen { fd, backlog } => {
            // The backlog is a hint, clamped by the kernel to somaxconn.
            let backlog = i32::try_from(*backlog).unwrap_or(i32::MAX);
            opcode::Listen::new(types::Fd(fd.raw()), backlog).build()
        }
        Op::Accept { fd } => {
            *addr_len.get_mut() = socklen_of::<libc::sockaddr_storage>();
            opcode::Accept::new(types::Fd(fd.raw()), addr.get().cast(), addr_len.get())
                .flags(libc::SOCK_CLOEXEC)
                .build()
        }
        Op::Connect { fd, addr: to } => {
            let len = encode(to, addr.get_mut());
            *addr_len.get_mut() = len;
            opcode::Connect::new(types::Fd(fd.raw()), addr.get().cast_const().cast(), len).build()
        }
        Op::Recv { fd, buf } => {
            let len = u32::try_from(buf.len()).expect("a valid Recv buffer's length is a count");
            opcode::Recv::new(types::Fd(fd.raw()), buf.as_mut_ptr(), len).build()
        }
        Op::PipeRead { fd, buf } => {
            let len = u32::try_from(buf.len()).expect("a valid pipe read buffer's length is a count");
            opcode::Read::new(types::Fd(fd.raw()), buf.as_mut_ptr(), len).offset(u64::MAX).build()
        }
        Op::Send { fd, bytes, from } => {
            let from = usize::try_from(*from).expect("a u32 fits in a usize");
            let left = bytes.get(from..).expect("a valid Send has bytes left from its offset");
            let len = u32::try_from(left.len()).expect("a valid Send's length is a count");
            opcode::Send::new(types::Fd(fd.raw()), left.as_ptr(), len).flags(libc::MSG_NOSIGNAL).build()
        }
        Op::PipeWrite { fd, bytes, from } => {
            let start = usize::try_from(*from).expect("a u32 fits in a usize");
            let left = bytes.get(start..).expect("a valid pipe write has bytes left");
            let len = u32::try_from(left.len()).expect("a valid pipe write's length is a count");
            opcode::Write::new(types::Fd(fd.raw()), left.as_ptr(), len).offset(u64::MAX).build()
        }
        Op::Shutdown { fd } => opcode::Shutdown::new(types::Fd(fd.raw()), libc::SHUT_WR).build(),
        Op::Close { fd } => opcode::Close::new(types::Fd(fd.raw())).build(),
        Op::Open { root, path: asked, how } => {
            *path = c_path(asked);
            *open_how.get_mut() = open(*how);
            open_entry(*root, path, open_how)
        }
        Op::Read { fd, buf, at } => {
            let len = u32::try_from(buf.len()).expect("a valid Read buffer's length is a count");
            opcode::Read::new(types::Fd(fd.raw()), buf.as_mut_ptr(), len).offset(*at).build()
        }
        Op::Write { fd, bytes, from, at } => {
            let from = usize::try_from(*from).expect("a u32 fits in a usize");
            let left = bytes.get(from..).expect("a valid Write has bytes left from its offset");
            let len = u32::try_from(left.len()).expect("a valid Write's length is a count");
            opcode::Write::new(types::Fd(fd.raw()), left.as_ptr(), len).offset(*at).build()
        }
        Op::Append { fd, bytes, from } => {
            let from = usize::try_from(*from).expect("a u32 fits in a usize");
            let left = bytes.get(from..).expect("a valid Append has bytes left");
            let len = u32::try_from(left.len()).expect("a valid Append counts bytes");
            opcode::Write::new(types::Fd(fd.raw()), left.as_ptr(), len).offset(u64::MAX).build()
        }
        Op::Sync { fd } => opcode::Fsync::new(types::Fd(fd.raw())).build(),
        Op::Stat { fd } => opcode::Statx::new(types::Fd(fd.raw()), EMPTY.as_ptr(), statx.get().cast())
            .flags(libc::AT_EMPTY_PATH)
            .mask(libc::STATX_TYPE | libc::STATX_SIZE | libc::STATX_MODE)
            .build(),
        Op::Rename { from_dir, from, to_dir, to } => {
            *path = c_path(from);
            *to_path = c_path(to);
            let (from_dir, to_dir) = (types::Fd(from_dir.raw()), types::Fd(to_dir.raw()));
            opcode::RenameAt::new(from_dir, path.as_ptr().cast(), to_dir, to_path.as_ptr().cast()).build()
        }
        Op::Remove { dir, name, directory } => {
            *path = c_path(name);
            let flags = if *directory { libc::AT_REMOVEDIR } else { 0 };
            opcode::UnlinkAt::new(types::Fd(dir.raw()), path.as_ptr().cast()).flags(flags).build()
        }
        Op::MakeDirectory { dir, name } => {
            *path = c_path(name);
            opcode::MkDirAt::new(types::Fd(dir.raw()), path.as_ptr().cast()).mode(DIRECTORY_MODE).build()
        }
        Op::List { fd, entries, names } => {
            let listed = list(fd.raw(), entries, names, listing);
            return Prepared::Done(match listed {
                Ok(n) => Ok(Done::Count(n)),
                Err(errno) => Err(error(kind, errno, false)),
            });
        }
        Op::Spawn { spawn: command } => return Prepared::Done(spawn(command)),
        Op::Signal { pidfd, signal, to } => return Prepared::Done(signal_child(*pidfd, *signal, *to)),
        Op::Usage => return Prepared::Done(process::usage().map(Done::Usage)),
        Op::ReadSignal { fd } => opcode::Read::new(
            types::Fd(fd.raw()),
            signal_info.get().cast(),
            u32::try_from(size_of::<libc::signalfd_siginfo>()).expect("signal record fits u32"),
        )
        .offset(u64::MAX)
        .build(),
        Op::Wait { pidfd, reap } => opcode::WaitId::new(
            libc::P_PIDFD,
            u32::try_from(pidfd.raw()).expect("a pidfd is positive"),
            libc::WEXITED | if *reap { 0 } else { libc::WNOWAIT },
        )
        .infop(siginfo.get().cast_const())
        .build(),
        Op::Cancel { target } => {
            let Some(target_data) = tokens.get(target) else {
                return Prepared::Done(Err(Error::TooLate));
            };
            let _first: bool = cancelled.insert(*target);
            opcode::AsyncCancel::new(*target_data).build()
        }
    };
    Prepared::Entry(entry)
}

/// The result of an operation the kernel completed with `res`, with the
/// backend defaults applied to a new socket: `TCP_NODELAY` on every TCP
/// socket (so on every connected one) and on every accepted one, and
/// `IPV6_V6ONLY` on every IPv6 socket, before io can bind it. A `Bind` reads
/// the address bound back with `getsockname`.
fn decode(flight: &mut Flight, res: i32, cancelled: bool) -> Result<Done, Error> {
    let Flight { kind, addr, addr_len, statx, siginfo, signal_info, .. } = flight;
    let (addr, addr_len) = (addr.get_mut(), addr_len.get_mut());
    if res < 0 {
        return Err(error(kind, 0_i32.saturating_sub(res), cancelled));
    }
    match kind {
        Op::Socket { family } => {
            let mut configured = set_option(res, libc::IPPROTO_TCP, libc::TCP_NODELAY);
            if configured.is_ok() && *family == Family::Ipv6 {
                configured = set_option(res, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY);
            }
            match configured {
                Ok(()) => Ok(Done::Fd(Fd::new(res))),
                // Not the operation's own error, so not mapped as one: an
                // EINVAL here is no bug of io's record.
                Err(errno) => {
                    close(res);
                    Err(Error::Other(errno))
                }
            }
        }
        Op::Accept { .. } => {
            // Both are kernel answers that cannot happen on a TCP socket the
            // kernel just accepted; failing here would take a connection
            // the contract says a failed Accept leaves waiting.
            let configured = set_option(res, libc::IPPROTO_TCP, libc::TCP_NODELAY);
            assert!(configured.is_ok(), "TCP_NODELAY applies to every accepted TCP socket: {configured:?}");
            let peer = decode_addr(addr, *addr_len).expect("an accepted TCP socket's peer is IPv4 or IPv6");
            Ok(Done::Accepted { fd: Fd::new(res), peer })
        }
        Op::Bind { fd, .. } => {
            let fd = *fd;
            match local_addr(fd.raw(), addr, addr_len) {
                Ok(()) => match decode_addr(addr, *addr_len) {
                    Some(bound) => Ok(Done::Bound(bound)),
                    None => Err(Error::Other(libc::EAFNOSUPPORT)),
                },
                Err(errno) => Err(error(kind, errno, false)),
            }
        }
        Op::Recv { .. }
        | Op::Send { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Append { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. } => Ok(Done::Count(u32::try_from(res).expect("a non-negative i32 fits in a u32"))),
        Op::Open { .. } => opened(res),
        Op::Stat { .. } => Ok(Done::Stat(stat(statx.get_mut()))),
        Op::Wait { .. } => {
            let info = siginfo.get_mut();
            // SAFETY: a successful waitid filled `info`; si_status reads
            // the status union selected by si_code.
            let status = unsafe { info.si_status() };
            match info.si_code {
                libc::CLD_EXITED => {
                    Ok(Done::Exit(Exit::Code(u8::try_from(status).expect("a child exit code fits in a byte"))))
                }
                libc::CLD_KILLED | libc::CLD_DUMPED => {
                    Ok(Done::Exit(Exit::Signal(u32::try_from(status).expect("a signal is positive"))))
                }
                _ => Err(Error::Other(libc::ECHILD)),
            }
        }
        Op::ReadSignal { .. } => {
            if usize::try_from(res).expect("non-negative result") != size_of::<libc::signalfd_siginfo>() {
                return Err(Error::Other(libc::EIO));
            }
            let signal = match signal_info.get_mut().ssi_signo {
                value if value == u32::try_from(libc::SIGINT).expect("positive signal") => ServiceSignal::Interrupt,
                value if value == u32::try_from(libc::SIGTERM).expect("positive signal") => ServiceSignal::Terminate,
                _ => return Err(Error::Other(libc::EINVAL)),
            };
            Ok(Done::ServiceSignal(signal))
        }
        Op::Listen { .. }
        | Op::Connect { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Sync { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::Cancel { .. } => Ok(Done::Nothing),
        Op::Usage | Op::List { .. } | Op::Spawn { .. } | Op::Signal { .. } => {
            unreachable!("a synchronous operation completes at its submit")
        }
    }
}

/// The error number `errno` of `kind`, as `skein_io::kernel::Error`
/// documents it, per operation. `cancelled` says a `Cancel` was submitted
/// for the operation, so an `EINTR` is that cancel landing.
fn error(kind: &Op, errno: i32, cancelled: bool) -> Error {
    match kind {
        Op::Cancel { .. } => match errno {
            libc::ENOENT | libc::EALREADY => Error::TooLate,
            libc::EINVAL => Error::InvalidArgument,
            other => Error::Other(other),
        },
        // Cancelled only if a Cancel was submitted for it, which io does
        // for an Open, a Read, a Write or a Sync only.
        Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Append { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. } => match errno {
            libc::ECANCELED | libc::EINTR if cancelled => Error::Cancelled,
            libc::ENOBUFS | libc::ENOMEM => Error::NoBufferSpace,
            libc::EINVAL => Error::InvalidArgument,
            other => file_error(kind, other),
        },
        Op::Usage | Op::Spawn { .. } | Op::Signal { .. } => Error::Other(errno),
        Op::ReadSignal { .. } => match errno {
            libc::ECANCELED => Error::Cancelled,
            libc::EINTR if cancelled => Error::Cancelled,
            libc::EINVAL => Error::InvalidArgument,
            other => Error::Other(other),
        },
        Op::Wait { .. } => match errno {
            libc::ECANCELED | libc::EINTR if cancelled => Error::Cancelled,
            libc::ENOBUFS | libc::ENOMEM => Error::NoBufferSpace,
            libc::EINVAL => Error::InvalidArgument,
            other => Error::Other(other),
        },
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. } => match errno {
            libc::ECANCELED => Error::Cancelled,
            libc::EINTR if cancelled => Error::Cancelled,
            libc::ENOBUFS | libc::ENOMEM => Error::NoBufferSpace,
            libc::EINVAL | libc::EAFNOSUPPORT => Error::InvalidArgument,
            other => operation_error(kind, other),
        },
    }
}

/// The errors that mean something on one operation and not another.
fn operation_error(kind: &Op, errno: i32) -> Error {
    let named = match kind {
        Op::Socket { .. } => match errno {
            libc::EMFILE | libc::ENFILE => Some(Error::TooManyOpenFiles),
            _ => None,
        },
        Op::Bind { .. } => match errno {
            libc::EADDRINUSE => Some(Error::AddressInUse),
            libc::EADDRNOTAVAIL => Some(Error::AddressNotAvailable),
            _ => None,
        },
        Op::Listen { .. } => match errno {
            libc::EADDRINUSE => Some(Error::AddressInUse),
            _ => None,
        },
        Op::Accept { .. } => match errno {
            libc::EMFILE | libc::ENFILE => Some(Error::TooManyOpenFiles),
            libc::ECONNABORTED => Some(Error::Reset),
            _ => None,
        },
        Op::Connect { .. } => match errno {
            libc::ECONNREFUSED => Some(Error::Refused),
            libc::EADDRNOTAVAIL => Some(Error::AddressNotAvailable),
            other => stream_error(other),
        },
        Op::Recv { .. } => match errno {
            libc::ENOTCONN => Some(Error::NotConnected),
            other => stream_error(other),
        },
        Op::Send { .. } => match errno {
            libc::EPIPE => Some(Error::BrokenPipe),
            other => stream_error(other),
        },
        Op::PipeRead { .. } | Op::PipeWrite { .. } => match errno {
            libc::EPIPE => Some(Error::BrokenPipe),
            _ => None,
        },
        Op::Shutdown { .. } => match errno {
            libc::ENOTCONN => Some(Error::NotConnected),
            _ => None,
        },
        Op::Close { .. } => None,
        Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Append { .. }
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
        | Op::Cancel { .. } => unreachable!("an operation on files' errors, and a Cancel's, are mapped apart"),
    };
    named.unwrap_or(Error::Other(errno))
}

/// The errors of an operation on files: each names those the contract
/// tables for it (`skein_io::kernel`), and anything else is `Other`.
fn file_error(kind: &Op, errno: i32) -> Error {
    let named = match kind {
        Op::Open { .. } => match errno {
            libc::ENOENT => Some(Error::NotFound),
            libc::EEXIST => Some(Error::Exists),
            libc::ENOTDIR => Some(Error::NotADirectory),
            libc::EISDIR => Some(Error::IsADirectory),
            libc::EACCES | libc::EPERM => Some(Error::Permission),
            libc::ENOSPC | libc::EDQUOT => Some(Error::NoSpace),
            libc::EROFS => Some(Error::ReadOnly),
            libc::ELOOP => Some(Error::TooManyLinks),
            libc::ENAMETOOLONG => Some(Error::NameTooLong),
            // RESOLVE_BENEATH's answer to a path out of its root.
            libc::EXDEV => Some(Error::Escape),
            // A socket, or a device with no driver.
            libc::ENXIO => Some(Error::NotAFile),
            libc::EMFILE | libc::ENFILE => Some(Error::TooManyOpenFiles),
            _ => None,
        },
        Op::Read { .. } => match errno {
            libc::EISDIR => Some(Error::IsADirectory),
            _ => None,
        },
        Op::Write { .. } | Op::Append { .. } => match errno {
            libc::ENOSPC | libc::EDQUOT => Some(Error::NoSpace),
            libc::EROFS => Some(Error::ReadOnly),
            _ => None,
        },
        Op::Sync { .. } => match errno {
            libc::ENOSPC | libc::EDQUOT => Some(Error::NoSpace),
            _ => None,
        },
        Op::Stat { .. } => None,
        // EXDEV here is two filesystems, not an escape: Other.
        Op::Rename { .. } => match errno {
            libc::ENOENT => Some(Error::NotFound),
            libc::ENOTDIR => Some(Error::NotADirectory),
            libc::EISDIR => Some(Error::IsADirectory),
            libc::ENOTEMPTY | libc::EEXIST => Some(Error::NotEmpty),
            libc::EACCES | libc::EPERM => Some(Error::Permission),
            libc::ENOSPC | libc::EDQUOT => Some(Error::NoSpace),
            libc::EROFS => Some(Error::ReadOnly),
            libc::EMLINK => Some(Error::TooManyLinks),
            libc::ENAMETOOLONG => Some(Error::NameTooLong),
            _ => None,
        },
        Op::Remove { .. } => match errno {
            libc::ENOENT => Some(Error::NotFound),
            libc::ENOTDIR => Some(Error::NotADirectory),
            libc::EISDIR => Some(Error::IsADirectory),
            libc::ENOTEMPTY | libc::EEXIST => Some(Error::NotEmpty),
            libc::EACCES | libc::EPERM => Some(Error::Permission),
            libc::EROFS => Some(Error::ReadOnly),
            libc::ENAMETOOLONG => Some(Error::NameTooLong),
            _ => None,
        },
        Op::MakeDirectory { .. } => match errno {
            libc::EEXIST => Some(Error::Exists),
            libc::ENOENT => Some(Error::NotFound),
            libc::ENOTDIR => Some(Error::NotADirectory),
            libc::EACCES | libc::EPERM => Some(Error::Permission),
            libc::ENOSPC | libc::EDQUOT => Some(Error::NoSpace),
            libc::EROFS => Some(Error::ReadOnly),
            libc::EMLINK => Some(Error::TooManyLinks),
            libc::ENAMETOOLONG => Some(Error::NameTooLong),
            _ => None,
        },
        Op::List { .. } => match errno {
            libc::ENOENT => Some(Error::NotFound),
            libc::ENOTDIR => Some(Error::NotADirectory),
            libc::ENAMETOOLONG => Some(Error::NameTooLong),
            _ => None,
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
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::Cancel { .. } => unreachable!("only an operation on files' errors are mapped here"),
    };
    named.unwrap_or(Error::Other(errno))
}

/// An `Open`'s entry, of the path and the `open_how` its slot holds.
fn open_entry(root: Fd, path: &[u8], open_how: &UnsafeCell<types::OpenHow>) -> squeue::Entry {
    opcode::OpenAt2::new(types::Fd(root.raw()), path.as_ptr().cast(), open_how.get().cast_const()).build()
}

/// `bytes` and a NUL after them: the string the kernel reads for a path or
/// a name, in which a valid record holds no NUL.
fn c_path(bytes: &[u8]) -> Box<[u8]> {
    let mut path = Vec::with_capacity(bytes.len().checked_add(1).expect("a path's length and one more fit a usize"));
    path.extend_from_slice(bytes);
    path.push(0);
    path.into_boxed_slice()
}

/// What an `Open` that the kernel answered with the descriptor `fd` comes
/// to: a file or a directory, or `NotAFile`, the descriptor closed. A file
/// blocks again, so that io_uring sends a read it cannot do at once to its
/// worker rather than answer `EAGAIN`.
fn opened(fd: i32) -> Result<Done, Error> {
    // SAFETY: `libc::stat` is a plain C struct of integers, for which all
    // zeroes is a valid value.
    let mut raw: libc::stat = unsafe { mem::zeroed() };
    // SAFETY: `raw` is a live, exclusive borrow of a `stat`, which the
    // kernel writes during the call only; `fd` was just opened for this
    // adapter.
    let stated = unsafe { libc::fstat(fd, ptr::from_mut(&mut raw)) };
    let checked = if stated != 0 {
        Err(Error::Other(last_errno()))
    } else {
        match kind_of(raw.st_mode) {
            Kind::File => blocking(fd),
            Kind::Directory => Ok(()),
            Kind::Symlink | Kind::Other => Err(Error::NotAFile),
        }
    };
    match checked {
        Ok(()) => Ok(Done::Fd(Fd::new(fd))),
        Err(error) => {
            close(fd);
            Err(error)
        }
    }
}

/// The status flags of what is open on `fd` (`F_GETFL`), or the error
/// number.
pub(crate) fn status_flags(fd: i32) -> Result<i32, i32> {
    // SAFETY: integers only.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 { Err(last_errno()) } else { Ok(flags) }
}

/// Clears `O_NONBLOCK` from the file open on `fd`.
fn blocking(fd: i32) -> Result<(), Error> {
    let flags = match status_flags(fd) {
        Ok(flags) => flags,
        Err(errno) => return Err(Error::Other(errno)),
    };
    // SAFETY: integers only.
    let set = unsafe { libc::fcntl(fd, libc::F_SETFL, flags & !libc::O_NONBLOCK) };
    if set < 0 { Err(Error::Other(last_errno())) } else { Ok(()) }
}

/// What `openat2` is told for each way io opens: close-on-exec always,
/// without blocking or taking a controlling terminal, and resolved beneath
/// the root, without magic links (kernel.md, 6.1).
fn open(how: OpenHow) -> types::OpenHow {
    let no_follow = matches!(how, OpenHow::ReadNoFollow | OpenHow::DirectoryNoFollow | OpenHow::CreateNoFollow { .. });
    let (flags, mode) = match how {
        OpenHow::Read | OpenHow::ReadNoFollow => (libc::O_RDONLY, 0),
        OpenHow::Directory | OpenHow::DirectoryNoFollow => (libc::O_RDONLY | libc::O_DIRECTORY, 0),
        OpenHow::Create { mode } | OpenHow::CreateNoFollow { mode } => {
            let mode = match mode {
                Some(mode) => u64::from(mode),
                None => FILE_MODE,
            };
            (libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL, mode)
        }
    };
    let flags = flags | libc::O_CLOEXEC | libc::O_NONBLOCK | libc::O_NOCTTY;
    let flags = u64::try_from(flags).expect("open's flags are positive");
    let resolve = if no_follow {
        libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS
    } else {
        libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS
    };
    types::OpenHow::new().flags(flags).mode(mode).resolve(resolve)
}

/// What a `Stat`'s `statx` found.
fn stat(raw: &libc::statx) -> Stat {
    let kind = if raw.stx_mask & libc::STATX_TYPE == 0 { Kind::Other } else { kind_of(u32::from(raw.stx_mode)) };
    let mode = if raw.stx_mask & libc::STATX_MODE == 0 { 0 } else { u32::from(raw.stx_mode) & PERMISSIONS };
    Stat { kind, size: raw.stx_size, mode }
}

/// The kind of a file whose mode is `mode`.
fn kind_of(mode: libc::mode_t) -> Kind {
    match mode & libc::S_IFMT {
        libc::S_IFREG => Kind::File,
        libc::S_IFDIR => Kind::Directory,
        libc::S_IFLNK => Kind::Symlink,
        _ => Kind::Other,
    }
}

/// The kind of a `getdents64` entry, or `None` when its filesystem did not
/// say (`DT_UNKNOWN`).
fn kind_of_entry(d_type: u8) -> Option<Kind> {
    match d_type {
        libc::DT_REG => Some(Kind::File),
        libc::DT_DIR => Some(Kind::Directory),
        libc::DT_LNK => Some(Kind::Symlink),
        libc::DT_UNKNOWN => None,
        _ => Some(Kind::Other),
    }
}

/// The offsets of a `linux_dirent64`'s fields: `d_off`, `d_reclen`,
/// `d_type`, and where `d_name` starts.
const D_OFF: usize = 8;
const D_RECLEN: usize = 16;
const D_TYPE: usize = 18;
const D_NAME: usize = 19;

/// A `List` (`skein_io::kernel`): `getdents64` into `listing`, the entries
/// copied out until `entries` or `names` is full, `.` and `..` skipped,
/// and the directory's position set back to just past the last entry taken,
/// so that the next `List` starts there. A filesystem that does not say an
/// entry's kind is asked with `fstatat`. Fails with the error number of a
/// call, or `ENAMETOOLONG` for a next name longer than all of `names`, when
/// no entry was taken.
pub(crate) fn list(fd: i32, entries: &mut [Entry], names: &mut [u8], listing: &mut [u8]) -> Result<u32, i32> {
    let mut count = 0_usize;
    let mut used = 0_usize;
    // Where the next List starts: past the last entry taken.
    let mut resume = seek(fd, 0, libc::SEEK_CUR)?;
    while count < entries.len() {
        let read = match getdents(fd, listing) {
            Ok(0) => break,
            Ok(read) => read,
            Err(errno) if count == 0 => return Err(errno),
            // Those taken are handed back; the next List meets the error.
            Err(_) => break,
        };
        let mut at = 0_usize;
        while at < read {
            let record = listing.get(at..read).expect("a record starts within what was read");
            let next = i64::from_ne_bytes(field(record, D_OFF));
            let len = usize::from(u16::from_ne_bytes(field(record, D_RECLEN)));
            let d_type = u8::from_ne_bytes(field(record, D_TYPE));
            let tail = record.get(D_NAME..len).expect("a record holds its name");
            let nul = tail.iter().position(|&byte| byte == 0).expect("a name ends in a NUL");
            let name = tail.get(..nul).expect("a name lies within its record");
            at = at.checked_add(len).expect("records lie within what was read");
            if name == b"." || name == b".." {
                resume = next;
                continue;
            }
            let end = used.checked_add(name.len()).expect("a name's end fits a usize");
            let (Some(entry), Some(room)) = (entries.get_mut(count), names.get_mut(used..end)) else {
                return match (count, seek(fd, resume, libc::SEEK_SET)) {
                    (0, Err(errno)) => Err(errno),
                    // A name longer than all of names: nothing can be taken.
                    (0, Ok(_)) => Err(libc::ENAMETOOLONG),
                    // Those taken are handed back, whether or not the
                    // position could be set back: they were read.
                    (_, Ok(_) | Err(_)) => Ok(u32::try_from(count).expect("no more entries than a valid List's count")),
                };
            };
            room.copy_from_slice(name);
            let kind = match kind_of_entry(d_type) {
                Some(kind) => kind,
                None => kind_at(fd, name),
            };
            let start = u32::try_from(used).expect("a valid List's names fit a count");
            let len = u32::try_from(name.len()).expect("a name's length fits a count");
            *entry = Entry { kind, start, len };
            count = count.checked_add(1).expect("no more entries than fit a usize");
            used = end;
            resume = next;
        }
    }
    Ok(u32::try_from(count).expect("no more entries than a valid List's count"))
}

/// The `N` bytes of a record from `at`.
fn field<const N: usize>(record: &[u8], at: usize) -> [u8; N] {
    let end = at.checked_add(N).expect("a field's end fits a usize");
    let bytes = record.get(at..end).expect("a record holds its fields");
    bytes.try_into().expect("N bytes")
}

/// The kind of the entry `name` of the directory open on `fd`, not
/// following it: `Other` if it is gone.
fn kind_at(fd: i32, name: &[u8]) -> Kind {
    let Ok(name) = CString::new(name) else {
        return Kind::Other;
    };
    // SAFETY: `libc::stat` is a plain C struct of integers, for which all
    // zeroes is a valid value.
    let mut raw: libc::stat = unsafe { mem::zeroed() };
    // SAFETY: `name` is a NUL-terminated string, and `raw` a live,
    // exclusive borrow of a `stat`, which the kernel writes during the call
    // only; both live across it.
    let done = unsafe { libc::fstatat(fd, name.as_ptr(), ptr::from_mut(&mut raw), libc::AT_SYMLINK_NOFOLLOW) };
    if done == 0 { kind_of(raw.st_mode) } else { Kind::Other }
}

/// The errors of a connection, on `Connect`, `Recv` and `Send`.
fn stream_error(errno: i32) -> Option<Error> {
    match errno {
        libc::ECONNRESET => Some(Error::Reset),
        libc::ENETUNREACH | libc::EHOSTUNREACH => Some(Error::Unreachable),
        libc::ETIMEDOUT => Some(Error::TimedOut),
        _ => None,
    }
}

fn domain(family: Family) -> i32 {
    match family {
        Family::Ipv4 => libc::AF_INET,
        Family::Ipv6 => libc::AF_INET6,
    }
}

/// An operation's `user_data`: its slot's generation, then its slot's index.
fn user_data(index: u32, generation: u32) -> u64 {
    (u64::from(generation) << 32_u32) | u64::from(index)
}

fn split(user_data: u64) -> (u32, u32) {
    let index = u32::try_from(user_data & u64::from(u32::MAX)).expect("masked to 32 bits");
    let generation = u32::try_from(user_data >> 32_u32).expect("shifted to 32 bits");
    (index, generation)
}

fn slot_index(index: u32) -> usize {
    usize::try_from(index).expect("a u32 fits in a usize")
}

fn socklen_of<T>() -> libc::socklen_t {
    libc::socklen_t::try_from(size_of::<T>()).expect("a socket structure's size fits a socklen_t")
}

fn errno_of(error: &io::Error) -> i32 {
    error.raw_os_error().unwrap_or(libc::EIO)
}

fn last_errno() -> i32 {
    errno_of(&io::Error::last_os_error())
}

fn family_of(family: i32) -> libc::sa_family_t {
    libc::sa_family_t::try_from(family).expect("an address family fits a sa_family_t")
}

fn zeroed_storage() -> libc::sockaddr_storage {
    // SAFETY: `sockaddr_storage` is a plain C struct of integers, for which
    // all zeroes is a valid value (an unspecified family).
    unsafe { mem::zeroed() }
}

fn zeroed_statx() -> libc::statx {
    // SAFETY: `statx` is a plain C struct of integers, for which all zeroes
    // is a valid value (nothing in its mask).
    unsafe { mem::zeroed() }
}

/// Writes `addr` into `storage` as the kernel lays it out, and returns its
/// length.
fn encode(addr: &Addr, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            let raw = libc::sockaddr_in {
                sin_family: family_of(libc::AF_INET),
                sin_port: v4.port().to_be(),
                // The octets are in network order, which is how s_addr is
                // laid out in memory.
                sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(v4.ip().octets()) },
                sin_zero: [0; 8],
            };
            // SAFETY: `sockaddr_storage` is larger than `sockaddr_in` and at
            // least as aligned, and `storage` is a live, exclusive borrow.
            unsafe { ptr::from_mut(storage).cast::<libc::sockaddr_in>().write(raw) };
            socklen_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(v6) => {
            let raw = libc::sockaddr_in6 {
                sin6_family: family_of(libc::AF_INET6),
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr { s6_addr: v6.ip().octets() },
                sin6_scope_id: v6.scope_id(),
            };
            // SAFETY: `sockaddr_storage` is larger than `sockaddr_in6` and at
            // least as aligned, and `storage` is a live, exclusive borrow.
            unsafe { ptr::from_mut(storage).cast::<libc::sockaddr_in6>().write(raw) };
            socklen_of::<libc::sockaddr_in6>()
        }
    }
}

/// The address the kernel wrote into `storage`, `len` bytes of it, or `None`
/// when it is neither IPv4 nor IPv6.
fn decode_addr(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<Addr> {
    let family = i32::from(storage.ss_family);
    if family == libc::AF_INET && len >= socklen_of::<libc::sockaddr_in>() {
        // SAFETY: the kernel wrote a `sockaddr_in`, as its family says, and
        // the storage is larger and at least as aligned.
        let raw = unsafe { ptr::from_ref(storage).cast::<libc::sockaddr_in>().read() };
        let ip = Ipv4Addr::from(raw.sin_addr.s_addr.to_ne_bytes());
        return Some(SocketAddr::V4(SocketAddrV4::new(ip, u16::from_be(raw.sin_port))));
    }
    if family == libc::AF_INET6 && len >= socklen_of::<libc::sockaddr_in6>() {
        // SAFETY: the kernel wrote a `sockaddr_in6`, as its family says, and
        // the storage is larger and at least as aligned.
        let raw = unsafe { ptr::from_ref(storage).cast::<libc::sockaddr_in6>().read() };
        let ip = Ipv6Addr::from(raw.sin6_addr.s6_addr);
        let port = u16::from_be(raw.sin6_port);
        return Some(SocketAddr::V6(SocketAddrV6::new(ip, port, raw.sin6_flowinfo, raw.sin6_scope_id)));
    }
    None
}

// The synchronous calls: what is not a ring operation, done when the record
// is submitted or completed (kernel.md), and the clock and the
// seed (shell.md, 5).

/// Sets the integer socket option `name` at `level` to 1.
fn set_option(fd: i32, level: i32, name: i32) -> Result<(), i32> {
    let one: libc::c_int = 1;
    let len = socklen_of::<libc::c_int>();
    // SAFETY: the value points at a live `c_int` of `len` bytes, which the
    // kernel reads during the call only.
    let set = unsafe { libc::setsockopt(fd, level, name, ptr::from_ref(&one).cast(), len) };
    if set == 0 { Ok(()) } else { Err(last_errno()) }
}

/// Reads the address `fd` is bound to into `storage`, and its length into
/// `len`.
fn local_addr(fd: i32, storage: &mut libc::sockaddr_storage, len: &mut libc::socklen_t) -> Result<(), i32> {
    *len = socklen_of::<libc::sockaddr_storage>();
    // SAFETY: `storage` is a live, exclusive borrow of `*len` bytes, and the
    // kernel writes at most that many, and the length, during the call only.
    let read = unsafe { libc::getsockname(fd, ptr::from_mut(storage).cast(), ptr::from_mut(len)) };
    if read == 0 { Ok(()) } else { Err(last_errno()) }
}

/// `getdents64` of the directory open on `fd` into `listing`: the bytes it
/// wrote, 0 at the end, or the error number.
fn getdents(fd: i32, listing: &mut [u8]) -> Result<usize, i32> {
    let len = libc::c_uint::try_from(listing.len()).expect("the listing buffer's length fits a c_uint");
    // SAFETY: `listing` is a live, exclusive borrow of `len` bytes, which the
    // kernel writes during the call only.
    let read = unsafe { libc::syscall(libc::SYS_getdents64, fd, listing.as_mut_ptr(), len) };
    match usize::try_from(read) {
        Ok(read) => Ok(read),
        Err(_) => Err(last_errno()),
    }
}

/// Moves the position of `fd`, as `lseek` does: where it is now.
fn seek(fd: i32, to: libc::off_t, whence: i32) -> Result<libc::off_t, i32> {
    // SAFETY: integers only.
    let at = unsafe { libc::lseek(fd, to, whence) };
    if at < 0 { Err(last_errno()) } else { Ok(at) }
}

/// Opens the directory at `path` as a root for io's files (kernel.md, 6.1),
/// as a service's `main` does at startup, from its configuration (shell.md,
/// 6): close-on-exec, and released only by a `Close` record. Fails with the
/// error number: `ENOTDIR` when `path` is not a directory, `EINVAL` when
/// it holds a NUL.
pub fn open_root(path: &Path) -> Result<Fd, i32> {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return Err(libc::EINVAL);
    };
    // SAFETY: `path` is a NUL-terminated string that lives across the call.
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) };
    if fd < 0 { Err(last_errno()) } else { Ok(Fd::new(fd)) }
}

/// Opens a regular file beneath `root` for append, creating it with `mode` if absent
/// (shell.md, 6). Existing contents and mode remain; links cannot escape the root.
pub fn open_append(root: Fd, path: &[u8], mode: u32) -> Result<Fd, Error> {
    if mode & !PERMISSIONS != 0 || path.is_empty() {
        return Err(Error::InvalidArgument);
    }
    let path_c = CString::new(path).map_err(|_| Error::InvalidArgument)?;
    let flags = libc::O_WRONLY | libc::O_APPEND | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NONBLOCK | libc::O_NOCTTY;
    // The syscall layout is three u64 fields, as Linux's struct open_how.
    let how = [
        u64::try_from(flags).expect("positive flags"),
        u64::from(mode),
        libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS,
    ];
    // SAFETY: both pointers remain live for the call; the kernel reads the
    // NUL-terminated path and exactly these three initialized u64 fields.
    let fd = unsafe { libc::syscall(libc::SYS_openat2, root.raw(), path_c.as_ptr(), how.as_ptr(), size_of_val(&how)) };
    if fd < 0 {
        let kind = Op::Open { root, path: Box::from(path), how: OpenHow::Create { mode: Some(mode) } };
        return Err(file_error(&kind, last_errno()));
    }
    let Done::Fd(fd) = opened(i32::try_from(fd).expect("a descriptor fits i32"))? else {
        unreachable!("opened returns a descriptor");
    };
    Ok(fd)
}

/// Releases a descriptor the adapter made but cannot hand up. The descriptor
/// is released whatever `close` answers, so there is nothing to do with it.
pub(crate) fn close(fd: i32) {
    // SAFETY: `fd` is a descriptor the kernel just made for this adapter,
    // which nothing else has seen.
    let _closed: i32 = unsafe { libc::close(fd) };
}

/// Nanoseconds on clock `id`, or `None` before its origin.
pub(crate) fn clock_nanos(id: libc::clockid_t) -> Option<u64> {
    let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `time` is a live, exclusive borrow of a `timespec`, which the
    // kernel writes during the call only.
    let read = unsafe { libc::clock_gettime(id, ptr::from_mut(&mut time)) };
    assert!(read == 0, "the shell's clocks exist and the timespec is valid");
    let secs = u64::try_from(time.tv_sec).ok()?;
    let nanos = u64::try_from(time.tv_nsec).ok()?;
    secs.checked_mul(NANOS_PER_SEC)?.checked_add(nanos)
}

/// Eight random bytes from `getrandom`, as a `u64`.
pub(crate) fn random() -> Result<u64, i32> {
    let mut bytes = [0_u8; 8];
    // SAFETY: `bytes` is a live, exclusive borrow of its length, which the
    // kernel writes during the call only.
    let read = unsafe { libc::getrandom(ptr::from_mut(&mut bytes).cast(), bytes.len(), 0) };
    match usize::try_from(read) {
        Ok(8) => Ok(u64::from_ne_bytes(bytes)),
        // A read of up to 256 bytes is never short once it starts.
        Ok(_) => Err(libc::EIO),
        Err(_) => Err(last_errno()),
    }
}

#[cfg(test)]
mod tests {
    use skein_io::kernel::{Done, Op, ServiceSignal, Submit};
    use skein_lib::{Duration, Queue, Token};

    use super::{Config, Kernel, Wait, open_termination_signals};
    use crate::Clock;

    #[test]
    fn blocked_termination_signal_arrives_through_the_ring() {
        let fd = open_termination_signals().expect("block the signals and open signalfd");
        let mut kernel = Kernel::open(Config { operations: 2 }).expect("open the ring");
        let mut submits = Queue::with_capacity(1);
        submits.push(Submit { op: Token::new(1), kind: Op::ReadSignal { fd } });
        kernel.submit(&mut submits, Wait::No);

        // SAFETY: pthread_self returns this live test thread's identifier.
        let thread = unsafe { libc::pthread_self() };
        // SAFETY: SIGTERM is blocked on this thread, so pthread_kill queues
        // it for the signalfd instead of invoking its default action.
        assert_eq!(unsafe { libc::pthread_kill(thread, libc::SIGTERM) }, 0);
        let deadline = Clock::new().now().now.saturating_add(Duration::from_secs(1));
        let mut answers = Queue::with_capacity(1);
        for _ in 0..3 {
            kernel.submit(&mut submits, Wait::Until(deadline));
            kernel.reap(&mut answers);
            if !answers.is_empty() {
                break;
            }
        }
        let answer = answers.pop().expect("the signal read completed");
        assert_eq!(answer.kind, Op::ReadSignal { fd });
        assert_eq!(answer.result, Ok(Done::ServiceSignal(ServiceSignal::Terminate)));

        submits.push(Submit { op: Token::new(2), kind: Op::Close { fd } });
        kernel.submit(&mut submits, Wait::No);
        let deadline = Clock::new().now().now.saturating_add(Duration::from_secs(1));
        for _ in 0..3 {
            kernel.submit(&mut submits, Wait::Until(deadline));
            kernel.reap(&mut answers);
            if !answers.is_empty() {
                break;
            }
        }
        let answer = answers.pop().expect("the signal descriptor closed");
        assert_eq!(answer.kind, Op::Close { fd });
        assert_eq!(answer.result, Ok(Done::Nothing));
    }
}
