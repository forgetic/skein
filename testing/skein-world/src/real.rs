//! The real loop (testing-strategy.md, 2.8): the same processes and referee
//! as a simulated world, in one thread and one loop, whose calls go to the
//! real kernel through one ring, on loopback, with deadlines on the real
//! clock (examples.md, section 6). It keeps token bindings, operation limits,
//! hosted factories, descriptor ownership and queued referee signals; it
//! never inspects service state. `World::host` selects a spawn's factory,
//! while `spawn_signalfd` and `spawn_signals` supply roots' signal sources.
//! `run` blocks only when every process and the control queue are idle,
//! and returns once exits, operations and descriptor closures settle. It
//! does not replay. Checked constructors meter each process separately; their
//! outcome verifies exact release on drop (testing-strategy.md, section 6).

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::format;
use alloc::vec::Vec;
use core::ops::{Deref, DerefMut};

use skein_heap::Span;
use skein_io::kernel::{Complete, Done, Error, Exit, Fd, Op, ServiceSignal, Signal, Spawn, Submit};
use skein_lib::{Duration, Queue, Time, Token};
use skein_shell::{Clock, Config, Kernel, Now, Wait};

use crate::Host;
use crate::heap::Heap;
pub use crate::referee::Controls;
use crate::referee::Referee;
use crate::{HostedProgram, Inherited, StartupRoots};
use std::os::unix::ffi::OsStrExt;

/// What a real run left.
#[derive(Debug)]
pub struct Outcome<P> {
    /// The processes as the run left them, settled.
    pub procs: Vec<P>,
    pub killed: Vec<Killed>,
    /// Per-process peak and worst case, in surviving admission order.
    pub heap: Option<Vec<(u64, u64)>>,
    /// Bytes each surviving process must release when dropped.
    pub held: Option<Vec<i64>>,
    pub iterations: u32,
    /// When the run began and settled, on the real clock.
    pub start: Time,
    pub end: Time,
}

/// A checked real outcome, borrowed for assertions; its final drop verifies
/// exact per-process release (testing-strategy.md, section 6).
#[derive(Debug)]
pub struct CheckedOutcome<P>(Outcome<P>);

impl<P> Deref for CheckedOutcome<P> {
    type Target = Outcome<P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P> Drop for CheckedOutcome<P> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let held = self.0.held.take().expect("a checked outcome keeps its ownership ledger");
        assert_eq!(self.0.procs.len(), held.len(), "an ownership entry per surviving process");
        for (at, (proc, held)) in self.0.procs.drain(..).zip(held).enumerate() {
            let span = Span::start();
            drop(proc);
            let freed = span.end().net.checked_neg().expect("a heap within an i64");
            assert_eq!(freed, held, "real process {at} must release exactly its own metered heap");
        }
    }
}

/// A real world whose constructors and iterations are metered separately.
/// Its checked outcome verifies final release, while the existing unchecked
/// world permits callers to take surviving processes by value.
#[derive(Debug)]
pub struct CheckedWorld<P, R>(World<P, R>);

impl<P, R> Deref for CheckedWorld<P, R> {
    type Target = World<P, R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P, R> DerefMut for CheckedWorld<P, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<P: Host, R: Referee<P>> CheckedWorld<P, R> {
    /// Runs on one ring and retains final ownership checking in the outcome.
    #[must_use]
    pub fn run(self, clock: &Clock, patience: Duration) -> CheckedOutcome<P> {
        CheckedOutcome(self.0.run(clock, patience))
    }
}

/// Runs unhosted processes on the shared real ring; `World` also hosts spawns.
#[must_use]
pub fn run<P: Host, R: Referee<P>>(procs: Vec<P>, referee: R, clock: &Clock, patience: Duration) -> Outcome<P> {
    let mut world = World::new(referee);
    for proc in procs {
        world.spawn(|| proc);
    }
    world.run(clock, patience)
}

/// A killed hosted child, reported after its operations and descriptors settle.
#[derive(Debug)]
pub struct Killed {
    pub host: usize,
    pub exit: Exit,
    /// Peak and bound after exact metered release, when checked.
    pub heap: Option<(u64, u64)>,
}

#[derive(Clone, Copy, Debug)]
enum SignalSource {
    Pipe(Fd),
    Real,
}

/// A real world built by the scenario, with factories shared with simulated worlds.
#[derive(Debug)]
pub struct World<P, R> {
    procs: Vec<P>,
    referee: R,
    programs: Vec<HostedProgram<P>>,
    startup: Vec<StartupRoots>,
    inherited: Vec<Vec<Fd>>,
    controls: Controls,
    signals: BTreeMap<usize, SignalSource>,
    signalfd: bool,
    heap: Option<Heap>,
}

impl<P: Host, R: Referee<P>> World<P, R> {
    /// Starts an empty real world with its observation referee.
    #[must_use]
    pub fn new(referee: R) -> Self {
        Self::new_controlled(|_| referee)
    }

    /// Constructs a referee with a handle for injecting signals into this world.
    #[must_use]
    pub fn new_controlled<F: FnOnce(Controls) -> R>(make_referee: F) -> Self {
        let controls = Controls::new();
        let referee = make_referee(controls.clone());
        Self {
            procs: Vec::new(),
            referee,
            programs: Vec::new(),
            startup: Vec::new(),
            inherited: Vec::new(),
            controls,
            signals: BTreeMap::new(),
            signalfd: false,
            heap: None,
        }
    }

    /// Starts a checked world under skein's counting allocator. Build every
    /// root inside its spawn closure; referee and backend storage is excluded.
    #[must_use]
    pub fn new_checked(referee: R) -> CheckedWorld<P, R> {
        Self::new_controlled_checked(|_| referee)
    }

    /// Constructs a checked world with referee-controlled signal injection.
    #[must_use]
    pub fn new_controlled_checked<F: FnOnce(Controls) -> R>(make_referee: F) -> CheckedWorld<P, R> {
        let mut world = Self::new_controlled(make_referee);
        world.heap = Some(Heap::new());
        CheckedWorld(world)
    }

    /// Adds a service whose signal records arrive through its own real pipe.
    pub fn spawn_signals<F: FnOnce(Fd) -> P>(&mut self, make: F) -> usize {
        let (reader, writer) = skein_shell::open_signal_pipe().expect("open the service signal pipe");
        let host = self.spawn_with_fds(vec![reader], || make(reader));
        self.signals.insert(host, SignalSource::Pipe(writer));
        host
    }

    /// Adds the one service that owns this process's blocked termination signalfd.
    /// Construct and run the world on the same thread.
    pub fn spawn_signalfd<F: FnOnce(Fd) -> P>(&mut self, make: F) -> usize {
        assert!(!self.signalfd, "only one service reads the process signalfd");
        let reader = skein_shell::open_termination_signals().expect("block termination signals and open signalfd");
        let host = self.spawn_with_fds(vec![reader], || make(reader));
        self.signals.insert(host, SignalSource::Real);
        self.signalfd = true;
        host
    }

    /// Registers a program and its maximum simultaneous instances before execution.
    pub fn host(&mut self, program: HostedProgram<P>) {
        self.host_roots(program, crate::program::no_roots);
    }

    /// Registers independently opened startup directories selected from each
    /// spawn, shared with simulated scenarios (testing-strategy.md, section 2.8).
    pub fn host_roots(&mut self, program: HostedProgram<P>, roots: StartupRoots) {
        assert!(program.instances > 0 && program.operations > 0, "a hosted program has room to run");
        assert!(!self.programs.iter().any(|entry| entry.program == program.program), "one factory per program");
        self.programs.push(program);
        self.startup.push(roots);
    }

    /// Adds a root host; descriptors opened at startup may be declared with `spawn_with_fds`.
    pub fn spawn<F: FnOnce() -> P>(&mut self, make: F) -> usize {
        self.spawn_with_fds(Vec::new(), make)
    }

    /// Adds a root host with descriptors it inherits and owns until close or exit.
    pub fn spawn_with_fds<F: FnOnce() -> P>(&mut self, fds: Vec<Fd>, make: F) -> usize {
        let host = self.procs.len();
        let proc = match &mut self.heap {
            Some(heap) => heap.admit(make, P::worst_case),
            None => make(),
        };
        self.procs.push(proc);
        self.inherited.push(fds);
        host
    }

    /// Opens one ring with room for every root, hosted instance and teardown operation.
    #[must_use]
    pub fn run(self, clock: &Clock, patience: Duration) -> Outcome<P> {
        assert!(!self.procs.is_empty(), "a real loop runs at least one process");
        let limits: Vec<u32> = self.procs.iter().map(Host::operations).collect();
        let roots = limits.iter().try_fold(0_u32, |sum, limit| sum.checked_add(*limit)).expect("root operations fit");
        let hosted = self
            .programs
            .iter()
            .try_fold(0_u32, |sum, program| sum.checked_add(program.operations.checked_mul(program.instances)?))
            .expect("hosted operations fit");
        // One cancellation for every outstanding hosted operation, plus one
        // close, ensures a killed child can settle even at its admission limit.
        let operations = roots
            .checked_add(hosted.checked_mul(2).expect("cleanup operations fit"))
            .and_then(|sum| sum.checked_add(1))
            .expect("world operations fit");
        let ring = Ring::new(operations, limits);
        let ids: Vec<usize> = (0..self.procs.len()).collect();
        let processes = self
            .inherited
            .iter()
            .enumerate()
            .map(|(host, fds)| {
                (host, Process { fds: fds.iter().copied().collect(), exit: None, cleanup: 0, cleaned: false })
            })
            .collect();
        Running {
            world: self,
            ring,
            ids,
            processes,
            children: BTreeMap::new(),
            placeholders: BTreeMap::new(),
            housekeeping: BTreeMap::new(),
            cleanup: VecDeque::new(),
            killed: Vec::new(),
            dying: BTreeSet::new(),
        }
        .run(clock, patience)
    }
}

struct Process {
    fds: BTreeSet<Fd>,
    exit: Option<Exit>,
    cleanup: u32,
    cleaned: bool,
}

impl Process {
    fn submitted(&mut self, operation: &Op) {
        // Close releases ownership at submission. Its completion may arrive
        // after another operation returned a newly reused descriptor number.
        if let Op::Close { fd } = operation {
            self.fds.remove(fd);
        }
    }

    fn completed(&mut self, complete: &Complete) {
        match &complete.result {
            Ok(Done::Fd(fd) | Done::Accepted { fd, .. } | Done::Spawned { pidfd: fd }) => {
                self.fds.insert(*fd);
            }
            Ok(
                Done::Nothing
                | Done::Count(_)
                | Done::Bound(_)
                | Done::Stat(_)
                | Done::Exit(_)
                | Done::Usage(_)
                | Done::ServiceSignal(_),
            )
            | Err(_) => {}
        }
        if let Op::Spawn { spawn } = &complete.kind {
            for pipe in &spawn.pipes {
                if let Some(fd) = pipe.parent {
                    self.fds.insert(fd);
                }
            }
        }
    }
}

struct Child {
    host: usize,
    program: usize,
    signal_writer: Fd,
    exit: Option<Exit>,
    waiters: Vec<Submit>,
    placeholder_open: bool,
}

struct Running<P, R> {
    world: World<P, R>,
    ring: Ring,
    ids: Vec<usize>,
    processes: BTreeMap<usize, Process>,
    children: BTreeMap<usize, Child>,
    placeholders: BTreeMap<Fd, usize>,
    housekeeping: BTreeMap<Token, usize>,
    cleanup: VecDeque<(usize, Op)>,
    killed: Vec<Killed>,
    dying: BTreeSet<usize>,
}

impl<P: Host, R: Referee<P>> Running<P, R> {
    fn run(mut self, clock: &Clock, patience: Duration) -> Outcome<P> {
        let start = clock.now().now;
        let deadline = start.saturating_add(patience);
        let mut iterations: u32 = 0;
        loop {
            iterations = iterations.checked_add(1).expect("fewer than 2^32 iterations");
            let Now { now, wall } = clock.now();
            assert!(now < deadline, "the real loop settles within {} ms", patience.as_nanos().div_euclid(1_000_000));
            self.world.referee.act(now, &mut self.world.procs);
            self.signals();
            self.reap();
            let mut at = 0;
            while at < self.world.procs.len() {
                let host = *self.ids.get(at).expect("an id per host");
                if self.processes.get(&host).expect("a process per host").exit.is_none() {
                    let proc = self.world.procs.get_mut(at).expect("a process per host");
                    self.ring.deliver(host, proc.completions());
                    match &mut self.world.heap {
                        Some(heap) => heap.around(at, || {
                            proc.iterate(now, wall);
                            proc.drain();
                        }),
                        None => {
                            proc.iterate(now, wall);
                            proc.drain();
                        }
                    }
                    let mut submits = Queue::with_capacity(proc.submissions().len());
                    while let Some(record) = proc.submissions().pop() {
                        submits.push(record);
                    }
                    while let Some(record) = submits.pop() {
                        let record = self.ring.register(host, record);
                        self.submit(record);
                    }
                    // Killed hosts remain admitted until their records return.
                    if self.ids.get(at) == Some(&host) {
                        let proc = self.world.procs.get(at).expect("a process per host");
                        if self.children.values().any(|child| child.host == host)
                            && let Some(exit) = proc.exit()
                        {
                            assert!(proc.is_empty(), "normal hosted exit releases every entity");
                            self.processes.get_mut(&host).expect("a process per host").exit = Some(exit);
                        }
                    }
                }
                if self.dying.contains(&host) {
                    self.discard(at, host);
                }
                if self.ids.get(at) == Some(&host) {
                    at = at.checked_add(1).expect("bounded host count");
                }
                self.world.referee.observe(now, &self.world.procs);
            }
            self.settle_children();
            self.settle_root_signals();
            self.submit_cleanup();
            self.ring.enter(Wait::No);
            if self.world.procs.iter().any(|proc| proc.work_pending(now))
                || self.ring.has_ready()
                || !self.world.controls.signals.borrow().is_empty()
            {
                continue;
            }
            if self.world.procs.iter().all(Host::is_empty)
                && self.ring.is_empty()
                && self.cleanup.is_empty()
                && self.children.values().all(|child| child.exit.is_some() && !child.placeholder_open)
                && self.world.referee.passed()
            {
                assert!(
                    self.processes.values().all(|process| process.fds.is_empty()),
                    "settled hosts leave no owned descriptor open"
                );
                return Outcome {
                    heap: self.world.heap.as_ref().map(Heap::report),
                    held: self.world.heap.as_ref().map(Heap::held),
                    procs: self.world.procs,
                    killed: self.killed,
                    iterations,
                    start,
                    end: now,
                };
            }
            if let Some(why) = self.world.referee.overdue(now) {
                crate::fail(&format!(
                    "at {} ms, the referee failed the real loop:\n{why}",
                    now.saturating_since(start).as_nanos().div_euclid(1_000_000)
                ));
            }
            let mut until = deadline;
            for at in
                self.world.procs.iter().map(Host::next_deadline).chain([self.world.referee.next_deadline()]).flatten()
            {
                until = until.min(at);
            }
            self.ring.enter(Wait::Until(until));
        }
    }

    fn signals(&mut self) {
        loop {
            let Some((host, signal)) = self.world.controls.signals.borrow_mut().pop() else { break };
            assert!(
                self.processes.get(&host).expect("a signal names an admitted host").exit.is_none(),
                "a signal names a running service"
            );
            match self.world.signals.get(&host).expect("a service declares its signal source") {
                SignalSource::Pipe(writer) => {
                    skein_shell::write_service_signal(*writer, signal).expect("deliver the referee signal");
                }
                SignalSource::Real => {
                    skein_shell::signal_current_thread(signal).expect("deliver the real termination signal");
                }
            }
        }
    }

    fn settle_root_signals(&mut self) {
        let finished: Vec<usize> = self
            .ids
            .iter()
            .zip(&self.world.procs)
            .filter_map(|(host, proc)| {
                (proc.is_empty() && self.ring.count(*host) == 0 && !self.children.contains_key(host)).then_some(*host)
            })
            .collect();
        for host in finished {
            if let Some(SignalSource::Pipe(writer)) = self.world.signals.remove(&host) {
                self.cleanup.push_back((host, Op::Close { fd: writer }));
            }
        }
    }

    fn reap(&mut self) {
        self.ring.kernel.reap(&mut self.ring.arrived);
        while let Some(complete) = self.ring.arrived.pop() {
            if let Some(host) = self.housekeeping.remove(&complete.op) {
                let process = self.processes.get_mut(&host).expect("cleanup names a process");
                process.cleanup = process.cleanup.checked_sub(1).expect("cleanup was in flight");
            } else {
                self.answer(complete);
            }
        }
    }

    fn answer(&mut self, complete: Complete) {
        let (host, complete) = self.ring.complete(complete);
        let process = self.processes.get_mut(&host).expect("a process per host");
        process.completed(&complete);
        self.ring.ready.get_mut(host).expect("a queue per host").push_back(complete);
    }

    fn synthetic(&mut self, record: Submit, result: Result<Done, Error>) {
        self.answer(Complete { op: record.op, kind: record.kind, result });
    }

    fn submit(&mut self, mut record: Submit) {
        assert!(record.kind.is_valid(), "only valid records cross the kernel boundary");
        let host = self.ring.bindings.get(&record.op).expect("a registered operation").host;
        self.processes.get_mut(&host).expect("an operation owner").submitted(&record.kind);
        match &mut record.kind {
            Op::Spawn { spawn } => {
                if let Some(program) = self.world.programs.iter().position(|entry| entry.program == spawn.program) {
                    let result = self.spawn(host, program, spawn);
                    self.synthetic(record, result);
                    return;
                }
            }
            Op::Wait { pidfd, .. } => {
                if let Some(child) = self.placeholders.get(pidfd).copied().and_then(|host| self.children.get_mut(&host))
                {
                    match child.exit {
                        Some(exit) => self.synthetic(record, Ok(Done::Exit(exit))),
                        None => child.waiters.push(record),
                    }
                    return;
                }
            }
            Op::Signal { pidfd, signal, .. } => {
                if let Some(child) = self.placeholders.get(pidfd).and_then(|host| self.children.get(host)) {
                    let child_host = child.host;
                    let writer = child.signal_writer;
                    let result = if child.exit.is_some()
                        || self.processes.get(&child_host).expect("child process").exit.is_some()
                    {
                        Err(Error::Other(3))
                    } else {
                        match signal {
                            Signal::Terminate => skein_shell::write_service_signal(writer, ServiceSignal::Terminate)
                                .map(|()| Done::Nothing),
                            Signal::Kill => {
                                self.kill(child_host);
                                Ok(Done::Nothing)
                            }
                        }
                    };
                    self.synthetic(record, result);
                    return;
                }
            }
            Op::Close { fd } => {
                if let Some(host) = self.placeholders.remove(fd) {
                    let child = self.children.get_mut(&host).expect("registered placeholder");
                    assert!(child.placeholder_open, "a placeholder closes once");
                    child.placeholder_open = false;
                    // A placeholder is a real descriptor, released by a real ring close.
                }
            }
            Op::Cancel { target } => {
                let mut cancelled = None;
                for child in self.children.values_mut() {
                    if let Some(at) = child.waiters.iter().position(|waiter| waiter.op == *target) {
                        cancelled = Some(child.waiters.remove(at));
                        break;
                    }
                }
                if let Some(waiter) = cancelled {
                    self.synthetic(waiter, Err(Error::Cancelled));
                    self.synthetic(record, Ok(Done::Nothing));
                    return;
                }
            }
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Open { .. }
            | Op::Read { .. }
            | Op::Write { .. }
            | Op::Append { .. }
            | Op::Sync { .. }
            | Op::Stat { .. }
            | Op::Rename { .. }
            | Op::Remove { .. }
            | Op::MakeDirectory { .. }
            | Op::List { .. }
            | Op::Usage
            | Op::ReadSignal { .. }
            | Op::PipeRead { .. }
            | Op::PipeWrite { .. } => {}
        }
        self.ring.submits.push(record);
    }

    fn spawn(&mut self, parent: usize, program: usize, spawn: &mut Spawn) -> Result<Done, Error> {
        let entry = self.world.programs.get(program).expect("a registered program");
        let active = self.children.values().filter(|child| child.program == program && child.exit.is_none()).count();
        assert!(
            active < usize::try_from(entry.instances).expect("instances fit"),
            "hosted instances fit their provision"
        );
        let make = entry.make;
        let operations = entry.operations;
        let declarations = self.world.startup.get(program).expect("startup selector per program")(spawn);
        crate::program::check_roots(&declarations);
        let mut roots = Vec::new();
        for declaration in declarations {
            let path = std::path::Path::new(std::ffi::OsStr::from_bytes(&declaration.path));
            match skein_shell::open_root(path) {
                Ok(fd) => roots.push((declaration.name, fd)),
                Err(errno) => {
                    for (_, fd) in roots {
                        self.cleanup.push_back((parent, Op::Close { fd }));
                    }
                    return Err(match errno {
                        2 => Error::NotFound,
                        13 => Error::Permission,
                        20 => Error::NotADirectory,
                        22 => Error::InvalidArgument,
                        24 => Error::TooManyOpenFiles,
                        _ => Error::Other(errno),
                    });
                }
            }
        }
        match skein_shell::hosted_pipes(spawn) {
            Ok(pipes) => {
                let inherited = Inherited { pipes: pipes.pipes, roots, signal: pipes.signal };
                let proc = match &mut self.world.heap {
                    Some(heap) => heap.admit(|| make(spawn, &inherited), P::worst_case),
                    None => make(spawn, &inherited),
                };
                assert!(proc.operations() <= operations, "hosted operations fit their provision");
                let child = self.ring.admit(proc.operations());
                self.world.procs.push(proc);
                self.ids.push(child);
                self.processes.insert(
                    child,
                    Process {
                        fds: inherited
                            .pipes
                            .iter()
                            .map(|(_, fd)| *fd)
                            .chain(inherited.roots.iter().map(|(_, fd)| *fd))
                            .chain([inherited.signal])
                            .collect(),
                        exit: None,
                        cleanup: 0,
                        cleaned: false,
                    },
                );
                self.world.signals.insert(child, SignalSource::Pipe(pipes.signal_writer));
                self.placeholders.insert(pipes.pidfd, child);
                self.children.insert(
                    child,
                    Child {
                        host: child,
                        program,
                        signal_writer: pipes.signal_writer,
                        exit: None,
                        waiters: Vec::new(),
                        placeholder_open: true,
                    },
                );
                Ok(Done::Spawned { pidfd: pipes.pidfd })
            }
            Err(error) => {
                for (_, fd) in roots {
                    self.cleanup.push_back((parent, Op::Close { fd }));
                }
                Err(error)
            }
        }
    }

    fn kill(&mut self, host: usize) {
        let exit = Exit::Signal(9);
        self.processes.get_mut(&host).expect("child process").exit = Some(exit);
        assert!(self.dying.insert(host), "a hosted child is killed once");
        // Keep the child until every submitted buffer returns. Returned records
        // are discarded under that child's meter, never by the backend.
        let targets: Vec<Token> =
            self.ring.bindings.iter().filter_map(|(token, binding)| (binding.host == host).then_some(*token)).collect();
        for target in targets {
            let mut waiter = None;
            for child in self.children.values_mut() {
                if let Some(at) = child.waiters.iter().position(|record| record.op == target) {
                    waiter = Some(child.waiters.remove(at));
                    break;
                }
            }
            if let Some(waiter) = waiter {
                self.synthetic(waiter, Err(Error::Cancelled));
            } else {
                self.cleanup.push_back((host, Op::Cancel { target }));
            }
        }
    }

    fn discard(&mut self, at: usize, host: usize) {
        let proc = self.world.procs.get_mut(at).expect("a dying process");
        self.ring.deliver(host, proc.completions());
        let mut discard = || {
            while let Some(complete) = proc.completions().pop() {
                drop(complete);
            }
        };
        match &mut self.world.heap {
            Some(heap) => heap.around(at, discard),
            None => discard(),
        }
    }

    fn release_killed(&mut self, host: usize, exit: Exit) {
        let at = self.ids.iter().position(|id| *id == host).expect("a dying child remains admitted");
        let proc = self.world.procs.remove(at);
        self.ids.remove(at);
        let heap = match &mut self.world.heap {
            Some(heap) => Some(heap.release(at, proc)),
            None => {
                drop(proc);
                None
            }
        };
        self.killed.push(Killed { host, exit, heap });
    }

    fn settle_children(&mut self) {
        let hosts: Vec<usize> =
            self.children.values().filter(|child| child.exit.is_none()).map(|child| child.host).collect();
        for host in hosts {
            let process = self.processes.get_mut(&host).expect("child process");
            let Some(exit) = process.exit else { continue };
            if self.ring.count(host) != 0 {
                continue;
            }
            if !process.cleaned {
                for fd in core::mem::take(&mut process.fds) {
                    self.cleanup.push_back((host, Op::Close { fd }));
                }
                let child = self.children.values().find(|child| child.host == host).expect("registered child");
                self.cleanup.push_back((host, Op::Close { fd: child.signal_writer }));
                process.cleaned = true;
            }
            if process.cleanup != 0 || self.cleanup.iter().any(|(owner, _)| *owner == host) {
                continue;
            }
            if self.dying.remove(&host) {
                self.release_killed(host, exit);
            }
            let child = self.children.values_mut().find(|child| child.host == host).expect("registered child");
            child.exit = Some(exit);
            self.world.signals.remove(&host);
            let waiters = core::mem::take(&mut child.waiters);
            for waiter in waiters {
                self.synthetic(waiter, Ok(Done::Exit(exit)));
            }
        }
    }

    fn submit_cleanup(&mut self) {
        let room =
            self.ring.kernel.room().checked_sub(self.ring.submits.len()).expect("pending submissions fit the ring");
        for _ in 0..room {
            let Some((host, kind)) = self.cleanup.pop_front() else { break };
            if let Op::Close { fd } = &kind
                && let Some(child_host) = self.placeholders.remove(fd)
            {
                self.children.get_mut(&child_host).expect("registered placeholder").placeholder_open = false;
            }
            let op = self.ring.fresh();
            self.housekeeping.insert(op, host);
            let process = self.processes.get_mut(&host).expect("cleanup owner");
            process.cleanup = process.cleanup.checked_add(1).expect("cleanup operations fit");
            self.ring.submits.push(Submit { op, kind });
        }
    }
}

/// One host's original operation identity, including a cancel's target.
struct Binding {
    host: usize,
    local: Token,
    cancel: Option<Token>,
}

/// The shared kernel and the bindings kept until records return to their host.
struct Ring {
    kernel: Kernel,
    next: u64,
    limits: Vec<u32>,
    counts: Vec<u32>,
    bindings: BTreeMap<Token, Binding>,
    tokens: BTreeMap<(usize, Token), Token>,
    ready: Vec<VecDeque<Complete>>,
    submits: Queue<Submit>,
    arrived: Queue<Complete>,
}

impl Ring {
    fn new(operations: u32, limits: Vec<u32>) -> Self {
        let kernel = match Kernel::open(Config { operations }) {
            Ok(kernel) => kernel,
            Err(error) => crate::fail(&format!("io_uring is not usable here, so the real loop cannot run: {error}")),
        };
        Self {
            kernel,
            next: 0,
            counts: vec![0; limits.len()],
            ready: (0..limits.len()).map(|_| VecDeque::new()).collect(),
            limits,
            bindings: BTreeMap::new(),
            tokens: BTreeMap::new(),
            submits: Queue::with_capacity(operations),
            arrived: Queue::with_capacity(operations),
        }
    }

    fn complete(&mut self, mut complete: Complete) -> (usize, Complete) {
        let binding = self.bindings.remove(&complete.op).expect("a world token has its binding");
        self.tokens.remove(&(binding.host, binding.local)).expect("a local token has its binding");
        complete.op = binding.local;
        if let Some(target) = binding.cancel {
            complete.kind = Op::Cancel { target };
        }
        (binding.host, complete)
    }

    fn deliver(&mut self, host: usize, out: &mut Queue<Complete>) {
        while out.room() > 0 {
            let Some(complete) = self.ready.get_mut(host).expect("a queue per host").pop_front() else { break };
            let count = self.counts.get_mut(host).expect("a count per host");
            *count = count.checked_sub(1).expect("a returned operation was in flight");
            out.push(complete);
        }
    }

    fn register(&mut self, host: usize, mut record: Submit) -> Submit {
        assert!(
            self.counts.get(host).expect("a count per host") < self.limits.get(host).expect("a limit per host"),
            "host {host} exceeds its own operation limit"
        );
        assert!(
            !self.tokens.contains_key(&(host, record.op))
                && !self.ready.get(host).expect("a queue per host").iter().any(|complete| complete.op == record.op),
            "a host never reuses a token in flight"
        );
        let token = self.fresh();
        let local = record.op;
        let cancel = if let Op::Cancel { target } = &mut record.kind {
            let original = *target;
            *target = self.tokens.get(&(host, original)).copied().unwrap_or(Token::new(0));
            Some(original)
        } else {
            None
        };
        self.tokens.insert((host, local), token);
        self.bindings.insert(token, Binding { host, local, cancel });
        let count = self.counts.get_mut(host).expect("a count per host");
        *count = count.checked_add(1).expect("within the host limit");
        record.op = token;
        record
    }

    fn fresh(&mut self) -> Token {
        self.next = self.next.checked_add(1).expect("world tokens do not wrap");
        Token::new(self.next)
    }

    fn admit(&mut self, limit: u32) -> usize {
        let host = self.limits.len();
        self.limits.push(limit);
        self.counts.push(0);
        self.ready.push(VecDeque::new());
        host
    }

    fn count(&self, host: usize) -> u32 {
        *self.counts.get(host).expect("a count per host")
    }

    fn enter(&mut self, wait: Wait) {
        self.kernel.submit(&mut self.submits, wait);
        assert!(self.submits.is_empty(), "the ring is provisioned for every host and cleanup");
    }

    fn has_ready(&self) -> bool {
        self.ready.iter().any(|ready| !ready.is_empty())
    }

    fn is_empty(&self) -> bool {
        self.kernel.in_flight() == 0 && !self.has_ready()
    }
}

#[cfg(test)]
mod tests {
    use super::Process;
    use skein_io::kernel::{Complete, Done, Family, Fd, Op};
    use skein_lib::Token;

    #[test]
    fn a_late_close_completion_does_not_erase_a_reused_descriptor() {
        let fd = Fd::new(42);
        let mut process = Process { fds: [fd].into_iter().collect(), exit: None, cleanup: 0, cleaned: false };
        process.submitted(&Op::Close { fd });
        process.completed(&Complete {
            op: Token::new(2),
            kind: Op::Socket { family: Family::Ipv4 },
            result: Ok(Done::Fd(fd)),
        });
        process.completed(&Complete { op: Token::new(1), kind: Op::Close { fd }, result: Ok(Done::Nothing) });
        assert!(
            process.fds.contains(&fd),
            "kernel.md: completions may arrive in any order; the new descriptor remains owned"
        );
    }
}
