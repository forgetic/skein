//! A simulated world (testing-strategy.md, 2.7): one thread, one loop,
//! driving each process's `iterate` over the simulator, iteration by
//! iteration, as each one's shell would, with the referee beside them.
//! It keeps processes, a predeclared program registry, and the fake machine;
//! it never inspects service state. `host` registers factories, `spawn_root`
//! supplies startup roots, and `run` answers hosted spawns and their exits
//! through the simulator (simulator.md, section 3; examples.md, section 6).

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::{self, Debug, Write};
use std::thread;

use skein_heap::Span;
use skein_io::kernel::{Done, Exit, Fd, Op};
use skein_lib::{Queue, Time};
use skein_sim::{Answer, Ask, Config, Entry, Handle, Pid, Program, Reply, Sim};

use crate::Host;
use crate::heap::{Heap, Memory};
use crate::program::Startup;
use crate::referee::{Controls, Referee};
use crate::{HostedProgram, Inherited, Machine, NoMachine, StartupAppends, StartupRoots};

/// The most iterations a world runs before it is declared stuck.
const STEPS: u32 = 1_000_000;

/// How many lines of the trace a failure prints.
const TAIL: usize = 80;

/// Startup descriptors prepared for a spawn before its completion is reaped.
struct PreparedStartup {
    roots: Vec<(Box<[u8]>, Handle)>,
    appends: Vec<(Box<[u8]>, Handle)>,
}

/// A world: the simulator, its processes, and the referee.
pub struct World<P, R, M = NoMachine> {
    seed: u64,
    sim: Sim,
    controls: Controls,
    signals: BTreeMap<usize, (Pid, Fd)>,
    next_host: usize,
    procs: Vec<P>,
    /// Each process's, in the simulator.
    pids: Vec<Pid>,
    referee: R,
    heap: Option<Heap>,
    programs: Vec<HostedProgram<P>>,
    startup: Vec<Startup>,
    pending_startup: BTreeMap<(Pid, skein_lib::Token), PreparedStartup>,
    hosted: Vec<Option<usize>>,
    finished: Vec<bool>,
    machine: M,
    killed: Vec<Killed>,
    calls: Queue<skein_sim::Call>,
    answers: Queue<Answer>,
}

/// Briefly: its seed, and how many processes it hosts.
impl<P, R, M> Debug for World<P, R, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("World").field("seed", &self.seed).field("procs", &self.procs.len()).finish_non_exhaustive()
    }
}

/// A terminated hosted child, reported by the world after its resources settle.
#[derive(Debug)]
pub struct Killed {
    pub pid: Pid,
    pub exit: Exit,
    /// Its separate peak and worst case, after exact ownership was checked on drop.
    pub heap: Option<(u64, u64)>,
}

/// What a run left, for a test to look at by reference.
///
/// When memory was checked, dropping it is the last check of the run: each
/// process, dropped in turn, must free exactly what was metered as its own,
/// or something it allocated was never accounted, or never freed. A test
/// that panicked skips it.
#[derive(Debug)]
pub struct Outcome<P, M = NoMachine> {
    /// The fake machine after every non-hosted call has been answered.
    pub machine: M,
    /// Terminated children already dropped under their own memory spans.
    pub killed: Vec<Killed>,
    pub seed: u64,
    /// Every submission and completion, and every fault drawn: what the same
    /// seed replays to.
    pub trace: Vec<Entry>,
    /// Surviving processes in admission order, settled; killed children are
    /// reported separately in `killed` after their metered drop.
    pub procs: Vec<P>,
    pub iterations: u32,
    /// When the world settled.
    pub end: Time,
    /// The most heap each process held at once, and its worst case, by
    /// index, when memory was checked.
    pub heap: Option<Vec<(u64, u64)>>,
    /// What each process held of its own once settled, by index, when memory
    /// was checked: what dropping it must free.
    pub held: Option<Vec<i64>>,
}

impl<P, M> Drop for Outcome<P, M> {
    fn drop(&mut self) {
        let Some(held) = self.held.take() else {
            return;
        };
        if thread::panicking() {
            return;
        }
        for (at, (proc, held)) in self.procs.drain(..).zip(held).enumerate() {
            let span = Span::start();
            drop(proc);
            let freed = span.end().net.checked_neg().expect("a heap within an i64");
            if freed != held {
                crate::fail(&format!(
                    "seed {}: process {at}, dropped once settled, freed {freed} bytes, not the {held} it held of \
                     its own: a leak, or heap made or freed outside its calls",
                    self.seed
                ));
            }
        }
    }
}

impl<P: Host, R: Referee<P>> World<P, R> {
    /// A world from `seed`, its simulator configured by `config`, its
    /// expectations held by `referee`; checking memory at every iteration if
    /// `memory` says so.
    #[must_use]
    pub fn new(seed: u64, config: Config, referee: R, memory: Memory) -> World<P, R> {
        Self::new_controlled(seed, config, memory, |_| referee)
    }

    /// Builds the referee with the same signal controls as a real world.
    #[must_use]
    pub fn new_controlled<F: FnOnce(Controls) -> R>(
        seed: u64,
        config: Config,
        memory: Memory,
        make_referee: F,
    ) -> World<P, R> {
        let controls = Controls::new();
        let referee = make_referee(controls.clone());
        let heap = match memory {
            Memory::Checked => Some(Heap::new()),
            Memory::Unchecked => None,
        };
        World {
            seed,
            sim: Sim::new(seed, config),
            controls,
            signals: BTreeMap::new(),
            next_host: 0,
            procs: Vec::new(),
            pids: Vec::new(),
            referee,
            heap,
            programs: Vec::new(),
            startup: Vec::new(),
            pending_startup: BTreeMap::new(),
            hosted: Vec::new(),
            finished: Vec::new(),
            machine: NoMachine,
            killed: Vec::new(),
            calls: Queue::with_capacity(256),
            answers: Queue::with_capacity(256),
        }
    }

    /// Supplies the scenario's fake machine before the world starts.
    pub fn with_machine<M: Machine>(self, machine: M) -> World<P, R, M> {
        World {
            seed: self.seed,
            sim: self.sim,
            controls: self.controls,
            signals: self.signals,
            next_host: self.next_host,
            procs: self.procs,
            pids: self.pids,
            referee: self.referee,
            heap: self.heap,
            programs: self.programs,
            startup: self.startup,
            pending_startup: self.pending_startup,
            hosted: self.hosted,
            finished: self.finished,
            machine,
            killed: self.killed,
            calls: self.calls,
            answers: self.answers,
        }
    }
}

impl<P: Host, R: Referee<P>, M: Machine> World<P, R, M> {
    /// Registers a hosted program before execution, rejecting duplicate names.
    pub fn host(&mut self, program: HostedProgram<P>) {
        self.host_roots(program, crate::program::no_roots);
    }

    /// Registers a factory and the named directories opened independently for
    /// each launch, before its factory runs (simulator.md, section 3.1).
    pub fn host_roots(&mut self, program: HostedProgram<P>, roots: StartupRoots) {
        self.host_startup(program, roots, crate::program::no_appends);
    }

    /// Registers independently opened roots and append files for each launch.
    pub fn host_startup(&mut self, program: HostedProgram<P>, roots: StartupRoots, appends: StartupAppends) {
        assert!(program.instances > 0 && program.operations > 0, "a hosted program has room to run");
        assert!(!self.programs.iter().any(|entry| entry.program == program.program), "one factory per program");
        self.programs.push(program);
        self.startup.push(Startup { roots, appends });
    }

    /// Adds a process with a fake-machine root inherited from startup.
    pub fn spawn_root<F: FnOnce(Fd) -> P>(&mut self, root: Handle, make: F) -> usize {
        let pid = self.sim.spawn_process();
        let root = self.sim.root(pid, root);
        self.admit(pid, None, || make(root))
    }

    /// Adds a process with an append file the scenario's machine opened at startup.
    pub fn spawn_append<F: FnOnce(Fd) -> P>(&mut self, file: Handle, make: F) -> usize {
        let pid = self.sim.spawn_process();
        let file = self.sim.append(pid, file);
        self.admit(pid, None, || make(file))
    }

    /// Adds the process `make` builds: its heap, from its making on, is the
    /// processes'.
    pub fn spawn<F: FnOnce() -> P>(&mut self, make: F) -> usize {
        let pid = self.sim.spawn_process();
        self.admit(pid, None, make)
    }

    /// Adds a process whose termination records arrive through its simulated signal source.
    pub fn spawn_signals<F: FnOnce(Fd) -> P>(&mut self, make: F) -> usize {
        let pid = self.sim.spawn_process();
        let signal = self.sim.open_signal_source(pid);
        self.signals.insert(self.next_host, (pid, signal));
        self.admit(pid, None, || make(signal))
    }

    fn admit<F: FnOnce() -> P>(&mut self, pid: Pid, hosted: Option<usize>, make: F) -> usize {
        let proc = match &mut self.heap {
            Some(heap) => heap.admit(make, P::worst_case),
            None => make(),
        };
        self.next_host = self.next_host.checked_add(1).expect("bounded admission count");
        self.pids.push(pid);
        self.procs.push(proc);
        self.hosted.push(hosted);
        self.finished.push(false);
        self.procs.len().checked_sub(1).expect("just pushed")
    }

    /// Runs the world until the referee passed and everything settled, then
    /// checks that every process holds nothing, and that the simulator has
    /// nothing in flight and no descriptor open.
    #[must_use]
    pub fn run(self) -> Outcome<P, M> {
        self.run_with_faults(|_, _, _| None)
    }

    /// Runs with a scenario's fault schedule at each kernel submission batch.
    /// The hook observes records outside the host's memory span; returning
    /// faults changes the simulator from that batch onward, while `None`
    /// keeps its current configuration. Records and their order are untouched.
    #[must_use]
    pub fn run_with_faults<F>(mut self, mut faults: F) -> Outcome<P, M>
    where
        F: FnMut(usize, Time, &Queue<skein_io::kernel::Submit>) -> Option<skein_sim::Faults>,
    {
        let mut iterations: u32 = 0;
        loop {
            iterations = iterations.checked_add(1).expect("within STEPS");
            if iterations >= STEPS {
                crate::fail(&format!(
                    "seed {}: the world settles within {STEPS} iterations\n{}",
                    self.seed,
                    self.tail()
                ));
            }
            let now = self.sim.now();
            let wall = self.sim.wall();
            self.referee.act(now, &mut self.procs);
            self.deliver_signals();
            let mut at = 0;
            while at < self.procs.len() {
                if self.hosted.get(at).expect("a status per process").is_some()
                    && !*self.finished.get(at).expect("a status per process")
                    && !self.sim.service_running(self.pids.get(at).copied().expect("a pid per process"))
                {
                    self.drop_killed(at);
                } else {
                    self.turn(at, now, wall, &mut faults);
                    at = at.checked_add(1).expect("a bounded process count");
                }
                self.referee.observe(now, &self.procs);
            }
            if self.busy(now) {
                continue;
            }
            let settled = self.settled();
            if settled && self.referee.passed() {
                break;
            }
            if let Some(why) = self.referee.overdue(now) {
                let (seed, at) = (self.seed, now.as_nanos());
                crate::fail(&format!("seed {seed}: at {at} ns, the referee failed the world:\n{why}\n{}", self.tail()));
            }
            match self.next() {
                Some(at) => self.sim.advance_to(at.max(now)),
                None => {
                    let (seed, at) = (self.seed, now.as_nanos());
                    crate::fail(&format!(
                        "seed {seed}: at {at} ns, the world is idle, unsettled, with nothing due\n{}",
                        self.tail()
                    ));
                }
            }
        }
        assert!(self.pending_startup.is_empty(), "all prepared startup descriptors were admitted or rolled back");
        for (proc, pid) in self.procs.iter().zip(&self.pids) {
            assert!(proc.is_empty(), "seed {}: {pid} holds nothing once settled", self.seed);
            self.sim.assert_quiescent(*pid);
            self.sim.assert_no_open_fds(*pid);
            self.sim.assert_hosted_settled(*pid);
        }
        Outcome {
            machine: self.machine,
            killed: self.killed,
            seed: self.seed,
            trace: self.sim.trace().to_vec(),
            end: self.sim.now(),
            heap: self.heap.as_ref().map(Heap::report),
            held: self.heap.as_ref().map(Heap::held),
            procs: self.procs,
            iterations,
        }
    }

    fn deliver_signals(&mut self) {
        loop {
            let Some((host, signal)) = self.controls.signals.borrow_mut().pop() else {
                break;
            };
            let (pid, source) = *self.signals.get(&host).expect("a service declares its signal source");
            let at = self.pids.iter().position(|current| *current == pid).expect("a signal names a running host");
            assert!(!self.procs.get(at).expect("admitted process").is_empty(), "a signal names a running service");
            self.sim.deliver_service_signal(pid, source, signal);
        }
    }

    /// One turn of process `at`'s loop: reap, iterate, submit.
    fn turn<F>(&mut self, at: usize, now: Time, wall: skein_lib::Wall, faults: &mut F)
    where
        F: FnMut(usize, Time, &Queue<skein_io::kernel::Submit>) -> Option<skein_sim::Faults>,
    {
        let pid = *self.pids.get(at).expect("a pid for each process");
        if *self.finished.get(at).expect("a status per process") {
            return;
        }
        // Inspect the completion before the parent's next iterate: a hosted
        // child already exists when its parent learns that it was spawned.
        let mut arrived =
            Queue::with_capacity(self.procs.get_mut(at).expect("a process at each index").completions().room());
        self.sim.reap(pid, &mut arrived);
        while let Some(complete) = arrived.pop() {
            if let (Op::Spawn { spawn }, Ok(Done::Spawned { pidfd, .. })) = (&complete.kind, &complete.result)
                && let Some((program_at, entry)) =
                    self.programs.iter().enumerate().find(|(_, entry)| entry.program == spawn.program)
            {
                let active = self
                    .hosted
                    .iter()
                    .zip(&self.finished)
                    .filter(|(hosted, finished)| **hosted == Some(program_at) && !**finished)
                    .count();
                assert!(
                    active < usize::try_from(entry.instances).expect("instances fit"),
                    "hosted program fits its instance provision"
                );
                let make = entry.make;
                let operations = entry.operations;
                let (child, pipes) = self.sim.bind_service(pid, *pidfd);
                let roots = self
                    .pending_startup
                    .remove(&(pid, complete.op))
                    .expect("a hosted launch prepared its startup descriptors");
                let appends =
                    roots.appends.into_iter().map(|(name, handle)| (name, self.sim.append(child, handle))).collect();
                let roots =
                    roots.roots.into_iter().map(|(name, handle)| (name, self.sim.root(child, handle))).collect();
                let inherited = Inherited { pipes, roots, appends, signal: self.sim.open_signal_source(child) };
                self.signals.insert(self.next_host, (child, inherited.signal));
                let child_at = self.admit(child, Some(program_at), || make(spawn, &inherited));
                assert!(
                    self.procs.get(child_at).expect("newly admitted child").operations() <= operations,
                    "hosted program operations fit its provision"
                );
            }
            self.procs.get_mut(at).expect("a process at each index").completions().push(complete);
        }
        let proc = self.procs.get_mut(at).expect("a process at each index");
        match &mut self.heap {
            Some(heap) => heap.around(at, || {
                proc.iterate(now, wall);
                proc.drain();
            }),
            None => {
                proc.iterate(now, wall);
                proc.drain();
            }
        }
        if let Some(faults) = faults(at, now, proc.submissions()) {
            self.sim.set_faults(faults);
        }
        self.sim.submit(pid, proc.submissions());
        if self.hosted.get(at).expect("a status per process").is_some()
            && let Some(exit) = proc.exit()
        {
            assert!(proc.is_empty(), "a normal hosted exit releases every entity");
            self.sim.assert_quiescent(pid);
            self.sim.assert_no_open_fds(pid);
            self.sim.finish_service(pid, exit);
            *self.finished.get_mut(at).expect("a status per process") = true;
        }
        self.serve_machine();
    }

    fn drop_killed(&mut self, at: usize) {
        let pid = *self.pids.get(at).expect("a pid per process");
        let exit = self.sim.service_exit(pid).expect("the child terminated");
        while self.sim.in_flight(pid) > 0 {
            let proc = self.procs.get_mut(at).expect("a process per index");
            self.sim.reap_exited_service(pid, proc.completions());
            if self.sim.in_flight(pid) > 0 {
                // Free returned records inside a call on the process's own
                // queue, metered as part of its destruction.
                let mut discard = || {
                    while let Some(complete) = proc.completions().pop() {
                        drop(complete);
                    }
                };
                match &mut self.heap {
                    Some(heap) => heap.around(at, discard),
                    None => discard(),
                }
            }
        }
        let proc = self.procs.remove(at);
        let heap = match &mut self.heap {
            Some(heap) => Some(heap.release(at, proc)),
            None => {
                drop(proc);
                None
            }
        };
        self.pids.remove(at);
        self.hosted.remove(at);
        self.finished.remove(at);
        self.sim.close_exited_service(pid);
        self.serve_machine();
        let mut closed = Queue::with_capacity(self.sim.in_flight(pid));
        self.sim.reap_exited_service(pid, &mut closed);
        while let Some(complete) = closed.pop() {
            assert!(matches!(complete.kind, Op::Close { .. }), "teardown creates only buffer-free close records");
        }
        self.sim.assert_quiescent(pid);
        self.sim.assert_no_open_fds(pid);
        self.killed.push(Killed { pid, exit, heap });
    }

    fn serve_machine(&mut self) {
        self.sim.calls(&mut self.calls);
        while let Some(call) = self.calls.pop() {
            if let Ask::Spawn { program, .. } = &call.ask
                && self.programs.iter().any(|entry| entry.program == *program)
            {
                let (pid, token, spawn) = self.sim.hosted_spawn(call.ticket);
                let program =
                    self.programs.iter().position(|entry| entry.program == spawn.program).expect("registered");
                let startup = self.startup.get(program).expect("startup selector per program");
                let declarations = (startup.roots)(spawn);
                let append_declarations = (startup.appends)(spawn);
                crate::program::check_appends(&append_declarations);
                crate::program::check_roots(&declarations);
                let descriptors = declarations
                    .len()
                    .checked_add(append_declarations.len())
                    .and_then(|count| count.checked_add(spawn.pipes.len()))
                    .and_then(|count| count.checked_add(1));
                if descriptors.is_none_or(|count| {
                    count > usize::try_from(self.sim.config().max_fds).expect("descriptor limit fits")
                }) {
                    self.answers
                        .push(Answer { ticket: call.ticket, result: Err(skein_io::kernel::Error::TooManyOpenFiles) });
                    continue;
                }
                let mut roots = Vec::new();
                let mut result = Ok(Reply::Program(Program::Service));
                for declaration in declarations {
                    match self.machine.open_root(&declaration.path) {
                        Ok(handle) => roots.push((declaration.name, handle)),
                        Err(error) => {
                            result = Err(error);
                            break;
                        }
                    }
                }
                let mut appends = Vec::new();
                if result.is_ok() {
                    for declaration in append_declarations {
                        let root = match self.machine.open_root(&declaration.root) {
                            Ok(root) => root,
                            Err(error) => {
                                result = Err(error);
                                break;
                            }
                        };
                        let opened = self.machine.open_append(root, &declaration.path, declaration.mode);
                        self.machine.close_root(root);
                        match opened {
                            Ok(file) => appends.push((declaration.name, file)),
                            Err(error) => {
                                result = Err(error);
                                break;
                            }
                        }
                    }
                }
                if result.is_ok() {
                    self.pending_startup.insert((pid, token), PreparedStartup { roots, appends });
                } else {
                    for (_, handle) in roots {
                        self.machine.close_root(handle);
                    }
                    for (_, handle) in appends {
                        self.machine.close_append(handle);
                    }
                }
                self.answers.push(Answer { ticket: call.ticket, result });
            } else {
                self.machine.step(call, &mut self.answers);
            }
        }
        self.sim.answer(&mut self.answers);
    }

    /// Whether any process has work now: its loop's, the kernel's deferred
    /// to its next entry, or completions delivered and not reaped.
    fn busy(&self, now: Time) -> bool {
        if !self.controls.signals.borrow().is_empty() {
            return true;
        }
        for (proc, pid) in self.procs.iter().zip(&self.pids) {
            if proc.work_pending(now) || self.sim.deferred(*pid) || self.sim.ready(*pid) > 0 {
                return true;
            }
        }
        false
    }

    /// Whether every process holds nothing and has nothing in flight.
    fn settled(&self) -> bool {
        for (at, (proc, pid)) in self.procs.iter().zip(&self.pids).enumerate() {
            if self.hosted.get(at).expect("a hosting status per process").is_some()
                && !self.finished.get(at).expect("a terminal status per process")
            {
                return false;
            }
            if !proc.is_empty() || self.sim.in_flight(*pid) > 0 {
                return false;
            }
        }
        true
    }

    /// The earliest of what the simulator has due and every deadline in the
    /// world: the processes' and the referee's (simulator.md, 3).
    fn next(&self) -> Option<Time> {
        let mut next = self.sim.next_due();
        let deadlines = self.procs.iter().map(Host::next_deadline).chain([self.referee.next_deadline()]);
        for at in deadlines.flatten() {
            next = Some(match next {
                Some(next) => next.min(at),
                None => at,
            });
        }
        next
    }

    /// The end of the trace, under its seed, for a failure.
    fn tail(&self) -> String {
        let text = self.sim.render_trace();
        let lines: Vec<&str> = text.lines().collect();
        let from = lines.len().saturating_sub(TAIL);
        let mut tail = format!("the last {} lines of the trace:\n", lines.len().saturating_sub(from));
        for line in lines.get(from..).unwrap_or_default() {
            writeln!(tail, "{line}").expect("writing to a String");
        }
        tail
    }
}
