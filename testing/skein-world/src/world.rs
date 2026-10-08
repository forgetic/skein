//! A simulated world (testing-strategy.md, 2.7): one thread, one loop,
//! driving each process's `iterate` over the simulator, iteration by
//! iteration, as each one's shell would, with the referee beside them.
//! It keeps processes, a predeclared program registry, and the fake machine;
//! it never inspects service state. `host` registers factories, `spawn_root`
//! supplies startup roots, and `run` answers hosted spawns and their exits
//! through the simulator (simulator.md, section 3; examples.md, section 6).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::{self, Debug, Write};
use std::thread;

use skein_heap::Span;
use skein_io::kernel::{Done, Fd, Op};
use skein_lib::{Queue, Time};
use skein_sim::{Answer, Ask, Config, Entry, Handle, Pid, Program, Reply, Sim};

use crate::heap::{Heap, Memory};
use crate::host::Host;
use crate::referee::Referee;
use crate::{HostedProgram, Inherited, Machine, NoMachine};

/// The most iterations a world runs before it is declared stuck.
const STEPS: u32 = 1_000_000;

/// How many lines of the trace a failure prints.
const TAIL: usize = 80;

/// A world: the simulator, its processes, and the referee.
pub struct World<P, R, M = NoMachine> {
    seed: u64,
    sim: Sim,
    procs: Vec<P>,
    /// Each process's, in the simulator.
    pids: Vec<Pid>,
    referee: R,
    heap: Option<Heap>,
    programs: Vec<HostedProgram<P>>,
    hosted: Vec<Option<usize>>,
    finished: Vec<bool>,
    machine: M,
    calls: Queue<skein_sim::Call>,
    answers: Queue<Answer>,
}

/// Briefly: its seed, and how many processes it hosts.
impl<P, R, M> Debug for World<P, R, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("World").field("seed", &self.seed).field("procs", &self.procs.len()).finish_non_exhaustive()
    }
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
    pub seed: u64,
    /// Every submission and completion, and every fault drawn: what the same
    /// seed replays to.
    pub trace: Vec<Entry>,
    /// The processes as the run left them, settled.
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
        let heap = match memory {
            Memory::Checked => Some(Heap::new()),
            Memory::Unchecked => None,
        };
        World {
            seed,
            sim: Sim::new(seed, config),
            procs: Vec::new(),
            pids: Vec::new(),
            referee,
            heap,
            programs: Vec::new(),
            hosted: Vec::new(),
            finished: Vec::new(),
            machine: NoMachine,
            calls: Queue::with_capacity(256),
            answers: Queue::with_capacity(256),
        }
    }

    /// Supplies the scenario's fake machine before the world starts.
    pub fn with_machine<M: Machine>(self, machine: M) -> World<P, R, M> {
        World {
            seed: self.seed,
            sim: self.sim,
            procs: self.procs,
            pids: self.pids,
            referee: self.referee,
            heap: self.heap,
            programs: self.programs,
            hosted: self.hosted,
            finished: self.finished,
            machine,
            calls: self.calls,
            answers: self.answers,
        }
    }
}

impl<P: Host, R: Referee<P>, M: Machine> World<P, R, M> {
    /// Registers a hosted program before execution, rejecting duplicate names.
    pub fn host(&mut self, program: HostedProgram<P>) {
        assert!(program.instances > 0 && program.operations > 0, "a hosted program has room to run");
        assert!(!self.programs.iter().any(|entry| entry.program == program.program), "one factory per program");
        self.programs.push(program);
    }

    /// Adds a process with a fake-machine root inherited from startup.
    pub fn spawn_root<F: FnOnce(Fd) -> P>(&mut self, root: Handle, make: F) -> usize {
        let pid = self.sim.spawn_process();
        let root = self.sim.root(pid, root);
        self.admit(pid, None, || make(root))
    }

    /// Adds the process `make` builds: its heap, from its making on, is the
    /// processes'.
    pub fn spawn<F: FnOnce() -> P>(&mut self, make: F) -> usize {
        let pid = self.sim.spawn_process();
        self.admit(pid, None, make)
    }

    fn admit<F: FnOnce() -> P>(&mut self, pid: Pid, hosted: Option<usize>, make: F) -> usize {
        let proc = match &mut self.heap {
            Some(heap) => heap.admit(make, P::worst_case),
            None => make(),
        };
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
    pub fn run(mut self) -> Outcome<P, M> {
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
            for at in 0..self.procs.len() {
                self.turn(at, now, wall);
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
        for (proc, pid) in self.procs.iter().zip(&self.pids) {
            assert!(proc.is_empty(), "seed {}: {pid} holds nothing once settled", self.seed);
            self.sim.assert_quiescent(*pid);
            self.sim.assert_no_open_fds(*pid);
        }
        Outcome {
            machine: self.machine,
            seed: self.seed,
            trace: self.sim.trace().to_vec(),
            end: self.sim.now(),
            heap: self.heap.as_ref().map(Heap::report),
            held: self.heap.as_ref().map(Heap::held),
            procs: self.procs,
            iterations,
        }
    }

    /// One turn of process `at`'s loop: reap, iterate, submit.
    fn turn(&mut self, at: usize, now: Time, wall: skein_lib::Wall) {
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
                let inherited = Inherited { pipes, signal: self.sim.open_signal_source(child) };
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
            Some(heap) => heap.around(at, || proc.iterate(now, wall)),
            None => proc.iterate(now, wall),
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

    fn serve_machine(&mut self) {
        self.sim.calls(&mut self.calls);
        while let Some(call) = self.calls.pop() {
            if let Ask::Spawn { program, .. } = &call.ask
                && self.programs.iter().any(|entry| entry.program == *program)
            {
                self.answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(Program::Service)) });
            } else {
                self.machine.step(call, &mut self.answers);
            }
        }
        self.sim.answer(&mut self.answers);
    }

    /// Whether any process has work now: its loop's, the kernel's deferred
    /// to its next entry, or completions delivered and not reaped.
    fn busy(&self, now: Time) -> bool {
        for (proc, pid) in self.procs.iter().zip(&self.pids) {
            if proc.work_pending(now) || self.sim.deferred(*pid) || self.sim.ready(*pid) > 0 {
                return true;
            }
        }
        false
    }

    /// Whether every process holds nothing and has nothing in flight.
    fn settled(&self) -> bool {
        for (proc, pid) in self.procs.iter().zip(&self.pids) {
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
