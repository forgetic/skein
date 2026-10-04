//! A simulated world (testing-strategy.md, 2.7): one thread, one loop,
//! driving each process's `iterate` over the simulator, iteration by
//! iteration, as each one's shell would, with the referee beside them.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::{self, Debug, Write};

use skein_lib::Time;
use skein_sim::{Config, Entry, Pid, Sim};

use crate::heap::{Heap, Memory};
use crate::host::Host;
use crate::referee::Referee;

/// The most iterations a world runs before it is declared stuck.
const STEPS: u32 = 1_000_000;

/// How many lines of the trace a failure prints.
const TAIL: usize = 80;

/// A world: the simulator, its processes, and the referee.
pub struct World<P, R> {
    seed: u64,
    sim: Sim,
    procs: Vec<P>,
    /// Each process's, in the simulator.
    pids: Vec<Pid>,
    referee: R,
    heap: Option<Heap>,
}

/// Briefly: its seed, and how many processes it hosts.
impl<P, R> Debug for World<P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("World").field("seed", &self.seed).field("procs", &self.procs.len()).finish_non_exhaustive()
    }
}

/// What a run left, for a test to compare or look at.
#[derive(Debug)]
pub struct Outcome<P> {
    pub seed: u64,
    /// Every submission and completion, and every fault drawn: what the same
    /// seed replays to.
    pub trace: Vec<Entry>,
    /// The processes as the run left them, settled.
    pub procs: Vec<P>,
    pub iterations: u32,
    /// When the world settled.
    pub end: Time,
    /// The most heap the processes held at once, and the sum of their worst
    /// cases, when memory was checked.
    pub heap: Option<(u64, u64)>,
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
        World { seed, sim: Sim::new(seed, config), procs: Vec::new(), pids: Vec::new(), referee, heap }
    }

    /// Adds the process `make` builds: its heap, from its making on, is the
    /// processes'.
    pub fn spawn<F: FnOnce() -> P>(&mut self, make: F) -> usize {
        let proc = match &mut self.heap {
            Some(heap) => heap.admit(make, P::worst_case),
            None => make(),
        };
        self.pids.push(self.sim.spawn_process());
        self.procs.push(proc);
        self.procs.len().checked_sub(1).expect("just pushed")
    }

    /// Runs the world until the referee passed and everything settled, then
    /// checks that every process holds nothing, and that the simulator has
    /// nothing in flight and no descriptor open.
    #[must_use]
    pub fn run(mut self) -> Outcome<P> {
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
            seed: self.seed,
            trace: self.sim.trace().to_vec(),
            end: self.sim.now(),
            heap: self.heap.as_ref().map(|heap| (heap.most(), heap.bound())),
            procs: self.procs,
            iterations,
        }
    }

    /// One turn of process `at`'s loop: reap, iterate, submit.
    fn turn(&mut self, at: usize, now: Time, wall: skein_lib::Wall) {
        let pid = *self.pids.get(at).expect("a pid for each process");
        let proc = self.procs.get_mut(at).expect("a process at each index");
        self.sim.reap(pid, proc.completions());
        match &mut self.heap {
            Some(heap) => heap.around(|| proc.iterate(now, wall)),
            None => proc.iterate(now, wall),
        }
        self.sim.submit(pid, proc.submissions());
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
