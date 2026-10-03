//! The world: one thread, one loop (testing-strategy.md, 1), driving each
//! process's io and its scripted owner over the simulator, iteration by
//! iteration, as a service's loop drives its layers (programming-model.md,
//! section 2; simulator.md, 3), and checking as it goes:
//!
//! - **contracts:** each entry point within its `MAX_OUT`; io's contract with
//!   the owner, in the [`Ledger`];
//! - **the scenario's expectations,** in the [`Referee`];
//! - **invariants once settled:** every slab empty and nothing in flight in
//!   io, nothing in flight in the simulator and every descriptor closed, every
//!   entity told `Closed`;
//! - **replay:** a run returns its trace, the simulator's records and io's
//!   events, for a test to compare with the same seed's.

use std::collections::VecDeque;
use std::fmt::Write;

use skein_io::kernel::{Complete, Submit};
use skein_io::{Event, Io, Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, Request};
use skein_lib::{Env, Queue, Time, Wall};
use skein_sim::{Config, Entry, Pid, Sim};

use crate::ledger::Ledger;
use crate::owner::{Directory, Owner};
use crate::referee::Referee;

/// The most iterations a world runs before it is declared stuck.
const STEPS: u32 = 200_000;

/// Room for what a process's loop holds between its stages.
const ROOM: u32 = 64;

/// One process: io, the owner above it, and the loop's queues between them.
#[derive(Debug)]
pub struct Proc {
    pub io: Io,
    pub env: Env<Limits>,
    pub owner: Owner,
    pub ledger: Ledger,
    pub completions: Queue<Complete>,
    pub subs: Queue<Submit>,
    events: Queue<Event>,
    requests: VecDeque<Request>,
    /// What io told, in order, for replay.
    pub log: Vec<String>,
}

impl Proc {
    #[must_use]
    pub fn new(limits: Limits, owner: Owner) -> Proc {
        let mut proc = Proc {
            io: Io::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            owner,
            ledger: Ledger::default(),
            completions: Queue::with_capacity(ROOM),
            subs: Queue::with_capacity(ROOM),
            events: Queue::with_capacity(ROOM),
            requests: VecDeque::new(),
            log: Vec::new(),
        };
        proc.owner.start(&mut proc.requests);
        proc
    }

    /// One iteration of the process's loop, between its reap and its submit:
    /// io's up stage, the owner's stage, io's down stage, the reclaim point.
    pub fn iterate(&mut self, now: Time, wall: Wall, directory: &mut Directory) {
        self.env.now = now;
        self.env.wall = wall;
        // io's stage: the ready list first, then completions, then deadlines.
        while self.io.is_ready() && self.room(MAX_OUT_RESUME) {
            let mark = self.marks();
            skein_io::resume(&mut self.io, &self.env, &mut self.events, &mut self.subs);
            self.within(mark, MAX_OUT_RESUME, "resume");
        }
        if !self.io.is_ready() {
            while !self.completions.is_empty() && self.room(MAX_OUT_UP) {
                let complete = self.completions.pop().expect("not empty");
                let mark = self.marks();
                skein_io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.subs);
                self.within(mark, MAX_OUT_UP, "up");
            }
        }
        while self.io.is_due(now) && self.room(MAX_OUT_FIRE) {
            let mark = self.marks();
            skein_io::fire(&mut self.io, &self.env, &mut self.events, &mut self.subs);
            self.within(mark, MAX_OUT_FIRE, "fire");
        }
        // The owner's stage.
        self.owner.begin();
        while let Some(event) = self.events.pop() {
            self.ledger.event(&event);
            self.log.push(format!("{} {event:?}", now.as_nanos()));
            self.owner.on(now, event, directory, &mut self.requests);
        }
        self.owner.tick(now, directory, &mut self.requests);
        // io's down stage.
        while self.io.takes() && self.room(MAX_OUT_DOWN) {
            let Some(request) = self.requests.pop_front() else {
                break;
            };
            self.ledger.request(&request);
            let mark = self.marks();
            skein_io::down(&mut self.io, &self.env, request, &mut self.subs);
            self.within(mark, MAX_OUT_DOWN, "down");
        }
        self.io.reclaim();
    }

    fn room(&self, max: MaxOut) -> bool {
        self.events.room() >= max.events && self.subs.room() >= max.submissions
    }

    fn marks(&self) -> (u32, u32) {
        (self.events.len(), self.subs.len())
    }

    /// An entry point emitted no more than it declared.
    fn within(&self, (events, subs): (u32, u32), max: MaxOut, entry: &str) {
        assert!(self.events.len() - events <= max.events, "{entry} told no more events than its MAX_OUT");
        assert!(self.subs.len() - subs <= max.submissions, "{entry} submitted no more than its MAX_OUT");
    }

    /// Whether the loop has work now, without the kernel or time.
    #[must_use]
    pub fn busy(&self, now: Time) -> bool {
        self.io.is_ready()
            || self.io.is_due(now)
            || !self.completions.is_empty()
            || !self.subs.is_empty()
            || !self.requests.is_empty()
    }
}

/// A world: the simulator, its processes, and the referee.
pub struct World {
    pub sim: Sim,
    pub procs: Vec<Proc>,
    /// Each process's in the simulator.
    pids: Vec<Pid>,
    pub referee: Referee,
    pub directory: Directory,
}

/// What a run left, for a test to compare or count.
#[derive(Debug)]
pub struct Outcome {
    pub trace: Vec<Entry>,
    pub logs: Vec<Vec<String>>,
    pub iterations: u32,
}

impl World {
    #[must_use]
    pub fn new(seed: u64, config: Config, referee: Referee) -> World {
        World { sim: Sim::new(seed, config), procs: Vec::new(), pids: Vec::new(), referee, directory: Directory::new() }
    }

    /// Adds a process running io under `limits`, with `owner` above it.
    pub fn spawn(&mut self, limits: Limits, owner: Owner) -> usize {
        self.pids.push(self.sim.spawn_process());
        self.procs.push(Proc::new(limits, owner));
        self.procs.len() - 1
    }

    /// Runs the world until the referee passed and everything settled, then
    /// checks the invariants of a settled world.
    #[must_use]
    pub fn run(mut self) -> Outcome {
        let mut iterations = 0;
        loop {
            iterations += 1;
            assert!(iterations < STEPS, "the world settles\n{}", self.trace());
            let now = self.sim.now();
            let wall = self.sim.wall();
            for (at, (proc, pid)) in self.procs.iter_mut().zip(&self.pids).enumerate() {
                self.sim.reap(*pid, &mut proc.completions);
                proc.iterate(now, wall, &mut self.directory);
                self.sim.submit(*pid, &mut proc.subs);
                self.referee.observe(now, at, &proc.owner);
            }
            if self.busy(now) {
                continue;
            }
            let owners: Vec<&Owner> = self.procs.iter().map(|proc| &proc.owner).collect();
            let settled = self.settled();
            if settled {
                self.referee.settled(&owners);
            }
            let (sim, procs) = (&self.sim, &self.procs);
            self.referee.overdue(now, &owners, &|| render(sim, procs));
            if self.referee.passed() && settled {
                break;
            }
            match self.next() {
                Some(at) => self.sim.advance_to(at),
                None => panic!("the world is idle, unsettled, with nothing due\n{}", self.trace()),
            }
        }
        for (proc, pid) in self.procs.iter().zip(&self.pids) {
            assert!(proc.io.is_empty(), "io holds nothing once settled: {:?}", proc.io);
            self.sim.assert_quiescent(*pid);
            self.sim.assert_no_open_fds(*pid);
            proc.ledger.settled();
        }
        Outcome {
            trace: self.sim.trace().to_vec(),
            logs: self.procs.into_iter().map(|proc| proc.log).collect(),
            iterations,
        }
    }

    /// Whether any process has work now: its loop's, or the kernel's deferred
    /// to its next entry, or completions delivered and not reaped.
    fn busy(&self, now: Time) -> bool {
        self.procs
            .iter()
            .zip(&self.pids)
            .any(|(proc, pid)| proc.busy(now) || self.sim.deferred(*pid) || self.sim.ready(*pid) > 0)
    }

    fn settled(&self) -> bool {
        self.procs
            .iter()
            .zip(&self.pids)
            .all(|(proc, pid)| proc.owner.done() && proc.io.is_empty() && self.sim.in_flight(*pid) == 0)
    }

    /// The earliest of what the simulator has due and every deadline in the
    /// world: io's, the owners', the referee's (simulator.md, 3).
    fn next(&self) -> Option<Time> {
        let now = self.sim.now();
        let mut next = self.sim.next_due();
        let mut at = |time: Option<Time>| {
            if let Some(time) = time {
                next = Some(next.map_or(time, |next| next.min(time)));
            }
        };
        for proc in &self.procs {
            at(proc.io.next_deadline());
            at(proc.owner.next_deadline(now));
        }
        at(self.referee.next_deadline());
        next
    }

    fn trace(&self) -> String {
        render(&self.sim, &self.procs)
    }
}

/// The run so far, for a failure: the simulator's trace under its seed, and
/// the last of what io told each process.
fn render(sim: &Sim, procs: &[Proc]) -> String {
    let mut text = sim.render_trace();
    for (at, proc) in procs.iter().enumerate() {
        let from = proc.log.len().saturating_sub(12);
        writeln!(text, "process {at}, io told, last:").expect("writing to a String");
        for line in &proc.log[from..] {
            writeln!(text, "  {line}").expect("writing to a String");
        }
    }
    text
}
