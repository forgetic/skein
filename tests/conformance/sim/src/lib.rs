//! The conformance suite against the simulator (kernel.md, 8): every
//! scenario of `skein_conformance`, over seeds, in a calm world and with the
//! faults loopback can show. The same scenarios run against the ring in
//! `tests/conformance/ring`.
//!
//! What the test binaries share: [`Simulated`], the simulator as the suite's
//! backend, with the minimal fake machine behind its seam for files; the
//! worlds the suite runs it in; and [`each_seed`], which names a failing seed
//! with the end of its trace. The focused tests
//! (`tests/conformance.rs`) run each scenario over the calm seeds and a few
//! of chaos; the fuzzy ones (`tests/fuzzy_conformance.rs`) over many seeds of
//! chaos, and count the outcomes each race shows over them
//! (testing-strategy.md, 8).

use std::panic::{AssertUnwindSafe, catch_unwind};

use skein_conformance::{
    Backend, Cancelling, Check, Item, Made, Race, cancel_accept_racing_a_connect, cancel_recv_racing_bytes,
};
use skein_fake_machine::{Machine, serve};
use skein_io::kernel::{Complete, Fd, Submit};
use skein_lib::{Duration, Queue, Time};
use skein_sim::{Config, Faults, Handle, Pid, Sim};

/// Seeds per scenario: a calm world draws only ports and the order of a
/// cancel and its target; chaos draws much more, a few seeds of it in the
/// focused tests and many in the fuzzy ones.
pub const CALM: u64 = 16;
pub const SMOKE: u64 = 4;
pub const CHAOS: u64 = 200;

/// How many trace entries a failure prints.
const TAIL: usize = 60;

/// The simulator as the suite's backend: a world, the minimal fake machine
/// it passes files to, and the processes the suite opened in it.
#[derive(Debug)]
pub struct Simulated {
    sim: Sim,
    machine: Machine,
    processes: Vec<Pid>,
}

impl Simulated {
    #[must_use]
    pub fn new(seed: u64, config: Config) -> Simulated {
        Simulated { sim: Sim::new(seed, config), machine: Machine::new(), processes: Vec::new() }
    }

    #[must_use]
    pub fn sim(&self) -> &Sim {
        &self.sim
    }
}

impl Backend for Simulated {
    type Process = Pid;

    fn open(&mut self) -> Pid {
        let pid = self.sim.spawn_process();
        self.processes.push(pid);
        pid
    }

    /// Submits, then has the machine answer what the simulator asked of
    /// it, as a world does after each submit (simulator.md, 3.1).
    fn submit(&mut self, process: Pid, records: &mut Queue<Submit>) {
        self.sim.submit(process, records);
        serve(&mut self.machine, &mut self.sim);
    }

    fn reap(&mut self, process: Pid, completions: &mut Queue<Complete>) {
        self.sim.reap(process, completions);
    }

    fn now(&self) -> Time {
        self.sim.now()
    }

    fn enter(&mut self, process: Pid) {
        self.sim.submit(process, &mut Queue::with_capacity(0));
    }

    /// Every process enters; then, unless that delivered something to
    /// `process`, time moves to what is next due, or by `bound` when that
    /// is later or nothing is due.
    fn pass(&mut self, process: Pid, bound: Duration) {
        for &pid in &self.processes {
            self.sim.submit(pid, &mut Queue::with_capacity(0));
        }
        if self.sim.ready(process) > 0 {
            return;
        }
        let until = self.sim.now().saturating_add(bound);
        match self.sim.next_due() {
            Some(at) if at < until => self.sim.advance_to(at),
            Some(_) | None => self.sim.advance_to(until),
        }
    }

    fn sleep(&mut self, span: Duration) {
        let until = self.sim.now().saturating_add(span);
        self.sim.advance_to(until);
    }

    fn assert_settled(&self, process: Pid) {
        self.sim.assert_quiescent(process);
        self.sim.assert_no_open_fds(process);
        assert_eq!(self.machine.open_handles(), 0, "the machine has nothing open once every descriptor closed");
    }

    /// A root laid in the machine, its handle given to the process.
    fn root(&mut self, process: Pid, tree: &[Item]) -> Fd {
        let mut items = Vec::new();
        for item in tree {
            let made = match &item.made {
                Made::File(bytes) => skein_fake_machine::Made::File(bytes.clone()),
                Made::Directory => skein_fake_machine::Made::Directory,
                Made::Link(target) => skein_fake_machine::Made::Link(target.clone()),
            };
            items.push(skein_fake_machine::Item { path: item.path.clone(), made, mode: item.mode });
        }
        let opened = self.machine.lay(&items);
        self.sim.root(process, Handle::new(opened.raw()))
    }
}

/// The chaos loopback and a healthy scratch directory can show:
/// [`Config::chaos`]'s small buffers and short backlog, latency, short
/// receives and sends, short reads and writes, raced cancels and late
/// resets, without the faults that model a network or a disk beyond them.
#[must_use]
pub const fn loopback_chaos() -> Config {
    let chaos = Config::chaos();
    let faults = Faults {
        reset: 0,
        refuse: 0,
        no_buffer: 0,
        timed_out: 0,
        cancel_unsubmitted: 0,
        no_space: 0,
        read_only: 0,
        io_error: 0,
        ..chaos.faults
    };
    Config { faults, ..chaos }
}

/// The chaos of loopback, and cancels the backend cannot submit: a fault
/// beyond loopback, which only the cancel scenarios meet.
#[must_use]
pub fn cancel_chaos() -> Config {
    let chaos = loopback_chaos();
    let faults = Faults { cancel_unsubmitted: Faults::CHAOS.cancel_unsubmitted, ..chaos.faults };
    Config { faults, ..chaos }
}

/// Runs `scenario` and checks what it saw, for each seed below `seeds`.
pub fn each_seed<S: Check>(config: Config, seeds: u64, scenario: fn(&mut Simulated) -> S) {
    for seed in 0..seeds {
        let mut world = Simulated::new(seed, config);
        let outcome = catch_unwind(AssertUnwindSafe(|| scenario(&mut world).check()));
        if outcome.is_err() {
            let trace = world.sim().trace();
            let tail = skein_sim::render(seed, &trace[trace.len().saturating_sub(TAIL)..]);
            panic!("the scenario failed at seed {seed} of {config:?}\n{tail}");
        }
    }
}

/// A target's race, as a scenario of the world alone.
pub type Racing = fn(Race) -> fn(&mut Simulated) -> Cancelling;

/// Each race of a target.
pub const RACES: [Race; 3] = [Race::CancelFirst, Race::ArrivedAway, Race::ArrivedEntered];

#[must_use]
pub fn racing_recv(race: Race) -> fn(&mut Simulated) -> Cancelling {
    match race {
        Race::CancelFirst => |world| cancel_recv_racing_bytes(world, Race::CancelFirst),
        Race::ArrivedAway => |world| cancel_recv_racing_bytes(world, Race::ArrivedAway),
        Race::ArrivedEntered => |world| cancel_recv_racing_bytes(world, Race::ArrivedEntered),
    }
}

#[must_use]
pub fn racing_accept(race: Race) -> fn(&mut Simulated) -> Cancelling {
    match race {
        Race::CancelFirst => |world| cancel_accept_racing_a_connect(world, Race::CancelFirst),
        Race::ArrivedAway => |world| cancel_accept_racing_a_connect(world, Race::ArrivedAway),
        Race::ArrivedEntered => |world| cancel_accept_racing_a_connect(world, Race::ArrivedEntered),
    }
}
