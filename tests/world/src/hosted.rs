//! Raw scripts for hosting stories, observing only pipes, signals and exits.

use std::collections::VecDeque;

use skein_io::kernel::{Complete, Done, Exit, Fd, Op, Pipe, Signal, Spawn, Submit, Way};
use skein_lib::{Queue, Time, Token, Wall};
use skein_world::{Host, Inherited, Machine};

/// One sequential kernel operation in a parent or child script.
#[derive(Clone, Copy, Debug)]
pub enum Act {
    /// The parent asks to start the hosted program.
    Spawn,
    /// The script writes these bytes to one inherited pipe.
    Write(usize, &'static [u8]),
    /// The script reads from one inherited pipe, possibly its end.
    Read(usize),
    /// The parent waits for its child to exit.
    Wait,
    /// The parent sends a termination or kill signal to its child.
    Signal(Signal),
    /// The child waits for a termination signal from its parent.
    ReadSignal,
    /// The script stops until this simulated instant.
    Pause(Time),
}

/// A script runs kernel operations and closes all its descriptors at the end.
#[derive(Debug)]
pub struct Script {
    acts: VecDeque<Act>,
    fds: Vec<Fd>,
    signal: Option<Fd>,
    pidfd: Option<Fd>,
    root: Option<Fd>,
    waiting: bool,
    next: u64,
    done: bool,
    pause: Option<Time>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    pub received: Vec<u8>,
    pub results: Vec<Result<Done, skein_io::kernel::Error>>,
    pub signals: Vec<skein_io::kernel::ServiceSignal>,
    pub child_exit: Option<Exit>,
    pub hog: usize,
    pub held: Box<[u8]>,
    pub leak: bool,
    pub terminal: Option<Exit>,
}

impl Script {
    #[must_use]
    pub fn new(acts: &[Act]) -> Self {
        Self {
            acts: acts.iter().copied().collect(),
            fds: Vec::new(),
            signal: None,
            pidfd: None,
            root: None,
            waiting: false,
            next: 1,
            done: false,
            pause: None,
            completions: Queue::with_capacity(8),
            submissions: Queue::with_capacity(8),
            received: Vec::new(),
            results: Vec::new(),
            signals: Vec::new(),
            child_exit: None,
            hog: 0,
            held: Box::new([]),
            leak: false,
            terminal: Some(Exit::Code(0)),
        }
    }

    #[must_use]
    pub fn parent(root: Fd, acts: &[Act]) -> Self {
        let mut script = Self::new(acts);
        script.root = Some(root);
        script
    }

    #[must_use]
    pub fn child(inherited: &Inherited, acts: &[Act]) -> Self {
        let mut script = Self::new(acts);
        script.fds = inherited.pipes.iter().map(|(_, fd)| *fd).collect();
        script.signal = Some(inherited.signal);
        script
    }

    fn submit(&mut self, kind: Op) {
        self.submissions.push(Submit { op: Token::new(self.next), kind });
        self.next += 1;
        self.waiting = true;
    }

    fn start(&mut self, act: Act) {
        let kind = match act {
            Act::Spawn => Op::Spawn {
                spawn: Box::new(Spawn {
                    program: Box::from(&b"hosted"[..]),
                    args: Box::new([Box::from(&b"argument"[..])]),
                    env: Box::new([Box::from(&b"KEY=value"[..])]),
                    root: self.root.expect("parent root"),
                    dir: Box::new([]),
                    pipes: Box::new([
                        Pipe { child: 0, way: Way::In, parent: None },
                        Pipe { child: 1, way: Way::Out, parent: None },
                    ]),
                }),
            },
            Act::Write(at, bytes) => Op::PipeWrite { fd: self.fds[at], bytes: Box::from(bytes), from: 0 },
            Act::Read(at) => Op::PipeRead { fd: self.fds[at], buf: Box::new([0; 32]) },
            Act::Wait => Op::Wait { pidfd: self.pidfd.expect("spawned child") },
            Act::Signal(signal) => Op::Signal { pidfd: self.pidfd.expect("spawned child"), signal },
            Act::ReadSignal => Op::ReadSignal { fd: self.signal.expect("child signal source") },
            Act::Pause(until) => {
                self.pause = Some(until);
                return;
            }
        };
        self.submit(kind);
    }
}

impl Host for Script {
    fn iterate(&mut self, now: Time, _wall: Wall) {
        if self.hog > 0 {
            self.held = vec![0; self.hog].into_boxed_slice();
            self.hog = 0;
        }
        while let Some(complete) = self.completions.pop() {
            self.waiting = false;
            match complete.result {
                Ok(Done::Spawned { pidfd }) => {
                    self.pidfd = Some(pidfd);
                    let Op::Spawn { spawn } = &complete.kind else { panic!("spawn record") };
                    self.fds = spawn.pipes.iter().map(|pipe| pipe.parent.expect("parent pipe end")).collect();
                }
                Ok(Done::Count(count)) => {
                    if let Op::PipeRead { buf, .. } = &complete.kind {
                        self.received.extend_from_slice(&buf[..usize::try_from(count).expect("small count")]);
                    }
                }
                Ok(Done::ServiceSignal(signal)) => self.signals.push(signal),
                Ok(Done::Exit(exit)) => self.child_exit = Some(exit),
                Ok(Done::Nothing | Done::Fd(_) | Done::Bound(_) | Done::Accepted { .. } | Done::Stat(_)) | Err(_) => {}
            }
            self.results.push(complete.result);
        }
        if self.waiting || self.done || self.pause.is_some_and(|at| now < at) {
            return;
        }
        self.pause = None;
        if let Some(act) = self.acts.pop_front() {
            self.start(act);
            return;
        }
        if let Some(fd) =
            self.fds.pop().or_else(|| self.signal.take()).or_else(|| self.pidfd.take()).or_else(|| self.root.take())
        {
            self.submit(Op::Close { fd });
        } else {
            self.done = true;
            if self.leak {
                self.leak = false;
                let _leaked = Box::leak(std::mem::take(&mut self.held));
            }
        }
    }

    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        !self.completions.is_empty() || (!self.waiting && !self.done && self.pause.is_none_or(|at| now >= at))
    }
    fn next_deadline(&self) -> Option<Time> {
        self.pause
    }
    fn is_empty(&self) -> bool {
        self.done && self.completions.is_empty() && self.submissions.is_empty()
    }
    fn exit(&self) -> Option<Exit> {
        if self.is_empty() { self.terminal } else { None }
    }

    fn worst_case(&self) -> u64 {
        64 * 1024
    }
    fn operations(&self) -> u32 {
        8
    }
}

/// A minimal root machine leaves hosted selection to the harness.
#[derive(Debug)]
pub struct RootMachine;

impl Machine for RootMachine {
    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<skein_sim::Answer>) {
        assert!(matches!(call.ask, skein_sim::Ask::Close { .. }), "only the parent's root close reaches this machine");
        answers.push(skein_sim::Answer { ticket: call.ticket, result: Ok(skein_sim::Reply::Done) });
    }
}

/// The referee has no scenario expectations; the test examines boundary facts.
#[derive(Debug)]
pub struct Judge;

impl skein_world::Referee<Script> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Script]) {}
    fn observe(&mut self, _now: Time, _procs: &[Script]) {}
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        true
    }
}

/// A hosted process story observed entirely through pipes and child exits.
#[derive(Clone, Copy, Debug)]
pub enum Story {
    /// A parent and child exchange bytes and drain the child's output to EOF.
    Exchange,
    /// The parent kills the child while its second input read waits.
    Kill,
    /// The child closes its input before the parent writes.
    EarlyExit,
    /// The child reads and answers its parent's termination signal.
    Terminate,
    /// The child exchanges while holding its largest scripted payload.
    Memory,
}

fn exchange_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply")])
}

fn killed_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply"), Act::Read(0)])
}

fn early_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[])
}

fn terminating_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::ReadSignal, Act::Write(1, b"terminated")])
}

fn full_child(spawn: &Spawn, inherited: &Inherited) -> Script {
    let mut script = exchange_child(spawn, inherited);
    script.hog = 60 * 1024;
    script
}

type Factory = fn(&Spawn, &Inherited) -> Script;

/// Runs one story with seeded completion latency and separately metered heaps.
#[must_use]
pub fn story(seed: u64, story: Story) -> skein_world::Outcome<Script, RootMachine> {
    let faults = skein_sim::Faults {
        latency: 1000,
        latency_max: skein_lib::Duration::from_millis(1),
        ..skein_sim::Faults::NONE
    };
    let mut world = skein_world::World::new(
        seed,
        skein_sim::Config { faults, ..skein_sim::Config::calm() },
        Judge,
        skein_world::Memory::Checked,
    )
    .with_machine(RootMachine);
    let (make, acts): (Factory, Vec<Act>) = match story {
        Story::Exchange => {
            (exchange_child, vec![Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Read(1), Act::Wait])
        }
        Story::Kill => (
            killed_child,
            vec![Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Signal(Signal::Kill), Act::Read(1), Act::Wait],
        ),
        Story::EarlyExit => (
            early_child,
            vec![
                Act::Spawn,
                Act::Pause(Time::from_nanos(10_000_000)),
                Act::Write(0, b"hello"),
                Act::Read(1),
                Act::Wait,
            ],
        ),
        Story::Terminate => {
            (terminating_child, vec![Act::Spawn, Act::Signal(Signal::Terminate), Act::Read(1), Act::Read(1), Act::Wait])
        }
        Story::Memory => (full_child, vec![Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Read(1), Act::Wait]),
    };
    world.host(skein_world::HostedProgram { program: Box::from(&b"hosted"[..]), make, instances: 1, operations: 8 });
    world.spawn_root(skein_sim::Handle::new(1), |root| Script::parent(root, &acts));
    world.run()
}

/// Checks the story's external evidence after the harness checks ownership.
pub fn check_story(story: Story, outcome: &skein_world::Outcome<Script, RootMachine>) {
    let parent = &outcome.procs[0];
    match story {
        Story::Exchange | Story::Memory => {
            assert_eq!(parent.received, b"reply", "the parent reads the child's reply then EOF");
            assert_eq!(outcome.procs[1].received, b"hello", "the child reads its parent's bytes");
            assert_eq!(parent.child_exit, Some(Exit::Code(0)), "the parent wait settles normally");
        }
        Story::Kill => {
            assert_eq!(parent.received, b"reply", "the exchange began before the kill and then reached EOF");
            assert_eq!(parent.child_exit, Some(Exit::Signal(9)), "the parent's wait preserves the kill terminal");
            assert_eq!(outcome.procs.len(), 1, "the child is dropped after its kill");
            assert_eq!(outcome.killed.len(), 1, "the harness records the terminated child");
            let (peak, bound) = outcome.killed[0].heap.expect("the killed child was metered and its drop checked");
            assert!(peak > 0 && peak <= bound, "the killed child's own heap fits its bound");
        }
        Story::EarlyExit => {
            assert!(
                parent.results.contains(&Err(skein_io::kernel::Error::BrokenPipe)),
                "writing after the child's input closes returns BrokenPipe"
            );
            assert_eq!(parent.child_exit, Some(Exit::Code(0)), "the early child exits normally");
            assert!(parent.received.is_empty(), "the parent reads EOF from the child without output");
        }
        Story::Terminate => {
            assert_eq!(parent.received, b"terminated", "the child handles its parent's termination signal");
            assert_eq!(
                outcome.procs[1].signals,
                [skein_io::kernel::ServiceSignal::Terminate],
                "the child reads exactly that signal"
            );
            assert_eq!(parent.child_exit, Some(Exit::Code(0)), "handling the termination signal exits normally");
        }
    }
    if matches!(story, Story::Memory) {
        let (peak, bound) = outcome.heap.as_ref().expect("checked heaps")[1];
        assert!(
            peak <= bound && peak + 4096 >= bound,
            "the child's largest payload reaches its declared bound within container headroom: {peak}/{bound}"
        );
    }
}
