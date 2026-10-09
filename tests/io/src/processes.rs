//! io's group lifetime over the shared world harness (io.md, sections 6 and 8).
//! The scripted owner sees tokens, pipe EOF and exit, never service state.
//! The same owner runs against the minimal machine and the real fixture.

use skein_io::kernel::{Complete, Fd, Op, Pipe, Signal, Spawn, Submit, Target, Way};
use skein_io::{Event, Io, Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, Request};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_sim::{Config, Handle};
use skein_world::{Host, Memory, Referee, World};

const OWNER: Token = Token::new(1);
const ROOT_CLOSE: Token = Token::new(u64::MAX);

/// The owner's action against a group whose descendant keeps stdout open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Story {
    /// Signal the live leader's group after the fixture's descendant starts.
    SignalRunning,
    /// Close the exited leader, which must kill its remaining group.
    CloseExited,
    /// Signal the group after the leader's observed exit, before its close.
    SignalExited,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lifecycle {
    Running,
    Exited,
    Closed,
}

/// A scripted process holding io and its queues until every terminal settles.
#[derive(Debug)]
pub struct Process {
    io: Io,
    env: Env<Limits>,
    story: Story,
    root: Option<Fd>,
    root_closing: bool,
    child: Option<Token>,
    pipes: Vec<Token>,
    ready: bool,
    acted: bool,
    lifecycle: Lifecycle,
    ended: u32,
    pipe_closed: u32,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    events: Queue<Event>,
    requests: Queue<Request>,
}

impl Process {
    fn mark(&self, max: MaxOut) -> (u32, u32) {
        assert!(self.events.room() >= max.events && self.submissions.room() >= max.submissions);
        (self.events.len(), self.submissions.len())
    }

    fn within(&self, before: (u32, u32), max: MaxOut) {
        assert!(self.events.len().checked_sub(before.0).expect("events append") <= max.events);
        assert!(self.submissions.len().checked_sub(before.1).expect("submissions append") <= max.submissions);
    }

    /// Starts the actual owner; `real` requires the fixture's complete readiness line.
    #[must_use]
    pub fn new(root: Fd, program: &[u8], story: Story, real: bool) -> Self {
        let limits = Limits { intake: 64, receive: 32, ..crate::scenarios::limits(3) };
        let mut requests = Queue::with_capacity(32);
        let mut pipes = vec![Pipe { child: 1, way: Way::Out, parent: None }];
        if real {
            pipes.push(Pipe { child: 2, way: Way::Out, parent: None });
        }
        requests.push(Request::Spawn {
            owner: OWNER,
            spawn: Spawn {
                root,
                dir: Box::from(b".".as_slice()),
                program: Box::from(program),
                args: Box::from([Box::from(match story {
                    Story::SignalRunning => b"fork-live".as_slice(),
                    Story::CloseExited | Story::SignalExited => b"fork-exit".as_slice(),
                })]),
                env: Box::new([]),
                pipes: pipes.into_boxed_slice(),
            },
        });
        Self {
            io: Io::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            story,
            root: Some(root),
            root_closing: false,
            child: None,
            pipes: Vec::with_capacity(2),
            ready: !real,
            acted: false,
            lifecycle: Lifecycle::Running,
            ended: 0,
            pipe_closed: 0,
            completions: Queue::with_capacity(32),
            submissions: Queue::with_capacity(32),
            events: Queue::with_capacity(32),
            requests,
        }
    }

    fn demand(&mut self, pipe: Token, read: Read) {
        self.requests.push(Request::Stream { stream: pipe, down: Down::Demand { read, room: 0 } });
    }

    fn observe(&mut self, event: Event) {
        match event {
            Event::Spawned { owner, child, pipes } => {
                assert_eq!(owner, OWNER);
                assert!(self.child.replace(child).is_none(), "one child admitted");
                self.pipes.extend(pipes.iter().copied());
                self.demand(pipes[0], Read::Fill(1));
                if let Some(&stderr) = pipes.get(1) {
                    self.demand(stderr, Read::Line { max: 64 });
                }
            }
            Event::Exited { owner, .. } => {
                assert_eq!(owner, OWNER);
                assert_eq!(self.lifecycle, Lifecycle::Running, "one observed exit");
                self.lifecycle = Lifecycle::Exited;
            }
            Event::Stream { owner, up: Up::Bytes(bytes) } => {
                assert_eq!(self.pipes.get(1), Some(&owner), "only stderr carries readiness");
                assert!(bytes.starts_with(b"descendant:") && bytes.ends_with(b"\n"));
                self.ready = true;
                self.demand(owner, Read::Fill(1));
            }
            Event::Stream { owner, up: Up::End } => {
                assert!(self.pipes.contains(&owner));
                self.ended = self.ended.checked_add(1).expect("two pipes");
                self.requests.push(Request::Close { entity: owner });
            }
            Event::Closed { owner } => {
                if owner == OWNER {
                    assert_eq!(self.lifecycle, Lifecycle::Exited, "one child terminal follows exit");
                    assert!(self.pipe_closed == u32::try_from(self.pipes.len()).expect("two pipes"));
                    self.lifecycle = Lifecycle::Closed;
                } else {
                    assert!(self.pipes.contains(&owner));
                    self.pipe_closed = self.pipe_closed.checked_add(1).expect("two pipes");
                }
            }
            Event::Failed { .. }
            | Event::Stream { up: Up::Failed(_) | Up::Room, .. }
            | Event::Output { .. }
            | Event::Listening { .. }
            | Event::Accepted { .. }
            | Event::Connecting { .. }
            | Event::Connected { .. }
            | Event::Shutdown { .. } => panic!("unexpected process event: {event:?}"),
        }
    }

    fn act(&mut self) {
        if self.acted || !self.ready {
            return;
        }
        let Some(child) = self.child else { return };
        let request = match self.story {
            Story::SignalRunning => Request::Signal { child, signal: Signal::Kill, to: Target::Group },
            Story::SignalExited if self.lifecycle == Lifecycle::Exited => {
                Request::Signal { child, signal: Signal::Kill, to: Target::Group }
            }
            Story::CloseExited if self.lifecycle == Lifecycle::Exited => Request::Close { entity: child },
            Story::SignalExited | Story::CloseExited => return,
        };
        self.acted = true;
        self.requests.push(request);
    }

    /// The referee checks exact exit, EOF and close counts, independent of internal phases.
    pub fn check(&self) {
        assert!(self.acted && self.lifecycle == Lifecycle::Closed);
        assert_eq!(self.ended, u32::try_from(self.pipes.len()).expect("two pipes"));
        assert_eq!(self.pipe_closed, self.ended);
        assert!(self.is_empty(), "the owner and io hold nothing");
    }
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.env.now = now;
        self.env.wall = wall;
        while self.io.is_ready() {
            let before = self.mark(MAX_OUT_RESUME);
            skein_io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            self.within(before, MAX_OUT_RESUME);
        }
        while let Some(complete) = self.completions.pop() {
            if complete.op == ROOT_CLOSE {
                assert!(complete.result.is_ok());
                self.root_closing = false;
            } else {
                let before = self.mark(MAX_OUT_UP);
                skein_io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.submissions);
                self.within(before, MAX_OUT_UP);
            }
        }
        while self.io.is_due(now) {
            let before = self.mark(MAX_OUT_FIRE);
            skein_io::fire(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            self.within(before, MAX_OUT_FIRE);
        }
        while let Some(event) = self.events.pop() {
            self.observe(event);
        }
        self.act();
        while let Some(request) = self.requests.pop() {
            let before = self.mark(MAX_OUT_DOWN);
            skein_io::down(&mut self.io, &self.env, request, &mut self.submissions);
            self.within(before, MAX_OUT_DOWN);
        }
        self.io.reclaim();
        if self.io.is_empty()
            && self.lifecycle == Lifecycle::Closed
            && let Some(fd) = self.root.take()
        {
            self.root_closing = true;
            self.submissions.push(Submit { op: ROOT_CLOSE, kind: Op::Close { fd } });
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.io.is_ready() || self.io.is_due(now) || !self.completions.is_empty() || !self.requests.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.lifecycle == Lifecycle::Closed
            && self.io.is_empty()
            && self.root.is_none()
            && !self.root_closing
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.requests.is_empty()
    }
    fn worst_case(&self) -> u64 {
        skein_io::worst_case(&self.env.limits).expect("small io limits").checked_add(64 * 1024).expect("small owner")
    }
    fn operations(&self) -> u32 {
        skein_io::operations(&self.env.limits).expect("small io limits").checked_add(1).expect("one startup root")
    }
}

/// The process referee requires every actual pipe to end and the child to close.
#[derive(Debug, Default)]
pub struct Judge {
    passed: bool,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Process]) {}
    fn observe(&mut self, _now: Time, procs: &[Process]) {
        if procs[0].is_empty() {
            procs[0].check();
            self.passed = true;
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        self.passed
    }
}

/// The minimal fake machine behind the world's generic machine face.
#[derive(Debug)]
pub struct Machine(skein_fake_machine::Machine);
impl skein_world::Machine for Machine {
    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<skein_sim::Answer>) {
        skein_fake_machine::step(&mut self.0, call, answers);
    }
}

/// Runs the process story with the shared loop, exact kernel trace and memory checking available.
#[must_use]
pub fn run(seed: u64, config: Config, story: Story, memory: Memory) -> skein_world::Outcome<Process, Machine> {
    let mut machine = skein_fake_machine::Machine::new();
    let root = machine.lay(&[]);
    let mut world = World::new(seed, config, Judge::default(), memory).with_machine(Machine(machine));
    world.spawn_root(Handle::new(root.raw()), |root| Process::new(root, b"process_fixture", story, false));
    let outcome = world.run();
    outcome.procs[0].check();
    outcome
}

/// Only completion latency changes the order in these process stories.
#[must_use]
pub fn chaos() -> Config {
    Config {
        faults: skein_sim::Faults { latency: 1000, latency_max: Duration::from_millis(2), ..skein_sim::Faults::NONE },
        ..Config::calm()
    }
}

#[cfg(test)]
mod tests {
    use super::{Judge, Lifecycle, Process, Story};
    use skein_io::kernel::Fd;
    use skein_lib::{Time, Token};
    use skein_world::Referee;

    fn terminal(ended: u32, pipe_closed: u32) -> Process {
        let mut process = Process::new(Fd::new(3), b"process_fixture", Story::SignalExited, false);
        process.root = None;
        process.lifecycle = Lifecycle::Closed;
        process.acted = true;
        process.pipes.push(Token::new(7));
        process.ended = ended;
        process.pipe_closed = pipe_closed;
        while process.requests.pop().is_some() {}
        process
    }

    #[test]
    fn the_referee_rejects_a_child_terminal_without_the_pipes_eof() {
        let result = std::panic::catch_unwind(|| Judge::default().observe(Time::ZERO, &[terminal(0, 1)]));
        assert!(result.is_err());
    }

    #[test]
    fn the_referee_rejects_a_child_terminal_before_its_pipe_closed() {
        let result = std::panic::catch_unwind(|| Judge::default().observe(Time::ZERO, &[terminal(1, 0)]));
        assert!(result.is_err());
    }

    #[test]
    fn the_referee_passes_only_after_the_whole_group_lifetime_settles() {
        let mut judge = Judge::default();
        assert!(!judge.passed());
        judge.observe(Time::ZERO, &[terminal(1, 1)]);
        assert!(judge.passed());
    }
}
