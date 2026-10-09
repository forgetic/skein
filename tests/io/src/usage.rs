//! Resource reads through io on the shared simulated and real worlds
//! (io.md, section 6.1). The owner observes exit, reads, closes, then reads
//! again; the referee checks that reaping alone adds the child's resources.

use skein_io::kernel::{Complete, Done, Fd, Op, Resources, Spawn, Submit, Usage};
use skein_io::{Event, Io, Measured, Request};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_sim::{Answer, Ask, Config, Handle, Program, Reply};
use skein_world::{Host, Memory, Referee, World};

const OWNER: Token = Token::new(1);
const READ: Token = Token::new(2);
const ROOT_CLOSE: Token = Token::new(u64::MAX);

pub const OWN: Resources =
    Resources { user: Duration::from_millis(11), system: Duration::from_millis(7), peak_rss_bytes: 8192 };
pub const CHILD: Resources =
    Resources { user: Duration::from_millis(5), system: Duration::from_millis(3), peak_rss_bytes: 4096 };

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Exiting,
    Before,
    Closing,
    After,
    Done,
}

/// An owner asking for usage before and after its child's close, ending empty.
#[derive(Debug)]
pub struct Process {
    io: Io,
    env: Env<skein_io::Limits>,
    phase: Phase,
    child: Option<Token>,
    pub pidfd: Option<Fd>,
    root: Option<Fd>,
    root_closing: bool,
    pub reads: Vec<Usage>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    events: Queue<Event>,
    requests: Queue<Request>,
}

impl Process {
    #[must_use]
    pub fn new(root: Fd, program: &[u8]) -> Self {
        let limits = crate::scenarios::limits(1);
        let mut requests = Queue::with_capacity(4);
        requests.push(Request::Spawn {
            owner: OWNER,
            spawn: Spawn {
                root,
                dir: Box::from(b".".as_slice()),
                program: Box::from(program),
                args: Box::from([Box::from(b"usage".as_slice())]),
                env: Box::new([]),
                pipes: Box::new([]),
            },
        });
        Self {
            io: Io::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            phase: Phase::Exiting,
            child: None,
            pidfd: None,
            root: Some(root),
            root_closing: false,
            reads: Vec::with_capacity(2),
            completions: Queue::with_capacity(4),
            submissions: Queue::with_capacity(4),
            events: Queue::with_capacity(4),
            requests,
        }
    }

    fn observe(&mut self, event: Event) {
        match event {
            Event::Spawned { owner, child, pipes } => {
                assert_eq!(owner, OWNER);
                assert!(pipes.is_empty());
                assert!(self.child.replace(child).is_none());
            }
            Event::Exited { owner, .. } => {
                assert_eq!(owner, OWNER);
                assert_eq!(self.phase, Phase::Exiting);
                self.phase = Phase::Before;
                self.requests.push(Request::Usage { owner: READ });
            }
            Event::Usage { owner, usage: Measured::Read(usage) } => {
                assert_eq!(owner, READ);
                assert!(self.reads.len() < 2);
                self.reads.push(usage);
                match self.phase {
                    Phase::Before => {
                        self.pidfd = None;
                        self.phase = Phase::Closing;
                        self.requests.push(Request::Close { entity: self.child.expect("spawned child") });
                    }
                    Phase::After => self.phase = Phase::Done,
                    Phase::Exiting | Phase::Closing | Phase::Done => panic!("usage at its requested point"),
                }
            }
            Event::Closed { owner } => {
                assert_eq!(owner, OWNER);
                assert_eq!(self.phase, Phase::Closing);
                self.pidfd = None;
                self.phase = Phase::After;
                self.requests.push(Request::Usage { owner: READ });
            }
            Event::Usage { usage: Measured::Unread, .. } => panic!("the test backend reads resources"),
            Event::Listening { .. }
            | Event::Accepted { .. }
            | Event::Connecting { .. }
            | Event::Connected { .. }
            | Event::Stream { .. }
            | Event::Output { .. }
            | Event::Shutdown { .. }
            | Event::Failed { .. } => panic!("unexpected usage-story event: {event:?}"),
        }
    }

    pub fn check(&self, simulated: bool) {
        assert_eq!(self.reads.len(), 2);
        let before = self.reads[0];
        let after = self.reads[1];
        if simulated {
            assert_eq!(before, Usage { own: OWN, children: Resources::ZERO });
            assert_eq!(after, Usage { own: OWN, children: CHILD });
        } else {
            assert!(
                after.children.user.as_nanos() + after.children.system.as_nanos()
                    > before.children.user.as_nanos() + before.children.system.as_nanos()
            );
            assert!(after.children.peak_rss_bytes > 0);
            assert!(after.children.peak_rss_bytes >= before.children.peak_rss_bytes);
            assert!(after.own.peak_rss_bytes > 0);
        }
        assert!(self.is_empty());
    }
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.env.now = now;
        self.env.wall = wall;
        while self.io.is_ready() {
            skein_io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
        }
        while let Some(complete) = self.completions.pop() {
            if complete.op == ROOT_CLOSE {
                assert_eq!(complete.result, Ok(Done::Nothing));
                self.root_closing = false;
            } else {
                if let Ok(Done::Spawned { pidfd }) = complete.result {
                    self.pidfd = Some(pidfd);
                }
                skein_io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.submissions);
            }
        }
        while let Some(event) = self.events.pop() {
            self.observe(event);
        }
        while let Some(request) = self.requests.pop() {
            skein_io::down(&mut self.io, &self.env, request, &mut self.submissions);
        }
        self.io.reclaim();
        if self.phase == Phase::Done
            && self.io.is_empty()
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
    fn work_pending(&self, _now: Time) -> bool {
        self.io.is_ready() || !self.completions.is_empty() || !self.requests.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.phase == Phase::Done
            && self.root.is_none()
            && !self.root_closing
            && self.io.is_empty()
            && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        skein_io::worst_case(&self.env.limits).expect("small limits") + 16 * 1024
    }
    fn operations(&self) -> u32 {
        skein_io::operations(&self.env.limits).expect("small limits") + 1
    }
}

/// The referee validates both usage terminals only after the owner settles.
#[derive(Debug)]
pub struct Judge {
    pub simulated: bool,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Process]) {}
    fn observe(&mut self, _now: Time, procs: &[Process]) {
        if procs[0].is_empty() {
            procs[0].check(self.simulated);
        }
    }
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

#[derive(Debug)]
pub struct Machine(skein_fake_machine::Machine);
impl skein_world::Machine for Machine {
    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<Answer>) {
        if let Ask::Spawn { .. } = call.ask {
            answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(Program::Exit(0))) });
        } else {
            skein_fake_machine::step(&mut self.0, call, answers);
        }
    }
}

#[must_use]
pub fn run(seed: u64, config: Config, memory: Memory) -> skein_world::Outcome<Process, Machine> {
    let mut machine = skein_fake_machine::Machine::new();
    let root = machine.lay(&[]);
    let mut world = World::new(seed, config, Judge { simulated: true }, memory).with_machine(Machine(machine));
    world.spawn_root(Handle::new(root.raw()), |root| Process::new(root, b"usage"));
    world.run_with_inputs(|sim, pids, procs| {
        sim.set_usage(pids[0], OWN);
        if let Some(pidfd) = procs[0].pidfd {
            sim.set_child_usage(pids[0], pidfd, CHILD);
        }
    })
}
