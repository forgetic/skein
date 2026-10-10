//! One append-stream owner over the shared worlds. It observes io's events
//! and the filesystem's bytes, never the pipe machine's private state.

use skein_fake_machine::{How, Item, Machine as Files, Opened};
use skein_io::kernel::{Complete, Fd, Submit};
use skein_io::{Event, Io, Limits, Request};
use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_sim::{Answer, Call, Config, Handle};
use skein_world::{Host, Machine, Memory, Referee, World};

const PARTS: [&[u8]; 3] = [b"one", b"two", b"end"];
pub const CONTENTS: &[u8] = b"prefix:onetwoend";

/// The owner's terminal policy, with deadlines kept by io.
#[derive(Clone, Copy, Debug)]
pub enum Story {
    Finish,
    Close,
    Abort,
    WriteDeadline,
}

/// Actual output-only stream observations, retained for seeded replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evidence {
    Room,
    Failed(Fault),
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Asking,
    Waiting,
    Granted,
    Idle,
    Closing,
    Closed,
}

/// A scripted owner that sends only within granted room and settles its stream.
#[derive(Debug)]
pub struct Writer {
    io: Io,
    env: Env<Limits>,
    stream: Token,
    story: Story,
    phase: Phase,
    sent: usize,
    pub events: Vec<Evidence>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    up: Queue<Event>,
}

impl Writer {
    #[must_use]
    pub fn new(file: Fd, story: Story, write_timeout: Duration) -> Self {
        let limits = Limits {
            sockets: 1,
            refusals: 1,
            intake: 4,
            receive: 4,
            output: 9,
            sends: 2,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_millis(100),
            retry: Duration::from_millis(1),
        };
        let mut io = Io::new(&limits);
        let stream = io.adopt_append(file, write_timeout).expect("one startup append file");
        Self {
            io,
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            stream,
            story,
            phase: Phase::Asking,
            sent: 0,
            events: Vec::with_capacity(8),
            completions: Queue::with_capacity(8),
            submissions: Queue::with_capacity(8),
            up: Queue::with_capacity(4),
        }
    }

    fn observed(&mut self) {
        while let Some(event) = self.up.pop() {
            assert!(self.events.len() < 8, "the three sends have bounded observation storage");
            let evidence = match event {
                Event::Stream { owner, up } => {
                    assert_eq!(owner, self.stream);
                    match up {
                        Up::Room => {
                            assert_eq!(self.phase, Phase::Waiting);
                            self.phase = Phase::Granted;
                            Evidence::Room
                        }
                        Up::Failed(fault) => {
                            self.phase = Phase::Closing;
                            Evidence::Failed(fault)
                        }
                        Up::Bytes(_) | Up::End => panic!("an append stream has no input"),
                    }
                }
                Event::Closed { owner } => {
                    assert_eq!(owner, self.stream);
                    self.phase = Phase::Closed;
                    Evidence::Closed
                }
                Event::Listening { .. }
                | Event::Accepted { .. }
                | Event::Connecting { .. }
                | Event::Connected { .. }
                | Event::Output { .. }
                | Event::Failed { .. }
                | Event::Spawned { .. }
                | Event::Exited { .. }
                | Event::Usage { .. }
                | Event::Shutdown { .. } => panic!("append stream event"),
            };
            if self.events.iter().any(|event| matches!(event, Evidence::Failed(_))) {
                assert_eq!(evidence, Evidence::Closed, "nothing after a failure but Closed");
            }
            assert!(!self.events.contains(&Evidence::Closed), "nothing follows Closed");
            self.events.push(evidence);
        }
    }

    fn ready(&mut self) {
        while self.io.is_ready() {
            skein_io::resume(&mut self.io, &self.env, &mut self.up, &mut self.submissions);
            self.observed();
        }
    }

    #[must_use]
    pub fn failures(&self) -> usize {
        self.events.iter().filter(|event| matches!(event, Evidence::Failed(_))).count()
    }
}

impl Host for Writer {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.env.now = now;
        self.env.wall = wall;
        self.ready();
        while let Some(complete) = self.completions.pop() {
            skein_io::up(&mut self.io, &self.env, complete, &mut self.up, &mut self.submissions);
            self.observed();
            self.ready();
        }
        while self.io.is_due(now) {
            skein_io::fire(&mut self.io, &self.env, &mut self.up, &mut self.submissions);
            self.observed();
        }
        let request = match self.phase {
            Phase::Asking if self.sent < PARTS.len() => {
                self.phase = Phase::Waiting;
                Some(Request::Stream {
                    stream: self.stream,
                    down: Down::Demand {
                        read: Read::Nothing,
                        room: u32::try_from(PARTS[self.sent].len()).expect("small part"),
                    },
                })
            }
            Phase::Asking => {
                self.phase = Phase::Closing;
                match self.story {
                    Story::Finish => Some(Request::Stream { stream: self.stream, down: Down::Finish }),
                    Story::Close => Some(Request::Close { entity: self.stream }),
                    Story::Abort => Some(Request::Abort { entity: self.stream }),
                    Story::WriteDeadline => {
                        self.phase = Phase::Idle;
                        None
                    }
                }
            }
            Phase::Granted => {
                let bytes = Box::from(PARTS[self.sent]);
                self.sent += 1;
                self.phase = Phase::Asking;
                Some(Request::Stream { stream: self.stream, down: Down::Send(bytes) })
            }
            Phase::Waiting | Phase::Idle | Phase::Closing | Phase::Closed => None,
        };
        if let Some(request) = request {
            skein_io::down(&mut self.io, &self.env, request, &mut self.submissions);
        }
    }

    fn drain(&mut self) {
        self.io.reclaim();
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.io.is_ready()
            || self.io.is_due(now)
            || !self.completions.is_empty()
            || matches!(self.phase, Phase::Asking | Phase::Granted)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }

    fn is_empty(&self) -> bool {
        self.phase == Phase::Closed && self.io.is_empty() && self.completions.is_empty() && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        skein_io::worst_case(&self.env.limits)
            .expect("small io bound")
            .checked_add(Queue::<Complete>::worst_case(8).expect("completion cells"))
            .and_then(|bound| bound.checked_add(Queue::<Submit>::worst_case(8).expect("submission cells")))
            .and_then(|bound| bound.checked_add(Queue::<Event>::worst_case(4).expect("event cells")))
            .and_then(|bound| {
                bound.checked_add(u64::try_from(8 * size_of::<Evidence>()).expect("bounded observations"))
            })
            .expect("small owner bound")
    }
    fn operations(&self) -> u32 {
        skein_io::operations(&self.env.limits).expect("small ring")
    }
}

/// The filesystem outside the owner's memory span.
#[derive(Debug)]
pub struct AppendFiles {
    pub files: Files,
    root: Opened,
}

impl AppendFiles {
    #[must_use]
    pub fn new() -> Self {
        let mut files = Files::new();
        let root = files.lay(&[Item::file(b"trace", b"prefix:")]);
        Self { files, root }
    }

    #[must_use]
    pub fn append(&mut self) -> Handle {
        Handle::new(self.files.open(self.root, b"trace", How::Append { mode: 0o600 }).expect("startup trace").raw())
    }

    #[must_use]
    pub fn bytes(&mut self) -> Vec<u8> {
        let file = self.files.open(self.root, b"trace", How::Read).expect("read trace");
        let bytes = self.files.read(file, 0, 64).expect("trace bytes");
        self.files.close(file);
        bytes
    }

    pub fn finish(&mut self) {
        self.files.close(self.root);
        assert_eq!(self.files.open_handles(), 0, "the world closed the append file");
    }
}

impl Default for AppendFiles {
    fn default() -> Self {
        Self::new()
    }
}

impl Machine for AppendFiles {
    fn step(&mut self, call: Call, answers: &mut Queue<Answer>) {
        skein_fake_machine::step(&mut self.files, call, answers);
    }
}

/// This referee reads the scripted owner's actual stream observations.
#[derive(Debug)]
pub struct Judge;

impl Referee<Writer> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Writer]) {}
    fn observe(&mut self, _now: Time, procs: &[Writer]) {
        assert!(procs[0].failures() <= 1);
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

#[must_use]
pub fn simulated(seed: u64, config: Config, story: Story, memory: Memory) -> skein_world::Outcome<Writer, AppendFiles> {
    let mut machine = AppendFiles::new();
    let file = machine.append();
    let mut world = World::new(seed, config, Judge, memory).with_machine(machine);
    world.spawn_append(file, |fd| Writer::new(fd, story, Duration::from_millis(10)));
    world.run()
}
