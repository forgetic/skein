//! Whole-file owners over the shared world, with a filesystem observer and
//! a referee beside them (io.md, section 5.2; testing.md, section 8).

use skein_fake_machine::{How, Item, Machine as Files, Opened};
use skein_io::digest::digest;
use skein_io::file::{Event, Expect, Request};
use skein_io::file_layer::{self, FileIo};
use skein_io::kernel::{Complete, Error, Fd, Submit};
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_sim::{Answer, Ask, Call, Config, Handle};
use skein_world::{Host, Machine, Memory, Referee, World};

pub const OLD: &[u8] = b"old";
pub const NEW: &[u8] = b"new";
pub const OTHER: &[u8] = b"bad";
const OWNER: Token = Token::new(1);
const LOAD: Token = Token::new(2);
const CLOSE: Token = Token::new(3);
const READ: u32 = 2;
const FILES: u32 = 4;
const ENTRIES: u32 = 4;

/// The initial target and the version the owner expects to replace.
#[derive(Clone, Copy, Debug)]
pub enum Story {
    Replace,
    Conflict,
    Link,
    Unreadable,
    AbsentUnreadable,
    DigestUnreadable,
    AbsentDangling,
    Dangling,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Store,
    StorePending,
    Load,
    LoadPending,
    Close,
    ClosePending,
    Settled,
}

/// The script knows requests and terminals, and never reads `FileIo`'s state.
#[derive(Debug)]
pub struct Owner {
    io: FileIo,
    root: Token,
    story: Story,
    max: u32,
    phase: Phase,
    pub events: Vec<Event>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    up: Queue<Event>,
}

impl Owner {
    #[must_use]
    pub fn new(root: Fd, story: Story, max: u32) -> Self {
        let mut io = FileIo::with_whole_limit(FILES, READ, ENTRIES, max, Duration::from_millis(10));
        io.seed_randomness(123);
        let root = io.adopt_root(root).expect("startup root");
        Self {
            io,
            root,
            story,
            max,
            phase: Phase::Store,
            events: Vec::with_capacity(3),
            completions: Queue::with_capacity(4),
            submissions: Queue::with_capacity(4),
            up: Queue::with_capacity(2),
        }
    }

    fn observe(&mut self) {
        while let Some(event) = self.up.pop() {
            assert!(self.events.len() < 3, "one terminal per admitted request");
            match self.phase {
                Phase::StorePending => {
                    assert_eq!(event.owner(), OWNER);
                    assert!(matches!(
                        event,
                        Event::Stored { .. } | Event::Conflict { .. } | Event::Failed { .. } | Event::Cancelled { .. }
                    ));
                    self.phase = Phase::Load;
                }
                Phase::LoadPending => {
                    assert_eq!(event.owner(), LOAD);
                    assert!(matches!(event, Event::Loaded { .. } | Event::Failed { .. }));
                    self.phase = Phase::Close;
                }
                Phase::ClosePending => {
                    assert_eq!(event, Event::Closed { owner: CLOSE });
                    self.phase = Phase::Settled;
                }
                Phase::Store | Phase::Load | Phase::Close | Phase::Settled => {
                    panic!("one terminal only while a request is owned")
                }
            }
            self.events.push(event);
        }
    }
}

impl Host for Owner {
    fn iterate(&mut self, now: Time, _wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            file_layer::up(&mut self.io, complete, &mut self.up, &mut self.submissions);
            self.observe();
        }
        if self.io.is_due(now) {
            file_layer::expire(&mut self.io, now, &mut self.submissions);
        }
        if !self.io.takes() {
            return;
        }
        let request = match self.phase {
            Phase::Store => {
                self.phase = Phase::StorePending;
                let expected = match self.story {
                    Story::Replace | Story::Link | Story::DigestUnreadable | Story::Conflict => {
                        Expect::Digest(digest(OLD))
                    }
                    Story::Unreadable | Story::Dangling => Expect::Any,
                    Story::AbsentUnreadable | Story::AbsentDangling => Expect::Absent,
                };
                let bytes = if self.max == 3 {
                    Box::from(NEW)
                } else {
                    vec![b'n'; usize::try_from(self.max).expect("bounded file")].into_boxed_slice()
                };
                Some(Request::Store {
                    owner: OWNER,
                    root: self.root,
                    path: Box::from(&b"record"[..]),
                    bytes,
                    expected,
                    no_follow: false,
                })
            }
            Phase::Load => {
                self.phase = Phase::LoadPending;
                Some(Request::Load {
                    owner: LOAD,
                    root: self.root,
                    path: Box::from(&b"record"[..]),
                    max: self.max,
                    no_follow: false,
                })
            }
            Phase::Close => {
                self.phase = Phase::ClosePending;
                Some(Request::Close { owner: CLOSE, file: self.root })
            }
            Phase::StorePending | Phase::LoadPending | Phase::ClosePending | Phase::Settled => None,
        };
        if let Some(request) = request {
            file_layer::down(&mut self.io, now, request, &mut self.up, &mut self.submissions);
            self.observe();
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        !self.completions.is_empty()
            || (self.io.takes() && matches!(self.phase, Phase::Store | Phase::Load | Phase::Close))
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.phase == Phase::Settled
            && self.io.takes()
            && self.io.open_files() == 0
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        FileIo::worst_case(FILES, READ, ENTRIES, self.max).expect("bounded driver")
            + Queue::<Complete>::worst_case(4).expect("completion cells")
            + Queue::<Submit>::worst_case(4).expect("submission cells")
            + Queue::<Event>::worst_case(2).expect("terminal cells")
            + u64::try_from(3 * size_of::<Event>()).expect("observations")
            + u64::from(self.max)
            + 255
    }
    fn operations(&self) -> u32 {
        4
    }
}

/// The scenario's filesystem, with one selected machine call failed once.
#[derive(Debug)]
pub struct FileSystem {
    pub files: Files,
    root: Opened,
    fail_at: Option<usize>,
    calls: usize,
    pub failed: bool,
    change_at_sync: bool,
}

impl FileSystem {
    #[must_use]
    pub fn new(story: Story, fail_at: Option<usize>) -> Self {
        let mut files = Files::new();
        let items = match story {
            Story::Replace | Story::Conflict => vec![Item::file(b"record", OLD)],
            Story::Link => vec![Item::file(b"target", OLD), Item::link(b"record", b"target")],
            Story::Unreadable | Story::AbsentUnreadable | Story::DigestUnreadable => {
                vec![Item { mode: 0, ..Item::file(b"record", OLD) }]
            }
            Story::AbsentDangling | Story::Dangling => vec![Item::link(b"record", b"missing")],
        };
        let root = files.lay(&items);
        Self { files, root, fail_at, calls: 0, failed: false, change_at_sync: matches!(story, Story::Conflict) }
    }
    #[must_use]
    pub fn startup(&mut self) -> Handle {
        Handle::new(self.files.open(self.root, b".", How::Directory).expect("independently opened startup root").raw())
    }
    #[must_use]
    pub fn bytes(&mut self, name: &[u8]) -> Vec<u8> {
        let file = self.files.open(self.root, name, How::Read).expect("read observed target");
        let bytes = self.files.read(file, 0, 1_048_576).expect("target contents");
        self.files.close(file);
        bytes
    }
    #[must_use]
    pub fn names(&mut self) -> Vec<Vec<u8>> {
        let dir = self.files.open(self.root, b".", How::Directory).expect("observer directory");
        let names = self
            .files
            .list(dir, 32, 8192)
            .expect("bounded observer listing")
            .into_iter()
            .map(|(_, name)| name.into_vec())
            .collect();
        self.files.close(dir);
        names
    }
    #[must_use]
    pub fn mode(&mut self, name: &[u8]) -> u32 {
        let file = self.files.open(self.root, name, How::Read).expect("read observed metadata");
        let mode = self.files.stat(file).mode;
        self.files.close(file);
        mode
    }

    pub fn finish(&mut self) {
        self.files.close(self.root);
        assert_eq!(self.files.open_handles(), 0, "all owner and observer handles released");
    }
}

impl Machine for FileSystem {
    fn step(&mut self, call: Call, answers: &mut Queue<Answer>) {
        let at = self.calls;
        self.calls += 1;
        if self.change_at_sync && matches!(call.ask, Ask::Sync { .. }) {
            self.change_at_sync = false;
            self.files.remove(self.root, b"record", false).expect("other writer's unlink");
            let file =
                self.files.open(self.root, b"record", How::Create { mode: 0o644 }).expect("other writer's replacement");
            self.files.write(file, 0, OTHER).expect("other writer's whole file");
            self.files.close(file);
        }
        if self.fail_at == Some(at) {
            self.failed = true;
            let ticket = call.ticket;
            if matches!(call.ask, Ask::Close { .. }) {
                skein_fake_machine::step(&mut self.files, call, answers);
                let answer = answers.pop().expect("close's answer");
                assert_eq!(answer.ticket, ticket);
            }
            answers.push(Answer { ticket, result: Err(Error::Other(5)) });
        } else {
            skein_fake_machine::step(&mut self.files, call, answers);
        }
    }
}

/// The referee observes only the owner's request terminals and loaded bytes.
#[derive(Debug)]
pub struct Judge;

impl Referee<Owner> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Owner]) {}
    fn observe(&mut self, _now: Time, procs: &[Owner]) {
        for event in &procs[0].events {
            if let Event::Loaded { bytes, .. } = event {
                assert!(
                    bytes.as_ref() == OLD
                        || bytes.as_ref() == NEW
                        || (bytes.as_ref() == OTHER && matches!(procs[0].story, Story::Conflict))
                        || (bytes.len() == usize::try_from(procs[0].max).expect("bounded file")
                            && bytes.iter().all(|byte| *byte == b'n')),
                    "whole old or new file"
                );
            }
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

pub type Outcome = skein_world::Outcome<Owner, FileSystem>;

#[must_use]
pub fn world(
    seed: u64,
    config: Config,
    story: Story,
    fail_at: Option<usize>,
    max: u32,
) -> World<Owner, Judge, FileSystem> {
    world_with_files(seed, config, story, max, FileSystem::new(story, fail_at))
}

#[must_use]
pub fn world_with_files(
    seed: u64,
    config: Config,
    story: Story,
    max: u32,
    mut files: FileSystem,
) -> World<Owner, Judge, FileSystem> {
    let root = files.startup();
    let mut world = World::new(seed, config, Judge, Memory::Checked).with_machine(files);
    world.spawn_root(root, |fd| Owner::new(fd, story, max));
    world
}
