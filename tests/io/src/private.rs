//! Private-root owners over the shared world (io.md, section 5.3).
//! The scenario observes metadata, reads and terminals; it never interprets secrets.

#![expect(clippy::wildcard_enum_match_arm, reason = "unexpected scenario terminals retain their debug value")]

use skein_fake_machine::{How, Item, Machine as Files, Opened};
use skein_io::file::{Event, Expect, Request, Unsafe};
use skein_io::file_layer::{self, FileIo};
use skein_io::kernel::{Complete, Fd, Kind, Submit};
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_sim::{Answer, Ask, Call, Config, Handle};
use skein_world::{Host, Machine, Memory, Referee, World};

pub const USER: u32 = 1234;
const MAX: u32 = 16;
const OPEN: Token = Token::new(1);
const STORE: Token = Token::new(2);
const LOAD: Token = Token::new(3);
const CLOSE_PRIVATE: Token = Token::new(4);
const CLOSE_ROOT: Token = Token::new(5);
const BYTES: &[u8] = b"opaque";

/// The planted metadata or whole replacement this owner exercises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Story {
    Create,
    Replace,
    RootMode,
    RootOwner,
    RootLink,
    RootKind,
    FileMode,
    FileOwner,
    FileLink,
    FileKind,
    FileLinks,
    RootDeniedSafe,
    FileDeniedSafe,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Open,
    Store,
    Load,
    ClosePrivate,
    CloseRoot,
    Waiting,
    Settled,
}

/// A file owner, configured with the startup effective user, with one request at a time.
#[derive(Debug)]
pub struct Owner {
    io: FileIo,
    root: Token,
    private: Option<Token>,
    story: Story,
    phase: Phase,
    pub events: Vec<Event>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    up: Queue<Event>,
}

impl Owner {
    #[must_use]
    pub fn new(root: Fd, story: Story, effective_user: u32) -> Self {
        let mut io = FileIo::with_whole_limit(3, 2, 4, MAX, Duration::from_secs(1)).with_effective_user(effective_user);
        io.seed_randomness(321);
        let root = io.adopt_root(root).expect("startup root");
        Self {
            io,
            root,
            private: None,
            story,
            phase: Phase::Open,
            events: Vec::with_capacity(5),
            completions: Queue::with_capacity(4),
            submissions: Queue::with_capacity(4),
            up: Queue::with_capacity(1),
        }
    }
    fn observe(&mut self) {
        while let Some(event) = self.up.pop() {
            assert!(self.events.len() < 5 && matches!(self.phase, Phase::Waiting));
            self.phase = match event.owner() {
                OPEN => match &event {
                    Event::Opened { file, .. } => {
                        self.private = Some(*file);
                        if matches!(self.story, Story::Create | Story::Replace) { Phase::Store } else { Phase::Load }
                    }
                    Event::Refused { .. } | Event::Failed { .. } => Phase::CloseRoot,
                    other => panic!("private opening: {other:?}"),
                },
                STORE => {
                    assert!(matches!(event, Event::Stored { .. }));
                    Phase::Load
                }
                LOAD => {
                    assert!(matches!(event, Event::Loaded { .. } | Event::Refused { .. } | Event::Failed { .. }));
                    Phase::ClosePrivate
                }
                CLOSE_PRIVATE => {
                    assert!(matches!(event, Event::Closed { .. }));
                    Phase::CloseRoot
                }
                CLOSE_ROOT => {
                    assert!(matches!(event, Event::Closed { .. }));
                    Phase::Settled
                }
                other => panic!("unexpected owner: {other:?}"),
            };
            self.events.push(event);
        }
    }
    pub fn check(&self, effective_user: u32) {
        if matches!(self.story, Story::RootDeniedSafe | Story::FileDeniedSafe) {
            assert_eq!(
                self.events
                    .iter()
                    .filter(|event| matches!(event, Event::Failed { error: skein_io::kernel::Error::Permission, .. }))
                    .count(),
                1,
                "story {:?}: {:?}",
                self.story,
                self.events
            );
            assert!(!self.events.iter().any(|event| matches!(event, Event::Loaded { .. })));
            assert!(self.is_empty());
            return;
        }
        let expected = match self.story {
            Story::Create | Story::Replace => None,
            Story::RootDeniedSafe | Story::FileDeniedSafe => unreachable!("denied entries checked above"),
            Story::RootMode => Some(Unsafe::Mode(0o755)),
            Story::RootOwner | Story::FileOwner => {
                Some(Unsafe::Owner { found: effective_user + 1, expected: effective_user })
            }
            Story::RootLink | Story::FileLink => Some(Unsafe::Link),
            Story::RootKind => Some(Unsafe::Kind(Kind::File)),
            Story::FileMode => Some(Unsafe::Mode(0o644)),
            Story::FileKind => Some(Unsafe::Kind(Kind::Directory)),
            Story::FileLinks => Some(Unsafe::Links(2)),
        };
        if let Some(found) = expected {
            assert_eq!(
                self.events
                    .iter()
                    .filter(|event| matches!(event, Event::Refused { found: actual, .. } if *actual == found))
                    .count(),
                1,
                "story {:?}: {:?}",
                self.story,
                self.events
            );
            assert!(!self.events.iter().any(|event| matches!(event, Event::Loaded { .. })), "refused unread");
        } else {
            assert!(
                self.events.iter().any(|event| matches!(event, Event::Loaded { bytes, .. } if bytes.as_ref() == BYTES))
            );
        }
        assert!(self.is_empty());
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
            Phase::Open => Some(Request::OpenPrivate { owner: OPEN, root: self.root, path: Box::from(&b"secret"[..]) }),
            Phase::Store => Some(Request::Store {
                owner: STORE,
                root: self.private.expect("the bounded private-file scenario supplies this value"),
                path: Box::from(&b"record"[..]),
                bytes: Box::from(BYTES),
                expected: Expect::Any,
                no_follow: false,
            }),
            Phase::Load => Some(Request::Load {
                owner: LOAD,
                root: self.private.expect("the bounded private-file scenario supplies this value"),
                path: Box::from(&b"record"[..]),
                max: MAX,
                no_follow: false,
            }),
            Phase::ClosePrivate => Some(Request::Close {
                owner: CLOSE_PRIVATE,
                file: self.private.take().expect("the bounded private-file scenario supplies this value"),
            }),
            Phase::CloseRoot => Some(Request::Close { owner: CLOSE_ROOT, file: self.root }),
            Phase::Waiting | Phase::Settled => None,
        };
        if let Some(request) = request {
            self.phase = Phase::Waiting;
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
        !self.completions.is_empty() || (self.io.takes() && !matches!(self.phase, Phase::Waiting | Phase::Settled))
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn is_empty(&self) -> bool {
        matches!(self.phase, Phase::Settled)
            && self.io.takes()
            && self.io.open_files() == 0
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        FileIo::worst_case(3, 2, 4, MAX).expect("the bounded private-file scenario supplies this value")
            + Queue::<Complete>::worst_case(4).expect("the bounded private-file scenario supplies this value")
            + Queue::<Submit>::worst_case(4).expect("the bounded private-file scenario supplies this value")
            + Queue::<Event>::worst_case(1).expect("the bounded private-file scenario supplies this value")
            + u64::try_from(5 * size_of::<Event>()).expect("the bounded private-file scenario supplies this value")
            + u64::from(MAX)
    }
    fn operations(&self) -> u32 {
        4
    }
}

/// A filesystem with planted node owners and an independent count of content reads.
#[derive(Debug)]
pub struct FileSystem {
    pub files: Files,
    root: Opened,
    pub reads: usize,
}

impl FileSystem {
    #[must_use]
    pub fn new(story: Story, effective_user: u32) -> Self {
        let mut files = Files::for_user(effective_user);
        let items = match story {
            Story::Create => vec![],
            Story::RootDeniedSafe => vec![Item::directory(b"secret").mode(0)],
            Story::FileDeniedSafe => {
                vec![Item::directory(b"secret").mode(0o700), Item::file(b"secret/record", BYTES).mode(0)]
            }
            Story::RootMode => vec![Item::directory(b"secret")],
            Story::RootOwner => vec![Item::directory(b"secret").mode(0o700).owner(effective_user + 1)],
            Story::RootLink => vec![Item::directory(b"target").mode(0o700), Item::link(b"secret", b"target")],
            Story::RootKind => vec![Item::file(b"secret", BYTES).mode(0o600)],
            Story::Replace | Story::FileMode => {
                vec![Item::directory(b"secret").mode(0o700), Item::file(b"secret/record", BYTES)]
            }
            Story::FileOwner => vec![
                Item::directory(b"secret").mode(0o700),
                Item::file(b"secret/record", BYTES).mode(0o600).owner(effective_user + 1),
            ],
            Story::FileLink => vec![
                Item::directory(b"secret").mode(0o700),
                Item::file(b"secret/target", BYTES).mode(0o600),
                Item::link(b"secret/record", b"target"),
            ],
            Story::FileKind => {
                vec![Item::directory(b"secret").mode(0o700), Item::directory(b"secret/record").mode(0o700)]
            }
            Story::FileLinks => vec![
                Item::directory(b"secret").mode(0o700),
                Item::file(b"secret/record", BYTES).mode(0o600),
                Item::hard_link(b"secret/other", b"secret/record"),
            ],
        };
        let root = files.lay(&items);
        Self { files, root, reads: 0 }
    }
    #[must_use]
    pub fn startup(&mut self) -> Handle {
        Handle::new(
            self.files
                .open(self.root, b".", How::Directory)
                .expect("the bounded private-file scenario supplies this value")
                .raw(),
        )
    }
    #[must_use]
    pub fn mode(&mut self, path: &[u8]) -> u32 {
        let file =
            self.files.open(self.root, path, How::Read).expect("the bounded private-file scenario supplies this value");
        let mode = self.files.stat(file).mode;
        self.files.close(file);
        mode
    }
    pub fn finish(&mut self) {
        self.files.close(self.root);
        assert_eq!(self.files.open_handles(), 0);
    }
}

impl Machine for FileSystem {
    fn step(&mut self, call: Call, answers: &mut Queue<Answer>) {
        self.reads += usize::from(matches!(call.ask, Ask::Read { .. }));
        skein_fake_machine::step(&mut self.files, call, answers);
    }
}

/// A referee observing only owner terminal records.
#[derive(Debug)]
pub struct Judge;
impl Referee<Owner> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Owner]) {}
    fn observe(&mut self, _now: Time, _procs: &[Owner]) {}
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
pub fn world(seed: u64, config: Config, story: Story) -> World<Owner, Judge, FileSystem> {
    let mut files = FileSystem::new(story, USER);
    let root = files.startup();
    let mut world = World::new(seed, config, Judge, Memory::Checked).with_machine(files);
    world.spawn_root(root, |fd| Owner::new(fd, story, USER));
    world
}
