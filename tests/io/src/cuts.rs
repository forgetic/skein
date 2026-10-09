//! Crash-cut file owners share the world's scheduler and memory accounting
//! (io.md, 5.2 and 5.3; simulator.md, 3.3). Recovery only reads the record.

#![expect(clippy::wildcard_enum_match_arm, reason = "unexpected scenario terminals retain their debug value")]

use skein_fake_machine::{How, Item, Machine as Files, Opened};
use skein_io::file::{Event, Expect, Request};
use skein_io::file_layer::{self, FileIo};
use skein_io::kernel::{Complete, Error, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_sim::{Answer, Call, Config, Handle};
use skein_world::{Cut, Host, Inherited, Machine, Memory, Referee, StartupRoot, World};

pub const OLD: &[u8] = b"old contents whole";
pub const NEW: &[u8] = b"replacement whole";
const MAX: u32 = 64;
const USER: u32 = 1000;
const OPEN: Token = Token::new(1);
const STORE: Token = Token::new(2);
const LOAD: Token = Token::new(3);
const CLOSE_VAULT: Token = Token::new(4);
const CLOSE_ROOT: Token = Token::new(5);
const CLOSE_SIGNAL: Token = Token::new(u64::MAX);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Open,
    Store,
    Load,
    CloseVault,
    CloseRoot,
    CloseSignal,
    Waiting,
    SignalPending,
    Settled,
}

/// The file owner stores once before a cut; its fresh incarnation only recovers.
#[derive(Debug)]
pub struct Owner {
    io: FileIo,
    root_fd: Fd,
    root: Token,
    signal: Fd,
    vault: Option<Token>,
    private: bool,
    start_store: bool,
    phase: Phase,
    pub recovered: Option<Box<[u8]>>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    up: Queue<Event>,
}

fn make(inherited: &Inherited) -> Owner {
    assert_eq!(inherited.roots.len(), 1);
    let (name, root_fd) = &inherited.roots[0];
    let mut io = FileIo::with_whole_limit(4, 4, 4, MAX, Duration::from_secs(1)).with_effective_user(USER);
    io.seed_randomness(123);
    let root = io.adopt_root(*root_fd).expect("startup root");
    Owner {
        io,
        root_fd: *root_fd,
        root,
        signal: inherited.signal,
        vault: None,
        private: name.as_ref() == b"private",
        start_store: false,
        phase: Phase::Open,
        recovered: None,
        completions: Queue::with_capacity(4),
        submissions: Queue::with_capacity(4),
        up: Queue::with_capacity(2),
    }
}

impl Owner {
    fn observe(&mut self) {
        while let Some(event) = self.up.pop() {
            assert_eq!(self.phase, Phase::Waiting);
            self.phase = match event.owner() {
                OPEN => match event {
                    Event::Opened { file, .. } => {
                        self.vault = Some(file);
                        if self.start_store { Phase::Store } else { Phase::Load }
                    }
                    other => panic!("opening recovery root: {other:?}"),
                },
                STORE => {
                    assert!(matches!(event, Event::Stored { .. }), "store terminal {event:?}");
                    Phase::Load
                }
                LOAD => match event {
                    Event::Loaded { bytes, .. } => {
                        assert!(bytes.as_ref() == OLD || bytes.as_ref() == NEW, "whole recovered bytes: {bytes:?}");
                        self.recovered = Some(bytes);
                        Phase::CloseVault
                    }
                    other => panic!("loading recovery record: {other:?}"),
                },
                CLOSE_VAULT => {
                    assert!(matches!(event, Event::Closed { .. }));
                    Phase::CloseRoot
                }
                CLOSE_ROOT => {
                    assert!(matches!(event, Event::Closed { .. }));
                    Phase::CloseSignal
                }
                other => panic!("unexpected owner: {other:?}"),
            };
        }
    }
}

impl Host for Owner {
    fn iterate(&mut self, now: Time, _wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            if complete.op == CLOSE_SIGNAL {
                assert_eq!(self.phase, Phase::SignalPending);
                assert!(complete.result.is_ok());
                self.phase = Phase::Settled;
            } else {
                file_layer::up(&mut self.io, complete, &mut self.up, &mut self.submissions);
                self.observe();
            }
        }
        if self.io.is_due(now) {
            file_layer::expire(&mut self.io, now, &mut self.submissions);
        }
        if !self.io.takes() {
            return;
        }
        let request = match self.phase {
            Phase::Open => Some(if self.private {
                Request::OpenPrivate { owner: OPEN, root: self.root, path: Box::from(&b"vault"[..]) }
            } else {
                Request::OpenDirectory {
                    owner: OPEN,
                    root: self.root_fd,
                    name: Box::from(&b"vault"[..]),
                    no_follow: true,
                }
            }),
            Phase::Store => Some(Request::Store {
                owner: STORE,
                root: self.vault.expect("opened vault"),
                path: Box::from(&b"record"[..]),
                bytes: Box::from(NEW),
                expected: Expect::Any,
                no_follow: false,
            }),
            Phase::Load => Some(Request::Load {
                owner: LOAD,
                root: self.vault.expect("opened vault"),
                path: Box::from(&b"record"[..]),
                max: MAX,
                no_follow: false,
            }),
            Phase::CloseVault => {
                Some(Request::Close { owner: CLOSE_VAULT, file: self.vault.take().expect("opened vault") })
            }
            Phase::CloseRoot => Some(Request::Close { owner: CLOSE_ROOT, file: self.root }),
            Phase::CloseSignal => {
                self.submissions.push(Submit { op: CLOSE_SIGNAL, kind: Op::Close { fd: self.signal } });
                self.phase = Phase::SignalPending;
                None
            }
            Phase::Waiting | Phase::SignalPending | Phase::Settled => None,
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
        !self.completions.is_empty()
            || (self.io.takes() && !matches!(self.phase, Phase::Waiting | Phase::SignalPending | Phase::Settled))
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.phase == Phase::Settled
            && self.io.open_files() == 0
            && self.io.takes()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.up.is_empty()
    }
    fn worst_case(&self) -> u64 {
        FileIo::worst_case(4, 4, 4, MAX).expect("bounded file owner")
            + Queue::<Complete>::worst_case(4).expect("bounded queue")
            + Queue::<Submit>::worst_case(4).expect("bounded queue")
            + Queue::<Event>::worst_case(2).expect("bounded queue")
            + u64::from(MAX)
            + 255
    }
    fn operations(&self) -> u32 {
        4
    }
}

/// The machine owns durable state and records which startup roots were reopened.
#[derive(Debug)]
pub struct FileSystem {
    pub files: Files,
    pub opened: [u32; 2],
    pub cuts: u32,
}

impl FileSystem {
    /// Observes metadata after recovery without retaining an observer handle.
    pub fn check_private(&mut self) {
        let root = self.files.reopen_root(0);
        let vault = self.files.open(root, b"vault", How::Directory).expect("safe private vault");
        let facts = self.files.stat(vault);
        assert_eq!(facts.owner, USER);
        assert_eq!(facts.mode, 0o700);
        for (_, name) in self.files.list(vault, 16, 4096).expect("bounded private entries") {
            let file = self.files.open(vault, &name, How::Read).expect("private record or residue");
            let facts = self.files.stat(file);
            assert_eq!(facts.owner, USER);
            assert_eq!(facts.mode, 0o600);
            self.files.close(file);
        }
        self.files.close(vault);
        self.files.close(root);
        assert_eq!(self.files.open_handles(), 0);
    }

    fn new(private: bool, peer: bool) -> Self {
        let mut files = Files::for_user(USER);
        for _ in 0..if peer { 2 } else { 1 } {
            let root = files.lay(&[
                Item::directory(b"vault").mode(0o700),
                Item::file(b"vault/record", OLD).mode(if private { 0o600 } else { 0o644 }),
            ]);
            files.close(root);
        }
        Self { files, opened: [0; 2], cuts: 0 }
    }
}

impl Machine for FileSystem {
    fn cut(&mut self, cut: Cut, held: &[Handle], seed: u64) {
        for handle in held {
            self.files.close(Opened::new(handle.raw()));
        }
        match cut {
            Cut::Kill => {}
            Cut::PowerLoss => self.files.crash(seed),
        }
        self.cuts += 1;
    }
    fn open_root(&mut self, path: &[u8]) -> Result<Handle, Error> {
        let index = match path {
            b"root-0" => 0,
            b"root-1" => 1,
            _ => return Err(Error::NotFound),
        };
        self.opened[index] += 1;
        Ok(Handle::new(self.files.reopen_root(u32::try_from(index).expect("two roots")).raw()))
    }
    fn close_root(&mut self, root: Handle) {
        self.files.close(Opened::new(root.raw()));
    }
    fn step(&mut self, call: Call, answers: &mut Queue<Answer>) {
        skein_fake_machine::step(&mut self.files, call, answers);
    }
}

/// The referee asks original incarnations to store once and only observes recovered bytes.
#[derive(Debug)]
pub struct Judge {
    started: bool,
}
impl Referee<Owner> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Owner]) {
        if !self.started {
            for proc in procs {
                proc.start_store = true;
            }
            self.started = true;
        }
    }
    fn observe(&mut self, _now: Time, procs: &[Owner]) {
        for proc in procs {
            if let Some(bytes) = &proc.recovered {
                assert!(bytes.as_ref() == OLD || bytes.as_ref() == NEW);
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

#[must_use]
pub fn world(seed: u64, config: Config, private: bool, peer: bool) -> World<Owner, Judge, FileSystem> {
    let mut world = World::new(seed, config, Judge { started: false }, Memory::Checked)
        .with_machine(FileSystem::new(private, peer));
    for path in if peer { vec![b"root-0".as_slice(), b"root-1".as_slice()] } else { vec![b"root-0".as_slice()] } {
        world.spawn_restartable(
            vec![StartupRoot {
                name: Box::from(if private { b"private".as_slice() } else { b"ordinary".as_slice() }),
                path: Box::from(path),
            }],
            make,
        );
    }
    world
}
