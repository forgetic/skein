//! Metadata and cleanup checks before private bytes are read (io.md, 5.3).

#![expect(
    clippy::wildcard_enum_match_arm,
    clippy::match_like_matches_macro,
    reason = "the test recognizes the expected kernel record cases"
)]

use crate::file::{Event, Expect, Request, Unsafe};
use crate::file_layer::{self, FileIo};
use crate::kernel::{Complete, Done, Error, Fd, Kind, Op, OpenHow, Stat, Submit};
use alloc::boxed::Box;
use alloc::vec::Vec;
use skein_lib::{Duration, Queue, Time, Token};

const OWNER: Token = Token::new(7);
const PARENT: Fd = Fd::new(42);
const PRIVATE: Fd = Fd::new(43);
const FILE: Fd = Fd::new(44);
const USER: u32 = 17;

struct Rig {
    io: FileIo,
    root: Token,
    events: Queue<Event>,
    subs: Queue<Submit>,
}

impl Rig {
    fn new() -> Self {
        let mut io = FileIo::new(3, 16, 4, Duration::from_secs(1)).with_effective_user(USER);
        io.seed_randomness(23);
        let root = io.adopt_root(Fd::new(41)).unwrap();
        Self { io, root, events: Queue::with_capacity(1), subs: Queue::with_capacity(2) }
    }
    fn down(&mut self, request: Request) {
        file_layer::down(&mut self.io, Time::ZERO, request, &mut self.events, &mut self.subs);
    }
    fn done(&mut self, result: Result<Done, Error>) {
        let Submit { op, kind } = self.subs.pop().expect("one operation");
        file_layer::up(&mut self.io, Complete { op, kind, result }, &mut self.events, &mut self.subs);
    }
    fn begin(&mut self) {
        self.down(Request::OpenPrivate { owner: OWNER, root: self.root, path: Box::from(&b"secret"[..]) });
        assert!(match self.subs.iter().next().unwrap().kind {
            Op::Open { how: OpenHow::DirectoryNoFollow, .. } => true,
            _ => false,
        });
        self.done(Ok(Done::Fd(PARENT)));
        let Op::Open { how, .. } = &self.subs.iter().next().unwrap().kind else { panic!("private open") };
        assert_eq!(*how, OpenHow::ReadNoFollow);
        self.done(Ok(Done::Fd(PRIVATE)));
    }
    fn opened(&mut self) -> Token {
        self.begin();
        self.done(Ok(Done::Stat(root_stat())));
        assert!(self.events.is_empty(), "parent closes before Opened");
        self.done(Ok(Done::Nothing));
        let Event::Opened { file, .. } = self.events.pop().unwrap() else { panic!("a verified root") };
        file
    }
    fn close_refusal(&mut self, expected: Unsafe, closes: usize) {
        for _ in 0..closes {
            assert!(self.events.is_empty(), "close descriptors before terminal");
            assert!(match self.subs.iter().next().unwrap().kind {
                Op::Close { .. } => true,
                _ => false,
            });
            self.done(Ok(Done::Nothing));
        }
        assert_eq!(self.events.pop(), Some(Event::Refused { owner: OWNER, found: expected }));
        assert!(self.subs.is_empty() && self.io.takes());
    }
}

fn root_stat() -> Stat {
    Stat { kind: Kind::Directory, size: 0, mode: 0o700, owner: USER, links: 2 }
}
fn file_stat() -> Stat {
    Stat { kind: Kind::File, size: 3, mode: 0o600, owner: USER, links: 1 }
}

#[test]
fn private_roots_refuse_each_group_or_other_bit_kind_and_another_user() {
    let mut cases = Vec::new();
    for bit in [0o040, 0o020, 0o010, 0o004, 0o002, 0o001] {
        cases.push((Stat { mode: 0o700 | bit, ..root_stat() }, Unsafe::Mode(0o700 | bit)));
    }
    cases.push((Stat { kind: Kind::File, ..root_stat() }, Unsafe::Kind(Kind::File)));
    cases.push((Stat { owner: USER + 1, ..root_stat() }, Unsafe::Owner { found: USER + 1, expected: USER }));
    for (stat, expected) in cases {
        let mut rig = Rig::new();
        rig.begin();
        rig.done(Ok(Done::Stat(stat)));
        rig.close_refusal(expected, 2);
        assert_eq!(rig.io.open_files(), 1, "only the original root remains");
    }
}

#[test]
fn private_loads_refuse_each_unsafe_metadata_before_submitting_any_read() {
    let mut cases = Vec::new();
    for bit in [0o040, 0o020, 0o010, 0o004, 0o002, 0o001] {
        cases.push((Stat { mode: 0o600 | bit, ..file_stat() }, Unsafe::Mode(0o600 | bit)));
    }
    cases.push((Stat { kind: Kind::Directory, ..file_stat() }, Unsafe::Kind(Kind::Directory)));
    cases.push((Stat { links: 2, ..file_stat() }, Unsafe::Links(2)));
    cases.push((Stat { owner: USER + 1, ..file_stat() }, Unsafe::Owner { found: USER + 1, expected: USER }));
    for (stat, expected) in cases {
        let mut rig = Rig::new();
        let root = rig.opened();
        rig.down(Request::Load { owner: OWNER, root, path: Box::from(&b"record"[..]), max: 16, no_follow: false });
        assert!(match rig.subs.iter().next().unwrap().kind {
            Op::Open { how: OpenHow::ReadNoFollow, .. } => true,
            _ => false,
        });
        rig.done(Ok(Done::Fd(FILE)));
        rig.done(Ok(Done::Stat(stat)));
        rig.close_refusal(expected, 1);
    }
}

#[test]
fn private_root_and_load_links_are_typed_refusals() {
    let mut rig = Rig::new();
    rig.down(Request::OpenPrivate { owner: OWNER, root: rig.root, path: Box::from(&b"secret"[..]) });
    rig.done(Ok(Done::Fd(PARENT)));
    rig.done(Err(Error::TooManyLinks));
    rig.close_refusal(Unsafe::Link, 1);
    let root = rig.opened();
    rig.down(Request::Load { owner: OWNER, root, path: Box::from(&b"record"[..]), max: 16, no_follow: false });
    rig.done(Err(Error::TooManyLinks));
    rig.close_refusal(Unsafe::Link, 0);
}

#[test]
fn making_a_directory_preserves_mode_and_exists_is_a_failure() {
    for result in [Ok(Done::Nothing), Err(Error::Exists)] {
        let mut rig = Rig::new();
        rig.down(Request::MakeDirectory { owner: OWNER, root: rig.root, path: Box::from(&b"new"[..]), mode: 0o711 });
        rig.done(Ok(Done::Fd(PARENT)));
        assert!(match rig.subs.iter().next().unwrap().kind {
            Op::MakeDirectory { mode: 0o711, .. } => true,
            _ => false,
        });
        rig.done(result.clone());
        rig.done(Ok(Done::Nothing));
        assert_eq!(
            rig.events.pop(),
            Some(match result {
                Ok(_) => Event::Made { owner: OWNER },
                Err(error) => Event::Failed { owner: OWNER, error, committed: false, residue: None },
            })
        );
    }
}

#[test]
fn cancelling_an_open_private_root_closes_a_descriptor_that_won_the_race() {
    let mut rig = Rig::new();
    rig.down(Request::OpenPrivate { owner: OWNER, root: rig.root, path: Box::from(&b"secret"[..]) });
    let target = rig.subs.pop().unwrap();
    file_layer::cancel(&mut rig.io, OWNER, &mut rig.subs);
    let cancel = rig.subs.pop().unwrap();
    file_layer::up(
        &mut rig.io,
        Complete { op: target.op, kind: target.kind, result: Ok(Done::Fd(PARENT)) },
        &mut rig.events,
        &mut rig.subs,
    );
    rig.done(Ok(Done::Nothing));
    assert_eq!(rig.events.pop(), Some(Event::Cancelled { owner: OWNER }));
    assert!(!rig.io.takes(), "cancel completion is still owned");
    file_layer::up(
        &mut rig.io,
        Complete { op: cancel.op, kind: cancel.kind, result: Err(Error::TooLate) },
        &mut rig.events,
        &mut rig.subs,
    );
    assert!(rig.io.takes() && rig.events.is_empty());
}

#[test]
fn private_stores_never_copy_the_old_files_group_bits() {
    let mut rig = Rig::new();
    let root = rig.opened();
    rig.down(Request::Store {
        owner: OWNER,
        root,
        path: Box::from(&b"record"[..]),
        bytes: Box::from(&b"new"[..]),
        expected: Expect::Any,
        no_follow: false,
    });
    let Op::Open { how, .. } = &rig.subs.iter().next().unwrap().kind else { panic!("parent open") };
    assert_eq!(*how, OpenHow::DirectoryNoFollow);
    rig.done(Ok(Done::Fd(PARENT)));
    rig.done(Ok(Done::Fd(FILE)));
    rig.done(Ok(Done::Stat(Stat { mode: 0o644, ..file_stat() })));
    rig.done(Ok(Done::Nothing));
    let Op::Open { how, .. } = &rig.subs.iter().next().unwrap().kind else { panic!("temporary open") };
    assert_eq!(*how, OpenHow::CreateNoFollow { mode: Some(0o600) });
}

#[test]
fn every_failed_private_open_stage_closes_its_owned_descriptors_before_one_terminal() {
    for fail_at in 0..4 {
        let mut rig = Rig::new();
        rig.down(Request::OpenPrivate { owner: OWNER, root: rig.root, path: Box::from(&b"secret"[..]) });
        let results = [Ok(Done::Fd(PARENT)), Ok(Done::Fd(PRIVATE)), Ok(Done::Stat(root_stat())), Ok(Done::Nothing)];
        for (at, result) in results.into_iter().enumerate() {
            if at == fail_at {
                rig.done(Err(Error::Other(5)));
                break;
            }
            rig.done(result);
        }
        while !rig.subs.is_empty() {
            assert!(rig.events.is_empty(), "cleanup precedes terminal");
            let Op::Close { .. } = &rig.subs.iter().next().unwrap().kind else { panic!("only cleanup closes") };
            rig.done(Ok(Done::Nothing));
        }
        assert_eq!(
            rig.events.pop(),
            Some(Event::Failed { owner: OWNER, error: Error::Other(5), committed: false, residue: None })
        );
        assert!(rig.events.is_empty() && rig.io.takes());
        assert_eq!(rig.io.open_files(), 1);
    }
}

#[test]
fn an_absent_private_root_is_made_once_with_owner_only_mode() {
    let mut rig = Rig::new();
    rig.down(Request::OpenPrivate { owner: OWNER, root: rig.root, path: Box::from(&b"secret"[..]) });
    rig.done(Ok(Done::Fd(PARENT)));
    rig.done(Err(Error::NotFound));
    let Op::MakeDirectory { mode, .. } = &rig.subs.iter().next().unwrap().kind else { panic!("make absent root") };
    assert_eq!(*mode, 0o700);
    rig.done(Ok(Done::Nothing));
    rig.done(Ok(Done::Fd(PRIVATE)));
    rig.done(Ok(Done::Stat(root_stat())));
    rig.done(Ok(Done::Nothing));
    assert!(rig.events.pop().is_some());
}

#[test]
fn denied_private_opens_use_path_metadata_only_to_name_a_refusal() {
    for root_request in [true, false] {
        for probe in [0_u8, 1, 2, 3] {
            let mut rig = Rig::new();
            if root_request {
                rig.down(Request::OpenPrivate { owner: OWNER, root: rig.root, path: Box::from(&b"secret"[..]) });
                rig.done(Ok(Done::Fd(PARENT)));
            } else {
                let root = rig.opened();
                rig.down(Request::Load {
                    owner: OWNER,
                    root,
                    path: Box::from(&b"record"[..]),
                    max: 16,
                    no_follow: false,
                });
            }
            let Op::Open { root: original_root, path: original_path, .. } = &rig.subs.iter().next().unwrap().kind
            else {
                panic!("the initial readable open")
            };
            let original_root = *original_root;
            let original_path = original_path.clone();
            rig.done(Err(Error::Permission));
            let Op::Open { root, path, how } = &rig.subs.iter().next().unwrap().kind else { panic!("path-only retry") };
            assert_eq!((*root, path.as_ref(), *how), (original_root, original_path.as_ref(), OpenHow::PathNoFollow));
            if probe == 2 {
                rig.done(Err(Error::NotFound));
            } else {
                rig.done(Ok(Done::Fd(FILE)));
                let base = if root_request { root_stat() } else { file_stat() };
                let stat = if probe == 0 { Stat { owner: USER + 1, ..base } } else { Stat { mode: 0, ..base } };
                if probe == 3 {
                    rig.done(Err(Error::Other(5)));
                } else {
                    rig.done(Ok(Done::Stat(stat)));
                }
            }
            while !rig.subs.is_empty() {
                assert!(rig.events.is_empty(), "path-only descriptor closes before the terminal");
                let Op::Close { .. } = rig.subs.iter().next().unwrap().kind else {
                    panic!("a path-only fd is never read or admitted")
                };
                rig.done(Ok(Done::Nothing));
            }
            let expected = if probe == 0 {
                Event::Refused { owner: OWNER, found: Unsafe::Owner { found: USER + 1, expected: USER } }
            } else {
                Event::Failed { owner: OWNER, error: Error::Permission, committed: false, residue: None }
            };
            assert_eq!(rig.events.pop(), Some(expected));
            assert_eq!(rig.io.open_files(), if root_request { 1 } else { 2 });
            assert!(rig.events.is_empty() && rig.io.takes());
        }
    }
}
