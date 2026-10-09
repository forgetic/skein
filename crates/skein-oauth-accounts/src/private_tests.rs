//! Private keeper entrance and terminal ownership (oauth.md, section 6.4).

use super::*;
use crate::{FileLower, Place, Unloaded};
use skein_io::{file, kernel};
use skein_lib::Token;

struct Owner {
    component: Component,
    env: Env<Limits>,
    above: Queue<Event>,
    io: Queue<Lower>,
    files: Queue<FileLower>,
}

impl Owner {
    fn new() -> Owner {
        let limits = limits();
        let mut configured = List::with_capacity(1);
        match configured.push(Account::SignIn {
            registration: skein_oauth::Registration {
                authorization_url: bytes::copy_of(b"https://issuer.example/authorize"),
                token_endpoint: bytes::copy_of(b"http://127.0.0.1:31000/token"),
                client_id: bytes::copy_of(b"client"),
                client_secret: Some(bytes::copy_of(b"fake secret")),
                redirect_uri: bytes::copy_of(b"https://service.example/callback"),
                scope: bytes::copy_of(b"read"),
                wire: skein_oauth::WireFormat::Form,
                pkce_for_confidential: false,
                metadata_claim: None,
            },
            endpoint: Endpoint {
                address: kernel::Addr::from(([127, 0, 0, 1], 31000)),
                transport: Transport::Plaintext,
            },
            keeper: Keeper::Private {
                root: Token::new(99),
                directory: bytes::copy_of(b"secret"),
                file: bytes::copy_of(b"record"),
            },
        }) {
            Ok(()) => {}
            Err(_) => unreachable!("account slot"),
        }
        Owner {
            component: Component::new(configured, &limits, 7).expect("private account"),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            above: Queue::with_capacity(32),
            io: Queue::with_capacity(32),
            files: Queue::with_capacity(32),
        }
    }
    fn ask(&mut self, request: Request) {
        self.component.down(&self.env, request, &mut self.above, &mut self.io, &mut self.files);
    }
    fn fire(&mut self) {
        let before_above = self.above.len();
        let before_io = self.io.len();
        let before_files = self.files.len();
        self.component.fire(&self.env, &mut self.above, &mut self.io, &mut self.files);
        assert!(self.above.len().saturating_sub(before_above) <= MAX_OUT_FIRE.above);
        assert!(self.io.len().saturating_sub(before_io) <= MAX_OUT_FIRE.io);
        assert!(self.files.len().saturating_sub(before_files) <= MAX_OUT_FIRE.files);
    }
    fn terminal(&mut self, event: file::Event) {
        let before_above = self.above.len();
        let before_io = self.io.len();
        let before_files = self.files.len();
        self.component.filed(&self.env, event, &mut self.above, &mut self.io, &mut self.files);
        assert!(self.above.len().saturating_sub(before_above) <= MAX_OUT_FILED.above);
        assert!(self.io.len().saturating_sub(before_io) <= MAX_OUT_FILED.io);
        assert!(self.files.len().saturating_sub(before_files) <= MAX_OUT_FILED.files);
    }
    fn next(&mut self) -> file::Request {
        for _ in 0_u32..32 {
            match self.files.pop() {
                Some(FileLower::Request { request, deadline }) => {
                    assert_eq!(deadline, self.env.now.saturating_add(self.env.limits.file_stall));
                    return request;
                }
                Some(FileLower::Cancel { .. }) => panic!("expected a file request"),
                None => self.fire(),
            }
        }
        panic!("one ready file request")
    }
    fn opened(&mut self) -> Token {
        let request = self.next();
        check_file(&request, 0);
        let owner = owner_of(&request);
        self.terminal(file::Event::Opened { owner, file: Token::new(100), len: 0 });
        let request = self.next();
        check_file(&request, 1);
        owner_of(&request)
    }
}

fn owner_of(request: &file::Request) -> Token {
    match request {
        file::Request::MakeDirectory { owner, .. }
        | file::Request::OpenPrivate { owner, .. }
        | file::Request::Create { owner, .. }
        | file::Request::CreateNoFollow { owner, .. }
        | file::Request::OpenRead { owner, .. }
        | file::Request::OpenReadNoFollow { owner, .. }
        | file::Request::OpenDirectory { owner, .. }
        | file::Request::Load { owner, .. }
        | file::Request::Scan { owner, .. }
        | file::Request::Store { owner, .. }
        | file::Request::Stat { owner, .. }
        | file::Request::WriteAt { owner, .. }
        | file::Request::ReadAt { owner, .. }
        | file::Request::Sync { owner, .. }
        | file::Request::Close { owner, .. }
        | file::Request::SyncDirectory { owner, .. }
        | file::Request::Rename { owner, .. }
        | file::Request::Remove { owner, .. }
        | file::Request::List { owner, .. } => *owner,
    }
}

#[test]
fn requests_during_a_load_join_it_and_end_once() {
    let mut owner = Owner::new();
    owner.ask(Request::SignIn { account: 0 });
    owner.ask(Request::Grant { account: 0 });
    owner.ask(Request::Grant { account: 0 });
    assert_refused(owner.above.pop(), 0, Asked::Grant, Refusal::Held);
    let load = owner.opened();
    assert!(owner.above.is_empty() && owner.io.is_empty(), "no visit before loading");
    owner.terminal(file::Event::Loaded {
        owner: load,
        bytes: skein_oauth::encode_record(&tests_record(1), &owner.env.limits.client.document).expect("record"),
    });
    for _ in 0_u32..4 {
        owner.fire();
    }
    assert_visit(owner.above.pop());
    owner.ask(Request::Cancel { account: 0 });
    assert_event(
        owner.above.pop(),
        Event::Failed { account: 0, ends: Ends::SignIn, failure: Failure::Exchange(skein_oauth::Failure::Cancelled) },
    );
    assert_event(
        owner.above.pop(),
        Event::Failed { account: 0, ends: Ends::Grant, failure: Failure::Exchange(skein_oauth::Failure::Cancelled) },
    );
    assert!(owner.above.is_empty());
}

#[test]
fn missing_and_unreadable_records_end_grants_but_admit_sign_in() {
    for unreadable in [false, true] {
        let mut owner = Owner::new();
        owner.ask(Request::SignIn { account: 0 });
        owner.ask(Request::Grant { account: 0 });
        let load = owner.opened();
        let event = if unreadable {
            file::Event::Loaded { owner: load, bytes: bytes::copy_of(b"old format") }
        } else {
            file::Event::Failed { owner: load, error: kernel::Error::NotFound, committed: false, residue: None }
        };
        owner.terminal(event);
        assert_event(
            owner.above.pop(),
            Event::Failed {
                account: 0,
                ends: Ends::Grant,
                failure: if unreadable {
                    Failure::Unloaded { at: Place::File, why: Unloaded::Unreadable }
                } else {
                    Failure::Expired
                },
            },
        );
        owner.fire();
        assert_visit(owner.above.pop());
        assert!(owner.above.is_empty());
    }
}

#[test]
fn an_unread_load_fails_sign_in_before_visit_and_the_next_request_reloads() {
    for why in [
        Unloaded::Refused(file::Unsafe::Mode(0o644)),
        Unloaded::Failed(kernel::Error::Permission),
        Unloaded::Stalled,
        Unloaded::TooLarge { size: 300 },
    ] {
        let mut owner = Owner::new();
        owner.ask(Request::SignIn { account: 0 });
        owner.ask(Request::Grant { account: 0 });
        let load = owner.opened();
        let event = match why {
            Unloaded::Refused(found) => file::Event::Refused { owner: load, found },
            Unloaded::Failed(error) => file::Event::Failed { owner: load, error, committed: false, residue: None },
            Unloaded::Stalled => {
                file::Event::Failed { owner: load, error: kernel::Error::TimedOut, committed: false, residue: None }
            }
            Unloaded::TooLarge { size } => file::Event::TooLarge { owner: load, size },
            Unloaded::Unreadable => unreachable!("unread failure stories"),
        };
        owner.terminal(event);
        for ends in [Ends::Grant, Ends::SignIn] {
            assert_event(
                owner.above.pop(),
                Event::Failed { account: 0, ends, failure: Failure::Unloaded { at: Place::File, why } },
            );
        }
        assert!(owner.above.is_empty() && owner.io.is_empty());
        owner.component.reclaim();
        owner.ask(Request::Grant { account: 0 });
        let request = owner.next();
        check_file(&request, 1);
        owner.terminal(file::Event::Loaded {
            owner: owner_of(&request),
            bytes: skein_oauth::encode_record(&tests_record(1), &owner.env.limits.client.document)
                .expect("person fixed record"),
        });
        assert_granted(owner.above.pop(), 1, 30);
        owner.ask(Request::Release { account: 0 });
        owner.ask(Request::Grant { account: 0 });
        assert_granted(owner.above.pop(), 1, 30);
        assert!(owner.files.is_empty(), "a held record is cached");
    }
}

#[test]
fn cancelling_a_sign_in_waiting_on_load_leaves_the_account_load_running() {
    let mut owner = Owner::new();
    owner.ask(Request::SignIn { account: 0 });
    let load = owner.opened();
    owner.ask(Request::Grant { account: 0 });
    owner.ask(Request::Cancel { account: 0 });
    assert_event(
        owner.above.pop(),
        Event::Failed { account: 0, ends: Ends::SignIn, failure: Failure::Exchange(skein_oauth::Failure::Cancelled) },
    );
    owner.terminal(file::Event::Loaded {
        owner: load,
        bytes: skein_oauth::encode_record(&tests_record(1), &owner.env.limits.client.document).expect("record"),
    });
    assert_granted(owner.above.pop(), 1, 30);
    assert!(owner.files.is_empty());
}

#[test]
fn close_finishes_the_load_then_closes_only_its_private_root() {
    let mut owner = Owner::new();
    owner.ask(Request::Grant { account: 0 });
    let load = owner.opened();
    owner.ask(Request::Close);
    assert!(owner.above.is_empty());
    owner.terminal(file::Event::Loaded {
        owner: load,
        bytes: skein_oauth::encode_record(&tests_record(1), &owner.env.limits.client.document).expect("record"),
    });
    assert_granted(owner.above.pop(), 1, 30);
    let close = owner.next();
    check_file(&close, 2);
    assert!(owner.above.is_empty());
    owner.terminal(file::Event::Closed { owner: owner_of(&close) });
    assert_event(owner.above.pop(), Event::Closed);
    assert!(!owner.component.has_work());
    assert_eq!(owner.component.next_deadline(), None);
}

#[test]
fn abort_cancels_a_load_and_waiters_and_waits_for_its_terminal_before_closed() {
    let mut owner = Owner::new();
    owner.ask(Request::SignIn { account: 0 });
    owner.ask(Request::Grant { account: 0 });
    let load = owner.opened();
    owner.ask(Request::Abort);
    for _ in 0_u32..4 {
        owner.fire();
    }
    for ends in [Ends::SignIn, Ends::Grant] {
        assert_event(
            owner.above.pop(),
            Event::Failed { account: 0, ends, failure: Failure::Exchange(skein_oauth::Failure::Cancelled) },
        );
    }
    match owner.files.pop() {
        Some(FileLower::Cancel { owner: token }) => assert_eq!(token, load),
        Some(FileLower::Request { .. }) | None => panic!("abort cancels the outstanding owner"),
    }
    assert!(owner.above.is_empty());
    owner.terminal(file::Event::Cancelled { owner: load });
    let close = owner.next();
    owner.terminal(file::Event::Closed { owner: owner_of(&close) });
    assert_event(owner.above.pop(), Event::Closed);
}

fn assert_visit(event: Option<Event>) {
    match event {
        Some(Event::Visit { .. }) => {}
        Some(
            Event::SignedIn { .. }
            | Event::Granted { .. }
            | Event::Expiring { .. }
            | Event::Keep { .. }
            | Event::Failed { .. }
            | Event::Refused { .. }
            | Event::Closed,
        )
        | None => panic!("one visit after loading"),
    }
}

fn check_file(request: &file::Request, shape: u32) {
    match request {
        file::Request::OpenPrivate { root, path, .. } => {
            assert_eq!(shape, 0);
            assert_eq!(*root, Token::new(99));
            assert_eq!(path.as_ref(), b"secret");
        }
        file::Request::Load { root, no_follow, .. } => {
            assert_eq!(shape, 1);
            assert_eq!(*root, Token::new(100));
            assert!(*no_follow);
        }
        file::Request::Close { file, .. } => {
            assert_eq!(shape, 2);
            assert_eq!(*file, Token::new(100));
        }
        file::Request::MakeDirectory { .. }
        | file::Request::Create { .. }
        | file::Request::CreateNoFollow { .. }
        | file::Request::OpenRead { .. }
        | file::Request::OpenReadNoFollow { .. }
        | file::Request::OpenDirectory { .. }
        | file::Request::Scan { .. }
        | file::Request::Store { .. }
        | file::Request::Stat { .. }
        | file::Request::WriteAt { .. }
        | file::Request::ReadAt { .. }
        | file::Request::Sync { .. }
        | file::Request::SyncDirectory { .. }
        | file::Request::Rename { .. }
        | file::Request::Remove { .. }
        | file::Request::List { .. } => panic!("one private open, load or close"),
    }
}
