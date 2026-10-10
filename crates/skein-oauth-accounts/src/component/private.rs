//! Private record load, durable keep and settlement (oauth.md, section 6.4).
//! Account requests join one load. A missing or unreadable record permits a
//! sign-in; every other unread cause ends it before Visit. An outstanding file
//! operation retains its owner token through cancellation and root closing.

use super::{Component, FileLower, Lifecycle, State, failed, recorded};
use crate::exchange::Stage;
use crate::keeper::{Operation, Pending, Store};
use crate::{Asked, Event, Failure, Keeping, Limits, Lower, Place, Refusal, Unloaded};
use skein_io::digest::digest;
use skein_io::{file, kernel};
use skein_lib::{Env, Queue, Token};
use skein_oauth as oauth;

impl Component {
    pub(super) fn wait_for_load(&mut self, account: u32, up: &mut Queue<Event>) -> bool {
        let empty = match self.accounts.get(account).expect("account state") {
            State::Empty => true,
            State::Record { .. } | State::Loaded { .. } => false,
        };
        let can_wait = match *self.bindings.get(account).expect("account binding") {
            Some(id) => match self.exchanges.get(id).expect("bound exchange").stage {
                Stage::Loading => true,
                Stage::Running | Stage::Keeping { .. } | Stage::Finished => false,
            },
            None => true,
        };
        match self.keepers.get_mut(account).expect("account keeper") {
            Store::Private(keeper) if empty && can_wait => {
                if keeper.waiting_grant {
                    up.push(Event::Refused { account, asked: Asked::Grant, why: Refusal::Held });
                } else {
                    keeper.waiting_grant = true;
                    keeper.reload();
                }
                true
            }
            Store::Private(_) | Store::Owner => false,
        }
    }

    pub(super) fn private_progress(
        &mut self,
        env: &Env<Limits>,
        up: &mut Queue<Event>,
        _io: &mut Queue<Lower>,
        files: &mut Queue<FileLower>,
    ) -> bool {
        for account in 0..self.keepers.len() {
            let keeper = match self.keepers.get_mut(account).expect("account keeper") {
                Store::Private(keeper) => keeper,
                Store::Owner => continue,
            };
            if self.lifecycle == Lifecycle::Aborting && keeper.waiting_grant {
                keeper.waiting_grant = false;
                failed(account, Failure::Exchange(oauth::Failure::Cancelled), up);
                return true;
            }
            match &mut keeper.pending {
                Some(pending) => {
                    if self.lifecycle == Lifecycle::Aborting && pending.operation != Operation::Close {
                        pending.cancel = true;
                    }
                    if pending.cancel && !pending.cancelled {
                        pending.cancelled = true;
                        pending.deadline = None;
                        files.push(FileLower::Cancel { owner: pending.owner });
                        return true;
                    }
                    match pending.deadline {
                        Some(deadline) if env.now >= deadline => {
                            // The owner's file adapter expires active and queued requests.
                            // Stop advertising a fired deadline while its terminal settles.
                            pending.deadline = None;
                            return true;
                        }
                        Some(_) | None => {}
                    }
                    continue;
                }
                None => {}
            }
            let binding = *self.bindings.get(account).expect("account binding");
            if self.lifecycle != Lifecycle::Live && binding.is_none() && !keeper.waiting_grant {
                keeper.next = match keeper.root {
                    Some(_) => Some(Operation::Close),
                    None => {
                        keeper.closed = true;
                        None
                    }
                };
            }
            let operation = match keeper.next.take() {
                Some(operation) => operation,
                None => continue,
            };
            self.file_owner = self.file_owner.checked_add(1).expect("file owner token space exhausted");
            let owner = Token::new(self.file_owner);
            let deadline = env.now.saturating_add(env.limits.file_stall);
            let request = match operation {
                Operation::Open => {
                    file::Request::OpenPrivate { owner, root: keeper.base, path: keeper.directory.clone() }
                }
                Operation::Load => file::Request::Load {
                    owner,
                    root: keeper.root.expect("private root"),
                    path: keeper.file.clone(),
                    max: env.limits.client.document.record_bytes,
                    no_follow: true,
                },
                Operation::Store { generation } => {
                    let id = binding.expect("keep exchange");
                    let exchange = self.exchanges.get(id).expect("bound exchange");
                    let bytes = match &exchange.stage {
                        Stage::Keeping { candidate } if candidate.generation == generation => {
                            oauth::encode_record(candidate, &env.limits.client.document).expect("client record bounds")
                        }
                        Stage::Loading | Stage::Running | Stage::Keeping { .. } | Stage::Finished => {
                            unreachable!("private keep retains its candidate until its file terminal")
                        }
                    };
                    file::Request::Store {
                        owner,
                        root: keeper.root.expect("private root"),
                        path: keeper.file.clone(),
                        bytes,
                        expected: keeper.expected,
                        no_follow: true,
                    }
                }
                Operation::Close => file::Request::Close { owner, file: keeper.root.expect("private root") },
            };
            keeper.pending =
                Some(Pending { operation, owner, deadline: Some(deadline), cancel: false, cancelled: false });
            files.push(FileLower::Request { request, deadline });
            return true;
        }
        self.settled(up);
        false
    }

    /// Routes one matching file terminal, retaining the private root until its close terminal.
    pub fn filed(
        &mut self,
        env: &Env<Limits>,
        event: file::Event,
        up: &mut Queue<Event>,
        io: &mut Queue<Lower>,
        _files: &mut Queue<FileLower>,
    ) {
        self.initialize(env);
        let owner = event.owner();
        let mut found = None;
        for account in 0..self.keepers.len() {
            match self.keepers.get(account).expect("account keeper") {
                Store::Private(keeper) => match &keeper.pending {
                    Some(pending) if pending.owner == owner => {
                        found = Some(account);
                        break;
                    }
                    Some(_) | None => {}
                },
                Store::Owner => {}
            }
        }
        let account = match found {
            Some(account) => account,
            None => return,
        };
        self.private_terminal(env, account, event, up, io);
    }

    fn private_terminal(
        &mut self,
        env: &Env<Limits>,
        account: u32,
        event: file::Event,
        up: &mut Queue<Event>,
        io: &mut Queue<Lower>,
    ) {
        let keeper = match self.keepers.get_mut(account).expect("account keeper") {
            Store::Private(keeper) => keeper,
            Store::Owner => unreachable!("matched private terminal"),
        };
        let pending = keeper.pending.take().expect("matched file owner");
        match pending.operation {
            Operation::Open => match event {
                file::Event::Opened { file, .. } => {
                    keeper.root = Some(file);
                    keeper.next = Some(Operation::Load);
                }
                other @ (file::Event::Made { .. }
                | file::Event::Refused { .. }
                | file::Event::Loaded { .. }
                | file::Event::Scanned { .. }
                | file::Event::Stored { .. }
                | file::Event::Conflict { .. }
                | file::Event::TooLarge { .. }
                | file::Event::Stated { .. }
                | file::Event::Written { .. }
                | file::Event::Read { .. }
                | file::Event::Synced { .. }
                | file::Event::Closed { .. }
                | file::Event::Renamed { .. }
                | file::Event::Removed { .. }
                | file::Event::Listed { .. }
                | file::Event::Failed { .. }
                | file::Event::Cancelled { .. }) => self.loaded(
                    env,
                    account,
                    None,
                    Some(Failure::Unloaded { at: Place::Directory, why: unloaded(other) }),
                    up,
                    io,
                ),
            },
            Operation::Load => {
                let failure = match event {
                    file::Event::Loaded { bytes, .. } => {
                        keeper.expected = file::Expect::Digest(digest(&bytes));
                        match oauth::decode_record(&bytes, &env.limits.client.document) {
                            Ok(record) => {
                                self.loaded(env, account, Some(record), None, up, io);
                                return;
                            }
                            Err(_) => Some(Failure::Unloaded { at: Place::File, why: Unloaded::Unreadable }),
                        }
                    }
                    file::Event::Failed { error: kernel::Error::NotFound, .. } => {
                        keeper.expected = file::Expect::Absent;
                        None
                    }
                    other @ (file::Event::Made { .. }
                    | file::Event::Refused { .. }
                    | file::Event::Opened { .. }
                    | file::Event::Scanned { .. }
                    | file::Event::Stored { .. }
                    | file::Event::Conflict { .. }
                    | file::Event::TooLarge { .. }
                    | file::Event::Stated { .. }
                    | file::Event::Written { .. }
                    | file::Event::Read { .. }
                    | file::Event::Synced { .. }
                    | file::Event::Closed { .. }
                    | file::Event::Renamed { .. }
                    | file::Event::Removed { .. }
                    | file::Event::Listed { .. }
                    | file::Event::Failed { .. }
                    | file::Event::Cancelled { .. }) => {
                        Some(Failure::Unloaded { at: Place::File, why: unloaded(other) })
                    }
                };
                self.loaded(env, account, None, failure, up, io);
            }
            Operation::Store { generation } => self.stored(env, account, generation, event, up, io),
            Operation::Close => {
                keeper.root = None;
                keeper.closed = true;
            }
        }
        self.settled(up);
    }

    fn stored(
        &mut self,
        env: &Env<Limits>,
        account: u32,
        generation: u64,
        event: file::Event,
        up: &mut Queue<Event>,
        io: &mut Queue<Lower>,
    ) {
        let keeper = match self.keepers.get_mut(account).expect("account keeper") {
            Store::Private(keeper) => keeper,
            Store::Owner => unreachable!("private storing"),
        };
        let keeping = match event {
            file::Event::Stored { digest, .. } => {
                keeper.expected = file::Expect::Digest(digest);
                Keeping::Kept
            }
            file::Event::Made { .. }
            | file::Event::Refused { .. }
            | file::Event::Opened { .. }
            | file::Event::Loaded { .. }
            | file::Event::Scanned { .. }
            | file::Event::Conflict { .. }
            | file::Event::TooLarge { .. }
            | file::Event::Stated { .. }
            | file::Event::Written { .. }
            | file::Event::Read { .. }
            | file::Event::Synced { .. }
            | file::Event::Closed { .. }
            | file::Event::Renamed { .. }
            | file::Event::Removed { .. }
            | file::Event::Listed { .. }
            | file::Event::Failed { .. }
            | file::Event::Cancelled { .. } => Keeping::NotKept,
        };
        let id = self.bindings.get(account).expect("account binding").expect("file retains exchange");
        self.exchanges.get_mut(id).expect("bound exchange").file_active = false;
        self.kept(env, account, generation, keeping, up, io);
        if self.exchanges.get(id).is_some() {
            self.collect(id, up);
        }
    }

    fn loaded(
        &mut self,
        env: &Env<Limits>,
        account: u32,
        record: Option<oauth::SavedToken>,
        failure: Option<Failure>,
        up: &mut Queue<Event>,
        io: &mut Queue<Lower>,
    ) {
        let waiting_grant = match self.keepers.get_mut(account).expect("account keeper") {
            Store::Private(keeper) => core::mem::replace(&mut keeper.waiting_grant, false),
            Store::Owner => unreachable!("private loading"),
        };
        let has_record = record.is_some();
        match record {
            Some(record) => *self.accounts.get_mut(account).expect("account state") = recorded(record, env, false),
            None => {}
        }
        let signer = match *self.bindings.get(account).expect("account binding") {
            Some(id) => match self.exchanges.get(id).expect("bound exchange").stage {
                Stage::Loading => Some(id),
                Stage::Running | Stage::Keeping { .. } | Stage::Finished => None,
            },
            None => None,
        };
        if self.lifecycle == Lifecycle::Aborting {
            if waiting_grant {
                failed(account, Failure::Exchange(oauth::Failure::Cancelled), up);
            }
            match signer {
                Some(id) => {
                    self.fail_exchange(env, id, Failure::Exchange(oauth::Failure::Cancelled), up, io);
                    self.collect(id, up);
                }
                None => {}
            }
            return;
        }
        let sign_in_allowed = match failure {
            None | Some(Failure::Unloaded { why: Unloaded::Unreadable, .. }) => true,
            Some(Failure::Unloaded { .. } | Failure::Expired | Failure::Exchange(_) | Failure::NotKept) => false,
        };
        if waiting_grant && !has_record {
            failed(account, failure.unwrap_or(Failure::Expired), up);
        }
        match signer {
            Some(_) if sign_in_allowed => self.begin_sign_in(env, account, io),
            Some(id) => {
                self.fail_exchange(env, id, failure.expect("unread cause prevents sign-in"), up, io);
                self.collect(id, up);
            }
            None => {}
        }
        if waiting_grant && has_record {
            self.grant_loaded(env, account, up);
        }
    }
}

fn unloaded(event: file::Event) -> Unloaded {
    match event {
        file::Event::Refused { found, .. } => Unloaded::Refused(found),
        file::Event::TooLarge { size, .. } => Unloaded::TooLarge { size },
        file::Event::Failed { error: kernel::Error::TimedOut, .. } => Unloaded::Stalled,
        file::Event::Failed { error, .. } => Unloaded::Failed(error),
        file::Event::Cancelled { .. } => Unloaded::Failed(kernel::Error::Cancelled),
        file::Event::Made { .. }
        | file::Event::Opened { .. }
        | file::Event::Loaded { .. }
        | file::Event::Scanned { .. }
        | file::Event::Stored { .. }
        | file::Event::Conflict { .. }
        | file::Event::Stated { .. }
        | file::Event::Written { .. }
        | file::Event::Read { .. }
        | file::Event::Synced { .. }
        | file::Event::Closed { .. }
        | file::Event::Renamed { .. }
        | file::Event::Removed { .. }
        | file::Event::Listed { .. } => Unloaded::Failed(kernel::Error::InvalidArgument),
    }
}
