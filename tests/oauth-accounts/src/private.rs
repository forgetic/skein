//! Private keepers on the minimal machine, with the existing independent socket
//! issuer and owner contract referee (oauth.md, sections 6.4 and 6.8).

use crate::world::{self, Client, Fact, Process, Story as OwnerStory};
use skein_fake_machine::{How, Item, Machine as Files, Opened};
use skein_fake_peers::Transport;
use skein_io::kernel::{Error, Op};
use skein_lib::{Duration, Queue, Time, Wall, bytes};
use skein_oauth as oauth;
use skein_sim::{Answer, Ask, Call, Handle};
use skein_world::{Host, Machine, Memory, Referee, World};
use std::net::Ipv4Addr;

/// The private record or file fault the scenario plants.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Story {
    /// Loads a kept valid record before answering Grant.
    Load,
    /// A failed read ends its waiting grant.
    LoadFailed,
    /// A silent read ends its waiting grant within `file_stall`.
    LoadStalled,
    /// A person fixes the record after a failed load; the next grant reloads it.
    Fixed,
    /// Another writer changes the file after loading; digest replacement refuses it.
    Conflict,
    /// A record larger than the configured bound is refused unread.
    TooLarge,
    /// Close during loading waits for the admitted grant's terminal.
    CloseLoading,
    /// Abort during loading cancels the admitted grant and file operation.
    AbortLoading,
    /// Refreshes and stores a candidate durably before lending it.
    Refresh,
    /// Rejects a directory with group permission bits unread.
    RootMode,
    /// Rejects a file with group permission bits unread.
    FileMode,
    /// Rejects another user's file unread.
    FileOwner,
    /// Rejects a symbolic file link unread.
    FileLink,
    /// Rejects a file with two names unread.
    FileLinks,
    /// A missing record answers Grant as Expired.
    Missing,
    /// A damaged record answers Grant with Unreadable.
    Unreadable,
    /// A failed write lends no new candidate.
    KeepFailed,
    /// A silent write is cancelled at `file_stall` and lends no new candidate.
    KeepStalled,
    /// Close during the private keep waits for its durability terminal.
    CloseKeeping,
    /// Abort during the private keep ends the exchange and settles its cancelled store.
    AbortKeeping,
}

/// The actual scenario filesystem, observing reads, writes and durable operations.
#[derive(Debug)]
pub struct FileSystem {
    pub files: Files,
    root: Opened,
    story: Story,
    pub reads: usize,
    pub writes: usize,
    pub syncs: usize,
    pub renames: usize,
    pub faulted: bool,
    observer_alive: bool,
    pub cuts: usize,
}

impl FileSystem {
    pub(crate) fn new(story: Story) -> Self {
        let profile = world::limits();
        let record = oauth::SavedToken {
            key: 0,
            generation: 0,
            access_token: bytes::copy_of(b"old"),
            refresh_token: Some(bytes::copy_of(b"seed")),
            metadata: None,
            expires_at: Wall::from_nanos(
                Duration::from_secs(
                    if matches!(story, Story::Load | Story::LoadFailed | Story::LoadStalled | Story::Fixed) {
                        30
                    } else {
                        5
                    },
                )
                .as_nanos(),
            ),
        };
        let encoded = oauth::encode_record(&record, &profile.client.document).expect("planted kept record");
        let mut items = vec![Item::directory(b"secret").mode(if story == Story::RootMode { 0o755 } else { 0o700 })];
        match story {
            Story::Missing => {}
            Story::TooLarge => items.push(Item::file(b"secret/record", &[b'x'; 1025]).mode(0o600)),
            Story::Unreadable | Story::Fixed => {
                items.push(Item::file(b"secret/record", b"another version").mode(0o600));
            }
            Story::FileLink => {
                items.push(Item::file(b"secret/target", &encoded).mode(0o600));
                items.push(Item::link(b"secret/record", b"target"));
            }
            Story::FileOwner => items.push(Item::file(b"secret/record", &encoded).mode(0o600).owner(1235)),
            Story::FileLinks => {
                items.push(Item::file(b"secret/record", &encoded).mode(0o600));
                items.push(Item::hard_link(b"secret/other", b"secret/record"));
            }
            Story::Load
            | Story::LoadFailed
            | Story::LoadStalled
            | Story::Conflict
            | Story::CloseLoading
            | Story::AbortLoading
            | Story::Refresh
            | Story::RootMode
            | Story::FileMode
            | Story::KeepFailed
            | Story::KeepStalled
            | Story::CloseKeeping
            | Story::AbortKeeping => {
                items.push(Item::file(b"secret/record", &encoded).mode(if story == Story::FileMode {
                    0o644
                } else {
                    0o600
                }));
            }
        }
        let mut files = Files::for_user(1234);
        let root = files.lay(&items);
        Self {
            files,
            root,
            story,
            reads: 0,
            writes: 0,
            syncs: 0,
            renames: 0,
            faulted: false,
            observer_alive: true,
            cuts: 0,
        }
    }
    pub(crate) fn for_cuts() -> Self {
        let mut files = Self::new(Story::Refresh);
        files.files.close(files.root);
        files.observer_alive = false;
        files
    }
    pub(crate) fn startup(&mut self) -> Handle {
        Handle::new(self.files.open(self.root, b".", How::Directory).expect("independent startup root").raw())
    }
    /// Reads the kept bytes through an independent observer after owner settlement.
    #[must_use]
    pub fn record(&mut self) -> oauth::SavedToken {
        if !self.observer_alive {
            self.root = self.files.reopen_root(0);
            self.observer_alive = true;
        }
        let file = self.files.open(self.root, b"secret/record", How::Read).expect("observed kept record");
        assert_eq!(self.files.stat(file).mode & 0o777, 0o600);
        let bytes = self.files.read(file, 0, 1024).expect("observed whole record");
        self.files.close(file);
        oauth::decode_record(&bytes, &world::limits().client.document).expect("observed record")
    }
    /// Returns the final bytes, including an external writer's conflicting replacement.
    #[must_use]
    pub fn contents(&mut self) -> Vec<u8> {
        let file = self.files.open(self.root, b"secret/record", How::Read).expect("observed private file");
        let contents = self.files.read(file, 0, 1024).expect("observed bytes");
        self.files.close(file);
        contents
    }
    fn person_writes(&mut self, contents: &[u8]) {
        let file = self.files.open(self.root, b"secret/record", How::Read).expect("person's file");
        self.files.write(file, 0, contents).expect("person's update");
        self.files.sync(file);
        self.files.close(file);
    }
    /// Releases the observer root and checks that all owner handles were released.
    pub fn finish(&mut self) {
        if self.observer_alive {
            self.files.close(self.root);
            self.observer_alive = false;
        }
        assert_eq!(self.files.open_handles(), 0, "private root and startup root settle separately");
    }
}

impl Machine for FileSystem {
    fn cut(&mut self, cut: skein_world::Cut, held: &[Handle], seed: u64) {
        for handle in held {
            self.files.close(Opened::new(handle.raw()));
        }
        if cut == skein_world::Cut::PowerLoss {
            self.files.crash(seed);
        }
        self.cuts += 1;
    }
    fn open_root(&mut self, path: &[u8]) -> Result<Handle, Error> {
        assert_eq!(path, b"root");
        Ok(Handle::new(self.files.reopen_root(0).raw()))
    }
    fn close_root(&mut self, root: Handle) {
        self.files.close(Opened::new(root.raw()));
    }

    fn step(&mut self, call: Call, answers: &mut Queue<Answer>) {
        self.reads += usize::from(matches!(call.ask, Ask::Read { .. }));
        self.writes += usize::from(matches!(call.ask, Ask::Write { .. }));
        self.syncs += usize::from(matches!(call.ask, Ask::Sync { .. }));
        self.renames += usize::from(matches!(call.ask, Ask::Rename { .. }));
        if !self.faulted && self.story == Story::Conflict && matches!(call.ask, Ask::Sync { .. }) {
            self.faulted = true;
            self.person_writes(b"another writer");
        }
        let fail_read = !self.faulted
            && matches!(call.ask, Ask::Read { .. })
            && matches!(self.story, Story::LoadFailed | Story::Fixed);
        if fail_read && self.story == Story::Fixed {
            let record = oauth::SavedToken {
                key: 0,
                generation: 0,
                access_token: bytes::copy_of(b"old"),
                refresh_token: Some(bytes::copy_of(b"seed")),
                metadata: None,
                expires_at: Wall::from_nanos(Duration::from_secs(30).as_nanos()),
            };
            self.person_writes(
                &oauth::encode_record(&record, &world::limits().client.document).expect("person's record"),
            );
        }
        if fail_read || (!self.faulted && matches!(call.ask, Ask::Write { .. }) && self.story == Story::KeepFailed) {
            self.faulted = true;
            answers.push(Answer { ticket: call.ticket, result: Err(Error::Other(5)) });
        } else {
            skein_fake_machine::step(&mut self.files, call, answers);
        }
    }
}

/// Routes the peer's bound address and ends the issuer only after the owner settles.
#[derive(Debug)]
pub struct Judge {
    delivered: bool,
    shutdown: bool,
    wake: Option<Time>,
    passed: bool,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, processes: &mut [Process]) {
        let address = match &processes[0] {
            Process::Issuer(peer) => peer.address(),
            Process::Client(_) => unreachable!("issuer process"),
        };
        let client = match &mut processes[1] {
            Process::Client(client) => client,
            Process::Issuer(_) => unreachable!("owner process"),
        };
        if !self.delivered
            && let Some(address) = address
        {
            client.address = Some(address);
            self.delivered = true;
        }
        if client.is_empty() && !self.shutdown {
            self.shutdown = true;
            match &mut processes[0] {
                Process::Issuer(peer) => peer.shutdown(),
                Process::Client(_) => unreachable!("issuer process"),
            }
        }
    }
    fn observe(&mut self, now: Time, processes: &[Process]) {
        let client = match &processes[1] {
            Process::Client(client) => client,
            Process::Issuer(_) => unreachable!("owner process"),
        };
        let address_ready = match &processes[0] {
            Process::Issuer(peer) => peer.address().is_some(),
            Process::Client(_) => false,
        };
        self.wake = ((!self.delivered && address_ready) || (client.is_empty() && !self.shutdown)).then_some(now);
        self.passed = self.shutdown && processes.iter().all(Host::is_empty);
    }
    fn next_deadline(&self) -> Option<Time> {
        self.wake.or(Some(Time::from_nanos(180_000_000_000)))
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= Time::from_nanos(180_000_000_000) && !self.passed).then(|| "private account did not settle".to_owned())
    }
    fn passed(&self) -> bool {
        self.passed
    }
}

/// Runs the actual account owner, `FileIo` and fake issuer on the minimal machine.
#[must_use]
pub fn run(seed: u64, story: Story) -> skein_world::Outcome<Process, FileSystem> {
    let mut files = FileSystem::new(story);
    let root = files.startup();
    let owner_story = if matches!(story, Story::Refresh | Story::CloseKeeping | Story::AbortKeeping) {
        OwnerStory::Rotate
    } else {
        OwnerStory::NotKept
    };
    let mut config = skein_sim::Config::calm();
    config.wall = Wall::EPOCH;
    let mut world = World::new(
        seed,
        config,
        Judge { delivered: false, shutdown: false, wake: None, passed: false },
        Memory::Checked,
    )
    .with_machine(files);
    world.spawn(|| world::issuer(Transport::Plaintext, owner_story, (Ipv4Addr::LOCALHOST, 31000).into()));
    world.spawn_root(root, |fd| {
        let mut client = Client::private(fd, owner_story);
        client.stop_at_store = match story {
            Story::CloseKeeping => Some(false),
            Story::AbortKeeping => Some(true),
            Story::Load
            | Story::LoadFailed
            | Story::LoadStalled
            | Story::Fixed
            | Story::Conflict
            | Story::TooLarge
            | Story::CloseLoading
            | Story::AbortLoading
            | Story::Refresh
            | Story::RootMode
            | Story::FileMode
            | Story::FileOwner
            | Story::FileLink
            | Story::FileLinks
            | Story::Missing
            | Story::Unreadable
            | Story::KeepFailed
            | Story::KeepStalled => None,
        };
        client.stop_at_load = match story {
            Story::CloseLoading => Some(false),
            Story::AbortLoading => Some(true),
            Story::Load
            | Story::LoadFailed
            | Story::LoadStalled
            | Story::Fixed
            | Story::Conflict
            | Story::TooLarge
            | Story::Refresh
            | Story::RootMode
            | Story::FileMode
            | Story::FileOwner
            | Story::FileLink
            | Story::FileLinks
            | Story::Missing
            | Story::Unreadable
            | Story::KeepFailed
            | Story::KeepStalled
            | Story::CloseKeeping
            | Story::AbortKeeping => None,
        };
        client.retry_load = matches!(story, Story::Fixed | Story::LoadFailed | Story::LoadStalled).then_some(());
        Process::Client(Box::new(client))
    });
    let mut stalled = false;
    let mut outcome = world.run_with_faults(|_, _, submissions| {
        let stall = !stalled
            && submissions.iter().any(|submit| {
                (story == Story::KeepStalled && matches!(submit.kind, Op::Write { .. }))
                    || (story == Story::LoadStalled && matches!(submit.kind, Op::Read { .. }))
            });
        stalled |= stall;
        Some(skein_sim::Faults { hung: if stall { 1000 } else { 0 }, ..skein_sim::Faults::NONE })
    });
    outcome.machine.faulted |= stalled;
    outcome
}

/// Content-free owner facts, retained for terminal and replay checks.
#[must_use]
pub fn facts(processes: &[Process]) -> &[Fact] {
    match &processes[1] {
        Process::Client(client) => &client.facts,
        Process::Issuer(_) => unreachable!("owner process"),
    }
}
