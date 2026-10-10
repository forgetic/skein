//! Cuts the actual private keeper's refresh through shared-world restart
//! (oauth.md, section 6.4; simulator.md, section 3.3). Recovery only loads and
//! lends the kept old or new record; it never runs another issuer exchange.

use crate::private::FileSystem;
use crate::world::{self, Client, Process as Inner, Story};
use skein_fake_peers::Transport;
use skein_io::kernel::{Complete, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token, Wall, bytes};
use skein_world::{Host, Inherited, Memory, Referee, StartupRoot, World};
use std::net::Ipv4Addr;

const SIGNAL: Token = Token::new(0x7fff_ffff_ffff_ffff);
const ROOM: u32 = 64;

/// One restartable process, retaining its inherited signal until its owner settles.
pub struct Process {
    inner: Inner,
    signal: Fd,
    closing: bool,
    closed: bool,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
}
impl Process {
    fn new(inner: Inner, signal: Fd) -> Self {
        Self {
            inner,
            signal,
            closing: false,
            closed: false,
            completions: Queue::with_capacity(ROOM),
            submissions: Queue::with_capacity(ROOM),
        }
    }
}
impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            if complete.op == SIGNAL {
                assert!(complete.result.is_ok());
                self.closed = true;
            } else {
                self.inner.completions().push(complete);
            }
        }
        self.inner.iterate(now, wall);
        while let Some(submit) = self.inner.submissions().pop() {
            assert_ne!(submit.op, SIGNAL, "finite child namespaces leave the signal token free");
            self.submissions.push(submit);
        }
        if self.inner.is_empty() && !self.closing {
            self.closing = true;
            self.submissions.push(Submit { op: SIGNAL, kind: Op::Close { fd: self.signal } });
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        !self.completions.is_empty() || self.inner.work_pending(now) || (self.inner.is_empty() && !self.closing)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.inner.next_deadline()
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        self.inner.next_policy_deadline()
    }

    fn is_empty(&self) -> bool {
        self.closed && self.inner.is_empty() && self.completions.is_empty() && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        self.inner.worst_case()
            + Queue::<Complete>::worst_case(ROOM).expect("wrapper completions")
            + Queue::<Submit>::worst_case(ROOM).expect("wrapper submissions")
    }
    fn operations(&self) -> u32 {
        self.inner.operations() + 1
    }
}

fn issuer(inherited: &Inherited) -> Process {
    Process::new(
        world::issuer(Transport::Plaintext, Story::NotKept, (Ipv4Addr::LOCALHOST, 31000).into()),
        inherited.signal,
    )
}
fn owner(inherited: &Inherited) -> Process {
    let root = inherited.roots.first().expect("private startup root").1;
    Process::new(Inner::Client(Box::new(Client::recovery(root))), inherited.signal)
}

/// Only the original owner refreshes; fresh incarnations only recover.
#[derive(Debug)]
pub struct Judge {
    started: bool,
    shutdown: bool,
    wake: Option<Time>,
    passed: bool,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        let address = match &procs[0].inner {
            Inner::Issuer(peer) => peer.address(),
            Inner::Client(_) => unreachable!("issuer"),
        };
        let client = match &mut procs[1].inner {
            Inner::Client(client) => client,
            Inner::Issuer(_) => unreachable!("owner"),
        };
        if let Some(address) = address {
            client.address = Some(address);
            if !self.started && client.unstarted() {
                client.refresh_at_start();
                self.started = true;
            }
        }
        if procs[1].is_empty() && !self.shutdown {
            self.shutdown = true;
            match &mut procs[0].inner {
                Inner::Issuer(peer) => peer.shutdown(),
                Inner::Client(_) => unreachable!("issuer"),
            }
        }
    }
    fn observe(&mut self, now: Time, procs: &[Process]) {
        let address_ready = match &procs[0].inner {
            Inner::Issuer(peer) => peer.address().is_some(),
            Inner::Client(_) => false,
        };
        let client = match &procs[1].inner {
            Inner::Client(client) => client,
            Inner::Issuer(_) => unreachable!("owner"),
        };
        self.wake = ((client.unstarted() && address_ready) || (procs[1].is_empty() && !self.shutdown)).then_some(now);
        self.passed = self.shutdown && procs.iter().all(Host::is_empty);
    }
    fn next_deadline(&self) -> Option<Time> {
        self.wake.or(Some(Time::from_nanos(Duration::from_secs(180).as_nanos())))
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= Time::from_nanos(Duration::from_secs(180).as_nanos()) && !self.passed)
            .then(|| "private recovery did not settle".to_owned())
    }
    fn passed(&self) -> bool {
        self.passed
    }
}

/// Constructs a restartable keeper world; the caller chooses shared-world cut points.
#[must_use]
pub fn world(seed: u64) -> World<Process, Judge, FileSystem> {
    let mut config = skein_sim::Config::calm();
    config.wall = Wall::EPOCH;
    let mut world =
        World::new(seed, config, Judge { started: false, shutdown: false, wake: None, passed: false }, Memory::Checked)
            .with_machine(FileSystem::for_cuts());
    world.spawn_restartable(Vec::new(), issuer);
    world.spawn_restartable(vec![StartupRoot { name: bytes::copy_of(b"root"), path: bytes::copy_of(b"root") }], owner);
    world
}

/// The kept generation loaded by the fresh owner after a cut.
#[must_use]
pub fn recovered(procs: &[Process]) -> u64 {
    match &procs[1].inner {
        Inner::Client(client) => client.recovered.expect("recovery loaded a record"),
        Inner::Issuer(_) => unreachable!("owner"),
    }
}
