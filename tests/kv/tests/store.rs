//! Store through file io, the simulated kernel and fake filesystem.

use skein_fake_machine::{Item, Machine, serve};
use skein_io::file;
use skein_io::file_layer::{self, FileIo};
use skein_io::kernel::{Complete, Fd, Submit};
use skein_kv::{Event, Limits, Op, Page, Range, Request, Store};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_sim::{Config, Faults, Handle, Pid, Sim};

fn limits(snapshot_after: u64) -> Limits {
    Limits {
        key: 64,
        value: 64,
        ops: 8,
        commit: 1024,
        queued: 8,
        queued_bytes: 8192,
        budget: 8192,
        segment: 4096,
        snapshot_after,
        chunk: 256,
        page: Page { rows: 16, bytes: 1024 },
        deadline: Duration::from_secs(1),
    }
}

fn put(key: &[u8], value: &[u8]) -> Op {
    Op::Put { key: Box::from(key), value: Box::from(value) }
}

struct World {
    machine: Machine,
    sim: Sim,
    pid: Pid,
    root: Fd,
    io: FileIo,
    store: Store,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<file::Request>,
    file_events: Queue<file::Event>,
    subs: Queue<Submit>,
    completions: Queue<Complete>,
}

impl World {
    fn new(snapshot_after: u64) -> World {
        Self::with_items(snapshot_after, &[])
    }

    fn with_items(snapshot_after: u64, items: &[Item]) -> World {
        let mut machine = Machine::new();
        let handle = machine.lay(items);
        let mut sim = Sim::new(1, Config::calm());
        let pid = sim.spawn_process();
        let root = sim.root(pid, Handle::new(handle.raw()));
        let limits = limits(snapshot_after);
        World {
            machine,
            sim,
            pid,
            root,
            io: FileIo::new(8, limits.chunk, 1024, limits.deadline),
            store: Store::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            above: Queue::with_capacity(128),
            below: Queue::with_capacity(128),
            file_events: Queue::with_capacity(128),
            subs: Queue::with_capacity(128),
            completions: Queue::with_capacity(128),
        }
    }

    fn request(&mut self, request: Request) {
        self.start(request);
        self.drive(true);
    }

    fn request_no_fire(&mut self, request: Request) {
        self.start(request);
        self.drive(false);
    }

    fn start(&mut self, request: Request) {
        skein_kv::down(&mut self.store, &self.env, request, &mut self.above, &mut self.below);
    }

    fn fire_once(&mut self) {
        skein_kv::fire(&mut self.store, &self.env, &mut self.above, &mut self.below);
        self.drive(false);
    }

    fn drive(&mut self, auto_fire: bool) {
        for _ in 0..10_000 {
            if !self.step(auto_fire) {
                return;
            }
        }
        panic!("store did not quiesce");
    }

    fn step(&mut self, auto_fire: bool) -> bool {
        let mut progress = false;
        if self.io.is_due(self.env.now) {
            file_layer::expire(&mut self.io, self.env.now, &mut self.subs);
            progress = true;
        }
        if self.io.takes()
            && let Some(request) = self.below.pop()
        {
            file_layer::down(&mut self.io, self.env.now, request, &mut self.file_events, &mut self.subs);
            progress = true;
        }
        if !self.subs.is_empty() {
            self.sim.submit(self.pid, &mut self.subs);
            serve(&mut self.machine, &mut self.sim);
            progress = true;
        }
        self.sim.reap(self.pid, &mut self.completions);
        while let Some(complete) = self.completions.pop() {
            file_layer::up(&mut self.io, complete, &mut self.file_events, &mut self.subs);
            progress = true;
        }
        while let Some(event) = self.file_events.pop() {
            skein_kv::up(&mut self.store, &self.env, event, &mut self.above, &mut self.below);
            progress = true;
        }
        if auto_fire && self.io.takes() && self.below.is_empty() && skein_kv::is_ready(&self.store, &self.env.limits) {
            skein_kv::fire(&mut self.store, &self.env, &mut self.above, &mut self.below);
            progress = true;
        }
        progress
    }

    fn crash(&mut self, seed: u64) {
        self.machine.crash(seed);
        let handle = self.machine.reopen_root(0);
        self.sim = Sim::new(seed, Config::calm());
        self.pid = self.sim.spawn_process();
        self.root = self.sim.root(self.pid, Handle::new(handle.raw()));
        self.io = FileIo::new(8, self.env.limits.chunk, 1024, self.env.limits.deadline);
        self.store = Store::new(&self.env.limits);
        self.above = Queue::with_capacity(128);
        self.below = Queue::with_capacity(128);
        self.file_events = Queue::with_capacity(128);
        self.subs = Queue::with_capacity(128);
        self.completions = Queue::with_capacity(128);
    }

    fn reopen(&mut self) {
        self.store = Store::new(&self.env.limits);
        self.io = FileIo::new(8, self.env.limits.chunk, 1024, self.env.limits.deadline);
        self.request(Request::Open { owner: Token::new(1), root: self.root });
    }

    fn pop(&mut self) -> Event {
        self.above.pop().expect("one owner event")
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 0 { crc >> 1 } else { (crc >> 1) ^ 0x82f6_3b78 };
        }
    }
    !crc
}

fn frame(number: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut body = vec![1_u8];
    body.extend_from_slice(&u32::try_from(key.len()).expect("key length").to_be_bytes());
    body.extend_from_slice(key);
    body.extend_from_slice(&u32::try_from(value.len()).expect("value length").to_be_bytes());
    body.extend_from_slice(value);
    let mut bytes = b"SKVC".to_vec();
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(&number.to_be_bytes());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(body.len()).expect("body length").to_be_bytes());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    bytes
}

#[test]
fn torn_tail_repairs_before_new_commits() {
    let mut bytes = frame(1, b"a", b"one");
    bytes.extend_from_slice(b"SKV");
    let mut world = World::with_items(100_000, &[Item::file(b"log-00000000000000000001", &bytes)]);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 1 });
    world.request(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"b", b"two")]) });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 2 });
    world.request(Request::Close { owner: Token::new(3) });
    assert_eq!(world.pop(), Event::Closed { owner: Token::new(3) });
    world.reopen();
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 2 });
    world.request(Request::Get { owner: Token::new(4), key: Box::from(&b"a"[..]) });
    assert_eq!(world.pop(), Event::Got { owner: Token::new(4), value: Some(Box::from(&b"one"[..])) });
    world.request(Request::Get { owner: Token::new(5), key: Box::from(&b"b"[..]) });
    assert_eq!(world.pop(), Event::Got { owner: Token::new(5), value: Some(Box::from(&b"two"[..])) });
}

#[test]
fn rejected_recovery_file_is_closed() {
    let oversized = vec![0_u8; 12_289];
    let mut world = World::with_items(100_000, &[Item::file(b"log-00000000000000000001", &oversized)]);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::OpenFailed { owner: Token::new(1), failure: skein_kv::Failure::Full });
    assert_eq!(world.io.open_files(), 0);
}

#[test]
fn crash_at_each_commit_operation_keeps_whole_prefix() {
    for cut in 0..8_u64 {
        let mut world = World::new(100_000);
        world.request(Request::Open { owner: Token::new(1), root: world.root });
        assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
        world.request(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"a", b"one")]) });
        assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
        world.start(Request::Commit { owner: Token::new(3), ops: Box::new([put(b"b", b"two")]) });
        for _ in 0..cut {
            if !world.step(false) {
                break;
            }
        }
        let acknowledged =
            world.above.iter().any(|event| *event == Event::Committed { owner: Token::new(3), number: 2 });
        world.crash(cut);
        world.request(Request::Open { owner: Token::new(1), root: world.root });
        let Event::Opened { last, .. } = world.pop() else {
            panic!("recovered");
        };
        assert!(last == 1 || last == 2, "cut {cut}: recovered whole prefix");
        if acknowledged {
            assert_eq!(last, 2, "acknowledged commit survives at cut {cut}");
        }
        world.request(Request::Get { owner: Token::new(4), key: Box::from(&b"a"[..]) });
        assert_eq!(world.pop(), Event::Got { owner: Token::new(4), value: Some(Box::from(&b"one"[..])) });
        world.request(Request::Get { owner: Token::new(5), key: Box::from(&b"b"[..]) });
        let expected = if last == 2 { Some(Box::from(&b"two"[..])) } else { None };
        assert_eq!(world.pop(), Event::Got { owner: Token::new(5), value: expected });
    }
}

#[test]
fn crash_during_each_snapshot_step_replays_log() {
    for cut in 0..25_u64 {
        let mut world = World::new(32);
        world.request(Request::Open { owner: Token::new(1), root: world.root });
        assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
        world.request_no_fire(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"a", b"one")]) });
        assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
        for _ in 0..cut {
            if !world.step(true) {
                break;
            }
        }
        world.crash(cut);
        world.request(Request::Open { owner: Token::new(1), root: world.root });
        assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 1 }, "cut {cut}");
        world.request(Request::Get { owner: Token::new(3), key: Box::from(&b"a"[..]) });
        assert_eq!(world.pop(), Event::Got { owner: Token::new(3), value: Some(Box::from(&b"one"[..])) }, "cut {cut}");
    }
}

#[test]
fn short_writes_and_reads_are_continued() {
    let mut world = World::new(100_000);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    world.sim.set_faults(Faults { short_write: 1000, ..Faults::NONE });
    world.request(Request::Commit {
        owner: Token::new(2),
        ops: Box::new([put(b"key", b"a longer value to cut repeatedly")]),
    });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
    world.sim.set_faults(Faults::NONE);
    world.request(Request::Close { owner: Token::new(3) });
    assert_eq!(world.pop(), Event::Closed { owner: Token::new(3) });
    world.sim.set_faults(Faults { short_read: 1000, ..Faults::NONE });
    world.reopen();
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 1 });
    world.request(Request::Get { owner: Token::new(4), key: Box::from(&b"key"[..]) });
    assert_eq!(
        world.pop(),
        Event::Got { owner: Token::new(4), value: Some(Box::from(&b"a longer value to cut repeatedly"[..])) }
    );
}

#[test]
fn failed_sync_reports_uncertain_commit_then_recovers() {
    let mut world = World::new(100_000);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    world.start(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"a", b"one")]) });
    assert!(world.step(false), "write lands and starts sync");
    world.sim.set_faults(Faults { io_error: 1000, ..Faults::NONE });
    assert!(world.step(false), "sync fails");
    assert_eq!(world.pop(), Event::Failed { owner: Token::new(2), number: 1 });
    world.sim.set_faults(Faults::NONE);
    world.drive(true);
    let Event::Recovered { last } = world.pop() else {
        panic!("reopened after repair");
    };
    assert!(last == 0 || last == 1);
    world.crash(7);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last });
}

#[test]
fn hung_write_times_out_and_recovers() {
    let mut world = World::new(100_000);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    world.sim.set_faults(Faults { hung: 1000, ..Faults::NONE });
    world.start(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"key", b"value")]) });
    assert!(world.step(false), "write is submitted");
    assert!(world.above.is_empty(), "a hung write is not acknowledged");
    world.env.now = Time::from_nanos(world.env.limits.deadline.as_nanos());
    world.sim.set_faults(Faults::NONE);
    world.drive(true);
    assert_eq!(world.pop(), Event::Failed { owner: Token::new(2), number: 1 });
    assert_eq!(world.pop(), Event::Recovered { last: 0 });
}

#[test]
fn commits_queued_during_sync_share_the_next_barrier() {
    let mut world = World::new(100_000);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    world.start(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"a", b"1")]) });
    assert!(world.step(false), "first write completes");
    world.start(Request::Commit { owner: Token::new(3), ops: Box::new([put(b"b", b"2")]) });
    world.start(Request::Commit { owner: Token::new(4), ops: Box::new([put(b"c", b"3")]) });
    assert!(world.step(false), "first sync completes");
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
    let Some(file::Request::WriteAt { bytes, .. }) = world.below.iter().next() else {
        panic!("queued frames write together");
    };
    assert_eq!(bytes.windows(4).filter(|window| *window == b"SKVC").count(), 2, "one write has two frames");
    world.drive(false);
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(3), number: 2 });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(4), number: 3 });
}

#[test]
fn close_drains_commits_queued_during_sync() {
    let mut world = World::new(100_000);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    world.start(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"a", b"1")]) });
    assert!(world.step(false), "first write completes");
    world.start(Request::Commit { owner: Token::new(3), ops: Box::new([put(b"b", b"2")]) });
    world.start(Request::Close { owner: Token::new(4) });
    world.drive(true);
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(3), number: 2 });
    assert_eq!(world.pop(), Event::Closed { owner: Token::new(4) });
    world.reopen();
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 2 });
}

#[test]
fn commits_race_fuzzy_snapshot_chunks() {
    let mut world = World::new(32);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    let value = [b'x'; 60];
    world.request_no_fire(Request::Commit {
        owner: Token::new(2),
        ops: Box::new([put(b"a", &value), put(b"b", &value), put(b"c", &value), put(b"d", &value)]),
    });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
    world.fire_once(); // rollover and announce the segment from S=2
    world.fire_once(); // create the snapshot temporary
    world.fire_once(); // header
    world.fire_once(); // first bounded chunk; a and b are behind the cursor
    world.request_no_fire(Request::Commit {
        owner: Token::new(3),
        ops: Box::new([put(b"a", b"changed"), Op::Erase { key: Box::from(&b"c"[..]) }, put(b"e", b"new")]),
    });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(3), number: 2 });
    world.drive(true); // finish the fuzzy snapshot
    world.request(Request::Close { owner: Token::new(4) });
    assert_eq!(world.pop(), Event::Closed { owner: Token::new(4) });
    world.reopen();
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 2 });
    world.request(Request::Get { owner: Token::new(5), key: Box::from(&b"a"[..]) });
    assert_eq!(world.pop(), Event::Got { owner: Token::new(5), value: Some(Box::from(&b"changed"[..])) });
    world.request(Request::Get { owner: Token::new(6), key: Box::from(&b"c"[..]) });
    assert_eq!(world.pop(), Event::Got { owner: Token::new(6), value: None });
    world.request(Request::Get { owner: Token::new(7), key: Box::from(&b"e"[..]) });
    assert_eq!(world.pop(), Event::Got { owner: Token::new(7), value: Some(Box::from(&b"new"[..])) });
}

#[test]
fn commits_recover_after_close() {
    let mut world = World::new(100_000);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    world.request(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"a", b"one"), put(b"b", b"two")]) });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
    world.request(Request::Commit { owner: Token::new(3), ops: Box::new([put(b"a", b"new")]) });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(3), number: 2 });
    world.request(Request::Close { owner: Token::new(4) });
    assert_eq!(world.pop(), Event::Closed { owner: Token::new(4) });
    world.reopen();
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 2 });
    world.request(Request::Get { owner: Token::new(5), key: Box::from(&b"a"[..]) });
    assert_eq!(world.pop(), Event::Got { owner: Token::new(5), value: Some(Box::from(&b"new"[..])) });
    world.request(Request::Load {
        owner: Token::new(6),
        range: Range::prefix(b""),
        max: Page { rows: 16, bytes: 1024 },
    });
    let Event::Loaded { rows, next, .. } = world.pop() else {
        panic!("load answered");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(next, None);
}

#[test]
fn snapshot_installs_and_recovers() {
    let mut world = World::new(32);
    world.request(Request::Open { owner: Token::new(1), root: world.root });
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 0 });
    world.request(Request::Commit { owner: Token::new(2), ops: Box::new([put(b"a", b"one")]) });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(2), number: 1 });
    world.request(Request::Commit { owner: Token::new(3), ops: Box::new([put(b"b", b"two")]) });
    assert_eq!(world.pop(), Event::Committed { owner: Token::new(3), number: 2 });
    world.request(Request::Close { owner: Token::new(4) });
    assert_eq!(world.pop(), Event::Closed { owner: Token::new(4) });
    world.reopen();
    assert_eq!(world.pop(), Event::Opened { owner: Token::new(1), last: 2 });
    world.request(Request::Get { owner: Token::new(5), key: Box::from(&b"a"[..]) });
    assert_eq!(world.pop(), Event::Got { owner: Token::new(5), value: Some(Box::from(&b"one"[..])) });
}
