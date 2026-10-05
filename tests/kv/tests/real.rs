//! The store through the real `io_uring` adapter and a scratch directory.

use skein_io::file;
use skein_io::file_layer::{self, FileIo};
use skein_io::kernel::{Complete, Submit};
use skein_kv::{Event, Limits, Op, Page, Request, Store};
use skein_lib::{Duration, Env, Queue, Time, Token};
use skein_scratch::Scratch;
use skein_shell::{Clock, Config, Kernel, Wait, open_root};

fn limits() -> Limits {
    Limits {
        key: 64,
        value: 64,
        ops: 8,
        commit: 1024,
        queued: 8,
        queued_bytes: 8192,
        budget: 8192,
        segment: 4096,
        snapshot_after: 4096,
        chunk: 256,
        page: Page { rows: 16, bytes: 1024 },
        deadline: Duration::from_secs(5),
    }
}

struct World {
    kernel: Kernel,
    clock: Clock,
    io: FileIo,
    store: Store,
    limits: Limits,
    above: Queue<Event>,
    below: Queue<file::Request>,
    file_events: Queue<file::Event>,
    subs: Queue<Submit>,
    completes: Queue<Complete>,
}

impl World {
    fn new() -> Self {
        let limits = limits();
        Self {
            kernel: Kernel::open(Config { operations: 32 }).expect("io_uring is available"),
            clock: Clock::new(),
            io: FileIo::new(8, limits.chunk, 1024, limits.deadline),
            store: Store::new(&limits),
            limits,
            above: Queue::with_capacity(128),
            below: Queue::with_capacity(128),
            file_events: Queue::with_capacity(128),
            subs: Queue::with_capacity(128),
            completes: Queue::with_capacity(128),
        }
    }

    fn env(&self) -> Env<Limits> {
        let now = self.clock.now();
        Env { now: now.now, wall: now.wall, limits: self.limits }
    }

    fn ask(&mut self, request: Request) -> Event {
        let env = self.env();
        skein_kv::down(&mut self.store, &env, request, &mut self.above, &mut self.below);
        let until = self.clock.now().now.checked_add(Duration::from_secs(5)).expect("deadline");
        for _ in 0..10_000 {
            if let Some(event) = self.above.pop() {
                return event;
            }
            assert!(self.clock.now().now < until, "the store answered in time");
            self.step(until);
        }
        panic!("store did not answer before the step limit");
    }

    fn step(&mut self, until: Time) {
        if self.io.is_due(self.clock.now().now) {
            file_layer::expire(&mut self.io, self.clock.now().now, &mut self.subs);
        }
        if self.io.takes()
            && let Some(request) = self.below.pop()
        {
            file_layer::down(&mut self.io, self.clock.now().now, request, &mut self.file_events, &mut self.subs);
        }
        if !self.subs.is_empty() {
            self.kernel.submit(&mut self.subs, Wait::No);
            assert!(self.subs.is_empty(), "the ring has room");
        }
        self.kernel.reap(&mut self.completes);
        if self.completes.is_empty() && self.file_events.is_empty() && !self.io.takes() {
            assert!(self.clock.now().now < until, "the file request completed in time");
            self.kernel.submit(&mut self.subs, Wait::Until(until));
            self.kernel.reap(&mut self.completes);
        }
        while let Some(complete) = self.completes.pop() {
            file_layer::up(&mut self.io, complete, &mut self.file_events, &mut self.subs);
        }
        while let Some(event) = self.file_events.pop() {
            let env = self.env();
            skein_kv::up(&mut self.store, &env, event, &mut self.above, &mut self.below);
        }
        if self.io.takes() && self.below.is_empty() && skein_kv::is_ready(&self.store, &self.limits) {
            let env = self.env();
            skein_kv::fire(&mut self.store, &env, &mut self.above, &mut self.below);
        }
    }

    fn reset(&mut self) {
        self.io = FileIo::new(8, self.limits.chunk, 1024, self.limits.deadline);
        self.store = Store::new(&self.limits);
    }
}

#[test]
fn commit_and_recover_through_real_ring() {
    let scratch = Scratch::new("kv");
    let mut world = World::new();
    let root = open_root(scratch.path()).expect("scratch directory opens");
    assert_eq!(
        world.ask(Request::Open { owner: Token::new(1), root }),
        Event::Opened { owner: Token::new(1), last: 0 }
    );
    let ops = Box::new([Op::Put { key: Box::from(&b"key"[..]), value: Box::from(&b"value"[..]) }]);
    assert_eq!(
        world.ask(Request::Commit { owner: Token::new(2), ops }),
        Event::Committed { owner: Token::new(2), number: 1 }
    );
    assert_eq!(world.ask(Request::Close { owner: Token::new(3) }), Event::Closed { owner: Token::new(3) });
    world.reset();
    assert_eq!(
        world.ask(Request::Open { owner: Token::new(4), root }),
        Event::Opened { owner: Token::new(4), last: 1 }
    );
    assert_eq!(
        world.ask(Request::Get { owner: Token::new(5), key: Box::from(&b"key"[..]) }),
        Event::Got { owner: Token::new(5), value: Some(Box::from(&b"value"[..])) }
    );
    assert_eq!(world.ask(Request::Close { owner: Token::new(6) }), Event::Closed { owner: Token::new(6) });
}
