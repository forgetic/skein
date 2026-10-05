//! The process path measured one io step at a time against `worst_case`.
//! One child consumes every entity slot with its pipes. Its spawn completion
//! temporarily holds the child pipe IDs, a clone of them, result descriptors,
//! and the announced tokens. The next pass arms a read on every pipe.

use skein_heap::{Counting, Meter};
use skein_io::kernel::{Complete, Done, Fd, Op, Pipe, Spawn, Submit, Way};
use skein_io::{Event, Io, Limits, Request, worst_case};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

#[global_allocator]
static HEAP: Counting = Counting;

const PIPES: usize = 7;

struct Driver {
    meter: Meter,
    bound: u64,
    most: u64,
    io: Io,
    env: Env<Limits>,
    events: Queue<Event>,
    submissions: Queue<Submit>,
    flights: Vec<Submit>,
    child: Option<Token>,
    pipes: [Option<Token>; PIPES],
}

impl Driver {
    fn new(limits: Limits) -> Self {
        // Make the observer's storage before the meter's base. The first
        // measured step constructs io and all its bounded tables.
        let events = Queue::with_capacity(16);
        let submissions = Queue::with_capacity(16);
        let flights = Vec::with_capacity(16);
        let meter = Meter::new();
        meter.start();
        let io = Io::new(&limits);
        let measured = meter.end();
        let bound = worst_case(&limits).expect("limits fit the memory bound");
        let most = meter.check(measured, bound, &"new");
        Self {
            meter,
            bound,
            most,
            io,
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            events,
            submissions,
            flights,
            child: None,
            pipes: [None; PIPES],
        }
    }

    fn end(&mut self, what: &'static str) {
        let measured = self.meter.end();
        while let Some(event) = self.events.pop() {
            match event {
                Event::Spawned { child, pipes, .. } => {
                    assert_eq!(pipes.len(), PIPES);
                    self.child = Some(child);
                    for (slot, token) in self.pipes.iter_mut().zip(pipes.iter()) {
                        *slot = Some(*token);
                    }
                }
                Event::Failed { .. }
                | Event::Closed { .. }
                | Event::Exited { .. }
                | Event::Stream { .. }
                | Event::Listening { .. }
                | Event::Accepted { .. }
                | Event::Connecting { .. }
                | Event::Connected { .. } => panic!("unexpected event in {what}: {event:?}"),
            }
        }
        self.most = self.most.max(self.meter.check(measured, self.bound, &what));
        while let Some(submit) = self.submissions.pop() {
            assert!(self.flights.len() < self.flights.capacity());
            self.flights.push(submit);
        }
    }

    fn down(&mut self, request: Request) {
        self.meter.start();
        skein_io::down(&mut self.io, &self.env, request, &mut self.submissions);
        self.end("down");
    }

    fn complete_spawn(&mut self) {
        let Submit { op, kind } = self.flights.remove(0);
        assert!(matches!(kind, Op::Spawn { .. }));
        let result = Done::Spawned {
            pidfd: Fd::new(40),
            pipes: (0..PIPES).map(|i| Fd::new(41 + i32::try_from(i).expect("small pipe count"))).collect(),
        };
        self.meter.start();
        skein_io::up(
            &mut self.io,
            &self.env,
            Complete { op, kind, result: Ok(result) },
            &mut self.events,
            &mut self.submissions,
        );
        self.end("spawn completion");
    }

    fn arm_pipes(&mut self) {
        self.meter.start();
        self.io.reclaim();
        self.end("reclaim");
        while self.io.is_ready() {
            self.meter.start();
            skein_io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            self.end("pipe resume");
        }
    }
}

#[test]
fn a_child_with_pipes_stays_within_ios_memory_bound() {
    let limits = Limits {
        sockets: 8,
        refusals: 1,
        intake: 64,
        receive: 32,
        output: 64,
        sends: 2,
        accepts: 1,
        backlog: 1,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    };
    let pipes: Box<[Pipe]> =
        (0..PIPES).map(|i| Pipe { child: u32::try_from(i + 3).expect("small pipe count"), way: Way::Out }).collect();
    let spawn = Spawn {
        program: Box::from(&b"/bin/true"[..]),
        args: Box::default(),
        env: Box::default(),
        root: Fd::new(3),
        dir: Box::from(&b"."[..]),
        pipes,
    };
    let mut driver = Driver::new(limits);
    driver.down(Request::Spawn { owner: Token::new(1), spawn });
    assert_eq!(driver.io.sockets(), limits.sockets, "child and pipes fill the entity slab");
    driver.complete_spawn();
    assert!(driver.child.is_some());
    assert!(driver.pipes.iter().all(Option::is_some));
    driver.arm_pipes();
    assert_eq!(driver.flights.iter().filter(|submit| matches!(submit.kind, Op::PipeRead { .. })).count(), PIPES);
    assert!(driver.most > 0);
}
