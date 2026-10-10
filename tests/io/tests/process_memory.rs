//! The process path measured one io step at a time against `worst_case`.
//! One child consumes every entity slot with its pipes. Its spawn completion
//! temporarily holds the child pipe IDs, a clone of them, result descriptors,
//! and the announced tokens. The next pass arms a read on every pipe.

use skein_heap::{Counting, Meter};
use skein_io::kernel::{Complete, Done, Exit, Fd, Op, Pipe, Spawn, Submit, Way};
use skein_io::{Event, Io, Limits, Request, worst_case};
use skein_lib::stream::{OutputDown, OutputOutcome, OutputUp};
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
    output_events: Vec<(Token, Token, OutputOutcome)>,
    child_owner: Option<Token>,
    pipe_closed: [bool; PIPES],
    exited: bool,
    child_closed: bool,
}

impl Driver {
    fn new(limits: Limits) -> Self {
        // Make the observer's storage before the meter's base. The first
        // measured step constructs io and all its bounded tables.
        let events = Queue::with_capacity(16);
        let submissions = Queue::with_capacity(16);
        let flights = Vec::with_capacity(16);
        let output_events = Vec::with_capacity(16);
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
            output_events,
            child_owner: None,
            pipe_closed: [false; PIPES],
            exited: false,
            child_closed: false,
        }
    }

    fn end(&mut self, what: &'static str) {
        let measured = self.meter.end();
        while let Some(event) = self.events.pop() {
            match event {
                Event::Spawned { owner, child, pipes } => {
                    self.child_owner = Some(owner);
                    assert_eq!(pipes.len(), PIPES);
                    self.child = Some(child);
                    for (slot, token) in self.pipes.iter_mut().zip(pipes.iter()) {
                        *slot = Some(*token);
                    }
                }
                Event::Output { owner, up: OutputUp::Settled { right, outcome } } => {
                    assert!(self.output_events.len() < self.output_events.capacity());
                    self.output_events.push((owner, right, outcome));
                }
                Event::Exited { owner, exit } => {
                    assert_eq!(self.child_owner, Some(owner));
                    assert_eq!(exit, Exit::Code(0));
                    assert!(!self.exited);
                    self.exited = true;
                }
                Event::Closed { owner } => {
                    if self.child_owner == Some(owner) {
                        assert!(self.exited && self.pipe_closed.iter().all(|closed| *closed));
                        assert!(!self.child_closed);
                        self.child_closed = true;
                    } else {
                        let index = self.pipes.iter().position(|pipe| *pipe == Some(owner)).expect("actual pipe owner");
                        assert!(!self.pipe_closed[index]);
                        self.pipe_closed[index] = true;
                    }
                }
                Event::Failed { .. }
                | Event::Stream { .. }
                | Event::Listening { .. }
                | Event::Accepted { .. }
                | Event::Connecting { .. }
                | Event::Connected { .. }
                | Event::Usage { .. }
                | Event::Shutdown { .. } => panic!("unexpected event in {what}: {event:?}"),
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
        let Submit { op, mut kind } = self.flights.remove(0);
        let Op::Spawn { spawn } = &mut kind else { panic!("spawn record") };
        for (index, pipe) in spawn.pipes.iter_mut().enumerate() {
            pipe.parent = Some(Fd::new(41 + i32::try_from(index).expect("small pipe count")));
        }
        let result = Done::Spawned { pidfd: Fd::new(40) };
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

    fn complete(&mut self, at: usize, result: Done) {
        let Submit { op, kind } = self.flights.remove(at);
        self.meter.start();
        skein_io::up(
            &mut self.io,
            &self.env,
            Complete { op, kind, result: Ok(result) },
            &mut self.events,
            &mut self.submissions,
        );
        self.end("native pipe completion");
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
    let pipes: Box<[Pipe]> = (0..PIPES)
        .map(|i| Pipe { child: u32::try_from(i + 3).expect("small pipe count"), way: Way::Out, parent: None })
        .collect();
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

#[test]
fn native_pipe_output_and_staged_close_terminals_fit_the_same_checked_bound_until_real_reap() {
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
    let pipes: Box<[Pipe]> = (0..PIPES)
        .map(|index| Pipe { child: u32::try_from(index).expect("bounded pipe count"), way: Way::In, parent: None })
        .collect();
    let spawn = Spawn {
        program: Box::from(&b"/bin/cat"[..]),
        args: Box::default(),
        env: Box::default(),
        root: Fd::new(3),
        dir: Box::from(&b"."[..]),
        pipes,
    };
    let mut driver = Driver::new(limits);
    driver.down(Request::Spawn { owner: Token::new(1001), spawn });
    driver.complete_spawn();
    driver.arm_pipes();
    for index in 0..PIPES {
        let pipe = driver.pipes[index].expect("actual writable pipe");
        let right = Token::new(1);
        let before = driver.output_events.len();
        driver.down(Request::Output { stream: pipe, down: OutputDown::Room { right, bytes: limits.output } });
        driver.arm_pipes();
        assert_eq!(
            driver.output_events.get(before..).expect("bounded terminal suffix"),
            &[(pipe, right, OutputOutcome::Granted)]
        );
        let bytes = vec![7; usize::try_from(limits.output).expect("original maximal pipe payload")].into_boxed_slice();
        driver.down(Request::Output { stream: pipe, down: OutputDown::Send { right, bytes } });
        let before = driver.output_events.len();
        driver.down(Request::Output { stream: pipe, down: OutputDown::Room { right: Token::new(2), bytes: 1 } });
        driver.arm_pipes();
        assert_eq!(driver.output_events.len(), before, "one real maximal pipe write fills the byte cap");
    }
    let wait =
        driver.flights.iter().position(|submit| matches!(submit.kind, Op::Wait { .. })).expect("actual child Wait");
    driver.complete(wait, Done::Exit(Exit::Code(0)));
    assert!(driver.exited && !driver.child_closed);
    for index in 0..PIPES {
        let pipe = driver.pipes[index].expect("actual writable pipe");
        let before = driver.output_events.len();
        driver.down(Request::Close { entity: pipe });
        driver.arm_pipes();
        assert_eq!(
            driver.output_events.get(before..).expect("bounded terminal suffix"),
            &[(pipe, Token::new(2), OutputOutcome::Cancelled)]
        );
        assert!(!driver.pipe_closed[index]);
    }
    for _ in 0..PIPES {
        let write = driver
            .flights
            .iter()
            .position(|submit| matches!(submit.kind, Op::PipeWrite { .. }))
            .expect("each actual maximal pipe write");
        driver.complete(write, Done::Count(limits.output));
    }
    for _ in 0..PIPES {
        let close =
            driver.flights.iter().position(|submit| matches!(submit.kind, Op::Close { .. })).expect("a pipe close");
        driver.complete(close, Done::Nothing);
    }
    let killing = driver
        .flights
        .iter()
        .position(|submit| matches!(submit.kind, Op::Signal { .. }))
        .expect("the group's closing kill");
    driver.complete(killing, Done::Nothing);
    let reap = driver
        .flights
        .iter()
        .position(|submit| matches!(submit.kind, Op::Wait { reap: true, .. }))
        .expect("the final child reap");
    driver.complete(reap, Done::Exit(Exit::Code(0)));
    for _ in 0..1 {
        let close = driver
            .flights
            .iter()
            .position(|submit| matches!(submit.kind, Op::Close { .. }))
            .expect("each actual pipe or child Close");
        driver.complete(close, Done::Nothing);
    }
    driver.arm_pipes();
    assert!(driver.child_closed && driver.pipe_closed.iter().all(|closed| *closed));
    assert!(driver.flights.is_empty() && driver.io.is_empty());
    assert!(driver.most > 0 && driver.most <= driver.bound);
}

#[test]
fn a_slab_full_of_children_and_pipes_with_each_signal_in_flight_fits_the_bound() {
    use skein_io::kernel::{Signal, Target};
    let limits = Limits { sockets: 8, ..skein_io_world::scenarios::limits(8) };
    let bound = worst_case(&limits).expect("small limits");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut events = Queue::with_capacity(32);
    let mut submissions = Queue::with_capacity(32);
    let mut flights = Vec::with_capacity(32);
    let meter = Meter::new();
    meter.start();
    let mut io = Io::new(&limits);
    let mut most = meter.check(meter.end(), bound, &"new");
    for index in 0..4_u32 {
        let request = Request::Spawn {
            owner: Token::new(u64::from(index)),
            spawn: Spawn {
                root: Fd::new(3),
                dir: Box::from(b".".as_slice()),
                program: Box::from(b"fork-live".as_slice()),
                args: Box::new([]),
                env: Box::new([]),
                pipes: Box::from([Pipe { child: 1, way: Way::Out, parent: None }]),
            },
        };
        meter.start();
        skein_io::down(&mut io, &env, request, &mut submissions);
        most = most.max(meter.check(meter.end(), bound, &"spawn"));
        let Submit { op, mut kind } = submissions.pop().expect("a spawn");
        let Op::Spawn { spawn } = &mut kind else { panic!("a spawn") };
        spawn.pipes[0].parent = Some(Fd::new(41 + i32::try_from(index * 2).expect("four children")));
        meter.start();
        skein_io::up(
            &mut io,
            &env,
            Complete {
                op,
                kind,
                result: Ok(Done::Spawned { pidfd: Fd::new(40 + i32::try_from(index * 2).expect("four children")) }),
            },
            &mut events,
            &mut submissions,
        );
        most = most.max(meter.check(meter.end(), bound, &"spawned"));
        let Event::Spawned { child, pipes, .. } = events.pop().expect("the spawn event") else { panic!("spawned") };
        let pipe = pipes[0];
        // Hand back the temporary event allocation before metering the next call.
        drop(pipes);
        meter.start();
        skein_io::down(
            &mut io,
            &env,
            Request::Stream {
                stream: pipe,
                down: skein_lib::stream::Down::Demand { read: skein_lib::stream::Read::Fill(1), room: 0 },
            },
            &mut submissions,
        );
        most = most.max(meter.check(meter.end(), bound, &"demand"));
        meter.start();
        skein_io::down(
            &mut io,
            &env,
            Request::Signal { child, signal: Signal::Terminate, to: Target::Group },
            &mut submissions,
        );
        most = most.max(meter.check(meter.end(), bound, &"signal"));
        while let Some(submit) = submissions.pop() {
            flights.push(submit);
        }
    }
    meter.start();
    io.reclaim();
    most = most.max(meter.check(meter.end(), bound, &"reclaim"));
    while io.is_ready() {
        meter.start();
        skein_io::resume(&mut io, &env, &mut events, &mut submissions);
        most = most.max(meter.check(meter.end(), bound, &"pipe resume"));
        while let Some(submit) = submissions.pop() {
            flights.push(submit);
        }
    }
    meter.start();
    skein_io::down(&mut io, &env, Request::Usage { owner: Token::new(99) }, &mut submissions);
    most = most.max(meter.check(meter.end(), bound, &"process usage beside full entity slab"));
    while let Some(submit) = submissions.pop() {
        flights.push(submit);
    }
    assert_eq!(flights.iter().filter(|submit| matches!(submit.kind, Op::Usage)).count(), 1);
    assert_eq!(io.sockets(), limits.sockets, "every child and pipe occupies an actual entity slot");
    assert_eq!(flights.iter().filter(|submit| matches!(submit.kind, Op::Signal { .. })).count(), 4);
    assert_eq!(flights.iter().filter(|submit| matches!(submit.kind, Op::Wait { .. })).count(), 4);
    assert_eq!(flights.iter().filter(|submit| matches!(submit.kind, Op::PipeRead { .. })).count(), 4);
    assert!(most > 0 && most <= bound);
}
