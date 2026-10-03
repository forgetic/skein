//! io's worst case against the counting allocator (programming-model.md,
//! 6.3; io.md, 3.4): io driven by hand, every call a step of the meter, to
//! its limits and back, and what it held of its own at a step's peak never
//! more than `worst_case(limits)`.
//!
//! The test plays the kernel and the owner, with storage it makes before the
//! meter's base, so that only io's heap is measured. What io submits stays
//! io's while the test holds it, as a buffer in flight is io's (io.md, 3.4);
//! what io tells up (the bytes it delivers) is handed out, and dropped before
//! the check.
//!
//! The fill: every socket connected, its intake full but for a receive in
//! flight, its output full with a send in flight and its queue of sends
//! full, room asked for and refused; the refusals held; half the sockets
//! closing gracefully, their deadlines armed. Then every socket aborted, and
//! every operation completed, until io holds nothing. And a listener's life:
//! sockets announced, rejected and bound until the slab is full, one
//! discarded for want of a slot, then everything closed.

use std::net::{Ipv4Addr, SocketAddr};

use skein_heap::{Counting, Meter};
use skein_io::kernel::{Complete, Done, Error, Fd, Op, Submit};
use skein_io::{Event, Io, Limits, Request, worst_case};
use skein_lib::stream::{Down, Read};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

#[global_allocator]
static HEAP: Counting = Counting;

/// io, and the storage of the test that drives it.
struct Driver {
    meter: Meter,
    bound: u64,
    limits: Limits,
    io: Option<Io>,
    env: Env<Limits>,
    up: Queue<Event>,
    subs: Queue<Submit>,
    /// What io submitted, held as the kernel holds it.
    flights: Vec<Submit>,
    /// io's tokens for its sockets, as `Connecting` told them.
    sockets: Vec<Token>,
    /// io's tokens for the sockets a listener announced.
    accepted: Vec<Token>,
    /// io's token for its listener, as `Listening` told it.
    listener: Option<Token>,
    next_fd: i32,
    /// The most io held of its own in a step.
    most: u64,
}

/// Room for every operation in flight, and every socket.
const ROOM: usize = 4096;

impl Driver {
    /// The driver's storage, made before the meter's base; then io, made in
    /// the first step.
    fn new(limits: Limits) -> Driver {
        let bound = worst_case(&limits).expect("the test's limits are priced");
        let mut driver = Driver {
            meter: Meter::new(),
            bound,
            limits,
            io: None,
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            up: Queue::with_capacity(64),
            subs: Queue::with_capacity(64),
            flights: Vec::with_capacity(ROOM),
            sockets: Vec::with_capacity(ROOM),
            accepted: Vec::with_capacity(ROOM),
            listener: None,
            next_fd: 3,
            most: 0,
        };
        driver.meter = Meter::new();
        driver.meter.start();
        driver.io = Some(Io::new(&limits));
        driver.end("new");
        driver
    }

    fn io(&mut self) -> &mut Io {
        self.io.as_mut().expect("made in the first step")
    }

    /// Ends a step: what io submitted is held, what it told is dropped as its
    /// receiver's, and what io held of its own is checked.
    fn end(&mut self, what: &str) {
        let measured = self.meter.end();
        let (mut connecting, mut accepted) = (None, None);
        while let Some(event) = self.up.pop() {
            match event {
                Event::Connecting { socket, .. } => connecting = Some(socket),
                Event::Accepted { socket, .. } => accepted = Some(socket),
                Event::Listening { listener, .. } => self.listener = Some(listener),
                other @ (Event::Connected { .. }
                | Event::Stream { .. }
                | Event::Failed { .. }
                | Event::Closed { .. }) => {
                    drop(other);
                }
            }
        }
        let own = self.meter.check(measured, self.bound, &(what, self.limits));
        self.most = self.most.max(own);
        while let Some(submit) = self.subs.pop() {
            assert!(self.flights.len() < ROOM, "room for every operation in flight");
            self.flights.push(submit);
        }
        self.sockets.extend(connecting);
        self.accepted.extend(accepted);
    }

    fn down(&mut self, request: Request) {
        self.meter.start();
        let io = self.io.as_mut().expect("made in the first step");
        skein_io::down(io, &self.env, request, &mut self.subs);
        self.end("down");
    }

    /// The reclaim point, then the ready list drained.
    fn next(&mut self) {
        self.meter.start();
        self.io().reclaim();
        self.end("reclaim");
        while self.io().is_ready() {
            self.meter.start();
            let io = self.io.as_mut().expect("made in the first step");
            skein_io::resume(io, &self.env, &mut self.up, &mut self.subs);
            self.end("resume");
        }
    }

    /// Completes the operation in flight at `at`, with what `answer` makes of
    /// it.
    fn complete(&mut self, at: usize, answer: fn(&mut Op, &mut i32) -> Result<Done, Error>) {
        let Submit { op, mut kind } = self.flights.remove(at);
        let result = answer(&mut kind, &mut self.next_fd);
        self.meter.start();
        let io = self.io.as_mut().expect("made in the first step");
        skein_io::up(io, &self.env, Complete { op, kind, result }, &mut self.up, &mut self.subs);
        self.end("up");
    }

    /// Completes, oldest first, every operation in flight that `wanted`
    /// matches; not those their completions submit, which go behind them.
    fn complete_all(&mut self, wanted: fn(&Op) -> bool, answer: fn(&mut Op, &mut i32) -> Result<Done, Error>) {
        let mut at = 0;
        let count = self.flights.len();
        for _ in 0..count {
            if wanted(&self.flights[at].kind) {
                self.complete(at, answer);
            } else {
                at += 1;
            }
        }
    }
}

fn is_socket(op: &Op) -> bool {
    matches!(op, Op::Socket { .. })
}

fn is_connect(op: &Op) -> bool {
    matches!(op, Op::Connect { .. })
}

fn is_recv(op: &Op) -> bool {
    matches!(op, Op::Recv { .. })
}

fn is_bind(op: &Op) -> bool {
    matches!(op, Op::Bind { .. })
}

fn is_listen(op: &Op) -> bool {
    matches!(op, Op::Listen { .. })
}

fn is_accept(op: &Op) -> bool {
    matches!(op, Op::Accept { .. })
}

fn is_close(op: &Op) -> bool {
    matches!(op, Op::Close { .. })
}

fn anything(_op: &Op) -> bool {
    true
}

/// A kernel that does what it is asked: a new descriptor, a connection, an
/// accepted socket, a receive that fills its buffer, a send that sends all
/// that was left, a close, a cancel that stops its target.
#[expect(clippy::unnecessary_wraps, reason = "an answer of the kernel's shape, as stopped gives")]
fn succeed(op: &mut Op, fd: &mut i32) -> Result<Done, Error> {
    match op {
        Op::Socket { .. } => {
            *fd += 1;
            Ok(Done::Fd(Fd::new(*fd)))
        }
        Op::Accept { .. } => {
            *fd += 1;
            Ok(Done::Accepted { fd: Fd::new(*fd), peer: SocketAddr::from((Ipv4Addr::LOCALHOST, 50000)) })
        }
        Op::Recv { buf, .. } => {
            buf.fill(7);
            Ok(Done::Count(u32::try_from(buf.len()).expect("a receive buffer's length")))
        }
        Op::Send { bytes, from, .. } => Ok(Done::Count(u32::try_from(bytes.len()).expect("a send's length") - *from)),
        Op::Bind { addr, .. } => Ok(Done::Bound(*addr)),
        Op::Listen { .. } | Op::Connect { .. } | Op::Shutdown { .. } | Op::Close { .. } | Op::Cancel { .. } => {
            Ok(Done::Nothing)
        }
    }
}

/// What a cancelled operation completes with: stopped, if it waits.
fn stopped(op: &mut Op, fd: &mut i32) -> Result<Done, Error> {
    match op {
        Op::Recv { .. } | Op::Send { .. } | Op::Connect { .. } | Op::Accept { .. } => Err(Error::Cancelled),
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Cancel { .. } => succeed(op, fd),
    }
}

/// Fills io to its limits, then winds it down; returns the most it held of
/// its own in a step, and its worst case.
fn fill_and_drain(limits: Limits) -> (u64, u64) {
    let mut driver = Driver::new(limits);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 80));
    // Every socket connects, and the refusals past them are held.
    for owner in 0..u64::from(limits.sockets + limits.refusals) {
        driver.down(Request::Connect { owner: Token::new(owner), addr });
    }
    driver.next();
    driver.complete_all(is_socket, succeed);
    driver.complete_all(is_connect, succeed);
    driver.next();
    // Each intake full but for one receive in flight.
    let fills = limits.intake / limits.receive - 1;
    for _ in 0..fills {
        driver.complete_all(is_recv, succeed);
    }
    // Each output full: a send in flight, and its queue of sends. The bytes
    // are made by the test, the owner, and io's once sent.
    let boxes = limits.sends + 1;
    let each = usize::try_from(limits.output / boxes).expect("a few bytes");
    let sockets = driver.sockets.len();
    for n in 0..sockets {
        let socket = driver.sockets[n];
        for _ in 0..boxes {
            let bytes = vec![1_u8; each].into_boxed_slice();
            driver.down(Request::Stream { stream: socket, down: Down::Send(bytes) });
        }
        // Read past what the intake holds, and room past what the output has:
        // both wait, on the ready list.
        let unmet = Down::Demand { read: Read::Fill(limits.intake), room: limits.output };
        driver.down(Request::Stream { stream: socket, down: unmet });
    }
    // Half of them closing gracefully, their deadlines armed.
    for n in (0..sockets).step_by(2) {
        let socket = driver.sockets[n];
        driver.down(Request::Close { entity: socket });
    }
    driver.next();
    // Every socket aborted, and every operation completed, cancels stopping
    // their targets, until io holds nothing.
    for n in 0..sockets {
        let socket = driver.sockets[n];
        driver.down(Request::Abort { entity: socket });
    }
    drain(&mut driver);
    (driver.most, driver.bound)
}

/// Completes every operation in flight, cancels stopping their targets,
/// until io holds nothing.
fn drain(driver: &mut Driver) {
    for _ in 0..64 {
        if driver.flights.is_empty() {
            break;
        }
        driver.complete_all(anything, stopped);
        driver.next();
    }
    assert!(driver.flights.is_empty(), "every operation completed");
    driver.next();
    assert!(driver.io().is_empty(), "io holds nothing, every socket closed");
}

/// A listener's life: its first socket rejected, the next bound until one
/// slot is left, which a connect takes while an accept is in flight, so that
/// the socket it accepts is discarded; then the listener and every socket
/// closed. Returns the most io held of its own in a step, and its worst
/// case.
fn listen_accept_and_discard(limits: Limits) -> (u64, u64) {
    assert!(limits.sockets >= 2, "a listener and the connect that takes the last slot");
    let mut driver = Driver::new(limits);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 80));
    driver.down(Request::Listen { owner: Token::new(0), addr });
    driver.complete_all(is_socket, succeed);
    driver.complete_all(is_bind, succeed);
    driver.complete_all(is_listen, succeed);
    // The first socket rejected, and closed.
    driver.complete_all(is_accept, succeed);
    let rejected = *driver.accepted.last().expect("a socket announced");
    driver.down(Request::Reject { socket: rejected });
    driver.complete_all(is_close, succeed);
    driver.next();
    // The next bound, until one slot is left.
    for n in 0..limits.sockets - 2 {
        driver.complete_all(is_accept, succeed);
        let socket = *driver.accepted.last().expect("a socket announced");
        driver.down(Request::Bind { socket, owner: Token::new(u64::from(n) + 1) });
        driver.next();
    }
    // A connect takes the last slot while an accept is in flight: the socket
    // it accepts has no slot, and is discarded.
    driver.down(Request::Connect { owner: Token::new(1000), addr });
    let announced = driver.accepted.len();
    driver.complete_all(is_accept, succeed);
    assert_eq!(driver.accepted.len(), announced, "no slot, so the socket is not announced");
    assert!(driver.flights.iter().any(|submit| is_close(&submit.kind)), "it is discarded");
    driver.complete_all(is_close, succeed);
    driver.next();
    // Everything closed: the listener's close, the sockets' aborts.
    for n in 0..driver.accepted.len() {
        let socket = driver.accepted[n];
        driver.down(Request::Abort { entity: socket });
    }
    for n in 0..driver.sockets.len() {
        let socket = driver.sockets[n];
        driver.down(Request::Abort { entity: socket });
    }
    driver.down(Request::Close { entity: driver.listener.expect("told Listening") });
    drain(&mut driver);
    (driver.most, driver.bound)
}

#[test]
fn io_never_holds_more_than_its_worst_case_filled_to_its_limits() {
    for limits in [
        Limits {
            sockets: 2,
            refusals: 1,
            intake: 16,
            receive: 4,
            output: 16,
            sends: 1,
            accepts: 1,
            backlog: 2,
            close_timeout: Duration::from_secs(1),
        },
        Limits {
            sockets: 8,
            refusals: 4,
            intake: 4096,
            receive: 1024,
            output: 8192,
            sends: 4,
            accepts: 2,
            backlog: 16,
            close_timeout: Duration::from_secs(1),
        },
        Limits {
            sockets: 33,
            refusals: 3,
            intake: 300,
            receive: 100,
            output: 600,
            sends: 2,
            accepts: 1,
            backlog: 8,
            close_timeout: Duration::from_secs(1),
        },
        // Receive buffers that dwarf the rest: a receive's buffer is freed
        // before the next is made, never both held at once.
        Limits {
            sockets: 2,
            refusals: 1,
            intake: 32768,
            receive: 16384,
            output: 64,
            sends: 1,
            accepts: 1,
            backlog: 2,
            close_timeout: Duration::from_secs(1),
        },
    ] {
        let (most, bound) = fill_and_drain(limits);
        assert!(most * 10 >= bound * 7, "{limits:?}: filled, io held {most} of its worst case of {bound}");
        let (most, bound) = listen_accept_and_discard(limits);
        assert!(most <= bound, "{limits:?}: a listener's life held {most} of {bound}");
    }
}
