//! The echo example's service (examples.md, 3): its [`iterate`], the pure
//! function its shell runs once per turn of the loop and the simulator's
//! worlds drive (programming-model.md, section 2); its [`Limits`], checked at
//! startup; and [`worst_case`], the sum of its layers' worst cases.
//!
//! # The loop
//!
//! The shell reaps completions into [`Service::completions`], reads the
//! clock, calls [`iterate`], and submits [`Service::submissions`], waiting
//! only when [`Service::work_pending`] says there is nothing to do, until
//! [`Service::next_deadline`]. `iterate` runs both passes and the reclaim
//! point:
//!
//! ```text
//! completions -> io -> protocol -> domain      (up pass)
//! submissions <- io <- protocol <- domain      (down pass)
//! ```
//!
//! Each stage drains its layer's ready list, then takes its input, then
//! fires its deadlines once every input is taken, so that progress that
//! came wins over a deadline that passed while the loop waited. The price:
//! under a load that leaves input over in every iteration, deadlines wait
//! for it to ease, as one that cut in could close a connection whose
//! progress sits untaken in the queue. Before each
//! call it reserves the call's `MAX_OUT` in its output queues, and after it
//! asserts that the call emitted no more: the loop is the service's own, so
//! it checks the room it reserves. What does not fit waits for the next
//! iteration.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(test)]
mod tests;

use domain::Domain;
use protocol::Protocol;
// The domain's and the protocol layer's crates, so that the shell and the
// worlds above the service name their limits through it: neither depends on
// them, as neither does in the role graph (programming-model.md, 4; README.md),
// though both name io's and lib's, which every role may.
pub use skein_echo_domain as domain;
pub use skein_echo_protocol as protocol;
use skein_io::kernel::{Addr, Complete, Fd, Submit};
use skein_io::{self as io, Io};
use skein_lib::{Env, Queue, Time, Token, Wall};

/// A service binding distinct from the protocol's listener and connections.
const SIGNALS: Token = Token::new(u64::MAX - 1);

/// The limits of every layer, and of the queues between them: the
/// configuration a shell reads (programming-model.md, 7).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub io: io::Limits,
    pub protocol: protocol::Limits,
    pub domain: domain::Limits,
    /// The capacity of each queue between the stages, completions and
    /// submissions included: at least [`Limits::LEAST_QUEUE`], the largest
    /// `MAX_OUT`.
    pub queue: u32,
}

/// Why the service cannot run under some limits: startup refuses them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Unusable {
    /// io's limits are not usable (`skein_io::Limits::is_usable`).
    Io,
    /// The protocol layer's are not (`skein_echo_protocol::Limits::is_usable`).
    Protocol,
    /// A queue too small for the largest `MAX_OUT`.
    Queue,
    /// The protocol layer's largest read is past io's intake, so it could
    /// never be met (io.md, 2).
    Read { largest: u32, intake: u32 },
    /// The protocol layer's largest room is past io's output cap.
    Room { largest: u32, output: u32 },
}

impl Limits {
    /// The smallest queue the loop can reserve every stage's `MAX_OUT` in:
    /// the maximum of IO's events/submissions, the protocol's events/requests,
    /// and the domain's requests. IO's independent terminal can accompany
    /// classic Bytes and End, so the current minimum is three (io.md, 3.3).
    pub const LEAST_QUEUE: u32 = {
        let io_down = larger(io::MAX_OUT_DOWN.events, io::MAX_OUT_DOWN.submissions);
        let io_up = larger(io::MAX_OUT_UP.events, io::MAX_OUT_UP.submissions);
        let io_resume = larger(io::MAX_OUT_RESUME.events, io::MAX_OUT_RESUME.submissions);
        let io_fire = larger(io::MAX_OUT_FIRE.events, io::MAX_OUT_FIRE.submissions);
        let io_maximum = larger(larger(io_down, io_up), larger(io_resume, io_fire));
        let protocol_up = larger(protocol::MAX_OUT_UP.events, protocol::MAX_OUT_UP.requests);
        let protocol_resume = larger(protocol::MAX_OUT_RESUME.events, protocol::MAX_OUT_RESUME.requests);
        let protocol_fire = larger(protocol::MAX_OUT_FIRE.events, protocol::MAX_OUT_FIRE.requests);
        let protocol_maximum =
            larger(larger(protocol_up, protocol_resume), larger(protocol_fire, protocol::MAX_OUT_DOWN));
        larger(io_maximum, larger(protocol_maximum, domain::MAX_OUT))
    };

    /// Whether the service can run under these limits: each layer's usable,
    /// queues that hold the largest `MAX_OUT`, and io's caps no smaller than
    /// the protocol layer's largest demands (io.md, 2).
    pub fn check(&self) -> Result<(), Unusable> {
        if !self.io.is_usable() {
            return Err(Unusable::Io);
        }
        if !self.protocol.is_usable() {
            return Err(Unusable::Protocol);
        }
        if self.queue < Limits::LEAST_QUEUE {
            return Err(Unusable::Queue);
        }
        let (read, room) = (self.protocol.largest_read(), self.protocol.largest_room());
        if read > self.io.largest_read() {
            return Err(Unusable::Read { largest: read, intake: self.io.largest_read() });
        }
        if room > self.io.largest_room() {
            return Err(Unusable::Room { largest: room, output: self.io.largest_room() });
        }
        Ok(())
    }
}

const fn larger(left: u32, right: u32) -> u32 {
    if left > right { left } else { right }
}

/// The most heap the service holds under `limits`, or `None` past a `u64`
/// (programming-model.md, 6.3): each layer's worst case, and the containers
/// of the queues between them. What the queues' records carry is counted by
/// the layer it belongs to: a buffer in flight by io, a line by the protocol
/// layer.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let queue = limits.queue;
    let queues = Queue::<Complete>::worst_case(queue)?
        .checked_add(Queue::<Submit>::worst_case(queue)?)?
        .checked_add(Queue::<io::Event>::worst_case(queue)?)?
        .checked_add(Queue::<io::Request>::worst_case(queue)?)?
        .checked_add(Queue::<domain::Event>::worst_case(queue)?)?
        .checked_add(Queue::<domain::Request>::worst_case(queue)?)?;
    io::worst_case(&limits.io)?
        .checked_add(protocol::worst_case(&limits.protocol)?)?
        .checked_add(domain::worst_case(&limits.domain)?)?
        .checked_add(queues)
}

/// The most operations the service has in flight at once: the size of its
/// ring (shell.md, 3), or `None` past a `u32`.
#[must_use]
pub fn operations(limits: &Limits) -> Option<u32> {
    io::operations(&limits.io)
}

/// The service's state: each layer, its environment, and the queues between
/// the stages.
#[derive(Debug)]
pub struct Service {
    io: Io,
    io_env: Env<io::Limits>,
    protocol: Protocol,
    protocol_env: Env<protocol::Limits>,
    domain: Domain,
    domain_env: Env<domain::Limits>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    /// What io told, for the protocol layer.
    told: Queue<io::Event>,
    /// What the protocol layer asked of io.
    asked: Queue<io::Request>,
    /// The protocol layer's calls and events, for the domain.
    calls: Queue<domain::Event>,
    /// The domain's replies and requests, for the protocol layer.
    answers: Queue<domain::Request>,
    /// The adopted signal source stays named until io reports its close.
    signals: Option<Token>,
    signals_closing: bool,
}

impl Service {
    /// The service under `limits`, which must pass [`Limits::check`], to
    /// listen at `addr`; `seed` is the random state of its layers.
    #[must_use]
    pub fn new(limits: &Limits, addr: Addr, seed: u64) -> Service {
        assert!(limits.check().is_ok(), "the service runs only under limits that pass their check");
        let start = Time::ZERO;
        Service {
            io: Io::new(&limits.io),
            io_env: Env { now: start, wall: Wall::EPOCH, limits: limits.io },
            protocol: Protocol::new(&limits.protocol, addr, seed),
            protocol_env: Env { now: start, wall: Wall::EPOCH, limits: limits.protocol },
            domain: Domain::new(&limits.domain),
            domain_env: Env { now: start, wall: Wall::EPOCH, limits: limits.domain },
            completions: Queue::with_capacity(limits.queue),
            submissions: Queue::with_capacity(limits.queue),
            told: Queue::with_capacity(limits.queue),
            asked: Queue::with_capacity(limits.queue),
            calls: Queue::with_capacity(limits.queue),
            answers: Queue::with_capacity(limits.queue),
            signals: None,
            signals_closing: false,
        }
    }

    /// Adopts the shell's blocked termination signalfd at startup (io.md,
    /// section 7), retaining ownership until every connection settles. If
    /// there is no io slot or a source was already adopted, the caller keeps it.
    pub fn adopt_signals(&mut self, descriptor: Fd) -> Result<(), Fd> {
        if self.signals.is_some() {
            return Err(descriptor);
        }
        match self.io.adopt_signals_for(descriptor, SIGNALS) {
            Ok(source) => {
                self.signals = Some(source);
                Ok(())
            }
            Err(descriptor) => Err(descriptor),
        }
    }

    /// Where the kernel's completions go, for `iterate` to take.
    pub const fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }

    /// What `iterate` asked of the kernel, for the shell to submit.
    pub const fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }

    /// Whether `iterate` has work at `now` without the kernel: a ready list,
    /// a deadline due, or a queue between the stages not empty. The loop
    /// blocks only when there is none.
    #[must_use]
    pub fn work_pending(&self, now: Time) -> bool {
        (self.protocol.is_empty() && !self.signals_closing && self.signals.is_some())
            || self.io.is_ready()
            || self.io.is_due(now)
            || self.protocol.is_ready()
            || self.protocol.is_due(now)
            || !self.completions.is_empty()
            || !self.told.is_empty()
            || !self.asked.is_empty()
            || !self.calls.is_empty()
            || !self.answers.is_empty()
    }

    /// The earliest deadline over every layer: io's and the protocol layer's
    /// (programming-model.md, 9). The domain keeps none.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let protocol = self.protocol.next_deadline();
        match self.io.next_deadline() {
            Some(io) => match protocol {
                Some(protocol) => Some(io.min(protocol)),
                None => Some(io),
            },
            None => protocol,
        }
    }

    /// The protocol layer's idle and listen-backoff deadlines, excluding io.
    #[must_use]
    pub fn next_policy_deadline(&self) -> Option<Time> {
        self.protocol.next_deadline()
    }

    /// Shuts the service down: the domain is told `Shutdown` in the next
    /// iteration, admits no one more and stops the listener; the connections
    /// it has run to their end. Lower-tier worlds use this entry point; the
    /// binary receives io's real `Shutdown` event (io.md, section 7).
    pub fn shutdown(&mut self) {
        self.protocol.shutdown();
    }

    /// The address the service listens at, once it does.
    #[must_use]
    pub const fn listening(&self) -> Option<Addr> {
        self.protocol.listening()
    }

    /// Why the listener stopped, if it failed.
    #[must_use]
    pub const fn failure(&self) -> Option<io::Error> {
        self.protocol.failure()
    }

    /// What refused the listen, while the listener waits to ask again: a
    /// shortage, not a failure.
    #[must_use]
    pub const fn retrying(&self) -> Option<io::Error> {
        self.protocol.retrying()
    }

    /// Whether the service holds nothing: its listener closed, every slab
    /// empty, nothing in flight, every queue empty. Once it is, it will do
    /// nothing more.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.io.is_empty()
            && self.protocol.is_empty()
            && self.domain.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.told.is_empty()
            && self.asked.is_empty()
            && self.calls.is_empty()
            && self.answers.is_empty()
    }
}

/// One iteration: both passes and the reclaim point (programming-model.md,
/// section 2), at `now` and `wall`, read once by the shell or the simulator.
pub fn iterate(svc: &mut Service, now: Time, wall: Wall) {
    svc.io_env.now = now;
    svc.io_env.wall = wall;
    svc.protocol_env.now = now;
    svc.protocol_env.wall = wall;
    svc.domain_env.now = now;
    svc.domain_env.wall = wall;
    // The up pass.
    io_up(svc, now);
    protocol_up(svc, now);
    domain_stage(svc);
    // The down pass.
    protocol_down(svc);
    // The service owns its startup signal source, while the protocol owns
    // the sockets. Close the source only once the protocol has settled.
    if svc.protocol.is_empty()
        && !svc.signals_closing
        && svc.asked.room() > 0
        && let Some(entity) = svc.signals
    {
        svc.signals_closing = true;
        svc.asked.push(io::Request::Close { entity });
    }
    io_down(svc);
    // The reclaim point.
    svc.io.reclaim();
    svc.protocol.reclaim();
    svc.domain.reclaim();
}

/// io's stage of the up pass: its ready list, then the completions (only
/// once the ready list is empty, so that what the down pass made is told
/// first: io.md, 2), then its deadlines.
fn io_up(svc: &mut Service, now: Time) {
    let limits = svc.io_env.limits;
    // Each entity is resumed at most once a stage, and each refusal told once.
    let entries = limits.sockets.checked_add(limits.refusals).expect("io's limits fit a u32");
    for _ in 0..entries {
        if !svc.io.is_ready() || !io_room(svc, io::MAX_OUT_RESUME) {
            break;
        }
        let marks = io_marks(svc);
        io::resume(&mut svc.io, &svc.io_env, &mut svc.told, &mut svc.submissions);
        io_within(svc, marks, io::MAX_OUT_RESUME);
    }
    if svc.io.is_ready() {
        return;
    }
    for _ in 0..svc.completions.capacity() {
        if !io_room(svc, io::MAX_OUT_UP) {
            break;
        }
        let Some(complete) = svc.completions.pop() else {
            break;
        };
        let marks = io_marks(svc);
        io::up(&mut svc.io, &svc.io_env, complete, &mut svc.told, &mut svc.submissions);
        io_within(svc, marks, io::MAX_OUT_UP);
    }
    if !svc.completions.is_empty() {
        return;
    }
    // A close's deadline and a retry's, per socket.
    let timers = limits.sockets.checked_mul(2).expect("io's limits fit a u32");
    for _ in 0..timers {
        if !svc.io.is_due(now) || !io_room(svc, io::MAX_OUT_FIRE) {
            break;
        }
        let marks = io_marks(svc);
        io::fire(&mut svc.io, &svc.io_env, &mut svc.told, &mut svc.submissions);
        io_within(svc, marks, io::MAX_OUT_FIRE);
    }
}

/// The protocol layer's stage of the up pass: its ready list, then what io
/// told, then its idle deadlines.
fn protocol_up(svc: &mut Service, now: Time) {
    // The listener, the shutdown, and each connection once.
    let entries = svc.protocol_env.limits.conns.checked_add(2).expect("the protocol's limits fit a u32");
    for _ in 0..entries {
        if !svc.protocol.is_ready() || !protocol_room(svc, protocol::MAX_OUT_RESUME) {
            break;
        }
        let marks = protocol_marks(svc);
        protocol::resume(&mut svc.protocol, &svc.protocol_env, &mut svc.calls, &mut svc.asked);
        protocol_within(svc, marks, protocol::MAX_OUT_RESUME);
    }
    if svc.protocol.is_ready() {
        return;
    }
    for _ in 0..svc.told.capacity() {
        if !protocol_room(svc, protocol::MAX_OUT_UP) {
            break;
        }
        let Some(event) = svc.told.pop() else {
            break;
        };
        let marks = protocol_marks(svc);
        match &event {
            io::Event::Closed { owner } if *owner == SIGNALS => svc.signals = None,
            io::Event::Failed { owner, error: _ } if *owner == SIGNALS => {
                svc.signals_closing = true;
                svc.protocol.shutdown();
            }
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Stream { .. }
            | io::Event::Output { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Usage { .. }
            | io::Event::Shutdown { .. }
            | io::Event::Failed { .. }
            | io::Event::Closed { .. } => {
                protocol::up(&mut svc.protocol, &svc.protocol_env, event, &mut svc.calls, &mut svc.asked);
            }
        }
        protocol_within(svc, marks, protocol::MAX_OUT_UP);
    }
    if !svc.told.is_empty() {
        return;
    }
    for _ in 0..svc.protocol_env.limits.conns {
        if !svc.protocol.is_due(now) || !protocol_room(svc, protocol::MAX_OUT_FIRE) {
            break;
        }
        let marks = protocol_marks(svc);
        protocol::fire(&mut svc.protocol, &svc.protocol_env, &mut svc.calls, &mut svc.asked);
        protocol_within(svc, marks, protocol::MAX_OUT_FIRE);
    }
}

/// The domain's stage: each call and event, while its replies fit.
fn domain_stage(svc: &mut Service) {
    for _ in 0..svc.calls.capacity() {
        if svc.answers.room() < domain::MAX_OUT {
            break;
        }
        let Some(event) = svc.calls.pop() else {
            break;
        };
        let before = svc.answers.len();
        domain::step(&mut svc.domain, &svc.domain_env, event, &mut svc.answers);
        within(before, svc.answers.len(), domain::MAX_OUT);
    }
}

/// The protocol layer's stage of the down pass: each of the domain's
/// requests, while what it asks of io fits.
fn protocol_down(svc: &mut Service) {
    for _ in 0..svc.answers.capacity() {
        if svc.asked.room() < protocol::MAX_OUT_DOWN {
            break;
        }
        let Some(request) = svc.answers.pop() else {
            break;
        };
        let before = svc.asked.len();
        protocol::down(&mut svc.protocol, &svc.protocol_env, request, &mut svc.asked);
        within(before, svc.asked.len(), protocol::MAX_OUT_DOWN);
    }
}

/// io's stage of the down pass: each request, while io can hold a refusal
/// (`Io::takes`) and what it submits fits.
fn io_down(svc: &mut Service) {
    for _ in 0..svc.asked.capacity() {
        if !svc.io.takes() || !io_room(svc, io::MAX_OUT_DOWN) {
            break;
        }
        let Some(request) = svc.asked.pop() else {
            break;
        };
        let marks = io_marks(svc);
        io::down(&mut svc.io, &svc.io_env, request, &mut svc.submissions);
        io_within(svc, marks, io::MAX_OUT_DOWN);
    }
}

/// The lengths of a stage's two output queues before a call.
#[derive(Clone, Copy, Debug)]
struct Marks {
    up: u32,
    down: u32,
}

fn io_room(svc: &Service, max: io::MaxOut) -> bool {
    svc.told.room() >= max.events && svc.submissions.room() >= max.submissions
}

fn io_marks(svc: &Service) -> Marks {
    Marks { up: svc.told.len(), down: svc.submissions.len() }
}

fn io_within(svc: &Service, marks: Marks, max: io::MaxOut) {
    within(marks.up, svc.told.len(), max.events);
    within(marks.down, svc.submissions.len(), max.submissions);
}

fn protocol_room(svc: &Service, max: protocol::MaxOut) -> bool {
    svc.calls.room() >= max.events && svc.asked.room() >= max.requests
}

fn protocol_marks(svc: &Service) -> Marks {
    Marks { up: svc.calls.len(), down: svc.asked.len() }
}

fn protocol_within(svc: &Service, marks: Marks, max: protocol::MaxOut) {
    within(marks.up, svc.calls.len(), max.events);
    within(marks.down, svc.asked.len(), max.requests);
}

/// A call emitted no more than its `MAX_OUT` into a queue it found at
/// `before` and left at `after`.
fn within(before: u32, after: u32, max: u32) {
    let emitted = after.checked_sub(before).expect("a step only adds to its output queues");
    assert!(emitted <= max, "an entry point emits no more than its MAX_OUT");
}
