//! The fake echo client (examples.md, 5; testing-strategy.md, 4): a step
//! machine, io and one layer above it that plays the echo's line protocol
//! over io's stream vocabulary, so that the simulator and the real loop host
//! it as they host the echo. It shares no type with the echo.
//!
//! Each of its connections follows a [`Plan`], over as many attempts as the
//! plan allows: it connects once it is told where the server is
//! ([`Client::dial`]) and its time has come, sends its lines ahead of their
//! answers, checks each answer as it comes, and ends as the plan says. It
//! checks its peer as it goes and fails the world on the first breach of
//! the echo's contract; what it saw it keeps, as [`Seen`], for a referee.
//!
//! # Driving it
//!
//! As a service is driven (programming-model.md, section 2): the kernel's
//! completions into [`Client::completions`], [`iterate`], and
//! [`Client::submissions`] to the kernel, waiting until
//! [`Client::next_deadline`] when [`Client::work_pending`] says there is
//! nothing to do. [`worst_case`] is what it may hold.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod conn;
#[cfg(test)]
mod tests;

use skein_io::kernel::{Addr, Complete, Submit};
use skein_io::{self as io, Io};
use skein_lib::{Deadlines, Duration, Env, List, Queue, Time, Token, Wall};

use crate::conn::{After, Conn, Timer};

/// The client's limits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub io: io::Limits,
    /// The server's line limit: the longest answer, its end of line
    /// included, and the most an answer's scan asks for. The line past the
    /// limit is twice as long.
    pub line: u32,
    /// The capacity of each queue between its stages.
    pub queue: u32,
}

/// How one connection behaves, over its attempts.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Plan {
    /// When it first connects, once it knows where the server is.
    pub at: Time,
    /// What its lines are: each attempt sends the same.
    pub seed: u64,
    /// How many lines it sends.
    pub lines: u32,
    /// The lengths of its lines, their end of line included, drawn between
    /// these, at least 1 and at most the server's limit.
    pub shortest: u32,
    pub longest: u32,
    /// The line, by index, sent past the server's limit, and the last sent.
    pub long: Option<u32>,
    /// Lines sent ahead of their answers, at least 1.
    pub ahead: u32,
    /// The most bytes in one send, at least 1 and at most io's output cap.
    pub piece: u32,
    /// When it starts reading answers; `None`, never. A demand is never
    /// replaced (lib.md, 7): one for room alone, still outstanding when the
    /// time comes, reads only once its room is granted. And a connection
    /// that never reads never hears the server's end, which waits behind the
    /// answers it left unread (io.md, 3.3): with lines to send, it needs an
    /// abort of its own.
    pub read_from: Option<Time>,
    /// The bytes after which it sends nothing more, in each attempt.
    pub send_limit: u64,
    /// What it does once every line is answered.
    pub then: Then,
    /// A half-close before every line is sent: after so many whole lines,
    /// and a piece of the next, with no end of line.
    pub half_close: Option<HalfClose>,
    /// When it aborts, whatever it is doing, and makes no more attempts.
    pub abort_at: Option<Time>,
    /// The attempts it makes after the first, while one ends unanswered.
    pub retries: u32,
    /// How long it waits before each retry.
    pub backoff: Duration,
}

/// A half-close before the plan's last line: it hands io `after` lines
/// whole, then `tail` bytes of the next with no end of line, then finishes,
/// and reads on. The echo answers the whole lines, then ends its stream,
/// the piece dropped, and the plan is answered once the whole lines are.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct HalfClose {
    pub after: u32,
    pub tail: u32,
}

/// What a connection does once every line is answered.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Then {
    /// Half-closes, then closes once the server ends its side.
    Finish,
    /// Closes, gracefully.
    Close,
    /// Aborts.
    Abort,
    /// Stays open, reading, until the server ends it.
    Linger,
}

/// What a connection saw, for the referee: facts, never the state that
/// produced them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Seen {
    /// Attempts made: connects asked.
    pub attempts: u32,
    /// When the last attempt connected.
    pub connected: Option<Time>,
    /// Lines answered in the last attempt.
    pub answered: u32,
    /// When the last attempt last made progress: connected, or an answer.
    pub progress: Option<Time>,
    /// Bytes handed io in the last attempt.
    pub handed: u64,
    /// Attempts the server told `busy`.
    pub busy: u32,
    /// Attempts the server ended without a word, unanswered.
    pub silent: u32,
    /// Connects that failed.
    pub failed: u32,
    /// Streams that broke.
    pub broken: u32,
    /// The server told the line past its limit `too long`.
    pub too_long: bool,
    /// When the server ended the last attempt's stream.
    pub ended: Option<Time>,
    /// An attempt had every line answered, or its line past the limit
    /// refused.
    pub complete: bool,
    /// When it finished, for good.
    pub done: Option<Time>,
}

impl Seen {
    const NOTHING: Seen = Seen {
        attempts: 0,
        connected: None,
        answered: 0,
        progress: None,
        handed: 0,
        busy: 0,
        silent: 0,
        failed: 0,
        broken: 0,
        too_long: false,
        ended: None,
        complete: false,
        done: None,
    };
}

/// The client: io, its connections, their timers, and the queues between
/// its stages.
#[derive(Debug)]
pub struct Client {
    io: Io,
    io_env: Env<io::Limits>,
    env: Env<Limits>,
    conns: List<Conn>,
    deadlines: Deadlines<(u32, Timer)>,
    server: Server,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    told: Queue<io::Event>,
    asked: Queue<io::Request>,
}

/// Where the server is, as the client was told.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Server {
    Unknown,
    /// Told, and its connections' timers not yet armed.
    Told(Addr),
    Known(Addr),
}

/// The most requests to io one event or timer of the client's emits: a
/// piece sent, the half-close it completes, and the next demand.
pub const MAX_OUT: u32 = 3;

/// Timers per connection.
const TIMERS: u32 = 3;

/// The most operations the client has in flight at once: the size of its
/// ring, or `None` past a `u32`.
#[must_use]
pub fn operations(limits: &Limits) -> Option<u32> {
    io::operations(&limits.io)
}

/// The most heap the client holds under `limits` with `conns` connections,
/// or `None` past a `u64` (programming-model.md, 6.3): io's worst case, its
/// connections and their timers, its queues, and per connection an answer
/// and a piece on their way.
#[must_use]
pub fn worst_case(limits: &Limits, conns: u32) -> Option<u64> {
    let queue = limits.queue;
    let queues = Queue::<Complete>::worst_case(queue)?
        .checked_add(Queue::<Submit>::worst_case(queue)?)?
        .checked_add(Queue::<io::Event>::worst_case(queue)?)?
        .checked_add(Queue::<io::Request>::worst_case(queue)?)?;
    let tables = List::<Conn>::worst_case(conns)?
        .checked_add(Deadlines::<(u32, Timer)>::worst_case(conns.checked_mul(TIMERS)?)?)?;
    let bytes = u64::from(conns).checked_mul(u64::from(limits.line).checked_add(u64::from(limits.io.output))?)?;
    io::worst_case(&limits.io)?.checked_add(queues)?.checked_add(tables)?.checked_add(bytes)
}

impl Client {
    /// The client under `limits`, which io must find usable, its
    /// connections following `plans`, each of which must fit the limits.
    #[must_use]
    pub fn new(limits: &Limits, plans: &[Plan]) -> Client {
        assert!(limits.queue >= MAX_OUT, "queues that hold the largest MAX_OUT");
        assert!(limits.line <= limits.io.largest_read(), "io's intake holds an answer");
        let count = u32::try_from(plans.len()).expect("fewer than 2^32 plans");
        let mut conns = List::with_capacity(count);
        for plan in plans {
            assert!(plan.ahead > 0 && plan.piece > 0, "a plan sends at least a line and a byte at a time");
            assert!(plan.piece <= limits.io.largest_room(), "a piece fits io's output cap");
            assert!(
                plan.shortest > 0 && plan.shortest <= plan.longest && plan.longest <= limits.line,
                "a plan's lines hold their end of line, within the server's limit"
            );
            if let Some(long) = plan.long {
                assert!(long < plan.lines, "the line past the limit is among the plan's");
            }
            if let Some(half) = plan.half_close {
                assert!(half.after < plan.lines || half.tail == 0, "the piece of a line is of one of the plan's");
                assert!(half.after <= plan.lines && plan.long.is_none(), "a half-close among the plan's lines");
                assert!(half.tail < limits.line, "a piece of a line, under the server's limit");
            }
            assert!(
                plan.read_from.is_some() || plan.abort_at.is_some() || plan.lines == 0,
                "a plan that sends lines and never reads their answers ends by an abort of its own"
            );
            conns.push(Conn::new(*plan)).expect("a slot for each plan");
        }
        let timers = count.checked_mul(TIMERS).expect("fewer than 2^30 plans");
        Client {
            io: Io::new(&limits.io),
            io_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.io },
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: *limits },
            conns,
            deadlines: Deadlines::with_capacity(timers),
            server: Server::Unknown,
            completions: Queue::with_capacity(limits.queue),
            submissions: Queue::with_capacity(limits.queue),
            told: Queue::with_capacity(limits.queue),
            asked: Queue::with_capacity(limits.queue),
        }
    }

    /// Tells the client where the server is: its connections start, each at
    /// its time, from the next iteration. Told again, it changes nothing.
    pub fn dial(&mut self, server: Addr) {
        match self.server {
            Server::Unknown => self.server = Server::Told(server),
            Server::Told(_) | Server::Known(_) => {}
        }
    }

    /// Where the kernel's completions go, for `iterate` to take.
    pub const fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }

    /// What `iterate` asked of the kernel, for the loop to submit.
    pub const fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }

    /// Whether `iterate` has work at `now` without the kernel.
    #[must_use]
    pub fn work_pending(&self, now: Time) -> bool {
        let told = match self.server {
            Server::Told(_) => true,
            Server::Unknown | Server::Known(_) => false,
        };
        let due = match self.deadlines.next() {
            Some(at) => at <= now,
            None => false,
        };
        told || due
            || self.io.is_ready()
            || self.io.is_due(now)
            || !self.completions.is_empty()
            || !self.told.is_empty()
            || !self.asked.is_empty()
    }

    /// The earliest deadline: io's, or a connection's timer.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let timers = self.deadlines.next();
        match self.io.next_deadline() {
            Some(io) => match timers {
                Some(timer) => Some(io.min(timer)),
                None => Some(io),
            },
            None => timers,
        }
    }

    /// How many connections it plans.
    #[must_use]
    pub fn conns(&self) -> u32 {
        self.conns.len()
    }

    /// What connection `conn` saw.
    #[must_use]
    pub fn seen(&self, conn: u32) -> Seen {
        self.conns.get(conn).expect("a planned connection").seen()
    }

    /// Whether every connection is done: no more attempts.
    #[must_use]
    pub fn done(&self) -> bool {
        for conn in &self.conns {
            if !conn.is_done() {
                return false;
            }
        }
        true
    }

    /// Whether the client holds nothing: every connection done, io empty,
    /// no timer armed, every queue empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.done()
            && self.io.is_empty()
            && self.deadlines.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.told.is_empty()
            && self.asked.is_empty()
    }
}

/// One iteration: io's stage, the client's, io's down stage, and the
/// reclaim point, each call within its `MAX_OUT`.
pub fn iterate(client: &mut Client, now: Time, wall: Wall) {
    client.io_env.now = now;
    client.io_env.wall = wall;
    client.env.now = now;
    client.env.wall = wall;
    io_up(client, now);
    client_stage(client, now);
    io_down(client);
    client.io.reclaim();
}

fn io_up(client: &mut Client, now: Time) {
    let limits = client.io_env.limits;
    let entries = limits.sockets.checked_add(limits.refusals).expect("io's limits fit a u32");
    for _ in 0..entries {
        if !client.io.is_ready() || !io_room(client, io::MAX_OUT_RESUME) {
            break;
        }
        let marks = io_marks(client);
        io::resume(&mut client.io, &client.io_env, &mut client.told, &mut client.submissions);
        io_within(client, marks, io::MAX_OUT_RESUME);
    }
    if client.io.is_ready() {
        return;
    }
    for _ in 0..client.completions.capacity() {
        if !io_room(client, io::MAX_OUT_UP) {
            break;
        }
        let Some(complete) = client.completions.pop() else {
            break;
        };
        let marks = io_marks(client);
        io::up(&mut client.io, &client.io_env, complete, &mut client.told, &mut client.submissions);
        io_within(client, marks, io::MAX_OUT_UP);
    }
    if !client.completions.is_empty() {
        return;
    }
    let timers = limits.sockets.checked_mul(2).expect("io's limits fit a u32");
    for _ in 0..timers {
        if !client.io.is_due(now) || !io_room(client, io::MAX_OUT_FIRE) {
            break;
        }
        let marks = io_marks(client);
        io::fire(&mut client.io, &client.io_env, &mut client.told, &mut client.submissions);
        io_within(client, marks, io::MAX_OUT_FIRE);
    }
}

/// The client's stage: its connections' timers armed once it is told the
/// server; what io told; then its timers, once io's events are all taken.
fn client_stage(client: &mut Client, now: Time) {
    match client.server {
        Server::Told(server) => {
            arm(client);
            client.server = Server::Known(server);
        }
        Server::Unknown | Server::Known(_) => {}
    }
    for _ in 0..client.told.capacity() {
        if client.asked.room() < MAX_OUT {
            break;
        }
        let Some(event) = client.told.pop() else {
            break;
        };
        let index = owner(&event);
        let before = client.asked.len();
        let conn = client.conns.get_mut(index).expect("io names a connection the client made");
        let after = conn::told(conn, event, now, &client.env.limits, &mut client.asked);
        within(before, client.asked.len());
        follow_up(client, index, after);
    }
    if !client.told.is_empty() {
        return;
    }
    let server = match client.server {
        Server::Known(server) => server,
        Server::Unknown | Server::Told(_) => return,
    };
    for _ in 0..client.deadlines.len() {
        if client.asked.room() < MAX_OUT {
            break;
        }
        let Some((index, timer)) = client.deadlines.expire(now) else {
            break;
        };
        let before = client.asked.len();
        let conn = client.conns.get_mut(index).expect("a timer names a connection the client made");
        let after = match timer {
            Timer::Start => {
                conn::start(conn, Token::new(u64::from(index)), server, &mut client.asked);
                After::Nothing
            }
            Timer::Abort => conn::abort(conn, now, &mut client.asked),
            Timer::Read => {
                conn::read(conn, now, &client.env.limits, &mut client.asked);
                After::Nothing
            }
        };
        within(before, client.asked.len());
        follow_up(client, index, after);
    }
}

/// Arms each connection's timers: its start, its abort and its time to
/// read, as its plan says.
fn arm(client: &mut Client) {
    for (index, conn) in client.conns.iter().enumerate() {
        let index = u32::try_from(index).expect("fewer than 2^32 plans");
        let plan = conn.plan();
        let timers = [(Timer::Start, Some(plan.at)), (Timer::Abort, plan.abort_at), (Timer::Read, plan.read_from)];
        for (timer, at) in timers {
            if let Some(at) = at {
                client.deadlines.arm((index, timer), at).expect("room for each connection's timers");
            }
        }
    }
}

/// What an ended attempt leaves: a retry's start, or no timers at all.
fn follow_up(client: &mut Client, index: u32, after: After) {
    match after {
        After::Nothing => {}
        After::Retry(at) => client.deadlines.arm((index, Timer::Start), at).expect("room for each connection's timers"),
        After::Done => {
            for timer in [Timer::Start, Timer::Abort, Timer::Read] {
                client.deadlines.cancel((index, timer));
            }
        }
    }
}

/// The connection an event of io's names, by its owner token: its index.
fn owner(event: &io::Event) -> u32 {
    let owner = match event {
        io::Event::Connecting { owner, .. }
        | io::Event::Connected { owner }
        | io::Event::Stream { owner, .. }
        | io::Event::Failed { owner, .. }
        | io::Event::Closed { owner } => *owner,
        io::Event::Output { .. }
        | io::Event::Listening { .. }
        | io::Event::Accepted { .. }
        | io::Event::Spawned { .. }
        | io::Event::Exited { .. }
        | io::Event::Usage { .. }
        | io::Event::Shutdown { .. } => unreachable!("the client listens to no one and spawns no child"),
    };
    u32::try_from(owner.raw()).expect("an owner token is a connection's index")
}

fn io_down(client: &mut Client) {
    for _ in 0..client.asked.capacity() {
        if !client.io.takes() || !io_room(client, io::MAX_OUT_DOWN) {
            break;
        }
        let Some(request) = client.asked.pop() else {
            break;
        };
        let marks = io_marks(client);
        io::down(&mut client.io, &client.io_env, request, &mut client.submissions);
        io_within(client, marks, io::MAX_OUT_DOWN);
    }
}

fn io_room(client: &Client, max: io::MaxOut) -> bool {
    client.told.room() >= max.events && client.submissions.room() >= max.submissions
}

/// The lengths of io's two output queues before a call.
fn io_marks(client: &Client) -> (u32, u32) {
    (client.told.len(), client.submissions.len())
}

fn io_within(client: &Client, (told, submissions): (u32, u32), max: io::MaxOut) {
    let events = client.told.len().checked_sub(told).expect("a step only adds to its output queues");
    let records = client.submissions.len().checked_sub(submissions).expect("a step only adds to its output queues");
    assert!(events <= max.events && records <= max.submissions, "io emits no more than its MAX_OUT");
}

/// A call emitted no more than [`MAX_OUT`] into a queue it found at `before`
/// and left at `after`.
fn within(before: u32, after: u32) {
    let emitted = after.checked_sub(before).expect("a step only adds to its output queue");
    assert!(emitted <= MAX_OUT, "the client emits no more than its MAX_OUT");
}
