//! A connection of the fake client (examples.md, 5): attempts to connect,
//! each one sending its plan's lines in pieces within the room io grants,
//! reading and checking their answers, and ending as its plan says. It
//! holds no copy of what it sent: a line is drawn from the plan's seed when
//! it is sent, and drawn again, from the same seed, to check its answer.

use core::mem;

use skein_io::kernel::Addr;
use skein_io::{Event as Told, Request as Io};
use skein_lib::stream::{Delimiter, Down, Read, Up};
use skein_lib::{Queue, Rng, Time, Token, Writer};

use crate::{Limits, Plan, Seen, Then};

/// The echo's refusals (examples.md, 3.1), as the client knows them: its own
/// copy, as a fake shares no type with what it stands in for.
const BUSY: &[u8] = b"busy\n";
const TOO_LONG: &[u8] = b"too long\n";

/// What a connection's timers are for.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) enum Timer {
    /// Its next attempt: the plan's time, or a retry's backoff.
    Start,
    /// The plan's abort.
    Abort,
    /// The plan's time to start reading.
    Read,
}

/// One planned connection, over its attempts.
#[derive(Debug)]
pub(crate) struct Conn {
    plan: Plan,
    seen: Seen,
    state: State,
    /// Aborted by its plan: no more attempts.
    aborted: bool,
}

#[derive(Debug)]
enum State {
    /// Waiting for its start timer.
    Waiting,
    /// `Connect` asked; `Connecting` not yet told; whether to abort once it is.
    Connecting {
        abort: bool,
    },
    /// `Connecting` told; `Connected` awaited.
    Dialing {
        socket: Token,
    },
    /// The connect failed: io closes what it made, and tells `Closed`.
    Failing,
    Open(Open),
    /// `Close` or `Abort` asked; `Closed` awaited.
    Closing,
    /// No more attempts.
    Done,
}

/// An attempt connected.
#[derive(Debug)]
struct Open {
    socket: Token,
    sender: Sender,
    /// Draws each line again, from the plan's seed, to check its answer.
    checker: Rng,
    /// A demand outstanding, and the room it asked for: the next is stated
    /// only once it is answered.
    demand: Option<u32>,
    /// `Finish` sent: nothing more is.
    finished: bool,
    refusal: Refusal,
}

/// The lines going out.
#[derive(Debug)]
struct Sender {
    rng: Rng,
    /// Lines begun.
    begun: u32,
    /// Bytes left of the line begun, its end of line included.
    left: u32,
    /// Lines handed whole.
    sent: u32,
    /// Bytes handed io.
    handed: u64,
}

/// What the server said instead of an answer.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Refusal {
    None,
    Busy,
    TooLong,
}

/// What an attempt that just ended leaves to do.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum After {
    /// Still going.
    Nothing,
    /// Another attempt, at this time.
    Retry(Time),
    /// No more: its timers go.
    Done,
}

impl Conn {
    pub(crate) const fn new(plan: Plan) -> Conn {
        Conn { plan, seen: Seen::NOTHING, state: State::Waiting, aborted: false }
    }

    pub(crate) const fn plan(&self) -> &Plan {
        &self.plan
    }

    pub(crate) const fn seen(&self) -> Seen {
        self.seen
    }

    pub(crate) const fn is_done(&self) -> bool {
        match self.state {
            State::Done => true,
            State::Waiting
            | State::Connecting { .. }
            | State::Dialing { .. }
            | State::Failing
            | State::Open(_)
            | State::Closing => false,
        }
    }
}

/// The start timer fired: an attempt connects to `server`, under `owner`.
pub(crate) fn start(conn: &mut Conn, owner: Token, server: Addr, down: &mut Queue<Io>) {
    let state = mem::replace(&mut conn.state, State::Done);
    conn.state = match state {
        State::Waiting => {
            conn.seen.attempts = conn.seen.attempts.checked_add(1).expect("fewer than 2^32 attempts");
            down.push(Io::Connect { owner, addr: server });
            State::Connecting { abort: false }
        }
        State::Connecting { .. }
        | State::Dialing { .. }
        | State::Failing
        | State::Open(_)
        | State::Closing
        | State::Done => unreachable!("the start timer runs only while waiting"),
    };
}

/// The plan's abort: whatever the connection is doing, it stops, and makes
/// no more attempts.
pub(crate) fn abort(conn: &mut Conn, now: Time, down: &mut Queue<Io>) -> After {
    conn.aborted = true;
    let state = mem::replace(&mut conn.state, State::Done);
    let (next, after) = match state {
        // Not connected: it never will be.
        State::Waiting => {
            conn.seen.done = Some(now);
            (State::Done, After::Done)
        }
        State::Connecting { .. } => (State::Connecting { abort: true }, After::Nothing),
        State::Dialing { socket } => (close(socket, true, down), After::Nothing),
        State::Open(open) => (close(open.socket, true, down), After::Nothing),
        state @ (State::Failing | State::Closing | State::Done) => (state, After::Nothing),
    };
    conn.state = next;
    after
}

/// The plan's time to start reading: the next demand reads.
pub(crate) fn read(conn: &mut Conn, now: Time, limits: &Limits, down: &mut Queue<Io>) {
    let Conn { plan, seen, state, .. } = conn;
    match state {
        State::Open(open) => follow(open, plan, seen, now, limits, down),
        State::Waiting
        | State::Connecting { .. }
        | State::Dialing { .. }
        | State::Failing
        | State::Closing
        | State::Done => {}
    }
}

/// An event io told about the connection: what the attempt does next, and
/// whether it ended.
pub(crate) fn told(conn: &mut Conn, event: Told, now: Time, limits: &Limits, down: &mut Queue<Io>) -> After {
    let state = mem::replace(&mut conn.state, State::Done);
    conn.state = match event {
        Told::Connecting { socket, .. } => connecting(state, socket, down),
        Told::Connected { .. } => connected(state, &conn.plan, &mut conn.seen, now, limits, down),
        Told::Failed { .. } => {
            conn.seen.failed = conn.seen.failed.checked_add(1).expect("fewer than 2^32 attempts");
            failing(state)
        }
        Told::Stream { up, .. } => stream(state, up, &conn.plan, &mut conn.seen, now, limits, down),
        Told::Closed { .. } => {
            return closed(conn, state, now);
        }
        Told::Listening { .. } | Told::Accepted { .. } => unreachable!("the client listens to no one"),
    };
    After::Nothing
}

fn connecting(state: State, socket: Token, down: &mut Queue<Io>) -> State {
    match state {
        State::Connecting { abort: false } => State::Dialing { socket },
        State::Connecting { abort: true } => close(socket, true, down),
        State::Waiting | State::Dialing { .. } | State::Failing | State::Open(_) | State::Closing | State::Done => {
            unreachable!("Connecting is told once, to a connect")
        }
    }
}

fn connected(state: State, plan: &Plan, seen: &mut Seen, now: Time, limits: &Limits, down: &mut Queue<Io>) -> State {
    match state {
        State::Dialing { socket } => {
            seen.connected = Some(now);
            seen.progress = Some(now);
            seen.answered = 0;
            seen.handed = 0;
            let mut open = Open {
                socket,
                sender: Sender { rng: Rng::new(plan.seed), begun: 0, left: 0, sent: 0, handed: 0 },
                checker: Rng::new(plan.seed),
                demand: None,
                finished: false,
                refusal: Refusal::None,
            };
            // A plan of no lines is answered as soon as it connects.
            if plan.lines == 0 {
                seen.complete = true;
                if let Some(state) = then(&mut open, plan, down) {
                    return state;
                }
            }
            follow(&mut open, plan, seen, now, limits, down);
            State::Open(open)
        }
        // Aborted while dialing: told before io took the abort.
        State::Closing => State::Closing,
        State::Waiting | State::Connecting { .. } | State::Failing | State::Open(_) | State::Done => {
            unreachable!("Connected follows Connecting")
        }
    }
}

fn failing(state: State) -> State {
    match state {
        State::Connecting { .. } | State::Dialing { .. } => State::Failing,
        // Aborted while dialing: told before io took the abort.
        State::Closing => State::Closing,
        State::Waiting | State::Failing | State::Open(_) | State::Done => {
            unreachable!("a connect fails once, before it connects")
        }
    }
}

fn stream(
    state: State,
    up: Up,
    plan: &Plan,
    seen: &mut Seen,
    now: Time,
    limits: &Limits,
    down: &mut Queue<Io>,
) -> State {
    let mut open = match state {
        State::Open(open) => open,
        // Told before io took the close.
        State::Closing => return State::Closing,
        State::Waiting | State::Connecting { .. } | State::Dialing { .. } | State::Failing | State::Done => {
            unreachable!("stream events come once connected")
        }
    };
    match up {
        Up::Bytes(answer) => {
            open.demand = None;
            check(&mut open, plan, seen, &answer, now, limits);
            let complete = seen.complete;
            if complete && let Some(state) = then(&mut open, plan, down) {
                return state;
            }
        }
        Up::Room => {
            let room = open.demand.take().expect("room comes only for a demand");
            send(&mut open, room, seen, down);
        }
        // The server ended its stream: it closes. What the attempt did not
        // get, it may try again for.
        Up::End => {
            seen.ended = Some(now);
            let silent = !seen.complete && seen.answered == 0 && open.refusal == Refusal::None;
            if silent {
                seen.silent = seen.silent.checked_add(1).expect("fewer than 2^32 attempts");
            }
            return close(open.socket, false, down);
        }
        Up::Failed(_) => {
            seen.broken = seen.broken.checked_add(1).expect("fewer than 2^32 attempts");
            return close(open.socket, false, down);
        }
    }
    follow(&mut open, plan, seen, now, limits, down);
    State::Open(open)
}

/// io told `Closed`: the attempt is over. Another follows if the plan was
/// not answered, it was not aborted, and it has retries left.
fn closed(conn: &mut Conn, state: State, now: Time) -> After {
    match state {
        State::Failing | State::Closing => {}
        State::Waiting | State::Connecting { .. } | State::Dialing { .. } | State::Open(_) | State::Done => {
            unreachable!("io tells Closed after a failed connect, or after the close")
        }
    }
    let tries = conn.plan.retries.saturating_add(1);
    if conn.seen.complete || conn.aborted || conn.seen.attempts >= tries {
        conn.state = State::Done;
        conn.seen.done = Some(now);
        return After::Done;
    }
    conn.state = State::Waiting;
    After::Retry(now.saturating_add(conn.plan.backoff))
}

/// Asks io to close the attempt's socket, gracefully or at once.
fn close(socket: Token, abort: bool, down: &mut Queue<Io>) -> State {
    if abort {
        down.push(Io::Abort { entity: socket });
    } else {
        down.push(Io::Close { entity: socket });
    }
    State::Closing
}

/// What the plan does once every line is answered: `None` to stay open.
fn then(open: &mut Open, plan: &Plan, down: &mut Queue<Io>) -> Option<State> {
    match plan.then {
        Then::Finish => {
            if !open.finished {
                down.push(Io::Stream { stream: open.socket, down: Down::Finish });
                open.finished = true;
            }
            None
        }
        Then::Close => Some(close(open.socket, false, down)),
        Then::Abort => Some(close(open.socket, true, down)),
        Then::Linger => None,
    }
}

/// States the next demand, once the last is answered: a line read, from the
/// plan's time to read on, and room for the next piece to send.
fn follow(open: &mut Open, plan: &Plan, seen: &Seen, now: Time, limits: &Limits, down: &mut Queue<Io>) {
    if open.demand.is_some() {
        return;
    }
    let reads = match plan.read_from {
        Some(from) => now >= from,
        None => false,
    };
    let room = piece(open, plan, seen, limits);
    if !reads && room == 0 {
        return;
    }
    let read = if reads { Read::Scan { until: Delimiter::LF, max: limits.line } } else { Read::Nothing };
    down.push(Io::Stream { stream: open.socket, down: Down::Demand { read, room } });
    open.demand = Some(room);
}

/// The size of the next piece to send, beginning the next line if the plan
/// allows one now; 0 for none.
fn piece(open: &mut Open, plan: &Plan, seen: &Seen, limits: &Limits) -> u32 {
    let sender = &mut open.sender;
    if open.finished || open.refusal != Refusal::None || sender.handed >= plan.send_limit {
        return 0;
    }
    if sender.left == 0 {
        let past_long = match plan.long {
            Some(long) => sender.begun > long,
            None => false,
        };
        let unanswered = sender.begun.checked_sub(seen.answered).expect("no more answered than begun");
        if sender.begun >= plan.lines || past_long || unanswered >= plan.ahead {
            return 0;
        }
        sender.left = length(plan, sender.begun, &mut sender.rng, limits);
        sender.begun = sender.begun.checked_add(1).expect("no more lines than the plan's u32");
    }
    let limit = plan.send_limit.checked_sub(sender.handed).expect("checked above");
    let limit = u32::try_from(limit).unwrap_or(u32::MAX);
    plan.piece.min(sender.left).min(limit)
}

/// Room granted: the next piece of the line begun, of the size demanded,
/// drawn from the seed.
fn send(open: &mut Open, room: u32, seen: &mut Seen, down: &mut Queue<Io>) {
    let sender = &mut open.sender;
    assert!(room > 0 && room <= sender.left, "room is demanded for a piece of the line begun");
    let mut writer = Writer::new(usize::try_from(room).expect("a u32 fits a usize"));
    for _ in 0..room {
        let byte = if sender.left == 1 { b'\n' } else { byte(&mut sender.rng) };
        writer.put(&[byte]).expect("the writer is sized for the piece");
        sender.left = sender.left.checked_sub(1).expect("within the line");
        if sender.left == 0 {
            sender.sent = sender.sent.checked_add(1).expect("no more lines than the plan's u32");
        }
    }
    sender.handed = sender.handed.checked_add(u64::from(room)).expect("fewer than 2^64 bytes");
    seen.handed = sender.handed;
    down.push(Io::Stream { stream: open.socket, down: Down::Send(writer.finish()) });
}

/// An answer, checked against the line it answers, drawn again from the
/// seed; or a refusal, where one may come. The echo's contract
/// (examples.md, 3.1): each answer is the oldest line unanswered, byte for
/// byte; `busy` comes only first; `too long` only to the line past the
/// limit; nothing comes after either but the end. A breach fails the world.
fn check(open: &mut Open, plan: &Plan, seen: &mut Seen, answer: &[u8], now: Time, limits: &Limits) {
    assert!(open.refusal == Refusal::None, "the echo says nothing after a refusal but the end");
    let index = seen.answered;
    if index == 0 && answer == BUSY {
        open.refusal = Refusal::Busy;
        seen.busy = seen.busy.checked_add(1).expect("fewer than 2^32 attempts");
        return;
    }
    if plan.long == Some(index) {
        assert!(answer == TOO_LONG, "the echo answers a line past its limit with too long");
        assert!(open.sender.begun > index, "too long comes once the line past the limit is begun");
        open.refusal = Refusal::TooLong;
        seen.too_long = true;
        seen.complete = true;
        seen.progress = Some(now);
        return;
    }
    assert!(index < open.sender.sent, "the echo answers only a line sent whole");
    let len = length(plan, index, &mut open.checker, limits);
    let Some((last, text)) = answer.split_last() else {
        unreachable!("io delivers no empty answer to a scan");
    };
    assert!(u32::try_from(answer.len()) == Ok(len), "the answer is the line it answers: its length");
    for got in text {
        assert!(*got == byte(&mut open.checker), "the answer is the line it answers, byte for byte");
    }
    assert!(*last == b'\n', "the answer ends its line");
    seen.answered = seen.answered.checked_add(1).expect("no more answers than lines");
    seen.progress = Some(now);
    if seen.answered == plan.lines {
        seen.complete = true;
    }
}

/// The length of line `index`, its end of line included, drawn from `rng`
/// unless it is the line past the server's limit, twice the limit long.
fn length(plan: &Plan, index: u32, rng: &mut Rng, limits: &Limits) -> u32 {
    if plan.long == Some(index) {
        return limits.line.checked_mul(2).expect("a line limit that doubles within a u32");
    }
    let drawn = rng.between(u64::from(plan.shortest), u64::from(plan.longest));
    u32::try_from(drawn).expect("between two u32s")
}

/// A byte of a line: anything but its end.
fn byte(rng: &mut Rng) -> u8 {
    let drawn = u8::try_from(rng.below(255)).expect("below 255");
    if drawn >= b'\n' { drawn.checked_add(1).expect("at most 255") } else { drawn }
}
