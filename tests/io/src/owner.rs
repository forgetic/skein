//! The scripted owner (testing-strategy.md, 2.6): the step above io in an io
//! world, playing a protocol layer. It listens and connects as its script
//! says, answers each socket announced to it, and runs each connection by a
//! [`Plan`]: what it sends, in sends within the room io granted; how it
//! demands what it reads; whether it echoes; when it finishes and when it
//! closes or aborts. It holds what each connection saw, for the referee.

use std::collections::{BTreeMap, VecDeque};

use skein_io::kernel::Addr;
use skein_io::{Error, Event, Request};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Rng, Time, Token};

/// The addresses listeners were told, by name, shared by every process of a
/// world, so that a client can dial a server by name.
pub type Directory = BTreeMap<&'static str, Addr>;

/// How one connection behaves.
#[derive(Clone, Debug)]
pub struct Plan {
    /// Its name, for the referee.
    pub name: &'static str,
    /// What it sends, in sends of at most `chunk` bytes, each within room io
    /// granted for it.
    pub send: Box<[u8]>,
    pub chunk: u32,
    /// How many bytes it receives before it starts sending.
    pub wait: u32,
    /// Whether it sends back what it receives, after `send`.
    pub echo: bool,
    pub reads: Reads,
    /// How much it expects to receive. A demand past the end of the stream
    /// is never met, and what the intake holds then is never delivered
    /// (io.md, 3.3), so a reader that must receive everything caps its
    /// demands by what is left, as a protocol's framing does.
    pub total: Total,
    /// Whether it finishes (half-closes) once it has sent everything.
    pub finish: bool,
    pub close: When,
    /// Whether it closes by `Abort` rather than `Close`.
    pub abort: bool,
}

/// How a connection demands what it reads (lib.md, 7).
#[derive(Clone, Copy, Debug)]
pub enum Reads {
    /// Fills of 1 to this many bytes, drawn from the seed.
    Fill(u32),
    /// Scans for the delimiter, within this many bytes.
    Scan(Delimiter, u32),
    /// Fills, single bytes, and scans for `\n` and `\r\n`, drawn from the
    /// seed, each of at most this many bytes.
    Mixed(u32),
    /// No demand until the time, then mixed.
    From(Time, u32),
    /// No demand ever.
    Never,
}

/// How much a connection expects to receive.
#[derive(Clone, Copy, Debug)]
pub enum Total {
    /// It does not know: its demands may run past the end.
    Unknown,
    /// This many bytes; then it demands one more, to hear the end.
    Known(usize),
    /// The first two bytes, big-endian, say how many follow them.
    Framed,
}

/// When a connection closes, once open. A connection whose stream failed
/// closes at once, whatever its plan says.
#[derive(Clone, Copy, Debug)]
pub enum When {
    /// Once it has sent everything, finished if its plan says so, and heard
    /// the end of the peer's stream.
    Done,
    /// Once it has handed io everything it sends, finished if its plan says
    /// so: io flushes it.
    Sent,
    /// Once it has received this many bytes, or the stream ended.
    Received(u32),
    /// At this time, whatever it is doing; a connect still in flight among it.
    At(Time),
}

/// A listener, and how it answers what it accepts.
#[derive(Clone, Debug)]
pub struct Serve {
    pub name: &'static str,
    pub addr: Addr,
    /// The plan of each socket accepted, in order, which it binds; `None`
    /// rejects it. Past the list, every socket is rejected.
    pub answers: Vec<Option<Plan>>,
    /// When the listener closes.
    pub close: ServeClose,
}

#[derive(Clone, Copy, Debug)]
pub enum ServeClose {
    /// Once every answer was given, or at the time, if the sockets it waits
    /// for never come (a connect that failed).
    Answered(Time),
    At(Time),
}

impl ServeClose {
    /// The latest it closes.
    #[must_use]
    pub const fn at(self) -> Time {
        match self {
            ServeClose::Answered(at) | ServeClose::At(at) => at,
        }
    }
}

/// A connect to make.
#[derive(Clone, Debug)]
pub struct Dial {
    pub to: Target,
    pub at: Time,
    pub plan: Plan,
}

#[derive(Clone, Copy, Debug)]
pub enum Target {
    /// The listener of that name, once it listens.
    Named(&'static str),
    Addr(Addr),
}

/// Where a connection or a listener is in its life, as its owner sees it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// Asked for; `Connecting` or `Listening` not told yet.
    Asked,
    /// Connecting, its socket told.
    Connecting,
    Open,
    /// Its owner closed or aborted it.
    Closing,
    Closed,
}

/// A connection, as its owner ran it and what it saw.
#[derive(Debug)]
pub struct Conn {
    pub plan: Plan,
    /// io's token for it, once told.
    pub socket: Option<Token>,
    pub phase: Phase,
    /// Bytes of `plan.send` handed to io.
    pub sent: usize,
    /// Every byte handed to io, echoes included.
    pub handed: u64,
    /// What it received and has yet to echo.
    pub echo: VecDeque<u8>,
    pub received: Vec<u8>,
    pub ended: bool,
    pub failed: Option<Fault>,
    /// Why its connect failed, if it did.
    pub error: Option<Error>,
    pub finished: bool,
    /// Its demand outstanding: a read, and room; nothing once answered. It
    /// states the next only then (lib.md, 7), and keeps it across `End`,
    /// which `Room` may still answer.
    pub asked: (Read, u32),
    pub closed_at: Option<Time>,
}

/// A listener, as its owner ran it.
#[derive(Debug)]
pub struct Listener {
    pub serve: Serve,
    pub token: Option<Token>,
    pub phase: Phase,
    pub addr: Option<Addr>,
    pub answered: usize,
    pub error: Option<Error>,
}

/// The step above io.
#[derive(Debug)]
pub struct Owner {
    next: u64,
    rng: Rng,
    pub listeners: BTreeMap<Token, Listener>,
    pub conns: BTreeMap<Token, Conn>,
    /// Connects not yet made, by time.
    dials: Vec<Dial>,
}

impl Conn {
    fn new(plan: Plan, socket: Option<Token>, phase: Phase) -> Conn {
        Conn {
            plan,
            socket,
            phase,
            sent: 0,
            handed: 0,
            echo: VecDeque::new(),
            received: Vec::new(),
            ended: false,
            failed: None,
            error: None,
            finished: false,
            asked: (Read::Nothing, 0),
            closed_at: None,
        }
    }

    /// Whether its stream broke or never was: what it received may be short.
    #[must_use]
    pub fn broken(&self) -> bool {
        self.failed.is_some() || self.error.is_some()
    }

    /// Whether a demand is outstanding.
    fn waits(&self) -> bool {
        self.asked != (Read::Nothing, 0)
    }

    /// How many bytes it has yet to receive, if it knows; a framed one, its
    /// header first.
    fn left(&self) -> Option<usize> {
        let received = self.received.len();
        match self.plan.total {
            Total::Unknown => None,
            Total::Known(total) => Some(total.saturating_sub(received)),
            Total::Framed => match self.received.get(..2) {
                Some(&[high, low]) => Some((2 + usize::from(u16::from_be_bytes([high, low]))).saturating_sub(received)),
                Some(_) | None => Some(2 - received),
            },
        }
    }

    fn has_to_send(&self) -> bool {
        self.sent < self.plan.send.len() || !self.echo.is_empty()
    }

    /// Everything it sends is handed to io; an echo waits for the peer's end.
    fn sent_all(&self) -> bool {
        !self.has_to_send() && (!self.plan.echo || self.ended || self.failed.is_some())
    }

    fn reading(&self, now: Time) -> bool {
        if self.ended || self.failed.is_some() {
            return false;
        }
        if self.plan.echo && self.echo.len() >= len(self.plan.chunk) {
            return false;
        }
        match self.plan.reads {
            Reads::Never => false,
            Reads::From(at, _) => now >= at,
            Reads::Fill(_) | Reads::Scan(..) | Reads::Mixed(_) => true,
        }
    }

    fn closes(&self, now: Time) -> bool {
        if self.failed.is_some() {
            return true;
        }
        let sent = self.sent_all() && (self.finished || !self.plan.finish);
        match self.plan.close {
            When::Done => sent && self.ended,
            When::Sent => sent,
            When::Received(n) => self.received.len() >= n as usize || self.ended,
            When::At(at) => now >= at,
        }
    }
}

impl Owner {
    #[must_use]
    pub fn new(seed: u64, serves: Vec<Serve>, dials: Vec<Dial>) -> Owner {
        let mut owner =
            Owner { next: 1, rng: Rng::new(seed ^ 0x0e1e), listeners: BTreeMap::new(), conns: BTreeMap::new(), dials };
        for serve in serves {
            let token = owner.token();
            owner.listeners.insert(
                token,
                Listener { serve, token: None, phase: Phase::Asked, addr: None, answered: 0, error: None },
            );
        }
        owner
    }

    fn token(&mut self) -> Token {
        let token = Token::new(self.next);
        self.next += 1;
        token
    }

    /// The name of the connection or listener `owner` names.
    #[must_use]
    pub fn name(&self, owner: Token) -> &'static str {
        if let Some(conn) = self.conns.get(&owner) {
            return conn.plan.name;
        }
        self.listeners.get(&owner).expect("an owner token this owner issued").serve.name
    }

    /// The connection named `name`.
    #[must_use]
    pub fn conn(&self, name: &str) -> Option<&Conn> {
        self.conns.values().find(|conn| conn.plan.name == name)
    }

    /// Whether everything it made is closed and nothing is left to make.
    #[must_use]
    pub fn done(&self) -> bool {
        self.dials.is_empty()
            && self.listeners.values().all(|listener| listener.phase == Phase::Closed)
            && self.conns.values().all(|conn| conn.phase == Phase::Closed)
    }

    /// When it next does something by itself, after `now`: what falls due by
    /// `now` its tick has done, or waits for an event (a dial for the address
    /// of a listener not yet listening).
    #[must_use]
    pub fn next_deadline(&self, now: Time) -> Option<Time> {
        let mut next: Option<Time> = None;
        let mut at = |time: Time| {
            if time > now {
                next = Some(next.map_or(time, |next| next.min(time)));
            }
        };
        for dial in &self.dials {
            at(dial.at);
        }
        for listener in self.listeners.values() {
            if listener.phase == Phase::Open {
                at(listener.serve.close.at());
            }
        }
        for conn in self.conns.values() {
            if let (When::At(time), Phase::Connecting | Phase::Open) = (conn.plan.close, conn.phase) {
                at(time);
            }
            if let (Reads::From(time, _), Phase::Open, None) = (conn.plan.reads, conn.phase, conn.failed) {
                at(time);
            }
        }
        next
    }

    /// Its first requests: a listen for each listener.
    pub fn start(&mut self, requests: &mut VecDeque<Request>) {
        for (owner, listener) in &self.listeners {
            requests.push_back(Request::Listen { owner: *owner, addr: listener.serve.addr });
        }
    }

    /// What it does at `now` by itself: the connects due, the closes due, the
    /// reads that start.
    pub fn tick(&mut self, now: Time, directory: &Directory, requests: &mut VecDeque<Request>) {
        let mut later = Vec::new();
        for dial in std::mem::take(&mut self.dials) {
            let addr = match dial.to {
                Target::Addr(addr) => Some(addr),
                Target::Named(name) => directory.get(name).copied(),
            };
            match addr {
                Some(addr) if dial.at <= now => {
                    let owner = self.token();
                    self.conns.insert(owner, Conn::new(dial.plan, None, Phase::Asked));
                    requests.push_back(Request::Connect { owner, addr });
                }
                Some(_) | None => later.push(dial),
            }
        }
        self.dials = later;
        for listener in self.listeners.values_mut() {
            if listener.phase == Phase::Open && now >= listener.serve.close.at() {
                close_listener(listener, requests);
            }
        }
        let owners: Vec<Token> = self.conns.keys().copied().collect();
        for owner in owners {
            self.follow(owner, now, requests);
        }
    }

    /// Takes one event io told.
    pub fn on(&mut self, now: Time, event: Event, directory: &mut Directory, requests: &mut VecDeque<Request>) {
        match event {
            Event::Listening { owner, listener, addr } => {
                let listening = self.listeners.get_mut(&owner).expect("a listener of this owner");
                listening.token = Some(listener);
                listening.addr = Some(addr);
                listening.phase = Phase::Open;
                directory.insert(listening.serve.name, addr);
                if now >= listening.serve.close.at() {
                    close_listener(listening, requests);
                }
            }
            Event::Accepted { owner, socket, peer: _ } => self.accepted(owner, socket, now, requests),
            Event::Connecting { owner, socket } => {
                let conn = self.conns.get_mut(&owner).expect("a connection of this owner");
                conn.socket = Some(socket);
                conn.phase = Phase::Connecting;
                self.follow(owner, now, requests);
            }
            Event::Connected { owner } => {
                self.conns.get_mut(&owner).expect("a connection of this owner").phase = Phase::Open;
                self.follow(owner, now, requests);
            }
            Event::Stream { owner, up } => {
                self.stream(owner, up, requests);
                self.follow(owner, now, requests);
            }
            Event::Failed { owner, error } => {
                if let Some(conn) = self.conns.get_mut(&owner) {
                    conn.error = Some(error);
                } else {
                    let listener = self.listeners.get_mut(&owner).expect("an entity of this owner");
                    listener.error = Some(error);
                    // A listener that never listened: its clients dial where
                    // nothing listens, and are refused, as by a server that
                    // is not there.
                    if listener.addr.is_none() {
                        directory.insert(listener.serve.name, nowhere(listener.serve.addr));
                    }
                }
            }
            Event::Closed { owner } => {
                if let Some(conn) = self.conns.get_mut(&owner) {
                    conn.phase = Phase::Closed;
                    conn.closed_at = Some(now);
                } else {
                    self.listeners.get_mut(&owner).expect("an entity of this owner").phase = Phase::Closed;
                }
            }
        }
    }

    fn accepted(&mut self, listener: Token, socket: Token, now: Time, requests: &mut VecDeque<Request>) {
        let listening = self.listeners.get_mut(&listener).expect("a listener of this owner");
        let answer = listening.serve.answers.get(listening.answered).cloned().flatten();
        listening.answered += 1;
        let answered_all = listening.answered >= listening.serve.answers.len();
        if answered_all && let (ServeClose::Answered(_), Phase::Open) = (listening.serve.close, listening.phase) {
            close_listener(listening, requests);
        }
        match answer {
            Some(plan) => {
                let owner = self.token();
                self.conns.insert(owner, Conn::new(plan, Some(socket), Phase::Open));
                requests.push_back(Request::Bind { socket, owner });
                self.follow(owner, now, requests);
            }
            None => requests.push_back(Request::Reject { socket }),
        }
    }

    fn stream(&mut self, owner: Token, up: Up, requests: &mut VecDeque<Request>) {
        let conn = self.conns.get_mut(&owner).expect("a connection of this owner");
        let socket = conn.socket.expect("a stream told is bound or connected");
        // An answer on its way when it closed, which withdrew its demand:
        // dropped, as the side above drops it (lib.md, 7).
        let answering = match up {
            Up::Bytes(_) | Up::Room => true,
            Up::End | Up::Failed(_) => false,
        };
        if answering && conn.phase != Phase::Open {
            return;
        }
        match up {
            Up::Bytes(bytes) => {
                assert!(conn.asked.0 != Read::Nothing, "Bytes answer a read outstanding");
                conn.asked = (Read::Nothing, 0);
                conn.received.extend_from_slice(&bytes);
                if conn.plan.echo {
                    conn.echo.extend(bytes.iter());
                }
            }
            Up::Room => {
                let room = conn.asked.1;
                assert!(room > 0, "Room answers room outstanding");
                conn.asked = (Read::Nothing, 0);
                let bytes: Box<[u8]> = if conn.sent < conn.plan.send.len() {
                    let end = conn.plan.send.len().min(conn.sent + len(room));
                    let chunk = Box::from(&conn.plan.send[conn.sent..end]);
                    conn.sent = end;
                    chunk
                } else {
                    let n = conn.echo.len().min(len(room));
                    conn.echo.drain(..n).collect()
                };
                conn.handed += bytes.len() as u64;
                requests.push_back(Request::Stream { stream: socket, down: Down::Send(bytes) });
            }
            Up::End => conn.ended = true,
            Up::Failed(fault) => conn.failed = Some(fault),
        }
    }

    /// What the connection's plan asks for now: a close, a finish, a demand.
    fn follow(&mut self, owner: Token, now: Time, requests: &mut VecDeque<Request>) {
        let conn = self.conns.get_mut(&owner).expect("a connection of this owner");
        let Some(socket) = conn.socket else {
            return;
        };
        match conn.phase {
            Phase::Connecting => {
                if let When::At(at) = conn.plan.close
                    && now >= at
                {
                    close(conn, socket, requests);
                }
                return;
            }
            Phase::Open => {}
            Phase::Asked | Phase::Closing | Phase::Closed => return,
        }
        if conn.closes(now) {
            close(conn, socket, requests);
            return;
        }
        if conn.failed.is_some() {
            return;
        }
        if conn.plan.finish && !conn.finished && conn.sent_all() {
            conn.finished = true;
            requests.push_back(Request::Stream { stream: socket, down: Down::Finish });
            if conn.closes(now) {
                close(conn, socket, requests);
                return;
            }
        }
        // The next demand only once the last is answered, never in place of
        // it (lib.md, 7).
        if conn.waits() {
            return;
        }
        let read = if conn.reading(now) {
            match conn.left() {
                Some(0) => Read::Fill(1),
                Some(left) => draw(&mut self.rng, conn.plan.reads, left),
                None => draw(&mut self.rng, conn.plan.reads, usize::MAX),
            }
        } else {
            Read::Nothing
        };
        let room = if !conn.finished && conn.has_to_send() && conn.received.len() >= len(conn.plan.wait) {
            let left =
                if conn.sent < conn.plan.send.len() { conn.plan.send.len() - conn.sent } else { conn.echo.len() };
            u32::try_from(left.min(len(conn.plan.chunk))).expect("a chunk fits a u32")
        } else {
            0
        };
        // A state that wants nothing more states nothing.
        if (read, room) != (Read::Nothing, 0) {
            conn.asked = (read, room);
            requests.push_back(Request::Stream { stream: socket, down: Down::Demand { read, room } });
        }
    }
}

/// An address of `addr`'s host where nothing listens: port 1.
fn nowhere(addr: Addr) -> Addr {
    Addr::new(addr.ip(), 1)
}

fn close(conn: &mut Conn, socket: Token, requests: &mut VecDeque<Request>) {
    conn.phase = Phase::Closing;
    requests.push_back(if conn.plan.abort {
        Request::Abort { entity: socket }
    } else {
        Request::Close { entity: socket }
    });
}

fn close_listener(listener: &mut Listener, requests: &mut VecDeque<Request>) {
    let token = listener.token.expect("a listener is closed once it listens");
    listener.phase = Phase::Closing;
    requests.push_back(Request::Close { entity: token });
}

/// The next read a plan demands. Mixed reads pause now and then: no read
/// demand, which a later event or tick replaces.
fn draw(rng: &mut Rng, reads: Reads, left: usize) -> Read {
    let cap = |max: u32| max.min(u32::try_from(left).unwrap_or(u32::MAX));
    match reads {
        Reads::Fill(max) => Read::Fill(upto(rng, cap(max))),
        Reads::Scan(until, max) => {
            let least = u32::try_from(until.as_bytes().len()).expect("a delimiter is a few bytes");
            Read::Scan { until, max: cap(max).max(least) }
        }
        Reads::Mixed(max) | Reads::From(_, max) => {
            let max = cap(max);
            match rng.below(5) {
                0 => Read::Fill(upto(rng, max)),
                1 => Read::Scan { until: Delimiter::LF, max: upto(rng, max) },
                2 if max >= 2 => Read::Scan { until: Delimiter::CRLF, max: upto(rng, max).max(2) },
                3 => Read::Line { max: upto(rng, max) },
                _ => Read::Fill(1),
            }
        }
        Reads::Never => Read::Nothing,
    }
}

/// A number from 1 to `max`.
fn upto(rng: &mut Rng, max: u32) -> u32 {
    u32::try_from(rng.between(1, u64::from(max))).expect("at most a u32")
}

fn len(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits a usize")
}
