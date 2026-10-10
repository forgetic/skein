//! A native output conversation over real simulated socket operations.
//! The client demands its reply before sending the request; both sides would
//! deadlock if that unanswered read prevented an independent output grant.

use std::net::{Ipv4Addr, SocketAddr};

use skein_io::kernel::{Complete, Submit};
use skein_io::{Event, Io, Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, Request};
use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_sim::{Config, Entry, Sim};

const LISTENER: Token = Token::new(1);
const CLIENT: Token = Token::new(2);
const SERVER: Token = Token::new(3);
const CLIENT_RIGHT: Token = Token::new(10);
const SERVER_RIGHT: Token = Token::new(11);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    New,
    Reading,
    Received,
    Ended,
    Closed,
}

#[derive(Debug)]
struct Peer {
    socket: Option<Token>,
    phase: Phase,
    pending: Option<Token>,
    granted: Option<Token>,
    terminals: u32,
}

impl Peer {
    fn new() -> Peer {
        Peer { socket: None, phase: Phase::New, pending: None, granted: None, terminals: 0 }
    }
}

/// Actual kernel trace and domain-independent IO events, for exact replay.
#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Test-only observation of actual simulated kernel operations in execution
    /// order, recorded by `Sim`; see `io.md` §3.3 and `lib.md` §7.1.
    pub trace: Vec<Entry>,

    /// Test-only observation of actual events emitted by `Io` in delivery order,
    /// including independent output terminals; see `io.md` §3.3 and `lib.md` §7.1.
    pub events: Vec<Event>,

    /// Driver iterations through the settled return, in `1..=20_000`; the test
    /// driver asserts this bound. See `io.md` §3.3 and `lib.md` §7.1.
    pub iterations: u32,
}

struct Driver {
    io: Io,
    env: Env<Limits>,
    subs: Queue<Submit>,
    up: Queue<Event>,
    requests: Queue<Request>,
    completions: Queue<Complete>,
    client: Peer,
    server: Peer,
    listener: Option<Token>,
    listener_closed: bool,
    log: Vec<Event>,
}

// Preserve the actual socket event for replay without copying process-owned
// records. This world's only owning event payload is a read of at most four
// bytes; the receiving handler also checks the exact request or reply.
fn observe(event: &Event) -> Event {
    match event {
        Event::Listening { owner, listener, addr } => {
            Event::Listening { owner: *owner, listener: *listener, addr: *addr }
        }
        Event::Accepted { owner, socket, peer } => Event::Accepted { owner: *owner, socket: *socket, peer: *peer },
        Event::Connecting { owner, socket } => Event::Connecting { owner: *owner, socket: *socket },
        Event::Connected { owner } => Event::Connected { owner: *owner },
        Event::Stream { owner, up } => {
            let observed = match up {
                Up::Bytes(bytes) => {
                    assert!(bytes.len() <= 4, "the actual socket read is bounded by the request payload");
                    Up::Bytes(Box::from(bytes.as_ref()))
                }
                Up::Room => Up::Room,
                Up::End => Up::End,
                Up::Failed(fault) => Up::Failed(*fault),
            };
            Event::Stream { owner: *owner, up: observed }
        }
        Event::Output { owner, up } => Event::Output { owner: *owner, up: *up },
        Event::Failed { owner, error } => Event::Failed { owner: *owner, error: *error },
        Event::Closed { owner } => Event::Closed { owner: *owner },
        Event::Spawned { .. } | Event::Exited { .. } | Event::Usage { .. } | Event::Shutdown { .. } => {
            panic!("socket observation owns no child records")
        }
    }
}

impl Driver {
    fn new() -> Driver {
        let limits = Limits {
            sockets: 3,
            refusals: 1,
            intake: 8,
            receive: 3,
            output: 8,
            sends: 1,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        };
        let mut driver = Driver {
            io: Io::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            subs: Queue::with_capacity(64),
            up: Queue::with_capacity(64),
            requests: Queue::with_capacity(64),
            completions: Queue::with_capacity(64),
            client: Peer::new(),
            server: Peer::new(),
            listener: None,
            listener_closed: false,
            log: Vec::new(),
        };
        driver.requests.push(Request::Listen { owner: LISTENER, addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)) });
        driver
    }

    fn room(&self, max: MaxOut) -> bool {
        self.up.room() >= max.events && self.subs.room() >= max.submissions
    }

    fn mark(&self) -> (u32, u32) {
        (self.up.len(), self.subs.len())
    }

    fn within(&self, before: (u32, u32), max: MaxOut) {
        assert!(self.up.len().checked_sub(before.0).expect("events only append") <= max.events);
        assert!(self.subs.len().checked_sub(before.1).expect("submissions only append") <= max.submissions);
    }

    fn request_read(&mut self, owner: Token, bytes: u32) {
        let peer = self.peer(owner);
        assert_eq!(peer.phase, Phase::New, "one actual classic read demand");
        peer.phase = Phase::Reading;
        let socket = peer.socket.expect("connected or bound peer");
        self.requests.push(Request::Stream { stream: socket, down: Down::Demand { read: Read::Fill(bytes), room: 0 } });
    }

    fn request_output(&mut self, owner: Token, right: Token) {
        let peer = self.peer(owner);
        assert!(peer.pending.is_none() && peer.granted.is_none(), "one live output obligation");
        peer.pending = Some(right);
        let socket = peer.socket.expect("connected or bound peer");
        self.requests.push(Request::Output { stream: socket, down: OutputDown::Room { right, bytes: 8 } });
    }

    fn peer(&mut self, owner: Token) -> &mut Peer {
        if owner == CLIENT {
            &mut self.client
        } else {
            assert_eq!(owner, SERVER, "only two concrete peers");
            &mut self.server
        }
    }

    fn on(&mut self, event: Event) {
        self.log.push(observe(&event));
        match event {
            Event::Listening { owner, listener, addr } => {
                assert_eq!(owner, LISTENER);
                self.listener = Some(listener);
                self.requests.push(Request::Connect { owner: CLIENT, addr });
            }
            Event::Connecting { owner, socket } => {
                assert_eq!(owner, CLIENT);
                self.client.socket = Some(socket);
            }
            Event::Accepted { owner, socket, peer: _ } => {
                assert_eq!(owner, LISTENER);
                self.server.socket = Some(socket);
                self.requests.push(Request::Bind { socket, owner: SERVER });
                self.request_read(SERVER, 4);
                self.requests.push(Request::Close { entity: self.listener.expect("actual listener") });
            }
            Event::Connected { owner } => {
                assert_eq!(owner, CLIENT);
                self.request_read(CLIENT, 2);
                self.request_output(CLIENT, CLIENT_RIGHT);
            }
            Event::Output { owner, up: OutputUp::Settled { right, outcome } } => self.output(owner, right, outcome),
            Event::Stream { owner, up } => self.stream(owner, up),
            Event::Closed { owner } => {
                if owner == LISTENER {
                    assert!(!self.listener_closed);
                    self.listener_closed = true;
                } else {
                    let peer = self.peer(owner);
                    assert_eq!(peer.phase, Phase::Ended);
                    assert!(
                        peer.pending.is_none() && peer.granted.is_none(),
                        "no invented output settlement at Closed"
                    );
                    peer.phase = Phase::Closed;
                }
            }
            Event::Failed { owner, error } => panic!("positive native owner {owner:?} failed: {error:?}"),
            Event::Spawned { .. } | Event::Exited { .. } | Event::Usage { .. } | Event::Shutdown { .. } => {
                panic!("socket conversation owns no child")
            }
        }
    }

    fn output(&mut self, owner: Token, right: Token, outcome: OutputOutcome) {
        let peer = self.peer(owner);
        assert_eq!(peer.pending.take(), Some(right), "one exact winning terminal");
        peer.terminals = peer.terminals.checked_add(1).expect("two bounded output terminals");
        assert_eq!(outcome, OutputOutcome::Granted, "positive scene must send both exact payloads");
        assert!(peer.granted.replace(right).is_none());
        let socket = peer.socket.expect("live output owner");
        assert_eq!(peer.granted.take(), Some(right), "the exact grant moves one box");
        let bytes: Box<[u8]> = if owner == CLIENT {
            assert_eq!(peer.phase, Phase::Reading, "Room progresses behind the unanswered reply read");
            Box::from(&b"ping"[..])
        } else {
            assert_eq!(peer.phase, Phase::Received, "reply follows the complete actual request");
            Box::from(&b"ok"[..])
        };
        self.requests.push(Request::Output { stream: socket, down: OutputDown::Send { right, bytes } });
        if owner == SERVER {
            self.requests.push(Request::Stream { stream: socket, down: Down::Finish });
        }
    }

    fn stream(&mut self, owner: Token, up: Up) {
        match up {
            Up::Bytes(bytes) => {
                let peer = self.peer(owner);
                assert_eq!(peer.phase, Phase::Reading, "one demanded complete record");
                peer.phase = Phase::Received;
                if owner == CLIENT {
                    assert_eq!(bytes.as_ref(), b"ok");
                    let socket = peer.socket.expect("actual client socket");
                    self.requests.push(Request::Stream { stream: socket, down: Down::Finish });
                } else {
                    assert_eq!(bytes.as_ref(), b"ping");
                    self.request_output(SERVER, SERVER_RIGHT);
                }
            }
            Up::End => {
                let peer = self.peer(owner);
                assert_eq!(peer.phase, Phase::Received, "EOF follows the actual complete payload once");
                peer.phase = Phase::Ended;
                let socket = peer.socket.expect("actual closing socket");
                self.requests.push(Request::Close { entity: socket });
            }
            Up::Room => panic!("independent grants never answer classic demand"),
            Up::Failed(fault) => panic!("positive native conversation failed: {fault:?}"),
        }
    }

    fn iterate(&mut self) {
        while self.io.is_ready() && self.room(MAX_OUT_RESUME) {
            let before = self.mark();
            skein_io::resume(&mut self.io, &self.env, &mut self.up, &mut self.subs);
            self.within(before, MAX_OUT_RESUME);
        }
        if !self.io.is_ready() {
            while self.room(MAX_OUT_UP) {
                let Some(completion) = self.completions.pop() else { break };
                let before = self.mark();
                skein_io::up(&mut self.io, &self.env, completion, &mut self.up, &mut self.subs);
                self.within(before, MAX_OUT_UP);
            }
        }
        while self.io.is_due(self.env.now) && self.room(MAX_OUT_FIRE) {
            let before = self.mark();
            skein_io::fire(&mut self.io, &self.env, &mut self.up, &mut self.subs);
            self.within(before, MAX_OUT_FIRE);
        }
        while let Some(event) = self.up.pop() {
            self.on(event);
        }
        while self.io.takes() && self.room(MAX_OUT_DOWN) {
            let Some(request) = self.requests.pop() else { break };
            let before = self.mark();
            skein_io::down(&mut self.io, &self.env, request, &mut self.subs);
            self.within(before, MAX_OUT_DOWN);
        }
        self.io.reclaim();
    }
}

/// Both concrete payloads complete behind independent output rights, with
/// exact replay and every actual IO/kernel lifetime settled.
///
/// This test driver returns only after both peers and the listener have closed,
/// each peer has one actual output terminal, IO and driver queues are empty,
/// and the simulated process has no pending operations or open descriptors.
/// It asserts the positive conversation's payload and lifetime contracts and
/// the limit of `20_000` driver iterations; a configuration that cannot complete
/// those contracts panics rather than returning an unsettled observation.
/// See `io.md` §3.3 and `lib.md` §7.1.
#[must_use]
pub fn conversation(seed: u64, config: Config) -> Outcome {
    let mut sim = Sim::new(seed, config);
    let pid = sim.spawn_process();
    let mut driver = Driver::new();
    let mut iterations = 0_u32;
    loop {
        iterations = iterations.checked_add(1).expect("bounded native world iterations");
        assert!(iterations <= 20_000, "native conversation settles: {}", sim.render_trace());
        driver.env.now = sim.now();
        driver.env.wall = sim.wall();
        sim.reap(pid, &mut driver.completions);
        driver.iterate();
        sim.submit(pid, &mut driver.subs);
        if driver.client.phase == Phase::Closed
            && driver.server.phase == Phase::Closed
            && driver.listener_closed
            && driver.io.is_empty()
        {
            assert_eq!((driver.client.terminals, driver.server.terminals), (1, 1));
            assert!(driver.requests.is_empty() && driver.completions.is_empty());
            sim.assert_quiescent(pid);
            sim.assert_no_open_fds(pid);
            return Outcome { trace: sim.trace().to_vec(), events: driver.log, iterations };
        }
        let busy = driver.io.is_ready()
            || !driver.requests.is_empty()
            || !driver.completions.is_empty()
            || sim.deferred(pid)
            || sim.ready(pid) > 0;
        if !busy {
            let next = match sim.next_due() {
                Some(kernel) => match driver.io.next_deadline() {
                    Some(io) => Some(kernel.min(io)),
                    None => Some(kernel),
                },
                None => driver.io.next_deadline(),
            };
            let next = next.expect("unsettled native conversation has real pending work");
            sim.advance_to(next);
        }
    }
}
