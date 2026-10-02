//! A client and a server, each a process, scripted as small state machines
//! that react to completions the way io will: bind port 0, listen, connect,
//! accept, exchange bytes both ways with short sends continued from where
//! they stopped, half-close, close. Run calm, and under chaos for many seeds.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use skein_io::kernel::{Addr, Complete, Done, Error, Family, Fd, Op, Submit};
use skein_lib::{Queue, Rng, Token};
use skein_sim::{Config, Entry, Pid, Sim};

use crate::support::local;

/// What an operation in flight was for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Purpose {
    Socket,
    Bind,
    Listen,
    Accept,
    Connect,
    Recv,
    Send,
    Shutdown,
    Close,
    Cancel,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Server,
    Client,
}

#[expect(clippy::struct_excessive_bools, reason = "a script's flags, read one at a time")]
struct Actor {
    pid: Pid,
    role: Role,
    next: u64,
    flights: BTreeMap<Token, Purpose>,
    out: Vec<Submit>,
    listener: Option<Fd>,
    conn: Option<Fd>,
    accept: Option<Token>,
    /// Where the client connects, once the server has bound.
    target: Option<Addr>,
    message: Box<[u8]>,
    sent: u32,
    received: Vec<u8>,
    /// The connection is up, and the two directions run.
    streaming: bool,
    reading: bool,
    writing: bool,
    /// The connection failed or broke: what arrived may be a prefix.
    broken: bool,
    /// The client could not connect; the server stops waiting for it.
    gave_up: bool,
    closing: bool,
    done: bool,
}

impl Actor {
    fn new(pid: Pid, role: Role, message: Box<[u8]>) -> Actor {
        Actor {
            pid,
            role,
            next: 1,
            flights: BTreeMap::new(),
            out: Vec::new(),
            listener: None,
            conn: None,
            accept: None,
            target: None,
            message,
            sent: 0,
            received: Vec::new(),
            streaming: false,
            reading: false,
            writing: false,
            broken: false,
            gave_up: false,
            closing: false,
            done: false,
        }
    }

    fn submit(&mut self, purpose: Purpose, op: Op) -> Token {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).unwrap();
        self.flights.insert(token, purpose);
        self.out.push(Submit { op: token, kind: op });
        token
    }

    fn in_flight_on_conn(&self) -> bool {
        for purpose in self.flights.values() {
            match purpose {
                Purpose::Recv | Purpose::Send | Purpose::Shutdown | Purpose::Connect => return true,
                Purpose::Socket
                | Purpose::Bind
                | Purpose::Listen
                | Purpose::Accept
                | Purpose::Close
                | Purpose::Cancel => {}
            }
        }
        false
    }

    fn start(&mut self) {
        if self.role == Role::Server {
            self.submit(Purpose::Socket, Op::Socket { family: Family::Ipv4 });
        }
    }

    /// Connects once both the socket and the server's address are known.
    fn connect_to(&mut self, addr: Option<Addr>) {
        if addr.is_some() {
            self.target = addr;
        }
        if let (Some(fd), Some(addr)) = (self.conn, self.target) {
            self.target = None;
            self.submit(Purpose::Connect, Op::Connect { fd, addr });
        }
    }

    fn recv(&mut self) {
        let fd = self.conn.expect("a connection");
        self.submit(Purpose::Recv, Op::Recv { fd, buf: vec![0; 97].into_boxed_slice() });
    }

    fn send(&mut self) {
        let fd = self.conn.expect("a connection");
        let bytes = self.message.clone();
        self.submit(Purpose::Send, Op::Send { fd, bytes, from: self.sent });
    }

    fn stream(&mut self) {
        self.streaming = true;
        self.reading = true;
        self.recv();
        self.writing = true;
        if self.message.is_empty() {
            self.submit(Purpose::Shutdown, Op::Shutdown { fd: self.conn.expect("a connection") });
        } else {
            self.send();
        }
    }

    /// Closes the connection once both directions are done and nothing is in
    /// flight on it: a close beside anything else breaks the contract.
    fn maybe_close(&mut self) {
        if !self.streaming || self.reading || self.writing || self.closing || self.in_flight_on_conn() {
            return;
        }
        if let Some(fd) = self.conn {
            self.closing = true;
            self.submit(Purpose::Close, Op::Close { fd });
        }
    }

    /// Reacts to one completion; answers the bound address, for the client.
    fn on(&mut self, complete: Complete) -> Option<Addr> {
        let purpose = self.flights.remove(&complete.op).expect("a completion of an operation in flight");
        let mut bound = None;
        match (purpose, complete.kind, complete.result) {
            (Purpose::Socket, _, Ok(Done::Fd(fd))) => match self.role {
                Role::Server => {
                    self.listener = Some(fd);
                    self.submit(Purpose::Bind, Op::Bind { fd, addr: local(0) });
                }
                Role::Client => {
                    self.conn = Some(fd);
                    self.connect_to(None);
                }
            },
            (Purpose::Bind, _, Ok(Done::Bound(addr))) => {
                bound = Some(addr);
                let fd = self.listener.expect("bound a listener");
                self.submit(Purpose::Listen, Op::Listen { fd, backlog: 4 });
            }
            (Purpose::Listen, _, Ok(Done::Nothing)) => {
                let fd = self.listener.expect("a listener");
                self.accept = Some(self.submit(Purpose::Accept, Op::Accept { fd }));
            }
            (Purpose::Accept, _, result) => {
                self.accept = None;
                let listener = self.listener.take().expect("a listener");
                self.submit(Purpose::Close, Op::Close { fd: listener });
                match result {
                    Ok(Done::Accepted { fd, .. }) => {
                        self.conn = Some(fd);
                        self.stream();
                    }
                    Err(Error::Cancelled) => {
                        self.broken = true;
                        self.done_unless_closing();
                    }
                    other => panic!("an accept: {other:?}"),
                }
            }
            (Purpose::Connect, _, Ok(Done::Nothing)) => self.stream(),
            (Purpose::Connect, _, Err(error)) => {
                assert_eq!(error, Error::Refused, "a loopback connect fails only as refused");
                self.broken = true;
                self.gave_up = true;
                let fd = self.conn.expect("a socket");
                self.closing = true;
                self.submit(Purpose::Close, Op::Close { fd });
            }
            (Purpose::Recv, Op::Recv { buf, .. }, Ok(Done::Count(0))) => {
                drop(buf);
                self.reading = false;
            }
            (Purpose::Recv, Op::Recv { buf, .. }, Ok(Done::Count(n))) => {
                self.received.extend_from_slice(&buf[..usize::try_from(n).unwrap()]);
                self.recv();
            }
            (Purpose::Recv, _, Err(error)) => {
                assert_eq!(error, Error::Reset, "a receive fails only by a reset here");
                self.broken = true;
                self.reading = false;
            }
            (Purpose::Send, _, Ok(Done::Count(n))) => {
                self.sent = self.sent.checked_add(n).unwrap();
                if usize::try_from(self.sent).unwrap() < self.message.len() {
                    self.send();
                } else {
                    let fd = self.conn.expect("a connection");
                    self.submit(Purpose::Shutdown, Op::Shutdown { fd });
                }
            }
            (Purpose::Send, _, Err(error)) => {
                assert!(matches!(error, Error::Reset | Error::BrokenPipe), "a send fails by a reset: {error:?}");
                self.broken = true;
                self.writing = false;
            }
            (Purpose::Shutdown, _, result) => {
                match result {
                    Ok(Done::Nothing) => {}
                    Err(Error::NotConnected) => self.broken = true,
                    other => panic!("a shutdown: {other:?}"),
                }
                self.writing = false;
            }
            (Purpose::Close, _, Ok(Done::Nothing)) => {
                if self.closing {
                    self.closing = false;
                    self.conn = None;
                    self.done = true;
                }
            }
            (Purpose::Cancel, _, result) => {
                assert!(matches!(result, Ok(Done::Nothing) | Err(Error::TooLate)), "a cancel: {result:?}");
            }
            (purpose, kind, result) => panic!("{purpose:?} answered {kind:?} with {result:?}"),
        }
        self.maybe_close();
        bound
    }

    fn done_unless_closing(&mut self) {
        if self.conn.is_none() {
            self.done = true;
        }
    }

    /// The world is idle: a server still waiting for a client that gave up
    /// cancels its accept, as io would on its deadline.
    fn on_idle(&mut self, client_gave_up: bool) -> bool {
        match self.accept {
            Some(accept) if client_gave_up && !self.flights.values().any(|p| *p == Purpose::Cancel) => {
                self.submit(Purpose::Cancel, Op::Cancel { target: accept });
                true
            }
            _ => false,
        }
    }

    fn flush(&mut self, sim: &mut Sim) {
        if self.out.is_empty() {
            return;
        }
        let mut queue = Queue::with_capacity(u32::try_from(self.out.len()).unwrap());
        for submit in self.out.drain(..) {
            queue.push(submit);
        }
        sim.submit(self.pid, &mut queue);
    }

    fn reap(&mut self, sim: &mut Sim) -> Vec<Complete> {
        let mut queue = Queue::with_capacity(8);
        sim.reap(self.pid, &mut queue);
        let mut out = Vec::new();
        while let Some(complete) = queue.pop() {
            out.push(complete);
        }
        out
    }
}

/// What one run of the exchange left behind.
pub struct Outcome {
    pub trace: Vec<Entry>,
    pub to_server: Vec<u8>,
    pub to_client: Vec<u8>,
    pub broken: bool,
}

fn message(rng: &mut Rng, most: u64) -> Box<[u8]> {
    let len = usize::try_from(rng.below(most.checked_add(1).unwrap())).unwrap();
    let mut bytes = vec![0_u8; len];
    for byte in &mut bytes {
        *byte = u8::try_from(rng.below(256)).unwrap();
    }
    bytes.into_boxed_slice()
}

/// Runs the exchange to its end and checks it: what each side received is
/// what the other sent, or a prefix of it when the connection broke.
pub fn run(seed: u64, config: Config) -> Outcome {
    let mut sim = Sim::new(seed, config);
    let mut rng = Rng::new(seed ^ 0x5eed);
    let mut server = Actor::new(sim.spawn_process(), Role::Server, message(&mut rng, 3000));
    let mut client = Actor::new(sim.spawn_process(), Role::Client, message(&mut rng, 3000));
    server.start();
    client.submit(Purpose::Socket, Op::Socket { family: Family::Ipv4 });
    let mut steps = 0_u32;
    loop {
        steps = steps.checked_add(1).unwrap();
        assert!(steps < 200_000, "the exchange ends\n{}", sim.render_trace());
        server.flush(&mut sim);
        client.flush(&mut sim);
        let mut progressed = false;
        for complete in server.reap(&mut sim) {
            progressed = true;
            if let Some(addr) = server.on(complete) {
                client.connect_to(Some(addr));
            }
        }
        for complete in client.reap(&mut sim) {
            progressed = true;
            assert!(client.on(complete).is_none(), "only the server binds");
        }
        if server.done && client.done && server.flights.is_empty() && client.flights.is_empty() {
            break;
        }
        if progressed || !server.out.is_empty() || !client.out.is_empty() || sim.advance() {
            continue;
        }
        assert!(server.on_idle(client.gave_up), "the world is idle before the exchange ended\n{}", sim.render_trace());
    }
    for pid in [server.pid, client.pid] {
        sim.assert_quiescent(pid);
        sim.assert_no_open_fds(pid);
    }
    let broken = server.broken || client.broken;
    for (received, sent) in [(&server.received, &client.message), (&client.received, &server.message)] {
        assert!(sent.starts_with(received), "bytes arrive intact and in order\n{}", sim.render_trace());
        if !broken {
            assert_eq!(received.len(), sent.len(), "every byte arrives\n{}", sim.render_trace());
        }
    }
    Outcome { trace: sim.trace().to_vec(), to_server: server.received, to_client: client.received, broken }
}

#[test]
fn a_calm_exchange_delivers_every_byte_both_ways() {
    for seed in 0..20_u64 {
        let outcome = run(seed, Config::calm());
        assert!(!outcome.broken, "nothing breaks in a calm world");
    }
}

#[test]
fn a_chaotic_exchange_keeps_every_invariant_for_many_seeds() {
    let mut broken = 0_u32;
    for seed in 0..200_u64 {
        if run(seed, Config::chaos()).broken {
            broken = broken.checked_add(1).unwrap();
        }
    }
    assert!(broken > 0, "chaos breaks some connections");
    assert!(broken < 100, "and most of them deliver everything: {broken}");
}

#[test]
fn the_same_seed_replays_to_the_same_trace() {
    for seed in [3_u64, 17, 4242] {
        let first = run(seed, Config::chaos());
        let second = run(seed, Config::chaos());
        assert!(first.trace == second.trace, "seed {seed} replays");
        assert_eq!((first.to_server, first.to_client), (second.to_server, second.to_client));
    }
    let a = run(1, Config::chaos()).trace;
    let b = run(2, Config::chaos()).trace;
    assert!(a != b, "another seed, another run");
}
