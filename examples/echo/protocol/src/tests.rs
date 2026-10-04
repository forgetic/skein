//! The protocol layer's step tests (testing-strategy.md, 2.1): the cells of
//! the connection and the listener (examples.md, 3.4) driven by hand, the
//! refusals at a full slab, the idle deadline and its spread, replies to
//! connections gone, and the worst case. Every entry point is called with
//! exactly its `MAX_OUT` of room, so one that emits more fails the test.

#![expect(clippy::disallowed_types, reason = "what a step emitted is collected in Vecs, for the test to look at")]

mod conn;
mod listener;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::net::{Ipv4Addr, SocketAddr};

use skein_echo_domain::{Event as Call, Reply, Request as Domain};
use skein_io::kernel::Addr;
use skein_io::{Event as Told, Request as Io};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Duration, Env, Queue, ReplyTo, Time, Token, Wall};

use crate::{Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, Protocol, down, fire, resume, up};

/// Tiny limits: two connections, lines of sixteen bytes, a second idle and
/// no spread, so that deadlines are exact.
const LIMITS: Limits = Limits {
    conns: 2,
    line: 16,
    idle: Duration::from_secs(1),
    spread: Duration::ZERO,
    retry: Duration::from_millis(10),
};

const SEED: u64 = 7;

fn addr() -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 7))
}

/// io's token for socket `n`.
const fn socket(n: u64) -> Token {
    Token::new(100_u64.saturating_add(n))
}

/// io's token for the listener.
const LISTENER: Token = Token::new(99);

/// What one or more calls emitted.
#[derive(Debug)]
struct Out {
    calls: Vec<Call>,
    io: Vec<Io>,
}

impl Out {
    fn new() -> Out {
        Out { calls: Vec::new(), io: Vec::new() }
    }

    fn drain(&mut self, up: &mut Queue<Call>, down: &mut Queue<Io>) {
        while let Some(call) = up.pop() {
            self.calls.push(call);
        }
        while let Some(request) = down.pop() {
            self.io.push(request);
        }
    }

    fn nothing(&self) {
        assert!(self.calls.is_empty() && self.io.is_empty(), "nothing emitted: {self:?}");
    }
}

/// The layer, driven by hand.
struct Rig {
    proto: Protocol,
    env: Env<Limits>,
    /// The owner token the layer gave the listener, from its `Listen`.
    owner: Option<Token>,
}

impl Rig {
    fn new() -> Rig {
        Rig::with(LIMITS)
    }

    fn with(limits: Limits) -> Rig {
        Rig {
            proto: Protocol::new(&limits, addr(), SEED),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            owner: None,
        }
    }

    fn at(&mut self, now: Time) {
        self.env.now = now;
    }

    fn queues(max: MaxOut) -> (Queue<Call>, Queue<Io>) {
        (Queue::with_capacity(max.events), Queue::with_capacity(max.requests))
    }

    fn resume(&mut self) -> Out {
        let (mut calls, mut io) = Rig::queues(MAX_OUT_RESUME);
        resume(&mut self.proto, &self.env, &mut calls, &mut io);
        let mut out = Out::new();
        out.drain(&mut calls, &mut io);
        out
    }

    fn up(&mut self, event: Told) -> Out {
        let (mut calls, mut io) = Rig::queues(MAX_OUT_UP);
        up(&mut self.proto, &self.env, event, &mut calls, &mut io);
        let mut out = Out::new();
        out.drain(&mut calls, &mut io);
        out
    }

    fn fire(&mut self) -> Out {
        let (mut calls, mut io) = Rig::queues(MAX_OUT_FIRE);
        fire(&mut self.proto, &self.env, &mut calls, &mut io);
        let mut out = Out::new();
        out.drain(&mut calls, &mut io);
        out
    }

    fn down(&mut self, request: Domain) -> Out {
        let mut io = Queue::with_capacity(MAX_OUT_DOWN);
        down(&mut self.proto, &self.env, request, &mut io);
        let mut out = Out::new();
        out.drain(&mut Queue::with_capacity(0), &mut io);
        out
    }

    /// The ready list drained, one entry at a time.
    fn drain(&mut self) -> Out {
        let mut all = Out::new();
        for _ in 0..8_u32 {
            if !self.proto.is_ready() {
                break;
            }
            let out = self.resume();
            all.calls.extend(out.calls);
            all.io.extend(out.io);
        }
        assert!(!self.proto.is_ready(), "the ready list drains");
        all
    }

    /// The listener listening, as io tells it.
    fn listen(&mut self) {
        let out = self.resume();
        let [Io::Listen { owner, addr: asked }] = out.io.as_slice() else {
            panic!("the first resume listens: {out:?}");
        };
        assert_eq!(*asked, addr(), "at the address it was made with");
        self.owner = Some(*owner);
        let listening = SocketAddr::from((Ipv4Addr::LOCALHOST, 4000));
        self.up(Told::Listening { owner: *owner, listener: LISTENER, addr: listening }).nothing();
        assert_eq!(self.proto.listening(), Some(listening), "the address io bound, port resolved");
    }

    fn listener_owner(&self) -> Token {
        self.owner.expect("listening")
    }

    /// Socket `n` accepted: what the layer answered.
    fn accept(&mut self, n: u64) -> Out {
        let owner = self.listener_owner();
        self.up(Told::Accepted { owner, socket: socket(n), peer: addr() })
    }

    /// Socket `n` accepted and bound: the connection's token.
    fn bound(&mut self, n: u64) -> Token {
        let out = self.accept(n);
        let [Io::Bind { socket: bound, owner }, demand] = out.io.as_slice() else {
            panic!("an accepted socket is bound: {out:?}");
        };
        assert_eq!(*bound, socket(n));
        assert_eq!(*demand, room(n), "greeting asks room for an answer");
        *owner
    }

    /// A connection admitted as `session`, reading.
    fn reading(&mut self, n: u64, session: Token) -> Token {
        let conn = self.bound(n);
        let out = self.up(stream(conn, Up::Room));
        assert_eq!(out.calls, [Call::Open { reply_to: ReplyTo::new(conn) }], "room granted: the domain is asked");
        let out = self.down(admitted(conn, session));
        assert_eq!(out.io, [scan(n)], "admitted: a line is demanded");
        conn
    }

    /// A line read: its call, checked.
    fn line(&mut self, conn: Token, session: Token, line: &[u8]) {
        let out = self.up(stream(conn, Up::Bytes(Box::from(line))));
        let (_end, text) = line.split_last().expect("a line ends with its end of line");
        let call = Call::Line { session, reply_to: ReplyTo::new(conn), text: Box::from(text) };
        assert_eq!(out.calls, [call], "a line is a call");
        assert!(out.io.is_empty(), "no demand while a call is out");
    }

    /// The connection's socket closed by io.
    fn closed(&mut self, conn: Token) -> Out {
        self.up(Told::Closed { owner: conn })
    }
}

fn stream(conn: Token, event: Up) -> Told {
    Told::Stream { owner: conn, up: event }
}

fn admitted(conn: Token, session: Token) -> Domain {
    Domain::Reply { to: ReplyTo::new(conn), reply: Reply::Admitted { session } }
}

fn reply(conn: Token, reply: Reply) -> Domain {
    Domain::Reply { to: ReplyTo::new(conn), reply }
}

/// The demand for room for an answer, on socket `n`.
fn room(n: u64) -> Io {
    Io::Stream { stream: socket(n), down: Down::Demand { read: Read::Nothing, room: LIMITS.line } }
}

/// The demand for a line, on socket `n`.
fn scan(n: u64) -> Io {
    let read = Read::Scan { until: Delimiter::LF, max: LIMITS.line };
    Io::Stream { stream: socket(n), down: Down::Demand { read, room: 0 } }
}

fn send(n: u64, bytes: &[u8]) -> Io {
    Io::Stream { stream: socket(n), down: Down::Send(Box::from(bytes)) }
}

fn close(n: u64) -> Io {
    Io::Close { entity: socket(n) }
}

fn failed() -> Up {
    Up::Failed(Fault::Reset)
}

const fn session(n: u64) -> Token {
    Token::new(500_u64.saturating_add(n))
}
