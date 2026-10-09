//! io's step tests (testing-strategy.md, 2.1): the kernel records' rules,
//! and the cells of the listener's and the stream's transition tables (io.md,
//! 3.2 and 3.3) driven by hand, refusals at full slabs, and stale tokens.
//! Every entry point is called with exactly its `MAX_OUT` of room, so one that
//! emits more fails the test.

#![expect(clippy::disallowed_types, reason = "what a step emitted is collected in Vecs, for the test to look at")]

mod append;
mod file;
mod kernel;
mod layer;
mod listener;
mod output;
mod process;
mod signals;
mod store;
mod stream;

use alloc::vec::Vec;
use core::net::{Ipv4Addr, SocketAddr};

use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

use crate::kernel::{Addr, Complete, Done, Error, Fd, Op, Submit};
use crate::{Event, Io, Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, Request};

/// Tiny limits, so that every admission point is in reach.
pub(crate) const fn limits() -> Limits {
    Limits {
        sockets: 2,
        refusals: 2,
        intake: 8,
        receive: 4,
        output: 8,
        sends: 2,
        accepts: 1,
        backlog: 4,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    }
}

/// `127.0.0.1:port`.
pub(crate) fn local(port: u16) -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// The token an owner above names its entity `n` by.
pub(crate) const fn owner(n: u64) -> Token {
    Token::new(1000_u64.saturating_add(n))
}

/// What an operation carries: a receive's buffer, or a send's bytes and the
/// offset it sends from.
pub(crate) fn buffer(op: &Op) -> (&[u8], u32) {
    match op {
        Op::Recv { buf, .. } => (buf, 0),
        Op::Send { bytes, from, .. } => (bytes, *from),
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Append { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Cancel { .. } => panic!("no buffer in {op:?}"),
    }
}

/// The kinds of operation, to pick a submission out by.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
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
    Spawn,
    Wait,
    Signal,
    Usage,
    ReadSignal,
    PipeRead,
    PipeWrite,
    Append,
}

pub(crate) const fn kind(op: &Op) -> Kind {
    match op {
        Op::Socket { .. } => Kind::Socket,
        Op::Bind { .. } => Kind::Bind,
        Op::Listen { .. } => Kind::Listen,
        Op::Accept { .. } => Kind::Accept,
        Op::Connect { .. } => Kind::Connect,
        Op::Recv { .. } => Kind::Recv,
        Op::Send { .. } => Kind::Send,
        Op::Shutdown { .. } => Kind::Shutdown,
        Op::Close { .. } => Kind::Close,
        Op::Cancel { .. } => Kind::Cancel,
        Op::Spawn { .. } => Kind::Spawn,
        Op::Wait { .. } => Kind::Wait,
        Op::Signal { .. } => Kind::Signal,
        Op::Usage => Kind::Usage,
        Op::ReadSignal { .. } => Kind::ReadSignal,
        Op::PipeRead { .. } => Kind::PipeRead,
        Op::PipeWrite { .. } => Kind::PipeWrite,
        Op::Append { .. } => Kind::Append,
        Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. } => {
            panic!("io submits no operation on files yet (io.md, 9)")
        }
    }
}

/// What one or more calls emitted.
#[derive(Debug)]
pub(crate) struct Out {
    pub(crate) events: Vec<Event>,
    pub(crate) subs: Vec<Submit>,
}

impl Out {
    fn new() -> Out {
        Out { events: Vec::new(), subs: Vec::new() }
    }

    fn drain(&mut self, up: &mut Queue<Event>, subs: &mut Queue<Submit>) {
        while let Some(event) = up.pop() {
            self.events.push(event);
        }
        while let Some(submit) = subs.pop() {
            self.subs.push(submit);
        }
    }

    /// The one submission of `kind`, taken out.
    pub(crate) fn take(&mut self, wanted: Kind) -> Submit {
        let mut found = Vec::new();
        for (at, submit) in self.subs.iter().enumerate() {
            if kind(&submit.kind) == wanted {
                found.push(at);
            }
        }
        assert_eq!(found.len(), 1, "one {wanted:?} among {:?}", self.kinds());
        self.subs.remove(found[0])
    }

    pub(crate) fn kinds(&self) -> Vec<Kind> {
        let mut kinds = Vec::new();
        for submit in &self.subs {
            kinds.push(kind(&submit.kind));
        }
        kinds
    }

    /// Nothing emitted at all.
    pub(crate) fn nothing(&self) {
        assert!(self.events.is_empty() && self.subs.is_empty(), "nothing emitted: {self:?}");
    }
}

/// io, driven by hand as the loop would drive it.
#[derive(Debug)]
pub(crate) struct Rig {
    pub(crate) io: Io,
    pub(crate) env: Env<Limits>,
}

impl Rig {
    pub(crate) fn new(limits: Limits) -> Rig {
        Rig { io: Io::new(&limits), env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits } }
    }

    pub(crate) fn down(&mut self, request: Request) -> Out {
        assert!(self.io.takes(), "the loop hands io a request only while it takes one");
        let mut up = Queue::with_capacity(0);
        let mut subs = Queue::with_capacity(MAX_OUT_DOWN.submissions);
        crate::down(&mut self.io, &self.env, request, &mut subs);
        let mut out = Out::new();
        out.drain(&mut up, &mut subs);
        out
    }

    /// The completion of `submit`, handed back with `result`.
    pub(crate) fn complete(&mut self, submit: Submit, result: Result<Done, Error>) -> Out {
        assert!(!self.io.is_ready(), "the loop drains the ready list before it hands io completions");
        let mut up = Queue::with_capacity(MAX_OUT_UP.events);
        let mut subs = Queue::with_capacity(MAX_OUT_UP.submissions);
        let complete = Complete { op: submit.op, kind: submit.kind, result };
        crate::up(&mut self.io, &self.env, complete, &mut up, &mut subs);
        let mut out = Out::new();
        out.drain(&mut up, &mut subs);
        out
    }

    /// The reclaim point, then the next up pass's ready list, drained.
    pub(crate) fn next(&mut self) -> Out {
        self.io.reclaim();
        let mut out = Out::new();
        while self.io.is_ready() {
            let mut up = Queue::with_capacity(MAX_OUT_RESUME.events);
            let mut subs = Queue::with_capacity(MAX_OUT_RESUME.submissions);
            crate::resume(&mut self.io, &self.env, &mut up, &mut subs);
            out.drain(&mut up, &mut subs);
        }
        out
    }

    /// Time passes to `now`, and the close deadlines due fire.
    pub(crate) fn at(&mut self, now: Time) -> Out {
        self.env.now = now;
        let mut out = Out::new();
        while self.io.is_due(now) {
            let mut up = Queue::with_capacity(MAX_OUT_FIRE.events);
            let mut subs = Queue::with_capacity(MAX_OUT_FIRE.submissions);
            crate::fire(&mut self.io, &self.env, &mut up, &mut subs);
            out.drain(&mut up, &mut subs);
        }
        out
    }

    /// io holds nothing once the iteration ends.
    pub(crate) fn empty(&mut self) {
        self.io.reclaim();
        assert!(self.io.is_empty(), "every socket closed and reclaimed, nothing in flight: {:?}", self.io);
    }
}

/// A listener of `owner` listening at a port of the kernel's choice on the
/// descriptor `fd`: its token, its address and its accept, armed.
pub(crate) fn listening(rig: &mut Rig, owner: Token, fd: Fd) -> (Token, Addr, Submit) {
    let socket = rig.down(Request::Listen { owner, addr: local(0) }).take(Kind::Socket);
    let bind = rig.complete(socket, Ok(Done::Fd(fd))).take(Kind::Bind);
    let addr = local(40000);
    let listen = rig.complete(bind, Ok(Done::Bound(addr))).take(Kind::Listen);
    let mut out = rig.complete(listen, Ok(Done::Nothing));
    let accept = out.take(Kind::Accept);
    let listener = match out.events.as_slice() {
        [Event::Listening { owner: told, listener, addr: bound }] => {
            assert_eq!((*told, *bound), (owner, addr), "told its owner, with the address bound");
            *listener
        }
        other => panic!("a listener is told Listening: {other:?}"),
    };
    (listener, addr, accept)
}

/// A connection of `owner`, made on the descriptor `fd`: its token and its
/// first receive, in flight.
pub(crate) fn connected(rig: &mut Rig, owner: Token, fd: Fd) -> (Token, Submit) {
    let socket = rig.down(Request::Connect { owner, addr: local(80) }).take(Kind::Socket);
    let told = rig.next();
    let token = match told.events.as_slice() {
        [Event::Connecting { owner: told, socket }] if *told == owner => *socket,
        other => panic!("a connect is told Connecting first: {other:?}"),
    };
    let connect = rig.complete(socket, Ok(Done::Fd(fd))).take(Kind::Connect);
    let mut out = rig.complete(connect, Ok(Done::Nothing));
    assert_eq!(out.events, [Event::Connected { owner }]);
    let recv = out.take(Kind::Recv);
    (token, recv)
}

/// A receive's completion with `bytes` in its buffer.
pub(crate) fn filled(rig: &mut Rig, recv: Submit, bytes: &[u8]) -> Out {
    let (op, fd, mut buf) = match recv {
        Submit { op, kind: Op::Recv { fd, buf } } => (op, fd, buf),
        other => panic!("a receive: {other:?}"),
    };
    assert!(bytes.len() <= buf.len(), "a receive fills no more than its buffer");
    for (slot, byte) in buf.iter_mut().zip(bytes) {
        *slot = *byte;
    }
    let n = u32::try_from(bytes.len()).unwrap();
    rig.complete(Submit { op, kind: Op::Recv { fd, buf } }, Ok(Done::Count(n)))
}
