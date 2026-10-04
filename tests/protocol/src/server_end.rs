//! The server's end (testing-strategy.md, 2.5; http.md, 5 and 6): a fake
//! LLM provider's stack as a service builds it, the HTTP server over the
//! stream, a JSON tokenizer on each request's body, and the event stream
//! writer on its response body, each event's data a document the JSON
//! writer wrote; routed as a connection routes between its machines, each
//! call held to its `MAX_OUT`; and a scripted user at the top, which reads
//! each request and answers with a stream of events, or at once with an
//! error, and closes when it is done or when it is told to.

use std::collections::VecDeque;

use skein_http::server::{self, Body, Response, Server};
use skein_http::sse::writer::{self, Outgoing, Writer};
use skein_http::{Header, MaxOut};
use skein_json::Token;
use skein_json::tokenizer::{self as json, Tokenizer};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Queue, Time, Wall};

use crate::wire::Bottom;

/// The limits of the server's machines.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub server: server::Limits,
    pub writer: writer::Limits,
    pub json: json::Limits,
}

impl Limits {
    /// The stack's startup checks: each machine's largest demand within its
    /// side below's.
    pub fn check(&self) {
        assert!(writer::largest_room(&self.writer) <= self.server.send, "the writer's room within the server's");
        assert!(
            json::largest_demand(&self.json) <= self.server.read,
            "the tokenizer's demands within the server's reads"
        );
    }
}

/// An event as the server's user writes it: its type, and its data.
pub type Written = (Box<[u8]>, Box<[u8]>);

/// What the server's user does.
#[derive(Clone, Debug)]
pub struct Script {
    /// The events it answers with: each one's type and data, a document the
    /// JSON writer wrote.
    pub events: Vec<Written>,
    /// A comment to keep the stream alive before every this many events.
    pub ping: Option<usize>,
    /// An error it answers with at once, before it reads the body: a status
    /// and a document.
    pub early: Option<(u16, Box<[u8]>)>,
    /// From this iteration on, the user closes, whatever its stack is
    /// doing.
    pub close: Option<u64>,
}

/// What the server's user saw.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Seen {
    /// The tokens of each request's body.
    pub requests: Vec<Vec<Token>>,
    /// How many events went down whole, `Sent`.
    pub sent: usize,
    /// The bytes of each event as the writer frames it, for those sent.
    pub framed: u64,
    /// How the stream's failure reached the writer, if it did.
    pub writer_failed: bool,
    /// Each terminal event and each answer to a `Next` with no call.
    pub outcomes: Vec<Outcome>,
    pub closed: bool,
}

/// How an exchange ended, or a `Next` was answered with no call.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    Ended,
    Done(server::Reuse),
    Failed(server::Error),
}

/// The server's end: its machines, the stream below, and the user.
#[derive(Debug)]
pub struct End {
    limits: Limits,
    server: Server,
    tokenizer: Option<Tokenizer>,
    writer: Option<Writer>,
    pub bottom: Bottom,
    script: Script,
    pub seen: Seen,
    tokens: Vec<Token>,
    /// A `Next` outstanding.
    asked: bool,
    exchange: Option<Exchange>,
    /// The connection is not to be used again.
    spent: bool,
    /// The iteration the user last acted at.
    now: u64,
}

/// The user's side of the exchange in progress.
#[derive(Debug)]
struct Exchange {
    responded: bool,
    /// The next event to write.
    next: usize,
    /// What the writer is writing, if anything.
    writing: Option<Item>,
    /// The event a comment went before last.
    pinged: Option<usize>,
    finished: bool,
    /// The early error's body, as the user writes it itself.
    error: Option<(Box<[u8]>, usize, Reply)>,
}

/// What the writer writes for the user.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Item {
    Comment,
    /// The event at this index.
    Event(usize),
}

/// The user's side of its own reply's stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reply {
    Idle,
    Wanted(u32),
    Granted(u32),
    Over,
}

/// What a machine emitted, still to be routed.
#[derive(Debug)]
enum Routed {
    Server(server::Event),
    Tokenizer(json::Event),
    TokenizerBelow(Down),
    Writer(writer::Event),
    WriterBelow(Down),
}

type Work = VecDeque<Routed>;

impl End {
    #[must_use]
    pub fn new(limits: Limits, bottom: Bottom, script: Script) -> End {
        limits.check();
        End {
            limits,
            server: Server::new(&limits.server),
            tokenizer: None,
            writer: None,
            bottom,
            script,
            seen: Seen::default(),
            tokens: Vec::new(),
            asked: false,
            exchange: None,
            spent: false,
            now: 0,
        }
    }

    /// Whether the user closed, and with it every machine and the stream.
    #[must_use]
    pub fn settled(&self) -> bool {
        self.seen.closed
            && self.tokenizer.is_none()
            && self.writer.is_none()
            && self.server.waiting() == server::Waiting::Nothing
    }

    /// Whether the writer waits for room below: the client is not reading.
    #[must_use]
    pub fn writer_blocked(&self) -> bool {
        self.writer.as_ref().is_some_and(|writer| writer.waiting() == writer::Waiting::Room)
    }

    /// The user's turn at `now`: the next request, the response, the next
    /// event, or the close.
    pub fn act(&mut self, now: u64) {
        self.now = now;
        if self.seen.closed {
            return;
        }
        if self.script.close.is_some_and(|at| now >= at) || self.spent {
            self.close();
            return;
        }
        let mut work = Work::new();
        match &mut self.exchange {
            None => {
                if !self.asked {
                    self.asked = true;
                    self.server_down(server::Request::Next, &mut work);
                }
            }
            Some(exchange) if exchange.error.is_some() => self.error_acts(&mut work),
            Some(exchange) => {
                if !exchange.responded {
                    if self.tokenizer.is_none() && !self.seen.requests.is_empty() {
                        self.respond(&mut work);
                    }
                } else if exchange.writing.is_none() && !exchange.finished && self.writer.is_some() {
                    self.write_next(&mut work);
                }
            }
        }
        self.drain(work);
    }

    /// The response to a request read: a stream of events, written by the
    /// writer stacked on the reply.
    fn respond(&mut self, work: &mut Work) {
        let response = Response {
            status: 200,
            headers: Box::new([header(b"Content-Type", b"text/event-stream"), header(b"Cache-Control", b"no-cache")]),
            body: Body::Chunked,
            close: false,
        };
        self.exchange.as_mut().expect("an exchange").responded = true;
        self.writer = Some(Writer::new(&self.limits.writer));
        self.server_down(server::Request::Respond(response), work);
    }

    /// The next event, or a comment before it, or the stream's end.
    fn write_next(&mut self, work: &mut Work) {
        let exchange = self.exchange.as_mut().expect("an exchange");
        let next = exchange.next;
        if next == self.script.events.len() {
            exchange.finished = true;
            self.writer_down(writer::Request::Finish, work);
            return;
        }
        if let Some(every) = self.script.ping
            && next.is_multiple_of(every)
            && exchange.pinged != Some(next)
        {
            exchange.pinged = Some(next);
            exchange.writing = Some(Item::Comment);
            self.writer_down(writer::Request::Comment(b"ping".as_slice().into()), work);
            return;
        }
        exchange.writing = Some(Item::Event(next));
        exchange.next += 1;
        let (name, data) = &self.script.events[next];
        let outgoing = Outgoing { name: name.clone(), data: data.clone(), id: None, retry: None };
        self.writer_down(writer::Request::Event(outgoing), work);
    }

    /// The early error's body, written by the user itself on the reply.
    fn error_acts(&mut self, work: &mut Work) {
        let exchange = self.exchange.as_mut().expect("an exchange");
        let (body, written, reply) = exchange.error.as_mut().expect("an early error");
        let left = body.len() - *written;
        match *reply {
            Reply::Idle if left == 0 => {
                *reply = Reply::Over;
                self.server_down(server::Request::Reply(Down::Finish), work);
            }
            Reply::Idle => {
                let room = self.limits.server.send.min(u32::try_from(left).expect("fits a u32"));
                *reply = Reply::Wanted(room);
                self.server_down(server::Request::Reply(Down::Demand { read: Read::Nothing, room }), work);
            }
            Reply::Granted(room) => {
                let end = *written + left.min(room as usize);
                let piece: Box<[u8]> = body[*written..end].into();
                *written = end;
                *reply = Reply::Idle;
                self.server_down(server::Request::Reply(Down::Send(piece)), work);
            }
            Reply::Wanted(_) | Reply::Over => {}
        }
    }

    /// The user closes: each machine above the server, then the server,
    /// then the stream, each close routed before the next.
    fn close(&mut self) {
        if self.tokenizer.is_some() {
            let mut work = Work::new();
            self.tokenizer_down(json::Request::Close, &mut work);
            self.drain(work);
        }
        if self.writer.is_some() {
            let mut work = Work::new();
            self.writer_down(writer::Request::Close, &mut work);
            self.drain(work);
        }
        let mut work = Work::new();
        self.server_down(server::Request::Close, &mut work);
        self.drain(work);
        self.bottom.close(self.now);
        self.seen.closed = true;
    }

    /// The stream below's answer to the server, if it has one now; and, once
    /// the server is closed, an answer to what it withdrew, on its way.
    pub fn below_acts(&mut self, grant: bool) {
        let up = if self.seen.closed { self.bottom.late() } else { self.bottom.answer(grant) };
        let Some(up) = up else { return };
        let mut work = Work::new();
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        server::up(&mut self.server, &env(self.limits.server), up, &mut above, &mut below);
        self.server_routed(server::UP_MAX_OUT, above, below, false, &mut work);
        self.drain(work);
    }

    fn server_down(&mut self, rq: server::Request, work: &mut Work) {
        let stopping = match &rq {
            server::Request::Close | server::Request::Discard | server::Request::Respond(_) => true,
            server::Request::Body(down) | server::Request::Reply(down) => {
                *down == Down::Demand { read: Read::Nothing, room: 0 }
            }
            server::Request::Next => false,
        };
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        server::down(&mut self.server, &env(self.limits.server), rq, &mut above, &mut below);
        self.server_routed(server::DOWN_MAX_OUT, above, below, stopping, work);
    }

    /// What the server emitted, checked against `max`: its requests go
    /// below, a withdrawal among them only as it stops reading; its events
    /// join `work`.
    fn server_routed(
        &mut self,
        max: MaxOut,
        mut above: Queue<server::Event>,
        mut below: Queue<Down>,
        stopping: bool,
        work: &mut Work,
    ) {
        assert!(above.len() <= max.above && below.len() <= max.below, "the server's MAX_OUT");
        let mut ending = stopping;
        while let Some(event) = above.pop() {
            ending |= matches!(
                event,
                server::Event::Done(_) | server::Event::Failed(_) | server::Event::Ended | server::Event::Closed
            );
            work.push_back(Routed::Server(event));
        }
        while let Some(request) = below.pop() {
            self.bottom.receive(request, ending);
        }
    }

    fn tokenizer_down(&mut self, rq: json::Request, work: &mut Work) {
        let tokenizer = self.tokenizer.as_mut().expect("a tokenizer");
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        json::down(tokenizer, &env(self.limits.json), rq, &mut above, &mut below);
        assert!(
            above.len() <= json::DOWN_MAX_OUT.above && below.len() <= json::DOWN_MAX_OUT.below,
            "the tokenizer's MAX_OUT"
        );
        queued(above, below, Routed::Tokenizer, Routed::TokenizerBelow, work);
    }

    fn tokenizer_up(&mut self, up: Up, work: &mut Work) {
        let tokenizer = self.tokenizer.as_mut().expect("a tokenizer");
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        json::up(tokenizer, &env(self.limits.json), up, &mut above, &mut below);
        assert!(
            above.len() <= json::UP_MAX_OUT.above && below.len() <= json::UP_MAX_OUT.below,
            "the tokenizer's MAX_OUT"
        );
        queued(above, below, Routed::Tokenizer, Routed::TokenizerBelow, work);
    }

    fn writer_down(&mut self, rq: writer::Request, work: &mut Work) {
        let writer = self.writer.as_mut().expect("a writer");
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        writer::down(writer, &env(self.limits.writer), rq, &mut above, &mut below);
        assert!(
            above.len() <= writer::DOWN_MAX_OUT.above && below.len() <= writer::DOWN_MAX_OUT.below,
            "the writer's MAX_OUT"
        );
        queued(above, below, Routed::Writer, Routed::WriterBelow, work);
    }

    fn writer_up(&mut self, up: Up, work: &mut Work) {
        let writer = self.writer.as_mut().expect("a writer");
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        writer::up(writer, &env(self.limits.writer), up, &mut above, &mut below);
        assert!(
            above.len() <= writer::UP_MAX_OUT.above && below.len() <= writer::UP_MAX_OUT.below,
            "the writer's MAX_OUT"
        );
        queued(above, below, Routed::Writer, Routed::WriterBelow, work);
    }

    /// Routes `work` and all it leads to, a machine's output at a time, as a
    /// connection routes within its step.
    fn drain(&mut self, mut work: Work) {
        for _ in 0..100_000 {
            let Some(routed) = work.pop_front() else { return };
            match routed {
                Routed::Server(event) => self.server_event(event, &mut work),
                Routed::Tokenizer(event) => self.tokenizer_event(event, &mut work),
                Routed::TokenizerBelow(request) => self.server_down(server::Request::Body(request), &mut work),
                Routed::Writer(event) => self.writer_event(event),
                Routed::WriterBelow(request) => self.server_down(server::Request::Reply(request), &mut work),
            }
        }
        panic!("routing settles in a few calls");
    }

    /// An event from the server, for the user or a machine on a body.
    fn server_event(&mut self, event: server::Event, work: &mut Work) {
        match event {
            server::Event::Call(call) => {
                assert_eq!(call.header(b"content-type"), Some(&b"application/json"[..]), "a JSON request");
                self.asked = false;
                let error = self.script.early.as_ref().map(|(_, body)| (body.clone(), 0, Reply::Idle));
                let early = error.is_some();
                self.exchange =
                    Some(Exchange { responded: early, next: 0, writing: None, pinged: None, finished: false, error });
                if let Some((status, body)) = &self.script.early {
                    let response = Response {
                        status: *status,
                        headers: Box::new([header(b"Content-Type", b"application/json")]),
                        body: Body::Length(body.len() as u64),
                        close: false,
                    };
                    self.server_down(server::Request::Respond(response), work);
                } else {
                    self.tokenizer = Some(Tokenizer::new(&self.limits.json));
                    self.tokenizer_down(json::Request::Next, work);
                }
            }
            server::Event::Body(up) => {
                if self.tokenizer.is_some() {
                    self.tokenizer_up(up, work);
                }
            }
            server::Event::Reply(up) => {
                if self.writer.is_some() {
                    self.writer_up(up, work);
                } else if let Some(Exchange { error: Some((_, _, reply)), .. }) = &mut self.exchange {
                    *reply = match up {
                        Up::Room => {
                            let Reply::Wanted(room) = *reply else { panic!("room for the reply's demand") };
                            Reply::Granted(room)
                        }
                        Up::Failed(_) => Reply::Over,
                        Up::Bytes(_) | Up::End => panic!("the reply is written: {up:?}"),
                    };
                }
            }
            server::Event::Refused(refusal) => panic!("a sound response refused: {refusal:?}"),
            server::Event::Done(reuse) => self.ended(Outcome::Done(reuse), work),
            server::Event::Failed(error) => self.ended(Outcome::Failed(error), work),
            server::Event::Ended => self.ended(Outcome::Ended, work),
            server::Event::Closed => {}
        }
    }

    /// An exchange ended, or a `Next` was answered with no call.
    fn ended(&mut self, outcome: Outcome, work: &mut Work) {
        self.asked = false;
        self.seen.outcomes.push(outcome);
        self.exchange = None;
        self.spent = outcome != Outcome::Done(server::Reuse::Keep);
        // The writer, its stream over, is closed with the exchange.
        if self.writer.is_some() {
            self.writer_down(writer::Request::Close, work);
        }
    }

    /// An event from the tokenizer: a token of the request, or its outcome.
    fn tokenizer_event(&mut self, event: json::Event, work: &mut Work) {
        match event {
            json::Event::Token(token) => {
                self.tokens.push(token);
                self.tokenizer_down(json::Request::Next, work);
            }
            json::Event::Done | json::Event::Failed(_) => {
                self.seen.requests.push(std::mem::take(&mut self.tokens));
                self.tokenizer_down(json::Request::Close, work);
            }
            json::Event::Closed => self.tokenizer = None,
        }
    }

    /// An event from the writer: the next may go, or the stream failed.
    fn writer_event(&mut self, event: writer::Event) {
        match event {
            writer::Event::Sent => {
                let exchange = self.exchange.as_mut().expect("an exchange");
                if let Some(Item::Event(index)) = exchange.writing.take() {
                    let (name, data) = &self.script.events[index];
                    self.seen.sent = index + 1;
                    self.seen.framed += framed_len(name, data);
                }
            }
            writer::Event::Refused(refusal) => panic!("a sound event refused: {refusal:?}"),
            writer::Event::Failed(_) => self.seen.writer_failed = true,
            writer::Event::Closed => self.writer = None,
        }
    }
}

/// A machine's output into `work`: its events up as `up`, its requests
/// below as `down`.
fn queued<E>(
    mut above: Queue<E>,
    mut below: Queue<Down>,
    up: fn(E) -> Routed,
    down: fn(Down) -> Routed,
    work: &mut Work,
) {
    while let Some(event) = above.pop() {
        work.push_back(up(event));
    }
    while let Some(request) = below.pop() {
        work.push_back(down(request));
    }
}

/// The bytes the writer frames an event of `name` and `data` as, data with
/// no line ending in it: its type's field, if it has one, its data's field,
/// and the blank line.
#[must_use]
pub fn framed_len(name: &[u8], data: &[u8]) -> u64 {
    assert!(!data.iter().any(|&byte| byte == b'\r' || byte == b'\n'), "a document the JSON writer wrote is one line");
    let name = if name.is_empty() { 0 } else { "event: ".len() + name.len() + 1 };
    (name + "data: ".len() + data.len() + 2) as u64
}

fn header(name: &[u8], value: &[u8]) -> Header {
    Header { name: name.into(), value: value.into() }
}

fn env<L>(limits: L) -> Env<L> {
    Env { now: Time::ZERO, wall: Wall::EPOCH, limits }
}
