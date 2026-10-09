//! The client's end (testing-strategy.md, 2.5; http.md, 2): an LLM
//! client's stack as a service builds it, the HTTP client over the stream,
//! the event stream reader on its response body, and a JSON tokenizer for
//! each event's data through `sse::Data`, or for the body itself when it
//! is no event stream; routed as a connection routes between its machines
//! (programming-model.md, 4), each call held to its `MAX_OUT`; and a
//! scripted user at the top, which makes its calls one after another on a
//! connection kept, each uploading a JSON request, reads events slowly or
//! not, and closes when it is done or when it is told to.

use std::collections::VecDeque;

use skein_http::client::{self, Body, Call, Client};
use skein_http::sse::{self, Data, Reader};
use skein_http::{Header, MaxOut, Method};
use skein_json::Token;
use skein_json::tokenizer::{self as json, Tokenizer};
use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Queue, Time, Wall};

use crate::wire::Bottom;

/// The limits of the client's machines.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub client: client::Limits,
    pub reader: sse::Limits,
    pub json: json::Limits,
}

impl Limits {
    /// The stack's startup checks, as whoever stacks the machines makes
    /// them (http.md, 2): each machine's largest demand within its side
    /// below's.
    pub fn check(&self) {
        assert!(sse::largest_demand(&self.reader) <= self.client.read, "the reader's scans within the client's reads");
        assert!(
            json::largest_demand(&self.json) <= self.client.read,
            "the tokenizer's demands within the client's reads"
        );
    }
}

/// What the client's user does.
#[derive(Clone, Debug)]
pub struct Script {
    /// Each call's request body, a JSON document the JSON writer wrote: a
    /// call after another while the connection is kept.
    pub bodies: Vec<Box<[u8]>>,
    /// After this many events, the user stops asking for the next for this
    /// many iterations: a slow reader.
    pub stall: Option<(usize, u64)>,
    /// From this iteration on, the user closes, whatever its stack is
    /// doing.
    pub close: Option<u64>,
}

/// What the client's user saw, over all its calls.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Seen {
    /// Each response's status.
    pub statuses: Vec<u16>,
    /// Each event's type and data, in order.
    pub events: Vec<(Vec<u8>, Vec<u8>)>,
    /// What the reader said its last event ID and its reconnection time
    /// were, as each event came.
    pub ids: Vec<(Vec<u8>, Option<u64>)>,
    /// The tokens of each document read: each event's data, or the body
    /// when it is no event stream.
    pub documents: Vec<Vec<Token>>,
    /// How each event stream ended.
    pub ended: Vec<sse::Event>,
    /// The fault an upload's stream failed with, if one did.
    pub upload_failed: Option<Fault>,
    /// How much of the last call's request body went down.
    pub uploaded: usize,
    /// Since when the user's demand for room for its upload has waited, if
    /// one has.
    pub upload_since: Option<u64>,
    /// Each call's terminal event.
    pub outcomes: Vec<Outcome>,
    /// When the user stopped reading for a while.
    pub stalled: Option<u64>,
    /// When it read again.
    pub resumed: Option<u64>,
    /// How many calls it made.
    pub calls: usize,
    pub closed: bool,
}

/// How the exchange ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    Done(client::Reuse),
    Failed(client::Error),
}

/// The client's end: its machines, the stream below, and the user.
#[derive(Debug)]
pub struct End {
    limits: Limits,
    client: Client,
    reader: Option<Reader>,
    /// The tokenizer of the document being read, and the event's data it
    /// reads from, or none when it reads the response body.
    tokenizer: Option<(Tokenizer, Option<Data>)>,
    pub bottom: Bottom,
    script: Script,
    pub seen: Seen,
    tokens: Vec<Token>,
    /// How many calls the user made.
    calls: usize,
    upload: Upload,
    /// A `Next` outstanding on the reader.
    asked: bool,
    /// The iteration the user last acted at.
    now: u64,
}

/// The user's side of the upload's stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Upload {
    Idle,
    Wanted(u32),
    Granted(u32),
    Over,
}

/// What a machine emitted, still to be routed.
#[derive(Debug)]
enum Routed {
    Client(client::Event),
    Reader(sse::Event),
    ReaderBelow(Down),
    Tokenizer(json::Event),
    TokenizerBelow(Down),
}

type Work = VecDeque<Routed>;

impl End {
    #[must_use]
    pub fn new(limits: Limits, bottom: Bottom, script: Script) -> End {
        limits.check();
        End {
            limits,
            client: Client::new(&limits.client),
            reader: None,
            tokenizer: None,
            bottom,
            script,
            seen: Seen::default(),
            tokens: Vec::new(),
            calls: 0,
            upload: Upload::Idle,
            asked: false,
            now: 0,
        }
    }

    /// Whether the user closed, and with it every machine and the stream.
    #[must_use]
    pub fn settled(&self) -> bool {
        self.seen.closed
            && self.reader.is_none()
            && self.tokenizer.is_none()
            && self.client.waiting() == client::Waiting::Nothing
    }

    /// The user's turn at `now`: the next call, the upload, the next
    /// event, or the close.
    pub fn act(&mut self, now: u64) {
        self.now = now;
        if self.seen.closed {
            return;
        }
        let mut work = Work::new();
        if self.script.close.is_some_and(|at| now >= at) {
            self.close();
            return;
        }
        let over = self.seen.outcomes.len() == self.calls;
        if over {
            // The next call once the last one's machines are closed, on a
            // connection kept; or the close.
            let kept = self.seen.outcomes.last().is_none_or(|outcome| *outcome == Outcome::Done(client::Reuse::Keep));
            if !kept || self.calls == self.script.bodies.len() {
                self.close();
                return;
            }
            if self.reader.is_some() || self.tokenizer.is_some() {
                return;
            }
            self.calls += 1;
            self.seen.calls = self.calls;
            self.upload = Upload::Idle;
            self.seen.uploaded = 0;
            let call = Call {
                method: Method::Post,
                target: b"/v1/messages".as_slice().into(),
                headers: Box::new([
                    header(b"Host", b"api.example.com"),
                    header(b"Content-Type", b"application/json"),
                    header(b"Accept", b"text/event-stream"),
                ]),
                body: Body::Length(self.body().len() as u64),
                close: false,
            };
            self.client_down(client::Request::Call(call), &mut work);
        } else {
            self.upload_acts(&mut work);
            if self.wants_next(now) {
                self.asked = true;
                self.reader_down(sse::Request::Next, &mut work);
            }
        }
        self.drain(work);
    }

    /// The request body of the call in progress, or the last.
    fn body(&self) -> &[u8] {
        &self.script.bodies[self.calls - 1]
    }

    /// Whether the user asks the reader for the next event now: one is not
    /// outstanding nor being tokenized, the stream has not ended, and the
    /// user is not stalled.
    fn wants_next(&mut self, now: u64) -> bool {
        if self.reader.is_none() || self.asked || self.tokenizer.is_some() {
            return false;
        }
        match self.script.stall {
            Some((after, iterations)) if self.seen.events.len() >= after => {
                let from = *self.seen.stalled.get_or_insert(now);
                let again = now >= from + iterations;
                if again && self.seen.resumed.is_none() {
                    self.seen.resumed = Some(now);
                }
                again
            }
            Some(_) | None => true,
        }
    }

    fn upload_acts(&mut self, work: &mut Work) {
        let left = self.body().len() - self.seen.uploaded;
        match self.upload {
            Upload::Idle if left == 0 => {
                self.upload = Upload::Over;
                self.client_down(client::Request::Upload(Down::Finish), work);
            }
            Upload::Idle => {
                let room = self.limits.client.send.min(u32::try_from(left).expect("fits a u32"));
                self.upload = Upload::Wanted(room);
                self.seen.upload_since = Some(self.now);
                self.client_down(client::Request::Upload(Down::Demand { read: Read::Nothing, room }), work);
            }
            Upload::Granted(room) => {
                let end = self.seen.uploaded + left.min(room as usize);
                let piece: Box<[u8]> = self.body()[self.seen.uploaded..end].into();
                self.seen.uploaded = end;
                self.upload = Upload::Idle;
                self.client_down(client::Request::Upload(Down::Send(piece)), work);
            }
            Upload::Wanted(_) | Upload::Over => {}
        }
    }

    /// The user closes: each machine above the client, then the client,
    /// then the stream, each close routed before the next.
    fn close(&mut self) {
        if self.tokenizer.is_some() {
            let mut work = Work::new();
            self.tokenizer_down(json::Request::Close, &mut work);
            self.drain(work);
        }
        if self.reader.is_some() {
            let mut work = Work::new();
            self.reader_down(sse::Request::Close, &mut work);
            self.drain(work);
        }
        let mut work = Work::new();
        self.client_down(client::Request::Close, &mut work);
        self.drain(work);
        self.bottom.close(self.now);
        self.seen.closed = true;
    }

    /// The stream below's answer to the client at `now`, if it has one;
    /// and, once the client is closed, an answer to what it withdrew, on its
    /// way.
    pub fn below_acts(&mut self, now: u64, grant: bool) {
        self.now = now;
        let up = if self.seen.closed { self.bottom.late() } else { self.bottom.answer(grant) };
        let Some(up) = up else { return };
        let mut work = Work::new();
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        client::up(&mut self.client, &env(self.limits.client), up, &mut above, &mut below);
        self.client_routed(client::UP_MAX_OUT, above, below, false, &mut work);
        self.drain(work);
    }

    fn client_down(&mut self, rq: client::Request, work: &mut Work) {
        let stopping = match &rq {
            client::Request::Close | client::Request::Discard => true,
            client::Request::Body(down) => *down == Down::Demand { read: Read::Nothing, room: 0 },
            client::Request::Call(_) | client::Request::Upload(_) => false,
        };
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        client::down(&mut self.client, &env(self.limits.client), rq, &mut above, &mut below);
        self.client_routed(client::DOWN_MAX_OUT, above, below, stopping, work);
    }

    /// What the client emitted, checked against `max`: its requests go below,
    /// a withdrawal among them only as it stops reading; its events join
    /// `work`.
    fn client_routed(
        &mut self,
        max: MaxOut,
        mut above: Queue<client::Event>,
        mut below: Queue<Down>,
        stopping: bool,
        work: &mut Work,
    ) {
        assert!(above.len() <= max.above && below.len() <= max.below, "the client's MAX_OUT");
        let mut ending = stopping;
        while let Some(event) = above.pop() {
            ending |= matches!(event, client::Event::Done(_) | client::Event::Failed(_) | client::Event::Closed);
            work.push_back(Routed::Client(event));
        }
        while let Some(request) = below.pop() {
            self.bottom.receive(request, ending);
        }
    }

    fn reader_down(&mut self, rq: sse::Request, work: &mut Work) {
        let reader = self.reader.as_mut().expect("a reader");
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        sse::down(reader, &env(self.limits.reader), rq, &mut above, &mut below);
        assert!(
            above.len() <= sse::DOWN_MAX_OUT.above && below.len() <= sse::DOWN_MAX_OUT.below,
            "the reader's MAX_OUT"
        );
        queued(above, below, Routed::Reader, Routed::ReaderBelow, work);
    }

    fn reader_up(&mut self, up: Up, work: &mut Work) {
        let reader = self.reader.as_mut().expect("a reader");
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        sse::up(reader, &env(self.limits.reader), up, &mut above, &mut below);
        assert!(above.len() <= sse::UP_MAX_OUT.above && below.len() <= sse::UP_MAX_OUT.below, "the reader's MAX_OUT");
        queued(above, below, Routed::Reader, Routed::ReaderBelow, work);
    }

    fn tokenizer_down(&mut self, rq: json::Request, work: &mut Work) {
        let (tokenizer, _) = self.tokenizer.as_mut().expect("a tokenizer");
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
        let (tokenizer, _) = self.tokenizer.as_mut().expect("a tokenizer");
        let mut above = Queue::with_capacity(8);
        let mut below = Queue::with_capacity(8);
        json::up(tokenizer, &env(self.limits.json), up, &mut above, &mut below);
        assert!(
            above.len() <= json::UP_MAX_OUT.above && below.len() <= json::UP_MAX_OUT.below,
            "the tokenizer's MAX_OUT"
        );
        queued(above, below, Routed::Tokenizer, Routed::TokenizerBelow, work);
    }

    /// Routes `work` and all it leads to, a machine's output at a time, as a
    /// connection routes within its step.
    fn drain(&mut self, mut work: Work) {
        for _ in 0..100_000 {
            let Some(routed) = work.pop_front() else { return };
            match routed {
                Routed::Client(event) => self.client_event(event, &mut work),
                Routed::Reader(event) => self.reader_event(event, &mut work),
                Routed::ReaderBelow(request) => self.client_down(client::Request::Body(request), &mut work),
                Routed::Tokenizer(event) => self.tokenizer_event(event, &mut work),
                Routed::TokenizerBelow(request) => self.tokenizer_below(request, &mut work),
            }
        }
        panic!("routing settles in a few calls");
    }

    /// An event from the client, for the user or the machine on the body.
    fn client_event(&mut self, event: client::Event, work: &mut Work) {
        match event {
            client::Event::Response(response) => {
                self.seen.statuses.push(response.status);
                let streams = response
                    .header(b"content-type")
                    .is_some_and(|value| value.to_ascii_lowercase().starts_with(b"text/event-stream"));
                if streams {
                    self.reader = Some(Reader::new(&self.limits.reader));
                } else {
                    self.tokenizer = Some((Tokenizer::new(&self.limits.json), None));
                    self.tokenizer_down(json::Request::Next, work);
                }
            }
            client::Event::Upload(Up::Room) => {
                let Upload::Wanted(room) = self.upload else { panic!("room for the upload's demand") };
                self.upload = Upload::Granted(room);
                self.seen.upload_since = None;
            }
            client::Event::Upload(Up::Failed(fault)) => {
                self.upload = Upload::Over;
                self.seen.upload_failed = Some(fault);
                self.seen.upload_since = None;
            }
            client::Event::Upload(other @ (Up::Bytes(_) | Up::End)) => panic!("the upload is written: {other:?}"),
            client::Event::Body(up) => {
                if self.reader.is_some() {
                    self.reader_up(up, work);
                } else if self.tokenizer.is_some() {
                    self.tokenizer_up(up, work);
                }
            }
            client::Event::Done(reuse) => self.seen.outcomes.push(Outcome::Done(reuse)),
            client::Event::Failed(error) => self.seen.outcomes.push(Outcome::Failed(error)),
            client::Event::Closed => {}
        }
    }

    /// An event from the reader: an event's data to tokenize, or the
    /// stream's end.
    fn reader_event(&mut self, event: sse::Event, work: &mut Work) {
        match event {
            sse::Event::Message(message) => {
                self.asked = false;
                // What the reader's own face says once the event came: what
                // a client that reconnects would send, and wait.
                let reader = self.reader.as_ref().expect("a reader");
                self.seen.ids.push((reader.last_event_id().to_vec(), reader.retry()));
                self.seen.events.push((message.name.to_vec(), message.data.to_vec()));
                self.tokenizer = Some((Tokenizer::new(&self.limits.json), Some(Data::new(message.data))));
                self.tokenizer_down(json::Request::Next, work);
            }
            sse::Event::Ended | sse::Event::Failed(_) => {
                self.asked = false;
                self.seen.ended.push(event);
                self.reader_down(sse::Request::Close, work);
            }
            sse::Event::Closed => self.reader = None,
        }
    }

    /// A demand from the tokenizer: met by the event's data at once, or
    /// passed to the client as the body's.
    fn tokenizer_below(&mut self, request: Down, work: &mut Work) {
        let (_, data) = self.tokenizer.as_mut().expect("a tokenizer");
        match data {
            Some(data) => {
                let Down::Demand { read, room: 0 } = request else { panic!("the tokenizer reads") };
                if let Some(answer) = data.answer(read) {
                    self.tokenizer_up(answer, work);
                }
            }
            None => self.client_down(client::Request::Body(request), work),
        }
    }

    /// An event from the tokenizer: a token, or the document's outcome.
    fn tokenizer_event(&mut self, event: json::Event, work: &mut Work) {
        match event {
            json::Event::Token(token) => {
                self.tokens.push(token);
                self.tokenizer_down(json::Request::Next, work);
            }
            json::Event::Done | json::Event::Failed(_) => {
                self.seen.documents.push(std::mem::take(&mut self.tokens));
                self.tokenizer_down(json::Request::Close, work);
            }
            json::Event::Long(_) | json::Event::Skipped(_) => unreachable!("only Next is demanded"),
            json::Event::Closed => self.tokenizer = None,
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

fn header(name: &[u8], value: &[u8]) -> Header {
    Header { name: name.into(), value: value.into() }
}

fn env<L>(limits: L) -> Env<L> {
    Env { now: Time::ZERO, wall: Wall::EPOCH, limits }
}
