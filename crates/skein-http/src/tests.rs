//! skein-http's step tests (testing-strategy.md, 2.1): each machine fed by
//! hand, one event at a time, and driven whole through a stream below that
//! meets its demands from a buffer. The machine worlds are in tests/http.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]

mod body;
mod data;
mod exchange;
mod request;
mod response;
mod server;
mod sse;
mod writer;

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue, Time, Wall};

use crate::Header;
use crate::client::{self, Body, Call, Client, Event, Limits, Method, Request, Response};
use crate::sse::{self as events, Reader};

/// Small limits, so that a test reaches each of them.
const LIMITS: Limits = Limits { request: 256, head: 256, headers: 8, read: 16, send: 16 };

fn env<L>(limits: L) -> Env<L> {
    Env { now: Time::ZERO, wall: Wall::EPOCH, limits }
}

fn boxed(bytes: &[u8]) -> Box<[u8]> {
    Box::from(bytes)
}

fn header(name: &[u8], value: &[u8]) -> Header {
    Header { name: boxed(name), value: boxed(value) }
}

/// A call of `method` for `/`, with a `Host`.
fn call(method: Method, body: Body) -> Call {
    Call { method, target: boxed(b"/"), headers: Box::new([header(b"Host", b"example.com")]), body, close: false }
}

fn get() -> Call {
    call(Method::Get, Body::None)
}

/// A client and its two queues, with room for what one call emits.
struct Machine {
    client: Client,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<Down>,
}

impl Machine {
    fn new(limits: Limits) -> Machine {
        Machine {
            client: Client::new(&limits),
            env: env(limits),
            above: Queue::with_capacity(client::UP_MAX_OUT.above.max(client::DOWN_MAX_OUT.above)),
            below: Queue::with_capacity(client::UP_MAX_OUT.below.max(client::DOWN_MAX_OUT.below)),
        }
    }

    /// Sends `rq` down; what came of it, above and below.
    fn down(&mut self, rq: Request) -> (Vec<Event>, Vec<Down>) {
        client::down(&mut self.client, &self.env, rq, &mut self.above, &mut self.below);
        assert!(self.above.len() <= client::DOWN_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= client::DOWN_MAX_OUT.below, "MAX_OUT below");
        self.take()
    }

    /// Sends `ev` up; what came of it, above and below.
    fn up(&mut self, ev: Up) -> (Vec<Event>, Vec<Down>) {
        client::up(&mut self.client, &self.env, ev, &mut self.above, &mut self.below);
        assert!(self.above.len() <= client::UP_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= client::UP_MAX_OUT.below, "MAX_OUT below");
        self.take()
    }

    fn bytes(&mut self, bytes: &[u8]) -> (Vec<Event>, Vec<Down>) {
        self.up(Up::Bytes(boxed(bytes)))
    }

    fn take(&mut self) -> (Vec<Event>, Vec<Down>) {
        let mut events = Vec::new();
        while let Some(event) = self.above.pop() {
            events.push(event);
        }
        let mut requests = Vec::new();
        while let Some(request) = self.below.pop() {
            requests.push(request);
        }
        (events, requests)
    }

    /// Makes a call and sends its head: what it wrote, and the demand for
    /// the response's first line that follows, unless the call has a body
    /// the side above has not demanded room for.
    fn called(&mut self, call: Call) -> (Box<[u8]>, Option<Down>) {
        let (events, mut requests) = self.down(Request::Call(call));
        assert!(events.is_empty(), "{events:?}");
        let Some(Down::Demand { read: Read::Nothing, room }) = requests.pop() else { panic!("room for the head") };
        let (events, mut requests) = self.up(Up::Room);
        assert!(events.is_empty(), "{events:?}");
        let demand = if requests.len() == 2 { requests.pop() } else { None };
        let Some(Down::Send(head)) = requests.pop() else { panic!("the head sent") };
        assert_eq!(head.len(), usize::try_from(room).unwrap(), "the room asked for is the head's length");
        (head, demand)
    }
}

/// What a whole exchange came to.
#[derive(Debug)]
struct Exchanged {
    /// What the client sent below.
    sent: Vec<u8>,
    response: Option<Response>,
    /// The body's bytes the side above received.
    body: Vec<u8>,
    /// Whether the body's stream ended with `End`.
    ended: bool,
    /// The fault the body's stream failed with, if it did.
    body_failed: Option<skein_lib::stream::Fault>,
    /// The exchange's terminal event: `Done` or `Failed`.
    outcome: Option<Event>,
    /// What the upload's stream was told, `Room`s aside.
    upload: Vec<Up>,
    /// What the client sent below in the step that ended the exchange,
    /// after its terminal event.
    sent_after: Vec<Down>,
}

/// How a driven exchange's neighbours behave.
#[derive(Clone, Copy, Debug)]
struct Drive<'a> {
    /// The request body the side above uploads, in pieces of at most
    /// [`Limits::send`], if the call has one.
    upload: Option<&'a [u8]>,
    /// What the side above demands of the response body, again and again.
    read: Read,
    /// Whether the stream below delivers bytes before it grants room, when
    /// a demand asks for both.
    respond_first: bool,
}

/// Runs `call` through a stream below that holds `response` whole, grants
/// all the room asked for, and ends once a read can no longer be met, with
/// a side above that uploads and reads as `drive` says, until the
/// exchange's terminal event.
fn exchange(machine: &mut Machine, call: Call, response: &[u8], drive: Drive<'_>) -> Exchanged {
    let cap = u32::try_from(response.len()).unwrap().max(client::largest_read(&machine.env.limits));
    let mut intake = Intake::with_capacity(cap);
    intake.append(response).unwrap();
    let mut out = Exchanged {
        sent: Vec::new(),
        response: None,
        body: Vec::new(),
        ended: false,
        body_failed: None,
        outcome: None,
        upload: Vec::new(),
        sent_after: Vec::new(),
    };
    let mut outstanding: Option<(Read, u32)> = None;
    let upload = drive.upload.unwrap_or_default();
    let mut uploaded = 0_usize;
    let mut wanted = 0;
    let mut events = Vec::new();
    let mut requests = Vec::new();
    let (e, r) = machine.down(Request::Call(call));
    events.extend(e);
    requests.extend(r);
    if drive.upload.is_some() {
        let (e, r) = upload_next(machine, upload.len(), &mut wanted);
        events.extend(e);
        requests.extend(r);
    }
    for _ in 0..100_000_u32 {
        if !requests.is_empty() {
            match requests.remove(0) {
                Down::Demand { read: Read::Nothing, room: 0 } => outstanding = None,
                Down::Demand { read, room } => {
                    assert!(outstanding.is_none(), "one demand at a time");
                    outstanding = Some((read, room));
                }
                Down::Send(bytes) => out.sent.extend_from_slice(&bytes),
                Down::Finish => panic!("the client never finishes the stream below"),
            }
            continue;
        }
        let (e, r) = if events.is_empty() {
            // The side below meets what is outstanding.
            let Some((read, room)) = outstanding.take() else { panic!("the exchange stalled: {out:?}") };
            let met = if drive.respond_first || room == 0 { intake.meet(read) } else { None };
            match met {
                Some(bytes) => machine.up(Up::Bytes(bytes)),
                None if room > 0 => machine.up(Up::Room),
                None => machine.up(Up::End),
            }
        } else {
            match events.remove(0) {
                Event::Response(response) => {
                    out.response = Some(response);
                    machine.down(Request::Body(Down::Demand { read: drive.read, room: 0 }))
                }
                Event::Body(Up::Bytes(bytes)) => {
                    out.body.extend_from_slice(&bytes);
                    machine.down(Request::Body(Down::Demand { read: drive.read, room: 0 }))
                }
                Event::Body(Up::End) => {
                    out.ended = true;
                    (Vec::new(), Vec::new())
                }
                Event::Body(Up::Failed(fault)) => {
                    out.body_failed = Some(fault);
                    (Vec::new(), Vec::new())
                }
                Event::Body(Up::Room) => panic!("room on the body's stream"),
                Event::Upload(Up::Room) => {
                    let end = uploaded.saturating_add(usize::try_from(wanted).unwrap()).min(upload.len());
                    let piece = &upload[uploaded..end];
                    uploaded = end;
                    let (mut e, mut r) = machine.down(Request::Upload(Down::Send(boxed(piece))));
                    let (more_e, more_r) = upload_next(machine, upload.len().saturating_sub(uploaded), &mut wanted);
                    e.extend(more_e);
                    r.extend(more_r);
                    (e, r)
                }
                Event::Upload(other) => {
                    out.upload.push(other);
                    (Vec::new(), Vec::new())
                }
                event @ (Event::Done(_) | Event::Failed(_)) => {
                    out.outcome = Some(event);
                    for request in requests {
                        out.sent_after.push(request);
                    }
                    return out;
                }
                Event::Closed => panic!("closed unasked"),
            }
        };
        events.extend(e);
        requests.extend(r);
    }
    panic!("an exchange settles in a few steps a byte");
}

/// The side above's next demand on the upload, with `left` bytes to go:
/// room for at most [`Limits::send`], or `Finish`.
fn upload_next(machine: &mut Machine, left: usize, wanted: &mut u32) -> (Vec<Event>, Vec<Down>) {
    if left == 0 {
        return machine.down(Request::Upload(Down::Finish));
    }
    *wanted = machine.env.limits.send.min(u32::try_from(left).unwrap());
    machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: *wanted }))
}

/// A reader and its two queues, with room for what one call emits.
struct Events {
    reader: Reader,
    env: Env<events::Limits>,
    above: Queue<events::Event>,
    below: Queue<Down>,
}

impl Events {
    fn new(limits: events::Limits) -> Events {
        Events {
            reader: Reader::new(&limits),
            env: env(limits),
            above: Queue::with_capacity(events::UP_MAX_OUT.above.max(events::DOWN_MAX_OUT.above)),
            below: Queue::with_capacity(events::UP_MAX_OUT.below.max(events::DOWN_MAX_OUT.below)),
        }
    }

    fn down(&mut self, rq: events::Request) -> (Option<events::Event>, Option<Down>) {
        events::down(&mut self.reader, &self.env, rq, &mut self.above, &mut self.below);
        self.take()
    }

    fn up(&mut self, ev: Up) -> (Option<events::Event>, Option<Down>) {
        events::up(&mut self.reader, &self.env, ev, &mut self.above, &mut self.below);
        self.take()
    }

    fn bytes(&mut self, bytes: &[u8]) -> (Option<events::Event>, Option<Down>) {
        self.up(Up::Bytes(boxed(bytes)))
    }

    fn take(&mut self) -> (Option<events::Event>, Option<Down>) {
        let taken = (self.above.pop(), self.below.pop());
        assert!(self.above.is_empty() && self.below.is_empty(), "one of each at most");
        taken
    }
}

/// What an event stream comes to, read to its outcome through a stream that
/// holds all of it and meets each demand from it: the events, the last its
/// outcome.
fn stream(bytes: &[u8], limits: events::Limits) -> Vec<events::Event> {
    let mut machine = Events::new(limits);
    let mut intake = Intake::with_capacity(u32::try_from(bytes.len()).unwrap().max(events::largest_demand(&limits)));
    intake.append(bytes).unwrap();
    let mut out = Vec::new();
    let mut demanded = None;
    for _ in 0..bytes.len().saturating_mul(4).saturating_add(8) {
        let (event, down) = match demanded.take() {
            None => machine.down(events::Request::Next),
            Some(read) => match intake.meet(read) {
                Some(bytes) => machine.up(Up::Bytes(bytes)),
                None => machine.up(Up::End),
            },
        };
        match down {
            Some(Down::Demand { read, room: 0 }) => demanded = Some(read),
            None => {}
            Some(other) => panic!("the reader sent {other:?} down"),
        }
        let Some(event) = event else { continue };
        assert!(demanded.is_none(), "an answer leaves nothing demanded");
        let over = match &event {
            events::Event::Message(_) => false,
            events::Event::Ended | events::Event::Failed(_) => true,
            events::Event::Closed => panic!("closed unasked"),
        };
        out.push(event);
        if over {
            return out;
        }
    }
    panic!("a stream is read in a few steps a line");
}
