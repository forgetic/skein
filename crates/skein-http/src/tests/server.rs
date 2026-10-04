//! The server's step tests (http.md, 5): a server fed by hand, one event
//! at a time, and driven whole through a stream below that holds the
//! client's bytes and meets the server's demands from them.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]

mod body;
mod exchange;
mod head;
mod reply;

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue};

use super::{boxed, env, header};
use crate::server::{self, Body, Call, Event, Limits, Request, Response, Server};

/// Small limits, so that a test reaches each of them.
const LIMITS: Limits = Limits { head: 256, headers: 8, body: 1024, read: 16, response: 256, send: 16 };

/// What the server sets aside before it reads a request: room for the
/// longest response head.
const ASIDE: Down = Down::Demand { read: Read::Nothing, room: 256 };

/// The demand for a head's first line: a scan to LF of all of the head.
const FIRST_LINE: Down = Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 256 }, room: 0 };

/// A server and its two queues, with room for what one call emits.
struct Machine {
    server: Server,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<Down>,
}

impl Machine {
    fn new(limits: Limits) -> Machine {
        Machine {
            server: Server::new(&limits),
            env: env(limits),
            above: Queue::with_capacity(server::UP_MAX_OUT.above.max(server::DOWN_MAX_OUT.above)),
            below: Queue::with_capacity(server::UP_MAX_OUT.below.max(server::DOWN_MAX_OUT.below)),
        }
    }

    /// Sends `rq` down; what came of it, above and below.
    fn down(&mut self, rq: Request) -> (Vec<Event>, Vec<Down>) {
        server::down(&mut self.server, &self.env, rq, &mut self.above, &mut self.below);
        assert!(self.above.len() <= server::DOWN_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= server::DOWN_MAX_OUT.below, "MAX_OUT below");
        self.take()
    }

    /// Sends `ev` up; what came of it, above and below.
    fn up(&mut self, ev: Up) -> (Vec<Event>, Vec<Down>) {
        server::up(&mut self.server, &self.env, ev, &mut self.above, &mut self.below);
        assert!(self.above.len() <= server::UP_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= server::UP_MAX_OUT.below, "MAX_OUT below");
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

    /// Asks for the next request, and grants the room set aside for its
    /// response: the demand for its first line follows.
    fn ready(&mut self) {
        let (events, requests) = self.down(Request::Next);
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: self.env.limits.response }]);
        let (events, requests) = self.up(Up::Room);
        assert!(events.is_empty(), "{events:?}");
        let budget = self.env.limits.head;
        assert_eq!(requests, [Down::Demand { read: Read::Scan { until: Delimiter::LF, max: budget }, room: 0 }]);
    }

    /// Reads `head` once ready, each demand met as a stream below holding
    /// it would meet it, until something goes up: what that step came to.
    fn head(&mut self, head: &[u8]) -> (Vec<Event>, Vec<Down>) {
        self.ready();
        let mut intake = Intake::with_capacity(u32::try_from(head.len()).unwrap().max(self.env.limits.head));
        intake.append(head).unwrap();
        let mut read = Read::Scan { until: Delimiter::LF, max: self.env.limits.head };
        for _ in 0..=head.len() {
            let (events, requests) = match intake.meet(read) {
                Some(bytes) => self.up(Up::Bytes(bytes)),
                None => panic!("{} is read whole before it runs out", head.escape_ascii()),
            };
            if !events.is_empty() {
                return (events, requests);
            }
            let [Down::Demand { read: next, room: 0 }] = requests[..] else { panic!("the next line: {requests:?}") };
            read = next;
        }
        panic!("a head is read a line at a time");
    }

    /// Reads `head`, which must make a call: the call.
    fn call(&mut self, head: &[u8]) -> Call {
        let (mut events, requests) = self.head(head);
        assert!(requests.is_empty(), "nothing is demanded until the side above acts: {requests:?}");
        match events.pop() {
            Some(Event::Call(call)) if events.is_empty() => call,
            other => panic!("a call, not {other:?}"),
        }
    }
}

fn get() -> &'static [u8] {
    b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
}

/// A response of `status` with a field, framed by `body`.
fn response(status: u16, body: Body) -> Response {
    Response { status, headers: Box::new([header(b"Server", b"skein")]), body, close: false }
}

/// When the side above responds, in a driven exchange.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum When {
    /// Once the call came, before it reads the body.
    First,
    /// Once it read the body to its end.
    AfterBody,
    /// Once it discarded the body, before its end came.
    Discarding,
}

/// How a driven exchange's neighbours behave.
#[derive(Clone, Copy, Debug)]
struct Drive<'a> {
    /// What the side above demands of the request body, again and again.
    read: Read,
    when: When,
    /// The response body the side above writes, in pieces of at most
    /// [`Limits::send`], if the response has one.
    reply: &'a [u8],
}

/// What a whole exchange came to.
#[derive(Debug)]
struct Served {
    call: Option<Call>,
    /// The request body's bytes the side above received.
    body: Vec<u8>,
    /// Whether the request body's stream ended with `End`.
    ended: bool,
    /// The fault the request body's stream failed with, if it did.
    body_failed: Option<Fault>,
    /// What the server sent below: the response, and anything before it.
    sent: Vec<u8>,
    /// What the reply's stream was told, `Room`s aside.
    reply: Vec<Up>,
    /// The exchange's terminal event, or the `Next`'s if no call answered.
    outcome: Option<Event>,
}

/// Runs one exchange: the side above asks for the next request, reads its
/// body as `drive` says, responds with `response` when it says, and writes
/// the reply; the stream below holds `request` whole, grants all the room
/// asked for, and ends once a read can no longer be met.
#[expect(clippy::too_many_lines, reason = "one loop that plays both neighbours, as the client's driver does")]
fn serve(machine: &mut Machine, request: &[u8], response: Response, drive: Drive<'_>) -> Served {
    let cap = u32::try_from(request.len()).unwrap().max(server::largest_read(&machine.env.limits));
    let mut intake = Intake::with_capacity(cap);
    intake.append(request).unwrap();
    let mut out = Served {
        call: None,
        body: Vec::new(),
        ended: false,
        body_failed: None,
        sent: Vec::new(),
        reply: Vec::new(),
        outcome: None,
    };
    let mut outstanding: Option<(Read, u32)> = None;
    let mut responded = false;
    let mut replies = false;
    let mut response = Some(response);
    let mut written = 0_usize;
    let mut wanted = 0;
    let mut events = Vec::new();
    let mut requests = Vec::new();
    let (e, r) = machine.down(Request::Next);
    events.extend(e);
    requests.extend(r);
    for _ in 0..100_000_u32 {
        if !requests.is_empty() {
            match requests.remove(0) {
                Down::Demand { read: Read::Nothing, room: 0 } => outstanding = None,
                Down::Demand { read, room } => {
                    assert!(outstanding.is_none(), "one demand at a time");
                    outstanding = Some((read, room));
                }
                Down::Send(bytes) => out.sent.extend_from_slice(&bytes),
                Down::Finish => panic!("the server never finishes the stream below"),
            }
            continue;
        }
        let (e, r) = if events.is_empty() {
            let Some((read, room)) = outstanding.take() else { panic!("the exchange stalled: {out:?}") };
            if room > 0 {
                machine.up(Up::Room)
            } else {
                match intake.meet(read) {
                    Some(bytes) => machine.up(Up::Bytes(bytes)),
                    None => machine.up(Up::End),
                }
            }
        } else {
            match events.remove(0) {
                Event::Call(call) => {
                    replies = has_reply(&call, response.as_ref().unwrap());
                    out.call = Some(call);
                    match drive.when {
                        When::First => {
                            responded = true;
                            let (mut e, mut r) = machine.down(Request::Respond(response.take().unwrap()));
                            let (more_e, more_r) = reply_next(machine, replies, drive.reply.len(), &mut wanted, &e);
                            e.extend(more_e);
                            r.extend(more_r);
                            (e, r)
                        }
                        When::AfterBody => machine.down(Request::Body(Down::Demand { read: drive.read, room: 0 })),
                        When::Discarding => {
                            responded = true;
                            let (mut e, mut r) = machine.down(Request::Discard);
                            let (more_e, more_r) = machine.down(Request::Respond(response.take().unwrap()));
                            e.extend(more_e);
                            r.extend(more_r);
                            let (more_e, more_r) = reply_next(machine, replies, drive.reply.len(), &mut wanted, &e);
                            e.extend(more_e);
                            r.extend(more_r);
                            (e, r)
                        }
                    }
                }
                Event::Body(Up::Bytes(bytes)) => {
                    out.body.extend_from_slice(&bytes);
                    machine.down(Request::Body(Down::Demand { read: drive.read, room: 0 }))
                }
                Event::Body(Up::End) => {
                    out.ended = true;
                    if responded {
                        (Vec::new(), Vec::new())
                    } else {
                        responded = true;
                        let (mut e, mut r) = machine.down(Request::Respond(response.take().unwrap()));
                        let (more_e, more_r) = reply_next(machine, replies, drive.reply.len(), &mut wanted, &e);
                        e.extend(more_e);
                        r.extend(more_r);
                        (e, r)
                    }
                }
                Event::Body(Up::Failed(fault)) => {
                    out.body_failed = Some(fault);
                    (Vec::new(), Vec::new())
                }
                Event::Body(Up::Room) => panic!("room on the request body's stream"),
                Event::Reply(Up::Room) => {
                    let end = written.saturating_add(usize::try_from(wanted).unwrap()).min(drive.reply.len());
                    let piece = &drive.reply[written..end];
                    written = end;
                    let (mut e, mut r) = machine.down(Request::Reply(Down::Send(boxed(piece))));
                    let left = drive.reply.len().saturating_sub(written);
                    let (more_e, more_r) = reply_next(machine, replies, left, &mut wanted, &e);
                    e.extend(more_e);
                    r.extend(more_r);
                    (e, r)
                }
                Event::Reply(other) => {
                    out.reply.push(other);
                    (Vec::new(), Vec::new())
                }
                Event::Refused(refusal) => panic!("the response was refused: {refusal:?}"),
                event @ (Event::Done(_) | Event::Failed(_) | Event::Ended) => {
                    out.outcome = Some(event);
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

/// The side above's next move on the reply, with `left` bytes to write:
/// room for at most [`Limits::send`], or `Finish`; nothing if the response
/// was refused, or has no body, or the exchange ended.
fn reply_next(
    machine: &mut Machine,
    replies: bool,
    left: usize,
    wanted: &mut u32,
    so_far: &[Event],
) -> (Vec<Event>, Vec<Down>) {
    for event in so_far {
        match event {
            Event::Refused(_) | Event::Done(_) | Event::Failed(_) => return (Vec::new(), Vec::new()),
            Event::Call(_) | Event::Ended | Event::Body(_) | Event::Reply(_) | Event::Closed => {}
        }
    }
    match machine.server.waiting() {
        server::Waiting::Close | server::Waiting::Nothing | server::Waiting::Next => return (Vec::new(), Vec::new()),
        server::Waiting::Room | server::Waiting::Request | server::Waiting::Body | server::Waiting::Above => {}
    }
    if !replies {
        return (Vec::new(), Vec::new());
    }
    if left == 0 {
        return machine.down(Request::Reply(Down::Finish));
    }
    *wanted = machine.env.limits.send.min(u32::try_from(left).unwrap());
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: *wanted }))
}

/// Whether `response` to `call` has a body for the side above to write.
fn has_reply(call: &Call, response: &Response) -> bool {
    let bodyless = call.method == crate::Method::Head || response.status == 204 || response.status == 304;
    !bodyless && response.body != Body::None
}
