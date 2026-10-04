//! The server's and the event writer's worst cases against the counting
//! allocator (http.md, 5.6 and 4.4; programming-model.md, 6.3): every call
//! of an entry point a step of the meter, and what the machine held of its
//! own never more than its `worst_case`.
//!
//! What the connection holds is made before the meter: the queues, the
//! stream below and its buffer. An input moved into a step was counted by
//! whoever made it (testing.md, 5). Each delivery is made between steps, by
//! the stream below, to the machine's demand: the machine's worst case
//! covers it. A response, a piece of the response body, and an event or a
//! comment to write are the side above's, which made them: the step that
//! takes one is checked against the worst case and the input's own size.
//! What a step emits is handed out: dropped before the check, as the next
//! step's input.

use std::mem::size_of;

use skein_heap::{Counting, Meter};
use skein_http::Header;
use skein_http::server::{self, Body, Event, Limits, Request, Response, Server};
use skein_http::sse::writer::{self, Outgoing, Writer};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue, Time, Wall};

#[global_allocator]
static HEAP: Counting = Counting;

/// What a run does besides its exchange.
#[derive(Clone, Copy, Debug)]
enum Interrupt {
    Nothing,
    /// The side above closes the server after this many steps.
    Close(u32),
    /// The stream fails after this many steps.
    Fail(u32),
    /// The side above discards the body after this many steps, once a call
    /// came.
    Discard(u32),
}

/// The heap a response holds: its list of fields and their text.
fn size_of_response(response: &Response) -> u64 {
    let mut size = response.headers.len() * size_of::<Header>();
    for header in &response.headers {
        size += header.name.len() + header.value.len();
    }
    u64::try_from(size).expect("fits a u64")
}

/// What the side above does for the call.
#[derive(Clone, Copy, Debug)]
struct Script<'a> {
    response: &'a Response,
    reply: &'a [u8],
    read: Read,
    /// Whether it responds before it reads the body.
    first: bool,
}

/// The steps of one request, each checked: the most the server held of its
/// own in a step, less the input it was given.
fn serve(limits: Limits, request: &[u8], script: Script<'_>, interrupt: Interrupt) -> u64 {
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(server::UP_MAX_OUT.above.max(server::DOWN_MAX_OUT.above));
    let mut below = Queue::with_capacity(server::UP_MAX_OUT.below.max(server::DOWN_MAX_OUT.below));
    let cap = u32::try_from(request.len()).expect("fits a u32").max(server::largest_read(&limits));
    let mut intake = Intake::with_capacity(cap);
    intake.append(request).expect("the stream holds it");
    let what = format!("{limits:?} {interrupt:?} {script:?} reading {}", request.escape_ascii());
    let bound = server::worst_case(&limits).expect("the limits are honoured");

    let meter = Meter::new();
    meter.start();
    let mut server = Server::new(&limits);
    let mut most = meter.check(meter.end(), bound, &what);
    let mut flags = Flags::default();
    meter.start();
    server::down(&mut server, &env, Request::Next, &mut above, &mut below);
    let step = meter.end();
    flags.take(&mut above, &mut below);
    most = most.max(meter.check(step, bound, &what));

    for steps in 1..100_000 {
        if flags.closed {
            return most;
        }
        let input = next(&mut flags, &mut intake, &limits, script, interrupt, steps);
        let input_size = match &input {
            Input::Down(Request::Respond(response)) => size_of_response(response),
            Input::Down(Request::Reply(Down::Send(piece))) => u64::try_from(piece.len()).expect("fits a u64"),
            Input::Up(_) | Input::Down(_) => 0,
        };
        meter.start();
        match input {
            Input::Up(ev) => server::up(&mut server, &env, ev, &mut above, &mut below),
            Input::Down(rq) => server::down(&mut server, &env, rq, &mut above, &mut below),
        }
        let step = meter.end();
        flags.take(&mut above, &mut below);
        most = most.max(meter.check(step, bound + input_size, &what).saturating_sub(input_size));
    }
    panic!("{what}: a request is served in a few steps a byte");
}

/// What the test knows of the exchange between steps: flags, no heap.
#[derive(Default, Debug)]
#[expect(clippy::struct_excessive_bools, reason = "the exchange's state, a flag each")]
struct Flags {
    /// The server's demand outstanding below.
    below: Option<(Read, u32)>,
    call: bool,
    responded: bool,
    body_wanted: bool,
    body_over: bool,
    reply: Reply,
    written: usize,
    discarded: bool,
    over: bool,
    closing: bool,
    closed: bool,
    failed_below: bool,
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
enum Reply {
    #[default]
    None,
    Idle,
    Wanted(u32),
    Granted(u32),
    Over,
}

impl Flags {
    /// What a step emitted: the events and requests taken, their payloads
    /// dropped as their receivers'.
    fn take(&mut self, above: &mut Queue<Event>, below: &mut Queue<Down>) {
        while let Some(event) = above.pop() {
            match event {
                Event::Call(call) => {
                    drop(call);
                    self.call = true;
                }
                Event::Ended | Event::Failed(_) | Event::Done(_) => self.over = true,
                Event::Body(Up::Bytes(bytes)) => {
                    drop(bytes);
                    self.body_wanted = false;
                }
                Event::Body(Up::End | Up::Failed(_)) => {
                    self.body_over = true;
                    self.body_wanted = false;
                }
                Event::Body(Up::Room) => panic!("room on the request body"),
                Event::Reply(Up::Room) => {
                    let Reply::Wanted(room) = self.reply else { panic!("room for a demand") };
                    self.reply = Reply::Granted(room);
                }
                Event::Reply(Up::Failed(_)) => self.reply = Reply::Over,
                Event::Reply(other) => panic!("{other:?} on the reply"),
                Event::Refused(refusal) => panic!("a sound response refused: {refusal:?}"),
                Event::Closed => self.closed = true,
            }
        }
        while let Some(request) = below.pop() {
            match request {
                Down::Demand { read: Read::Nothing, room: 0 } => self.below = None,
                Down::Demand { read, room } => self.below = Some((read, room)),
                Down::Send(bytes) => drop(bytes),
                Down::Finish => panic!("the server never finishes"),
            }
        }
    }
}

enum Input {
    Up(Up),
    Down(Request),
}

/// The next input: an interruption, the side above's next move, or the
/// side below's answer.
fn next(
    flags: &mut Flags,
    intake: &mut Intake,
    limits: &Limits,
    script: Script<'_>,
    interrupt: Interrupt,
    steps: u32,
) -> Input {
    match interrupt {
        Interrupt::Close(at) if steps >= at && !flags.closing => {
            flags.closing = true;
            return Input::Down(Request::Close);
        }
        Interrupt::Fail(at) if steps >= at && !flags.failed_below && !flags.over => {
            flags.failed_below = true;
            flags.below = None;
            return Input::Up(Up::Failed(Fault::Reset));
        }
        Interrupt::Discard(at) if steps >= at && flags.call && !flags.discarded && !flags.body_over && !flags.over => {
            flags.discarded = true;
            flags.body_over = true;
            return Input::Down(Request::Discard);
        }
        Interrupt::Nothing | Interrupt::Close(_) | Interrupt::Fail(_) | Interrupt::Discard(_) => {}
    }
    if flags.over || flags.failed_below {
        flags.closing = true;
        return Input::Down(Request::Close);
    }
    // The side above: its body, its response, then its reply.
    if flags.call && !flags.responded && (script.first || flags.body_over) {
        flags.responded = true;
        let sends = script.response.status != 204 && script.response.body != Body::None;
        flags.reply = if sends { Reply::Idle } else { Reply::None };
        return Input::Down(Request::Respond(script.response.clone()));
    }
    if flags.call && !flags.body_over && !flags.body_wanted {
        flags.body_wanted = true;
        return Input::Down(Request::Body(Down::Demand { read: script.read, room: 0 }));
    }
    let left = script.reply.len() - flags.written;
    match flags.reply {
        Reply::Idle if left == 0 => {
            flags.reply = Reply::Over;
            return Input::Down(Request::Reply(Down::Finish));
        }
        Reply::Idle => {
            let room = limits.send.min(u32::try_from(left).expect("fits a u32"));
            flags.reply = Reply::Wanted(room);
            return Input::Down(Request::Reply(Down::Demand { read: Read::Nothing, room }));
        }
        Reply::Granted(room) => {
            let len = left.min(usize::try_from(room).expect("fits a usize"));
            let piece = script.reply[flags.written..flags.written + len].to_vec();
            flags.written += len;
            flags.reply = Reply::Idle;
            return Input::Down(Request::Reply(Down::Send(piece.into())));
        }
        Reply::None | Reply::Wanted(_) | Reply::Over => {}
    }
    // The side below.
    let (demanded, room) = flags.below.take().expect("the exchange waits for the side below");
    if room > 0 {
        return Input::Up(Up::Room);
    }
    match intake.meet(demanded) {
        Some(bytes) => Input::Up(Up::Bytes(bytes)),
        None => Input::Up(Up::End),
    }
}

const LIMITS: Limits = Limits { head: 512, headers: 10, body: 4096, read: 96, response: 256, send: 64 };

/// A request head at the limits: its request line, then `headers` fields
/// in all, `fields` among them, the last bringing the head to its limit,
/// the blank line included.
fn full_head(limits: &Limits, line: &[u8], fields: &[u8]) -> Vec<u8> {
    let mut out = line.to_vec();
    out.extend_from_slice(b"Host: example.com\r\n");
    out.extend_from_slice(fields);
    let mut count = 1;
    for &byte in fields {
        count += usize::from(byte == b'\n');
    }
    while count + 1 < usize::try_from(limits.headers).expect("fits a usize") {
        out.extend_from_slice(format!("X-{count}: v\r\n").as_bytes());
        count += 1;
    }
    let head = usize::try_from(limits.head).expect("fits a usize");
    let room = head - out.len() - 2;
    assert!(room >= 6, "the limits leave room for the last field");
    out.extend_from_slice(b"X:");
    out.extend_from_slice(&vec![b'a'; room - 4]);
    out.extend_from_slice(b"\r\n\r\n");
    assert_eq!(out.len(), head);
    out
}

/// A response whose head is exactly the limit, framed by `body`, on a
/// connection that closes after it; a byte short of it on one kept.
fn full_response(limits: &Limits, body: Body) -> Response {
    let mut response = Response { status: 200, headers: Box::new([]), body, close: false };
    let base = head_len(&response);
    let room = usize::try_from(limits.response).expect("fits a usize") - base - 5;
    response.headers = Box::new([Header { name: b"X".to_vec().into(), value: vec![b'v'; room].into() }]);
    assert_eq!(head_len(&response), usize::try_from(limits.response).expect("fits a usize"));
    response
}

/// A `Date` field: every head the server writes has one, of this length.
const DATE: &str = "Date: Thu, 01 Jan 1970 00:00:00 GMT\r\n";

/// The length of the head the server writes for `response`, on a
/// connection that closes after it.
fn head_len(response: &Response) -> usize {
    let mut len = "HTTP/1.1 200 OK\r\n".len() + DATE.len() + 2 + "Connection: close\r\n".len();
    for header in &response.headers {
        len += header.name.len() + 2 + header.value.len() + 2;
    }
    len + match response.body {
        Body::None => "Content-Length: 0\r\n".len(),
        Body::Length(length) => format!("Content-Length: {length}\r\n").len(),
        Body::Chunked => "Transfer-Encoding: chunked\r\n".len(),
    }
}

#[test]
fn every_entry_point_of_the_server_holds_no_more_than_its_worst_case_at_its_limits() {
    let body: Vec<u8> = (0..400).map(|n| b'a' + u8::try_from(n % 26).unwrap()).collect();
    let mut by_length =
        full_head(&LIMITS, b"POST /x HTTP/1.1\r\n", format!("Content-Length: {}\r\n", body.len()).as_bytes());
    by_length.extend_from_slice(&body);
    let mut chunked = full_head(&LIMITS, b"POST /x HTTP/1.1\r\n", b"Transfer-Encoding: chunked\r\n");
    for piece in body.chunks(37) {
        chunked.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
        chunked.extend_from_slice(piece);
        chunked.extend_from_slice(b"\r\n");
    }
    chunked.extend_from_slice(b"0\r\n");
    chunked.extend_from_slice(format!("Trailer: {}\r\n\r\n", "t".repeat(498)).as_bytes());
    let mut expecting = full_head(
        &LIMITS,
        b"POST /x HTTP/1.1\r\n",
        format!("Expect: 100-continue\r\nContent-Length: {}\r\n", body.len()).as_bytes(),
    );
    expecting.extend_from_slice(&body);
    let get = full_head(&LIMITS, b"GET / HTTP/1.1\r\n", b"");
    let head = full_head(&LIMITS, b"HEAD / HTTP/1.1\r\n", b"");
    let mut rejected = b"GET / HTTP/1.1\r\nX: ".to_vec();
    rejected.extend_from_slice(&[b'r'; 600]);
    let truncated = b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 50\r\n\r\nshort".to_vec();
    let reply: Vec<u8> = (0..300).map(|n| b'A' + u8::try_from(n % 26).unwrap()).collect();
    let by_length_reply = full_response(&LIMITS, Body::Length(300));
    let chunked_reply = full_response(&LIMITS, Body::Chunked);
    let no_reply = full_response(&LIMITS, Body::None);
    let cases: [(&[u8], &Response, &[u8], &str); 8] = [
        (&by_length, &by_length_reply, &reply, "a head at its limits, a body by length, a reply by length"),
        (&chunked, &chunked_reply, &reply, "a chunked body, its trailer section at the limit, a chunked reply"),
        (&expecting, &by_length_reply, &reply, "a 100 (Continue) before the body"),
        (&get, &chunked_reply, &reply, "no body, a chunked reply"),
        (&head, &by_length_reply, b"", "a response to HEAD"),
        (&get, &no_reply, b"", "a response without a body"),
        (&rejected, &no_reply, b"", "a head rejected"),
        (&truncated, &no_reply, b"", "a body cut short"),
    ];
    for (request, response, reply, what) in cases {
        for read in [Read::Fill(LIMITS.read), Read::Fill(1), Read::Scan { until: Delimiter::LF, max: LIMITS.read }] {
            for first in [false, true] {
                let script = Script { response, reply, read, first };
                let most = serve(LIMITS, request, script, Interrupt::Nothing);
                assert!(most > 0, "{what}");
            }
        }
        let script = Script { response, reply, read: Read::Fill(LIMITS.read), first: false };
        for at in 0..60 {
            let _ = serve(LIMITS, request, script, Interrupt::Close(at));
            let _ = serve(LIMITS, request, script, Interrupt::Fail(at));
            let _ = serve(LIMITS, request, script, Interrupt::Discard(at));
        }
    }
}

#[test]
fn the_server_s_peak_is_its_worst_case_in_either_phase() {
    // Reading a head, the larger phase under these limits: a request line
    // filling the head, ended by an LF alone, and its target's copy.
    let mut line = b"GET /".to_vec();
    line.resize(usize::try_from(LIMITS.head).unwrap() - " HTTP/1.1\n".len(), b't');
    line.extend_from_slice(b" HTTP/1.1\n");
    let response = Response { status: 204, headers: Box::new([]), body: Body::None, close: false };
    let script = Script { response: &response, reply: b"", read: Read::Fill(LIMITS.read), first: false };
    let most = serve(LIMITS, &line, script, Interrupt::Nothing);
    assert_eq!(most, server::worst_case(&LIMITS).unwrap(), "a head at its limit");
    // An exchange, the larger phase under these: a response head at its
    // limit, held for a connection kept as the side above discards the
    // body, and a piece of the most a read is.
    let limits = Limits { head: 64, headers: 2, body: 4096, read: 1024, response: 2048, send: 64 };
    let mut request = b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 2000\r\n\r\n".to_vec();
    request.resize(request.len() + 2000, b'b');
    let mut response = Response { status: 200, headers: Box::new([]), body: Body::None, close: false };
    let kept = head_len(&response) - "Connection: close\r\n".len();
    let value = vec![b'v'; usize::try_from(limits.response).unwrap() - kept - "X: \r\n".len()];
    response.headers = Box::new([Header { name: b"X".to_vec().into(), value: value.into() }]);
    let script = Script { response: &response, reply: b"", read: Read::Fill(limits.read), first: false };
    let most = serve(limits, &request, script, Interrupt::Discard(0));
    assert_eq!(most, server::worst_case(&limits).unwrap(), "a response head at its limit, held");
}

#[test]
fn the_server_s_head_its_carry_over_and_a_delivery_count_together() {
    // The intake full but for a byte, and a delivery that fills it: the
    // carry-over and the delivery are held at once.
    let mut request = b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    request.extend_from_slice(format!("{:x}\r\n{}\r\n", 95, "b".repeat(95)).as_bytes());
    request.extend_from_slice(b"1\r\nc\r\n0\r\n\r\n");
    let response = Response { status: 204, headers: Box::new([]), body: Body::None, close: false };
    let script = Script { response: &response, reply: b"", read: Read::Fill(LIMITS.read), first: false };
    let most = serve(LIMITS, &request, script, Interrupt::Nothing);
    assert!(most > u64::from(LIMITS.read) + 95, "the intake and the delivery at once: {most}");
}

/// The steps of writing `items`, each checked: the most the writer held of
/// its own in a step, less the item it was given.
fn written(limits: writer::Limits, items: &[Outgoing], close: Option<u32>) -> u64 {
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(writer::UP_MAX_OUT.above.max(writer::DOWN_MAX_OUT.above));
    let mut below = Queue::with_capacity(writer::UP_MAX_OUT.below.max(writer::DOWN_MAX_OUT.below));
    let what = format!("{limits:?} {close:?} writing {items:?}");
    let bound = writer::worst_case(&limits).expect("the limits are honoured");
    let meter = Meter::new();
    meter.start();
    let mut machine = Writer::new(&limits);
    let mut most = meter.check(meter.end(), bound, &what);
    let mut next = 0;
    let mut room = false;
    let mut pending = false;
    let mut finished = false;
    for steps in 0..100_000 {
        let closing = close == Some(steps) || (finished && !pending);
        // The item, made between steps, and read by the next.
        let item = if closing || room { None } else { items.get(next).cloned() };
        let input_size = match &item {
            Some(item) => item.name.len() + item.data.len() + item.id.as_ref().map_or(0, |id| id.len()),
            None => 0,
        };
        meter.start();
        if closing {
            writer::down(&mut machine, &env, writer::Request::Close, &mut above, &mut below);
        } else if room {
            room = false;
            writer::up(&mut machine, &env, Up::Room, &mut above, &mut below);
        } else if let Some(item) = item {
            next += 1;
            pending = true;
            writer::down(&mut machine, &env, writer::Request::Event(item), &mut above, &mut below);
        } else {
            finished = true;
            writer::down(&mut machine, &env, writer::Request::Finish, &mut above, &mut below);
        }
        let step = meter.end();
        let mut closed = false;
        while let Some(event) = above.pop() {
            match event {
                writer::Event::Sent | writer::Event::Refused(_) | writer::Event::Failed(_) => pending = false,
                writer::Event::Closed => closed = true,
            }
        }
        while let Some(request) = below.pop() {
            match request {
                Down::Demand { read: Read::Nothing, room: 0 } => room = false,
                Down::Demand { .. } => room = true,
                Down::Send(bytes) => drop(bytes),
                Down::Finish => {}
            }
        }
        let input_size = u64::try_from(input_size).expect("fits a u64");
        most = most.max(meter.check(step, bound + input_size, &what) - input_size);
        if closed {
            return most;
        }
    }
    panic!("{what}: events are written in a few steps a piece");
}

#[test]
fn the_writer_holds_no_more_than_its_worst_case_and_its_peak_is_its_worst_case_at_its_limits() {
    let limits = writer::Limits { event: 300, chunk: 32 };
    // `event: `, the type, `id: `, the id, `data: ` and the data, each with an
    // LF, and the blank line: 300 bytes.
    let at_limit = Outgoing {
        name: b"delta".to_vec().into(),
        data: vec![b'd'; 300 - 13 - 6 - 7 - 1].into(),
        id: Some(b"1".to_vec().into()),
        retry: None,
    };
    let bound = writer::worst_case(&limits).expect("the limits are honoured");
    assert_eq!(
        written(limits, std::slice::from_ref(&at_limit), None),
        bound,
        "at its limits, its peak is its worst case exactly"
    );
    let past = Outgoing { data: vec![b'x'; 400].into(), ..at_limit.clone() };
    let small = Outgoing { name: Box::new([]), data: b"a\nb\r\nc".to_vec().into(), id: None, retry: Some(5) };
    let items = [small.clone(), at_limit.clone(), past, small];
    for at in 0..40 {
        let _ = written(limits, &items, Some(at));
    }
}
