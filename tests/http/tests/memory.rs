//! The client's and the reader's worst cases against the counting
//! allocator (http.md, 3.5 and 4.2; programming-model.md, 6.3): every call of
//! an entry point a step of the meter, and what the machine held of its own
//! never more than its `worst_case`.
//!
//! What the connection holds is made before the meter: the queues, the
//! stream below and its buffer. An input moved into a step was counted by
//! whoever made it (testing.md, 5). Each delivery is made between steps, by
//! the stream below, to the machine's demand: the machine's worst case
//! covers it. A call, and a piece of the request body, which the client
//! passes on in the same step, are the side above's, which made them: the
//! step that takes one is checked against the worst case and the input's
//! own size. What a step emits is handed out: dropped before the check, as
//! the next step's input.

use std::mem::size_of;

use skein_heap::{Counting, Meter};
use skein_http::Header;
use skein_http::client::{self, Body, Call, Client, Event, Limits, Method, Request};
use skein_http::sse::{self, Reader};
use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue, Time, Wall};

#[global_allocator]
static HEAP: Counting = Counting;

/// What a run does besides its exchange.
#[derive(Clone, Copy, Debug)]
enum Interrupt {
    Nothing,
    /// The side above closes the client after this many steps.
    Close(u32),
    /// The stream fails after this many steps.
    Fail(u32),
    /// The side above discards the body after this many steps, if it reads it.
    Discard(u32),
}

/// The heap a call holds: its text and its list of fields.
fn size_of_call(call: &Call) -> u64 {
    let mut size = call.target.len() + call.headers.len() * size_of::<Header>();
    for header in &call.headers {
        size += header.name.len() + header.value.len();
    }
    u64::try_from(size).expect("fits a u64")
}

/// What a run came to: the most the machine held of its own in a step, and
/// whether its exchange was done, if it ended.
#[derive(Debug)]
struct Ran {
    most: u64,
    done: Option<bool>,
}

/// The steps of one exchange, each checked.
fn exchange(limits: Limits, call: &Call, upload: usize, response: &[u8], read: Read, interrupt: Interrupt) -> Ran {
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(client::UP_MAX_OUT.above.max(client::DOWN_MAX_OUT.above));
    let mut below = Queue::with_capacity(client::UP_MAX_OUT.below.max(client::DOWN_MAX_OUT.below));
    let cap = u32::try_from(response.len()).expect("fits a u32").max(client::largest_read(&limits));
    let mut intake = Intake::with_capacity(cap);
    intake.append(response).expect("the stream holds it");
    let what = format!("{limits:?} {interrupt:?} {read:?} reading {}", response.escape_ascii());
    let bound = client::worst_case(&limits).expect("the limits are honoured");

    let meter = Meter::new();
    meter.start();
    let mut client = Client::new(&limits);
    let mut most = meter.check(meter.end(), bound, &what);
    // The call, made between steps, and read by the first.
    let call = call.clone();
    let call_size = size_of_call(&call);
    let body = call.body;
    meter.start();
    client::down(&mut client, &env, Request::Call(call), &mut above, &mut below);
    let step = meter.end();
    let mut flags = Flags { upload_left: upload, upload_over: body == Body::None, ..Flags::default() };
    flags.take(&mut above, &mut below);
    most = most.max(meter.check(step, bound + call_size, &what));

    for steps in 1..100_000 {
        if flags.closed {
            return Ran { most, done: flags.done };
        }
        let input = next(&mut flags, &mut intake, &limits, read, interrupt, steps);
        let input_size = match &input {
            Input::Down(Request::Upload(Down::Send(piece))) => u64::try_from(piece.len()).expect("fits a u64"),
            Input::Up(_) | Input::Down(_) => 0,
        };
        meter.start();
        match input {
            Input::Up(ev) => client::up(&mut client, &env, ev, &mut above, &mut below),
            Input::Down(rq) => client::down(&mut client, &env, rq, &mut above, &mut below),
        }
        let step = meter.end();
        flags.take(&mut above, &mut below);
        most = most.max(meter.check(step, bound + input_size, &what));
    }
    panic!("{what}: an exchange is read in a few steps a byte");
}

/// What the test knows of the exchange between steps: flags, no heap.
#[derive(Default, Debug)]
#[expect(clippy::struct_excessive_bools, reason = "the exchange's state, a flag each")]
struct Flags {
    /// The client's demand outstanding below.
    below: Option<(Read, u32)>,
    granted: bool,
    upload_left: usize,
    upload_wanted: bool,
    upload_room: bool,
    upload_over: bool,
    response: bool,
    body_wanted: bool,
    discarded: bool,
    over: bool,
    closing: bool,
    closed: bool,
    failed_below: bool,
    /// Whether the exchange was done, once it ended.
    done: Option<bool>,
}

impl Flags {
    /// What a step emitted: the events and requests taken, their payloads
    /// dropped as their receivers'.
    fn take(&mut self, above: &mut Queue<Event>, below: &mut Queue<Down>) {
        while let Some(event) = above.pop() {
            match event {
                Event::Response(response) => {
                    drop(response);
                    self.response = true;
                }
                Event::Body(Up::Bytes(bytes)) => {
                    drop(bytes);
                    self.body_wanted = false;
                }
                Event::Body(Up::End | Up::Failed(_)) => self.body_wanted = false,
                Event::Done(_) | Event::Failed(_) => {
                    self.over = true;
                    self.done = Some(matches!(event, Event::Done(_)));
                }
                Event::Body(Up::Room) => panic!("room on the body"),
                Event::Upload(Up::Room) => {
                    self.upload_wanted = false;
                    self.upload_room = true;
                }
                Event::Upload(Up::Failed(_)) => self.upload_over = true,
                Event::Upload(other) => panic!("{other:?} on the upload"),
                Event::Closed => self.closed = true,
            }
        }
        while let Some(request) = below.pop() {
            match request {
                Down::Demand { read: Read::Nothing, room: 0 } => self.below = None,
                Down::Demand { read, room } => self.below = Some((read, room)),
                Down::Send(bytes) => {
                    drop(bytes);
                    self.granted = false;
                }
                Down::Finish => panic!("the client never finishes"),
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
    read: Read,
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
        Interrupt::Discard(at) if steps >= at && flags.response && !flags.discarded && !flags.over => {
            flags.discarded = true;
            return Input::Down(Request::Discard);
        }
        Interrupt::Nothing | Interrupt::Close(_) | Interrupt::Fail(_) | Interrupt::Discard(_) => {}
    }
    if flags.over || flags.failed_below {
        flags.closing = true;
        return Input::Down(Request::Close);
    }
    // The side above: its upload, then the body.
    if flags.upload_room {
        flags.upload_room = false;
        let len = flags.upload_left.min(usize::try_from(limits.send).expect("fits a usize"));
        flags.upload_left -= len;
        return Input::Down(Request::Upload(Down::Send(vec![b'u'; len].into())));
    }
    if !flags.upload_over && !flags.upload_wanted && flags.upload_left > 0 {
        flags.upload_wanted = true;
        let room = limits.send.min(u32::try_from(flags.upload_left).expect("fits a u32"));
        return Input::Down(Request::Upload(Down::Demand { read: Read::Nothing, room }));
    }
    if !flags.upload_over && !flags.upload_wanted && flags.upload_left == 0 {
        flags.upload_over = true;
        return Input::Down(Request::Upload(Down::Finish));
    }
    if flags.response && !flags.body_wanted && !flags.discarded {
        flags.body_wanted = true;
        return Input::Down(Request::Body(Down::Demand { read, room: 0 }));
    }
    // The side below.
    let (demanded, room) = flags.below.take().expect("the exchange waits for the side below");
    if room > 0 {
        flags.granted = true;
        return Input::Up(Up::Room);
    }
    match intake.meet(demanded) {
        Some(bytes) => Input::Up(Up::Bytes(bytes)),
        None => Input::Up(Up::End),
    }
}

fn host() -> Header {
    Header { name: b"Host".to_vec().into(), value: b"example.com".to_vec().into() }
}

fn get() -> Call {
    Call {
        method: Method::Get,
        target: b"/".to_vec().into(),
        headers: Box::new([host()]),
        body: Body::None,
        close: false,
    }
}

/// A head at the limits: `fields` fields, folds among them, its length
/// `head` exactly, then `framing` and the blank line.
fn full_head(limits: &Limits, framing: &[u8]) -> Vec<u8> {
    let mut out = b"HTTP/1.1 200 OK\r\n".to_vec();
    let fields = usize::try_from(limits.headers).expect("fits a usize") - 1 - usize::from(!framing.is_empty());
    for n in 0..fields {
        out.extend_from_slice(format!("X-{n}: v\r\n").as_bytes());
        if n % 2 == 0 {
            out.extend_from_slice(b" folded\r\n");
        }
    }
    out.extend_from_slice(framing);
    let head = usize::try_from(limits.head).expect("fits a usize");
    // A last field that brings the head to its limit, the blank line
    // included.
    let room = head - out.len() - 2;
    assert!(room >= 6, "the limits leave room for the last field");
    out.extend_from_slice(b"X:");
    out.extend_from_slice(&vec![b'a'; room - 4]);
    out.extend_from_slice(b"\r\n\r\n");
    assert_eq!(out.len(), head);
    out
}

const LIMITS: Limits = Limits { request: 256, head: 512, headers: 10, read: 96, send: 64 };

#[test]
fn every_entry_point_holds_no_more_than_its_worst_case_at_its_limits() {
    let read_whole = Read::Fill(LIMITS.read);
    let body: Vec<u8> = (0..400).map(|n| b'a' + u8::try_from(n % 26).unwrap()).collect();
    let mut by_length = full_head(&LIMITS, format!("Content-Length: {}\r\n", body.len()).as_bytes());
    by_length.extend_from_slice(&body);
    let mut chunked = full_head(&LIMITS, b"Transfer-Encoding: chunked\r\n");
    for piece in body.chunks(37) {
        chunked.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
        chunked.extend_from_slice(piece);
        chunked.extend_from_slice(b"\r\n");
    }
    chunked.extend_from_slice(b"0\r\n");
    chunked.extend_from_slice(format!("Trailer: {}\r\n\r\n", "t".repeat(400)).as_bytes());
    let mut until_end = full_head(&LIMITS, b"");
    until_end.extend_from_slice(&body);
    let interim =
        [b"HTTP/1.1 100 Continue\r\n\r\n".to_vec(), b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_vec()]
            .concat();
    let post = Call { method: Method::Post, body: Body::Length(300), ..get() };
    let long_call = Call { target: vec![b'/'; 200].into(), ..get() };
    let cases: [(&Call, usize, &[u8], bool, &str); 7] = [
        (&get(), 0, &by_length, true, "a head at its limits, a body by length"),
        (&get(), 0, &chunked, true, "a chunked body, its trailer section at the limit"),
        (&get(), 0, &until_end, true, "a body to the end of the stream"),
        (&get(), 0, &interim, true, "an interim head"),
        (&post, 300, &by_length, true, "an upload"),
        (&long_call, 0, &by_length, true, "a request head near its limit"),
        (&get(), 0, b"HTTP/1.1 200 OK\r\nX: a\r\n", false, "a response cut short"),
    ];
    for (call, upload, response, done, what) in cases {
        for read in [read_whole, Read::Fill(1), Read::Scan { until: Delimiter::LF, max: LIMITS.read }] {
            let ran = exchange(LIMITS, call, upload, response, read, Interrupt::Nothing);
            assert_eq!(ran.done, Some(done), "{what}, {read:?}");
        }
        for at in 0..60 {
            let _ = exchange(LIMITS, call, upload, response, read_whole, Interrupt::Close(at));
            let _ = exchange(LIMITS, call, upload, response, read_whole, Interrupt::Fail(at));
            let _ = exchange(LIMITS, call, upload, response, read_whole, Interrupt::Discard(at));
        }
    }
}

#[test]
fn the_carry_over_counts_with_the_head_and_a_delivery() {
    // The intake full but for a byte, and a delivery that fills it: the
    // carry-over and the delivery are held at once.
    let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    response.extend_from_slice(format!("{:x}\r\n{}\r\n", 95, "b".repeat(95)).as_bytes());
    response.extend_from_slice(b"1\r\nc\r\n0\r\n\r\n");
    let ran = exchange(LIMITS, &get(), 0, &response, Read::Fill(LIMITS.read), Interrupt::Nothing);
    assert_eq!(ran.done, Some(true));
    assert!(ran.most > u64::from(LIMITS.read) + 95, "the intake and the delivery at once: {}", ran.most);
}

/// The steps of a stream read to its outcome, each checked: the most the
/// reader held of its own in a step.
fn events(limits: sse::Limits, stream: &[u8], close: Option<u32>) -> u64 {
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(sse::UP_MAX_OUT.above.max(sse::DOWN_MAX_OUT.above));
    let mut below = Queue::with_capacity(sse::UP_MAX_OUT.below.max(sse::DOWN_MAX_OUT.below));
    let cap = u32::try_from(stream.len()).expect("fits a u32").max(sse::largest_demand(&limits));
    let mut intake = Intake::with_capacity(cap);
    intake.append(stream).expect("the stream holds it");
    let what = format!("{limits:?} {close:?} reading {}", stream.escape_ascii());
    let bound = sse::worst_case(&limits).expect("the limits are honoured");

    let meter = Meter::new();
    meter.start();
    let mut reader = Reader::new(&limits);
    let mut most = meter.check(meter.end(), bound, &what);
    let mut demanded: Option<Read> = None;
    let mut over = false;
    for steps in 0..u32::try_from(stream.len().checked_mul(8).expect("a test stream budget"))
        .expect("test budget fits u32")
        .saturating_add(64)
    {
        let closing = close == Some(steps) || over;
        // The delivery, made between steps to the reader's demand.
        let delivered = match (closing, demanded.take()) {
            (false, Some(read)) => Some(match intake.meet(read) {
                Some(bytes) => Up::Bytes(bytes),
                None => Up::End,
            }),
            (true, _) | (false, None) => None,
        };
        meter.start();
        match (closing, delivered) {
            (true, _) => sse::down(&mut reader, &env, sse::Request::Close, &mut above, &mut below),
            (false, Some(ev)) => sse::up(&mut reader, &env, ev, &mut above, &mut below),
            (false, None) => {
                let request = match reader.waiting() {
                    sse::Waiting::Above => sse::Request::Data(Down::Demand { read: Read::Fill(limits.chunk), room: 0 }),
                    sse::Waiting::Next => sse::Request::Next,
                    other @ (sse::Waiting::Bytes | sse::Waiting::Close | sse::Waiting::Nothing) => {
                        panic!("unexpected wait {other:?}")
                    }
                };
                sse::down(&mut reader, &env, request, &mut above, &mut below);
            }
        }
        let step = meter.end();
        let mut closed = false;
        while let Some(event) = above.pop() {
            match event {
                sse::Event::Opened => {}
                sse::Event::Data(data) => drop(data),
                sse::Event::Dispatched(dispatch) => drop(dispatch),
                sse::Event::Ended | sse::Event::Failed(_) => over = true,
                sse::Event::Closed => closed = true,
            }
        }
        while let Some(request) = below.pop() {
            match request {
                Down::Demand { read: Read::Nothing, .. } => {}
                Down::Demand { read, .. } => demanded = Some(read),
                Down::Send(_) | Down::Finish => panic!("the reader sends nothing"),
            }
        }
        most = most.max(meter.check(step, bound, &what));
        if closed {
            return most;
        }
    }
    panic!("{what}: a stream is read in a few steps a line");
}

#[test]
fn the_reader_holds_no_more_than_its_worst_case_at_its_limits() {
    let limits = sse::Limits { line: 64, event: 200, field: 16, chunk: 24 };
    let mut at_limits = Vec::new();
    // An event at its limit, its data one line at the line limit and more,
    // its type and id at theirs, lines ended every way, a whole chunk
    // delivered among them.
    at_limits.extend_from_slice(b"\xef\xbb\xbfevent: ");
    at_limits.extend_from_slice(&[b't'; 16]);
    at_limits.extend_from_slice(b"\r\nid: ");
    at_limits.extend_from_slice(&[b'i'; 16]);
    at_limits.extend_from_slice(b"\rdata: ");
    at_limits.extend_from_slice(&[b'd'; 58]);
    at_limits.extend_from_slice(b"\ndata: ");
    at_limits.extend_from_slice(&[b'e'; 40]);
    at_limits.extend_from_slice(b"\n\ndata:1\r\rdata:2\r\r: ping\n\nretry: 1000\ndata\n\n");
    let mut too_long = b"data: ".to_vec();
    too_long.extend_from_slice(&[b'x'; 70]);
    let mut never_ends = Vec::new();
    for _ in 0..20 {
        never_ends.extend_from_slice(b"data: 0123456789\n");
    }
    let cases: [(&[u8], &str); 4] = [
        (&at_limits, "an event at its limits"),
        (&too_long, "a line past its limit"),
        (&never_ends, "an event that never ends"),
        (b"event: 01234567890123456789\ndata\n\n", "a type past its limit"),
    ];
    let bound = sse::worst_case(&limits).expect("the limits are honoured");
    assert_eq!(events(limits, &at_limits, None), bound, "at its limits, its peak is its worst case exactly");
    for (stream, what) in cases {
        let most = events(limits, stream, None);
        assert!(most > 0, "{what}");
        for at in 0..40 {
            let _ = events(limits, stream, Some(at));
        }
    }
}

#[test]
fn a_large_event_holds_only_its_intake_piece_and_delivery() {
    let limits = sse::Limits { line: 256, event: 256, field: 8, chunk: 64 };
    let mut stream = b"data: ".to_vec();
    stream.extend_from_slice(&[b'x'; 240]);
    stream.extend_from_slice(b"\n\n");
    let most = events(limits, &stream, None);
    assert!(most <= sse::worst_case(&limits).expect("valid limits"), "bounded pieces: {most}");
    assert!(most < 240, "no full event buffer: {most}");
}

#[test]
fn an_event_of_a_megabyte_costs_the_same_reader_memory_as_a_small_one() {
    let limits = sse::Limits { line: 2 << 20, event: 2 << 20, field: 8, chunk: 4 };
    let small = format!("event: 12345678\nid: 12345678\ndata: {}\n\n", "a".repeat(64));
    let large = format!("event: 12345678\nid: 12345678\ndata: {}\n\n", "a".repeat(1 << 20));
    let small = events(limits, small.as_bytes(), None);
    let large = events(limits, large.as_bytes(), None);
    assert_eq!(small, large, "the reader holds no event buffer");
    assert_eq!(large, sse::worst_case(&limits).expect("valid limits"));
}
