//! The server-sent events reader (http.md, 4): fields, line endings and
//! the line scans that read them, dispatch, every limit at and past its
//! edge, the stream's end and failure, and closing in each state.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]

use alloc::vec::Vec;

use skein_lib::Intake;
use skein_lib::stream::{Down, Fault, Read, Up};

use super::{Events, Gathered as Event, Message, boxed, stream};
use crate::sse::{Error, Limits, Request, Waiting};

const LIMITS: Limits = Limits { line: 64, event: 128, field: 16, chunk: 8 };

fn message(name: &[u8], data: &[u8], id: &[u8]) -> Event {
    Event::Message(Message { name: boxed(name), data: boxed(data), id: boxed(id) })
}

/// The messages a stream comes to, which must end cleanly.
fn messages(bytes: &[u8]) -> Vec<Event> {
    let mut events = stream(bytes, LIMITS);
    assert_eq!(events.pop(), Some(Event::Ended), "{}", bytes.escape_ascii());
    events
}

fn failure(bytes: &[u8], limits: Limits) -> Error {
    match stream(bytes, limits).pop() {
        Some(Event::Failed(error)) => error,
        other => panic!("{} ends with {other:?}", bytes.escape_ascii()),
    }
}

/// The one demand the reader makes: a line scan of `max`.
#[expect(clippy::unnecessary_wraps, reason = "compared with what a call sent below, an Option")]
fn line(max: u32) -> Option<Down> {
    Some(Down::Demand { read: Read::Line { max }, room: 0 })
}

/// The messages a stream comes to, which must end cleanly, and the
/// deliveries it took.
fn read_counting(bytes: &[u8], limits: Limits) -> (Vec<Event>, usize) {
    let mut events = Events::new(limits);
    let mut intake = Intake::with_capacity(u32::try_from(bytes.len()).unwrap().max(limits.chunk));
    intake.append(bytes).unwrap();
    let mut out = Vec::new();
    let mut deliveries = 0;
    let (mut event, mut down) = events.down(Request::Next);
    for _ in 0..bytes.len().saturating_mul(2).saturating_add(8) {
        match event {
            None => {}
            Some(Event::Message(_)) => {
                out.push(event.take().unwrap());
                (event, down) = events.down(Request::Next);
                continue;
            }
            Some(Event::Ended) => return (out, deliveries),
            other => panic!("{} came to {other:?}", bytes.escape_ascii()),
        }
        let Some(Down::Demand { read, .. }) = down else { panic!("a demand while reading") };
        let answer = match intake.meet(read) {
            Some(piece) => {
                deliveries = deliveries.saturating_add(1);
                Up::Bytes(piece)
            }
            None => Up::End,
        };
        (event, down) = events.up(answer);
    }
    panic!("{} read in a few steps a line", bytes.escape_ascii());
}

#[test]
fn an_event_is_dispatched_at_its_blank_line_with_its_fields() {
    assert_eq!(
        messages(b"event: delta\ndata: {\"a\":1}\nid: 7\n\ndata: second\n\n"),
        [message(b"delta", b"{\"a\":1}", b"7"), message(b"message", b"second", b"7")],
        "the id lasts; the type does not"
    );
}

#[test]
fn data_lines_are_joined_by_lf_and_the_last_lf_dropped() {
    assert_eq!(messages(b"data: a\ndata:b\ndata\ndata:  c\n\n"), [message(b"message", b"a\nb\n\n c", b"")]);
    assert_eq!(messages(b"data:\n\n"), [message(b"message", b"", b"")], "one empty data line is an event");
}

#[test]
fn a_block_without_data_dispatches_nothing() {
    assert_eq!(messages(b"event: ping\n\n: comment\n\nid: 3\n\ndata: x\n\n"), [message(b"message", b"x", b"3")]);
    assert_eq!(messages(b"\n\n\n"), []);
}

#[test]
fn comments_and_unknown_fields_are_ignored() {
    assert_eq!(
        messages(b": this is a comment\nfoo: bar\ndatas: no\nDATA: no\n data: no\ndata: yes\n\n"),
        [message(b"message", b"yes", b"")]
    );
}

#[test]
fn an_id_with_a_nul_is_ignored_and_an_empty_one_resets() {
    assert_eq!(
        messages(b"id: 1\ndata: a\n\nid: 2\0x\ndata: b\n\nid\ndata: c\n\nid:\ndata: d\n\n"),
        [
            message(b"message", b"a", b"1"),
            message(b"message", b"b", b"1"),
            message(b"message", b"c", b""),
            message(b"message", b"d", b"")
        ]
    );
}

#[test]
fn retry_sets_the_reconnection_time_from_digits_only() {
    for (bytes, retry) in [
        (&b"retry: 3000\n\n"[..], Some(3000)),
        (b"retry: 3000\nretry: x\n\n", Some(3000)),
        (b"retry: 30x\n\n", None),
        (b"retry:\n\n", None),
        (b"retry\n\n", None),
        (b"retry: 18446744073709551616\n\n", None),
        (b"retry: 18446744073709551615\n\n", Some(u64::MAX)),
        (b"retry: 1\nretry: 2\n\n", Some(2)),
    ] {
        let mut events = Events::new(LIMITS);
        let mut intake = Intake::with_capacity(64);
        intake.append(bytes).unwrap();
        let (_, mut down) = events.down(Request::Next);
        for _ in 0..64_u32 {
            let Some(Down::Demand { read, .. }) = down else { break };
            let answer = match intake.meet(read) {
                Some(piece) => Up::Bytes(piece),
                None => Up::End,
            };
            down = events.up(answer).1;
        }
        assert_eq!(events.reader.retry(), retry, "{}", bytes.escape_ascii());
    }
}

#[test]
fn lines_end_at_lf_crlf_or_cr_alone() {
    let expected = [message(b"one", b"1\n2", b""), message(b"message", b"3", b"")];
    assert_eq!(messages(b"event: one\ndata: 1\ndata: 2\n\ndata: 3\n\n"), expected);
    assert_eq!(messages(b"event: one\r\ndata: 1\r\ndata: 2\r\n\r\ndata: 3\r\n\r\n"), expected);
    assert_eq!(messages(b"event: one\rdata: 1\rdata: 2\r\rdata: 3\r\r"), expected);
    assert_eq!(messages(b"event: one\r\ndata: 1\rdata: 2\n\r\ndata: 3\n\n"), expected, "mixed");
}

#[test]
fn each_demand_is_a_line_scan_and_a_cr_s_lf_is_skipped() {
    let mut events = Events::new(LIMITS);
    assert_eq!(events.down(Request::Next), (None, line(8)));
    assert_eq!(events.bytes(b"data: a\n"), (None, line(8)));
    assert_eq!(events.bytes(b"data: b\r"), (None, line(8)), "a CR ends its line at once");
    assert_eq!(events.bytes(b"\n"), (None, line(8)), "the CR's pair: skipped");
    assert_eq!(events.bytes(b"data: c\r"), (None, line(8)));
    assert_eq!(events.bytes(b"\r"), (Some(message(b"message", b"a\nb\nc", b"")), None), "a blank line, at once");
    assert_eq!(events.down(Request::Next), (None, line(8)));
    assert_eq!(events.bytes(b"\n"), (None, line(8)), "the blank line's pair: skipped, no second blank line");
    assert_eq!(events.bytes(b"data: d\r"), (None, line(8)));
    assert_eq!(events.bytes(b"\n"), (None, line(8)));
    assert_eq!(events.bytes(b"\n"), (Some(message(b"message", b"d", b"")), None), "an LF alone: the blank line");
    assert_eq!(events.down(Request::Next), (None, line(8)));
    assert_eq!(events.bytes(b"data: lo"), (None, line(8)), "a line longer than a chunk comes in several");
    assert_eq!(events.bytes(b"ng\r"), (None, line(8)));
    assert_eq!(events.bytes(b"\r"), (Some(message(b"message", b"long", b"")), None));
}

#[test]
fn streams_shorter_than_one_scan_are_read_whatever_their_endings() {
    let limits = Limits { chunk: 64, ..LIMITS };
    let expected = [message(b"one", b"1\n2", b""), message(b"message", b"3", b"")];
    for (bytes, deliveries, what) in [
        (&b"event: one\ndata: 1\ndata: 2\n\ndata: 3\n\n"[..], 6, "LF: one delivery a line"),
        (b"event: one\rdata: 1\rdata: 2\r\rdata: 3\r\r", 6, "CR: one delivery a line"),
        (b"event: one\r\ndata: 1\r\ndata: 2\r\n\r\ndata: 3\r\n\r\n", 12, "CRLF: two"),
        (b"event: one\r\ndata: 1\rdata: 2\n\r\ndata: 3\n\r", 8, "mixed"),
    ] {
        assert!(bytes.len() < 64, "{what}");
        let (read, taken) = read_counting(bytes, limits);
        assert_eq!((&read[..], taken), (&expected[..], deliveries), "{what}");
    }
    let (read, taken) = read_counting(b"data: x\r", limits);
    assert_eq!((&read[..], taken), (&[][..], 1), "a CR at the very end ends its line");
    let (read, taken) = read_counting(b"data: x\r\r", limits);
    assert_eq!((&read[..], taken), (&[message(b"message", b"x", b"")][..], 2));
}

#[test]
fn an_event_ends_its_delivery_so_nothing_is_held_between_events() {
    let mut events = Events::new(Limits { chunk: 16, ..LIMITS });
    assert_eq!(events.down(Request::Next), (None, line(16)));
    assert_eq!(events.bytes(b"data:1\r"), (None, line(16)));
    assert_eq!(events.bytes(b"\r"), (Some(message(b"message", b"1", b"")), None));
    assert_eq!(events.reader.waiting(), Waiting::Next);
    assert_eq!(events.down(Request::Next), (None, line(16)), "the next event begins with the next byte demanded");
}

#[test]
fn an_end_or_a_failure_while_idle_answers_the_next_next() {
    let idle = || {
        let mut events = Events::new(Limits { chunk: 16, ..LIMITS });
        events.down(Request::Next);
        events.bytes(b"data:1\n");
        assert_eq!(events.bytes(b"\n"), (Some(message(b"message", b"1", b"")), None));
        events
    };
    let mut events = idle();
    assert_eq!(events.up(Up::End), (None, None));
    assert_eq!(events.down(Request::Next), (Some(Event::Ended), None));

    let mut events = idle();
    assert_eq!(events.up(Up::Failed(Fault::Reset)), (None, None));
    assert_eq!(events.down(Request::Next), (Some(Event::Failed(Error::Stream(Fault::Reset))), None));

    let mut events = idle();
    assert_eq!(events.up(Up::End), (None, None));
    assert_eq!(events.up(Up::Failed(Fault::Reset)), (None, None));
    assert_eq!(events.down(Request::Next), (Some(Event::Ended), None), "after the end, what was read stands");
}

#[test]
fn the_end_of_the_stream_drops_an_event_cut_short() {
    assert_eq!(stream(b"data: a\n\ndata: b\n", LIMITS), [message(b"message", b"a", b""), Event::Ended]);
    assert_eq!(stream(b"data: a\n\ndata: b", LIMITS), [message(b"message", b"a", b""), Event::Ended]);
    assert_eq!(stream(b"", LIMITS), [Event::Ended]);
}

#[test]
fn the_stream_failing_fails_the_reader_in_any_state() {
    let mut events = Events::new(LIMITS);
    events.down(Request::Next);
    assert_eq!(events.up(Up::Failed(Fault::Invalid)), (Some(Event::Failed(Error::Stream(Fault::Invalid))), None));
    assert_eq!(events.reader.waiting(), Waiting::Close);
    assert_eq!(events.up(Up::End), (None, None), "nothing follows the outcome");

    let mut events = Events::new(LIMITS);
    assert_eq!(events.up(Up::Failed(Fault::Other)), (None, None), "held while idle");
    assert_eq!(events.down(Request::Next), (Some(Event::Failed(Error::Stream(Fault::Other))), None));
}

#[test]
fn one_byte_order_mark_is_skipped_and_only_at_the_start() {
    assert_eq!(messages(b"\xef\xbb\xbfdata: a\n\n"), [message(b"message", b"a", b"")]);
    assert_eq!(messages(b"\xef\xbb\xbf\xef\xbb\xbfdata: a\n\n"), [], "a second one begins an unknown field");
    assert_eq!(messages(b"\xef\xbbdata: a\n\n"), [], "half of one is the line's");
    assert_eq!(messages(b"data: \xef\xbb\xbf\n\n"), [message(b"message", b"\xef\xbb\xbf", b"")]);
}

#[test]
fn a_line_at_its_limit_is_read_and_past_it_fails() {
    let limits = Limits { line: 10, ..LIMITS };
    assert_eq!(stream(b"data: 1234\n\n", limits)[0], message(b"message", b"1234", b""));
    assert_eq!(failure(b"data: 12345\n\n", limits), Error::LineTooLong);
    assert_eq!(failure(b": a comment that is long\n\n", limits), Error::LineTooLong, "a comment too");
    assert_eq!(stream(b"data: 1234\r\n\r\n", limits)[0], message(b"message", b"1234", b""), "its ending not counted");
}

#[test]
fn an_event_at_its_limit_is_read_and_past_it_fails() {
    let at = b"data: 12\nid: 9\n\n";
    let limits = Limits { event: u32::try_from(at.len()).unwrap(), ..LIMITS };
    assert_eq!(stream(at, limits)[0], message(b"message", b"12", b"9"), "its blank line included");
    let limits = Limits { event: u32::try_from(at.len()).unwrap() - 1, ..LIMITS };
    assert_eq!(failure(at, limits), Error::EventTooLong);
    let mut endless = Vec::new();
    for _ in 0..100_u32 {
        endless.extend_from_slice(b": keep-alive\n");
    }
    assert_eq!(failure(&endless, LIMITS), Error::EventTooLong, "an event that never ends, whatever it sends");
    let mut kept_alive = Vec::new();
    for _ in 0..100_u32 {
        kept_alive.extend_from_slice(b": keep-alive\n\n");
    }
    kept_alive.extend_from_slice(b"data: x\n\n");
    assert_eq!(messages(&kept_alive), [message(b"message", b"x", b"")], "each blank line starts over");
}

#[test]
fn a_type_or_an_id_at_its_limit_is_read_and_past_it_fails() {
    let limits = Limits { field: 4, ..LIMITS };
    assert_eq!(stream(b"event: abcd\nid: wxyz\ndata\n\n", limits)[0], message(b"abcd", b"", b"wxyz"));
    assert_eq!(failure(b"event: abcde\ndata\n\n", limits), Error::FieldTooLong);
    assert_eq!(failure(b"id: vwxyz\ndata\n\n", limits), Error::FieldTooLong);
}

#[test]
fn every_cut_of_the_stream_reads_the_same() {
    let bytes = b"\xef\xbb\xbfevent: a\r\ndata: 1\rdata: 2\n\n: c\r\nid: x\nretry: 5\ndata: 3\r\r\n";
    let whole = messages(bytes);
    assert_eq!(whole.len(), 2);
    for chunk in 1..=20 {
        let limits = Limits { chunk, ..LIMITS };
        let mut events = stream(bytes, limits);
        assert_eq!(events.pop(), Some(Event::Ended), "chunk {chunk}");
        assert_eq!(events, whole, "chunk {chunk}");
    }
}

#[test]
fn the_last_event_id_is_the_buffer_at_the_last_blank_line() {
    let mut events = Events::new(Limits { chunk: 64, ..LIMITS });
    events.down(Request::Next);
    for delivery in [&b"id: 1\n"[..], b"\n", b"id: 2\n"] {
        assert_eq!(events.bytes(delivery), (None, line(64)));
    }
    assert_eq!(events.reader.last_event_id(), b"1", "the block that set 2 has not ended");
    assert_eq!(events.bytes(b"\n"), (None, line(64)));
    assert_eq!(events.reader.last_event_id(), b"2");
}

#[test]
fn a_close_in_each_state_withdraws_what_was_demanded_and_answers_closed() {
    let mut events = Events::new(LIMITS);
    assert_eq!(events.down(Request::Close), (Some(Event::Closed), None), "idle: nothing to withdraw");
    assert_eq!(events.reader.waiting(), Waiting::Nothing);

    let mut events = Events::new(LIMITS);
    events.down(Request::Next);
    let withdrawal = Some(Down::Demand { read: Read::Nothing, room: 0 });
    assert_eq!(events.down(Request::Close), (Some(Event::Closed), withdrawal));
    assert_eq!(events.bytes(b"data: late\n\n"), (None, None), "a delivery on its way, dropped");
    assert_eq!(events.up(Up::End), (None, None));

    let mut events = Events::new(LIMITS);
    events.down(Request::Next);
    events.up(Up::End);
    assert_eq!(events.down(Request::Close), (Some(Event::Closed), None), "after the outcome");
}

#[test]
fn what_the_reader_waits_for_follows_its_state() {
    let mut events = Events::new(LIMITS);
    assert_eq!(events.reader.waiting(), Waiting::Next);
    events.down(Request::Next);
    assert_eq!(events.reader.waiting(), Waiting::Bytes);
    events.bytes(b": ping\n");
    assert_eq!(events.reader.waiting(), Waiting::Bytes, "a comment is progress, not an event");
    events.up(Up::End);
    assert_eq!(events.reader.waiting(), Waiting::Close);
    events.down(Request::Close);
    assert_eq!(events.reader.waiting(), Waiting::Nothing);
}

#[test]
#[should_panic(expected = "Next is asked between events")]
fn one_next_at_a_time() {
    let mut events = Events::new(LIMITS);
    events.down(Request::Next);
    events.down(Request::Next);
}

#[test]
#[should_panic(expected = "Next is asked between events")]
fn no_next_after_the_outcome() {
    let mut events = Events::new(LIMITS);
    events.down(Request::Next);
    events.up(Up::End);
    events.down(Request::Next);
}

#[test]
#[should_panic(expected = "a delivery meets the body's one demand")]
fn no_bytes_without_a_demand() {
    let mut events = Events::new(LIMITS);
    events.bytes(b"data\n\n");
}

/// The native face, keeping every output so stream order is asserted.
struct Face {
    reader: crate::sse::Reader,
    env: skein_lib::Env<Limits>,
    above: skein_lib::Queue<crate::sse::Event>,
    below: skein_lib::Queue<Down>,
}

impl Face {
    fn new() -> Face {
        Face {
            reader: crate::sse::Reader::new(&LIMITS),
            env: super::env(LIMITS),
            above: skein_lib::Queue::with_capacity(crate::sse::UP_MAX_OUT.above),
            below: skein_lib::Queue::with_capacity(1),
        }
    }

    fn down(&mut self, request: Request) -> (Vec<crate::sse::Event>, Option<Down>) {
        crate::sse::down(&mut self.reader, &self.env, request, &mut self.above, &mut self.below);
        assert!(self.above.len() <= crate::sse::DOWN_MAX_OUT.above);
        self.take()
    }

    fn up(&mut self, event: Up) -> (Vec<crate::sse::Event>, Option<Down>) {
        crate::sse::up(&mut self.reader, &self.env, event, &mut self.above, &mut self.below);
        assert!(self.above.len() <= crate::sse::UP_MAX_OUT.above);
        self.take()
    }

    fn take(&mut self) -> (Vec<crate::sse::Event>, Option<Down>) {
        let mut events = Vec::new();
        for _ in 0..crate::sse::UP_MAX_OUT.above {
            match self.above.pop() {
                Some(event) => events.push(event),
                None => break,
            }
        }
        assert!(self.below.len() <= 1);
        (events, self.below.pop())
    }
}

fn face_request(read: Read) -> Request {
    Request::Data(Down::Demand { read, room: 0 })
}

/// Each byte delivery is held until its owner asks; a tail shorter than a
/// fill is discarded by stream End, as in lib.md, 7.
fn native_read(bytes: &[u8], read: Read) -> (Vec<u8>, Vec<crate::sse::Event>) {
    let mut face = Face::new();
    let mut intake = Intake::with_capacity(u32::try_from(bytes.len()).unwrap().max(LIMITS.chunk));
    intake.append(bytes).unwrap();
    let (mut events, mut below) = face.down(Request::Next);
    let mut data = Vec::new();
    let mut seen = Vec::new();
    for _ in 0..bytes.len().saturating_mul(4).saturating_add(32) {
        let mut next = None;
        for event in events {
            match &event {
                crate::sse::Event::Opened | crate::sse::Event::Data(Up::Bytes(_)) => next = Some(face_request(read)),
                crate::sse::Event::Data(Up::End) => {}
                crate::sse::Event::Data(Up::Failed(_)) | crate::sse::Event::Dispatched(_) => next = Some(Request::Next),
                crate::sse::Event::Ended | crate::sse::Event::Failed(_) => {
                    seen.push(event);
                    return (data, seen);
                }
                crate::sse::Event::Data(Up::Room) | crate::sse::Event::Closed => panic!("read-only live face"),
            }
            if let crate::sse::Event::Data(Up::Bytes(bytes)) = &event {
                data.extend_from_slice(bytes);
            }
            seen.push(event);
        }
        if let Some(request) = next {
            assert!(below.is_none());
            (events, below) = face.down(request);
        } else {
            let Some(Down::Demand { read, room: 0 }) = below else { panic!("the body is demanded") };
            let delivery = match intake.meet(read) {
                Some(piece) => Up::Bytes(piece),
                None => Up::End,
            };
            (events, below) = face.up(delivery);
        }
    }
    panic!("bounded steps per byte");
}

#[test]
fn data_streams_under_fills_scans_and_line_reads_with_final_metadata() {
    use crate::sse::Event as Native;
    use skein_lib::stream::Delimiter;
    let input = b": comment\r\ndata: abcdef\r\ndata: ghijkl\r\nevent: late\r\nid: 9\r\n\r\n";
    let expected = b"abcdef\nghijkl";
    for count in 1..=LIMITS.chunk {
        let (data, events) = native_read(input, Read::Fill(count));
        let length =
            expected.len().checked_sub(expected.len().checked_rem(usize::try_from(count).unwrap()).unwrap()).unwrap();
        assert_eq!(data, &expected[..length]);
        let dispatch = dispatch_index(&events).expect("an event dispatch");
        assert_eq!(events[dispatch.checked_sub(1).unwrap()], Native::Data(Up::End));
        assert_eq!(
            events[dispatch],
            Native::Dispatched(crate::sse::Dispatch { name: boxed(b"late"), id: boxed(b"9") })
        );
        for read in [Read::Scan { until: Delimiter::LF, max: count }, Read::Line { max: count }] {
            let (data, _) = native_read(input, read);
            let first = count.min(7);
            let tail = 6_u32.checked_rem(count).unwrap();
            assert_eq!(
                data,
                &expected[..expected.len().checked_sub(usize::try_from(tail).unwrap()).unwrap()],
                "{read:?} first span {first}"
            );
        }
    }
}

#[test]
fn an_open_event_holds_its_piece_without_a_body_demand_and_can_withdraw() {
    use crate::sse::Event as Native;
    let mut face = Face::new();
    assert_eq!(face.down(Request::Next), (Vec::new(), line(8)));
    assert_eq!(face.up(Up::Bytes(boxed(b"data: ab"))), (Vec::from([Native::Opened]), None));
    assert_eq!(face.reader.waiting(), Waiting::Above);
    assert_eq!(face.down(face_request(Read::Fill(1))), (Vec::from([Native::Data(Up::Bytes(boxed(b"a")))]), None));
    assert_eq!(face.down(face_request(Read::Nothing)), (Vec::new(), line(8)));
    assert_eq!(face.up(Up::Bytes(boxed(b"cd\n"))), (Vec::new(), line(8)));
    let (events, below) = face.up(Up::Bytes(boxed(b"\n")));
    assert!(below.is_none());
    assert_eq!(
        events,
        [Native::Data(Up::End), Native::Dispatched(crate::sse::Dispatch { name: boxed(b"message"), id: boxed(b"") })]
    );
}

#[test]
fn an_open_event_cut_before_its_blank_line_fails_data_and_never_dispatches() {
    use crate::sse::Event as Native;
    for ending in [Up::End, Up::Failed(Fault::Invalid)] {
        let mut face = Face::new();
        drop(face.down(Request::Next));
        assert_eq!(face.up(Up::Bytes(boxed(b"data: x\n"))), (Vec::from([Native::Opened]), None));
        let (events, _) = face.down(face_request(Read::Fill(8)));
        assert!(events.is_empty());
        let (events, below) = face.up(ending);
        assert!(below.is_none());
        match events.as_slice() {
            [Native::Data(Up::Failed(_))] => {}
            other => panic!("the cut fails only the data: {other:?}"),
        }
        let (events, below) = face.down(Request::Next);
        assert!(below.is_none());
        match events.as_slice() {
            [Native::Ended | Native::Failed(_)] => {}
            other => panic!("Next receives the reader terminal: {other:?}"),
        }
    }
}

#[test]
fn closing_an_open_face_drops_its_held_piece_and_crossed_data() {
    use crate::sse::Event as Native;
    let mut face = Face::new();
    drop(face.down(Request::Next));
    drop(face.up(Up::Bytes(boxed(b"data: x\n"))));
    drop(face.down(face_request(Read::Fill(8))));
    assert_eq!(
        face.down(Request::Close),
        (Vec::from([Native::Closed]), Some(Down::Demand { read: Read::Nothing, room: 0 }))
    );
    assert_eq!(face.up(Up::Bytes(boxed(b"\n"))), (Vec::new(), None));
    assert_eq!(face.reader.waiting(), Waiting::Nothing);
}

#[test]
fn a_two_byte_scan_spans_joined_lines_and_comments() {
    use skein_lib::stream::Delimiter;
    let read = Read::Scan { until: Delimiter::CRLF, max: 8 };
    let (data, events) = native_read(b"data: ab\r: ignored\rdata: cd\r\r", read);
    assert!(data.is_empty(), "the unmatched five-byte tail ends a read of eight");
    assert!(dispatch_index(&events).is_some());
    let (data, _) = native_read(b"data: abc\r: ignored\rdata: defg\r\r", read);
    assert_eq!(data, b"abc\ndefg", "the synthesized LF belongs to the data, with CRs removed");
}

#[test]
fn a_limit_passed_in_open_data_fails_the_face_before_the_reader() {
    use crate::sse::Event as Native;
    for (line, event, expected) in [(7, 128, Error::LineTooLong), (64, 7, Error::EventTooLong)] {
        let mut face = Face::new();
        face.env.limits.line = line;
        face.env.limits.event = event;
        drop(face.down(Request::Next));
        assert_eq!(face.up(Up::Bytes(boxed(b"data: ab"))), (Vec::from([Native::Opened]), None));
        assert_eq!(
            face.down(face_request(Read::Fill(8))),
            (Vec::from([Native::Data(Up::Failed(Fault::Invalid))]), None)
        );
        assert_eq!(face.down(Request::Next), (Vec::from([Native::Failed(expected)]), None));
    }
}

#[test]
fn withdrawing_an_outstanding_read_discards_its_crossed_delivery() {
    use crate::sse::Event as Native;
    let mut face = Face::new();
    drop(face.down(Request::Next));
    drop(face.up(Up::Bytes(boxed(b"data: a\n"))));
    assert_eq!(face.down(face_request(Read::Fill(8))), (Vec::new(), line(8)));
    assert_eq!(face.down(face_request(Read::Nothing)), (Vec::new(), None));
    let (events, below) = face.up(Up::Bytes(boxed(b"\n")));
    assert!(below.is_none());
    match events.as_slice() {
        [Native::Data(Up::End), Native::Dispatched(_)] => {}
        other => panic!("withdrawal still dispatches: {other:?}"),
    }
}

fn dispatch_index(events: &[crate::sse::Event]) -> Option<usize> {
    use crate::sse::Event as Native;
    for (index, event) in events.iter().enumerate() {
        match event {
            Native::Dispatched(_) => return Some(index),
            Native::Opened | Native::Data(_) | Native::Ended | Native::Failed(_) | Native::Closed => {}
        }
    }
    None
}

#[test]
fn an_undemanded_body_end_checks_the_held_line_tail_before_its_outcome() {
    use crate::sse::Event as Native;
    let mut face = Face::new();
    face.env.limits.line = 5;
    drop(face.down(Request::Next));
    assert_eq!(face.up(Up::Bytes(boxed(b"data: "))), (Vec::from([Native::Opened]), None));
    assert_eq!(face.up(Up::End), (Vec::from([Native::Data(Up::Failed(Fault::Invalid))]), None));
    assert_eq!(face.down(Request::Next), (Vec::from([Native::Failed(Error::LineTooLong)]), None));
}
