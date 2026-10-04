//! The server-sent events reader (http.md, 4): fields, line endings,
//! dispatch, every limit at and past its edge, the rest of a delivery held,
//! the stream's end and failure, and closing in each state.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]

use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};

use super::{Events, boxed, stream};
use crate::sse::{Error, Event, Limits, Message, Request, Waiting};

const LIMITS: Limits = Limits { line: 64, event: 128, field: 16, chunk: 8 };
const CR: Delimiter = Delimiter::new(b"\r").expect("one byte");

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

#[expect(clippy::unnecessary_wraps, reason = "compared with what a call sent below, an Option")]
fn scan(until: Delimiter) -> Option<Down> {
    Some(Down::Demand { read: Read::Scan { until, max: LIMITS.chunk }, room: 0 })
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
        let mut intake = skein_lib::Intake::with_capacity(64);
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
fn the_scan_follows_the_last_line_s_ending() {
    let mut events = Events::new(LIMITS);
    assert_eq!(events.down(Request::Next), (None, scan(Delimiter::LF)));
    assert_eq!(events.bytes(b"data: a\n"), (None, scan(Delimiter::LF)));
    assert_eq!(
        events.bytes(b"data: b\r"),
        (None, scan(Delimiter::LF)),
        "a CR at the end: alone or a pair, not yet known"
    );
    assert_eq!(events.bytes(b"\n"), (None, scan(Delimiter::LF)), "a pair");
    assert_eq!(events.bytes(b"data: c\r"), (None, scan(Delimiter::LF)));
    assert_eq!(events.bytes(b"\r"), (Some(message(b"message", b"a\nb\nc", b"")), None), "alone: the blank line");
    assert_eq!(events.down(Request::Next), (None, scan(CR)), "a CR alone ended a line: scan to CR");
    assert_eq!(events.bytes(b"data: d\r"), (None, scan(CR)));
    assert_eq!(events.bytes(b"\ndata:e\n"), (None, scan(Delimiter::LF)), "an LF alone ended a line");
    assert_eq!(events.bytes(b"\n"), (Some(message(b"message", b"d\ne", b"")), None));
}

#[test]
fn a_delivery_with_more_than_one_event_is_held_and_read_before_more_is_demanded() {
    let mut events = Events::new(Limits { chunk: 16, ..LIMITS });
    let (_, down) = events.down(Request::Next);
    assert_eq!(down, Some(Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 16 }, room: 0 }));
    assert_eq!(events.bytes(b"data:1\r\rdata:2\r\r"), (Some(message(b"message", b"1", b"")), None));
    assert_eq!(events.reader.waiting(), Waiting::Next);
    assert_eq!(events.down(Request::Next), (Some(message(b"message", b"2", b"")), None), "from what was held");
    assert_eq!(events.up(Up::End), (None, None));
    assert_eq!(events.down(Request::Next), (Some(Event::Ended), None));
}

#[test]
fn an_end_after_a_held_rest_comes_once_the_rest_is_read() {
    let mut events = Events::new(Limits { chunk: 16, ..LIMITS });
    events.down(Request::Next);
    assert_eq!(events.bytes(b"data:1\r\rdata:2\r"), (Some(message(b"message", b"1", b"")), None));
    assert_eq!(events.up(Up::End), (None, None));
    assert_eq!(events.down(Request::Next), (Some(Event::Ended), None), "data:2 had no blank line: dropped");
}

#[test]
fn a_failure_before_the_end_overrides_a_held_rest_and_after_it_does_not() {
    let mut events = Events::new(Limits { chunk: 16, ..LIMITS });
    events.down(Request::Next);
    events.bytes(b"data:1\r\rdata:2\r\r");
    assert_eq!(events.up(Up::Failed(Fault::Reset)), (None, None));
    assert_eq!(events.down(Request::Next), (Some(Event::Failed(Error::Stream(Fault::Reset))), None));

    let mut events = Events::new(Limits { chunk: 16, ..LIMITS });
    events.down(Request::Next);
    events.bytes(b"data:1\r\rdata:2\r\r");
    assert_eq!(events.up(Up::End), (None, None));
    assert_eq!(events.up(Up::Failed(Fault::Reset)), (None, None));
    assert_eq!(events.down(Request::Next), (Some(message(b"message", b"2", b"")), None), "what was read stands");
    assert_eq!(events.down(Request::Next), (Some(Event::Ended), None));
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
    assert_eq!(events.bytes(b"id: 1\n\nid: 2\n"), (None, scan_of(64)));
    assert_eq!(events.reader.last_event_id(), b"1", "the block that set 2 has not ended");
    assert_eq!(events.bytes(b"\n"), (None, scan_of(64)));
    assert_eq!(events.reader.last_event_id(), b"2");
}

#[expect(clippy::unnecessary_wraps, reason = "compared with what a call sent below, an Option")]
fn scan_of(max: u32) -> Option<Down> {
    Some(Down::Demand { read: Read::Scan { until: Delimiter::LF, max }, room: 0 })
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
#[should_panic(expected = "a Next before the last one was answered")]
fn one_next_at_a_time() {
    let mut events = Events::new(LIMITS);
    events.down(Request::Next);
    events.down(Request::Next);
}

#[test]
#[should_panic(expected = "a Next after the stream's outcome")]
fn no_next_after_the_outcome() {
    let mut events = Events::new(LIMITS);
    events.down(Request::Next);
    events.up(Up::End);
    events.down(Request::Next);
}

#[test]
#[should_panic(expected = "bytes delivered without a read demand")]
fn no_bytes_without_a_demand() {
    let mut events = Events::new(LIMITS);
    events.bytes(b"data\n\n");
}
