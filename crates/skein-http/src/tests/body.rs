//! The response body (http.md, 3.3): each framing, demands met across
//! chunks, the reads below shaped by the demand above, discarding, and
//! every framing error.

#![expect(clippy::disallowed_types, reason = "a test builds what it sends in a Vec")]

use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};

use super::{Drive, Exchanged, LIMITS, Machine, exchange, get};
use crate::client::{Error, Event, Limits, Request, Reuse, Waiting};

const LINE: Read = Read::Scan { until: Delimiter::LF, max: 16 };

fn drive(read: Read) -> Drive<'static> {
    Drive { upload: None, read, respond_first: false }
}

fn respond(response: &[u8], read: Read) -> Exchanged {
    exchange(&mut Machine::new(LIMITS), get(), response, drive(read))
}

fn failure(response: &[u8]) -> Error {
    match respond(response, LINE).outcome {
        Some(Event::Failed(error)) => error,
        other => panic!("{} ends with {other:?}", response.escape_ascii()),
    }
}

/// `head` and the body after it.
fn response(head: &[u8], body: &[u8]) -> Vec<u8> {
    let mut response = Vec::from(head);
    response.extend_from_slice(body);
    response
}

const CHUNKED: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";

#[test]
fn a_body_by_length_is_read_to_its_length_and_ended() {
    // What each read sees of `hello\nworld`: a read larger than what is
    // left never meets the rest (lib.md, 7).
    let reads: [(Read, &[u8]); 5] = [
        (Read::Fill(1), b"hello\nworld"),
        (Read::Fill(3), b"hello\nwor"),
        (Read::Fill(16), b""),
        (LINE, b"hello\n"),
        (Read::Scan { until: Delimiter::CRLF, max: 5 }, b"hello\nworl"),
    ];
    for (read, seen) in reads {
        let exchanged = respond(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nhello\nworld", read);
        assert!(exchanged.ended, "{read:?}");
        assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)), "{read:?}");
        assert_eq!(exchanged.body, seen, "{read:?}");
    }
}

#[test]
fn a_chunked_body_is_read_across_its_chunks_whatever_the_demands() {
    let body = b"5\r\nhello\r\n1;ext=\"v\"\r\n\n\r\n7 \r\nworld!\n\r\n0\r\nTrailer: t\r\n\r\n";
    for read in [Read::Fill(1), Read::Fill(13), LINE, Read::Scan { until: Delimiter::LF, max: 4 }] {
        let exchanged = respond(&response(CHUNKED, body), read);
        assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)), "{read:?}");
        assert!(exchanged.ended);
        assert_eq!(exchanged.body, b"hello\nworld!\n", "{read:?}");
    }
}

#[test]
fn chunk_lines_may_end_with_lf_alone_and_sizes_may_be_long_and_mixed_case() {
    let body = b"0000000000000000000A\nabcdefghij\nFf\n";
    let mut body = Vec::from(&body[..]);
    body.extend_from_slice(&[b'x'; 255]);
    body.extend_from_slice(b"\n0\n\n");
    let exchanged = respond(&response(CHUNKED, &body), Read::Fill(5));
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)));
    assert_eq!(exchanged.body.len(), 265);
}

#[test]
fn a_scan_whose_delimiter_is_split_across_two_chunks_finds_it() {
    let body = b"3\r\nab\r\r\n3\r\n\ncd\r\n0\r\n\r\n";
    let exchanged = respond(&response(CHUNKED, body), Read::Scan { until: Delimiter::CRLF, max: 8 });
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)));
    assert_eq!(exchanged.body, b"ab\r\n", "then cd, which no scan meets before the end");
}

#[test]
fn a_body_to_the_end_of_the_stream_ends_with_it_on_a_connection_not_used_again() {
    let exchanged = respond(b"HTTP/1.1 200 OK\r\n\r\nline one\nline two\n", LINE);
    assert!(exchanged.ended);
    assert_eq!(exchanged.body, b"line one\nline two\n");
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Close)));
}

#[test]
fn the_reads_below_take_the_shape_of_the_demand_above_bounded_by_the_framing() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 10\r\n");
    machine.bytes(b"\r\n");
    let (events, requests) = machine.down(Request::Body(Down::Demand { read: LINE, room: 0 }));
    assert!(events.is_empty());
    assert_eq!(
        requests,
        [Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 10 }, room: 0 }],
        "a scan passes as a scan"
    );
    assert_eq!(machine.client.waiting(), Waiting::Body);
    let (events, requests) = machine.bytes(b"abc\n");
    assert_eq!(events, [Event::Body(Up::Bytes(super::boxed(b"abc\n")))], "met whole: up as it came");
    assert!(requests.is_empty(), "nothing read ahead");
    assert_eq!(machine.client.waiting(), Waiting::Above);
    let (_, requests) = machine.down(Request::Body(Down::Demand { read: LINE, room: 0 }));
    assert_eq!(requests, [Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 6 }, room: 0 }]);
    let (events, requests) = machine.bytes(b"defghi");
    assert_eq!(events, [Event::Body(Up::End), Event::Done(Reuse::Keep)], "no LF, short of the scan's maximum");
    assert!(requests.is_empty(), "nothing past the body");
}

#[test]
fn a_fill_past_what_is_left_of_the_body_is_answered_by_its_end() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 6\r\n");
    machine.bytes(b"\r\n");
    let (_, requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(16), room: 0 }));
    assert_eq!(requests, [Down::Demand { read: Read::Fill(6), room: 0 }], "no fill past the body");
    let (events, requests) = machine.bytes(b"defghi");
    assert_eq!(events, [Event::Body(Up::End), Event::Done(Reuse::Keep)], "a read larger than what is left (lib.md, 7)");
    assert!(requests.is_empty());
    assert_eq!(machine.client.waiting(), Waiting::Call);
}

#[test]
fn a_fill_across_two_deliveries_is_met_from_the_intake() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Transfer-Encoding: chunked\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(6), room: 0 }));
    let (_, requests) = machine.bytes(b"3\r\n");
    assert_eq!(requests, [Down::Demand { read: Read::Fill(3), room: 0 }], "within the chunk");
    let (events, _) = machine.bytes(b"def");
    assert!(events.is_empty(), "held: three bytes cannot meet a fill of six");
    machine.bytes(b"\r\n");
    let (_, requests) = machine.bytes(b"3\r\n");
    assert_eq!(requests, [Down::Demand { read: Read::Fill(3), room: 0 }], "what the fill still needs");
    let (events, requests) = machine.bytes(b"ghi");
    assert_eq!(events, [Event::Body(Up::Bytes(super::boxed(b"defghi")))]);
    assert!(requests.is_empty());
}

#[test]
fn a_withdrawn_demand_is_never_answered_and_what_is_read_for_it_is_dropped() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 6\r\n");
    machine.bytes(b"\r\n");
    let (_, requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    assert_eq!(requests, [Down::Demand { read: Read::Fill(4), room: 0 }]);
    let (events, requests) = machine.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 }));
    assert!(events.is_empty() && requests.is_empty(), "the read below goes on: it is not withdrawn but by a close");
    let (events, requests) = machine.bytes(b"abcd");
    assert!(events.is_empty() && requests.is_empty(), "dropped, and nothing more read until the rest is discarded");
    assert_eq!(machine.client.waiting(), Waiting::Above);
    let (events, requests) = machine.down(Request::Discard);
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Fill(2), room: 0 }]);
    let (events, _) = machine.bytes(b"ef");
    assert_eq!(events, [Event::Done(Reuse::Keep)]);
}

#[test]
fn a_withdrawal_that_crosses_its_answer_withdraws_all_the_same() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 6\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(2), room: 0 }));
    let (events, _) = machine.bytes(b"ab");
    assert_eq!(events, [Event::Body(Up::Bytes(super::boxed(b"ab")))]);
    // The side above withdrew before it saw the answer (lib.md, 7).
    let (events, requests) = machine.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 }));
    assert!(events.is_empty() && requests.is_empty());
    let (_, requests) = machine.down(Request::Discard);
    assert_eq!(requests, [Down::Demand { read: Read::Fill(4), room: 0 }], "the rest, discarded");
    let (events, _) = machine.bytes(b"cdef");
    assert_eq!(events, [Event::Done(Reuse::Keep)]);
}

#[test]
fn a_withdrawal_or_a_discard_that_crosses_the_body_s_end_is_dropped() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 0\r\n");
    machine.bytes(b"\r\n");
    let (events, _) = machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    assert_eq!(events, [Event::Body(Up::End), Event::Done(Reuse::Keep)]);
    let nothing = (Vec::new(), Vec::new());
    assert_eq!(machine.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 })), nothing);
    assert_eq!(machine.down(Request::Discard), nothing);
    assert_eq!(machine.client.waiting(), Waiting::Call, "the connection is kept");
    let (head, _) = machine.called(get());
    assert!(head.starts_with(b"GET / HTTP/1.1\r\n"), "the next call goes out");
}

#[test]
#[should_panic(expected = "a body demand with no exchange in progress")]
fn no_body_demand_once_the_exchange_is_done() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 0\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
}

#[test]
#[should_panic(expected = "a body demand after its withdrawal")]
fn no_body_demand_after_a_withdrawal() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    machine.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 }));
    machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
}

#[test]
fn the_framing_of_a_chunked_body_is_read_only_for_a_demand() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Transfer-Encoding: chunked\r\n");
    let (_, requests) = machine.bytes(b"\r\n");
    assert!(requests.is_empty(), "no size line before a demand");
    let (_, requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    assert_eq!(requests, [Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 256 }, room: 0 }]);
    let (_, requests) = machine.bytes(b"2\r\n");
    assert_eq!(requests, [Down::Demand { read: Read::Fill(2), room: 0 }], "within the chunk");
    let (_, requests) = machine.bytes(b"ab");
    assert_eq!(
        requests,
        [Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 2 }, room: 0 }],
        "its line ending"
    );
    let (_, requests) = machine.bytes(b"\r\n");
    assert_eq!(requests, [Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 256 }, room: 0 }]);
    let (_, requests) = machine.bytes(b"3\r\n");
    assert_eq!(requests, [Down::Demand { read: Read::Fill(2), room: 0 }], "what the fill still needs");
    let (events, requests) = machine.bytes(b"cd");
    assert_eq!(events, [Event::Body(Up::Bytes(super::boxed(b"abcd")))]);
    assert!(requests.is_empty());
}

#[test]
fn a_chunked_body_that_breaks_its_framing_fails() {
    let chunked = |body: &[u8]| failure(&response(CHUNKED, body));
    assert_eq!(chunked(b"x\r\n"), Error::ChunkSize);
    assert_eq!(chunked(b"\r\n"), Error::ChunkSize, "no digits");
    assert_eq!(chunked(b"-1\r\n"), Error::ChunkSize);
    assert_eq!(chunked(b"0x5\r\nhello\r\n0\r\n\r\n"), Error::ChunkSize);
    assert_eq!(chunked(b"5 6\r\n"), Error::ChunkSize);
    assert_eq!(chunked(b"5;\x01\r\n"), Error::ChunkSize, "a control character in an extension");
    assert_eq!(chunked(b"10000000000000000\r\n"), Error::ChunkSize, "past a u64");
    assert_eq!(
        chunked(b"FFFFFFFFFFFFFFFF\r\n"),
        Error::Truncated { answered: true },
        "a u64, and the stream ends in it"
    );
    let mut long = Vec::from(&b"1;"[..]);
    long.extend_from_slice(&[b'e'; 300]);
    long.extend_from_slice(b"\r\nx\r\n0\r\n\r\n");
    assert_eq!(chunked(&long), Error::ChunkSize, "a size line past Limits::head");
    assert_eq!(chunked(b"2\r\nabc\r\n0\r\n\r\n"), Error::Chunk, "data longer than its size");
    assert_eq!(chunked(b"2\r\nab\rx0\r\n\r\n"), Error::Chunk);
    let mut trailers = Vec::from(&b"0\r\n"[..]);
    for _ in 0..30_u32 {
        trailers.extend_from_slice(b"Trailer: abcdefgh\r\n");
    }
    trailers.extend_from_slice(b"\r\n");
    assert_eq!(chunked(&trailers), Error::Trailer);
    assert_eq!(chunked(b"5\r\nhel"), Error::Truncated { answered: true });
    assert_eq!(chunked(b"5\r\nhello\r\n"), Error::Truncated { answered: true }, "no last chunk");
    assert_eq!(chunked(b"5\r\nhello\r\n0\r\n"), Error::Truncated { answered: true }, "no end to the trailer section");
    assert_eq!(failure(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort"), Error::Truncated { answered: true });
}

#[test]
fn a_framing_error_fails_the_body_stream_then_the_exchange() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Transfer-Encoding: chunked\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    let (events, requests) = machine.bytes(b"zz\r\n");
    assert_eq!(events, [Event::Body(Up::Failed(Fault::Invalid)), Event::Failed(Error::ChunkSize)]);
    assert!(requests.is_empty());
    assert_eq!(machine.client.waiting(), Waiting::Close);
    let (events, requests) = machine.up(Up::End);
    assert!(events.is_empty() && requests.is_empty(), "nothing after the exchange failed");
}

#[test]
fn a_failure_of_the_stream_fails_the_body_with_its_fault() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 4\r\n");
    machine.bytes(b"\r\n");
    let (events, _) = machine.up(Up::Failed(Fault::Reset));
    assert_eq!(events, [Event::Body(Up::Failed(Fault::Reset)), Event::Failed(Error::Stream(Fault::Reset))]);
}

#[test]
fn a_failure_after_the_body_is_all_read_below_leaves_it_whole_on_a_connection_not_reused() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"Content-Length: 4\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(2), room: 0 }));
    machine.bytes(b"ab");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(2), room: 0 }));
    let (events, _) = machine.bytes(b"cd");
    assert_eq!(events, [Event::Body(Up::Bytes(super::boxed(b"cd")))], "the last of it");
    let (events, requests) = machine.up(Up::Failed(Fault::Reset));
    assert!(events.is_empty() && requests.is_empty(), "the body stands");
    let (events, _) = machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    assert_eq!(events, [Event::Body(Up::End), Event::Done(Reuse::Close)]);
}

#[test]
fn a_discarded_body_is_read_and_dropped_and_the_connection_used_again() {
    for body in [
        &b"Content-Length: 40\r\n\r\n0123456789012345678901234567890123456789"[..],
        b"Transfer-Encoding: chunked\r\n\r\n20\r\n01234567890123456789012345678901\r\n8\r\nabcdefgh\r\n0\r\n\r\n",
    ] {
        let mut machine = Machine::new(LIMITS);
        machine.called(get());
        let mut response = Vec::from(&b"HTTP/1.1 200 OK\r\n"[..]);
        response.extend_from_slice(body);
        let mut intake = skein_lib::Intake::with_capacity(u32::try_from(response.len()).unwrap().max(256));
        intake.append(&response).unwrap();
        let mut read = Read::Scan { until: Delimiter::LF, max: 256 };
        let mut events = Vec::new();
        for _ in 0..200_u32 {
            let (more, requests) = machine.up(Up::Bytes(intake.meet(read).expect("met")));
            events.extend(more);
            match requests.as_slice() {
                [Down::Demand { read: next, room: 0 }] => read = *next,
                [] => break,
                other => panic!("{other:?}"),
            }
        }
        let [Event::Response(_)] = events.as_slice() else { panic!("{events:?}") };
        let (mut events, mut requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
        let (more, extra) = machine.down(Request::Discard);
        events.extend(more);
        requests.extend(extra);
        for _ in 0..200_u32 {
            let Some(Down::Demand { read, room: 0 }) = requests.pop() else { break };
            assert!(requests.is_empty());
            let (more, next) = machine.up(Up::Bytes(intake.meet(read).expect("met")));
            events.extend(more);
            requests = next;
        }
        assert_eq!(events, [Event::Done(Reuse::Keep)], "nothing more on the body's stream");
        assert!(intake.is_empty(), "read to its end");
        assert_eq!(machine.client.waiting(), Waiting::Call);
    }
}

#[test]
fn a_discard_of_a_body_to_the_end_of_the_stream_ends_the_exchange_and_withdraws_its_read() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"\r\n");
    let (_, requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    assert_eq!(requests, [Down::Demand { read: Read::Fill(4), room: 0 }]);
    let (events, requests) = machine.down(Request::Discard);
    assert_eq!(events, [Event::Done(Reuse::Close)]);
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 0 }]);
    assert_eq!(machine.client.waiting(), Waiting::Close);
    let (events, requests) = machine.bytes(b"late");
    assert!(events.is_empty() && requests.is_empty(), "an answer on its way, dropped");
}

#[test]
fn what_a_body_leaves_unread_does_not_reach_the_next_exchange() {
    let mut machine = Machine::new(LIMITS);
    let first =
        exchange(&mut machine, get(), b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabcde", drive(Read::Fill(3)));
    assert_eq!(first.body, b"abc", "de never meets a fill of three");
    assert_eq!(first.outcome, Some(Event::Done(Reuse::Keep)));
    let second =
        exchange(&mut machine, get(), b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nxyz", drive(Read::Fill(3)));
    assert_eq!(second.body, b"xyz");
}

#[test]
#[should_panic(expected = "no demand past Limits::read")]
fn a_demand_past_what_the_intake_holds_is_the_side_above_s_bug() {
    let mut machine = Machine::new(Limits { read: 4, ..LIMITS });
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(5), room: 0 }));
}
