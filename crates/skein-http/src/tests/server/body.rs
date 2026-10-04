//! The request body (http.md, 5.3): each framing under demands of every
//! shape, the end of an empty body, discarding on a connection kept and
//! on one not, every framing error, a withdrawal, and the 100 (Continue).

#![expect(clippy::disallowed_types, reason = "a test builds what it sends in a Vec")]

use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};

use super::{Drive, LIMITS, Machine, Served, When, response, serve};
use crate::server::{Body, Error, Event, Request, Reuse, Waiting};

const LINE: Read = Read::Scan { until: Delimiter::LF, max: 16 };
const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";
const NO_CONTENT: &[u8] = b"HTTP/1.1 204 No Content\r\nServer: skein\r\n\r\n";

/// `head` and the body after it.
fn request(head: &[u8], body: &[u8]) -> Vec<u8> {
    let mut request = Vec::from(head);
    request.extend_from_slice(body);
    request
}

/// Serves `request`, reading its body with `read` before a 204.
fn read_by(request: &[u8], read: Read) -> Served {
    let drive = Drive { read, when: When::AfterBody, reply: b"" };
    serve(&mut Machine::new(LIMITS), request, response(204, Body::None), drive)
}

const POST_11: &[u8] = b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\n\r\n";
const CHUNKED: &[u8] = b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n";

#[test]
fn a_body_by_length_is_read_to_its_length_and_ended() {
    // What each read sees of `hello\nworld`: a read larger than what is
    // left never meets the rest (lib.md, 7).
    let reads: [(Read, &[u8]); 5] = [
        (Read::Fill(1), b"hello\nworld"),
        (Read::Fill(3), b"hello\nwor"),
        (Read::Fill(16), b""),
        (LINE, b"hello\n"),
        (Read::Line { max: 4 }, b"hello\nworl"),
    ];
    for (read, seen) in reads {
        let served = read_by(&request(POST_11, b"hello\nworld"), read);
        assert!(served.ended, "{read:?}");
        assert_eq!(served.body, seen, "{read:?}");
        assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)), "{read:?}");
        assert!(served.sent.ends_with(NO_CONTENT), "{}", served.sent.escape_ascii());
    }
}

#[test]
fn a_chunked_body_is_read_across_its_chunks_whatever_the_demands() {
    let body = b"5\r\nhello\r\n1;ext=\"v\"\r\n\n\r\n7 \r\nworld!\n\r\n0\r\nTrailer: t\r\n\r\n";
    for read in [Read::Fill(1), Read::Fill(13), LINE, Read::Scan { until: Delimiter::LF, max: 4 }] {
        let served = read_by(&request(CHUNKED, body), read);
        assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)), "{read:?}");
        assert!(served.ended);
        assert_eq!(served.body, b"hello\nworld!\n", "{read:?}");
    }
}

#[test]
fn the_first_demand_of_an_empty_body_gets_its_end() {
    for head in [&b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"[..], b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n"]
    {
        let mut machine = Machine::new(LIMITS);
        let call = machine.call(head);
        assert!(call.body == Body::None || call.body == Body::Length(0));
        let (events, requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
        assert_eq!(events, [Event::Body(Up::End)]);
        assert!(requests.is_empty(), "nothing read below for it");
        assert_eq!(machine.server.waiting(), Waiting::Above, "for the response");
    }
}

#[test]
fn chunk_framing_that_is_not_one_fails_the_exchange_and_its_stream() {
    let mut trailer = Vec::from(&b"1\r\na\r\n0\r\nX: "[..]);
    trailer.extend_from_slice(&[b'a'; 300]);
    trailer.extend_from_slice(b"\r\n\r\n");
    let bodies: [(&[u8], Error); 5] = [
        (b"x\r\n", Error::ChunkSize),
        (b"FFFFFFFFFFFFFFFFF\r\n", Error::ChunkSize),
        (b"3\r\nabcX\r\n", Error::Chunk),
        (b"3;\x01\r\nabc\r\n", Error::ChunkSize),
        (&trailer, Error::Trailer),
    ];
    for (body, error) in bodies {
        let served = read_by(&request(CHUNKED, body), Read::Fill(1));
        assert_eq!(served.outcome, Some(Event::Failed(error)), "{}", body.escape_ascii());
        assert_eq!(served.body_failed, Some(Fault::Invalid), "the side above's stream told it is invalid");
        assert!(served.sent.is_empty(), "bad framing closes, unanswered");
    }
}

const TOO_LARGE: &[u8] = b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// A chunked body of `sizes`, each chunk's data `a`s, its size line with
/// an extension of `extension` bytes, and its last chunk.
fn chunks(sizes: &[usize], extension: usize) -> Vec<u8> {
    let mut body = Vec::new();
    for &size in sizes {
        let mut digits = Vec::new();
        let mut left = size;
        loop {
            digits.push(b"0123456789abcdef"[left % 16]);
            left /= 16;
            if left == 0 {
                break;
            }
        }
        digits.reverse();
        body.extend_from_slice(&digits);
        if extension > 0 {
            body.push(b';');
            body.resize(body.len().saturating_add(extension.saturating_sub(1)), b'e');
        }
        body.extend_from_slice(b"\r\n");
        body.resize(body.len().saturating_add(size), b'a');
        if size > 0 {
            body.extend_from_slice(b"\r\n");
        }
    }
    body.extend_from_slice(b"\r\n");
    body
}

#[test]
fn a_chunked_body_holds_up_to_the_limit_and_fails_at_the_size_line_that_takes_it_past() {
    // The limit is 1024 bytes of data.
    let served = read_by(&request(CHUNKED, &chunks(&[1000, 24, 0], 0)), Read::Fill(16));
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)), "a body at the limit");
    assert_eq!(served.body.len(), 1024);
    // The size line is enough: no data of the chunk past the limit is read.
    let bodies: [(&[u8], usize); 3] =
        [(b"401\r\n", 0), (&chunks(&[1000, 25, 0], 0), 1000), (b"7fffffffffffffff;e\r\n", 0)];
    for (body, seen) in bodies {
        let served = read_by(&request(CHUNKED, body), Read::Fill(8));
        assert_eq!(served.outcome, Some(Event::Failed(Error::BodyTooLong)), "{}", body.escape_ascii());
        assert_eq!(served.body.len(), seen, "what went up before the chunk past the limit");
        assert_eq!(served.body_failed, Some(Fault::Invalid));
        assert_eq!(served.sent, TOO_LARGE, "a 413 in the room set aside");
    }
}

#[test]
fn a_chunked_body_past_the_limit_after_a_continue_or_a_response_closes_unanswered() {
    let expect = b"POST / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\n\r\n";
    let served = read_by(&request(expect, &chunks(&[1025, 0], 0)), Read::Fill(16));
    assert_eq!(served.outcome, Some(Event::Failed(Error::BodyTooLong)));
    assert_eq!(served.sent, CONTINUE, "the room set aside went to the 100");
    // A discard reads to the same limit, and its response, waiting for the
    // body's end, never goes.
    let drive = Drive { read: Read::Fill(16), when: When::Discarding, reply: b"" };
    let served = serve(
        &mut Machine::new(LIMITS),
        &request(CHUNKED, &chunks(&[1000, 25, 0], 0)),
        response(204, Body::None),
        drive,
    );
    assert_eq!(served.outcome, Some(Event::Failed(Error::BodyTooLong)));
    assert_eq!(served.body_failed, None, "a stream the side above reads no more is told nothing");
    assert!(served.sent.is_empty(), "the response waited for the body's end: {}", served.sent.escape_ascii());
}

#[test]
fn a_chunked_body_s_extensions_hold_up_to_the_head_s_limit_in_all() {
    // The limit is 256 bytes, the extensions of every size line together.
    let served = read_by(&request(CHUNKED, &chunks(&[1, 1, 1, 0], 64)), Read::Fill(16));
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)), "256 bytes of extensions");
    let served = read_by(&request(CHUNKED, &chunks(&[1, 1, 1, 1, 0], 52)), Read::Fill(1));
    assert_eq!(served.outcome, Some(Event::Failed(Error::Extensions)), "260 bytes of them");
    assert_eq!(served.body, b"aaaa", "the chunks before the last line");
    assert_eq!(served.body_failed, Some(Fault::Invalid));
    assert!(served.sent.is_empty(), "closed unanswered, as for bad framing");
}

#[test]
fn a_discard_on_a_connection_kept_reads_the_body_to_its_end_and_drops_it() {
    let mut machine = Machine::new(LIMITS);
    let mut served = Vec::new();
    for body in [&b"hello world"[..], b"another one"] {
        let drive = Drive { read: Read::Fill(1), when: When::Discarding, reply: b"" };
        served.push(serve(&mut machine, &request(POST_11, body), response(204, Body::None), drive));
    }
    for served in served {
        assert!(served.body.is_empty() && !served.ended, "nothing goes up of a body discarded");
        assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)), "the connection kept");
        assert_eq!(served.sent, NO_CONTENT, "the head waited for the body's end, and kept the connection");
    }
}

#[test]
fn a_discard_on_a_connection_not_kept_reads_nothing_more() {
    let head = b"POST / HTTP/1.1\r\nHost: h\r\nConnection: close\r\nContent-Length: 11\r\n\r\n";
    let mut machine = Machine::new(LIMITS);
    machine.call(head);
    let (events, requests) = machine.down(Request::Discard);
    assert!(events.is_empty() && requests.is_empty(), "nothing read below");
    let (events, requests) = machine.down(Request::Respond(response(204, Body::None)));
    assert_eq!(events, [Event::Done(Reuse::Close)]);
    let [Down::Send(head)] = &requests[..] else { panic!("the head: {requests:?}") };
    assert!(head.ends_with(b"Connection: close\r\n\r\n"), "{}", head.escape_ascii());
}

#[test]
fn a_read_outstanding_when_the_body_is_discarded_on_a_connection_not_kept_is_withdrawn() {
    let head = b"POST / HTTP/1.0\r\nContent-Length: 11\r\n\r\n";
    let mut machine = Machine::new(LIMITS);
    machine.call(head);
    let (_, requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    assert_eq!(requests, [Down::Demand { read: Read::Fill(4), room: 0 }]);
    let (events, requests) = machine.down(Request::Discard);
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 0 }], "withdrawn");
    let (events, requests) = machine.bytes(b"hell");
    assert!(events.is_empty() && requests.is_empty(), "its answer, on its way, dropped");
}

#[test]
fn a_withdrawal_then_a_discard_reads_the_rest_and_drops_it() {
    let mut machine = Machine::new(LIMITS);
    machine.call(POST_11);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    let (events, requests) = machine.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 }));
    assert!(events.is_empty() && requests.is_empty(), "the read below stands: what it reads is dropped");
    let (events, _) = machine.bytes(b"hell");
    assert!(events.is_empty(), "read for a demand withdrawn: dropped");
    let (events, requests) = machine.down(Request::Discard);
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Fill(7), room: 0 }], "the rest, to drop");
    machine.bytes(b"o world");
    let (events, requests) = machine.down(Request::Respond(response(204, Body::None)));
    assert_eq!(events, [Event::Done(Reuse::Keep)]);
    assert_eq!(requests, [Down::Send(super::boxed(NO_CONTENT))]);
}

#[test]
fn a_continue_goes_before_the_body_s_first_read_and_the_head_asks_for_room_of_its_own() {
    let head = b"POST / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n";
    let mut machine = Machine::new(LIMITS);
    machine.call(head);
    let (events, requests) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Send(super::boxed(CONTINUE)), Down::Demand { read: Read::Fill(4), room: 0 }]);
    let (events, _) = machine.bytes(b"body");
    assert_eq!(events, [Event::Body(Up::Bytes(super::boxed(b"body")))]);
    let (events, requests) = machine.down(Request::Respond(response(204, Body::None)));
    assert!(events.is_empty());
    let room = u32::try_from(NO_CONTENT.len()).unwrap();
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room }], "the room set aside went to the 100");
    assert_eq!(machine.server.waiting(), Waiting::Room);
    let (events, requests) = machine.up(Up::Room);
    assert!(events.is_empty(), "the body's end is still to be read: {events:?}");
    assert_eq!(requests, [Down::Send(super::boxed(NO_CONTENT))]);
    let (events, _) = machine.down(Request::Body(Down::Demand { read: Read::Fill(4), room: 0 }));
    assert_eq!(events, [Event::Body(Up::End), Event::Done(Reuse::Keep)]);
}

#[test]
fn no_continue_goes_where_the_client_waits_for_none_or_a_final_response_answers_first() {
    let heads: [&[u8]; 4] = [
        b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\n\r\n",
        b"POST / HTTP/1.0\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n",
        b"POST / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 0\r\n\r\n",
        b"POST / HTTP/1.1\r\nHost: h\r\nExpect: something-else\r\nContent-Length: 4\r\n\r\n",
    ];
    for head in heads {
        let served = read_by(&request(head, b"body"), Read::Fill(2));
        assert!(!served.sent.starts_with(b"HTTP/1.1 100"), "{}", head.escape_ascii());
    }
    // A final response first: the client never sends the body, which is
    // given up.
    let head = b"POST / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n";
    let drive = Drive { read: Read::Fill(1), when: When::First, reply: b"" };
    let served = serve(&mut Machine::new(LIMITS), head, response(417, Body::None), drive);
    assert!(served.sent.starts_with(b"HTTP/1.1 417 Expectation Failed\r\n"), "{}", served.sent.escape_ascii());
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Close)));
}

#[test]
#[should_panic(expected = "no demand past Limits::read")]
fn no_body_demand_past_the_limit() {
    let mut machine = Machine::new(LIMITS);
    machine.call(POST_11);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(17), room: 0 }));
}

#[test]
#[should_panic(expected = "one demand at a time")]
fn one_body_demand_at_a_time() {
    let mut machine = Machine::new(LIMITS);
    machine.call(POST_11);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
}

#[test]
#[should_panic(expected = "no body demand once the body is over")]
fn no_body_demand_after_its_end() {
    let mut machine = Machine::new(LIMITS);
    machine.call(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
}
