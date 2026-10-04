//! The response (http.md, 5.4): the head written sized, its framing and
//! its connection the server's own, every refusal in order, and the body
//! by length, in chunks, and to the end of the stream.

#![expect(clippy::disallowed_types, reason = "a test builds what it expects in a Vec")]

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::stream::{Down, Read, Up};

use super::{Drive, LIMITS, Machine, Served, When, boxed, get, header, response, serve};
use crate::server::{Body, Event, Limits, Refusal, Request, Response, Reuse, Waiting};

/// Serves `request` with `response`, reading the body first, the reply
/// written in pieces of at most [`Limits::send`].
fn served(request: &[u8], response: Response, reply: &[u8]) -> Served {
    let drive = Drive { read: Read::Fill(16), when: When::AfterBody, reply };
    serve(&mut Machine::new(LIMITS), request, response, drive)
}

/// Why `response` is refused, and that the exchange is as it was.
fn refused(response: Response, limits: Limits) -> Refusal {
    let mut machine = Machine::new(limits);
    machine.call(get());
    let (events, requests) = machine.down(Request::Respond(response));
    assert!(requests.is_empty(), "a refused response writes nothing: {requests:?}");
    assert_eq!(machine.server.waiting(), Waiting::Above, "the exchange is as it was");
    let (again, sent) = machine.down(Request::Respond(super::response(200, Body::None)));
    assert!(again.is_empty() && sent.len() == 1, "a response may follow: {again:?}");
    match events.as_slice() {
        [Event::Refused(refusal)] => *refusal,
        other => panic!("refused, not {other:?}"),
    }
}

#[test]
fn a_response_is_its_status_line_its_fields_and_the_server_s_own() {
    let response = Response {
        status: 200,
        headers: Box::new([header(b"Content-Type", b"application/json"), header(b"x-request-id", b"r1")]),
        body: Body::Length(2),
        close: false,
    };
    let served = served(get(), response, b"{}");
    let expected =
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nx-request-id: r1\r\nContent-Length: 2\r\n\r\n{}";
    assert_eq!(served.sent, expected);
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn a_status_has_the_standard_s_reason_or_none() {
    let reasons: [(u16, &[u8]); 6] = [
        (201, b"201 Created\r\n"),
        (404, b"404 Not Found\r\n"),
        (429, b"429 Too Many Requests\r\n"),
        (500, b"500 Internal Server Error\r\n"),
        (529, b"529 \r\n"),
        (299, b"299 \r\n"),
    ];
    for (status, line) in reasons {
        let served = served(get(), response(status, Body::None), b"");
        assert!(served.sent[9..].starts_with(line), "{}", served.sent.escape_ascii());
    }
}

#[test]
fn no_body_is_said_by_a_length_of_zero_but_in_a_204_or_a_304() {
    let served_none = served(get(), Response { headers: Box::new([]), ..response(200, Body::None) }, b"");
    assert_eq!(served_none.sent, b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    for status in [204, 304] {
        let served = served(get(), Response { headers: Box::new([]), ..response(status, Body::None) }, b"");
        assert_eq!(skein_lib::bytes::find(&served.sent, b"Content-Length:"), None, "{status}");
        assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)));
    }
}

#[test]
fn a_response_to_head_says_its_framing_and_sends_no_body() {
    let head = b"HEAD / HTTP/1.1\r\nHost: h\r\n\r\n";
    let served_length = served(head, response(200, Body::Length(1234)), b"");
    assert!(served_length.sent.ends_with(b"Content-Length: 1234\r\n\r\n"), "{}", served_length.sent.escape_ascii());
    assert_eq!(served_length.outcome, Some(Event::Done(Reuse::Keep)));
    let served_chunks = served(head, response(200, Body::Chunked), b"");
    assert!(served_chunks.sent.ends_with(b"Transfer-Encoding: chunked\r\n\r\n"));
    assert_eq!(served_chunks.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn a_body_by_length_goes_down_in_the_pieces_the_side_above_sends() {
    let mut body = Vec::new();
    for byte in 0..40_u8 {
        body.push(byte);
    }
    let served_body = served(get(), response(200, Body::Length(40)), &body);
    assert!(served_body.sent.ends_with(&body), "the body after the head, whole");
    assert_eq!(served_body.outcome, Some(Event::Done(Reuse::Keep)));
    // An empty one is finished at once.
    let served_empty = served(get(), response(200, Body::Length(0)), b"");
    assert!(served_empty.sent.ends_with(b"Content-Length: 0\r\n\r\n"));
    assert_eq!(served_empty.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn a_chunked_body_goes_down_a_chunk_a_send_and_ends_with_the_last_chunk() {
    let mut body = Vec::new();
    for _ in 0..2_u8 {
        for byte in b'a'..=b'z' {
            body.push(byte);
        }
    }
    let served = served(get(), response(200, Body::Chunked), &body);
    let mut expected = Vec::from(&b"HTTP/1.1 200 OK\r\nServer: skein\r\nTransfer-Encoding: chunked\r\n\r\n"[..]);
    for piece in body.chunks(16) {
        expected.extend_from_slice(if piece.len() == 16 { b"10\r\n" } else { b"4\r\n" });
        expected.extend_from_slice(piece);
        expected.extend_from_slice(b"\r\n");
    }
    expected.extend_from_slice(b"0\r\n\r\n");
    assert_eq!(served.sent, expected, "{}", served.sent.escape_ascii());
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn room_for_a_chunk_is_its_size_line_and_its_endings_and_an_empty_piece_is_no_chunk() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    let (_, requests) = machine.down(Request::Respond(response(200, Body::Chunked)));
    assert_eq!(requests.len(), 1, "the head, at once");
    let (_, requests) = machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 16 }));
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 16 + 2 + 4 }]);
    let (events, _) = machine.up(Up::Room);
    assert_eq!(events, [Event::Reply(Up::Room)]);
    let (events, requests) = machine.down(Request::Reply(Down::Send(boxed(b""))));
    assert!(events.is_empty() && requests.is_empty(), "no chunk for nothing");
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 9 }));
    machine.up(Up::Room);
    let (_, requests) = machine.down(Request::Reply(Down::Send(boxed(b"abcdefghi"))));
    assert_eq!(requests, [Down::Send(boxed(b"9\r\nabcdefghi\r\n"))]);
    let (events, requests) = machine.down(Request::Reply(Down::Finish));
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 5 }], "room for the last chunk");
    assert_eq!(machine.server.waiting(), Waiting::Room);
    let (events, requests) = machine.up(Up::Room);
    assert_eq!(requests, [Down::Send(boxed(b"0\r\n\r\n"))]);
    assert_eq!(events, [Event::Done(Reuse::Keep)]);
}

#[test]
fn a_chunked_body_goes_to_an_http_one_zero_client_to_the_end_of_the_stream() {
    let served = served(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n", response(200, Body::Chunked), b"hello");
    assert_eq!(served.sent, b"HTTP/1.1 200 OK\r\nServer: skein\r\nConnection: close\r\n\r\nhello");
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Close)));
}

#[test]
fn what_the_side_above_gets_wrong_is_refused_in_order() {
    let with = |headers: Box<[crate::Header]>| Response { headers, ..response(200, Body::None) };
    assert_eq!(refused(response(199, Body::None), LIMITS), Refusal::Status);
    assert_eq!(refused(response(101, Body::None), LIMITS), Refusal::Status);
    assert_eq!(refused(response(600, Body::None), LIMITS), Refusal::Status);
    assert_eq!(refused(with(Box::new([header(b"", b"v")])), LIMITS), Refusal::Name);
    assert_eq!(refused(with(Box::new([header(b"Bad Name", b"v")])), LIMITS), Refusal::Name);
    assert_eq!(refused(with(Box::new([header(b"X", b"a\r\nInjected: yes")])), LIMITS), Refusal::Value);
    assert_eq!(refused(with(Box::new([header(b"X", b"\0")])), LIMITS), Refusal::Value);
    for name in [&b"Content-Length"[..], b"transfer-encoding", b"CONNECTION"] {
        assert_eq!(refused(with(Box::new([header(name, b"5")])), LIMITS), Refusal::Reserved);
    }
    assert_eq!(refused(response(204, Body::Length(1)), LIMITS), Refusal::Body);
    assert_eq!(refused(response(304, Body::Chunked), LIMITS), Refusal::Body);
    let long = with(Box::new([header(b"X", &[b'v'; 240])]));
    assert_eq!(refused(long, LIMITS), Refusal::TooLong);
    // In order: the status before the fields, a name before its value.
    let both = Response { status: 99, ..with(Box::new([header(b"Bad Name", b"\0")])) };
    assert_eq!(refused(both, LIMITS), Refusal::Status);
    assert_eq!(refused(with(Box::new([header(b"Bad Name", b"\0")])), LIMITS), Refusal::Name);
}

#[test]
fn a_head_at_the_limit_goes_and_one_past_it_is_refused() {
    let head_of = |len: usize| {
        let mut value = Vec::new();
        value.resize(len, b'v');
        Response { headers: Box::new([header(b"X", &value)]), ..response(200, Body::None) }
    };
    // `HTTP/1.1 200 OK\r\n`, `X: ` and the value and `\r\n`,
    // `Content-Length: 0\r\n`, `\r\n`: 17 + 5 + 19 + 2 bytes and the value.
    let fits = usize::try_from(LIMITS.response).unwrap() - 43;
    let served = served(get(), head_of(fits), b"");
    assert_eq!(served.sent.len(), usize::try_from(LIMITS.response).unwrap());
    assert_eq!(refused(head_of(fits + 1), LIMITS), Refusal::TooLong);
}

#[test]
fn a_withdrawal_of_the_reply_s_demand_withdraws_the_room_below_and_waits_for_the_close() {
    // As a writer stacked on the reply withdraws its demand when it closes.
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.down(Request::Respond(response(200, Body::Chunked)));
    let (_, requests) = machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 8 }));
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 8 + 1 + 4 }]);
    let (events, requests) = machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 0 }));
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 0 }], "the room below withdrawn with it");
    assert_eq!(machine.server.waiting(), Waiting::Above, "for the close");
    let (events, requests) = machine.up(Up::Room);
    assert!(events.is_empty() && requests.is_empty(), "room on its way, dropped");
    let (events, requests) = machine.down(Request::Close);
    assert_eq!(events, [Event::Closed]);
    assert!(requests.is_empty(), "nothing left to withdraw");
    // One that crosses the room's answer withdraws all the same.
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.down(Request::Respond(response(200, Body::Length(4))));
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 4 }));
    let (events, _) = machine.up(Up::Room);
    assert_eq!(events, [Event::Reply(Up::Room)]);
    let (events, requests) = machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 0 }));
    assert!(events.is_empty() && requests.is_empty(), "nothing demanded below to withdraw");
    let (events, _) = machine.up(Up::Failed(skein_lib::stream::Fault::Reset));
    assert_eq!(
        events,
        [Event::Failed(crate::server::Error::Stream(skein_lib::stream::Fault::Reset))],
        "nothing on a reply withdrawn"
    );
}

#[test]
#[should_panic(expected = "a reply before the response")]
fn no_reply_before_the_response() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 4 }));
}

#[test]
#[should_panic(expected = "a reply demand for a body that has none")]
fn no_reply_for_a_response_without_a_body() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Respond(response(200, Body::None)));
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 4 }));
}

#[test]
#[should_panic(expected = "a Send within the room granted")]
fn no_reply_send_without_room() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Respond(response(200, Body::Length(4))));
    machine.down(Request::Reply(Down::Send(boxed(b"body"))));
}

#[test]
#[should_panic(expected = "Finish once the response's length is sent")]
fn no_finish_short_of_the_length() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Respond(response(200, Body::Length(4))));
    machine.down(Request::Reply(Down::Finish));
}

#[test]
#[should_panic(expected = "no room past Limits::send")]
fn no_reply_room_past_the_limit() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Respond(response(200, Body::Chunked)));
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 17 }));
}

#[test]
#[should_panic(expected = "no more than the response's length")]
fn no_reply_past_the_length() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.down(Request::Respond(response(200, Body::Length(2))));
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 4 }));
    machine.up(Up::Room);
    machine.down(Request::Reply(Down::Send(boxed(b"abc"))));
}

#[test]
#[should_panic(expected = "one response per call")]
fn one_response_per_call() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Respond(response(200, Body::Length(1))));
    machine.down(Request::Respond(response(200, Body::Length(1))));
}
