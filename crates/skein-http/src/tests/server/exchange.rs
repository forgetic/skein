//! An exchange's life (http.md, 5.5): reuse by each rule, a response given
//! before the request body is read, the stream ending or failing in each
//! state, a close in each state, and what the server waits for.

#![expect(clippy::disallowed_types, reason = "a test builds what it sends in a Vec")]

use alloc::vec::Vec;

use skein_lib::stream::{Down, Fault, Read, Up};

use super::{ASIDE, Drive, FIRST_LINE, LIMITS, Machine, When, boxed, get, response, serve};
use crate::server::{Body, Error, Event, Request, Response, Reuse, Waiting};

const POST: &[u8] = b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 8\r\n\r\n";

/// Serves `request` whole: the body read, then `response` with no body.
fn reuse_of(request: &[u8], response: Response) -> (Reuse, bool) {
    let drive = Drive { read: Read::Fill(8), when: When::AfterBody, reply: b"" };
    let served = serve(&mut Machine::new(LIMITS), request, response, drive);
    let Some(Event::Done(reuse)) = served.outcome else { panic!("done: {served:?}") };
    let says_close = skein_lib::bytes::find(&served.sent, b"\r\nConnection: close\r\n").is_some();
    (reuse, says_close)
}

#[test]
fn a_connection_carries_one_exchange_after_another() {
    let mut machine = Machine::new(LIMITS);
    for _ in 0..4_u8 {
        let mut request = Vec::from(POST);
        request.extend_from_slice(b"12345678");
        let drive = Drive { read: Read::Fill(4), when: When::AfterBody, reply: b"ok" };
        let served = serve(&mut machine, &request, response(200, Body::Length(2)), drive);
        assert_eq!(served.body, b"12345678");
        assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)));
        assert_eq!(machine.server.waiting(), Waiting::Next);
    }
}

#[test]
fn a_connection_persists_by_each_rule() {
    let ok = response(204, Body::None);
    assert_eq!(reuse_of(get(), ok.clone()), (Reuse::Keep, false), "HTTP/1.1");
    let close = b"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n";
    assert_eq!(reuse_of(close, ok.clone()), (Reuse::Close, true), "the request asks to close");
    assert_eq!(reuse_of(get(), Response { close: true, ..ok.clone() }), (Reuse::Close, true), "the response asks");
    assert_eq!(reuse_of(b"GET / HTTP/1.0\r\n\r\n", ok.clone()), (Reuse::Close, true), "HTTP/1.0");
    let kept = b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n";
    let drive = Drive { read: Read::Fill(8), when: When::AfterBody, reply: b"" };
    let served = serve(&mut Machine::new(LIMITS), kept, ok, drive);
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Keep)), "HTTP/1.0 kept alive");
    assert!(served.sent.ends_with(b"Connection: keep-alive\r\n\r\n"), "{}", served.sent.escape_ascii());
}

#[test]
fn a_response_given_before_the_body_is_read_gives_up_the_rest_on_a_connection_not_used_again() {
    let mut request = Vec::from(POST);
    request.extend_from_slice(b"12345678");
    let drive = Drive { read: Read::Fill(2), when: When::First, reply: b"no" };
    let served = serve(&mut Machine::new(LIMITS), &request, response(413, Body::Length(2)), drive);
    assert!(served.sent.starts_with(b"HTTP/1.1 413 Content Too Large\r\n"));
    assert!(
        served.sent.ends_with(b"Content-Length: 2\r\nConnection: close\r\n\r\nno"),
        "{}",
        served.sent.escape_ascii()
    );
    assert_eq!(served.outcome, Some(Event::Done(Reuse::Close)));
    // The side above reading it, its stream is told; a read outstanding is
    // withdrawn, and its answer dropped.
    let mut machine = Machine::new(LIMITS);
    machine.call(POST);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(2), room: 0 }));
    let (events, requests) = machine.bytes(b"12");
    assert_eq!(events, [Event::Body(Up::Bytes(boxed(b"12")))]);
    assert!(requests.is_empty());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(2), room: 0 }));
    let (events, requests) = machine.down(Request::Respond(response(204, Body::None)));
    assert_eq!(events, [Event::Body(Up::Failed(Fault::Other)), Event::Done(Reuse::Close)]);
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[0], Down::Demand { read: Read::Nothing, room: 0 });
    let Down::Send(head) = &requests[1] else { panic!("the head") };
    assert!(head.ends_with(b"Connection: close\r\n\r\n"));
    let (events, requests) = machine.bytes(b"34");
    assert!(events.is_empty() && requests.is_empty(), "the answer on its way, dropped");
}

#[test]
fn a_response_given_while_the_body_is_discarded_waits_for_its_end_and_keeps_the_connection() {
    let mut machine = Machine::new(LIMITS);
    machine.call(POST);
    let (_, requests) = machine.down(Request::Discard);
    assert_eq!(requests, [Down::Demand { read: Read::Fill(8), room: 0 }]);
    let (events, requests) = machine.down(Request::Respond(response(204, Body::None)));
    assert!(events.is_empty() && requests.is_empty(), "the head waits for the body's end");
    assert_eq!(machine.server.waiting(), Waiting::Body);
    let (events, requests) = machine.bytes(b"12345678");
    assert_eq!(events, [Event::Done(Reuse::Keep)]);
    let [Down::Send(head)] = &requests[..] else { panic!("the head: {requests:?}") };
    assert!(!head.ends_with(b"Connection: close\r\n\r\n"));
}

#[test]
fn the_stream_ending_mid_body_fails_the_exchange_and_after_it_lets_the_response_go() {
    // Mid-body: the request was cut short.
    let mut machine = Machine::new(LIMITS);
    machine.call(POST);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(8), room: 0 }));
    let (events, requests) = machine.up(Up::End);
    assert_eq!(events, [Event::Body(Up::Failed(Fault::Other)), Event::Failed(Error::Truncated)]);
    assert!(requests.is_empty(), "a read crosses the end");
    assert_eq!(machine.server.waiting(), Waiting::Close);
    // After it: the client only half-closed, and reads the response.
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    let (events, _) = machine.up(Up::End);
    assert!(events.is_empty());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    let (events, requests) = machine.down(Request::Respond(response(204, Body::None)));
    assert_eq!(events, [Event::Done(Reuse::Close)], "no other request follows");
    let [Down::Send(head)] = &requests[..] else { panic!("the head: {requests:?}") };
    assert!(head.ends_with(b"Connection: close\r\n\r\n"));
    // Room still comes after the end, for the body.
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.down(Request::Respond(response(200, Body::Length(2))));
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 2 }));
    let (events, requests) = machine.up(Up::End);
    assert!(events.is_empty() && requests.is_empty(), "room may still come");
    let (events, _) = machine.up(Up::Room);
    assert_eq!(events, [Event::Reply(Up::Room)]);
    let (_, requests) = machine.down(Request::Reply(Down::Send(boxed(b"ok"))));
    assert_eq!(requests, [Down::Send(boxed(b"ok"))]);
    let (events, _) = machine.down(Request::Reply(Down::Finish));
    assert_eq!(events, [Event::Done(Reuse::Close)]);
}

#[test]
fn the_stream_failing_tells_each_stream_still_open_then_fails_the_exchange() {
    let fault = Fault::Reset;
    // Reading the body.
    let mut machine = Machine::new(LIMITS);
    machine.call(POST);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(8), room: 0 }));
    let (events, requests) = machine.up(Up::Failed(fault));
    assert_eq!(events, [Event::Body(Up::Failed(fault)), Event::Failed(Error::Stream(fault))]);
    assert!(requests.is_empty(), "nothing follows a failure: nothing to withdraw");
    let (events, requests) = machine.down(Request::Respond(response(204, Body::None)));
    assert!(events.is_empty() && requests.is_empty(), "on its way: dropped");
    // Writing the reply, the body all read below but its end not yet read
    // above.
    let mut machine = Machine::new(LIMITS);
    machine.call(b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.bytes(b"a");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    machine.bytes(b"b");
    machine.down(Request::Respond(response(200, Body::Chunked)));
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 4 }));
    let (events, _) = machine.up(Up::Failed(fault));
    assert_eq!(
        events,
        [Event::Reply(Up::Failed(fault)), Event::Body(Up::Failed(fault)), Event::Failed(Error::Stream(fault))],
        "three at once: UP_MAX_OUT"
    );
}

#[test]
fn a_close_in_each_state_withdraws_what_was_demanded_and_answers_closed() {
    let closed = |machine: &mut Machine, withdraws: bool| {
        let (events, requests) = machine.down(Request::Close);
        assert_eq!(events, [Event::Closed]);
        let withdrawal = [Down::Demand { read: Read::Nothing, room: 0 }];
        assert_eq!(requests.as_slice(), if withdraws { &withdrawal[..] } else { &[][..] });
        assert_eq!(machine.server.waiting(), Waiting::Nothing);
        let (events, requests) = machine.up(Up::Bytes(boxed(b"late")));
        assert!(events.is_empty() && requests.is_empty(), "late answers are dropped");
    };
    // Idle.
    closed(&mut Machine::new(LIMITS), false);
    // Setting room aside.
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Next);
    closed(&mut machine, true);
    // Reading the head.
    let mut machine = Machine::new(LIMITS);
    machine.ready();
    machine.bytes(b"GET / HTTP/1.1\r\n");
    closed(&mut machine, true);
    // A call up, nothing demanded.
    let mut machine = Machine::new(LIMITS);
    machine.call(POST);
    closed(&mut machine, false);
    // Reading the body.
    let mut machine = Machine::new(LIMITS);
    machine.call(POST);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(3), room: 0 }));
    closed(&mut machine, true);
    // Waiting for room for the reply.
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Respond(response(200, Body::Length(3))));
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 3 }));
    closed(&mut machine, true);
    // Spent.
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::End);
    machine.down(Request::Next);
    closed(&mut machine, false);
}

#[test]
fn what_the_server_waits_for_follows_its_state() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.server.waiting(), Waiting::Next);
    assert_eq!(machine.down(Request::Next).1, [ASIDE]);
    assert_eq!(machine.server.waiting(), Waiting::Room);
    assert_eq!(machine.up(Up::Room).1, [FIRST_LINE]);
    assert_eq!(machine.server.waiting(), Waiting::Request);
    machine.bytes(b"POST / HTTP/1.1\r\n");
    assert_eq!(machine.server.waiting(), Waiting::Request);
    machine.bytes(b"Host: h\r\n");
    machine.bytes(b"Content-Length: 1\r\n");
    machine.bytes(b"\r\n");
    assert_eq!(machine.server.waiting(), Waiting::Above);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    assert_eq!(machine.server.waiting(), Waiting::Body);
    machine.bytes(b"x");
    assert_eq!(machine.server.waiting(), Waiting::Above);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    assert_eq!(machine.server.waiting(), Waiting::Above, "for the response");
    machine.down(Request::Respond(response(200, Body::Length(1))));
    assert_eq!(machine.server.waiting(), Waiting::Above, "for the reply");
    machine.down(Request::Reply(Down::Demand { read: Read::Nothing, room: 1 }));
    assert_eq!(machine.server.waiting(), Waiting::Room);
    machine.up(Up::Room);
    assert_eq!(machine.server.waiting(), Waiting::Above);
    machine.down(Request::Reply(Down::Send(boxed(b"y"))));
    machine.down(Request::Reply(Down::Finish));
    assert_eq!(machine.server.waiting(), Waiting::Next);
    machine.down(Request::Close);
    assert_eq!(machine.server.waiting(), Waiting::Nothing);
}

#[test]
fn a_withdrawal_or_a_discard_that_crosses_the_body_s_end_is_dropped() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    let (events, requests) = machine.down(Request::Body(Down::Demand { read: Read::Nothing, room: 0 }));
    assert!(events.is_empty() && requests.is_empty());
    let (events, requests) = machine.down(Request::Discard);
    assert!(events.is_empty() && requests.is_empty());
    let (events, _) = machine.down(Request::Respond(response(204, Body::None)));
    assert_eq!(events, [Event::Done(Reuse::Keep)]);
    let (events, requests) = machine.down(Request::Discard);
    assert!(events.is_empty() && requests.is_empty(), "after Done too");
}

#[test]
#[should_panic(expected = "a Next while a request is in progress")]
fn one_request_at_a_time() {
    let mut machine = Machine::new(LIMITS);
    machine.call(get());
    machine.down(Request::Next);
}

#[test]
#[should_panic(expected = "a Next on a connection not to be used again")]
fn no_next_on_a_spent_connection() {
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::End);
    machine.down(Request::Next);
    machine.down(Request::Next);
}

#[test]
#[should_panic(expected = "a Respond with no call in progress")]
fn no_response_before_a_call() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Respond(response(200, Body::None)));
}

#[test]
#[should_panic(expected = "a Close after Closed")]
fn no_close_after_closed() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Close);
    machine.down(Request::Close);
}
