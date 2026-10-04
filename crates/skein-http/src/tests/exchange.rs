//! An exchange's life (http.md, 3): the upload, a response that comes
//! first, reuse, the stream ending or failing in each state, and a close in
//! each state.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]

use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};

use super::{Drive, LIMITS, Machine, boxed, call, exchange, get};
use crate::client::{Body, Error, Event, Method, Request, Reuse, Waiting};

const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
const LINE: Read = Read::Fill(1);
const HEAD_LINE: Read = Read::Scan { until: Delimiter::LF, max: 256 };

fn post(len: u64) -> crate::client::Call {
    call(Method::Post, Body::Length(len))
}

#[test]
fn a_body_is_uploaded_in_the_pieces_the_side_above_sends_then_the_response_read() {
    let mut body = Vec::new();
    for byte in 0..40_u8 {
        body.push(byte);
    }
    let mut machine = Machine::new(LIMITS);
    let drive = Drive { upload: Some(&body), read: LINE, respond_first: false };
    let exchanged = exchange(&mut machine, post(40), OK, drive);
    let head = b"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 40\r\n\r\n";
    assert_eq!(&exchanged.sent[..head.len()], head);
    assert_eq!(&exchanged.sent[head.len()..], &body[..], "the body after the head, whole");
    assert!(exchanged.upload.is_empty(), "the upload finished");
    assert_eq!(exchanged.body, b"ok");
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn an_empty_body_is_finished_at_once() {
    let mut machine = Machine::new(LIMITS);
    let drive = Drive { upload: Some(b""), read: LINE, respond_first: false };
    let exchanged = exchange(&mut machine, post(0), OK, drive);
    assert!(exchanged.sent.ends_with(b"Content-Length: 0\r\n\r\n"));
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn nothing_is_read_while_the_upload_waits_for_the_side_above() {
    let mut machine = Machine::new(LIMITS);
    let (events, requests) = machine.down(Request::Call(post(4)));
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 57 }]);
    let (_, requests) = machine.up(Up::Room);
    assert_eq!(requests.len(), 1, "the head, and no demand alone for the response: {requests:?}");
    assert_eq!(machine.client.waiting(), Waiting::Above);
    let (_, requests) = machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: 4 }));
    assert_eq!(requests, [Down::Demand { read: HEAD_LINE, room: 4 }], "room, with the response's first line");
    assert_eq!(machine.client.waiting(), Waiting::Room);
    let (events, requests) = machine.up(Up::Room);
    assert_eq!(events, [Event::Upload(Up::Room)]);
    assert!(requests.is_empty());
    let (_, requests) = machine.down(Request::Upload(Down::Send(boxed(b"body"))));
    assert_eq!(requests, [Down::Send(boxed(b"body"))]);
    assert_eq!(machine.client.waiting(), Waiting::Above, "for the Finish");
    let (_, requests) = machine.down(Request::Upload(Down::Finish));
    assert_eq!(requests, [Down::Demand { read: HEAD_LINE, room: 0 }]);
    assert_eq!(machine.client.waiting(), Waiting::Response);
}

#[test]
fn room_demanded_before_the_head_is_sent_is_asked_for_after_it() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(4)));
    let (events, requests) = machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: 4 }));
    assert!(events.is_empty() && requests.is_empty(), "the head's demand is outstanding");
    let (_, requests) = machine.up(Up::Room);
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1], Down::Demand { read: HEAD_LINE, room: 4 });
}

#[test]
fn a_response_that_comes_first_stops_the_upload_and_ends_the_exchange() {
    let body = [b'x'; 64];
    let refusal = b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 4\r\n\r\nbig!";
    let mut machine = Machine::new(LIMITS);
    let drive = Drive { upload: Some(&body), read: LINE, respond_first: true };
    let exchanged = exchange(&mut machine, post(64), refusal, drive);
    assert_eq!(exchanged.response.expect("the response").status, 413);
    assert_eq!(exchanged.upload, [Up::Failed(Fault::Other)], "the upload stops");
    assert_eq!(exchanged.body, b"big!");
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Close)), "on a connection not used again");
    assert!(exchanged.sent.len() < 56 + 64, "the body was not all sent");
}

#[test]
fn an_interim_response_mid_upload_lets_it_go_on() {
    let body = [b'y'; 40];
    let response = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n";
    let mut machine = Machine::new(LIMITS);
    let mut machine_too = Machine::new(LIMITS);
    let drive = Drive { upload: Some(&body), read: LINE, respond_first: true };
    let exchanged = exchange(&mut machine, post(40), response, drive);
    // The final response comes before the body is all sent, here too: the
    // world sends it at once. Read after it, the upload goes whole.
    assert_eq!(exchanged.upload, [Up::Failed(Fault::Other)]);
    let drive = Drive { respond_first: false, ..drive };
    let exchanged = exchange(&mut machine_too, post(40), response, drive);
    assert!(exchanged.upload.is_empty());
    assert!(exchanged.sent.ends_with(&body));
    assert_eq!(exchanged.response.expect("the final response").status, 201);
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn a_connection_carries_one_exchange_after_another() {
    let mut machine = Machine::new(LIMITS);
    for n in 0..5_u8 {
        let body = [b'a' + n; 3];
        let mut response = Vec::from(&b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n"[..]);
        response.extend_from_slice(&body);
        let drive = Drive { upload: None, read: Read::Fill(3), respond_first: false };
        let exchanged = exchange(&mut machine, get(), &response, drive);
        assert_eq!(exchanged.body, body);
        assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)));
        assert_eq!(machine.client.waiting(), Waiting::Call);
    }
}

#[test]
fn the_stream_ending_before_the_request_is_sent_is_closed_and_after_it_truncated() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(get()));
    let (events, requests) = machine.up(Up::End);
    assert_eq!(events, [Event::Failed(Error::Closed)], "nothing was sent");
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 0 }], "the room no longer wanted");
    assert_eq!(machine.client.waiting(), Waiting::Close);
    let (events, requests) = machine.up(Up::Room);
    assert!(events.is_empty() && requests.is_empty(), "room on its way, dropped");

    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    let (events, requests) = machine.up(Up::End);
    assert_eq!(events, [Event::Failed(Error::Truncated)]);
    assert!(requests.is_empty(), "a read crosses the end: nothing to withdraw");
}

#[test]
fn the_stream_ending_mid_upload_fails_the_upload_and_the_exchange() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(8)));
    machine.up(Up::Room);
    machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: 8 }));
    let (events, requests) = machine.up(Up::End);
    assert_eq!(events, [Event::Upload(Up::Failed(Fault::Invalid)), Event::Failed(Error::Truncated)]);
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 0 }]);
}

#[test]
fn the_stream_failing_in_each_state_fails_the_exchange_with_its_fault() {
    let fault = Fault::Reset;
    // Writing the head.
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(get()));
    let (events, requests) = machine.up(Up::Failed(fault));
    assert_eq!(events, [Event::Failed(Error::Stream(fault))]);
    assert!(requests.is_empty(), "nothing follows a failure: nothing to withdraw");
    // Uploading.
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(8)));
    machine.up(Up::Room);
    let (events, _) = machine.up(Up::Failed(fault));
    assert_eq!(events, [Event::Upload(Up::Failed(fault)), Event::Failed(Error::Stream(fault))]);
    let (events, requests) = machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: 8 }));
    assert!(events.is_empty() && requests.is_empty(), "on its way: dropped");
    // Reading the head.
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    let (events, _) = machine.up(Up::Failed(fault));
    assert_eq!(events, [Event::Failed(Error::Stream(fault))]);
}

#[test]
fn a_close_in_each_state_withdraws_what_was_demanded_and_answers_closed() {
    let closed = |machine: &mut Machine, withdraws: bool| {
        let (events, requests) = machine.down(Request::Close);
        assert_eq!(events, [Event::Closed]);
        let withdrawal = [Down::Demand { read: Read::Nothing, room: 0 }];
        assert_eq!(requests.as_slice(), if withdraws { &withdrawal[..] } else { &[][..] });
        assert_eq!(machine.client.waiting(), Waiting::Nothing);
        let (events, requests) = machine.up(Up::Bytes(boxed(b"late")));
        assert!(events.is_empty() && requests.is_empty(), "late answers are dropped");
    };
    // Idle.
    closed(&mut Machine::new(LIMITS), false);
    // Writing the head.
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(get()));
    closed(&mut machine, true);
    // Waiting for the side above's upload.
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(8)));
    machine.up(Up::Room);
    closed(&mut machine, false);
    // Reading the head.
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    closed(&mut machine, true);
    // The response up, its body not demanded.
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"\r\n");
    closed(&mut machine, false);
    // Reading the body.
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    machine.bytes(b"\r\n");
    machine.down(Request::Body(Down::Demand { read: Read::Fill(3), room: 0 }));
    closed(&mut machine, true);
    // Spent.
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::End);
    machine.down(Request::Call(get()));
    closed(&mut machine, false);
}

#[test]
fn what_the_client_waits_for_follows_its_state() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.client.waiting(), Waiting::Call);
    machine.down(Request::Call(get()));
    assert_eq!(machine.client.waiting(), Waiting::Room);
    machine.up(Up::Room);
    assert_eq!(machine.client.waiting(), Waiting::Response);
    machine.bytes(b"HTTP/1.1 200 OK\r\n");
    assert_eq!(machine.client.waiting(), Waiting::Response);
    machine.bytes(b"Content-Length: 1\r\n");
    machine.bytes(b"\r\n");
    assert_eq!(machine.client.waiting(), Waiting::Above);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    assert_eq!(machine.client.waiting(), Waiting::Body);
    machine.bytes(b"x");
    assert_eq!(machine.client.waiting(), Waiting::Above);
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    assert_eq!(machine.client.waiting(), Waiting::Call);
    machine.down(Request::Close);
    assert_eq!(machine.client.waiting(), Waiting::Nothing);
}

#[test]
#[should_panic(expected = "a Call while an exchange is in progress")]
fn one_exchange_at_a_time() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(get()));
    machine.down(Request::Call(get()));
}

#[test]
#[should_panic(expected = "an upload for a call without a body")]
fn no_upload_for_a_call_without_a_body() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(get()));
    machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: 4 }));
}

#[test]
#[should_panic(expected = "a Send within the room granted")]
fn no_send_without_room() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(4)));
    machine.down(Request::Upload(Down::Send(boxed(b"no"))));
}

#[test]
#[should_panic(expected = "Finish once the call's length is sent")]
fn no_finish_short_of_the_length() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(4)));
    machine.down(Request::Upload(Down::Finish));
}

#[test]
#[should_panic(expected = "no room past Limits::send")]
fn no_room_past_the_limit() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(4)));
    machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: 99 }));
}

#[test]
#[should_panic(expected = "no more than the call's length")]
fn no_body_past_the_length() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Call(post(2)));
    machine.down(Request::Upload(Down::Demand { read: Read::Nothing, room: 4 }));
    machine.up(Up::Room);
    machine.up(Up::Room);
    machine.down(Request::Upload(Down::Send(boxed(b"abc"))));
}

#[test]
#[should_panic(expected = "a body demand before the response")]
fn no_body_demand_before_the_response() {
    let mut machine = Machine::new(LIMITS);
    machine.called(get());
    machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
}

#[test]
#[should_panic(expected = "a Call on a connection told not to be used again")]
fn no_call_on_a_spent_connection() {
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::End);
    machine.down(Request::Call(get()));
    machine.down(Request::Call(get()));
}

#[test]
#[should_panic(expected = "a Close after Closed")]
fn no_close_after_closed() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Close);
    machine.down(Request::Close);
}
