//! The request head (http.md, 3.1): written sized, its framing the
//! client's own, and every refusal before anything is sent.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};

use super::{LIMITS, Machine, boxed, call, get, header};
use crate::client::{Body, Call, Error, Event, Limits, Method, Refusal, Request, Waiting};

/// The head a call is written as, sent once room is granted.
fn written(call: Call) -> Box<[u8]> {
    let mut machine = Machine::new(LIMITS);
    machine.called(call).0
}

/// Why a call is refused, and that the client is still idle after it.
fn refused(call: Call, limits: Limits) -> Refusal {
    let mut machine = Machine::new(limits);
    let (events, requests) = machine.down(Request::Call(call));
    assert!(requests.is_empty(), "a refused call writes nothing: {requests:?}");
    assert_eq!(machine.client.waiting(), Waiting::Call, "the connection is as it was");
    match events.as_slice() {
        [Event::Failed(Error::Refused(refusal))] => *refusal,
        other => panic!("refused, not {other:?}"),
    }
}

#[test]
fn a_get_is_its_request_line_its_fields_and_a_blank_line() {
    assert_eq!(&*written(get()), b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n");
}

#[test]
fn every_method_is_spelt_as_the_standard_spells_it() {
    let methods: [(Method, &[u8]); 7] = [
        (Method::Get, b"GET"),
        (Method::Head, b"HEAD"),
        (Method::Post, b"POST"),
        (Method::Put, b"PUT"),
        (Method::Patch, b"PATCH"),
        (Method::Delete, b"DELETE"),
        (Method::Options, b"OPTIONS"),
    ];
    for (method, name) in methods {
        let head = written(call(method, Body::None));
        assert!(head.starts_with(name) && head[name.len()] == b' ', "{method:?}");
    }
}

#[test]
fn a_body_is_announced_by_its_length_and_none_by_zero_where_a_body_means_something() {
    let head = written(call(Method::Post, Body::Length(1234)));
    assert_eq!(&*head, b"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 1234\r\n\r\n");
    let head = written(call(Method::Put, Body::None));
    assert_eq!(&*head, b"PUT / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 0\r\n\r\n");
    let head = written(call(Method::Delete, Body::None));
    assert_eq!(&*head, b"DELETE / HTTP/1.1\r\nHost: example.com\r\n\r\n", "nothing where it means nothing");
    let head = written(call(Method::Get, Body::Length(0)));
    assert_eq!(&*head, b"GET / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 0\r\n\r\n");
}

#[test]
fn a_call_that_closes_says_so() {
    let head = written(Call { close: true, ..get() });
    assert_eq!(&*head, b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n");
}

#[test]
fn fields_are_written_in_order_as_given() {
    let fields = Box::new([
        header(b"Host", b"api.example"),
        header(b"x-api-key", b"k\xc3\xa9y"),
        header(b"Accept", b"text/event-stream"),
        header(b"Empty", b""),
    ]);
    let head = written(Call { target: boxed(b"/v1/messages?beta=true"), headers: fields, ..get() });
    assert_eq!(
        &*head,
        &b"GET /v1/messages?beta=true HTTP/1.1\r\nHost: api.example\r\nx-api-key: k\xc3\xa9y\r\n\
           Accept: text/event-stream\r\nEmpty: \r\n\r\n"[..]
    );
}

#[test]
fn the_room_asked_for_is_the_head_and_nothing_is_read_before_it_is_sent() {
    let mut machine = Machine::new(LIMITS);
    let (events, requests) = machine.down(Request::Call(get()));
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 37 }]);
    assert_eq!(machine.client.waiting(), Waiting::Room);
    let (events, requests) = machine.up(Up::Room);
    assert!(events.is_empty());
    let [Down::Send(head), next] = requests.as_slice() else { panic!("the head, then a demand: {requests:?}") };
    assert_eq!(head.len(), 37);
    assert_eq!(*next, Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 256 }, room: 0 });
    assert_eq!(machine.client.waiting(), Waiting::Response);
}

#[test]
fn each_thing_a_call_gets_wrong_is_refused_in_order() {
    let target = |target: &[u8]| Call { target: boxed(target), ..get() };
    assert_eq!(refused(target(b""), LIMITS), Refusal::Target);
    assert_eq!(refused(target(b"/a b"), LIMITS), Refusal::Target);
    assert_eq!(refused(target(b"/\x7f"), LIMITS), Refusal::Target);
    assert_eq!(refused(target(b"/caf\xc3\xa9"), LIMITS), Refusal::Target, "a target is ASCII, encoded");
    assert_eq!(refused(target(b"/\r\nX: y"), LIMITS), Refusal::Target, "no header injected through the target");

    let fields = |name: &[u8], value: &[u8]| Call { headers: Box::new([header(name, value)]), ..get() };
    assert_eq!(refused(fields(b"", b"v"), LIMITS), Refusal::Name);
    assert_eq!(refused(fields(b"Bad Name", b"v"), LIMITS), Refusal::Name);
    assert_eq!(refused(fields(b"Name:", b"v"), LIMITS), Refusal::Name);
    assert_eq!(refused(fields(b"X", b"a\r\nInjected: yes"), LIMITS), Refusal::Value);
    assert_eq!(refused(fields(b"X", b"a\nb"), LIMITS), Refusal::Value);
    assert_eq!(refused(fields(b"X", b"a\0b"), LIMITS), Refusal::Value);
    assert_eq!(refused(fields(b"X", b"\x7f"), LIMITS), Refusal::Value);
    assert_eq!(refused(fields(b"Bad Name", b"\n"), LIMITS), Refusal::Name, "the name before the value");
    assert_eq!(refused(fields(b"content-LENGTH", b"5"), LIMITS), Refusal::Reserved);
    assert_eq!(refused(fields(b"Transfer-Encoding", b"chunked"), LIMITS), Refusal::Reserved);
    assert_eq!(refused(fields(b"Connection", b"keep-alive"), LIMITS), Refusal::Reserved);
    assert_eq!(
        refused(Call { target: boxed(b""), ..fields(b"X", b"\n") }, LIMITS),
        Refusal::Target,
        "the target first"
    );
}

#[test]
fn a_head_past_the_limit_is_refused_and_one_at_it_is_written() {
    let head = written(get());
    let at = u32::try_from(head.len()).unwrap();
    let limits = Limits { request: at, ..LIMITS };
    let mut machine = Machine::new(limits);
    assert_eq!(machine.called(get()).0, head, "a head exactly at the limit");
    assert_eq!(refused(get(), Limits { request: at - 1, ..LIMITS }), Refusal::TooLong);
    let long = Call { target: boxed(&[b'a'; 300]), ..get() };
    assert_eq!(refused(long, LIMITS), Refusal::TooLong);
}

#[test]
fn a_refused_call_leaves_the_connection_ready_for_the_next() {
    let mut machine = Machine::new(LIMITS);
    let (events, _) = machine.down(Request::Call(Call { target: boxed(b""), ..get() }));
    assert_eq!(events, [Event::Failed(Error::Refused(Refusal::Target))]);
    let (head, _) = machine.called(get());
    assert_eq!(&*head, b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n");
}

#[test]
fn a_call_on_a_connection_that_ended_or_failed_while_idle_fails_at_once() {
    let nothing = (Vec::new(), Vec::new());
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.up(Up::End), nothing);
    assert_eq!(machine.client.waiting(), Waiting::Close);
    let (events, requests) = machine.down(Request::Call(get()));
    assert_eq!(events, [Event::Failed(Error::Closed)]);
    assert!(requests.is_empty());
    assert_eq!(machine.client.waiting(), Waiting::Close);

    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.up(Up::Failed(Fault::Reset)), nothing);
    assert_eq!(machine.up(Up::End), nothing, "nothing follows a failure");
    let (events, requests) = machine.down(Request::Call(get()));
    assert_eq!(events, [Event::Failed(Error::Stream(Fault::Reset))]);
    assert!(requests.is_empty());
}
