//! The request head (http.md, 5.2): the room set aside before it is read,
//! the request line and the fields, every limit at and past its edge, the
//! framing each head decides, every rejection and its answer, and the
//! stream ending or failing while a head is read.

#![expect(clippy::disallowed_types, reason = "a test builds what it sends in a Vec")]

use alloc::vec::Vec;

use skein_lib::stream::{Down, Fault, Read, Up};

use super::{ASIDE, FIRST_LINE, LIMITS, Machine, get};
use crate::server::{Body, Error, Event, Limits, Rejection, Request, Waiting};
use crate::{Header, Method, Version};

/// What `head` comes to: the call, or the rejection and the answer sent
/// for it.
fn read(head: &[u8], limits: Limits) -> Result<crate::server::Call, (Rejection, Vec<u8>)> {
    let mut machine = Machine::new(limits);
    let (mut events, mut requests) = machine.head(head);
    match events.pop() {
        Some(Event::Call(call)) => {
            assert!(events.is_empty() && requests.is_empty());
            assert_eq!(machine.server.waiting(), Waiting::Above);
            Ok(call)
        }
        Some(Event::Failed(Error::Rejected(rejection))) => {
            assert!(events.is_empty());
            assert_eq!(machine.server.waiting(), Waiting::Close, "a rejection ends the connection");
            let Some(Down::Send(answer)) = requests.pop() else { panic!("the answer sent: {requests:?}") };
            assert!(requests.is_empty());
            assert!(answer.len() <= usize::try_from(limits.response).unwrap(), "within the room set aside");
            Err((rejection, Vec::from(&*answer)))
        }
        other => panic!("{} came to {other:?}", head.escape_ascii()),
    }
}

fn rejected(head: &[u8]) -> Rejection {
    match read(head, LIMITS) {
        Ok(call) => panic!("{} was read: {call:?}", head.escape_ascii()),
        Err((rejection, _)) => rejection,
    }
}

fn framed(head: &[u8]) -> Body {
    match read(head, LIMITS) {
        Ok(call) => call.body,
        Err((rejection, _)) => panic!("{} was rejected: {rejection:?}", head.escape_ascii()),
    }
}

#[test]
fn limits_that_cannot_be_honoured_have_no_worst_case() {
    assert!(crate::server::worst_case(&LIMITS).is_some());
    assert_eq!(crate::server::worst_case(&Limits { head: 1, ..LIMITS }), None);
    assert_eq!(crate::server::worst_case(&Limits { read: 0, ..LIMITS }), None, "a read of nothing");
    assert_eq!(crate::server::worst_case(&Limits { send: 0, ..LIMITS }), None, "room for nothing of a body");
    assert_eq!(crate::server::worst_case(&Limits { response: 80, ..LIMITS }), None, "no room for a 431's answer");
    assert!(crate::server::worst_case(&Limits { response: 87, ..LIMITS }).is_some(), "room for every answer");
    assert_eq!(crate::server::worst_case(&Limits { send: u32::MAX, ..LIMITS }), None, "a chunk's room past a u32");
    let largest = Limits { head: u32::MAX, read: u32::MAX, response: u32::MAX, send: u32::MAX - 12, ..LIMITS };
    assert!(crate::server::worst_case(&largest).is_some(), "the largest limits fit a u64");
}

#[test]
fn nothing_is_read_until_room_for_the_response_is_granted() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.server.waiting(), Waiting::Next);
    let (events, requests) = machine.down(Request::Next);
    assert!(events.is_empty());
    assert_eq!(requests, [ASIDE], "room for the longest response head, and no read");
    assert_eq!(machine.server.waiting(), Waiting::Room);
    let (events, requests) = machine.up(Up::Room);
    assert!(events.is_empty());
    assert_eq!(requests, [FIRST_LINE]);
    assert_eq!(machine.server.waiting(), Waiting::Request);
}

#[test]
fn a_get_is_its_request_line_and_its_fields() {
    let call = read(b"GET /v1/models?limit=2 HTTP/1.1\r\nHost: example.com\r\nAccept:  */* \r\n\r\n", LIMITS).unwrap();
    assert_eq!(call.method, Method::Get);
    assert_eq!(&*call.target, b"/v1/models?limit=2");
    assert_eq!(call.version, Version::Http11);
    let host = Header { name: b"Host".as_slice().into(), value: b"example.com".as_slice().into() };
    let accept = Header { name: b"Accept".as_slice().into(), value: b"*/*".as_slice().into() };
    assert_eq!(&*call.headers, &[host, accept], "in order, values trimmed");
    assert_eq!(call.header(b"accept"), Some(&b"*/*"[..]), "names without regard to case");
    assert_eq!(call.body, Body::None);
}

#[test]
fn lines_may_end_with_lf_alone_and_blank_lines_before_the_request_line_are_skipped() {
    let call = read(b"\r\n\nPOST /x HTTP/1.1\nHost: h\nContent-Length: 3\n\n", LIMITS).unwrap();
    assert_eq!(call.method, Method::Post);
    assert_eq!(call.body, Body::Length(3));
}

#[test]
fn every_method_the_server_knows_is_read_and_another_is_not_implemented() {
    for method in [Method::Get, Method::Head, Method::Post, Method::Put, Method::Patch, Method::Delete, Method::Options]
    {
        let mut head = Vec::from(method.as_bytes());
        head.extend_from_slice(b" / HTTP/1.1\r\nHost: h\r\n\r\n");
        assert_eq!(read(&head, LIMITS).unwrap().method, method);
    }
    let (rejection, answer) = read(b"PROPFIND / HTTP/1.1\r\nHost: h\r\n\r\n", LIMITS).unwrap_err();
    assert_eq!(rejection, Rejection::Method);
    assert_eq!(answer, b"HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    assert_eq!(rejected(b"get / HTTP/1.1\r\n"), Rejection::Method, "methods are compared with regard to case");
}

#[test]
fn a_request_line_that_is_not_one_is_a_bad_request() {
    for line in [
        &b"GET  / HTTP/1.1\r\n"[..],
        b"GET / HTTP/1.1 \r\n",
        b"GET /  HTTP/1.1\r\n",
        b" GET / HTTP/1.1\r\n",
        b"GET\t/ HTTP/1.1\r\n",
        b"GET / http/1.1\r\n",
        b"GET / HTTP/1\r\n",
        b"GET / HTTP/1.10\r\n",
        b"GET / HTTP/x.1\r\n",
        b"GET /\r\n",
        b"GET\r\n",
        b"G@T / HTTP/1.1\r\n",
        b"GET /a\x00b HTTP/1.1\r\n",
        b"GET /caf\xc3\xa9 HTTP/1.1\r\n",
        b"GET /\x7f HTTP/1.1\r\n",
        b"GET / HTTP/1.1\rX\r\n",
    ] {
        assert_eq!(rejected(line), Rejection::RequestLine, "{}", line.escape_ascii());
    }
    let (_, answer) = read(b"GET\r\n", LIMITS).unwrap_err();
    assert_eq!(answer, b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
}

#[test]
fn another_major_version_is_not_supported_and_a_later_minor_is_read_as_one_one() {
    let (rejection, answer) = read(b"GET / HTTP/2.0\r\n", LIMITS).unwrap_err();
    assert_eq!(rejection, Rejection::Version);
    assert_eq!(answer, b"HTTP/1.1 505 HTTP Version Not Supported\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    assert_eq!(rejected(b"GET / HTTP/0.9\r\n"), Rejection::Version);
    assert_eq!(rejected(b"PROPFIND / HTTP/2.0\r\n"), Rejection::Version, "the version before the method");
    assert_eq!(rejected(b"G@T / HTTP/2.0\r\n"), Rejection::RequestLine, "the line's form before the version");
    assert_eq!(read(b"GET / HTTP/1.9\r\nHost: h\r\n\r\n", LIMITS).unwrap().version, Version::Http11);
    assert_eq!(read(b"GET / HTTP/1.0\r\n\r\n", LIMITS).unwrap().version, Version::Http10);
}

#[test]
fn a_request_line_that_does_not_fit_is_a_target_too_long_and_a_field_a_head_too_long() {
    let limits = Limits { head: 32, ..LIMITS };
    let mut long = Vec::from(&b"GET /"[..]);
    long.extend_from_slice(&[b'a'; 40]);
    long.extend_from_slice(b" HTTP/1.1\r\n");
    let (rejection, answer) = read(&long, limits).unwrap_err();
    assert_eq!(rejection, Rejection::TargetTooLong);
    assert_eq!(answer, b"HTTP/1.1 414 URI Too Long\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    // The line at the limit, its blank line still due.
    let mut at = Vec::from(&b"GET /"[..]);
    at.extend_from_slice(&[b'a'; 16]);
    at.extend_from_slice(b" HTTP/1.1\r\n");
    assert_eq!(at.len(), 32);
    assert_eq!(read(&at, limits).unwrap_err().0, Rejection::HeadTooLong);
    // A field that does not fit.
    let (rejection, answer) = read(b"GET / HTTP/1.1\r\nX: aaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n", limits).unwrap_err();
    assert_eq!(rejection, Rejection::HeadTooLong);
    assert_eq!(
        answer,
        b"HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    // A head that ends exactly at the limit.
    let exact = b"GET / HTTP/1.1\r\nHost: abcdefgh\r\n\r\n";
    assert_eq!(exact.len(), 34);
    read(exact, Limits { head: 34, ..LIMITS }).unwrap();
    assert_eq!(read(exact, Limits { head: 33, ..LIMITS }).unwrap_err().0, Rejection::HeadTooLong);
}

#[test]
fn fields_are_counted_and_the_one_past_the_limit_is_too_many() {
    let mut head = Vec::from(&b"GET / HTTP/1.1\r\nHost: h\r\n"[..]);
    for n in 0..7 {
        head.extend_from_slice(b"X-");
        head.push(b'0' + n);
        head.extend_from_slice(b": v\r\n");
    }
    let mut full = head.clone();
    full.extend_from_slice(b"\r\n");
    assert_eq!(read(&full, LIMITS).unwrap().headers.len(), 8);
    head.extend_from_slice(b"X-8: v\r\n\r\n");
    assert_eq!(read(&head, LIMITS).unwrap_err().0, Rejection::TooManyHeaders);
}

#[test]
fn a_field_that_is_not_one_and_an_obsolete_fold_are_bad_requests() {
    for field in [
        &b"Host : h\r\n"[..],
        b"Host\t: h\r\n",
        b": h\r\n",
        b"Host h\r\n",
        b"Ho st: h\r\n",
        b"Host: a\x00b\r\n",
        b"Host: a\rb\r\n",
        b"Host: \x1b[0m\r\n",
        b" folded\r\n",
        b"\tfolded\r\n",
    ] {
        let mut head = Vec::from(&b"GET / HTTP/1.1\r\n"[..]);
        head.extend_from_slice(field);
        head.extend_from_slice(b"\r\n");
        assert_eq!(rejected(&head), Rejection::Header, "{}", field.escape_ascii());
    }
    assert_eq!(rejected(b"GET / HTTP/1.1\r\nHost: h\r\n continued\r\n\r\n"), Rejection::Header);
}

#[test]
fn one_host_is_required_of_http_one_one_and_two_are_refused_of_any() {
    assert_eq!(rejected(b"GET / HTTP/1.1\r\nAccept: */*\r\n\r\n"), Rejection::Host);
    assert_eq!(rejected(b"GET / HTTP/1.1\r\nHost: a\r\nhost: a\r\n\r\n"), Rejection::Host);
    assert_eq!(rejected(b"GET / HTTP/1.0\r\nHost: a\r\nHost: b\r\n\r\n"), Rejection::Host);
    assert!(read(b"GET / HTTP/1.0\r\n\r\n", LIMITS).is_ok(), "HTTP/1.0 needs none");
    assert!(read(b"GET / HTTP/1.1\r\nHost:\r\n\r\n", LIMITS).is_ok(), "an empty one is one");
}

#[test]
fn the_framing_each_head_decides() {
    let head = |fields: &str| {
        let mut head = Vec::from(&b"POST / HTTP/1.1\r\nHost: h\r\n"[..]);
        head.extend_from_slice(fields.as_bytes());
        head.extend_from_slice(b"\r\n");
        head
    };
    assert_eq!(framed(&head("")), Body::None);
    assert_eq!(framed(&head("Content-Length: 42\r\n")), Body::Length(42));
    assert_eq!(framed(&head("Content-Length: 0\r\n")), Body::Length(0));
    assert_eq!(framed(&head("Content-Length: 7, 7\r\ncontent-length: 7\r\n")), Body::Length(7), "one length");
    assert_eq!(framed(&head("Transfer-Encoding: chunked\r\n")), Body::Chunked);
    assert_eq!(framed(&head("transfer-encoding: CHUNKED\r\n")), Body::Chunked);
    assert_eq!(framed(&head("Transfer-Encoding: ,chunked,\r\n")), Body::Chunked, "empty elements skipped");
    let refused = [
        ("Content-Length: 7, 8\r\n", Rejection::Framing),
        ("Content-Length: 7\r\nContent-Length: 8\r\n", Rejection::Framing),
        ("Content-Length: -1\r\n", Rejection::Framing),
        ("Content-Length: 0x10\r\n", Rejection::Framing),
        ("Content-Length:\r\n", Rejection::Framing),
        ("Content-Length: 18446744073709551616\r\n", Rejection::Framing),
        ("Transfer-Encoding: chunked\r\nContent-Length: 5\r\n", Rejection::Framing),
        ("Content-Length: 5\r\nTransfer-Encoding: chunked\r\n", Rejection::Framing),
        ("Transfer-Encoding: gzip\r\n", Rejection::Framing),
        ("Transfer-Encoding: chunked, gzip\r\n", Rejection::Framing),
        ("Transfer-Encoding: chunked, chunked\r\n", Rejection::Framing),
        ("Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n", Rejection::Framing),
        ("Transfer-Encoding:\r\n", Rejection::Framing),
        ("Transfer-Encoding: gzip, chunked\r\n", Rejection::Coding),
        ("Transfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n", Rejection::Coding),
        ("Content-Length: 1025\r\n", Rejection::BodyTooLong),
    ];
    for (fields, rejection) in refused {
        assert_eq!(rejected(&head(fields)), rejection, "{fields}");
    }
    assert_eq!(framed(&head("Content-Length: 1024\r\n")), Body::Length(1024), "a body at the limit");
    let (_, answer) = read(&head("Content-Length: 1025\r\n"), LIMITS).unwrap_err();
    assert_eq!(answer, b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    assert_eq!(rejected(b"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n"), Rejection::Framing);
    assert_eq!(rejected(b"POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"), Rejection::Host, "Host first");
}

#[test]
fn every_rejection_s_answer_fits_the_room_set_aside_and_names_its_status() {
    let mut long = Vec::from(&b"GET /"[..]);
    long.extend_from_slice(&[b'a'; 300]);
    let mut many = Vec::from(&b"GET / HTTP/1.1\r\n"[..]);
    for _ in 0..9_u8 {
        many.extend_from_slice(b"X: v\r\n");
    }
    let mut fat = Vec::from(&b"GET / HTTP/1.1\r\nX: "[..]);
    fat.extend_from_slice(&[b'v'; 300]);
    let heads: [(&[u8], Rejection, &[u8]); 11] = [
        (b"GET\r\n", Rejection::RequestLine, b"400 Bad Request"),
        (&long, Rejection::TargetTooLong, b"414 URI Too Long"),
        (b"GET / HTTP/3.0\r\n", Rejection::Version, b"505 HTTP Version Not Supported"),
        (b"TRACE / HTTP/1.1\r\n", Rejection::Method, b"501 Not Implemented"),
        (b"GET / HTTP/1.1\r\nX : v\r\n", Rejection::Header, b"400 Bad Request"),
        (&fat, Rejection::HeadTooLong, b"431 Request Header Fields Too Large"),
        (&many, Rejection::TooManyHeaders, b"431 Request Header Fields Too Large"),
        (b"GET / HTTP/1.1\r\n\r\n", Rejection::Host, b"400 Bad Request"),
        (b"GET / HTTP/1.1\r\nHost: h\r\nContent-Length: x\r\n\r\n", Rejection::Framing, b"400 Bad Request"),
        (
            b"GET / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: br, chunked\r\n\r\n",
            Rejection::Coding,
            b"501 Not Implemented",
        ),
        (
            b"GET / HTTP/1.1\r\nHost: h\r\nContent-Length: 2000\r\n\r\n",
            Rejection::BodyTooLong,
            b"413 Content Too Large",
        ),
    ];
    for (head, rejection, status) in heads {
        let (rejected, answer) = read(head, LIMITS).unwrap_err();
        assert_eq!(rejected, rejection);
        assert!(answer.starts_with(b"HTTP/1.1 ") && answer[9..].starts_with(status), "{rejection:?}");
        assert_eq!(&answer[9..12], &status[..3]);
        assert_eq!(
            u16::from(answer[9] - b'0') * 100 + u16::from(answer[10] - b'0') * 10 + u16::from(answer[11] - b'0'),
            rejection.status()
        );
        assert!(answer.ends_with(b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), "{rejection:?}");
        assert!(answer.len() <= 87, "{rejection:?}: within the least room set aside");
    }
}

#[test]
fn the_stream_ending_before_a_request_line_is_the_end_and_after_one_a_request_cut_short() {
    // Idle: the next Next hears it.
    let mut machine = Machine::new(LIMITS);
    let (events, requests) = machine.up(Up::End);
    assert!(events.is_empty() && requests.is_empty());
    assert_eq!(machine.server.waiting(), Waiting::Next);
    let (events, requests) = machine.down(Request::Next);
    assert_eq!(events, [Event::Ended]);
    assert!(requests.is_empty(), "nothing demanded of a stream that ended");
    assert_eq!(machine.server.waiting(), Waiting::Close);
    // While the room is set aside: the room is no longer wanted.
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Next);
    let (events, requests) = machine.up(Up::End);
    assert_eq!(events, [Event::Ended]);
    assert_eq!(requests, [Down::Demand { read: Read::Nothing, room: 0 }]);
    let (events, requests) = machine.up(Up::Room);
    assert!(events.is_empty() && requests.is_empty(), "room on its way, dropped");
    // With blank lines only: still no request.
    let mut machine = Machine::new(LIMITS);
    machine.ready();
    machine.bytes(b"\r\n");
    let (events, requests) = machine.up(Up::End);
    assert_eq!(events, [Event::Ended]);
    assert!(requests.is_empty(), "a line's read crosses the end");
    // Mid-head.
    let mut machine = Machine::new(LIMITS);
    machine.ready();
    machine.bytes(b"GET / HTTP/1.1\r\n");
    let (events, _) = machine.up(Up::End);
    assert_eq!(events, [Event::Failed(Error::Truncated)]);
}

#[test]
fn the_stream_failing_while_idle_or_reading_a_head_fails_the_next_request() {
    let fault = Fault::Reset;
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::Failed(fault));
    let (events, requests) = machine.down(Request::Next);
    assert_eq!(events, [Event::Failed(Error::Stream(fault))]);
    assert!(requests.is_empty());
    // After the end, a failure says only that the stream cannot send.
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::End);
    machine.up(Up::Failed(fault));
    assert_eq!(machine.down(Request::Next).0, [Event::Ended]);
    for lines in [&[][..], &[&b"GET / HTTP/1.1\r\n"[..]]] {
        let mut machine = Machine::new(LIMITS);
        machine.ready();
        for line in lines {
            machine.bytes(line);
        }
        let (events, requests) = machine.up(Up::Failed(fault));
        assert_eq!(events, [Event::Failed(Error::Stream(fault))]);
        assert!(requests.is_empty(), "nothing follows a failure: nothing to withdraw");
    }
}

#[test]
fn a_head_after_a_kept_connection_is_read_within_a_budget_of_its_own() {
    let mut machine = Machine::new(Limits { head: 40, ..LIMITS });
    for _ in 0..3_u8 {
        let call = machine.call(get());
        assert_eq!(call.method, Method::Get);
        machine.down(Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
        let response = super::response(204, Body::None);
        let (events, _) = machine.down(Request::Respond(response));
        assert_eq!(events, [Event::Done(crate::server::Reuse::Keep)]);
    }
}
