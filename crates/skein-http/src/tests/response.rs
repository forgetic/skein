//! The response head (http.md, 3.2): the status line, the fields and their
//! folds, every limit at and past its edge, interim responses, and the
//! framing and persistence a head decides.

#![expect(clippy::disallowed_types, reason = "a test builds what it sends in a Vec")]

use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Down, Read, Up};

use super::{Drive, Exchanged, LIMITS, Machine, call, exchange, get};
use crate::client::{Body, Call, Error, Event, Framing, Limits, Method, Response, Reuse, Version};

const DRIVE: Drive<'static> = Drive { upload: None, read: Read::Fill(1), respond_first: false };

fn respond_to(call: Call, response: &[u8], limits: Limits) -> Exchanged {
    exchange(&mut Machine::new(limits), call, response, DRIVE)
}

fn respond(response: &[u8]) -> Exchanged {
    respond_to(get(), response, LIMITS)
}

/// The final response's head, of a response read whole.
fn head(response: &[u8]) -> Response {
    let exchanged = respond(response);
    match exchanged.outcome {
        Some(Event::Done(_)) => {}
        ref other => panic!("{} ends with {other:?}", response.escape_ascii()),
    }
    exchanged.response.expect("a response")
}

fn failure_with(response: &[u8], limits: Limits) -> Error {
    match respond_to(get(), response, limits).outcome {
        Some(Event::Failed(error)) => error,
        other => panic!("{} ends with {other:?}", response.escape_ascii()),
    }
}

fn failure(response: &[u8]) -> Error {
    failure_with(response, LIMITS)
}

#[test]
fn a_status_line_gives_its_version_and_code_and_the_reason_is_dropped() {
    let response = head(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    assert_eq!((response.version, response.status), (Version::Http11, 200));
    let response = head(b"HTTP/1.0 404 Not Found\r\nContent-Length: 0\r\n\r\n");
    assert_eq!((response.version, response.status), (Version::Http10, 404));
    assert_eq!(head(b"HTTP/1.1 503\r\nContent-Length: 0\r\n\r\n").status, 503, "no reason, no space");
    assert_eq!(head(b"HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n").status, 200, "an empty reason");
    assert_eq!(head(b"HTTP/1.1 201 \xe2\x9c\x93 made\t!\r\nContent-Length: 0\r\n\r\n").status, 201);
    assert_eq!(head(b"HTTP/1.9 200 OK\r\nContent-Length: 0\r\n\r\n").version, Version::Http11, "a later minor");
}

#[test]
fn a_status_line_that_is_not_one_is_refused() {
    for line in [
        &b"HTTP/1.1 20 OK"[..],
        b"HTTP/1.1 2000 OK",
        b"HTTP/1.1 600 Nope",
        b"HTTP/1.1 099 Low",
        b"HTTP/1.1  200 OK",
        b"HTTP/1.1 200OK",
        b"HTTP/1.1\t200 OK",
        b"http/1.1 200 OK",
        b"HTTP/1 200 OK",
        b"HTTP/1.1 2x0 OK",
        b"HTTP/1.1 200 O\x01K",
        b"ICY 200 OK",
        b"",
        b"\xef\xbb\xbfHTTP/1.1 200 OK",
    ] {
        let mut response = Vec::from(line);
        response.extend_from_slice(b"\r\nContent-Length: 0\r\n\r\n");
        assert_eq!(failure(&response), Error::Status, "{}", line.escape_ascii());
    }
    assert_eq!(failure(b"HTTP/2.0 200 OK\r\n\r\n"), Error::Version);
    assert_eq!(failure(b"HTTP/0.9 200 OK\r\n\r\n"), Error::Version);
}

#[test]
fn fields_are_kept_in_order_trimmed_and_found_without_regard_to_case() {
    let response = head(b"HTTP/1.1 200 OK\r\nContent-Type:  text/plain \r\nX-Empty:\r\nx-dup: a\r\nX-Dup: b\r\nContent-Length: 0\r\n\r\n");
    let mut fields: Vec<(&[u8], &[u8])> = Vec::new();
    for header in &response.headers {
        fields.push((&header.name, &header.value));
    }
    assert_eq!(
        fields,
        [
            (&b"Content-Type"[..], &b"text/plain"[..]),
            (b"X-Empty", b""),
            (b"x-dup", b"a"),
            (b"X-Dup", b"b"),
            (b"Content-Length", b"0"),
        ]
    );
    assert_eq!(response.header(b"content-type"), Some(&b"text/plain"[..]));
    assert_eq!(response.header(b"X-DUP"), Some(&b"a"[..]), "the first of two");
    assert_eq!(response.header(b"missing"), None);
}

#[test]
fn a_head_may_end_its_lines_with_lf_alone() {
    let response = head(b"HTTP/1.1 200 OK\nContent-Length: 2\n\nhi");
    assert_eq!(response.header(b"content-length"), Some(&b"2"[..]));
    assert_eq!(respond(b"HTTP/1.1 200 OK\r\nContent-Length: 2\n\r\nhi").body, b"hi", "mixed");
}

#[test]
fn a_fold_joins_a_value_with_one_space() {
    let response = head(b"HTTP/1.1 200 OK\r\nX-Long: one \r\n  two\r\n\tthree\r\n \r\nContent-Length: 0\r\n\r\n");
    assert_eq!(response.header(b"x-long"), Some(&b"one two three"[..]));
    let response = head(b"HTTP/1.1 200 OK\r\nX-Empty:\r\n then\r\nContent-Length: 0\r\n\r\n");
    assert_eq!(response.header(b"x-empty"), Some(&b"then"[..]));
    assert_eq!(failure(b"HTTP/1.1 200 OK\r\n folded\r\n\r\n"), Error::Header, "a fold before any field");
}

#[test]
fn a_line_that_is_not_a_field_is_refused() {
    for line in [
        &b"NoColon"[..],
        b": no name",
        b"Bad Name: v",
        b"Name : v",
        b"Na\x01me: v",
        b"Name: a\x00b",
        b"Name: a\rb",
        b"Name: a\x7fb",
        b"Name: \x1b[31m",
    ] {
        let mut response = Vec::from(&b"HTTP/1.1 200 OK\r\n"[..]);
        response.extend_from_slice(line);
        response.extend_from_slice(b"\r\nContent-Length: 0\r\n\r\n");
        assert_eq!(failure(&response), Error::Header, "{}", line.escape_ascii());
    }
}

#[test]
fn the_head_limit_counts_every_byte_of_every_head_and_a_head_at_it_is_read() {
    let response = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let heads = u32::try_from(response.len()).unwrap();
    let limits = Limits { head: heads, ..LIMITS };
    let exchanged = respond_to(get(), response, limits);
    assert_eq!(exchanged.outcome, Some(Event::Done(Reuse::Keep)), "heads exactly at the limit");
    let limits = Limits { head: heads - 1, ..LIMITS };
    assert_eq!(failure_with(response, limits), Error::HeadTooLong, "the interim head counts too");
    assert_eq!(
        failure_with(b"HTTP/1.1 200 OK\r\nX: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\r\n", Limits { head: 40, ..LIMITS }),
        Error::HeadTooLong
    );
}

#[test]
fn an_interim_head_that_ends_at_the_limit_leaves_none_for_the_final_one() {
    let interim = b"HTTP/1.1 100 Continue\r\n\r\n";
    let response = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let limits = Limits { head: u32::try_from(interim.len()).unwrap(), ..LIMITS };
    assert_eq!(failure_with(response, limits), Error::HeadTooLong);
}

#[test]
fn a_head_that_never_ends_fails_at_the_limit_or_is_cut_by_the_end_of_the_stream() {
    let mut endless = Vec::from(&b"HTTP/1.1 200 OK\r\n"[..]);
    for _ in 0..40_u32 {
        endless.extend_from_slice(b"X: y\r\n");
    }
    assert_eq!(failure_with(&endless, Limits { headers: 64, ..LIMITS }), Error::HeadTooLong);
    assert_eq!(failure(b"HTTP/1.1 200 OK\r\nX: y\r\n"), Error::Truncated { answered: true });
    assert_eq!(failure(b"HTTP/1.1 200 O"), Error::Truncated { answered: false }, "a status line cut short");
    assert_eq!(failure(b""), Error::Truncated { answered: false }, "no response at all");
}

#[test]
fn a_head_holds_at_most_its_fields() {
    let mut response = Vec::from(&b"HTTP/1.1 200 OK\r\n"[..]);
    for _ in 0..7_u32 {
        response.extend_from_slice(b"X: y\r\n");
    }
    response.extend_from_slice(b"Content-Length: 0\r\n\r\n");
    assert_eq!(head(&response).headers.len(), 8, "at the limit");
    let mut response = Vec::from(&b"HTTP/1.1 200 OK\r\n"[..]);
    for _ in 0..8_u32 {
        response.extend_from_slice(b"X: y\r\n");
    }
    response.extend_from_slice(b"Content-Length: 0\r\n\r\n");
    assert_eq!(failure(&response), Error::TooManyHeaders);
}

#[test]
fn interim_responses_are_skipped_and_a_101_is_refused() {
    let exchanged = respond(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    assert_eq!(exchanged.response.expect("the final response").status, 200);
    assert_eq!(exchanged.body, b"ok");
    assert_eq!(failure(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n"), Error::Upgrade);
}

#[test]
fn the_framing_is_read_from_the_head() {
    let framing = |response: &[u8]| head(response).framing;
    assert_eq!(framing(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"), Framing::Length(3));
    assert_eq!(
        framing(b"HTTP/1.1 200 OK\r\nContent-Length: 3, 3\r\ncontent-length: 03\r\n\r\nabc"),
        Framing::Length(3)
    );
    assert_eq!(framing(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: Chunked\r\n\r\n0\r\n\r\n"), Framing::Chunked);
    assert_eq!(framing(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: , chunked,\r\n\r\n0\r\n\r\n"), Framing::Chunked);
    assert_eq!(framing(b"HTTP/1.1 200 OK\r\n\r\nto the end"), Framing::UntilEnd);
    assert_eq!(framing(b"HTTP/1.1 204 No Content\r\nContent-Length: 9\r\n\r\n"), Framing::Empty);
    assert_eq!(framing(b"HTTP/1.1 304 Not Modified\r\nTransfer-Encoding: chunked\r\n\r\n"), Framing::Empty);
    let response =
        respond_to(call(Method::Head, Body::None), b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n", LIMITS);
    assert_eq!(response.response.expect("a head").framing, Framing::Empty, "a response to HEAD has no body");
    assert_eq!(response.outcome, Some(Event::Done(Reuse::Keep)));
}

#[test]
fn conflicting_or_unknown_framing_is_refused() {
    for fields in [
        &b"Transfer-Encoding: chunked\r\nContent-Length: 5"[..],
        b"Content-Length: 5\r\nTransfer-Encoding: chunked",
        b"Content-Length: 5\r\nContent-Length: 6",
        b"Content-Length: 5, 6",
        b"Content-Length: -1",
        b"Content-Length: 0x10",
        b"Content-Length: 1 0",
        b"Content-Length:",
        b"Content-Length: 18446744073709551616",
        b"Transfer-Encoding: gzip, chunked",
        b"Transfer-Encoding: chunked, chunked",
        b"Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked",
        b"Transfer-Encoding: identity",
        b"Transfer-Encoding:",
    ] {
        let mut response = Vec::from(&b"HTTP/1.1 200 OK\r\n"[..]);
        response.extend_from_slice(fields);
        response.extend_from_slice(b"\r\n\r\n");
        assert_eq!(failure(&response), Error::Framing, "{}", fields.escape_ascii());
    }
    assert_eq!(
        failure(b"HTTP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"),
        Error::Framing,
        "chunked in 1.0"
    );
}

#[test]
fn a_connection_persists_by_the_rules_of_its_version() {
    let reuse = |response: &[u8]| respond(response).outcome;
    let done = |reuse| Some(Event::Done(reuse));
    assert_eq!(reuse(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"), done(Reuse::Keep));
    assert_eq!(reuse(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"), done(Reuse::Close));
    assert_eq!(
        reuse(b"HTTP/1.1 200 OK\r\nConnection: Upgrade, CLOSE\r\nContent-Length: 0\r\n\r\n"),
        done(Reuse::Close)
    );
    assert_eq!(reuse(b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n"), done(Reuse::Close));
    assert_eq!(reuse(b"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"), done(Reuse::Keep));
    assert_eq!(
        reuse(b"HTTP/1.0 200 OK\r\nConnection: keep-alive, close\r\nContent-Length: 0\r\n\r\n"),
        done(Reuse::Close)
    );
    assert_eq!(reuse(b"HTTP/1.1 200 OK\r\n\r\nall of it"), done(Reuse::Close), "a body to the end of the stream");
    let closing = respond_to(Call { close: true, ..get() }, b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", LIMITS);
    assert_eq!(closing.outcome, done(Reuse::Close), "the call asked to close");
}

#[test]
fn the_response_goes_up_when_its_head_is_whole_and_not_before() {
    let mut machine = Machine::new(LIMITS);
    let (_, demand) = machine.called(get());
    assert_eq!(demand, Some(Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 256 }, room: 0 }));
    let (events, requests) = machine.bytes(b"HTTP/1.1 200 OK\r\n");
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Demand { read: Read::Scan { until: Delimiter::LF, max: 239 }, room: 0 }]);
    let (events, requests) = machine.bytes(b"Content-Length: 2\r\n");
    assert!(events.is_empty() && requests.len() == 1);
    let (events, requests) = machine.bytes(b"\r\n");
    assert!(requests.is_empty(), "nothing is read until the body is demanded");
    let [Event::Response(response)] = events.as_slice() else { panic!("{events:?}") };
    assert_eq!(response.framing, Framing::Length(2));
    // An end with nothing demanded comes only once nothing is held below
    // (lib.md, 7): the body's two bytes never came.
    let (events, _) = machine.up(Up::End);
    assert_eq!(
        events,
        [Event::Body(Up::Failed(skein_lib::stream::Fault::Other)), Event::Failed(Error::Truncated { answered: true })]
    );
}
