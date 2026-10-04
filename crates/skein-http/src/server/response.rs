//! The response (http.md, 5.4): what the side above asks for, checked,
//! then measured and written into a box of exactly its length
//! (programming-model.md, 8), with the `Date` it was written at; the fixed
//! answers the server writes itself, a 100 (Continue) and each rejection;
//! and the framing of a chunked body.

use alloc::boxed::Box;

use skein_lib::{Decimal, Wall, Writer};

use super::{Body, Limits, Rejection, Response};
use crate::header::{is_field_byte, is_tchar};
use crate::{Method, Version};

/// Why the server refused a response, before writing anything: a fault of
/// the side above, which it can fix.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// A status outside 200 to 599: the server writes no interim response
    /// but its own 100 (Continue), and switches no protocols.
    Status,
    /// A field's name is not a token.
    Name,
    /// A field's value holds a control character other than a tab: a CR,
    /// an LF or a NUL among them.
    Value,
    /// A field the server writes itself: `Content-Length`,
    /// `Transfer-Encoding`, `Connection` or `Date`.
    Reserved,
    /// A body for a 204, which has none (RFC 9110, 15.3.5), or one in
    /// chunks for a 304, which has none either (15.4.5) but may say the
    /// length a 200 would have had (8.6).
    Body,
    /// The head is longer than [`Limits::response`].
    TooLong,
}

/// How the response's body goes down once its head has.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Framing {
    /// No body is sent: the response to `HEAD`, a 204, a 304, or one
    /// without a body.
    Nothing,
    /// Exactly this many bytes.
    Length(u64),
    /// In chunks, to an HTTP/1.1 client.
    Chunked,
    /// To the end of the stream: a body the side above would chunk, to an
    /// HTTP/1.0 client, which knows no chunks (RFC 9112, 6.1).
    UntilEnd,
}

/// What the server writes for an `Expect: 100-continue` before it reads
/// the body (RFC 9110, 10.1.1).
pub(super) const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// The last chunk of a chunked body, with no trailer section.
pub(super) const LAST_CHUNK: &[u8] = b"0\r\n\r\n";

const VERSION: &[u8] = b"HTTP/1.1 ";
const CONTENT_LENGTH: &[u8] = b"Content-Length: ";
const CHUNKED: &[u8] = b"Transfer-Encoding: chunked\r\n";
const CLOSE: &[u8] = b"Connection: close\r\n";
const KEEP_ALIVE: &[u8] = b"Connection: keep-alive\r\n";
const CRLF: &[u8] = b"\r\n";

/// What ends each of the server's own answers, after its status line and
/// its `Date`.
const ANSWER_END: &[u8] = b"Content-Length: 0\r\nConnection: close\r\n\r\n";

/// How long the `Date` field is, in every head the server writes but a 100
/// (Continue): `Date: `, an IMF-fixdate (RFC 9110, 5.6.7), always 29 bytes
/// as a [`Wall`] ends in 2554, and a line ending.
const DATE_LEN: usize = 37;

/// How the response's body is framed, for a request of `method` in
/// `version`.
pub(super) fn framing(response: &Response, method: Method, version: Version) -> Framing {
    if method == Method::Head || response.status == 204 || response.status == 304 {
        return Framing::Nothing;
    }
    match response.body {
        Body::None => Framing::Nothing,
        Body::Length(length) => Framing::Length(length),
        Body::Chunked => match version {
            Version::Http11 => Framing::Chunked,
            Version::Http10 => Framing::UntilEnd,
        },
    }
}

/// The head `response` is written as, in a box of exactly its length, or
/// why it is refused, for a request in `version`, on a connection that
/// persists after it or not. The checks run in a fixed order: the status,
/// then each field, its name before its value, then the body, then the
/// length.
///
/// The server writes the status line, `HTTP/1.1` whatever the request's
/// version (RFC 9110, 2.5), and its reason; the `Date`, `wall`'s (RFC
/// 9110, 6.6.1); the side above's fields in order; then its own: the
/// body's framing, as the response says it even to `HEAD` (RFC 9110,
/// 9.3.2), `Content-Length: 0` for no body but in a 204 or a 304; and
/// `Connection: close` for a connection that does not persist,
/// `keep-alive` for an HTTP/1.0 one that does.
pub(super) fn write(
    response: &Response,
    version: Version,
    persist: bool,
    limits: &Limits,
    wall: Wall,
) -> Result<Box<[u8]>, Refusal> {
    check(response)?;
    let status = Decimal::of(u64::from(response.status));
    let reason = reason(response.status);
    let length = match response.body {
        Body::Length(length) => Some(Decimal::of(length)),
        Body::None if response.status != 204 && response.status != 304 => Some(Decimal::of(0)),
        Body::None | Body::Chunked => None,
    };
    let chunked = response.body == Body::Chunked && version == Version::Http11;
    let connection = match (persist, version) {
        (false, Version::Http10 | Version::Http11) => Some(CLOSE),
        (true, Version::Http10) => Some(KEEP_ALIVE),
        (true, Version::Http11) => None,
    };
    let mut len = Measure(Some(0));
    len.add(VERSION.len());
    len.add(status.as_bytes().len());
    len.add(1);
    len.add(reason.len());
    len.add(CRLF.len());
    len.add(DATE_LEN);
    for header in &response.headers {
        len.add(header.name.len());
        len.add(2);
        len.add(header.value.len());
        len.add(CRLF.len());
    }
    if let Some(length) = &length {
        len.add(CONTENT_LENGTH.len());
        len.add(length.as_bytes().len());
        len.add(CRLF.len());
    }
    if chunked {
        len.add(CHUNKED.len());
    }
    if let Some(connection) = connection {
        len.add(connection.len());
    }
    len.add(CRLF.len());
    let len = match len.0 {
        Some(len) if len <= limits.response => len,
        Some(_) | None => return Err(Refusal::TooLong),
    };
    let mut head = Writer::new(usize::try_from(len).expect("a u32 fits a usize"));
    put(&mut head, VERSION);
    put(&mut head, status.as_bytes());
    put(&mut head, b" ");
    put(&mut head, reason);
    put(&mut head, CRLF);
    put_date(&mut head, wall);
    for header in &response.headers {
        put(&mut head, &header.name);
        put(&mut head, b": ");
        put(&mut head, &header.value);
        put(&mut head, CRLF);
    }
    if let Some(length) = &length {
        put(&mut head, CONTENT_LENGTH);
        put(&mut head, length.as_bytes());
        put(&mut head, CRLF);
    }
    if chunked {
        put(&mut head, CHUNKED);
    }
    if let Some(connection) = connection {
        put(&mut head, connection);
    }
    put(&mut head, CRLF);
    Ok(head.finish())
}

/// What the response gets wrong, the first thing in order.
fn check(response: &Response) -> Result<(), Refusal> {
    if !(200..=599).contains(&response.status) {
        return Err(Refusal::Status);
    }
    for header in &response.headers {
        if header.name.is_empty() {
            return Err(Refusal::Name);
        }
        for &byte in &header.name {
            if !is_tchar(byte) {
                return Err(Refusal::Name);
            }
        }
        for &byte in &header.value {
            if !is_field_byte(byte) {
                return Err(Refusal::Value);
            }
        }
        if header.is(b"content-length")
            || header.is(b"transfer-encoding")
            || header.is(b"connection")
            || header.is(b"date")
        {
            return Err(Refusal::Reserved);
        }
    }
    match response.body {
        Body::Length(_) | Body::Chunked if response.status == 204 => Err(Refusal::Body),
        Body::Chunked if response.status == 304 => Err(Refusal::Body),
        Body::None | Body::Length(_) | Body::Chunked => Ok(()),
    }
}

/// The answer the server writes for a request it rejects at the entrance
/// (programming-model.md, 8), at `wall`: small and of a fixed length, its
/// status line, its `Date`, `Content-Length: 0` and `Connection: close`,
/// with no body, on a connection it closes.
pub(super) fn answer(rejection: Rejection, wall: Wall) -> Box<[u8]> {
    let line = status_line(rejection);
    let mut answer = Writer::new(answer_len(line));
    put(&mut answer, line);
    put_date(&mut answer, wall);
    put(&mut answer, ANSWER_END);
    answer.finish()
}

/// The status line of a rejection's answer.
fn status_line(rejection: Rejection) -> &'static [u8] {
    match rejection {
        Rejection::RequestLine | Rejection::Header | Rejection::Host | Rejection::Framing => {
            b"HTTP/1.1 400 Bad Request\r\n"
        }
        Rejection::BodyTooLong => b"HTTP/1.1 413 Content Too Large\r\n",
        Rejection::TargetTooLong => b"HTTP/1.1 414 URI Too Long\r\n",
        Rejection::HeadTooLong | Rejection::TooManyHeaders => b"HTTP/1.1 431 Request Header Fields Too Large\r\n",
        Rejection::Method | Rejection::Coding => b"HTTP/1.1 501 Not Implemented\r\n",
        Rejection::Version => b"HTTP/1.1 505 HTTP Version Not Supported\r\n",
    }
}

/// How long an answer with status line `line` is.
fn answer_len(line: &[u8]) -> usize {
    line.len().saturating_add(DATE_LEN).saturating_add(ANSWER_END.len())
}

/// The `Date` field for `wall`, as an IMF-fixdate (RFC 9110, 5.6.7): the
/// day of the week, the date and the time of day, in GMT. The date comes
/// from the days since the epoch by whole 400-year cycles of the Gregorian
/// calendar, each counted from a March 1, so that a leap day ends its
/// year (Howard Hinnant's `civil_from_days`). Nothing saturates: a `Wall`
/// counts at most some 214,000 days.
fn put_date(head: &mut Writer, wall: Wall) {
    let seconds = wall.as_secs();
    let days = seconds / 86_400;
    let time = seconds % 86_400;
    // 1970-01-01 is 719,468 days after 0000-03-01, which starts a cycle.
    let shifted = days.saturating_add(719_468);
    let cycle = shifted / 146_097;
    let of_cycle = shifted % 146_097;
    // The year of the cycle, less the leap days before it: a cycle's
    // years have 365 days, one in four one more but one in a hundred, and
    // one in four hundred one more again.
    let year_of_cycle =
        of_cycle.saturating_sub(of_cycle / 1_460).saturating_add(of_cycle / 36_524).saturating_sub(of_cycle / 146_096)
            / 365;
    let day_of_year = of_cycle.saturating_sub(
        year_of_cycle.saturating_mul(365).saturating_add(year_of_cycle / 4).saturating_sub(year_of_cycle / 100),
    );
    // The month from March, 0 to 11: five months of 153 days.
    let month = day_of_year.saturating_mul(5).saturating_add(2) / 153;
    let day = day_of_year.saturating_sub(month.saturating_mul(153).saturating_add(2) / 5).saturating_add(1);
    // January and February are the next year's.
    let january = u64::from(month >= 10);
    let year = cycle.saturating_mul(400).saturating_add(year_of_cycle).saturating_add(january);
    put(head, b"Date: ");
    put(head, weekday(days.saturating_add(4) % 7));
    put(head, b", ");
    put_digits(head, day, 2);
    put(head, b" ");
    put(head, month_name(month));
    put(head, b" ");
    put_digits(head, year, 4);
    put(head, b" ");
    put_digits(head, time / 3_600, 2);
    put(head, b":");
    put_digits(head, time / 60 % 60, 2);
    put(head, b":");
    put_digits(head, time % 60, 2);
    put(head, b" GMT\r\n");
}

/// The day of the week, from Sunday, 0.
fn weekday(day: u64) -> &'static [u8] {
    match day {
        0 => b"Sun",
        1 => b"Mon",
        2 => b"Tue",
        3 => b"Wed",
        4 => b"Thu",
        5 => b"Fri",
        6 => b"Sat",
        _ => unreachable!("a day of the week is below 7"),
    }
}

/// The month, from March, 0.
fn month_name(month: u64) -> &'static [u8] {
    match month {
        0 => b"Mar",
        1 => b"Apr",
        2 => b"May",
        3 => b"Jun",
        4 => b"Jul",
        5 => b"Aug",
        6 => b"Sep",
        7 => b"Oct",
        8 => b"Nov",
        9 => b"Dec",
        10 => b"Jan",
        11 => b"Feb",
        _ => unreachable!("a month is below 12"),
    }
}

/// `n`'s last `places` decimal digits, with leading zeros.
fn put_digits(head: &mut Writer, n: u64, places: u32) {
    let mut place = 1_u64;
    for _ in 1..places {
        place = place.saturating_mul(10);
    }
    // The digits, most significant first, a place at a time.
    for _ in 0..places {
        let digit = u8::try_from(n.checked_div(place).expect("a place is at least 1") % 10).expect("a digit");
        put(head, core::slice::from_ref(&b'0'.saturating_add(digit)));
        place /= 10;
    }
}

/// The longest of the server's own answers: room for it is what the
/// server sets aside before it reads a request, so [`Limits::response`]
/// is at least this.
pub(super) fn longest_answer() -> u32 {
    let rejections = [
        Rejection::RequestLine,
        Rejection::TargetTooLong,
        Rejection::Version,
        Rejection::Method,
        Rejection::Header,
        Rejection::HeadTooLong,
        Rejection::TooManyHeaders,
        Rejection::Host,
        Rejection::Framing,
        Rejection::Coding,
        Rejection::BodyTooLong,
    ];
    let mut longest = CONTINUE.len();
    for rejection in rejections {
        longest = longest.max(answer_len(status_line(rejection)));
    }
    u32::try_from(longest).expect("a fixed answer fits a u32")
}

/// The reason phrase written after a status (RFC 9110, 15): the
/// standard's for the codes it defines, and none for another, which a
/// client ignores either way.
fn reason(status: u16) -> &'static [u8] {
    match status {
        200 => b"OK",
        201 => b"Created",
        202 => b"Accepted",
        203 => b"Non-Authoritative Information",
        204 => b"No Content",
        205 => b"Reset Content",
        206 => b"Partial Content",
        300 => b"Multiple Choices",
        301 => b"Moved Permanently",
        302 => b"Found",
        303 => b"See Other",
        304 => b"Not Modified",
        307 => b"Temporary Redirect",
        308 => b"Permanent Redirect",
        400 => b"Bad Request",
        401 => b"Unauthorized",
        402 => b"Payment Required",
        403 => b"Forbidden",
        404 => b"Not Found",
        405 => b"Method Not Allowed",
        406 => b"Not Acceptable",
        407 => b"Proxy Authentication Required",
        408 => b"Request Timeout",
        409 => b"Conflict",
        410 => b"Gone",
        411 => b"Length Required",
        412 => b"Precondition Failed",
        413 => b"Content Too Large",
        414 => b"URI Too Long",
        415 => b"Unsupported Media Type",
        416 => b"Range Not Satisfiable",
        417 => b"Expectation Failed",
        421 => b"Misdirected Request",
        422 => b"Unprocessable Content",
        426 => b"Upgrade Required",
        428 => b"Precondition Required",
        429 => b"Too Many Requests",
        431 => b"Request Header Fields Too Large",
        500 => b"Internal Server Error",
        501 => b"Not Implemented",
        502 => b"Bad Gateway",
        503 => b"Service Unavailable",
        504 => b"Gateway Timeout",
        505 => b"HTTP Version Not Supported",
        _ => b"",
    }
}

/// The room a chunk of up to `n` bytes takes: its size in hexadecimal and
/// a line ending, the bytes, and a line ending (RFC 9112, 7.1). `None`
/// past a `u32`.
pub(crate) fn chunk_room(n: u32) -> Option<u32> {
    n.checked_add(hex_digits(n))?.checked_add(4)
}

/// `bytes`, at least one, as a chunk: its size line, the bytes, and the
/// line ending after them.
pub(super) fn chunk(bytes: &[u8]) -> Box<[u8]> {
    let len = u32::try_from(bytes.len()).expect("a piece within Limits::send");
    let room = chunk_room(len).expect("a chunk within Limits::send's room");
    let mut chunk = Writer::new(usize::try_from(room).expect("a u32 fits a usize"));
    let digits = hex_digits(len);
    // The digits, most significant first: a nibble at a time, bounded by
    // the eight a u32 has.
    for place in (0..digits).rev() {
        let nibble = len.checked_shr(place.saturating_mul(4)).expect("within a u32") & 0xF;
        let digit = b"0123456789abcdef".get(usize::try_from(nibble).expect("a nibble fits a usize"));
        put(&mut chunk, core::slice::from_ref(digit.expect("a nibble is a hexadecimal digit")));
    }
    put(&mut chunk, CRLF);
    put(&mut chunk, bytes);
    put(&mut chunk, CRLF);
    chunk.finish()
}

/// How many hexadecimal digits `n` takes: at least one.
fn hex_digits(n: u32) -> u32 {
    let bits = u32::BITS.saturating_sub(n.leading_zeros());
    bits.div_ceil(4).max(1)
}

/// A head's length, added up with checks: `None` past a `u32`.
struct Measure(Option<u32>);

impl Measure {
    fn add(&mut self, len: usize) {
        let Some(sum) = self.0 else { return };
        self.0 = match u32::try_from(len) {
            Ok(len) => sum.checked_add(len),
            Err(_) => None,
        };
    }
}

/// Puts what was measured to fit.
fn put(head: &mut Writer, bytes: &[u8]) {
    head.put(bytes).expect("the head was measured to fit");
}
