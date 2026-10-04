//! The response (http.md, 5.4): what the side above asks for, checked,
//! then measured and written into a box of exactly its length
//! (programming-model.md, 8); the fixed answers the server writes itself,
//! a 100 (Continue) and each rejection; and the framing of a chunked body.

use alloc::boxed::Box;

use skein_lib::{Decimal, Writer};

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
    /// `Transfer-Encoding` or `Connection`.
    Reserved,
    /// A body for a 204 or a 304, which have none (RFC 9110, 15.3.5 and
    /// 15.4.5).
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
/// version (RFC 9110, 2.5), and its reason; the side above's fields in
/// order; then its own: the body's framing, as the response says it even
/// to `HEAD` (RFC 9110, 9.3.2), `Content-Length: 0` for no body but in a
/// 204 or a 304; and `Connection: close` for a connection that does not
/// persist, `keep-alive` for an HTTP/1.0 one that does.
pub(super) fn write(
    response: &Response,
    version: Version,
    persist: bool,
    limits: &Limits,
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
        if header.is(b"content-length") || header.is(b"transfer-encoding") || header.is(b"connection") {
            return Err(Refusal::Reserved);
        }
    }
    match response.body {
        Body::Length(_) | Body::Chunked if response.status == 204 || response.status == 304 => Err(Refusal::Body),
        Body::None | Body::Length(_) | Body::Chunked => Ok(()),
    }
}

/// The answer the server writes for a request it rejects at the entrance
/// (programming-model.md, 8): small and fixed, with no body, on a
/// connection it closes.
pub(super) fn answer(rejection: Rejection) -> &'static [u8] {
    match rejection {
        Rejection::RequestLine | Rejection::Header | Rejection::Host | Rejection::Framing => {
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        }
        Rejection::BodyTooLong => b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        Rejection::TargetTooLong => b"HTTP/1.1 414 URI Too Long\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        Rejection::HeadTooLong | Rejection::TooManyHeaders => {
            b"HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        }
        Rejection::Method | Rejection::Coding => {
            b"HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        }
        Rejection::Version => {
            b"HTTP/1.1 505 HTTP Version Not Supported\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        }
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
        longest = longest.max(answer(rejection).len());
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
