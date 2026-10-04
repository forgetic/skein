//! The request head (http.md, 5.2): read a line at a time, each line
//! parsed as it comes into a bounded head; then, at its blank line, the
//! `Host`, the body's framing and whether the connection persists decided
//! from it (RFC 9112, 3.2, 6.3 and 9.3). What is wrong with it is a
//! [`Rejection`], which the server answers itself.

use alloc::boxed::Box;

use skein_lib::{List, bytes};

use super::{Body, Call, Limits, Rejection};
use crate::header::{self, content, is_field_byte, is_ows, is_tchar, trim};
use crate::{Header, Method, Version};

/// A request head being read.
#[derive(Debug)]
pub(super) struct Head {
    /// What is left of [`Limits::head`] for this head.
    budget: u32,
    /// The request line, once read.
    line: Option<RequestLine>,
    headers: List<Header>,
}

#[derive(Debug)]
struct RequestLine {
    method: Method,
    target: Box<[u8]>,
    version: Version,
}

/// What a line of a head comes to.
pub(super) enum Parsed {
    /// The head goes on.
    More,
    /// It was the blank line that ends the head.
    Complete,
}

/// A whole request head, checked: the call that goes up, and what the
/// server keeps of it for the exchange.
#[derive(Debug)]
pub(super) struct Request {
    pub(super) call: Call,
    /// Whether the request lets the connection carry another exchange
    /// (RFC 9112, 9.3).
    pub(super) persist: bool,
    /// Whether the client waits for a 100 (Continue) before it sends the
    /// body (RFC 9110, 10.1.1).
    pub(super) expects: bool,
}

impl Head {
    pub(super) fn new(limits: &Limits) -> Head {
        Head { budget: limits.head, line: None, headers: List::with_capacity(limits.headers) }
    }

    /// The most the next line may be: what is left of [`Limits::head`].
    pub(super) fn budget(&self) -> u32 {
        self.budget
    }

    /// Whether the request line was read: before it, the stream ending is
    /// the connection's end, not a request cut short.
    pub(super) fn begun(&self) -> bool {
        self.line.is_some()
    }

    /// A line of the head, as a scan to LF of at most what is left of the
    /// head delivered it.
    ///
    /// The errors of a line are decided in a fixed order: its length first
    /// (a line that met no LF filled what was left of the head: 414 for the
    /// request line, 431 for a field), then its bytes, then the room for one
    /// more field. Empty lines before the request line are skipped (RFC
    /// 9112, 2.2), within the budget.
    pub(super) fn line(&mut self, bytes: &[u8]) -> Result<Parsed, Rejection> {
        let len = u32::try_from(bytes.len()).expect("a delivery's length fits a u32");
        self.budget = self.budget.checked_sub(len).expect("a scan within what is left of the head");
        let Some(content) = content(bytes) else {
            return Err(if self.begun() { Rejection::HeadTooLong } else { Rejection::TargetTooLong });
        };
        match &self.line {
            None if content.is_empty() => {}
            None => self.line = Some(request_line(content)?),
            Some(_) if content.is_empty() => return Ok(Parsed::Complete),
            Some(_) => match content.first() {
                // An obsolete fold, which a server may refuse (RFC 9112, 5.2),
                // or whitespace before the first field (RFC 9112, 2.2).
                Some(&first) if is_ows(first) => return Err(Rejection::Header),
                Some(_) | None => field(content, &mut self.headers)?,
            },
        }
        // The blank line that ends the head is still due.
        if self.budget == 0 {
            return Err(Rejection::HeadTooLong);
        }
        Ok(Parsed::More)
    }

    /// The head, whole, checked in a fixed order: one `Host` (RFC 9112,
    /// 3.2), and a sound one, then the body's framing (RFC 9112, 6.1 and
    /// 6.3), then its length against [`Limits::body`].
    pub(super) fn request(self, limits: &Limits) -> Result<Request, Rejection> {
        let line = self.line.expect("a head is complete only after its request line");
        let headers = self.headers.into_boxed();
        let mut hosts = 0_u32;
        let mut sound = true;
        for header in &headers {
            if header.is(b"host") {
                hosts = hosts.saturating_add(1);
                sound &= is_host(&header.value);
            }
        }
        let host = sound
            && match line.version {
                Version::Http11 => hosts == 1,
                Version::Http10 => hosts <= 1,
            };
        if !host {
            return Err(Rejection::Host);
        }
        let body = framing(line.version, &headers)?;
        match body {
            Body::Length(length) if length > limits.body => return Err(Rejection::BodyTooLong),
            Body::None | Body::Length(_) | Body::Chunked => {}
        }
        let persist = persistent(line.version, &headers);
        let sends = match body {
            Body::None | Body::Length(0) => false,
            Body::Length(_) | Body::Chunked => true,
        };
        // An HTTP/1.0 client knows no 100 (Continue): its expectation is
        // ignored (RFC 9110, 10.1.1), as is any expectation but this one.
        let expects = sends && line.version == Version::Http11 && header::lists(&headers, b"expect", b"100-continue");
        let call = Call { method: line.method, target: line.target, version: line.version, headers, body };
        Ok(Request { call, persist, expects })
    }
}

/// Where a `Host`'s value is read to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum HostPart {
    /// A registered name or an IPv4 address.
    Name,
    /// An IP literal, within its brackets.
    Literal,
    /// Past an IP literal's closing bracket.
    Closed,
    /// The port, past its colon.
    Port,
}

/// Whether `value` is a `Host` (RFC 9110, 7.2): `uri-host [":" port]`, the
/// host an IP literal in brackets, of hexadecimal digits, colons, dots
/// and the bytes of a future one, or a registered name or an IPv4 address,
/// of unreserved bytes, sub-delimiters and percent-escapes (RFC 3986,
/// 3.2.2), and the port digits. It may be empty, as a client sends it for a
/// target with no authority (RFC 9112, 3.2).
fn is_host(value: &[u8]) -> bool {
    let mut part = HostPart::Name;
    // The hexadecimal digits a percent-escape still holds.
    let mut escape = 0_u8;
    for (at, &byte) in value.iter().enumerate() {
        part = match part {
            HostPart::Name if at == 0 && byte == b'[' => HostPart::Literal,
            HostPart::Name if escape > 0 && byte.is_ascii_hexdigit() => {
                escape = escape.saturating_sub(1);
                HostPart::Name
            }
            HostPart::Name if escape == 0 && byte == b'%' => {
                escape = 2;
                HostPart::Name
            }
            HostPart::Name if escape == 0 && (is_unreserved(byte) || is_sub_delim(byte)) => HostPart::Name,
            HostPart::Literal if byte == b']' => HostPart::Closed,
            HostPart::Literal if is_unreserved(byte) || is_sub_delim(byte) || byte == b':' => HostPart::Literal,
            HostPart::Name | HostPart::Closed if escape == 0 && byte == b':' => HostPart::Port,
            HostPart::Port if byte.is_ascii_digit() => HostPart::Port,
            HostPart::Name | HostPart::Literal | HostPart::Closed | HostPart::Port => return false,
        };
    }
    match part {
        HostPart::Name => escape == 0,
        HostPart::Literal => false,
        HostPart::Closed | HostPart::Port => true,
    }
}

/// `ALPHA / DIGIT / "-" / "." / "_" / "~"` (RFC 3986, 2.3).
fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"-._~".contains(&byte)
}

/// `"!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="` (RFC
/// 3986, 2.2).
fn is_sub_delim(byte: u8) -> bool {
    b"!$&'()*+,;=".contains(&byte)
}

/// `GET /v1/messages HTTP/1.1` (RFC 9112, 3): a method, a space, a target,
/// a space, and the version, each exactly so: a request line that is not
/// one is refused rather than repaired.
///
/// What is wrong is decided in a fixed order: the line's form (400), then
/// the version's major (505), then the method (501).
fn request_line(line: &[u8]) -> Result<RequestLine, Rejection> {
    let mut first = None;
    for (at, &byte) in line.iter().enumerate() {
        if byte == b' ' {
            first = Some(at);
            break;
        }
    }
    let (method, rest) = match first {
        Some(at) if at > 0 => (line.get(..at), line.get(at.saturating_add(1)..)),
        Some(_) | None => return Err(Rejection::RequestLine),
    };
    let (method, rest) = (method.expect("before the space"), rest.expect("after the space"));
    for &byte in method {
        if !is_tchar(byte) {
            return Err(Rejection::RequestLine);
        }
    }
    let mut second = None;
    for (at, &byte) in rest.iter().enumerate() {
        if byte == b' ' {
            second = Some(at);
            break;
        }
        if !(0x21..=0x7E).contains(&byte) {
            return Err(Rejection::RequestLine);
        }
    }
    let (target, version) = match second {
        Some(at) if at > 0 => (rest.get(..at), rest.get(at.saturating_add(1)..)),
        Some(_) | None => return Err(Rejection::RequestLine),
    };
    let (target, version) = (target.expect("before the space"), version.expect("after the space"));
    let version = match *version {
        [b'H', b'T', b'T', b'P', b'/', major, b'.', minor] if major.is_ascii_digit() && minor.is_ascii_digit() => {
            if major != b'1' {
                return Err(Rejection::Version);
            }
            if minor == b'0' { Version::Http10 } else { Version::Http11 }
        }
        _ => return Err(Rejection::RequestLine),
    };
    let Some(method) = Method::from_bytes(method) else { return Err(Rejection::Method) };
    Ok(RequestLine { method, target: bytes::copy_of(target), version })
}

/// `Name: value` (RFC 9112, 5): a token, a colon with no whitespace
/// before it, and a value, trimmed.
fn field(line: &[u8], headers: &mut List<Header>) -> Result<(), Rejection> {
    let mut colon = None;
    for (at, &byte) in line.iter().enumerate() {
        if byte == b':' {
            colon = Some(at);
            break;
        }
        if !is_tchar(byte) {
            return Err(Rejection::Header);
        }
    }
    let (name, value) = match colon {
        Some(at) if at > 0 => (line.get(..at), line.get(at.saturating_add(1)..)),
        Some(_) | None => return Err(Rejection::Header),
    };
    let name = name.expect("before the colon");
    let value = trim(value.expect("after the colon"));
    for &byte in value {
        if !is_field_byte(byte) {
            return Err(Rejection::Header);
        }
    }
    if headers.room() == 0 {
        return Err(Rejection::TooManyHeaders);
    }
    let header = Header { name: bytes::copy_of(name), value: bytes::copy_of(value) };
    headers.push(header).expect("room was checked");
    Ok(())
}

/// How the body of a request is framed (RFC 9112, 6.1 and 6.3), or why
/// its framing is refused.
///
/// - `Transfer-Encoding` is read only in HTTP/1.1 and only alone: in
///   HTTP/1.0, or beside a `Content-Length`, the framing is faulty
///   (`Framing`, 400), as it is how requests are smuggled. Its codings must
///   end with `chunked`, given once (`Framing`), and hold no other, which
///   the server does not undo (`Coding`, 501).
/// - By length for `Content-Length`, every value of it alike, or `Framing`.
/// - Otherwise, no body.
fn framing(version: Version, headers: &[Header]) -> Result<Body, Rejection> {
    let mut coded = false;
    let mut lengths = false;
    for header in headers {
        coded |= header.is(b"transfer-encoding");
        lengths |= header.is(b"content-length");
    }
    if coded {
        if version == Version::Http10 || lengths {
            return Err(Rejection::Framing);
        }
        return codings(headers);
    }
    if !lengths {
        return Ok(Body::None);
    }
    match content_length(headers) {
        Some(length) => Ok(Body::Length(length)),
        None => Err(Rejection::Framing),
    }
}

/// The codings of a request's `Transfer-Encoding`, in order across its
/// fields: `chunked` last and once, and no other.
fn codings(headers: &[Header]) -> Result<Body, Rejection> {
    let mut chunked = 0_u32;
    let mut other = false;
    let mut last_chunked = false;
    for header in headers {
        if !header.is(b"transfer-encoding") {
            continue;
        }
        let mut at = 0;
        // Bounded by the value: each element moves past its comma.
        for _ in 0..=header.value.len() {
            let Some((element, next)) = header::element(&header.value, at) else { break };
            at = next;
            if element.is_empty() {
                continue;
            }
            last_chunked = element.eq_ignore_ascii_case(b"chunked");
            if last_chunked {
                chunked = chunked.saturating_add(1);
            } else {
                other = true;
            }
        }
    }
    if !last_chunked || chunked > 1 {
        return Err(Rejection::Framing);
    }
    if other {
        return Err(Rejection::Coding);
    }
    Ok(Body::Chunked)
}

/// One length, however many times it is given (RFC 9110, 8.6): `None` for
/// one that is not a length, past a `u64`, two different ones, or none
/// listed.
fn content_length(headers: &[Header]) -> Option<u64> {
    let mut length = None;
    for header in headers {
        if !header.is(b"content-length") {
            continue;
        }
        let mut at = 0;
        // Bounded by the value: each element moves past its comma.
        for _ in 0..=header.value.len() {
            let Some((element, next)) = header::element(&header.value, at) else { break };
            at = next;
            if element.is_empty() {
                continue;
            }
            let this = decimal(element)?;
            match length {
                Some(earlier) if earlier != this => return None,
                Some(_) | None => length = Some(this),
            }
        }
    }
    length
}

/// `1*DIGIT` as a `u64`, or `None`: empty, another byte, or too large.
fn decimal(digits: &[u8]) -> Option<u64> {
    if digits.is_empty() {
        return None;
    }
    let mut value = 0_u64;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(byte.saturating_sub(b'0')))?;
    }
    Some(value)
}

/// Whether the connection persists after the exchange, as the request
/// asks (RFC 9112, 9.3): in HTTP/1.1 unless it says `Connection: close`;
/// in HTTP/1.0 only if it says `Connection: keep-alive`, and not `close`.
fn persistent(version: Version, headers: &[Header]) -> bool {
    let close = header::lists(headers, b"connection", b"close");
    match version {
        Version::Http11 => !close,
        Version::Http10 => !close && header::lists(headers, b"connection", b"keep-alive"),
    }
}
