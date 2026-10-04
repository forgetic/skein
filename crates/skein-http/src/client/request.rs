//! The request head (http.md, 3.1): what the side above asks for, checked,
//! then measured and written into a box of exactly its length
//! (programming-model.md, 8).

use alloc::boxed::Box;

use skein_lib::{Decimal, Writer};

use super::Limits;
use crate::Header;
use crate::header::{is_field_byte, is_tchar};

/// A request method (RFC 9110, 9.3): those an API's client sends.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Method {
    Get,
    /// A response to it has no body, whatever its head says.
    Head,
    Post,
    Put,
    Patch,
    Delete,
    Options,
}

impl Method {
    /// The method's name, as the request line spells it.
    #[must_use]
    pub fn as_bytes(self) -> &'static [u8] {
        match self {
            Method::Get => b"GET",
            Method::Head => b"HEAD",
            Method::Post => b"POST",
            Method::Put => b"PUT",
            Method::Patch => b"PATCH",
            Method::Delete => b"DELETE",
            Method::Options => b"OPTIONS",
        }
    }

    /// Whether the method gives a body a meaning, so that a call without
    /// one says `Content-Length: 0` (RFC 9110, 8.6).
    fn expects_body(self) -> bool {
        match self {
            Method::Post | Method::Put | Method::Patch => true,
            Method::Get | Method::Head | Method::Delete | Method::Options => false,
        }
    }
}

/// The request body a call announces in its head.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Body {
    /// None: nothing is uploaded.
    None,
    /// Exactly this many bytes, by `Content-Length`, which the side above
    /// writes as a stream (`Request::Upload`), then finishes.
    Length(u64),
}

/// An exchange the side above asks for: the request's head, and the
/// length of its body.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Call {
    pub method: Method,
    /// The request target, in origin form: `/v1/messages?beta=true`.
    /// Visible ASCII, at least one byte.
    pub target: Box<[u8]>,
    /// The fields the request carries, written in this order: one `Host`
    /// among them (RFC 9112, 3.2), and none the client writes itself,
    /// `Content-Length`, `Transfer-Encoding` and `Connection`.
    pub headers: Box<[Header]>,
    pub body: Body,
    /// Whether the connection ends with this exchange: the client says
    /// `Connection: close`, and does not use it again.
    pub close: bool,
}

/// Why the client refused a call, before writing anything: a fault of the
/// side above, which it can fix.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// The target is empty, or holds a byte that is not visible ASCII.
    Target,
    /// A field's name is not a token.
    Name,
    /// A field's value holds a control character other than a tab: a CR,
    /// an LF or a NUL among them.
    Value,
    /// A field the client writes itself: `Content-Length`,
    /// `Transfer-Encoding` or `Connection`.
    Reserved,
    /// No `Host` field, or more than one: an HTTP/1.1 request carries
    /// exactly one (RFC 9112, 3.2), and a server refuses it otherwise.
    Host,
    /// The head is longer than [`Limits::request`].
    TooLong,
}

const VERSION: &[u8] = b" HTTP/1.1\r\n";
const CONTENT_LENGTH: &[u8] = b"Content-Length: ";
const CLOSE: &[u8] = b"Connection: close\r\n";
const CRLF: &[u8] = b"\r\n";

/// The head `call` is written as, in a box of exactly its length, or why
/// it is refused. The checks run in a fixed order: the target, then each
/// field, its name before its value, then the one `Host`, then the length.
pub(super) fn write(call: &Call, limits: &Limits) -> Result<Box<[u8]>, Refusal> {
    check(call)?;
    let length = framing(call);
    let mut len = Measure(Some(0));
    len.add(call.method.as_bytes().len());
    len.add(1);
    len.add(call.target.len());
    len.add(VERSION.len());
    for header in &call.headers {
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
    if call.close {
        len.add(CLOSE.len());
    }
    len.add(CRLF.len());
    let len = match len.0 {
        Some(len) if len <= limits.request => len,
        Some(_) | None => return Err(Refusal::TooLong),
    };
    let mut head = Writer::new(usize::try_from(len).expect("a u32 fits a usize"));
    put(&mut head, call.method.as_bytes());
    put(&mut head, b" ");
    put(&mut head, &call.target);
    put(&mut head, VERSION);
    for header in &call.headers {
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
    if call.close {
        put(&mut head, CLOSE);
    }
    put(&mut head, CRLF);
    Ok(head.finish())
}

/// What the call gets wrong, the first thing in order.
fn check(call: &Call) -> Result<(), Refusal> {
    if call.target.is_empty() {
        return Err(Refusal::Target);
    }
    for &byte in &call.target {
        if !(0x21..=0x7E).contains(&byte) {
            return Err(Refusal::Target);
        }
    }
    let mut hosts: u32 = 0;
    for header in &call.headers {
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
        if header.is(b"host") {
            hosts = hosts.saturating_add(1);
        }
    }
    if hosts != 1 {
        return Err(Refusal::Host);
    }
    Ok(())
}

/// The `Content-Length` the head says, if it says one: the body's, or none
/// for a method that gives a body a meaning (RFC 9110, 8.6).
fn framing(call: &Call) -> Option<Decimal> {
    match call.body {
        Body::Length(length) => Some(Decimal::of(length)),
        Body::None if call.method.expects_body() => Some(Decimal::of(0)),
        Body::None => None,
    }
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
