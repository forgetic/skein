//! The response head (http.md, 3.2): read a line at a time, each line
//! parsed as it comes into a bounded head, then the body's framing and
//! whether the connection persists decided from it (RFC 9112, 6.3 and 9.3).

use skein_lib::{List, Reader, Writer, bytes};

use super::{Error, Framing, Method, Reading, Status, Version};
use crate::Header;
use crate::header::{self, content, is_field_byte, is_ows, is_tchar, trim};

/// What a line of a head comes to.
pub(super) enum Parsed {
    /// The head goes on.
    More,
    /// It was the blank line that ends the head.
    Complete,
}

/// A line of a response head, as a scan to LF of at most what is left of
/// the head delivered it, read into `reading`.
///
/// The errors of a line are decided in a fixed order: its length first (a
/// line that met no LF filled what was left of the head), then its bytes,
/// then the room for one more field.
pub(super) fn line(reading: &mut Reading, bytes: &[u8]) -> Result<Parsed, Error> {
    let len = u32::try_from(bytes.len()).expect("a delivery's length fits a u32");
    reading.budget = reading.budget.checked_sub(len).expect("a scan within what is left of the head");
    let Some(content) = content(bytes) else { return Err(Error::HeadTooLong) };
    match reading.status {
        None => reading.status = Some(status_line(content)?),
        Some(_) if content.is_empty() => return Ok(Parsed::Complete),
        Some(_) => match content.first() {
            Some(&first) if is_ows(first) => fold(content, &mut reading.headers)?,
            Some(_) | None => field(content, &mut reading.headers)?,
        },
    }
    // The blank line that ends the head is still due.
    if reading.budget == 0 {
        return Err(Error::HeadTooLong);
    }
    Ok(Parsed::More)
}

/// `HTTP/1.1 200 OK` (RFC 9112, 4): the version, a space, three digits,
/// then nothing, or a space and a reason phrase, which the client checks
/// and does not keep (RFC 9110, 15).
///
/// A version of another major than 1 is `Version`; a minor past 1 is read
/// as 1.1 (RFC 9112, 2.3). A code outside 100 to 599 is `Status`.
fn status_line(line: &[u8]) -> Result<Status, Error> {
    let mut reader = Reader::new(line);
    if reader.bytes(5) != Some(b"HTTP/") {
        return Err(Error::Status);
    }
    let version = match reader.bytes(3) {
        Some(&[major, b'.', minor]) if major.is_ascii_digit() && minor.is_ascii_digit() => {
            if major != b'1' {
                return Err(Error::Version);
            }
            if minor == b'0' { Version::Http10 } else { Version::Http11 }
        }
        Some(_) | None => return Err(Error::Status),
    };
    if reader.u8() != Some(b' ') {
        return Err(Error::Status);
    }
    let code = match reader.bytes(3) {
        Some(&[hundreds, tens, units])
            if (b'1'..=b'5').contains(&hundreds) && tens.is_ascii_digit() && units.is_ascii_digit() =>
        {
            digit(hundreds)
                .saturating_mul(100)
                .saturating_add(digit(tens).saturating_mul(10))
                .saturating_add(digit(units))
        }
        Some(_) | None => return Err(Error::Status),
    };
    match reader.u8() {
        None => {}
        Some(b' ') => {
            let reason = reader.bytes(reader.remaining()).expect("the rest of the line");
            for &byte in reason {
                if !is_field_byte(byte) {
                    return Err(Error::Status);
                }
            }
        }
        Some(_) => return Err(Error::Status),
    }
    Ok(Status { version, code })
}

fn digit(byte: u8) -> u16 {
    u16::from(byte.saturating_sub(b'0'))
}

/// `Name: value` (RFC 9112, 5): a token, a colon with no whitespace
/// before it, and a value, trimmed.
fn field(line: &[u8], headers: &mut List<Header>) -> Result<(), Error> {
    let mut colon = None;
    for (at, &byte) in line.iter().enumerate() {
        if byte == b':' {
            colon = Some(at);
            break;
        }
        if !is_tchar(byte) {
            return Err(Error::Header);
        }
    }
    let (name, value) = match colon {
        Some(at) if at > 0 => (line.get(..at), line.get(at.saturating_add(1)..)),
        Some(_) | None => return Err(Error::Header),
    };
    let name = name.expect("before the colon");
    let value = trim(value.expect("after the colon"));
    check(value)?;
    if headers.room() == 0 {
        return Err(Error::TooManyHeaders);
    }
    let header = Header { name: bytes::copy_of(name), value: bytes::copy_of(value) };
    headers.push(header).expect("room was checked");
    Ok(())
}

/// A line that begins with whitespace: an obsolete fold of the last
/// field's value onto a line of its own, which a client replaces with one
/// space (RFC 9112, 5.2). A fold before any field is `Header`.
fn fold(line: &[u8], headers: &mut List<Header>) -> Result<(), Error> {
    let more = trim(line);
    check(more)?;
    let Some(last) = headers.len().checked_sub(1) else { return Err(Error::Header) };
    let header = headers.get_mut(last).expect("the last field");
    if more.is_empty() {
        return Ok(());
    }
    if header.value.is_empty() {
        header.value = bytes::copy_of(more);
        return Ok(());
    }
    let len = header.value.len().saturating_add(1).saturating_add(more.len());
    let mut joined = Writer::new(len);
    for piece in [&*header.value, b" ", more] {
        joined.put(piece).expect("measured to fit");
    }
    header.value = joined.finish();
    Ok(())
}

/// A value holds only what a field's value may (RFC 9110, 5.5): no control
/// character but a tab, and so no CR, LF or NUL.
fn check(value: &[u8]) -> Result<(), Error> {
    for &byte in value {
        if !is_field_byte(byte) {
            return Err(Error::Header);
        }
    }
    Ok(())
}

/// How the body of a final response to `method` is framed (RFC 9112,
/// 6.3), or why its framing is refused (`Framing`).
///
/// - No body, whatever the head says, for a `HEAD`, a 204 and a 304.
/// - Chunked for `Transfer-Encoding: chunked`, alone and in HTTP/1.1 only.
///   With a `Content-Length` beside it, the framing is refused rather than
///   read by the one that wins (RFC 9112, 6.3, 3: "ought to be handled as
///   an error"), and so is any other coding: the client undoes none.
/// - By length for `Content-Length`, every value of it alike.
/// - Otherwise, to the end of the stream.
pub(super) fn framing(method: Method, status: Status, headers: &[Header]) -> Result<Framing, Error> {
    if method == Method::Head || status.code == 204 || status.code == 304 {
        return Ok(Framing::Empty);
    }
    match transfer_encoding(headers) {
        Coding::Absent => match content_length(headers) {
            Length::Absent => Ok(Framing::UntilEnd),
            Length::Valid(length) => Ok(Framing::Length(length)),
            Length::Invalid => Err(Error::Framing),
        },
        Coding::Chunked => match content_length(headers) {
            Length::Absent => match status.version {
                Version::Http11 => Ok(Framing::Chunked),
                Version::Http10 => Err(Error::Framing),
            },
            Length::Valid(_) | Length::Invalid => Err(Error::Framing),
        },
        Coding::Other => Err(Error::Framing),
    }
}

/// Whether the connection persists after the response (RFC 9112, 9.3): in
/// HTTP/1.1 unless it says `Connection: close`; in HTTP/1.0 only if it says
/// `Connection: keep-alive`, and not `close`.
pub(super) fn persistent(version: Version, headers: &[Header]) -> bool {
    let close = header::lists(headers, b"connection", b"close");
    match version {
        Version::Http11 => !close,
        Version::Http10 => !close && header::lists(headers, b"connection", b"keep-alive"),
    }
}

/// What a head's `Transfer-Encoding` says.
enum Coding {
    Absent,
    /// `chunked`, alone.
    Chunked,
    /// Anything else: another coding, `chunked` twice, or nothing listed.
    Other,
}

fn transfer_encoding(headers: &[Header]) -> Coding {
    let mut present = false;
    let mut chunked = 0_u32;
    let mut other = false;
    for header in headers {
        if !header.is(b"transfer-encoding") {
            continue;
        }
        present = true;
        let mut at = 0;
        // Bounded by the value: each element moves past its comma.
        for _ in 0..=header.value.len() {
            let Some((element, next)) = header::element(&header.value, at) else { break };
            at = next;
            if element.is_empty() {
                continue;
            }
            if element.eq_ignore_ascii_case(b"chunked") {
                chunked = chunked.saturating_add(1);
            } else {
                other = true;
            }
        }
    }
    if !present {
        return Coding::Absent;
    }
    if chunked == 1 && !other { Coding::Chunked } else { Coding::Other }
}

/// What a head's `Content-Length` says.
enum Length {
    Absent,
    /// One length, however many times it is given (RFC 9110, 8.6).
    Valid(u64),
    /// Not a length, past a `u64`, two different ones, or none listed.
    Invalid,
}

fn content_length(headers: &[Header]) -> Length {
    let mut present = false;
    let mut length = None;
    for header in headers {
        if !header.is(b"content-length") {
            continue;
        }
        present = true;
        let mut at = 0;
        // Bounded by the value: each element moves past its comma.
        for _ in 0..=header.value.len() {
            let Some((element, next)) = header::element(&header.value, at) else { break };
            at = next;
            if element.is_empty() {
                continue;
            }
            let Some(this) = decimal(element) else { return Length::Invalid };
            match length {
                Some(earlier) if earlier != this => return Length::Invalid,
                Some(_) | None => length = Some(this),
            }
        }
    }
    match length {
        Some(length) => Length::Valid(length),
        None if present => Length::Invalid,
        None => Length::Absent,
    }
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
