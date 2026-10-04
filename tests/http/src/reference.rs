//! Simple reference readers (http.md, 6): a response, and an event stream,
//! each read whole from bytes held in memory, which the machines are
//! checked against.
//!
//! They share no code with the machines: plain loops over a whole buffer,
//! with the standard library's conveniences. What they mirror is only what
//! the machines promise:
//!
//! - **what their scans see:** the client reads a head and the framing of
//!   a chunked body a line at a time, each line a scan to LF of at most
//!   what is left of its limit, and the reader scans to the byte that ended
//!   the last line; a scan the end of the stream leaves unmet is never
//!   seen;
//! - **the order of errors:** a line's length before its bytes, its bytes
//!   before the room for one more field; an event's size before a line's
//!   length, a byte at a time.

use skein_http::client::{Error, Framing, Limits, Method, Reuse, Version};
use skein_http::sse;

/// A response's head, as the reference reads it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Head {
    pub version: Version,
    pub status: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub framing: Framing,
}

/// How an exchange ended.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Outcome {
    Done(Reuse),
    Failed(Error),
}

/// What a response read whole comes to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Response {
    /// The final head, if it was read whole.
    pub head: Option<Head>,
    /// The body, as far as the bytes hold it.
    pub body: Vec<u8>,
    pub outcome: Outcome,
    /// How many of the bytes the response took, for the next on the same
    /// connection.
    pub used: usize,
}

/// What `bytes`, the stream from a server, come to as the response to a
/// call of `method`, which asked to close or not, under `limits`.
#[must_use]
pub fn response(bytes: &[u8], method: Method, close: bool, limits: &Limits) -> Response {
    let mut reader = Lines { bytes, at: 0 };
    let mut out = Response { head: None, body: Vec::new(), outcome: Outcome::Done(Reuse::Keep), used: 0 };
    let mut budget = limits.head;
    let head = loop {
        match head(&mut reader, &mut budget, limits) {
            Ok(head) if head.status == 101 => return failed(out, &reader, Error::Upgrade),
            Ok(head) if (100..200).contains(&head.status) => {}
            Ok(head) => break head,
            Err(error) => return failed(out, &reader, error),
        }
    };
    let framing = match framing(method, &head) {
        Ok(framing) => framing,
        Err(error) => return failed(out, &reader, error),
    };
    let persists = persistent(&head) && !close && framing != Framing::UntilEnd;
    out.head = Some(Head { framing, ..head });
    let result = match framing {
        Framing::Empty => Ok(()),
        Framing::Length(length) => reader.take(length, &mut out.body),
        Framing::Chunked => chunked(&mut reader, limits, &mut out.body),
        Framing::UntilEnd => {
            out.body.extend_from_slice(&bytes[reader.at..]);
            reader.at = bytes.len();
            Ok(())
        }
    };
    match result {
        Ok(()) => {
            out.outcome = Outcome::Done(if persists { Reuse::Keep } else { Reuse::Close });
            out.used = reader.at;
            out
        }
        Err(error) => failed(out, &reader, error),
    }
}

fn failed(mut out: Response, reader: &Lines<'_>, error: Error) -> Response {
    out.outcome = Outcome::Failed(error);
    out.used = reader.at;
    out
}

/// A line read as the client's scan to LF of at most `max` reads it.
enum Line<'a> {
    /// Its content, without its LF and a CR before it, and its length.
    Whole(&'a [u8], usize),
    /// No LF within `max`: too long.
    Long,
    /// No LF, and fewer than `max` bytes before the end: never seen.
    Short,
}

struct Lines<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Lines<'a> {
    fn line(&mut self, max: u32) -> Line<'a> {
        let rest = &self.bytes[self.at..];
        let max = usize::try_from(max).expect("fits a usize");
        let window = &rest[..max.min(rest.len())];
        match window.iter().position(|&byte| byte == b'\n') {
            Some(lf) => {
                self.at += lf + 1;
                let content = &window[..lf];
                Line::Whole(content.strip_suffix(b"\r").unwrap_or(content), lf + 1)
            }
            None if window.len() == max => Line::Long,
            None => Line::Short,
        }
    }

    /// `n` bytes of the body, or as many as there are and `Truncated`.
    fn take(&mut self, n: u64, body: &mut Vec<u8>) -> Result<(), Error> {
        let rest = &self.bytes[self.at..];
        let n = usize::try_from(n).unwrap_or(usize::MAX);
        let taken = n.min(rest.len());
        body.extend_from_slice(&rest[..taken]);
        self.at += taken;
        if taken < n { Err(Error::Truncated { answered: true }) } else { Ok(()) }
    }
}

/// A head, interim or final, within what is left of `budget`.
fn head(reader: &mut Lines<'_>, budget: &mut u32, limits: &Limits) -> Result<Head, Error> {
    let mut head = Head { version: Version::Http11, status: 0, headers: Vec::new(), framing: Framing::Empty };
    let mut first = true;
    loop {
        let (content, len) = match reader.line(*budget) {
            Line::Whole(content, len) => (content, len),
            Line::Long => return Err(Error::HeadTooLong),
            // Answered once any line of a head, interim or final, came.
            Line::Short => return Err(Error::Truncated { answered: *budget < limits.head }),
        };
        *budget -= u32::try_from(len).expect("fits a u32");
        if first {
            (head.version, head.status) = status_line(content)?;
            first = false;
        } else if content.is_empty() {
            return Ok(head);
        } else if content[0] == b' ' || content[0] == b'\t' {
            let more = trim(content);
            check_value(more)?;
            let Some((_, value)) = head.headers.last_mut() else { return Err(Error::Header) };
            if !more.is_empty() {
                if !value.is_empty() {
                    value.push(b' ');
                }
                value.extend_from_slice(more);
            }
        } else {
            let colon = content.iter().position(|&byte| byte == b':');
            let name_end = colon.unwrap_or(content.len());
            if name_end == 0 || colon.is_none() || !content[..name_end].iter().all(|&byte| is_tchar(byte)) {
                return Err(Error::Header);
            }
            let value = trim(&content[name_end + 1..]);
            check_value(value)?;
            if head.headers.len() >= usize::try_from(limits.headers).expect("fits a usize") {
                return Err(Error::TooManyHeaders);
            }
            head.headers.push((content[..name_end].to_vec(), value.to_vec()));
        }
        if *budget == 0 {
            return Err(Error::HeadTooLong);
        }
    }
}

fn status_line(line: &[u8]) -> Result<(Version, u16), Error> {
    let rest = line.strip_prefix(b"HTTP/").ok_or(Error::Status)?;
    let [major, b'.', minor, rest @ ..] = rest else { return Err(Error::Status) };
    if !major.is_ascii_digit() || !minor.is_ascii_digit() {
        return Err(Error::Status);
    }
    if *major != b'1' {
        return Err(Error::Version);
    }
    let version = if *minor == b'0' { Version::Http10 } else { Version::Http11 };
    let [b' ', a, b, c, rest @ ..] = rest else { return Err(Error::Status) };
    if !(b'1'..=b'5').contains(a) || !b.is_ascii_digit() || !c.is_ascii_digit() {
        return Err(Error::Status);
    }
    let code = u16::from(a - b'0') * 100 + u16::from(b - b'0') * 10 + u16::from(c - b'0');
    match rest {
        [] => {}
        [b' ', reason @ ..] if reason.iter().all(|&byte| is_field_byte(byte)) => {}
        _ => return Err(Error::Status),
    }
    Ok((version, code))
}

fn is_tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn is_field_byte(byte: u8) -> bool {
    byte == b'\t' || (b' '..=b'~').contains(&byte) || byte >= 0x80
}

fn check_value(value: &[u8]) -> Result<(), Error> {
    if value.iter().all(|&byte| is_field_byte(byte)) { Ok(()) } else { Err(Error::Header) }
}

fn trim(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|&byte| byte != b' ' && byte != b'\t').unwrap_or(bytes.len());
    let end = bytes.iter().rposition(|&byte| byte != b' ' && byte != b'\t').map_or(start, |at| at + 1);
    &bytes[start..end.max(start)]
}

/// The non-empty elements of every field named `name`, trimmed.
fn elements<'a>(head: &'a Head, name: &str) -> (bool, Vec<&'a [u8]>) {
    let mut present = false;
    let mut elements = Vec::new();
    for (field, value) in &head.headers {
        if field.eq_ignore_ascii_case(name.as_bytes()) {
            present = true;
            for element in value.split(|&byte| byte == b',') {
                let element = trim(element);
                if !element.is_empty() {
                    elements.push(element);
                }
            }
        }
    }
    (present, elements)
}

fn framing(method: Method, head: &Head) -> Result<Framing, Error> {
    if method == Method::Head || head.status == 204 || head.status == 304 {
        return Ok(Framing::Empty);
    }
    let (coded, codings) = elements(head, "transfer-encoding");
    let (lengthed, lengths) = elements(head, "content-length");
    if coded {
        let chunked = codings.len() == 1 && codings[0].eq_ignore_ascii_case(b"chunked");
        if !chunked || lengthed || head.version == Version::Http10 {
            return Err(Error::Framing);
        }
        return Ok(Framing::Chunked);
    }
    if !lengthed {
        return Ok(Framing::UntilEnd);
    }
    let mut length = None;
    for element in lengths {
        let parsed = std::str::from_utf8(element)
            .ok()
            .filter(|text| text.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or(Error::Framing)?;
        if length.is_some_and(|earlier| earlier != parsed) {
            return Err(Error::Framing);
        }
        length = Some(parsed);
    }
    length.map(Framing::Length).ok_or(Error::Framing)
}

fn persistent(head: &Head) -> bool {
    let (_, tokens) = elements(head, "connection");
    let close = tokens.iter().any(|token| token.eq_ignore_ascii_case(b"close"));
    let keep = tokens.iter().any(|token| token.eq_ignore_ascii_case(b"keep-alive"));
    match head.version {
        Version::Http11 => !close,
        Version::Http10 => keep && !close,
    }
}

fn chunked(reader: &mut Lines<'_>, limits: &Limits, body: &mut Vec<u8>) -> Result<(), Error> {
    loop {
        let size = match reader.line(limits.head) {
            Line::Whole(content, _) => chunk_size(content).ok_or(Error::ChunkSize)?,
            Line::Long => return Err(Error::ChunkSize),
            Line::Short => return Err(Error::Truncated { answered: true }),
        };
        if size == 0 {
            let mut budget = limits.head;
            loop {
                match reader.line(budget) {
                    Line::Whole([], _) => return Ok(()),
                    Line::Whole(_, len) => {
                        budget -= u32::try_from(len).expect("fits a u32");
                        if budget == 0 {
                            return Err(Error::Trailer);
                        }
                    }
                    Line::Long => return Err(Error::Trailer),
                    Line::Short => return Err(Error::Truncated { answered: true }),
                }
            }
        }
        reader.take(size, body)?;
        // Its line ending: a scan to LF of at most two bytes.
        let rest = &reader.bytes[reader.at..];
        match rest {
            [b'\n', ..] => reader.at += 1,
            [b'\r', b'\n', ..] => reader.at += 2,
            [_, _, ..] => return Err(Error::Chunk),
            [_] | [] => return Err(Error::Truncated { answered: true }),
        }
    }
}

fn chunk_size(line: &[u8]) -> Option<u64> {
    let digits = line.iter().take_while(|byte| byte.is_ascii_hexdigit()).count();
    if digits == 0 {
        return None;
    }
    let mut size: u64 = 0;
    for &byte in &line[..digits] {
        size = size.checked_mul(16)?.checked_add(u64::from(char::from(byte).to_digit(16)?))?;
    }
    let rest = trim(&line[digits..]);
    match rest.first() {
        None => Some(size),
        Some(b';') if rest.iter().all(|&byte| is_field_byte(byte)) => Some(size),
        Some(_) => None,
    }
}

/// How an event stream ended.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Ending {
    Ended,
    Failed(sse::Error),
}

/// An event, as the reference reads it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Event {
    pub name: Vec<u8>,
    pub data: Vec<u8>,
    pub id: Vec<u8>,
}

/// What an event stream read whole comes to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Events {
    pub events: Vec<Event>,
    pub ending: Ending,
    /// The reconnection time the last `retry` of digits set.
    pub retry: Option<u64>,
    /// The last event ID, as the last blank line left it.
    pub last_id: Vec<u8>,
    /// Where in the bytes a failure arose: the byte that passed a limit.
    pub failed_at: Option<usize>,
}

/// What `bytes`, the whole of a body, come to as an event stream under
/// `limits` (WHATWG HTML, 9.2.6): read a byte at a time by the standard,
/// with the reader's limits on lines, events and fields. Bytes after the
/// last line end are an incomplete line, which the end of the stream
/// drops.
#[must_use]
pub fn events(bytes: &[u8], limits: &sse::Limits) -> Events {
    let mut stream = Stream::new();
    for (at, &byte) in bytes.iter().enumerate() {
        if let Err(error) = stream.byte(byte, limits) {
            return stream.ended(Ending::Failed(error), Some(at));
        }
    }
    stream.ended(Ending::Ended, None)
}

/// The standard's parser, a byte at a time.
struct Stream {
    events: Vec<Event>,
    bom: usize,
    bom_over: bool,
    line: Vec<u8>,
    after_cr: bool,
    size: u64,
    data: Vec<u8>,
    name: Vec<u8>,
    id: Vec<u8>,
    last_id: Vec<u8>,
    retry: Option<u64>,
}

const BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

impl Stream {
    fn new() -> Stream {
        Stream {
            events: Vec::new(),
            bom: 0,
            bom_over: false,
            line: Vec::new(),
            after_cr: false,
            size: 0,
            data: Vec::new(),
            name: Vec::new(),
            id: Vec::new(),
            last_id: Vec::new(),
            retry: None,
        }
    }

    fn ended(self, ending: Ending, failed_at: Option<usize>) -> Events {
        Events { events: self.events, ending, retry: self.retry, last_id: self.last_id, failed_at }
    }

    fn byte(&mut self, byte: u8, limits: &sse::Limits) -> Result<(), sse::Error> {
        if !self.bom_over {
            if BOM[self.bom] == byte {
                self.bom += 1;
                self.bom_over = self.bom == BOM.len();
                return Ok(());
            }
            self.bom_over = true;
            for earlier in BOM[..self.bom].to_vec() {
                self.line_byte(earlier, limits)?;
            }
        }
        self.line_byte(byte, limits)
    }

    fn line_byte(&mut self, byte: u8, limits: &sse::Limits) -> Result<(), sse::Error> {
        // A line ends at CRLF, at LF, or at CR alone: an LF right after a CR
        // is the CRLF's.
        let paired = self.after_cr && byte == b'\n';
        self.after_cr = false;
        // Every byte counts against the event, endings included, from the
        // line end that dispatched the last one.
        self.size += 1;
        if self.size > u64::from(limits.event) {
            return Err(sse::Error::EventTooLong);
        }
        if paired {
            return Ok(());
        }
        match byte {
            b'\r' => {
                self.after_cr = true;
                self.end_line();
                Ok(())
            }
            b'\n' => {
                self.end_line();
                Ok(())
            }
            _ => {
                self.line.push(byte);
                if self.line.len() > usize::try_from(limits.line).expect("fits a usize") {
                    return Err(sse::Error::LineTooLong);
                }
                // A type or an id fails as soon as its value is too long.
                let (name, value) = split(&self.line);
                let field = usize::try_from(limits.field).expect("fits a usize");
                if (name == b"event" || name == b"id") && value.is_some_and(|value| value.len() > field) {
                    return Err(sse::Error::FieldTooLong);
                }
                Ok(())
            }
        }
    }

    fn end_line(&mut self) {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            self.dispatch();
            return;
        }
        if line[0] == b':' {
            return;
        }
        let (name, value) = split(&line);
        let value = value.unwrap_or(b"");
        match name {
            b"data" => {
                self.data.extend_from_slice(value);
                self.data.push(b'\n');
            }
            b"event" => self.name = value.to_vec(),
            b"id" if !value.contains(&0) => self.id = value.to_vec(),
            b"retry" if !value.is_empty() && value.iter().all(u8::is_ascii_digit) => {
                if let Ok(retry) = std::str::from_utf8(value).expect("digits are ASCII").parse::<u64>() {
                    self.retry = Some(retry);
                }
            }
            _ => {}
        }
    }

    fn dispatch(&mut self) {
        self.size = 0;
        self.last_id.clone_from(&self.id);
        if self.data.is_empty() {
            self.name.clear();
            return;
        }
        self.data.pop();
        let name = if self.name.is_empty() { b"message".to_vec() } else { std::mem::take(&mut self.name) };
        self.events.push(Event { name, data: std::mem::take(&mut self.data), id: self.last_id.clone() });
    }
}

/// A line's field name and its value, if it has a colon: the value
/// without the one space after the colon.
fn split(line: &[u8]) -> (&[u8], Option<&[u8]>) {
    match line.iter().position(|&byte| byte == b':') {
        Some(colon) => {
            let value = &line[colon + 1..];
            (&line[..colon], Some(value.strip_prefix(b" ").unwrap_or(value)))
        }
        None => (line, None),
    }
}
