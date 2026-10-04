//! The transcripts (testing-strategy.md, 4.1): responses kept as files in
//! `transcripts/`, each `<name>.http` beside `<name>.expect`, what it must
//! decode to.
//!
//! An expectation lists the response's head, then its body, or the events
//! its body holds, then the exchange's outcome:
//!
//! ```text
//! # a comment
//! limits head 16384 headers 64 read 4096   (optional; any of request, head, headers, read, send)
//! sse line 4096 event 65536 field 64 chunk 64   (optional; any of them)
//! method HEAD                                    (optional; GET by default)
//! status HTTP/1.1 200
//! header Content-Type "text/event-stream"        (each field, in order)
//! framing chunked                                (or: length 42, until-end, empty)
//! body "{\"ok\":true}"                           (the body; lines joined, if it is not events)
//! event "message_start" "{...}"                  (an event: its type and data; id "x" after, if any)
//! sse ended                                      (the events' outcome: or sse failed event-too-long)
//! done keep                                      (or: done close, failed truncated, ...)
//! ```
//!
//! Text is quoted, with `\\`, `\"`, `\n`, `\r`, `\t` and `\xHH` escapes.
//! The limits not given are [`LIMITS`]' and [`SSE`]'.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use skein_http::client::{Error, Framing, Limits, Method, Reuse, Version};
use skein_http::sse;

use crate::reference::{self, Ending, Events, Head, Outcome};

/// The client's limits a transcript is read with, unless its expectation
/// says otherwise.
pub const LIMITS: Limits = Limits { request: 4096, head: 16384, headers: 64, read: 4096, send: 4096 };

/// The reader's limits a transcript's events are read with, unless its
/// expectation says otherwise.
pub const SSE: sse::Limits = sse::Limits { line: 1 << 14, event: 1 << 14, field: 256, chunk: 4096 };

/// A transcript and what it must decode to.
#[derive(Clone, Debug)]
pub struct Transcript {
    pub name: String,
    pub bytes: Vec<u8>,
    pub limits: Limits,
    pub sse: sse::Limits,
    pub method: Method,
    /// What it must decode to: `None` while it has no expectation yet.
    pub expected: Option<Decoded>,
}

/// What a response decodes to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Decoded {
    pub head: Option<Head>,
    /// The body, unless it holds events.
    pub body: Option<Vec<u8>>,
    /// The events the body holds, if it holds them.
    pub events: Option<Events>,
    pub outcome: Outcome,
}

/// Every transcript, in the order of their names.
#[must_use]
pub fn all() -> Vec<Transcript> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("transcripts");
    let mut names = Vec::new();
    for entry in fs::read_dir(&directory).expect("the transcripts directory") {
        let path = entry.expect("a directory entry").path();
        if path.extension().is_some_and(|extension| extension == "http") {
            names.push(path.file_stem().expect("a name").to_string_lossy().into_owned());
        }
    }
    names.sort();
    let mut transcripts = Vec::new();
    for name in names {
        let bytes = fs::read(directory.join(format!("{name}.http"))).expect("the response");
        let transcript = match fs::read(directory.join(format!("{name}.expect"))) {
            Ok(text) => {
                let parsed = parse(&text).unwrap_or_else(|error| panic!("{name}.expect: {error}"));
                Transcript {
                    name,
                    bytes,
                    limits: parsed.limits,
                    sse: parsed.sse,
                    method: parsed.method,
                    expected: Some(parsed.decoded),
                }
            }
            Err(_) => Transcript { name, bytes, limits: LIMITS, sse: SSE, method: Method::Get, expected: None },
        };
        transcripts.push(transcript);
    }
    transcripts
}

/// What the reference readers make of `transcript`: the response, and the
/// events in its body if its type says it holds them.
#[must_use]
pub fn read(transcript: &Transcript) -> Decoded {
    let response = reference::response(&transcript.bytes, transcript.method, false, &transcript.limits);
    let streams = response.head.as_ref().is_some_and(|head| {
        head.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(b"content-type") && value.to_ascii_lowercase().starts_with(b"text/event-stream")
        })
    });
    let (body, events) = if streams {
        (None, Some(reference::events(&response.body, &transcript.sse)))
    } else if response.head.is_some() {
        (Some(response.body), None)
    } else {
        (None, None)
    };
    Decoded { head: response.head, body, events, outcome: response.outcome }
}

/// An expectation, as parsed.
#[derive(Clone, Debug)]
pub struct Parsed {
    pub limits: Limits,
    pub sse: sse::Limits,
    pub method: Method,
    pub decoded: Decoded,
}

/// An expectation's limits, method and decoding.
///
/// # Errors
///
/// What is wrong with the expectation, and on which line.
#[expect(clippy::too_many_lines, reason = "one arm for each kind of line")]
pub fn parse(text: &[u8]) -> Result<Parsed, String> {
    let mut limits = LIMITS;
    let mut sse = SSE;
    let mut method = Method::Get;
    let mut head: Option<Head> = None;
    let mut body: Option<Vec<u8>> = None;
    let mut events: Option<Vec<reference::Event>> = None;
    let mut ending: Option<Ending> = None;
    let mut outcome = None;
    for (number, line) in text.split(|&byte| byte == b'\n').enumerate() {
        let line = line.trim_ascii();
        if line.is_empty() || line.starts_with(b"#") {
            continue;
        }
        let at = |message: &str| format!("line {}: {message}", number + 1);
        if outcome.is_some() {
            return Err(at("nothing follows the outcome"));
        }
        let (keyword, rest) = word(line);
        match keyword {
            b"limits" => {
                for (name, value) in pairs(rest).ok_or_else(|| at("a limit is a name and a number"))? {
                    match name {
                        b"request" => limits.request = value,
                        b"head" => limits.head = value,
                        b"headers" => limits.headers = value,
                        b"read" => limits.read = value,
                        b"send" => limits.send = value,
                        _ => return Err(at("an unknown limit")),
                    }
                }
            }
            b"sse" if rest.starts_with(b"ended") || rest.starts_with(b"failed") => {
                ending = Some(match word_of(rest) {
                    (b"ended", _) => Ending::Ended,
                    (_, error) => Ending::Failed(sse_error(error).ok_or_else(|| at("an event stream error's name"))?),
                });
            }
            b"sse" => {
                for (name, value) in pairs(rest).ok_or_else(|| at("a limit is a name and a number"))? {
                    match name {
                        b"line" => sse.line = value,
                        b"event" => sse.event = value,
                        b"field" => sse.field = value,
                        b"chunk" => sse.chunk = value,
                        _ => return Err(at("an unknown limit")),
                    }
                }
            }
            b"method" => {
                method = match rest {
                    b"GET" => Method::Get,
                    b"HEAD" => Method::Head,
                    b"POST" => Method::Post,
                    _ => return Err(at("GET, HEAD or POST")),
                }
            }
            b"status" => {
                let (version, code) = word(rest);
                let version = match version {
                    b"HTTP/1.1" => Version::Http11,
                    b"HTTP/1.0" => Version::Http10,
                    _ => return Err(at("HTTP/1.1 or HTTP/1.0")),
                };
                let status =
                    std::str::from_utf8(code).ok().and_then(|code| code.parse().ok()).ok_or_else(|| at("a code"))?;
                head = Some(Head { version, status, headers: Vec::new(), framing: Framing::Empty });
            }
            b"header" => {
                let (name, value) = word(rest);
                let value = unquote(value).ok_or_else(|| at("a quoted value"))?;
                head.as_mut().ok_or_else(|| at("a field after the status"))?.headers.push((name.to_vec(), value));
            }
            b"framing" => {
                let framing = match word(rest) {
                    (b"chunked", _) => Framing::Chunked,
                    (b"until-end", _) => Framing::UntilEnd,
                    (b"empty", _) => Framing::Empty,
                    (b"length", length) => Framing::Length(
                        std::str::from_utf8(length)
                            .ok()
                            .and_then(|length| length.parse().ok())
                            .ok_or_else(|| at("a length"))?,
                    ),
                    _ => return Err(at("chunked, until-end, empty or length N")),
                };
                head.as_mut().ok_or_else(|| at("framing after the status"))?.framing = framing;
            }
            b"body" => body.get_or_insert_with(Vec::new).extend(unquote(rest).ok_or_else(|| at("a quoted body"))?),
            b"event" => {
                let (name, rest) = quoted(rest).ok_or_else(|| at("a quoted type"))?;
                let (data, rest) = quoted(rest).ok_or_else(|| at("quoted data"))?;
                let id = match word(rest) {
                    (b"", _) => Vec::new(),
                    (b"id", id) => unquote(id).ok_or_else(|| at("a quoted id"))?,
                    _ => return Err(at("an event's type, data, then its id if any")),
                };
                events.get_or_insert_with(Vec::new).push(reference::Event { name, data, id });
            }
            b"done" => {
                outcome = Some(Outcome::Done(match rest {
                    b"keep" => Reuse::Keep,
                    b"close" => Reuse::Close,
                    _ => return Err(at("done keep, or done close")),
                }));
            }
            b"failed" => outcome = Some(Outcome::Failed(error(rest).ok_or_else(|| at("an error's name"))?)),
            _ => return Err(at("an unknown line")),
        }
    }
    let outcome = outcome.ok_or("no outcome")?;
    let events = match (events, ending) {
        (None, None) => None,
        (events, Some(ending)) => {
            let last_id =
                events.as_ref().and_then(|events| events.last()).map(|event| event.id.clone()).unwrap_or_default();
            Some(Events { events: events.unwrap_or_default(), ending, retry: None, last_id })
        }
        (Some(_), None) => return Err("events without an sse outcome".into()),
    };
    if events.is_some() && body.is_some() {
        return Err("a body, or its events, not both".into());
    }
    let body = if events.is_none() && head.is_some() { Some(body.unwrap_or_default()) } else { body };
    Ok(Parsed { limits, sse, method, decoded: Decoded { head, body, events, outcome } })
}

/// A draft of an expectation for `transcript`, from what the reference
/// readers make of it: to be read, and checked against the response by
/// hand, before it is kept.
#[must_use]
pub fn render(transcript: &Transcript, decoded: &Decoded) -> String {
    let mut out = String::new();
    if transcript.limits != LIMITS {
        let Limits { request, head, headers, read, send } = transcript.limits;
        writeln!(out, "limits request {request} head {head} headers {headers} read {read} send {send}")
            .expect("writing to a String");
    }
    if transcript.sse != SSE {
        let sse::Limits { line, event, field, chunk } = transcript.sse;
        writeln!(out, "sse line {line} event {event} field {field} chunk {chunk}").expect("writing to a String");
    }
    if let Some(head) = &decoded.head {
        let version = match head.version {
            Version::Http10 => "HTTP/1.0",
            Version::Http11 => "HTTP/1.1",
        };
        writeln!(out, "status {version} {}", head.status).expect("writing to a String");
        for (name, value) in &head.headers {
            writeln!(out, "header {} {}", String::from_utf8_lossy(name), quote(value)).expect("writing to a String");
        }
        match head.framing {
            Framing::Empty => out.push_str("framing empty\n"),
            Framing::Length(length) => writeln!(out, "framing length {length}").expect("writing to a String"),
            Framing::Chunked => out.push_str("framing chunked\n"),
            Framing::UntilEnd => out.push_str("framing until-end\n"),
        }
    }
    if let Some(body) = &decoded.body
        && !body.is_empty()
    {
        for line in body.split_inclusive(|&byte| byte == b'\n') {
            writeln!(out, "body {}", quote(line)).expect("writing to a String");
        }
    }
    if let Some(events) = &decoded.events {
        for event in &events.events {
            write!(out, "event {} {}", quote(&event.name), quote(&event.data)).expect("writing to a String");
            if !event.id.is_empty() {
                write!(out, " id {}", quote(&event.id)).expect("writing to a String");
            }
            out.push('\n');
        }
        match events.ending {
            Ending::Ended => out.push_str("sse ended\n"),
            Ending::Failed(error) => {
                writeln!(out, "sse failed {}", sse_error_name(error)).expect("writing to a String");
            }
        }
    }
    match decoded.outcome {
        Outcome::Done(Reuse::Keep) => out.push_str("done keep\n"),
        Outcome::Done(Reuse::Close) => out.push_str("done close\n"),
        Outcome::Failed(error) => writeln!(out, "failed {}", error_name(error)).expect("writing to a String"),
    }
    out
}

const ERRORS: [(Error, &str); 12] = [
    (Error::Closed, "closed"),
    (Error::Truncated, "truncated"),
    (Error::Status, "status"),
    (Error::Version, "version"),
    (Error::Header, "header"),
    (Error::HeadTooLong, "head-too-long"),
    (Error::TooManyHeaders, "too-many-headers"),
    (Error::Framing, "framing"),
    (Error::ChunkSize, "chunk-size"),
    (Error::Chunk, "chunk"),
    (Error::Trailer, "trailer"),
    (Error::Upgrade, "upgrade"),
];

fn error(name: &[u8]) -> Option<Error> {
    ERRORS.iter().find(|(_, known)| known.as_bytes() == name).map(|(error, _)| *error)
}

/// An error's name in an expectation.
#[must_use]
pub fn error_name(error: Error) -> &'static str {
    ERRORS.iter().find(|(known, _)| *known == error).map_or("other", |(_, name)| name)
}

const SSE_ERRORS: [(sse::Error, &str); 3] = [
    (sse::Error::LineTooLong, "line-too-long"),
    (sse::Error::EventTooLong, "event-too-long"),
    (sse::Error::FieldTooLong, "field-too-long"),
];

fn sse_error(name: &[u8]) -> Option<sse::Error> {
    SSE_ERRORS.iter().find(|(_, known)| known.as_bytes() == name).map(|(error, _)| *error)
}

fn sse_error_name(error: sse::Error) -> &'static str {
    SSE_ERRORS.iter().find(|(known, _)| *known == error).map_or("stream", |(_, name)| name)
}

/// The first word of `line`, and the rest after the spaces that follow it.
fn word(line: &[u8]) -> (&[u8], &[u8]) {
    match line.iter().position(|&byte| byte == b' ') {
        Some(space) => (&line[..space], line[space + 1..].trim_ascii_start()),
        None => (line, &[]),
    }
}

fn word_of(line: &[u8]) -> (&[u8], &[u8]) {
    word(line)
}

/// `name number` pairs.
fn pairs(line: &[u8]) -> Option<Vec<(&[u8], u32)>> {
    let words: Vec<&[u8]> = line.split(|&byte| byte == b' ').filter(|word| !word.is_empty()).collect();
    let mut pairs = Vec::new();
    for pair in words.chunks(2) {
        let [name, value] = pair else { return None };
        pairs.push((*name, std::str::from_utf8(value).ok()?.parse().ok()?));
    }
    Some(pairs)
}

/// Text in quotes, as an expectation writes it: printable ASCII as it is,
/// and the rest escaped.
fn quote(text: &[u8]) -> String {
    let mut out = String::from("\"");
    for chunk in text.utf8_chunks() {
        for character in chunk.valid().chars() {
            match character {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                _ if character.is_control() => {
                    let mut buffer = [0; 4];
                    for byte in character.encode_utf8(&mut buffer).as_bytes() {
                        write!(out, "\\x{byte:02x}").expect("writing to a String");
                    }
                }
                _ => out.push(character),
            }
        }
        for byte in chunk.invalid() {
            write!(out, "\\x{byte:02x}").expect("writing to a String");
        }
    }
    out.push('"');
    out
}

/// The text of `"..."`, unescaped, which must be all of `quoted`.
fn unquote(quoted: &[u8]) -> Option<Vec<u8>> {
    match self::quoted(quoted)? {
        (text, []) => Some(text),
        _ => None,
    }
}

/// The text of the `"..."` `line` begins with, unescaped, and the rest
/// after it.
fn quoted(line: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let inner = line.strip_prefix(b"\"")?;
    let mut text = Vec::new();
    let mut at = 0;
    while at < inner.len() {
        let byte = inner[at];
        at += 1;
        match byte {
            b'"' => return Some((text, inner[at..].trim_ascii_start())),
            b'\\' => {
                let escape = *inner.get(at)?;
                at += 1;
                match escape {
                    b'\\' => text.push(b'\\'),
                    b'"' => text.push(b'"'),
                    b'n' => text.push(b'\n'),
                    b'r' => text.push(b'\r'),
                    b't' => text.push(b'\t'),
                    b'x' => {
                        let digits = std::str::from_utf8(inner.get(at..at + 2)?).ok()?;
                        text.push(u8::from_str_radix(digits, 16).ok()?);
                        at += 2;
                    }
                    _ => return None,
                }
            }
            _ => text.push(byte),
        }
    }
    None
}
