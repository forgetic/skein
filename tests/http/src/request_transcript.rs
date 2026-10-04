//! The request transcripts (testing-strategy.md, 4.1): requests kept as
//! files in `transcripts/requests/`, each `<name>.http` beside
//! `<name>.expect`, what the server must make of it, served by a side
//! above that reads each body to its end and answers `200` with `ok`.
//!
//! An expectation lists what each `Next` came to, in order:
//!
//! ```text
//! # a comment
//! limits head 16384 headers 64 body 1048576   (optional; any of head, headers, body, read, response, send)
//! request POST /v1/messages HTTP/1.1          (a call: its request line)
//! header Content-Type "application/json"      (each field, in order)
//! framing length 42                           (or: chunked, none)
//! body "{\"ok\":true}"                        (its body; lines joined)
//! done keep                                   (its outcome: done close, failed truncated, ...)
//! ended                                       (a Next the client's end answered)
//! rejected head-too-long                      (a Next a rejection answered)
//! failed truncated                            (a Next with no call that failed)
//! ```
//!
//! Text is quoted as in the response transcripts. The limits not given are
//! [`LIMITS`]'.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use skein_http::server::{self, Body, Error, Rejection, Response, Reuse};
use skein_http::{Method, Version};

use crate::reference::{self, RequestEnding, RequestHead};
use crate::server_world::{Outcome, Plan, When};
use crate::transcript::{quote, quoted, unquote, word};

/// The server's limits a transcript is read with, unless its expectation
/// says otherwise.
pub const LIMITS: server::Limits =
    server::Limits { head: 16384, headers: 64, body: 1 << 20, read: 4096, response: 4096, send: 4096 };

/// A request transcript, and what it must come to.
#[derive(Clone, Debug)]
pub struct Transcript {
    pub name: String,
    pub bytes: Vec<u8>,
    pub limits: server::Limits,
    /// What it must come to: `None` while it has no expectation yet.
    pub expected: Option<Vec<Entry>>,
}

/// What one `Next` came to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Entry {
    /// The call's head, if one answered.
    pub head: Option<RequestHead>,
    /// Its body.
    pub body: Vec<u8>,
    pub outcome: Outcome,
}

/// The plan the side above follows for every call of a transcript: it reads
/// the body to its end, then answers `200` with `ok`.
#[must_use]
pub fn plan() -> Plan {
    let response = Response { status: 200, headers: Box::new([]), body: Body::Length(2), close: false };
    Plan { response, reply: b"ok".to_vec(), when: When::AfterBody }
}

/// Every request transcript, in the order of their names.
#[must_use]
pub fn all() -> Vec<Transcript> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("transcripts").join("requests");
    let mut names = Vec::new();
    for entry in fs::read_dir(&directory).expect("the request transcripts directory") {
        let path = entry.expect("a directory entry").path();
        if path.extension().is_some_and(|extension| extension == "http") {
            names.push(path.file_stem().expect("a name").to_string_lossy().into_owned());
        }
    }
    names.sort();
    let mut transcripts = Vec::new();
    for name in names {
        let bytes = fs::read(directory.join(format!("{name}.http"))).expect("the request");
        let transcript = match fs::read(directory.join(format!("{name}.expect"))) {
            Ok(text) => {
                let (limits, entries) = parse(&text).unwrap_or_else(|error| panic!("{name}.expect: {error}"));
                Transcript { name, bytes, limits, expected: Some(entries) }
            }
            Err(_) => Transcript { name, bytes, limits: LIMITS, expected: None },
        };
        transcripts.push(transcript);
    }
    transcripts
}

/// What the reference reader makes of `transcript`, served by [`plan`]:
/// each request in turn, until one is not kept.
#[must_use]
pub fn read(transcript: &Transcript) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut offset = 0;
    for _ in 0..1000 {
        let request = reference::request(&transcript.bytes[offset..], &transcript.limits);
        let (outcome, more) = match request.ending {
            RequestEnding::None => (Outcome::Ended, false),
            RequestEnding::Rejected(rejection) => (Outcome::Failed(Error::Rejected(rejection)), false),
            RequestEnding::Failed(error) => (Outcome::Failed(error), false),
            RequestEnding::Whole if request.persist => (Outcome::Done(Reuse::Keep), true),
            RequestEnding::Whole => (Outcome::Done(Reuse::Close), false),
        };
        let body = if request.head.is_some() { request.body } else { Vec::new() };
        entries.push(Entry { head: request.head, body, outcome });
        if !more {
            return entries;
        }
        offset += request.used;
    }
    panic!("a transcript holds fewer than a thousand requests");
}

/// A draft of an expectation for `transcript`, from what the reference
/// reader makes of it: to be read, and checked against the requests by
/// hand, before it is kept.
#[must_use]
pub fn render(transcript: &Transcript, entries: &[Entry]) -> String {
    let mut out = String::new();
    if transcript.limits != LIMITS {
        let server::Limits { head, headers, body, read, response, send } = transcript.limits;
        writeln!(out, "limits head {head} headers {headers} body {body} read {read} response {response} send {send}")
            .expect("writing to a String");
    }
    for entry in entries {
        if let Some(head) = &entry.head {
            let version = match head.version {
                Version::Http10 => "HTTP/1.0",
                Version::Http11 => "HTTP/1.1",
            };
            let method = String::from_utf8_lossy(head.method.as_bytes()).into_owned();
            let target = String::from_utf8_lossy(&head.target).into_owned();
            writeln!(out, "request {method} {target} {version}").expect("writing to a String");
            for (name, value) in &head.headers {
                writeln!(out, "header {} {}", String::from_utf8_lossy(name), quote(value))
                    .expect("writing to a String");
            }
            match head.body {
                Body::None => out.push_str("framing none\n"),
                Body::Length(length) => writeln!(out, "framing length {length}").expect("writing to a String"),
                Body::Chunked => out.push_str("framing chunked\n"),
            }
            for line in entry.body.split_inclusive(|&byte| byte == b'\n') {
                writeln!(out, "body {}", quote(line)).expect("writing to a String");
            }
        }
        match entry.outcome {
            Outcome::Ended => out.push_str("ended\n"),
            Outcome::Done(Reuse::Keep) => out.push_str("done keep\n"),
            Outcome::Done(Reuse::Close) => out.push_str("done close\n"),
            Outcome::Failed(Error::Rejected(rejection)) => {
                writeln!(out, "rejected {}", rejection_name(rejection)).expect("writing to a String");
            }
            Outcome::Failed(error) => writeln!(out, "failed {}", error_name(error)).expect("writing to a String"),
        }
    }
    out
}

/// An expectation's limits and entries.
///
/// # Errors
///
/// What is wrong with the expectation, and on which line.
pub fn parse(text: &[u8]) -> Result<(server::Limits, Vec<Entry>), String> {
    let mut limits = LIMITS;
    let mut entries = Vec::new();
    let mut head: Option<RequestHead> = None;
    let mut body = Vec::new();
    for (number, line) in text.split(|&byte| byte == b'\n').enumerate() {
        let line = line.trim_ascii();
        if line.is_empty() || line.starts_with(b"#") {
            continue;
        }
        let at = |message: &str| format!("line {}: {message}", number + 1);
        let (keyword, rest) = word(line);
        let outcome = match keyword {
            b"limits" => {
                let words: Vec<&[u8]> = rest.split(|&byte| byte == b' ').filter(|word| !word.is_empty()).collect();
                for pair in words.chunks(2) {
                    let [name, value] = pair else { return Err(at("a limit is a name and a number")) };
                    let value: u64 = std::str::from_utf8(value)
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .ok_or_else(|| at("a number"))?;
                    let small = || u32::try_from(value).map_err(|_| at("a u32"));
                    match *name {
                        b"head" => limits.head = small()?,
                        b"headers" => limits.headers = small()?,
                        b"body" => limits.body = value,
                        b"read" => limits.read = small()?,
                        b"response" => limits.response = small()?,
                        b"send" => limits.send = small()?,
                        _ => return Err(at("an unknown limit")),
                    }
                }
                continue;
            }
            b"request" => {
                let parts: Vec<&[u8]> = rest.split(|&byte| byte == b' ').collect();
                let [method, target, version] = parts[..] else { return Err(at("a method, a target, a version")) };
                let method = Method::from_bytes(method).ok_or_else(|| at("a method"))?;
                let version = match version {
                    b"HTTP/1.1" => Version::Http11,
                    b"HTTP/1.0" => Version::Http10,
                    _ => return Err(at("HTTP/1.1 or HTTP/1.0")),
                };
                head = Some(RequestHead {
                    method,
                    target: target.to_vec(),
                    version,
                    headers: Vec::new(),
                    body: Body::None,
                });
                continue;
            }
            b"header" => {
                let (name, value) = word(rest);
                let value = unquote(value).ok_or_else(|| at("a quoted value"))?;
                head.as_mut().ok_or_else(|| at("a field after the request"))?.headers.push((name.to_vec(), value));
                continue;
            }
            b"framing" => {
                let framing = match word(rest) {
                    (b"none", _) => Body::None,
                    (b"chunked", _) => Body::Chunked,
                    (b"length", length) => Body::Length(
                        std::str::from_utf8(length)
                            .ok()
                            .and_then(|length| length.parse().ok())
                            .ok_or_else(|| at("a length"))?,
                    ),
                    _ => return Err(at("none, chunked or length N")),
                };
                head.as_mut().ok_or_else(|| at("framing after the request"))?.body = framing;
                continue;
            }
            b"body" => {
                let (text, _) = quoted(rest).ok_or_else(|| at("a quoted body"))?;
                body.extend(text);
                continue;
            }
            b"ended" => Outcome::Ended,
            b"done" => Outcome::Done(match rest {
                b"keep" => Reuse::Keep,
                b"close" => Reuse::Close,
                _ => return Err(at("done keep, or done close")),
            }),
            b"rejected" => Outcome::Failed(Error::Rejected(rejection(rest).ok_or_else(|| at("a rejection's name"))?)),
            b"failed" => Outcome::Failed(error(rest).ok_or_else(|| at("an error's name"))?),
            _ => return Err(at("an unknown line")),
        };
        entries.push(Entry { head: head.take(), body: std::mem::take(&mut body), outcome });
    }
    if head.is_some() || !body.is_empty() {
        return Err("a request without its outcome".into());
    }
    Ok((limits, entries))
}

const REJECTIONS: [(Rejection, &str); 11] = [
    (Rejection::RequestLine, "request-line"),
    (Rejection::TargetTooLong, "target-too-long"),
    (Rejection::Version, "version"),
    (Rejection::Method, "method"),
    (Rejection::Header, "header"),
    (Rejection::HeadTooLong, "head-too-long"),
    (Rejection::TooManyHeaders, "too-many-headers"),
    (Rejection::Host, "host"),
    (Rejection::Framing, "framing"),
    (Rejection::Coding, "coding"),
    (Rejection::BodyTooLong, "body-too-long"),
];

fn rejection(name: &[u8]) -> Option<Rejection> {
    REJECTIONS.iter().find(|(_, known)| known.as_bytes() == name).map(|(rejection, _)| *rejection)
}

fn rejection_name(rejection: Rejection) -> &'static str {
    REJECTIONS.iter().find(|(known, _)| *known == rejection).map_or("other", |(_, name)| name)
}

const ERRORS: [(Error, &str); 6] = [
    (Error::Truncated, "truncated"),
    (Error::ChunkSize, "chunk-size"),
    (Error::Chunk, "chunk"),
    (Error::Trailer, "trailer"),
    (Error::Extensions, "extensions"),
    (Error::BodyTooLong, "body-too-long"),
];

fn error(name: &[u8]) -> Option<Error> {
    ERRORS.iter().find(|(_, known)| known.as_bytes() == name).map(|(error, _)| *error)
}

fn error_name(error: Error) -> &'static str {
    ERRORS.iter().find(|(known, _)| *known == error).map_or("other", |(_, name)| name)
}
