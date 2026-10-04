//! Calls, responses and event streams from a seed (testing-strategy.md,
//! 2.4): valid ones, written as servers write them and with the liberties
//! the standards allow, and those mutated.

use skein_http::Header;
use skein_http::client::{Body, Call, Limits, Method, Refusal};
use skein_lib::Rng;

fn pick<'a, T>(rng: &mut Rng, items: &'a [T]) -> &'a T {
    &items[usize::try_from(rng.below(items.len() as u64)).expect("fits a usize")]
}

fn draw(rng: &mut Rng, low: usize, high: usize) -> usize {
    usize::try_from(rng.between(low as u64, high as u64)).expect("fits a usize")
}

/// Text a value or a body is made of: printable ASCII, tabs, and UTF-8 of
/// two, three and four bytes.
fn text(rng: &mut Rng, low: usize, high: usize, out: &mut Vec<u8>) {
    const PIECES: &[&str] =
        &["a", "b", "z", "Q", "0", "9", " ", "\t", "-", "/", "=", ";", ",", ":", "\"", "é", "€", "😀"];
    for _ in 0..draw(rng, low, high) {
        out.extend_from_slice(pick(rng, PIECES).as_bytes());
    }
}

/// A call, and the body the side above uploads for it. Now and then the
/// call gets one thing wrong that the client refuses (http.md, 3.1).
#[must_use]
pub fn call(rng: &mut Rng) -> (Call, Vec<u8>) {
    let method = *pick(
        rng,
        &[
            Method::Get,
            Method::Get,
            Method::Get,
            Method::Post,
            Method::Post,
            Method::Head,
            Method::Put,
            Method::Delete,
            Method::Options,
            Method::Patch,
        ],
    );
    let mut target = b"/".to_vec();
    for _ in 0..draw(rng, 0, 3) {
        target.extend_from_slice(
            pick(rng, &["v1/", "messages", "repos/ai/temper", "issues?state=open", "%20"]).as_bytes(),
        );
    }
    let mut headers = vec![Header { name: b"Host".to_vec().into(), value: b"example.com".to_vec().into() }];
    for _ in 0..draw(rng, 0, 3) {
        let name = pick(rng, &["Accept", "content-type", "X-Api-Key", "Authorization", "User-Agent"]).as_bytes();
        let mut value = Vec::new();
        text(rng, 0, 12, &mut value);
        headers.push(Header { name: name.to_vec().into(), value: value.into() });
    }
    let upload = match method {
        Method::Post | Method::Put | Method::Patch if rng.chance(900) => {
            let mut body = Vec::new();
            text(rng, 0, 120, &mut body);
            Some(body)
        }
        Method::Post | Method::Put | Method::Patch | Method::Get | Method::Head | Method::Delete | Method::Options => {
            None
        }
    };
    let body = match &upload {
        Some(body) => Body::Length(body.len() as u64),
        None => Body::None,
    };
    if rng.chance(20) {
        flaw(rng, &mut target, &mut headers);
    }
    let call = Call { method, target: target.into(), headers: headers.into(), body, close: rng.chance(100) };
    (call, upload.unwrap_or_default())
}

/// One thing wrong with a call: its target, a field's name or value, a
/// field the client writes itself, or its `Host`, none or two.
fn flaw(rng: &mut Rng, target: &mut Vec<u8>, headers: &mut Vec<Header>) {
    let header = |name: &[u8], value: &[u8]| Header { name: name.to_vec().into(), value: value.to_vec().into() };
    let bad = match rng.below(6) {
        0 => {
            let at = draw(rng, 0, target.len());
            match rng.below(3) {
                0 => target.clear(),
                1 => target.insert(at, *pick(rng, &[b' ', 0x7F, 0xC3, b'\r', b'\n', 0])),
                _ => target.splice(at..at, b"\r\nX: y".iter().copied()).for_each(drop),
            }
            return;
        }
        1 => header(one_of(rng, &[b"", b"Bad Name", b"Name:", b"X\x01", b"caf\xC3\xA9"]), b"v"),
        2 => header(b"X-Value", one_of(rng, &[b"a\r\nInjected: yes", b"a\nb", b"a\0b", b"\x7F", b"\x1B[0m"])),
        3 => header(one_of(rng, &[b"Content-Length", b"transfer-encoding", b"CONNECTION"]), b"5"),
        4 => {
            headers.retain(|header| !header.name.eq_ignore_ascii_case(b"host"));
            return;
        }
        _ => header(b"host", b"other.example"),
    };
    let at = draw(rng, 0, headers.len());
    headers.insert(at, bad);
}

fn one_of(rng: &mut Rng, items: &[&'static [u8]]) -> &'static [u8] {
    items[draw(rng, 0, items.len() - 1)]
}

/// Why the client must refuse `call` under `limits`, if it must: the first
/// thing wrong, in the order of http.md, 3.1, read independently of the
/// client's checks.
#[must_use]
pub fn refusal(call: &Call, limits: &Limits) -> Option<Refusal> {
    if call.target.is_empty() || !call.target.iter().all(u8::is_ascii_graphic) {
        return Some(Refusal::Target);
    }
    for header in &call.headers {
        let token = |byte: &u8| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(byte);
        if header.name.is_empty() || !header.name.iter().all(token) {
            return Some(Refusal::Name);
        }
        if header.value.iter().any(|&byte| byte.is_ascii_control() && byte != b'\t') {
            return Some(Refusal::Value);
        }
        let name = header.name.to_ascii_lowercase();
        if [&b"content-length"[..], b"transfer-encoding", b"connection"].contains(&&name[..]) {
            return Some(Refusal::Reserved);
        }
    }
    if call.headers.iter().filter(|header| header.name.eq_ignore_ascii_case(b"host")).count() != 1 {
        return Some(Refusal::Host);
    }
    if request(call).len() > usize::try_from(limits.request).expect("fits a usize") {
        return Some(Refusal::TooLong);
    }
    None
}

/// The head a call must be written as: a writer of its own, independent of
/// the client's.
#[must_use]
pub fn request(call: &Call) -> Vec<u8> {
    let method = match call.method {
        Method::Get => "GET",
        Method::Head => "HEAD",
        Method::Post => "POST",
        Method::Put => "PUT",
        Method::Patch => "PATCH",
        Method::Delete => "DELETE",
        Method::Options => "OPTIONS",
    };
    let mut out = format!("{method} ").into_bytes();
    out.extend_from_slice(&call.target);
    out.extend_from_slice(b" HTTP/1.1\r\n");
    for header in &call.headers {
        out.extend_from_slice(&header.name);
        out.extend_from_slice(b": ");
        out.extend_from_slice(&header.value);
        out.extend_from_slice(b"\r\n");
    }
    let length = match (call.body, call.method) {
        (Body::Length(length), _) => Some(length),
        (Body::None, Method::Post | Method::Put | Method::Patch) => Some(0),
        (Body::None, _) => None,
    };
    if let Some(length) = length {
        out.extend_from_slice(format!("Content-Length: {length}\r\n").as_bytes());
    }
    if call.close {
        out.extend_from_slice(b"Connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// How a generated response frames its body.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Frame {
    Length,
    Chunked,
    UntilEnd,
}

/// A valid response to a call of `method`, its body drawn by `body`: a
/// server's way of writing it, the head's line endings, its fields' case
/// and folds, interim heads before it, and its body's framing all drawn at
/// random. `last` says whether nothing follows it on the connection, so
/// that its body may run to the end of the stream.
#[must_use]
pub fn response(rng: &mut Rng, method: Method, body: &[u8], last: bool) -> Vec<u8> {
    let lf_only = rng.chance(150);
    let mixed = rng.chance(100);
    let mut out = Vec::new();
    let line = |rng: &mut Rng, out: &mut Vec<u8>, content: &[u8]| {
        out.extend_from_slice(content);
        let lf = lf_only || (mixed && rng.chance(500));
        out.extend_from_slice(if lf { b"\n" } else { b"\r\n" });
    };
    if rng.chance(100) {
        line(rng, &mut out, b"HTTP/1.1 100 Continue");
        line(rng, &mut out, b"");
    }
    if rng.chance(50) {
        line(rng, &mut out, b"HTTP/1.1 103 Early Hints");
        line(rng, &mut out, b"Link: </style.css>; rel=preload");
        line(rng, &mut out, b"");
    }
    let http10 = rng.chance(80);
    let status = *pick(rng, &[200_u16, 200, 200, 200, 201, 204, 304, 400, 404, 429, 500, 503]);
    let reason = pick(rng, &[" OK", " ", "", " Some Reason", " \u{2713}"]);
    let version = if http10 { "1.0" } else { "1.1" };
    line(rng, &mut out, format!("HTTP/{version} {status}{reason}").as_bytes());
    let bodyless = method == Method::Head || status == 204 || status == 304;
    let frame = if http10 || (last && rng.chance(150)) {
        if last && rng.chance(500) { Frame::UntilEnd } else { Frame::Length }
    } else if rng.chance(500) {
        Frame::Chunked
    } else {
        Frame::Length
    };
    // Each field, and now and then the obsolete fold of its value onto a
    // line of its own after it: only the server's own fields fold.
    let mut fields: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for _ in 0..draw(rng, 0, 4) {
        let name = pick(rng, &["Date", "Server", "content-type", "X-Request-Id", "CACHE-CONTROL", "Vary", "ETag"]);
        let mut field = format!("{name}:").into_bytes();
        field.extend_from_slice(pick(rng, &[" ", "", "  ", "\t"]).as_bytes());
        text(rng, 0, 16, &mut field);
        let fold = if rng.chance(60) {
            let mut fold = pick(rng, &[" ", "\t", "  "]).as_bytes().to_vec();
            fold.push(b'x');
            text(rng, 0, 6, &mut fold);
            Some(trim_end(fold))
        } else {
            None
        };
        fields.push((trim_end(field), fold));
    }
    match frame {
        Frame::Length if !bodyless || rng.chance(300) => {
            let name = pick(rng, &["Content-Length", "content-length", "CONTENT-LENGTH"]);
            fields.push((format!("{name}: {}", body.len()).into_bytes(), None));
        }
        Frame::Chunked => {
            let field = pick(rng, &["Transfer-Encoding: chunked", "transfer-encoding: Chunked"]);
            fields.push((field.as_bytes().to_vec(), None));
        }
        Frame::Length | Frame::UntilEnd => {}
    }
    if http10 && rng.chance(500) {
        fields.push((b"Connection: keep-alive".to_vec(), None));
    } else if rng.chance(100) {
        fields.push((b"Connection: close".to_vec(), None));
    }
    shuffle(rng, &mut fields);
    for (field, fold) in &fields {
        line(rng, &mut out, field);
        if let Some(fold) = fold {
            line(rng, &mut out, fold);
        }
    }
    line(rng, &mut out, b"");
    if bodyless {
        return out;
    }
    match frame {
        Frame::Length | Frame::UntilEnd => out.extend_from_slice(body),
        Frame::Chunked => chunks(rng, body, &mut out),
    }
    out
}

/// Without the whitespace it ends with, as a field's value is read.
fn trim_end(mut field: Vec<u8>) -> Vec<u8> {
    while field.last().is_some_and(|byte| *byte == b' ' || *byte == b'\t') {
        field.pop();
    }
    field
}

fn shuffle<T>(rng: &mut Rng, items: &mut [T]) {
    for at in (1..items.len()).rev() {
        let other = usize::try_from(rng.below(at as u64 + 1)).expect("fits a usize");
        items.swap(at, other);
    }
}

/// `body` as chunks of sizes drawn at random, each size in either case and
/// with leading zeros now and then, an extension now and then, lines ended
/// by CRLF or LF, then the last chunk and a trailer section.
fn chunks(rng: &mut Rng, body: &[u8], out: &mut Vec<u8>) {
    let lf = |rng: &mut Rng| if rng.chance(100) { &b"\n"[..] } else { &b"\r\n"[..] };
    let mut rest = body;
    while !rest.is_empty() {
        let size = draw(rng, 1, 48).min(rest.len());
        let (chunk, after) = rest.split_at(size);
        let digits = if rng.chance(500) { format!("{size:x}") } else { format!("{size:X}") };
        let most = if rng.chance(100) { 4 } else { 0 };
        let zeros = "0".repeat(draw(rng, 0, most));
        out.extend_from_slice(format!("{zeros}{digits}").as_bytes());
        if rng.chance(100) {
            out.extend_from_slice(pick(rng, &[";name=value", " ;a", ";q=\"x y\""]).as_bytes());
        }
        out.extend_from_slice(lf(rng));
        out.extend_from_slice(chunk);
        out.extend_from_slice(lf(rng));
        rest = after;
    }
    out.extend_from_slice(b"0");
    out.extend_from_slice(lf(rng));
    if rng.chance(100) {
        out.extend_from_slice(b"Server-Timing: total;dur=12");
        out.extend_from_slice(lf(rng));
    }
    out.extend_from_slice(lf(rng));
}

/// A body: lines of text, the last one ended or not.
#[must_use]
pub fn body(rng: &mut Rng) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..draw(rng, 0, 8) {
        text(rng, 0, 24, &mut out);
        out.push(b'\n');
    }
    if rng.chance(300) {
        text(rng, 1, 8, &mut out);
    }
    out
}

/// An event stream: events of every field, comments, unknown fields and
/// blank lines between them, lines ended by LF, CRLF or CR, alone or
/// mixed, a byte order mark now and then, and the last event cut short
/// now and then.
#[must_use]
pub fn events(rng: &mut Rng) -> Vec<u8> {
    let style = rng.below(10);
    let mut out = Vec::new();
    if rng.chance(50) {
        out.extend_from_slice(&[0xEF, 0xBB, 0xBF]);
    }
    let ending = |rng: &mut Rng| -> &'static [u8] {
        match style {
            0..=4 => b"\n",
            5..=6 => b"\r\n",
            7 => b"\r",
            _ => {
                let endings: [&'static [u8]; 3] = [b"\n", b"\r\n", b"\r"];
                endings[usize::try_from(rng.below(3)).expect("fits a usize")]
            }
        }
    };
    for _ in 0..draw(rng, 0, 10) {
        let mut lines: Vec<Vec<u8>> = Vec::new();
        if rng.chance(300) {
            let mut line = b"event:".to_vec();
            space(rng, &mut line);
            line.extend_from_slice(pick(rng, &["message_start", "content_block_delta", "ping", "x"]).as_bytes());
            lines.push(line);
        }
        if rng.chance(150) {
            let mut line = b"id:".to_vec();
            space(rng, &mut line);
            text(rng, 0, 6, &mut line);
            lines.push(line);
        }
        if rng.chance(100) {
            lines.push(format!("retry: {}", rng.below(100_000)).into_bytes());
        }
        if rng.chance(150) {
            let mut line = b":".to_vec();
            text(rng, 0, 12, &mut line);
            lines.push(line);
        }
        if rng.chance(50) {
            lines.push(pick(rng, &[&b"foo: bar"[..], b"data", b"datum: x", b"event", b"id"]).to_vec());
        }
        let least = usize::from(rng.chance(900));
        for _ in 0..draw(rng, least, 3) {
            let mut line = b"data:".to_vec();
            space(rng, &mut line);
            text(rng, 0, 32, &mut line);
            lines.push(line);
        }
        shuffle(rng, &mut lines);
        for line in lines {
            out.extend_from_slice(&line);
            out.extend_from_slice(ending(rng));
        }
        out.extend_from_slice(ending(rng));
        let blank = if rng.chance(100) { 2 } else { 0 };
        for _ in 0..draw(rng, 0, blank) {
            out.extend_from_slice(ending(rng));
        }
    }
    if rng.chance(100) {
        out.extend_from_slice(b"data: cut short");
    }
    out
}

fn space(rng: &mut Rng, line: &mut Vec<u8>) {
    if rng.chance(800) {
        line.push(b' ');
    }
}

/// `server` with one corruption a hostile or broken server makes, where a
/// random edit seldom lands: another major version, a code past 599, an
/// upgrade nobody asked for, a coding the client does not undo, framing
/// headers that conflict, a chunk size past a `u64`, or a trailer section
/// that does not end.
#[must_use]
pub fn corrupt(rng: &mut Rng, server: &[u8]) -> Vec<u8> {
    let mut out = server.to_vec();
    let at_status = find(&out, b"HTTP/1.");
    match rng.below(7) {
        0 => {
            if let Some(at) = at_status {
                out[at + 5] = b'2';
            }
        }
        1 => {
            if let Some(at) = at_status {
                out.splice(at + 9..(at + 12).min(out.len()), *b"600");
            }
        }
        2 => {
            out.splice(0..0, *b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n");
        }
        3 | 4 => {
            let field: &[u8] = if rng.chance(500) {
                b"Transfer-Encoding: gzip\r\n"
            } else {
                b"Transfer-Encoding: chunked\r\nContent-Length: 1\r\n"
            };
            if let Some(at) = find(&out, b"\n") {
                out.splice((at + 1)..=at, field.iter().copied());
            }
        }
        5 => {
            if let Some(at) = find(&out, b"\r\n0\r\n") {
                out.splice(at + 2..at + 3, *b"1FFFFFFFFFFFFFFFF");
            }
        }
        _ => {
            let mut trailers = Vec::new();
            for _ in 0..200 {
                trailers.extend_from_slice(b"Trailer-Field: a value that goes on\r\n");
            }
            if let Some(at) = find(&out, b"\n0\r\n") {
                out.splice(at + 4..at + 4, trailers);
            }
        }
    }
    out
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

/// Bytes a mutation inserts or writes over another: the framing of heads,
/// chunks and event streams, digits, and bytes no text allows.
const BYTES: &[u8] = b"\r\n:; \t0123456789aAfFxX-,HTP/.\x00\x01\x7f\x80\xef\xbb\xbf\xff";

/// `bytes` with a few edits drawn at random.
#[must_use]
pub fn mutate(rng: &mut Rng, bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for _ in 0..rng.between(1, 3) {
        let at = usize::try_from(rng.below(out.len() as u64 + 1)).expect("fits a usize");
        let byte = *pick(rng, BYTES);
        match rng.below(6) {
            0 | 1 => out.insert(at, byte),
            2 if at < out.len() => out[at] = byte,
            3 if at < out.len() => {
                out.remove(at);
            }
            4 => {
                let end = (at + draw(rng, 1, 16)).min(out.len());
                let copied = out[at..end].to_vec();
                let to = usize::try_from(rng.below(out.len() as u64 + 1)).expect("fits a usize");
                out.splice(to..to, copied);
            }
            _ => out.truncate(at),
        }
    }
    out
}
