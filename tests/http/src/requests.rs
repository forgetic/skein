//! The server's side of the world's traffic (testing-strategy.md, 2.4):
//! requests from a seed, as clients write them and with the liberties the
//! standards allow, and those corrupted; the responses a service gives
//! them, now and then with one thing wrong; and a writer of the head the
//! server must write for a response, independent of the server's.

use skein_http::server::{Body, Limits, Refusal, Response};
use skein_http::{Header, Method, Version};
use skein_lib::Rng;

use crate::generate::{chunks, draw, find, pick, shuffle, text};

/// A request as a client writes it: its head's line endings and its
/// fields' case and spacing drawn at random, its body by length or in
/// chunks, an `Expect: 100-continue` or a `Connection` field now and then.
#[must_use]
#[expect(clippy::too_many_lines, reason = "one draw for each part of a request, in the order it is written")]
pub fn request(rng: &mut Rng) -> Vec<u8> {
    let lf_only = rng.chance(150);
    let mixed = rng.chance(100);
    let mut out = Vec::new();
    let line = |rng: &mut Rng, out: &mut Vec<u8>, content: &[u8]| {
        out.extend_from_slice(content);
        let lf = lf_only || (mixed && rng.chance(500));
        out.extend_from_slice(if lf { b"\n" } else { b"\r\n" });
    };
    if rng.chance(30) {
        line(rng, &mut out, b"");
    }
    let method = *pick(
        rng,
        &[
            Method::Get,
            Method::Get,
            Method::Get,
            Method::Post,
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
    if rng.chance(30) {
        target = b"http://example.com/v1/models".to_vec();
    } else if method == Method::Options && rng.chance(300) {
        target = b"*".to_vec();
    }
    let version: &[u8] = if rng.chance(100) {
        b"HTTP/1.0"
    } else if rng.chance(20) {
        b"HTTP/1.2"
    } else {
        b"HTTP/1.1"
    };
    let http10 = version == b"HTTP/1.0";
    let mut request_line = method.as_bytes().to_vec();
    request_line.push(b' ');
    request_line.extend_from_slice(&target);
    request_line.push(b' ');
    request_line.extend_from_slice(version);
    line(rng, &mut out, &request_line);
    let mut fields: Vec<Vec<u8>> = Vec::new();
    if !http10 || rng.chance(500) {
        let name = *pick(rng, &["Host", "host", "HOST"]);
        fields.push(field(rng, name, b"example.com"));
    }
    for _ in 0..draw(rng, 0, 4) {
        let name =
            *pick(rng, &["Accept", "content-type", "User-Agent", "x-api-key", "Authorization", "anthropic-version"]);
        let mut value = Vec::new();
        text(rng, 0, 12, &mut value);
        fields.push(field(rng, name, trim_end(&value)));
    }
    let body = match method {
        Method::Post | Method::Put | Method::Patch if rng.chance(900) => {
            let mut body = Vec::new();
            text(rng, 0, 160, &mut body);
            Some(body)
        }
        Method::Post | Method::Put | Method::Patch | Method::Get | Method::Head | Method::Delete | Method::Options => {
            None
        }
    };
    let chunked = body.is_some() && !http10 && rng.chance(400);
    match &body {
        Some(_) if chunked => {
            let name = *pick(rng, &["Transfer-Encoding", "transfer-encoding"]);
            let coding = *pick(rng, &["chunked", "Chunked"]);
            fields.push(field(rng, name, coding.as_bytes()));
        }
        Some(body) => {
            let name = *pick(rng, &["Content-Length", "content-length"]);
            fields.push(field(rng, name, body.len().to_string().as_bytes()));
        }
        None if method != Method::Get && rng.chance(300) => fields.push(field(rng, "Content-Length", b"0")),
        None => {}
    }
    if body.is_some() && rng.chance(150) {
        fields.push(field(rng, "Expect", b"100-continue"));
    }
    if http10 && rng.chance(500) {
        fields.push(field(rng, "Connection", b"keep-alive"));
    } else if rng.chance(100) {
        fields.push(field(rng, "Connection", b"close"));
    }
    shuffle(rng, &mut fields);
    for field in &fields {
        line(rng, &mut out, field);
    }
    line(rng, &mut out, b"");
    match body {
        Some(body) if chunked => chunks(rng, &body, &mut out),
        Some(body) => out.extend_from_slice(&body),
        None => {}
    }
    out
}

/// `name: value`, with the spaces a client may put around the value.
fn field(rng: &mut Rng, name: &str, value: &[u8]) -> Vec<u8> {
    let mut field = format!("{name}:").into_bytes();
    field.extend_from_slice(pick(rng, &[" ", "", "  ", "\t"]).as_bytes());
    field.extend_from_slice(value);
    if !value.is_empty() && rng.chance(100) {
        field.push(b' ');
    }
    field
}

fn trim_end(value: &[u8]) -> &[u8] {
    let mut end = value.len();
    while end > 0 && (value[end - 1] == b' ' || value[end - 1] == b'\t') {
        end -= 1;
    }
    &value[..end]
}

/// `client` with one corruption a hostile or broken client makes, where a
/// random edit seldom lands: another major version, a method the server
/// does not know, an obsolete fold, whitespace before a colon, both
/// framing headers, a coding not undone, two lengths, a chunk size past a
/// `u64`, no `Host`, a field past the head, too many fields, a request line
/// past the head, a body past the limit, a trailer section that never
/// ends, or HTTP/2's preface.
#[must_use]
pub fn corrupt(rng: &mut Rng, client: &[u8]) -> Vec<u8> {
    let mut out = client.to_vec();
    let after_line = find(&out, b"\n").map_or(0, |at| at + 1);
    let insert = |out: &mut Vec<u8>, bytes: &[u8]| {
        out.splice(after_line..after_line, bytes.iter().copied());
    };
    match rng.below(15) {
        0 => {
            if let Some(at) = find(&out, b" HTTP/1.") {
                out[at + 6] = b'2';
            }
        }
        1 => {
            if let Some(at) = find(&out, b" ") {
                out.splice(..at, *b"PROPFIND");
            }
        }
        2 => insert(&mut out, b"X-Folded: a\r\n continued\r\n"),
        3 => insert(&mut out, b"X-Space : a\r\n"),
        4 => insert(&mut out, b"Transfer-Encoding: chunked\r\nContent-Length: 1\r\n"),
        5 => insert(&mut out, b"Transfer-Encoding: gzip, chunked\r\n"),
        6 => insert(&mut out, b"Content-Length: 1\r\nContent-Length: 2\r\n"),
        7 => {
            if let Some(at) = find(&out, b"\r\n0\r\n") {
                out.splice(at + 2..at + 3, *b"1FFFFFFFFFFFFFFFF");
            } else {
                insert(&mut out, b"Transfer-Encoding: chunked\r\n");
                let end = find(&out, b"\r\n\r\n").map_or(out.len(), |at| at + 4);
                out.splice(end..end, *b"FFFFFFFFFFFFFFFFF\r\n");
            }
        }
        8 => {
            // Every line of the head that is a `Host`, gone.
            let end = find(&out, b"\n\r\n").or_else(|| find(&out, b"\n\n")).map_or(out.len(), |at| at + 1);
            let mut head: Vec<u8> = Vec::new();
            for line in out[..end].split_inclusive(|&byte| byte == b'\n') {
                if !line.to_ascii_lowercase().starts_with(b"host:") {
                    head.extend_from_slice(line);
                }
            }
            head.extend_from_slice(&out[end..]);
            out = head;
        }
        9 => {
            let mut huge = b"X-Huge: ".to_vec();
            huge.resize(5000, b'h');
            huge.extend_from_slice(b"\r\n");
            insert(&mut out, &huge);
        }
        10 => {
            let mut many = Vec::new();
            for n in 0..80 {
                many.extend_from_slice(format!("X-{n}: v\r\n").as_bytes());
            }
            insert(&mut out, &many);
        }
        11 => {
            if let Some(at) = find(&out, b" ") {
                let mut long = vec![b'/'; 1];
                long.resize(6000, b'a');
                let start = at + 1;
                out.splice(start..start, long);
            }
        }
        12 => insert(&mut out, b"Content-Length: 99999999999\r\n"),
        13 => {
            // A chunked body's last chunk, then trailer fields past the head.
            let mut trailers = Vec::new();
            for _ in 0..200 {
                trailers.extend_from_slice(b"Trailer-Field: a value that goes on\r\n");
            }
            if let Some(at) = find(&out, b"\r\n0\r\n") {
                let start = at + 5;
                out.splice(start..start, trailers);
            } else {
                insert(&mut out, b"Transfer-Encoding: chunked\r\n");
                let end = find(&out, b"\r\n\r\n").map_or(out.len(), |at| at + 4);
                let mut body = b"0\r\n".to_vec();
                body.extend_from_slice(&trailers);
                out.splice(end..end, body);
            }
        }
        _ => out = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec(),
    }
    out
}

/// The response a service gives a call of `method`, and the body it
/// writes for it: a status, the fields an API answers with, and a body by
/// length, in chunks, or none; now and then one that closes, and one in
/// fifty with one thing wrong, which the server refuses (http.md, 5.4).
#[must_use]
pub fn response(rng: &mut Rng) -> (Response, Vec<u8>) {
    let status = *pick(rng, &[200_u16, 200, 200, 200, 201, 204, 304, 400, 401, 404, 413, 429, 500, 503, 529]);
    let mut headers = Vec::new();
    for _ in 0..draw(rng, 0, 4) {
        let name = pick(rng, &["Content-Type", "X-Request-Id", "cache-control", "request-id", "Retry-After", "Vary"]);
        let mut value = Vec::new();
        text(rng, 0, 16, &mut value);
        headers.push(header(name.as_bytes(), trim_end(&value).trim_ascii_start()));
    }
    let mut body = Vec::new();
    let framing = if status == 204 || status == 304 || rng.chance(100) {
        Body::None
    } else {
        body = crate::generate::body(rng);
        if rng.chance(500) { Body::Chunked } else { Body::Length(body.len() as u64) }
    };
    let mut response = Response { status, headers: headers.into(), body: framing, close: rng.chance(100) };
    if rng.chance(20) {
        flaw(rng, &mut response);
    }
    (response, body)
}

/// One thing wrong with a response: its status, a field's name or value,
/// a field the server writes itself, a body for a status that takes none,
/// or a head too long.
fn flaw(rng: &mut Rng, response: &mut Response) {
    let mut headers = response.headers.to_vec();
    match rng.below(6) {
        0 => response.status = *pick(rng, &[0_u16, 100, 101, 199, 600, 999]),
        1 => {
            let name = *pick(rng, &[&b""[..], b"Bad Name", b"Name:", b"X\x01"]);
            headers.push(header(name, b"v"));
        }
        2 => {
            let value = *pick(rng, &[&b"a\r\nInjected: yes"[..], b"a\nb", b"a\0b"]);
            headers.push(header(b"X-Value", value));
        }
        3 => {
            let name = *pick(rng, &[&b"Content-Length"[..], b"transfer-encoding", b"CONNECTION"]);
            headers.push(header(name, b"5"));
        }
        4 => {
            response.status = *pick(rng, &[204_u16, 304]);
            response.body = *pick(rng, &[Body::Length(3), Body::Chunked]);
        }
        _ => headers.push(header(b"X-Huge", &vec![b'h'; 5000])),
    }
    response.headers = headers.into();
}

fn header(name: &[u8], value: &[u8]) -> Header {
    Header { name: name.into(), value: value.into() }
}

/// Why the server must refuse `response` under `limits`, to a request in
/// `version`, on a connection that persists after it or not, if it must:
/// the first thing wrong, in the order of http.md, 5.4, read independently
/// of the server's checks.
#[must_use]
pub fn refusal(response: &Response, version: Version, persist: bool, limits: &Limits) -> Option<Refusal> {
    if !(200..=599).contains(&response.status) {
        return Some(Refusal::Status);
    }
    for header in &response.headers {
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
    if (response.status == 204 || response.status == 304) && response.body != Body::None {
        return Some(Refusal::Body);
    }
    if head(response, version, persist).len() > usize::try_from(limits.response).expect("fits a usize") {
        return Some(Refusal::TooLong);
    }
    None
}

/// The head the server must write for `response` to a request in
/// `version`, on a connection that persists after it or not: a writer of
/// the test's own.
#[must_use]
pub fn head(response: &Response, version: Version, persist: bool) -> Vec<u8> {
    let mut out = format!("HTTP/1.1 {} ", response.status).into_bytes();
    out.extend_from_slice(reason(response.status).as_bytes());
    out.extend_from_slice(b"\r\n");
    for header in &response.headers {
        out.extend_from_slice(&header.name);
        out.extend_from_slice(b": ");
        out.extend_from_slice(&header.value);
        out.extend_from_slice(b"\r\n");
    }
    match response.body {
        Body::Length(length) => out.extend_from_slice(format!("Content-Length: {length}\r\n").as_bytes()),
        Body::None if response.status != 204 && response.status != 304 => {
            out.extend_from_slice(b"Content-Length: 0\r\n");
        }
        Body::Chunked if version == Version::Http11 => out.extend_from_slice(b"Transfer-Encoding: chunked\r\n"),
        Body::None | Body::Chunked => {}
    }
    if !persist {
        out.extend_from_slice(b"Connection: close\r\n");
    } else if version == Version::Http10 {
        out.extend_from_slice(b"Connection: keep-alive\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// The reason phrase RFC 9110, 15, gives a status, or none.
fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        203 => "Non-Authoritative Information",
        204 => "No Content",
        205 => "Reset Content",
        206 => "Partial Content",
        300 => "Multiple Choices",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Content Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        417 => "Expectation Failed",
        421 => "Misdirected Request",
        422 => "Unprocessable Content",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        _ => "",
    }
}

/// The answer the server writes for a request it rejects: its status, and
/// nothing else but that the connection closes.
#[must_use]
pub fn answer(status: u16) -> Vec<u8> {
    format!("HTTP/1.1 {status} {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", reason(status)).into_bytes()
}

/// `piece` as the chunk the server sends it in.
#[must_use]
pub fn chunk(piece: &[u8]) -> Vec<u8> {
    let mut out = format!("{:x}\r\n", piece.len()).into_bytes();
    out.extend_from_slice(piece);
    out.extend_from_slice(b"\r\n");
    out
}

/// Whether a response of `status` to a call of `method` sends a body.
#[must_use]
pub fn sends_body(method: Method, response: &Response) -> bool {
    method != Method::Head && response.status != 204 && response.status != 304 && response.body != Body::None
}
