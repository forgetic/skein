//! The HTTP client stacked on the TLS client (tls.md, 5; http.md, 2), as a
//! connection routes between them, against a rustls server in memory that
//! answers one HTTP response: the handshake, then the call once `Ready`
//! came, its body uploaded through TLS's room, the response read through
//! TLS's plaintext stream, and both closed, `close_notify` last; and a
//! response read to the end of the stream, which only the server's
//! `close_notify` makes whole.
//!
//! The stream below is prompt (`drive::Wire`): what is checked here is the
//! routing and what goes through it, not the neighbours' faults, which the
//! worlds of each machine cover.

use std::collections::VecDeque;

use skein_http::Header;
use skein_http::client::{self as http, Body, Call, Framing, Method, Reuse};
use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Queue, Rng, Time};
use skein_tls::Name;
use skein_tls::client::{self as tls, Agreed, Client, Version};
use skein_tls_world::drive::Pair;
use skein_tls_world::pki;
use skein_tls_world::server::Server;
use skein_tls_world::world;

const HTTP: http::Limits = http::Limits { request: 1_024, head: 4_096, headers: 32, read: 4_096, send: 16_384 };
const TLS: tls::Limits = tls::Limits { read: 4_096, send: 16_384, records: 2 * tls::MAX_RECORD };

/// What goes from one machine to the other, in the order it was emitted.
enum Hop {
    Tls(tls::Event),
    Http(http::Event),
    /// A request of the HTTP client's for its stream below, TLS's plaintext.
    Below(Down),
}

/// How the server ends the stream once it answered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ending {
    /// It keeps it open.
    Open,
    /// It sends `close_notify`, then ends it.
    CloseNotify,
    /// It ends it without `close_notify`: a truncation.
    Cut,
}

/// A connection: the TLS client and the HTTP client stacked on it, and a
/// user of the HTTP client that makes one call.
struct Connection {
    tls: Pair,
    http: http::Client,
    env: Env<http::Limits>,
    events: Queue<http::Event>,
    requests: Queue<Down>,
    hops: VecDeque<Hop>,
    call: Call,
    upload: Vec<u8>,
    uploaded: usize,
    agreed: Option<Agreed>,
    status: Option<(u16, Framing)>,
    body: Vec<u8>,
    /// The end or the failure the response's body heard.
    body_over: Option<Up>,
    done: Option<Reuse>,
    http_failed: Option<http::Error>,
    tls_failed: Option<tls::Error>,
    http_closed: bool,
    tls_closed: bool,
    /// Whether the server wrote its response: once.
    responded: bool,
}

impl Connection {
    fn new(call: Call, upload: Vec<u8>) -> Connection {
        // What whoever stacks the machines checks at startup (http.md, 2):
        // the HTTP client's largest demands within TLS's plaintext stream.
        assert!(http::largest_read(&HTTP) <= TLS.read && http::largest_room(&HTTP) <= TLS.send);
        let name = Name::new("skein.test").expect("a name");
        let client = Client::new(&pki::client(&[b"http/1.1"]), name, &TLS);
        let server = pki::Server { alpn: vec![b"http/1.1".to_vec()], ..pki::Server::plain() };
        let env = Env { now: Time::ZERO, wall: pki::VALID, limits: TLS };
        Connection {
            tls: Pair::new(client, env, Server::new(server.config())),
            http: http::Client::new(&HTTP),
            env: Env { now: Time::ZERO, wall: pki::VALID, limits: HTTP },
            events: Queue::with_capacity(4),
            requests: Queue::with_capacity(4),
            hops: VecDeque::new(),
            call,
            upload,
            uploaded: 0,
            agreed: None,
            status: None,
            body: Vec::new(),
            body_over: None,
            done: None,
            http_failed: None,
            tls_failed: None,
            http_closed: false,
            tls_closed: false,
            responded: false,
        }
    }

    /// Runs the connection until both machines are closed, the server
    /// answering `response` once it has the whole request, then ending the
    /// stream as `ending` says.
    fn run(&mut self, response: &[u8], ending: Ending) {
        let events = self.tls.down(tls::Request::Handshake);
        self.hops.extend(events.into_iter().map(Hop::Tls));
        for _ in 0..100_000 {
            self.route();
            if self.tls_closed {
                return;
            }
            let server = &mut self.tls.wire.server;
            if !self.responded && whole(&server.received) {
                server.write(response);
                if ending == Ending::CloseNotify {
                    server.close_notify();
                }
                self.tls.wire.pull();
                self.tls.wire.eof = ending != Ending::Open;
                self.responded = true;
            }
            let events = self.tls.settle();
            self.hops.extend(events.into_iter().map(Hop::Tls));
        }
        panic!("the connection closes");
    }

    /// Routes what the machines emitted, each to the other or to the user,
    /// until nothing is left.
    fn route(&mut self) {
        while let Some(hop) = self.hops.pop_front() {
            match hop {
                Hop::Tls(tls::Event::Ready(agreed)) => {
                    self.agreed = Some(agreed);
                    let call = self.call.clone();
                    self.http_down(http::Request::Call(call));
                    self.demand_upload();
                }
                Hop::Tls(tls::Event::Stream(up)) => {
                    http::up(&mut self.http, &self.env, up, &mut self.events, &mut self.requests);
                    self.emitted();
                }
                Hop::Tls(tls::Event::Failed(error)) => self.tls_failed = Some(error),
                Hop::Tls(tls::Event::Closed) => self.tls_closed = true,
                Hop::Below(down) => {
                    let events = self.tls.down(tls::Request::Stream(down));
                    self.hops.extend(events.into_iter().map(Hop::Tls));
                }
                Hop::Http(event) => self.user(event),
            }
        }
    }

    fn http_down(&mut self, rq: http::Request) {
        http::down(&mut self.http, &self.env, rq, &mut self.events, &mut self.requests);
        self.emitted();
    }

    fn emitted(&mut self) {
        while let Some(event) = self.events.pop() {
            self.hops.push_back(Hop::Http(event));
        }
        while let Some(request) = self.requests.pop() {
            self.hops.push_back(Hop::Below(request));
        }
    }

    /// The HTTP client's user: it uploads the body within the room granted,
    /// reads the response's body, and closes both machines once it is done.
    fn user(&mut self, event: http::Event) {
        match event {
            http::Event::Response(response) => {
                self.status = Some((response.status, response.framing));
                self.demand_body();
            }
            http::Event::Upload(Up::Room) => {
                let left = self.upload.len() - self.uploaded;
                let piece = left.min(usize::try_from(HTTP.send).expect("fits"));
                let bytes = self.upload[self.uploaded..self.uploaded + piece].to_vec();
                self.uploaded += piece;
                self.http_down(http::Request::Upload(Down::Send(bytes.into())));
                self.demand_upload();
            }
            http::Event::Upload(other) => panic!("the upload: {other:?}"),
            http::Event::Body(Up::Bytes(bytes)) => {
                self.body.extend_from_slice(&bytes);
                self.demand_body();
            }
            http::Event::Body(over @ (Up::End | Up::Failed(_))) => self.body_over = Some(over),
            http::Event::Body(Up::Room) => panic!("room for a body read"),
            http::Event::Done(reuse) => {
                self.done = Some(reuse);
                self.http_down(http::Request::Close);
            }
            http::Event::Failed(error) => {
                self.http_failed = Some(error);
                self.http_down(http::Request::Close);
            }
            http::Event::Closed => {
                self.http_closed = true;
                let events = self.tls.down(tls::Request::Close);
                self.hops.extend(events.into_iter().map(Hop::Tls));
            }
        }
    }

    /// The next piece of the body, read by its framing (http.md, 3.3): what
    /// is left of a length, at most a thousand bytes at once; a chunked one,
    /// a byte at a time, as only the client knows where its chunks end.
    fn demand_body(&mut self) {
        let read = match self.status {
            Some((_, Framing::Length(length))) => {
                let left = length - u64::try_from(self.body.len()).expect("fits");
                Read::Fill(u32::try_from(left.min(1_000)).expect("fits").max(1))
            }
            Some((_, Framing::Chunked | Framing::UntilEnd | Framing::Empty)) | None => Read::Fill(1),
        };
        self.http_down(http::Request::Body(Down::Demand { read, room: 0 }));
    }

    /// Room for the rest of the request's body, or its end.
    fn demand_upload(&mut self) {
        if self.call.body == Body::None {
            return;
        }
        if self.uploaded == self.upload.len() {
            self.http_down(http::Request::Upload(Down::Finish));
            return;
        }
        let room = u32::try_from(self.upload.len() - self.uploaded).expect("fits").min(HTTP.send);
        self.http_down(http::Request::Upload(Down::Demand { read: Read::Nothing, room }));
    }
}

/// Whether `received` holds a whole request: its head, and the body its
/// `Content-Length` announces.
fn whole(received: &[u8]) -> bool {
    let Some(end) = received.windows(4).position(|window| window == b"\r\n\r\n") else { return false };
    let head = String::from_utf8_lossy(&received[..end]).to_lowercase();
    let length = match head.split("\r\n").find_map(|line| line.strip_prefix("content-length: ")) {
        Some(length) => length.parse::<usize>().expect("a length"),
        None => 0,
    };
    received.len() >= end + 4 + length
}

fn header(name: &str, value: &str) -> Header {
    Header { name: name.as_bytes().into(), value: value.as_bytes().into() }
}

#[test]
fn a_get_reads_a_response_by_length_over_tls() {
    let body = br#"{"data":[{"id":"model-a"},{"id":"model-b"}]}"#;
    let mut response =
        format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len())
            .into_bytes();
    response.extend_from_slice(body);
    let call = Call {
        method: Method::Get,
        target: b"/v1/models"[..].into(),
        headers: vec![header("Host", "skein.test")].into(),
        body: Body::None,
        close: false,
    };
    let mut connection = Connection::new(call, Vec::new());
    connection.run(&response, Ending::Open);
    assert_eq!(connection.agreed, Some(Agreed { version: Version::Tls13, alpn: Some(b"http/1.1"[..].into()) }));
    assert_eq!(connection.status, Some((200, Framing::Length(u64::try_from(body.len()).expect("fits")))));
    assert_eq!(connection.body, body);
    assert_eq!(connection.done, Some(Reuse::Keep));
    assert!(connection.http_closed && connection.tls_closed);
    let server = &connection.tls.wire.server;
    assert_eq!(server.received, b"GET /v1/models HTTP/1.1\r\nHost: skein.test\r\n\r\n");
    assert!(server.closed, "close_notify after the exchange");
}

#[test]
fn a_post_uploads_records_of_its_body_and_reads_a_chunked_response() {
    let mut rng = Rng::new(1);
    let upload = world::text(&mut rng, 40_000);
    let body = world::text(&mut rng, 30_000);
    let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    for chunk in body.chunks(7_000) {
        response.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        response.extend_from_slice(chunk);
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"0\r\n\r\n");
    let call = Call {
        method: Method::Post,
        target: b"/v1/messages"[..].into(),
        headers: vec![header("Host", "skein.test"), header("Content-Type", "application/json")].into(),
        body: Body::Length(u64::try_from(upload.len()).expect("fits")),
        close: false,
    };
    let mut connection = Connection::new(call, upload.clone());
    connection.run(&response, Ending::Open);
    assert_eq!(connection.status, Some((200, Framing::Chunked)));
    assert_eq!(connection.body, body);
    assert_eq!(connection.done, Some(Reuse::Keep));
    let server = &connection.tls.wire.server;
    let head = b"POST /v1/messages HTTP/1.1\r\nHost: skein.test\r\nContent-Type: application/json\r\nContent-Length: 40000\r\n\r\n";
    assert_eq!(&server.received[..head.len()], head);
    assert_eq!(&server.received[head.len()..], upload, "the body, through TLS's records");
    assert!(server.closed);
}

/// A response whose body runs to the end of the stream (RFC 9112, 6.3),
/// and the call that asks for it.
fn until_end(body: &[u8]) -> (Vec<u8>, Call) {
    let mut response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n".to_vec();
    response.extend_from_slice(body);
    let call = Call {
        method: Method::Get,
        target: b"/v1/log"[..].into(),
        headers: vec![header("Host", "skein.test")].into(),
        body: Body::None,
        close: false,
    };
    (response, call)
}

#[test]
fn a_body_read_to_the_end_is_whole_once_close_notify_ends_it() {
    let body = world::text(&mut Rng::new(2), 3_000);
    let (response, call) = until_end(&body);
    let mut connection = Connection::new(call, Vec::new());
    connection.run(&response, Ending::CloseNotify);
    assert_eq!(connection.status, Some((200, Framing::UntilEnd)));
    assert_eq!(connection.body, body);
    assert_eq!(connection.body_over, Some(Up::End));
    assert_eq!(connection.done, Some(Reuse::Close));
    assert_eq!((connection.http_failed, connection.tls_failed), (None, None));
    assert!(connection.http_closed && connection.tls_closed);
}

#[test]
fn a_body_read_to_the_end_cut_without_close_notify_fails_as_invalid() {
    let body = world::text(&mut Rng::new(3), 3_000);
    let (response, call) = until_end(&body);
    let mut connection = Connection::new(call, Vec::new());
    connection.run(&response, Ending::Cut);
    assert_eq!(connection.status, Some((200, Framing::UntilEnd)));
    // Every byte deciphered is delivered, and still the body is not whole:
    // a truncation is never taken for its end.
    assert_eq!(connection.body, body);
    assert_eq!(connection.body_over, Some(Up::Failed(Fault::Invalid)));
    assert_eq!(connection.done, None);
    assert_eq!(connection.http_failed, Some(http::Error::Stream(Fault::Invalid)));
    assert_eq!(connection.tls_failed, Some(tls::Error::Truncated));
    assert!(connection.http_closed && connection.tls_closed);
    assert!(!connection.tls.wire.server.closed, "nothing sent after the failure");
}
