//! A scripted HTTP browser; it knows no OAuth machine, keeper or listener.
use super::{ROOM, Story, confidential, io_limits};
use skein_http::{Header, Method, client as http};
use skein_io::{self as io, kernel};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Queue, Time, Token, Wall, bytes};
use skein_world::Host;
use std::net::{Ipv4Addr, SocketAddr};
const WEB: Token = Token::new(3);
fn http_limits() -> http::Limits {
    http::Limits { request: 4096, ..crate::world::limits().http }
}
struct Exchange {
    machine: http::Client,
    events: Queue<http::Event>,
    requests: Queue<http::Request>,
    below: Queue<Down>,
    body: Box<[u8]>,
    offset: usize,
    response: Option<http::Response>,
    received: Vec<u8>,
    pub(super) done: bool,
}
impl Exchange {
    fn new(url: &[u8], body: Box<[u8]>, content_type: Option<&[u8]>, id: Option<u64>) -> Self {
        let (_, authority, target) = url_parts(url);
        let mut headers = vec![Header { name: bytes::copy_of(b"host"), value: bytes::copy_of(authority) }];
        if let Some(content_type) = content_type {
            headers.push(Header { name: bytes::copy_of(b"content-type"), value: bytes::copy_of(content_type) });
        }
        let mut requests = Queue::with_capacity(ROOM);
        requests.push(http::Request::Call(http::Call {
            method: if id.is_some() { Method::Post } else { Method::Get },
            target: bytes::copy_of(target),
            headers: headers.into_boxed_slice(),
            body: if id.is_some() {
                http::Body::Length(u64::try_from(body.len()).expect("bounded upload"))
            } else {
                http::Body::None
            },
            close: true,
        }));
        if id.is_some() {
            requests.push(http::Request::Upload(Down::Demand { read: Read::Nothing, room: 256 }));
        }
        Self {
            machine: http::Client::new(&http_limits()),
            events: Queue::with_capacity(ROOM),
            requests,
            below: Queue::with_capacity(ROOM),
            body,
            offset: 0,
            response: None,
            received: Vec::with_capacity(1024),
            done: false,
        }
    }
    fn up(&mut self, up: Up, now: Time, wall: Wall) {
        http::up(&mut self.machine, &Env { now, wall, limits: http_limits() }, up, &mut self.events, &mut self.below);
    }
    fn progress(
        &mut self,
        socket: Token,
        connected: bool,
        now: Time,
        wall: Wall,
        io_requests: &mut Queue<io::Request>,
    ) {
        if !connected {
            return;
        }
        if let Some(event) = self.events.pop() {
            match event {
                http::Event::Response(response) => {
                    self.response = Some(response);
                    self.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
                }
                http::Event::Upload(Up::Room) => {
                    let end = self.offset.checked_add(256).expect("bounded upload").min(self.body.len());
                    self.requests.push(http::Request::Upload(Down::Send(bytes::copy_of(&self.body[self.offset..end]))));
                    self.offset = end;
                    if end == self.body.len() {
                        self.requests.push(http::Request::Upload(Down::Finish));
                    } else {
                        self.requests.push(http::Request::Upload(Down::Demand { read: Read::Nothing, room: 256 }));
                    }
                }
                http::Event::Body(Up::Bytes(bytes)) => {
                    assert!(
                        self.received.len().checked_add(bytes.len()).is_some_and(|size| size <= 1024),
                        "bounded token response"
                    );
                    self.received.extend_from_slice(&bytes);
                    self.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
                }
                http::Event::Body(Up::End) | http::Event::Closed => {}
                http::Event::Done(_) => {
                    self.done = true;
                    self.requests.push(http::Request::Close);
                    io_requests.push(io::Request::Close { entity: socket });
                }
                http::Event::Failed(_)
                | http::Event::Upload(Up::Failed(_) | Up::Bytes(_) | Up::End)
                | http::Event::Body(Up::Room | Up::Failed(_)) => panic!("positive HTTP exchange failed: {event:?}"),
            }
        }
        if self.events.room() >= 2
            && self.below.room() >= 2
            && let Some(request) = self.requests.pop()
        {
            http::down(
                &mut self.machine,
                &Env { now, wall, limits: http_limits() },
                request,
                &mut self.events,
                &mut self.below,
            );
        }
        if let Some(down) = self.below.pop() {
            io_requests.push(io::Request::Stream { stream: socket, down });
        }
    }
    fn has_work(&self) -> bool {
        !self.events.is_empty() || !self.requests.is_empty() || !self.below.is_empty()
    }
}

/// A scripted browser owns only its HTTP requests, and knows no OAuth state.
pub struct Browser {
    io: io::Io,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
    submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    exchange: Option<Exchange>,
    socket: Option<Token>,
    connected: bool,
    pub(super) inbox: [u8; 1024],
    pub(super) inbox_len: usize,
    pub(super) redirect: Option<Box<[u8]>>,
    stage: u32,
    pub(super) done: bool,
    story: Story,
}
impl Browser {
    pub(super) fn new(story: Story) -> Self {
        Self {
            io: io::Io::new(&io_limits()),
            events: Queue::with_capacity(ROOM),
            requests: Queue::with_capacity(ROOM),
            submissions: Queue::with_capacity(ROOM),
            completions: Queue::with_capacity(ROOM),
            exchange: None,
            socket: None,
            connected: false,
            inbox: [0; 1024],
            inbox_len: 0,
            redirect: None,
            stage: 0,
            done: false,
            story,
        }
    }
    fn open(&mut self, url: &[u8]) {
        assert!(self.exchange.is_none());
        self.exchange = Some(Exchange::new(url, Box::new([]), None, None));
        self.requests.push(io::Request::Connect { owner: WEB, addr: url_parts(url).0 });
    }
    fn progress(&mut self, now: Time, wall: Wall) {
        if let Some(exchange) = &mut self.exchange {
            if let Some(socket) = self.socket {
                exchange.progress(socket, self.connected, now, wall, &mut self.requests);
            }
            if exchange.done
                && let Some(response) = exchange.response.take()
            {
                match self.stage {
                    1 => {
                        assert_eq!(response.status, 302);
                        self.redirect = Some(bytes::copy_of(response.header(b"location").expect("issuer redirect")));
                        self.stage = 2;
                        if confidential(self.story) {
                            self.done = true;
                        }
                    }
                    3 => {
                        assert_eq!(response.status, if self.story == Story::LongHead { 414 } else { 404 });
                        self.stage = 4;
                    }
                    5 => {
                        assert_eq!(response.status, 200);
                        assert!(exchange.received.starts_with(b"Sign-in received."));
                        self.done = true;
                        self.stage = 6;
                    }
                    _ => panic!("browser stage"),
                }
            }
        }
        if self.exchange.is_none() {
            match self.stage {
                0 if self.inbox_len > 0 => {
                    let url = bytes::copy_of(&self.inbox[..self.inbox_len]);
                    self.inbox_len = 0;
                    self.open(&url);
                    self.stage = 1;
                }
                2 if !confidential(self.story) => {
                    let mut url = self.redirect.as_ref().expect("redirect").to_vec();
                    match self.story {
                        Story::WrongPath => {
                            let at = url
                                .windows(b"/callback".len())
                                .position(|part| part == b"/callback")
                                .expect("callback");
                            url.splice(at..at + 9, b"/wrong".iter().copied());
                        }
                        Story::WrongHost => {
                            let at = url
                                .windows(b"localhost".len())
                                .position(|part| part == b"localhost")
                                .expect("localhost");
                            url.splice(at..at + 9, b"127.0.0.1".iter().copied());
                        }
                        Story::LongHead => {
                            url.extend_from_slice(b"&padding=");
                            url.resize(3000, b'x');
                        }
                        Story::Public
                        | Story::Confidential
                        | Story::CloseWaiting
                        | Story::CloseConfidential
                        | Story::Cancel
                        | Story::Abort
                        | Story::NotKept
                        | Story::Timeout => {}
                    }
                    self.open(&url);
                    self.stage =
                        if matches!(self.story, Story::WrongPath | Story::WrongHost | Story::LongHead) { 3 } else { 5 };
                }
                4 => {
                    let url = self.redirect.as_ref().expect("redirect").clone();
                    self.open(&url);
                    self.stage = 5;
                }
                _ => {}
            }
        }
    }
}
impl Host for Browser {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: io_limits() };
        for _ in 0..ROOM {
            if self.events.room() < 3 || self.submissions.room() < 2 || !self.io.is_ready() {
                break;
            }
            io::resume(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.events.room() < 3 || self.submissions.room() < 2 {
                break;
            }
            let Some(complete) = self.completions.pop() else { break };
            io::up(&mut self.io, &env, complete, &mut self.events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.events.room() < 3 || self.submissions.room() < 2 || !self.io.is_due(now) {
                break;
            }
            io::fire(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            let Some(event) = self.events.pop() else { break };
            match event {
                io::Event::Connecting { socket, .. } => self.socket = Some(socket),
                io::Event::Connected { .. } => self.connected = true,
                io::Event::Stream { up, .. } => self.exchange.as_mut().expect("browser exchange").up(up, now, wall),
                io::Event::Closed { .. } => {
                    self.socket = None;
                    self.connected = false;
                    self.exchange = None;
                }
                other @ (io::Event::Listening { .. }
                | io::Event::Accepted { .. }
                | io::Event::Output { .. }
                | io::Event::Spawned { .. }
                | io::Event::Exited { .. }
                | io::Event::Usage { .. }
                | io::Event::Shutdown { .. }
                | io::Event::Failed { .. }) => panic!("browser event {other:?}"),
            }
        }
        self.progress(now, wall);
        for _ in 0..ROOM {
            if !self.io.takes() || self.submissions.room() < 2 {
                break;
            }
            let Some(request) = self.requests.pop() else { break };
            io::down(&mut self.io, &env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _: Time) -> bool {
        self.io.is_ready()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || !self.completions.is_empty()
            || self.inbox_len > 0
            || (self.exchange.is_none() && matches!(self.stage, 2 | 4) && !confidential(self.story))
            || (self.connected && self.exchange.as_ref().is_some_and(Exchange::has_work))
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.io.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
            && self.submissions.is_empty()
            && self.completions.is_empty()
            && self.exchange.is_none()
    }
    fn worst_case(&self) -> u64 {
        io::worst_case(&io_limits()).expect("io bound") + 512 * 1024
    }
    fn operations(&self) -> u32 {
        io::operations(&io_limits()).expect("operations")
    }
}
pub(super) fn url_parts(url: &[u8]) -> (SocketAddr, &[u8], &[u8]) {
    let rest = url.strip_prefix(b"http://").expect("fake HTTP url");
    let at = rest.iter().position(|byte| *byte == b'/').expect("path");
    let authority = &rest[..at];
    let address = if authority == b"localhost:31234" {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 31234))
    } else {
        std::str::from_utf8(authority).expect("ascii").parse::<SocketAddr>().expect("literal address")
    };
    assert!(address.ip().is_loopback());
    (address, authority, &rest[at..])
}
