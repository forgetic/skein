//! The actual OAuth machine signs in and refreshes against an independent
//! issuer process; a scripted browser follows its HTTP 302 to the client's
//! real simulated redirect listener. The shared harness owns scheduling,
//! replay and per-process heap checks (oauth.md, section 5;
//! testing-strategy.md, sections 4, 6 and 7).
use std::net::{Ipv4Addr, SocketAddr};

use skein_fake_oauth as fake;
use skein_fake_peers::{Transport, oauth as peer};
use skein_http::{Header, Method, client as http, server};
use skein_io::{self as io, kernel};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall, bytes};
use skein_oauth as oauth;
use skein_world::{Host, Memory, Outcome, Referee, World};

const LISTENER: Token = Token::new(1);
const CALLBACK: Token = Token::new(2);
const WEB: Token = Token::new(3);
const ROOM: u32 = 64;
const AUTHORIZE: &[u8] = b"http://127.0.0.1:31000/authorize";
const TOKEN: &[u8] = b"http://127.0.0.1:31000/token";
const REDIRECT: &[u8] = b"http://127.0.0.1:31234/callback";
const STATE: &[u8] = b"0123456789abcdef";

fn document() -> oauth::Limits {
    oauth::Limits {
        document_bytes: 1024,
        string_bytes: 256,
        token_bytes: 256,
        client_bytes: 64,
        detail_bytes: 64,
        record_bytes: 1024,
        depth: 8,
        tokens: 64,
    }
}
fn client_limits() -> oauth::ClientLimits {
    oauth::ClientLimits {
        document: document(),
        uri_bytes: 256,
        scope_bytes: 64,
        state_bytes: 64,
        code_bytes: 64,
        url_bytes: 1024,
        request_bytes: 1024,
        sign_in_time: Duration::from_secs(120),
        request_time: Duration::from_secs(10),
        backoff_base: Duration::from_secs(1),
        backoff_ceiling: Duration::from_secs(4),
        max_attempts: 1,
    }
}
fn registration() -> oauth::Registration {
    oauth::Registration {
        authorization_url: bytes::copy_of(AUTHORIZE),
        token_endpoint: bytes::copy_of(TOKEN),
        client_id: bytes::copy_of(b"client"),
        redirect_uri: bytes::copy_of(REDIRECT),
        scope: bytes::copy_of(b"read"),
        wire: oauth::WireFormat::Form,
        client_secret: None,
        pkce_for_confidential: false,
        metadata_claim: None,
    }
}
fn io_limits() -> io::Limits {
    io::Limits {
        sockets: 4,
        refusals: 1,
        intake: 32_768,
        receive: 1024,
        output: 32_768,
        sends: 4,
        accepts: 1,
        backlog: 2,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(1),
    }
}
fn http_limits() -> http::Limits {
    http::Limits { request: 2048, head: 2048, headers: 16, read: 256, send: 256 }
}
fn server_limits() -> server::Limits {
    server::Limits { head: 2048, headers: 16, body: 1024, read: 256, response: 2048, send: 256 }
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
    id: Option<u64>,
    done: bool,
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
            id,
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

struct Callback {
    socket: Token,
    http: server::Server,
    events: Queue<server::Event>,
    requests: Queue<server::Request>,
    below: Queue<Down>,
}
impl Callback {
    fn new(socket: Token) -> Self {
        let mut requests = Queue::with_capacity(ROOM);
        requests.push(server::Request::Next);
        Self {
            socket,
            http: server::Server::new(&server_limits()),
            events: Queue::with_capacity(ROOM),
            requests,
            below: Queue::with_capacity(ROOM),
        }
    }
}

/// OAuth service or scripted browser, each with its own io and outside records.
#[expect(clippy::struct_excessive_bools, reason = "outside facts and independent listener/web lifecycle flags")]
pub struct Actor {
    browser: bool,
    io: io::Io,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
    submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    oauth: Option<oauth::Client>,
    oauth_requests: Queue<oauth::Request>,
    listener: Option<Token>,
    callback: Option<Callback>,
    exchange: Option<Exchange>,
    web_socket: Option<Token>,
    connected: bool,
    http_pending: Option<oauth::HttpRequest>,
    visit: Option<Box<[u8]>>,
    inbox: [u8; 1024],
    inbox_len: usize,
    stage: u32,
    records: Vec<oauth::SavedToken>,
    close: bool,
    browser_done: bool,
    redirected: bool,
}
impl Actor {
    fn new(browser: bool) -> Self {
        let mut requests = Queue::with_capacity(ROOM);
        if !browser {
            requests
                .push(io::Request::Listen { owner: LISTENER, addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 31234)) });
        }
        Self {
            browser,
            io: io::Io::new(&io_limits()),
            events: Queue::with_capacity(ROOM),
            requests,
            submissions: Queue::with_capacity(ROOM),
            completions: Queue::with_capacity(ROOM),
            oauth: if browser {
                None
            } else {
                Some(oauth::Client::new(client_limits()).expect("bounded OAuth client"))
            },
            oauth_requests: Queue::with_capacity(ROOM),
            listener: None,
            callback: None,
            exchange: None,
            web_socket: None,
            connected: false,
            http_pending: None,
            visit: None,
            inbox: [0; 1024],
            inbox_len: 0,
            stage: 0,
            records: Vec::with_capacity(2),
            close: false,
            browser_done: false,
            redirected: false,
        }
    }
    fn open(&mut self, url: &[u8], body: Box<[u8]>, content_type: Option<&[u8]>, id: Option<u64>) {
        assert!(self.exchange.is_none(), "one HTTP exchange per actor");
        let (address, _, _) = url_parts(url);
        self.exchange = Some(Exchange::new(url, body, content_type, id));
        self.requests.push(io::Request::Connect { owner: WEB, addr: address });
    }
    fn event(&mut self, event: io::Event, now: Time, wall: Wall) {
        match event {
            io::Event::Listening { owner, listener, .. } => {
                assert_eq!(owner, LISTENER);
                self.listener = Some(listener);
                self.oauth.as_mut().expect("client").step(
                    oauth::Event::SignIn {
                        registration: registration(),
                        key: 7,
                        generation: 0,
                        state: bytes::copy_of(STATE),
                        verifier: Some(bytes::copy_of(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")),
                        now,
                    },
                    &mut self.oauth_requests,
                );
            }
            io::Event::Accepted { owner, socket, .. } => {
                assert_eq!(owner, LISTENER);
                assert!(self.callback.is_none());
                self.requests.push(io::Request::Bind { socket, owner: CALLBACK });
                self.callback = Some(Callback::new(socket));
            }
            io::Event::Connecting { owner, socket } => {
                assert_eq!(owner, WEB);
                self.web_socket = Some(socket);
            }
            io::Event::Connected { owner } => {
                assert_eq!(owner, WEB);
                self.connected = true;
            }
            io::Event::Stream { owner, up } => {
                if owner == WEB {
                    self.exchange.as_mut().expect("HTTP exchange").up(up, now, wall);
                } else {
                    assert_eq!(owner, CALLBACK);
                    let callback = self.callback.as_mut().expect("redirect connection");
                    server::up(
                        &mut callback.http,
                        &Env { now, wall, limits: server_limits() },
                        up,
                        &mut callback.events,
                        &mut callback.below,
                    );
                }
            }
            io::Event::Closed { owner } => {
                if owner == WEB {
                    self.web_socket = None;
                    self.connected = false;
                    self.exchange = None;
                } else if owner == CALLBACK {
                    self.callback = None;
                } else {
                    assert_eq!(owner, LISTENER);
                }
            }
            io::Event::Failed { .. }
            | io::Event::Output { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Shutdown { .. } => panic!("unexpected positive-world io event: {event:?}"),
        }
    }
    fn progress(&mut self, now: Time, wall: Wall) {
        if self.browser && self.stage == 0 && self.inbox_len > 0 {
            let url = bytes::copy_of(&self.inbox[..self.inbox_len]);
            self.open(&url, Box::new([]), None, None);
            self.stage = 1;
        }
        if self.exchange.is_none()
            && let Some(request) = self.http_pending.take()
        {
            self.open(&request.endpoint, request.body, Some(request.content_type), Some(request.id));
        }
        if let Some(exchange) = &mut self.exchange {
            if let Some(socket) = self.web_socket {
                exchange.progress(socket, self.connected, now, wall, &mut self.requests);
            }
            if exchange.done && exchange.response.is_some() {
                let response = exchange.response.take().expect("completed head");
                if self.browser {
                    if self.stage == 1 {
                        assert_eq!(response.status, 302, "issuer approved sign-in");
                        let location = response.header(b"location").expect("approved redirect");
                        assert!(location.len() <= self.inbox.len());
                        self.inbox[..location.len()].copy_from_slice(location);
                        self.inbox_len = location.len();
                        self.stage = 2;
                    } else {
                        assert_eq!(response.status, 204, "client received browser redirect");
                        self.browser_done = true;
                    }
                } else {
                    let body = exchange.received.clone().into_boxed_slice();
                    self.oauth.as_mut().expect("client").step(
                        oauth::Event::Http(oauth::HttpResponse {
                            id: exchange.id.expect("token POST id"),
                            status: response.status,
                            body,
                            retry_after: Duration::ZERO,
                            evidence: oauth::HttpEvidence::Response,
                            now,
                            wall,
                        }),
                        &mut self.oauth_requests,
                    );
                }
            }
        }
        if self.browser && self.stage == 2 && self.exchange.is_none() {
            let url = bytes::copy_of(&self.inbox[..self.inbox_len]);
            self.open(&url, Box::new([]), None, None);
            self.stage = 3;
        }
        self.callback(now, wall);
        if let Some(request) = self.oauth_requests.pop() {
            match request {
                oauth::Request::Visit { url } => self.visit = Some(url),
                oauth::Request::Http(request) => {
                    assert!(self.http_pending.is_none());
                    self.http_pending = Some(request);
                }
                oauth::Request::Tokens { record } => {
                    assert!(self.records.len() < 2, "one sign-in and one refresh terminal");
                    self.records.push(record);
                    if self.records.len() == 1 {
                        let client = self.oauth.as_mut().expect("client");
                        client.step(oauth::Event::Reset, &mut self.oauth_requests);
                        client.step(
                            oauth::Event::Refresh {
                                registration: registration(),
                                prior: self.records[0].refresh_state(),
                                now,
                            },
                            &mut self.oauth_requests,
                        );
                    }
                }
                oauth::Request::Failed { failure } => panic!("positive OAuth flow failed: {failure:?}"),
            }
        }
        if self.close
            && let Some(listener) = self.listener.take()
        {
            self.requests.push(io::Request::Close { entity: listener });
        }
    }
    fn callback(&mut self, now: Time, wall: Wall) {
        let Some(callback) = &mut self.callback else { return };
        let env = Env { now, wall, limits: server_limits() };
        if let Some(event) = callback.events.pop() {
            match event {
                server::Event::Call(call) => {
                    assert_eq!(call.method, Method::Get);
                    let target = call.target.as_ref();
                    assert!(target.starts_with(b"/callback?"), "redirect URI matches bound listener and path");
                    let query = &target[b"/callback?".len()..];
                    let code = parameter(query, b"code");
                    let state = parameter(query, b"state");
                    self.oauth.as_mut().expect("client").step(
                        oauth::Event::Redirected {
                            uri: bytes::copy_of(REDIRECT),
                            state: bytes::copy_of(state),
                            code: Some(bytes::copy_of(code)),
                            error: None,
                            now,
                        },
                        &mut self.oauth_requests,
                    );
                    self.redirected = true;
                    callback.requests.push(server::Request::Discard);
                    callback.requests.push(server::Request::Respond(server::Response {
                        status: 204,
                        headers: Box::new([]),
                        body: server::Body::None,
                        close: true,
                    }));
                }
                server::Event::Done(_) => {
                    callback.requests.push(server::Request::Close);
                    self.requests.push(io::Request::Close { entity: callback.socket });
                }
                server::Event::Closed => {}
                server::Event::Body(_)
                | server::Event::Reply(_)
                | server::Event::Ended
                | server::Event::Failed(_)
                | server::Event::Refused(_) => panic!("scripted redirect request failed: {event:?}"),
            }
        }
        if callback.events.room() >= 4
            && callback.below.room() >= 4
            && let Some(request) = callback.requests.pop()
        {
            server::down(&mut callback.http, &env, request, &mut callback.events, &mut callback.below);
        }
        if let Some(down) = callback.below.pop() {
            self.requests.push(io::Request::Stream { stream: callback.socket, down });
        }
    }
}
impl Host for Actor {
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
            if self.requests.room() < 8 {
                break;
            }
            let Some(event) = self.events.pop() else { break };
            self.event(event, now, wall);
        }
        if self.requests.room() >= 8 {
            self.progress(now, wall);
        }
        if let Some(client) = &mut self.oauth
            && client.next_deadline().is_some_and(|due| due <= now)
        {
            client.step(oauth::Event::Tick { now }, &mut self.oauth_requests);
        }
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
    fn work_pending(&self, now: Time) -> bool {
        self.io.is_ready()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || !self.completions.is_empty()
            || !self.oauth_requests.is_empty()
            || (self.close && self.listener.is_some())
            || (self.exchange.is_none()
                && (self.http_pending.is_some() || (self.browser && self.inbox_len > 0 && self.stage < 3)))
            || (self.connected && self.exchange.as_ref().is_some_and(Exchange::has_work))
            || self.callback.as_ref().is_some_and(|callback| {
                !callback.events.is_empty() || !callback.requests.is_empty() || !callback.below.is_empty()
            })
            || self.oauth.as_ref().is_some_and(|client| client.next_deadline().is_some_and(|due| due <= now))
    }
    fn next_deadline(&self) -> Option<Time> {
        [self.io.next_deadline(), self.oauth.as_ref().and_then(oauth::Client::next_deadline)]
            .into_iter()
            .flatten()
            .min()
    }
    fn is_empty(&self) -> bool {
        self.io.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
            && self.submissions.is_empty()
            && self.completions.is_empty()
            && self.exchange.is_none()
            && self.callback.is_none()
            && self.oauth_requests.is_empty()
    }
    fn worst_case(&self) -> u64 {
        io::worst_case(&io_limits())
            .expect("io envelope")
            .checked_add(oauth::client_worst_case(&client_limits()).expect("client envelope"))
            .expect("combined envelope")
            .checked_add(512 * 1024)
            .expect("HTTP machines, queues, copies and outside records")
    }
    fn operations(&self) -> u32 {
        io::operations(&io_limits()).expect("bounded operations")
    }
}

/// The service, scripted browser and issuer have separate process heaps.
pub enum Process {
    /// The actual client emits Visit and Tokens; its pipes and sockets settle at shutdown.
    Client(Box<Actor>),
    /// The scripted browser follows the issuer's actual redirect and receives 204.
    Browser(Box<Actor>),
    /// The independent issuer receives authorization and token requests and closes on shutdown.
    Issuer(Box<peer::Peer>),
}
impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.iterate(now, wall),
            Self::Issuer(peer) => peer.iterate(now, wall),
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.completions(),
            Self::Issuer(peer) => peer.completions(),
        }
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.submissions(),
            Self::Issuer(peer) => peer.submissions(),
        }
    }
    fn work_pending(&self, now: Time) -> bool {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.work_pending(now),
            Self::Issuer(peer) => peer.work_pending(now),
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.next_deadline(),
            Self::Issuer(peer) => peer.next_deadline(),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.is_empty(),
            Self::Issuer(peer) => peer.is_empty(),
        }
    }
    fn worst_case(&self) -> u64 {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.worst_case(),
            Self::Issuer(peer) => peer.worst_case(),
        }
        .checked_add(8192)
        .expect("boxed actor wrapper")
    }
    fn operations(&self) -> u32 {
        match self {
            Self::Client(actor) | Self::Browser(actor) => actor.operations(),
            Self::Issuer(peer) => peer.operations(),
        }
    }
}

struct Judge {
    delivered: bool,
    passed: bool,
    size: usize,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, processes: &mut [Process]) {
        if !self.delivered {
            let mut inbox = [0; 1024];
            let mut length = 0;
            let ready = processes.iter().any(|process| match process {
                Process::Issuer(peer) => peer.address().is_some(),
                Process::Client(_) | Process::Browser(_) => false,
            });
            for process in processes.iter() {
                if let Process::Client(actor) = process
                    && let Some(url) = &actor.visit
                {
                    length = url.len();
                    inbox[..length].copy_from_slice(url);
                }
            }
            if ready && length > 0 {
                for process in processes.iter_mut() {
                    if let Process::Browser(actor) = process {
                        actor.inbox[..length].copy_from_slice(&inbox[..length]);
                        actor.inbox_len = length;
                    }
                }
                self.delivered = true;
            }
        }
        if self.passed {
            for process in processes {
                match process {
                    Process::Client(actor) => actor.close = true,
                    Process::Issuer(peer) => peer.shutdown(),
                    Process::Browser(_) => {}
                }
            }
        }
    }
    fn observe(&mut self, _now: Time, processes: &[Process]) {
        let mut tokens = false;
        let mut browser = false;
        let mut posts = 0;
        let mut redirected = false;
        for process in processes {
            match process {
                Process::Client(actor) => {
                    assert!(actor.records.len() <= 2, "one terminal per exchange");
                    if actor.records.len() == 2 {
                        assert_eq!(actor.records[0].generation, 1);
                        assert_eq!(actor.records[1].generation, 2);
                        assert_eq!(actor.records[0].access_token, vec![b'a'; self.size].into_boxed_slice());
                        assert_eq!(actor.records[1].access_token, vec![b'b'; self.size].into_boxed_slice());
                        assert_eq!(
                            actor.records[1].refresh_token.as_ref(),
                            if self.size == 256 { vec![b't'; 256] } else { b"r2".to_vec() }
                        );
                        tokens = true;
                        assert!(actor.redirected, "sign-in reached the client's listener");
                    }
                }
                Process::Browser(actor) => browser = actor.browser_done,
                Process::Issuer(peer) => {
                    for record in peer.observations() {
                        match record {
                            peer::Observation::Post { body, .. } => {
                                assert!(!body.is_empty());
                                posts += 1;
                            }
                            peer::Observation::Redirect { location, .. } => {
                                assert!(location.starts_with(REDIRECT));
                                redirected = true;
                            }
                            peer::Observation::Answered { status, .. } => assert_eq!(*status, 200),
                            peer::Observation::Accepted { .. }
                            | peer::Observation::Authorize { .. }
                            | peer::Observation::Closed { .. } => {}
                        }
                    }
                }
            }
        }
        assert!(posts <= 2, "exactly sign-in and refresh POSTs");
        self.passed = tokens && browser && posts == 2 && redirected;
    }
    fn next_deadline(&self) -> Option<Time> {
        if !self.delivered {
            Some(Time::ZERO)
        } else if self.passed {
            None
        } else {
            Some(Time::from_nanos(20_000_000_000))
        }
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= Time::from_nanos(20_000_000_000) && !self.passed)
            .then(|| "issuer sign-in and refresh did not finish".to_owned())
    }
    fn passed(&self) -> bool {
        self.passed
    }
}

/// Sign-in, redirect followed over sockets, then rotating refresh; large fills issuer token caps.
#[must_use]
pub fn run(seed: u64, faulted: bool, large: bool, memory: Memory) -> Outcome<Process> {
    let size = if large { 256 } else { 16 };
    let mut config = skein_sim::Config::calm();
    config.buffer = if faulted { 127 } else { 1024 };
    if faulted {
        config.faults.short_recv = 300;
        config.faults.short_send = 300;
    }
    let mut world = World::new(seed, config, Judge { delivered: false, passed: false, size }, memory);
    world.spawn(|| {
        let limits = skein_fake_peers::Limits {
            io: io::Limits { sockets: 2, ..io_limits() },
            connections: 1,
            queue: ROOM,
            plaintext: 32_768,
            ciphertext: 32_768,
            observations: 16,
            observation_bytes: 4096,
        };
        let mut issuer = peer::Peer::new(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 31000)),
            Transport::Plaintext,
            limits,
            fake::Config {
                authorization_url: bytes::copy_of(AUTHORIZE),
                token_endpoint: bytes::copy_of(TOKEN),
                client_id: bytes::copy_of(b"client"),
                client_secret: None,
                redirect_uri: bytes::copy_of(REDIRECT),
                refresh_token: if large { vec![b's'; 256].into_boxed_slice() } else { bytes::copy_of(b"seed") },
            },
            fake::Limits {
                document: document(),
                uri_bytes: 256,
                request_bytes: 1024,
                codes: 1,
                rotations: 2,
                plans: 2,
            },
            server_limits(),
        )
        .expect("loopback issuer");
        for (access, refresh) in [(b'a', b"r1".as_slice()), (b'b', b"r2".as_slice())] {
            issuer
                .queue(fake::Plan {
                    status: 200,
                    body: fake::Body::Token(oauth::TokenResponse {
                        access_token: vec![access; size].into_boxed_slice(),
                        refresh_token: Some(if large {
                            vec![if access == b'a' { b'r' } else { b't' }; 256].into_boxed_slice()
                        } else {
                            bytes::copy_of(refresh)
                        }),
                        expires_in: 30,
                    }),
                    delay: Duration::from_millis(5),
                    retry_after: Duration::ZERO,
                })
                .expect("plans fill the domain queue");
        }
        Process::Issuer(Box::new(issuer))
    });
    world.spawn(|| Process::Client(Box::new(Actor::new(false))));
    world.spawn(|| Process::Browser(Box::new(Actor::new(true))));
    world.run()
}

fn url_parts(url: &[u8]) -> (SocketAddr, &[u8], &[u8]) {
    let url = url.strip_prefix(b"http://").expect("scripted loopback HTTP URL");
    let at = url.iter().position(|byte| *byte == b'/').expect("scripted path");
    let authority = &url[..at];
    let address =
        std::str::from_utf8(authority).expect("loopback ASCII").parse::<SocketAddr>().expect("loopback address");
    assert!(address.ip().is_loopback());
    (address, authority, &url[at..])
}
fn parameter<'a>(query: &'a [u8], name: &[u8]) -> &'a [u8] {
    let mut found = None;
    for pair in query.split(|byte| *byte == b'&') {
        let equal = pair.iter().position(|byte| *byte == b'=').expect("scripted query pair");
        if &pair[..equal] == name {
            assert!(found.is_none(), "no duplicate scripted redirect parameter");
            found = Some(&pair[equal + 1..]);
        }
    }
    found.expect("scripted redirect parameter")
}

/// Compares the io trace and actual public client/browser/issuer records.
pub fn assert_replay(first: &Outcome<Process>, second: &Outcome<Process>) {
    assert_eq!(first.trace, second.trace, "wire records replay");
    for (first, second) in first.procs.iter().zip(&second.procs) {
        match (first, second) {
            (Process::Issuer(first), Process::Issuer(second)) => {
                assert!(first.observations() == second.observations(), "issuer observations replay");
            }
            (Process::Client(first), Process::Client(second)) => {
                assert!(first.records == second.records, "actual token records replay");
                assert!(first.visit == second.visit, "actual Visit URL replays");
            }
            (Process::Browser(first), Process::Browser(second)) => {
                assert_eq!(first.browser_done, second.browser_done, "browser's received terminal replays");
            }
            (
                Process::Issuer(_) | Process::Client(_) | Process::Browser(_),
                Process::Issuer(_) | Process::Client(_) | Process::Browser(_),
            ) => panic!("process order replays"),
        }
    }
}
