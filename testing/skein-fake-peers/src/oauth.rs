//! Hosted issuer HTTP face (oauth.md, section 5). The independent issuer
//! checks registration, PKCE, code reuse and refresh rotation. This face
//! owns bounded HTTP connections and redirects, never a client's state or
//! credential durability. `new` listens; `queue` installs bounded response
//! plans; `Host::iterate` drives io; `shutdown` settles connections and
//! keeps an already accepted delayed issuer response until its terminal.

use std::mem::size_of;
use std::net::SocketAddr;

use skein_fake_oauth as issuer;
use skein_http::{Header, Method, server as http};
use skein_io::{self as io, kernel};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Decimal, Duration, Env, List, Queue, Time, Token, Wall, bytes};
use skein_oauth::HttpRequest;
use skein_world::Host;

use crate::face::{self, Face, LISTENER};
use crate::transport::{self, Wire};
use crate::{Error, Limits, Transport};

/// The issuer's outside records for a world's referee; secret bodies have no Debug.
#[derive(PartialEq, Eq)]
pub enum Observation {
    /// The server accepted this connection from a client or scripted browser.
    Accepted { connection: Token },
    /// The browser sent this authorization target to the registered endpoint.
    Authorize { connection: Token, target: Box<[u8]> },
    /// The token endpoint received this complete POST body.
    Post { connection: Token, body: Box<[u8]> },
    /// The actual issuer approved sign-in and the server sent this Location.
    Redirect { connection: Token, location: Box<[u8]> },
    /// The real issuer emitted this token-endpoint terminal, even if disconnected.
    Answered { connection: Option<Token>, id: u64, status: u16 },
    /// io settled the connection's close.
    Closed { connection: Token },
}

struct Response {
    body: Box<[u8]>,
    offset: usize,
}

struct Connection {
    owner: Token,
    wire: Wire,
    http: http::Server,
    events: Queue<http::Event>,
    requests: Queue<http::Request>,
    below: Queue<Down>,
    plain: Queue<Up>,
    body: List<u8>,
    content_type: Option<&'static [u8]>,
    token_post: bool,
    active: Option<u64>,
    response: Option<Response>,
    closing: bool,
}

/// A world's fake issuer process, serving authorization and token endpoints.
pub struct Peer {
    face: Face,
    transport: Transport,
    issuer: issuer::Issuer,
    issuer_limits: issuer::Limits,
    http_limits: http::Limits,
    authorization_url: Box<[u8]>,
    authorization_path: Box<[u8]>,
    token_endpoint: Box<[u8]>,
    token_path: Box<[u8]>,
    outgoing: Queue<issuer::Request>,
    connections: Vec<Connection>,
    next: u64,
    exchange: u64,
    observations: Vec<Observation>,
    observed_bytes: u64,
    bound: u64,
}

impl Peer {
    /// Starts a loopback issuer with explicit registration and immutable machine bounds.
    pub fn new(
        address: SocketAddr,
        transport: Transport,
        limits: Limits,
        config: issuer::Config,
        issuer_limits: issuer::Limits,
        http_limits: http::Limits,
    ) -> Result<Self, Error> {
        transport::validate(&limits, transport)?;
        let bound = worst_case(&limits, &issuer_limits, &http_limits, transport).ok_or(Error::Limits)?;
        let authorization_path = path(&config.authorization_url, transport)?;
        let token_path = path(&config.token_endpoint, transport)?;
        let authorization_url = config.authorization_url.clone();
        let token_endpoint = config.token_endpoint.clone();
        Ok(Self {
            face: Face::new(address, limits)?,
            transport,
            issuer: issuer::Issuer::new(config, issuer_limits).map_err(|_| Error::Limits)?,
            issuer_limits,
            http_limits,
            authorization_url,
            authorization_path,
            token_endpoint,
            token_path,
            outgoing: Queue::with_capacity(limits.queue),
            connections: Vec::with_capacity(usize::try_from(limits.connections).expect("u32 fits")),
            next: 1,
            exchange: 1,
            observations: Vec::with_capacity(usize::try_from(limits.observations).expect("u32 fits")),
            observed_bytes: 0,
            bound,
        })
    }

    /// Adds one actual issuer response plan; its domain enforces count and byte limits.
    pub fn queue(&mut self, plan: issuer::Plan) -> Result<(), issuer::Error> {
        self.issuer.queue(plan)
    }

    /// The actual listener address after io announces Listening.
    #[must_use]
    pub fn address(&self) -> Option<SocketAddr> {
        self.face.address
    }

    /// Frozen outside records, retained under observation bounds supplied by the world.
    #[must_use]
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }

    /// Stops admission and closes connections without forgetting accepted issuer work.
    pub fn shutdown(&mut self) {
        self.face.shutdown();
    }

    fn observe(&mut self, record: Observation, payload_bytes: u64) {
        self.reserve_observation(payload_bytes);
        self.observed_bytes = self.observed_bytes.checked_add(payload_bytes).expect("bounded observations");
        self.observations.push(record);
    }

    fn reserve_observation(&self, payload_bytes: u64) {
        assert!(
            self.observations.len() < usize::try_from(self.face.limits.observations).expect("u32 fits"),
            "world observation count cap"
        );
        assert!(
            self.observed_bytes
                .checked_add(payload_bytes)
                .is_some_and(|bytes| bytes <= u64::from(self.face.limits.observation_bytes)),
            "world observation byte cap before copying"
        );
    }

    fn io_event(&mut self, event: io::Event) {
        match event {
            io::Event::Listening { owner, listener, addr } => {
                assert_eq!(owner, LISTENER, "one issuer listener");
                self.face.listener = Some(listener);
                self.face.address = Some(addr);
            }
            io::Event::Accepted { owner, socket, .. } => {
                assert_eq!(owner, LISTENER, "one issuer listener accepts");
                if self.face.stopping
                    || self.connections.len() == usize::try_from(self.face.limits.connections).expect("u32 fits")
                {
                    self.face.requests.push(io::Request::Abort { entity: socket });
                    return;
                }
                let owner = Token::new(self.next);
                self.next =
                    self.next.checked_add(1).filter(|next| *next < u64::MAX).expect("finite world connection ids");
                self.face.requests.push(io::Request::Bind { socket, owner });
                let limits = self.face.limits;
                let mut requests = Queue::with_capacity(limits.queue);
                requests.push(http::Request::Next);
                self.connections.push(Connection {
                    owner,
                    wire: Wire::new(socket, self.transport, limits),
                    http: http::Server::new(&self.http_limits),
                    events: Queue::with_capacity(limits.queue),
                    requests,
                    below: Queue::with_capacity(limits.queue),
                    plain: Queue::with_capacity(limits.queue),
                    body: List::with_capacity(self.issuer_limits.request_bytes),
                    content_type: None,
                    token_post: false,
                    active: None,
                    response: None,
                    closing: false,
                });
                self.observe(Observation::Accepted { connection: owner }, 0);
            }
            io::Event::Output { owner, up } => {
                if let Some(connection) = self.connections.iter_mut().find(|connection| connection.owner == owner) {
                    connection.wire.output(up, &mut connection.plain, &mut self.face.requests);
                }
            }
            io::Event::Stream { owner, up } => {
                if let Some(connection) = self.connections.iter_mut().find(|connection| connection.owner == owner) {
                    connection.wire.up(up, &mut connection.plain, &mut self.face.requests);
                }
            }
            io::Event::Closed { owner } => {
                if owner == LISTENER {
                    return;
                }
                if let Some(index) = self.connections.iter().position(|connection| connection.owner == owner) {
                    self.connections.remove(index);
                    self.observe(Observation::Closed { connection: owner }, 0);
                }
            }
            io::Event::Failed { owner, .. } => {
                assert_ne!(owner, LISTENER, "issuer listener must bind");
                if let Some(connection) = self.connections.iter_mut().find(|connection| connection.owner == owner) {
                    connection.wire.close(&mut self.face.requests);
                    connection.closing = true;
                }
            }
            io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Shutdown { .. } => {
                panic!("issuer only listens and serves classic byte streams");
            }
        }
    }

    fn http_event(&mut self, index: usize, event: http::Event, now: Time, wall: Wall) {
        match event {
            http::Event::Call(call) => self.call(index, call, now),
            http::Event::Body(Up::Bytes(bytes)) => {
                let connection = &mut self.connections[index];
                if bytes.len() > usize::try_from(connection.body.room()).expect("u32 fits") {
                    self.close(index);
                    return;
                }
                for byte in bytes {
                    connection.body.push(byte).expect("admitted request body");
                }
                connection.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
            }
            http::Event::Body(Up::End) => self.post(index, now, wall),
            http::Event::Reply(Up::Room) => {
                let connection = &mut self.connections[index];
                let response = connection.response.as_mut().expect("one HTTP response body");
                let end = response
                    .offset
                    .checked_add(usize::try_from(self.http_limits.send).expect("u32 fits"))
                    .expect("bounded response")
                    .min(response.body.len());
                connection
                    .requests
                    .push(http::Request::Reply(Down::Send(bytes::copy_of(&response.body[response.offset..end]))));
                response.offset = end;
                if end == response.body.len() {
                    connection.requests.push(http::Request::Reply(Down::Finish));
                } else {
                    connection
                        .requests
                        .push(http::Request::Reply(Down::Demand { read: Read::Nothing, room: self.http_limits.send }));
                }
            }
            http::Event::Done(http::Reuse::Keep) => {
                let connection = &mut self.connections[index];
                connection.response = None;
                connection.active = None;
                connection.requests.push(http::Request::Next);
            }
            http::Event::Done(http::Reuse::Close)
            | http::Event::Ended
            | http::Event::Failed(_)
            | http::Event::Refused(_)
            | http::Event::Reply(Up::Bytes(_) | Up::End | Up::Failed(_))
            | http::Event::Body(Up::Room | Up::Failed(_)) => self.close(index),
            http::Event::Closed => {}
        }
    }

    fn call(&mut self, index: usize, call: http::Call, now: Time) {
        let target_base = call.target.split(|byte| *byte == b'?').next().expect("target has a base");
        if call.method == Method::Get && target_base == self.authorization_path.as_ref() {
            let owner = self.connections[index].owner;
            if self
                .authorization_url
                .len()
                .checked_add(call.target.len().checked_sub(target_base.len()).expect("target suffix"))
                .is_none_or(|length| length > usize::try_from(self.issuer_limits.request_bytes).expect("u32 fits"))
            {
                self.respond(index, 400, Box::new([]), Box::new([]), Duration::ZERO);
                return;
            }
            let mut url = Vec::with_capacity(
                self.authorization_url
                    .len()
                    .checked_add(call.target.len().checked_sub(target_base.len()).expect("suffix"))
                    .expect("bounded URL"),
            );
            url.extend_from_slice(&self.authorization_url);
            if let Some(query) = call.target.get(target_base.len()..) {
                url.extend_from_slice(query);
            }
            if url.len() > usize::try_from(self.issuer_limits.request_bytes).expect("u32 fits") {
                self.respond(index, 400, Box::new([]), Box::new([]), Duration::ZERO);
                return;
            }
            let payload_bytes = u64::try_from(call.target.len()).expect("bounded target");
            self.observe(Observation::Authorize { connection: owner, target: call.target }, payload_bytes);
            self.issuer.step(issuer::Event::Authorize { url: url.into_boxed_slice(), now }, &mut self.outgoing);
            self.deliver(Some(index));
        } else if call.method == Method::Post && call.target == self.token_path {
            let connection = &mut self.connections[index];
            connection.body.clear();
            connection.content_type = None;
            connection.token_post = true;
            let mut count = 0;
            for header in &call.headers {
                if header.is(b"content-type") {
                    count += 1;
                    connection.content_type = match header.value.as_ref() {
                        b"application/json" => Some(b"application/json"),
                        b"application/x-www-form-urlencoded" => Some(b"application/x-www-form-urlencoded"),
                        _ => None,
                    };
                }
            }
            if count != 1 {
                connection.content_type = None;
            }
            connection.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
        } else {
            self.respond(index, 404, Box::new([]), Box::new([]), Duration::ZERO);
        }
    }

    fn post(&mut self, index: usize, now: Time, wall: Wall) {
        let connection = &mut self.connections[index];
        if !connection.token_post {
            return;
        }
        connection.token_post = false;
        let Some(content_type) = connection.content_type.take() else {
            self.respond(index, 400, Box::new([]), Box::new([]), Duration::ZERO);
            return;
        };
        let body = bytes::copy_of(connection.body.as_slice());
        connection.body.clear();
        let owner = connection.owner;
        let id = self.exchange;
        self.exchange = self.exchange.checked_add(1).expect("finite world HTTP identities");
        connection.active = Some(id);
        let payload_bytes = u64::try_from(body.len()).expect("bounded request");
        self.reserve_observation(payload_bytes);
        self.observe(Observation::Post { connection: owner, body: body.clone() }, payload_bytes);
        self.issuer.step(
            issuer::Event::Post {
                request: HttpRequest {
                    id,
                    endpoint: self.token_endpoint.clone(),
                    content_type,
                    body,
                    deadline: now.saturating_add(Duration::from_secs(60)),
                },
                now,
                wall,
            },
            &mut self.outgoing,
        );
        self.deliver(Some(index));
    }

    fn deliver(&mut self, origin: Option<usize>) {
        if let Some(output) = self.outgoing.pop() {
            match output {
                issuer::Request::Redirect { uri, state, code } => {
                    let index = origin.expect("authorization emits synchronously");
                    let location = redirect(&uri, &state, &code, self.http_limits.response)
                        .expect("configured redirect response cap");
                    let payload_bytes = u64::try_from(location.len()).expect("bounded redirect");
                    self.reserve_observation(payload_bytes);
                    self.observe(
                        Observation::Redirect { connection: self.connections[index].owner, location: location.clone() },
                        payload_bytes,
                    );
                    self.respond(
                        index,
                        302,
                        Box::new([Header { name: bytes::copy_of(b"location"), value: location }]),
                        Box::new([]),
                        Duration::ZERO,
                    );
                }
                issuer::Request::Http(response) => {
                    let index = self.connections.iter().position(|connection| connection.active == Some(response.id));
                    self.observe(
                        Observation::Answered {
                            connection: index.map(|index| self.connections[index].owner),
                            id: response.id,
                            status: response.status,
                        },
                        0,
                    );
                    if let Some(index) = index {
                        self.respond(
                            index,
                            response.status,
                            Box::new([Header {
                                name: bytes::copy_of(b"content-type"),
                                value: bytes::copy_of(b"application/json"),
                            }]),
                            response.body,
                            response.retry_after,
                        );
                    }
                }
                issuer::Request::Refused => {
                    if let Some(index) = origin {
                        self.respond(index, 400, Box::new([]), Box::new([]), Duration::ZERO);
                    }
                }
            }
        }
    }

    fn respond(&mut self, index: usize, status: u16, headers: Box<[Header]>, body: Box<[u8]>, retry_after: Duration) {
        let connection = &mut self.connections[index];
        let mut headers = headers.into_vec();
        if retry_after != Duration::ZERO {
            let seconds = Decimal::of(retry_after.as_nanos().div_euclid(1_000_000_000));
            headers.push(Header { name: bytes::copy_of(b"retry-after"), value: bytes::copy_of(seconds.as_bytes()) });
        }
        let empty = body.is_empty();
        connection.requests.push(http::Request::Discard);
        connection.requests.push(http::Request::Respond(http::Response {
            status,
            headers: headers.into_boxed_slice(),
            body: if empty {
                http::Body::None
            } else {
                http::Body::Length(u64::try_from(body.len()).expect("bounded token response"))
            },
            close: false,
        }));
        connection.response = Some(Response { body, offset: 0 });
        if !empty {
            connection
                .requests
                .push(http::Request::Reply(Down::Demand { read: Read::Nothing, room: self.http_limits.send }));
        }
    }

    fn close(&mut self, index: usize) {
        let connection = &mut self.connections[index];
        if !connection.closing {
            connection.closing = true;
            connection.requests.push(http::Request::Close);
            connection.wire.close(&mut self.face.requests);
        }
    }

    fn progress(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.http_limits };
        for index in 0..self.connections.len() {
            if self.face.requests.room() < 8 {
                break;
            }
            if self.face.stopping {
                self.close(index);
            }
            let connection = &mut self.connections[index];
            if connection.events.room() >= 4
                && connection.below.room() >= 4
                && let Some(up) = connection.plain.pop()
            {
                http::up(&mut connection.http, &env, up, &mut connection.events, &mut connection.below);
            }
            if let Some(event) = connection.events.pop() {
                self.http_event(index, event, now, wall);
            }
            let connection = &mut self.connections[index];
            if connection.events.room() >= 4
                && connection.below.room() >= 4
                && let Some(request) = connection.requests.pop()
            {
                http::down(&mut connection.http, &env, request, &mut connection.events, &mut connection.below);
            }
            if connection.plain.room() >= 3 {
                if let Some(down) = connection.below.pop() {
                    connection.wire.down(down, &mut connection.plain, &mut self.face.requests);
                }
                connection.wire.pump(&mut connection.plain, &mut self.face.requests);
            }
        }
        if self.issuer.next_deadline().is_some_and(|deadline| deadline <= now) {
            self.issuer.step(issuer::Event::Tick { now, wall }, &mut self.outgoing);
            self.deliver(None);
        }
    }
}

impl Host for Peer {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.face.up(now, wall);
        for _ in 0..self.face.limits.queue {
            if self.face.requests.room() < 8 {
                break;
            }
            let Some(event) = self.face.events.pop() else { break };
            self.io_event(event);
        }
        self.face.close_listener();
        self.progress(now, wall);
        self.face.down(now, wall);
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.face.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.face.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.face.work_pending()
            || !self.outgoing.is_empty()
            || self.issuer.next_deadline().is_some_and(|due| due <= now)
            || self.connections.iter().any(|connection| {
                !connection.events.is_empty()
                    || !connection.requests.is_empty()
                    || !connection.below.is_empty()
                    || !connection.plain.is_empty()
                    || connection.wire.work_pending()
            })
    }
    fn next_deadline(&self) -> Option<Time> {
        [self.face.io.next_deadline(), self.issuer.next_deadline()].into_iter().flatten().min()
    }
    fn is_empty(&self) -> bool {
        self.face.is_empty()
            && self.connections.is_empty()
            && self.outgoing.is_empty()
            && self.issuer.next_deadline().is_none()
    }
    fn worst_case(&self) -> u64 {
        self.bound
    }
    fn operations(&self) -> u32 {
        io::operations(&self.face.limits.io).expect("validated io limits")
    }
}

/// Checked complete process envelope, including issuer plans and observations.
#[must_use]
pub fn worst_case(limits: &Limits, issuer: &issuer::Limits, http: &http::Limits, transport: Transport) -> Option<u64> {
    if http::largest_read(http) > limits.plaintext
        || http::largest_room(http) > limits.ciphertext.checked_sub(1024)?
        || http.body < u64::from(issuer.request_bytes)
        || http.read == 0
        || http.send == 0
        || http.headers < 2
        || http.response
            < issuer.uri_bytes.checked_add(issuer.document.string_bytes.checked_mul(3)?)?.checked_add(256)?
    {
        return None;
    }
    let connection = transport::worst_case(limits, transport)?
        .checked_add(http::worst_case(http)?)?
        .checked_add(u64::try_from(size_of::<Connection>()).ok()?)?
        .checked_add(Queue::<http::Event>::worst_case(limits.queue)?)?
        .checked_add(Queue::<http::Request>::worst_case(limits.queue)?)?
        .checked_add(Queue::<Down>::worst_case(limits.queue)?)?
        .checked_add(Queue::<Up>::worst_case(limits.queue)?)?
        .checked_add(u64::from(issuer.request_bytes).checked_mul(4)?)?
        .checked_add(u64::from(issuer.document.document_bytes).checked_mul(4)?)?
        .checked_add(
            u64::from(limits.queue).checked_mul(u64::from(limits.plaintext).checked_add(u64::from(http.response))?)?,
        )?;
    face::worst_case(limits)?
        .checked_add(issuer::worst_case(issuer)?)?
        .checked_add(connection.checked_mul(u64::from(limits.connections))?)?
        .checked_add(Queue::<issuer::Request>::worst_case(limits.queue)?)?
        .checked_add(u64::from(issuer.uri_bytes).checked_mul(4)?)?
        .checked_add(u64::from(limits.observations).checked_mul(u64::try_from(size_of::<Observation>()).ok()?)?)?
        .checked_add(u64::from(limits.observation_bytes))
}

fn path(url: &[u8], transport: Transport) -> Result<Box<[u8]>, Error> {
    let remainder = match transport {
        Transport::Plaintext => url.strip_prefix(b"http://"),
        Transport::Tls => url.strip_prefix(b"https://"),
    }
    .ok_or(Error::Limits)?;
    let at = remainder.iter().position(|byte| *byte == b'/').ok_or(Error::Limits)?;
    let target = &remainder[at..];
    if target.contains(&b'?') || target.iter().any(|byte| !(0x21..=0x7e).contains(byte)) {
        return Err(Error::Limits);
    }
    Ok(bytes::copy_of(target))
}

fn redirect(uri: &[u8], state: &[u8], code: &[u8], cap: u32) -> Option<Box<[u8]>> {
    let size =
        uri.len().checked_add(state.len().checked_mul(3)?)?.checked_add(code.len().checked_mul(3)?)?.checked_add(13)?;
    if size > usize::try_from(cap).ok()? {
        return None;
    }
    let mut location = Vec::with_capacity(size);
    location.extend_from_slice(uri);
    location.extend_from_slice(if uri.contains(&b'?') { b"&code=" } else { b"?code=" });
    percent(code, &mut location);
    location.extend_from_slice(b"&state=");
    percent(state, &mut location);
    Some(location.into_boxed_slice())
}

fn percent(value: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8] = b"0123456789ABCDEF";
    for &byte in value {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(byte);
        } else {
            out.extend_from_slice(&[b'%', HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 15)]]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::redirect;

    #[test]
    fn redirect_preserves_registered_uri_and_escapes_code_and_state() {
        assert_eq!(
            redirect(b"http://127.0.0.1:1234/callback", b"state with +&%", b"code?=#", 1024).unwrap().as_ref(),
            b"http://127.0.0.1:1234/callback?code=code%3F%3D%23&state=state%20with%20%2B%26%25"
        );
        assert_eq!(
            redirect(b"http://127.0.0.1:1234/callback?kept=yes", b"0123456789abcdef", b"code", 1024).unwrap().as_ref(),
            b"http://127.0.0.1:1234/callback?kept=yes&code=code&state=0123456789abcdef"
        );
        assert!(redirect(b"http://127.0.0.1:1234/callback", b"state", b"code", 4).is_none());
    }
}
