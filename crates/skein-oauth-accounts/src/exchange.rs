//! One issuer exchange and its HTTP/TLS socket (oauth.md, sections 2, 3 and 6.4).
//! Keeps bounded child queues and one response document; knows no grant policy
//! or durable store. The component routes io events and advances one child at a
//! time, then consumes the client's terminal before closing the connection.

#![expect(clippy::single_match, reason = "explicit socket presence at the transport boundary")]

use crate::{Endpoint, Limits, Transport};

use alloc::boxed::Box;
use skein_http::{Header, Method, client as http};
use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, List, Queue, Token, bytes};
use skein_oauth as oauth;
use skein_tls::client as tls;

pub(crate) const ROUTES: u32 = 8;

pub(crate) enum Stage {
    Running,
    Keeping { candidate: oauth::SavedToken },
    Finished,
}

pub(crate) enum Purpose {
    Refresh,
    SignIn { waiting: bool },
}

pub(crate) struct Exchange {
    pub(crate) account: u32,
    pub(crate) client: oauth::Client,
    pub(crate) above: Queue<oauth::Request>,
    pub(crate) web: Option<Web>,
    pub(crate) stage: Stage,
    pub(crate) pending_grant: bool,
    pub(crate) aborted: bool,
    pub(crate) purpose: Purpose,
    pub(crate) visit: Option<Box<[u8]>>,
    pub(crate) listener: Option<crate::listener::Listener>,
}

impl Exchange {
    pub(crate) fn signing_in(&self) -> bool {
        match self.purpose {
            Purpose::Refresh => false,
            Purpose::SignIn { .. } => true,
        }
    }
    pub(crate) fn waiting(&self) -> bool {
        match self.purpose {
            Purpose::Refresh => false,
            Purpose::SignIn { waiting } => waiting,
        }
    }
    pub(crate) fn received(&mut self) {
        match &mut self.purpose {
            Purpose::SignIn { waiting } => *waiting = false,
            Purpose::Refresh => {}
        }
    }

    pub(crate) fn new(account: u32, client: oauth::Client, pending_grant: bool) -> Exchange {
        Exchange {
            account,
            client,
            above: Queue::with_capacity(oauth::MAX_OUT),
            web: None,
            stage: Stage::Running,
            pending_grant,
            aborted: false,
            purpose: Purpose::Refresh,
            visit: None,
            listener: None,
        }
    }

    pub(crate) fn has_work(&self) -> bool {
        if self.settled()
            || !self.above.is_empty()
            || match &self.listener {
                Some(listener) => listener.has_work() || (self.visit.is_some() && listener.ready()),
                None => self.visit.is_some(),
            }
        {
            return true;
        }
        match &self.web {
            Some(web) => web.has_work(),
            None => false,
        }
    }

    pub(crate) fn settled(&self) -> bool {
        match self.stage {
            Stage::Finished => {
                self.web.is_none()
                    && match &self.listener {
                        Some(listener) => listener.settled(),
                        None => true,
                    }
                    && self.above.is_empty()
            }
            Stage::Running | Stage::Keeping { .. } => false,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Connecting,
    Handshaking,
    Calling,
    Answered,
    Closing,
    Closed,
}

pub(crate) struct Web {
    pub(crate) socket: Option<Token>,
    phase: Phase,
    tls: Option<tls::Client>,
    http: http::Client,
    events: Queue<http::Event>,
    requests: Queue<http::Request>,
    plain: Queue<Down>,
    tls_events: Queue<tls::Event>,
    cipher: Queue<Down>,
    upload: Box<[u8]>,
    offset: usize,
    document: List<u8>,
    response: Option<http::Response>,
    pub(crate) answer: Option<oauth::HttpResponse>,
    pub(crate) failure: Option<oauth::Failure>,
    close_sent: bool,
    id: u64,
    sent: bool,
    abort: bool,
}

impl Web {
    pub(crate) fn new(request: oauth::HttpRequest, endpoint: &Endpoint, limits: &Limits) -> Option<Web> {
        let authority_start = bytes::find(&request.endpoint, b"://")?.checked_add(3)?;
        let rest = request.endpoint.get(authority_start..)?;
        let slash = bytes::find(rest, b"/")?;
        let authority = rest.get(..slash)?;
        let target = rest.get(slash..)?;
        let mut headers = List::with_capacity(2);
        headers.push(Header { name: bytes::copy_of(b"host"), value: bytes::copy_of(authority) }).expect("two headers");
        headers
            .push(Header { name: bytes::copy_of(b"content-type"), value: bytes::copy_of(request.content_type) })
            .expect("two headers");
        let mut requests = Queue::with_capacity(ROUTES);
        requests.push(http::Request::Call(http::Call {
            method: Method::Post,
            target: bytes::copy_of(target),
            headers: headers.into_boxed(),
            body: http::Body::Length(u64::try_from(request.body.len()).ok()?),
            close: true,
        }));
        requests.push(http::Request::Upload(Down::Demand { read: Read::Nothing, room: limits.http.send }));
        let tls = match &endpoint.transport {
            Transport::Tls { server_name, trust } => Some(tls::Client::new(trust, server_name.clone(), &limits.tls)),
            Transport::Plaintext => None,
        };
        Some(Web {
            socket: None,
            phase: Phase::Connecting,
            tls,
            http: http::Client::new(&limits.http),
            events: Queue::with_capacity(ROUTES),
            requests,
            plain: Queue::with_capacity(ROUTES),
            tls_events: Queue::with_capacity(ROUTES),
            cipher: Queue::with_capacity(ROUTES),
            upload: request.body,
            offset: 0,
            document: List::with_capacity(limits.client.document.document_bytes),
            response: None,
            answer: None,
            failure: None,
            close_sent: false,
            id: request.id,
            sent: false,
            abort: false,
        })
    }

    pub(crate) fn up(&mut self, env: &Env<Limits>, event: IoEvent, io: &mut Queue<IoRequest>) {
        match event {
            IoEvent::Connecting { socket, .. } => {
                self.socket = Some(socket);
                if self.phase == Phase::Closing {
                    self.close_socket(io);
                }
            }
            IoEvent::Connected { .. } => {
                if self.phase == Phase::Connecting {
                    match &mut self.tls {
                        Some(client) => {
                            self.phase = Phase::Handshaking;
                            tls::down(
                                client,
                                &Env { now: env.now, wall: env.wall, limits: env.limits.tls },
                                tls::Request::Handshake,
                                &mut self.tls_events,
                                &mut self.cipher,
                            );
                        }
                        None => self.phase = Phase::Calling,
                    }
                }
            }
            IoEvent::Stream { up, .. } => {
                if self.phase == Phase::Closed {
                    return;
                }
                match &mut self.tls {
                    Some(client) => tls::up(
                        client,
                        &Env { now: env.now, wall: env.wall, limits: env.limits.tls },
                        up,
                        &mut self.tls_events,
                        &mut self.cipher,
                    ),
                    None if self.phase == Phase::Closing => {}
                    None => http::up(
                        &mut self.http,
                        &Env { now: env.now, wall: env.wall, limits: env.limits.http },
                        up,
                        &mut self.events,
                        &mut self.plain,
                    ),
                }
            }
            IoEvent::Failed { .. } => {
                self.transport_failed(env);
                // io owns failed-connect settlement and sends Closed itself.
            }
            IoEvent::Closed { .. } => {
                if self.phase != Phase::Answered && self.phase != Phase::Closing {
                    self.transport_failed(env);
                }
                self.phase = Phase::Closed;
                self.socket = None;
            }
            IoEvent::Listening { .. }
            | IoEvent::Accepted { .. }
            | IoEvent::Output { .. }
            | IoEvent::Spawned { .. }
            | IoEvent::Exited { .. }
            | IoEvent::Usage { .. }
            | IoEvent::Shutdown { .. } => {}
        }
    }

    fn transport_failed(&mut self, env: &Env<Limits>) {
        if self.answer.is_none()
            && self.phase != Phase::Answered
            && self.phase != Phase::Closing
            && self.phase != Phase::Closed
        {
            self.answer = Some(oauth::HttpResponse {
                id: self.id,
                status: 0,
                body: Box::new([]),
                retry_after: Duration::ZERO,
                evidence: if self.sent { oauth::HttpEvidence::Unknown } else { oauth::HttpEvidence::Unsent },
                now: env.now,
                wall: env.wall,
            });
            self.phase = Phase::Answered;
        }
    }

    pub(crate) fn progress(&mut self, env: &Env<Limits>, io: &mut Queue<IoRequest>) {
        let http_env = Env { now: env.now, wall: env.wall, limits: env.limits.http };
        let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
        if self.phase == Phase::Closing || self.phase == Phase::Closed {
            return;
        }
        if let Some(event) = self.events.pop() {
            self.http_event(env, event);
            return;
        }
        if let Some(event) = self.tls_events.pop() {
            match event {
                tls::Event::Ready(_) => self.phase = Phase::Calling,
                tls::Event::Stream(up) => http::up(&mut self.http, &http_env, up, &mut self.events, &mut self.plain),
                tls::Event::Failed(_) => self.transport_failed(env),
                tls::Event::Closed => {
                    self.phase = Phase::Closing;
                    self.close_socket(io);
                }
            }
            return;
        }
        if let Some(down) = self.cipher.pop() {
            match self.socket {
                Some(socket) => io.push(IoRequest::Stream { stream: socket, down }),
                None => {}
            }
            return;
        }
        if self.phase != Phase::Calling {
            return;
        }
        if let Some(down) = self.plain.pop() {
            match &down {
                Down::Send(bytes) if !bytes.is_empty() => self.sent = true,
                Down::Send(_) | Down::Demand { .. } | Down::Finish => {}
            }
            match &mut self.tls {
                Some(client) => {
                    tls::down(client, &tls_env, tls::Request::Stream(down), &mut self.tls_events, &mut self.cipher);
                }
                None => match self.socket {
                    Some(socket) => io.push(IoRequest::Stream { stream: socket, down }),
                    None => {}
                },
            }
            return;
        }
        if let Some(request) = self.requests.pop() {
            http::down(&mut self.http, &http_env, request, &mut self.events, &mut self.plain);
        }
    }

    fn http_event(&mut self, env: &Env<Limits>, event: http::Event) {
        match event {
            http::Event::Response(response) => {
                self.response = Some(response);
                self.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
            }
            http::Event::Upload(Up::Room) => {
                let end = self
                    .offset
                    .saturating_add(usize::try_from(env.limits.http.send).expect("u32 fits usize"))
                    .min(self.upload.len());
                let bytes = bytes::copy_of(self.upload.get(self.offset..end).expect("bounded upload"));
                self.requests.push(http::Request::Upload(Down::Send(bytes)));
                self.offset = end;
                if end == self.upload.len() {
                    self.requests.push(http::Request::Upload(Down::Finish));
                } else {
                    self.requests
                        .push(http::Request::Upload(Down::Demand { read: Read::Nothing, room: env.limits.http.send }));
                }
            }
            http::Event::Body(Up::Bytes(bytes)) => {
                let length = u32::try_from(bytes.len()).ok();
                let too_large = match length {
                    Some(len) => len > self.document.room(),
                    None => true,
                };
                if too_large {
                    self.failure = Some(oauth::Failure::Limit);
                    self.phase = Phase::Answered;
                } else {
                    for byte in &bytes {
                        self.document.push(*byte).expect("response document admitted");
                    }
                    self.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
                }
            }
            http::Event::Done(_) => {
                let response = self.response.take().expect("HTTP done follows response");
                self.answer = Some(oauth::HttpResponse {
                    id: self.id,
                    status: response.status,
                    body: self.document.to_boxed(),
                    retry_after: retry_after(&response),
                    evidence: oauth::HttpEvidence::Response,
                    now: env.now,
                    wall: env.wall,
                });
                self.phase = Phase::Answered;
            }
            http::Event::Failed(_)
            | http::Event::Upload(Up::Failed(_) | Up::End | Up::Bytes(_))
            | http::Event::Body(Up::Failed(_) | Up::Room) => self.transport_failed(env),
            http::Event::Body(Up::End) | http::Event::Closed => {}
        }
    }

    pub(crate) fn closing(&self) -> bool {
        self.phase == Phase::Closing
    }

    pub(crate) fn close(&mut self, env: &Env<Limits>, abort: bool, io: &mut Queue<IoRequest>) {
        if self.phase == Phase::Closed {
            return;
        }
        if abort {
            self.answer = None;
            self.failure = None;
            if !self.abort {
                self.close_sent = false;
            }
            self.abort = true;
            self.phase = Phase::Closing;
            self.close_socket(io);
        } else if self.phase != Phase::Closing {
            self.phase = Phase::Closing;
            match &mut self.tls {
                Some(client) => tls::down(
                    client,
                    &Env { now: env.now, wall: env.wall, limits: env.limits.tls },
                    tls::Request::Close,
                    &mut self.tls_events,
                    &mut self.cipher,
                ),
                None => self.close_socket(io),
            }
        }
    }

    pub(crate) fn close_progress(&mut self, _env: &Env<Limits>, io: &mut Queue<IoRequest>) {
        if self.phase != Phase::Closing {
            return;
        }
        if let Some(down) = self.cipher.pop() {
            match self.socket {
                Some(socket) => io.push(IoRequest::Stream { stream: socket, down }),
                None => {}
            }
        } else if let Some(event) = self.tls_events.pop() {
            match event {
                tls::Event::Closed => self.close_socket(io),
                tls::Event::Ready(_) | tls::Event::Stream(_) | tls::Event::Failed(_) => {}
            }
        }
    }

    fn close_socket(&mut self, io: &mut Queue<IoRequest>) {
        if self.close_sent {
            return;
        }
        match self.socket {
            Some(socket) => {
                self.close_sent = true;
                if self.abort {
                    io.push(IoRequest::Abort { entity: socket });
                } else {
                    io.push(IoRequest::Close { entity: socket });
                }
            }
            None => {}
        }
    }

    pub(crate) fn closed(&self) -> bool {
        self.phase == Phase::Closed
    }

    pub(crate) fn has_work(&self) -> bool {
        match self.phase {
            Phase::Closed | Phase::Answered => self.answer.is_some() || self.failure.is_some(),
            Phase::Closing | Phase::Handshaking => !self.cipher.is_empty() || !self.tls_events.is_empty(),
            Phase::Connecting => false,
            Phase::Calling => {
                !self.events.is_empty()
                    || !self.requests.is_empty()
                    || !self.plain.is_empty()
                    || !self.tls_events.is_empty()
                    || !self.cipher.is_empty()
            }
        }
    }
}

fn retry_after(response: &http::Response) -> Duration {
    match response.header(b"retry-after") {
        Some(bytes) => match parse_seconds(bytes) {
            Some(seconds) => Duration::from_secs(seconds),
            None => Duration::ZERO,
        },
        None => Duration::ZERO,
    }
}

fn parse_seconds(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0_u64;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(byte.checked_sub(b'0')?))?;
    }
    Some(value)
}
