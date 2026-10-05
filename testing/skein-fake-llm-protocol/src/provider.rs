//! A real HTTP/SSE fake provider service. HTTP documents enter the neutral
//! fake domain; its answers leave through the dialect server and SSE writer.
//!
//! Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, sections 4.4 and 6.3.

use crate::documents;
use alloc::boxed::Box;
use core::mem::size_of;
use skein_fake_llm_domain::{self as domain, api};
use skein_http::{Header, Method, server as http, sse::writer as sse};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Decimal, Env, Id, List, Queue, ReplyTo, Slab, Token, bytes};
use skein_llm::anthropic;
use skein_llm::openai;

/// Maximum owner-directed records emitted by one connection entry point.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub const MAX_UP: u32 = 3;

/// Maximum transport-directed records emitted by one connection entry point.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub const MAX_DOWN: u32 = 18;

const EVENTS: u32 = 4;

const REQUESTS: u32 = 4;

const ROUTES: u32 = 8;

/// Immutable ownership and document caps supplied by the caller to every entry point.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    /// Maximum provider calls held at once.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub calls: u32,
    /// HTTP server bounds supplied by the byte world.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub http: http::Limits,
    /// SSE framing bounds supplied by the byte world.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub sse: sse::Limits,
    /// Document codec bounds for the selected fake provider.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub documents: documents::Limits,
}

/// Immutable scripted peer configuration supplied by the byte world; it contains no live IO.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[expect(missing_debug_implementations, reason = "configured header values must not enter traces")]
pub struct Config {
    /// Provider dialect selected by this scripted service.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub provider: documents::Provider,
    /// Exact configured HTTP request target accepted by this peer.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub path: Box<[u8]>,
    /// Verified compatibility data chosen by the world/deployment; this fake
    /// does not certify historical identities as current provider identities.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub headers: Box<[Header]>,
}

/// Typed refusal or terminal failure of the fake peer, independent of the agent's policy.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    /// Configuration or constructor ownership caps cannot be honored.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Limits,
    /// Input has invalid syntax or violates the codec's structural contract.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Malformed,
    /// A configured ownership, count or encoded-byte cap would be exceeded.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    TooLarge,
}

/// Decoded provider or peer event; the enclosing entry point checks order and correlation.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    /// Typed request forwarded to the neutral fake provider domain.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Domain(domain::Event),
    /// The owner requests connection closure; the lower layer still supplies Closed.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Close,
    /// The lower layer has completed connection closure.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    New,
    Reading,
    Body,
    Waiting,
    Streaming,
    Erring,
    Closing,
    Closed,
}

struct Call {
    server: Token,
    active: bool,
}

/// Bounded fake-provider service owning routing and pending calls across its connections.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[expect(missing_debug_implementations, reason = "configured headers remain protocol-only")]
pub struct Service {
    config: Config,
    calls: Slab<Call>,
    count: u64,
}

impl Service {
    /// Constructs empty bounded state under the supplied immutable limits; no IO or clocks are consulted.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub fn new(config: Config, limits: &Limits) -> Result<Service, Error> {
        if worst_case(limits).is_none() || config.path.first() != Some(&b'/') {
            return Err(Error::Limits);
        }
        if config.path.len() > usize::try_from(limits.http.head).expect("u32 fits usize")
            || config.headers.len() > usize::try_from(limits.http.headers).expect("u32 fits usize")
        {
            return Err(Error::Limits);
        }
        for &byte in &config.path {
            if !(0x21..=0x7e).contains(&byte) {
                return Err(Error::Limits);
            }
        }
        let mut encoded = config.path.len();
        for (index, header) in config.headers.iter().enumerate() {
            if header.name.is_empty() || header.is(b"authorization") || header.is(b"chatgpt-account-id") {
                return Err(Error::Limits);
            }
            for &byte in &header.name {
                if !byte.is_ascii_alphanumeric() && !b"!#$%&'*+-.^_`|~".contains(&byte) {
                    return Err(Error::Limits);
                }
            }
            for &byte in &header.value {
                if !(0x20..=0x7e).contains(&byte) && byte != b'\t' {
                    return Err(Error::Limits);
                }
            }
            for prior in config.headers.get(..index).ok_or(Error::Limits)? {
                if prior.is(&header.name) {
                    return Err(Error::Limits);
                }
            }
            encoded = encoded
                .checked_add(header.name.len())
                .ok_or(Error::Limits)?
                .checked_add(header.value.len())
                .ok_or(Error::Limits)?
                .checked_add(4)
                .ok_or(Error::Limits)?;
        }
        if encoded > usize::try_from(limits.http.head).expect("u32 fits usize") {
            return Err(Error::Limits);
        }
        Ok(Service { config, calls: Slab::with_capacity(limits.calls), count: 0 })
    }

    /// Number of fake provider requests observed since service construction.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Routes one owned fake-domain terminal to its pending HTTP connection.
    /// Returns the terminal alongside its target, preserving its reply right for
    /// `down`; an unmatched or closed route returns the same owned terminal as `Err`.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub fn target(&self, request: domain::Request) -> Result<(Token, domain::Request), domain::Request> {
        let domain::Request::Reply { to, result } = request;
        let token = to.into_token();
        let request = domain::Request::Reply { to: ReplyTo::new(token), result };
        let Some(call) = self.calls.get(Id::<Call>::from_token(token)) else {
            return Err(request);
        };
        if call.active { Ok((call.server, request)) } else { Err(request) }
    }

    /// Iteration-end reclamation of retired entries; it must follow delivery of owned outputs.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub fn reclaim(&mut self) {
        self.calls.reclaim();
    }
}

/// Bounded per-connection HTTP/SSE state driven by start, input, resume and close events.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[expect(missing_debug_implementations, reason = "HTTP input queues temporarily hold authorization values")]
pub struct Server {
    owner: Token,
    state: State,
    http: http::Server,
    writer: sse::Writer,
    events: Queue<http::Event>,
    writer_events: Queue<sse::Event>,
    requests: Queue<http::Request>,
    body: List<u8>,
    call: Option<Id<Call>>,
    answer: Option<api::Answer>,
    sequence: u32,
    response_id: u64,
    reply: Option<Box<[u8]>>,
    reply_offset: usize,
    refused: Option<api::Error>,
    writer_closed: bool,
}

impl Server {
    /// Constructs empty bounded state under the supplied immutable limits; no IO or clocks are consulted.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub fn new(owner: Token, limits: &Limits) -> Result<Server, Error> {
        if worst_case(limits).is_none() {
            return Err(Error::Limits);
        }
        let request = limits.documents.anthropic.request_bytes.max(limits.documents.openai.request_bytes);
        Ok(Server {
            owner,
            state: State::New,
            http: http::Server::new(&limits.http),
            writer: sse::Writer::new(&limits.sse),
            events: Queue::with_capacity(EVENTS),
            writer_events: Queue::with_capacity(1),
            requests: Queue::with_capacity(REQUESTS),
            body: List::with_capacity(request),
            call: None,
            answer: None,
            sequence: 0,
            response_id: 0,
            reply: None,
            reply_offset: 0,
            refused: None,
            writer_closed: false,
        })
    }

    /// Whether this peer can progress from its queued input or injected time without fresh transport input.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    #[must_use]
    pub fn has_work(&self) -> bool {
        !self.events.is_empty() || !self.writer_events.is_empty() || !self.requests.is_empty()
    }
}

/// Starts this connection's bounded HTTP state; reserve the declared output maxima before calling.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn start(
    server: &mut Server,
    service: &mut Service,
    credential: &skein_llm::Credential,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if server.state != State::New {
        return;
    }
    server.state = State::Reading;
    http_down(server, env, http::Request::Next, below);
    resume(server, service, credential, env, above, below);
}

/// Consumes one lower transport event and routes bounded HTTP progress or terminals to the owner.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn up(
    server: &mut Server,
    service: &mut Service,
    credential: &skein_llm::Credential,
    env: &Env<Limits>,
    event: Up,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    match server.state {
        State::New | State::Closing | State::Closed => return,
        State::Reading | State::Body | State::Waiting | State::Streaming | State::Erring => {}
    }
    let child = Env { now: env.now, wall: env.wall, limits: env.limits.http };
    http::up(&mut server.http, &child, event, &mut server.events, below);
    resume(server, service, credential, env, above, below);
}

/// Accepts one neutral fake-domain terminal and schedules its bounded HTTP/SSE response.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn down(
    server: &mut Server,
    service: &mut Service,
    credential: &skein_llm::Credential,
    env: &Env<Limits>,
    request: domain::Request,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    let (to, result) = match request {
        domain::Request::Reply { to, result } => (to, result),
    };
    let id = Id::<Call>::from_token(to.into_token());
    if server.call != Some(id) || server.state != State::Waiting {
        return;
    }
    let Some(call) = service.calls.get_mut(id) else {
        return;
    };
    if !call.active || call.server != server.owner {
        return;
    }
    call.active = false;
    service.calls.retire(id);
    server.call = None;
    match result {
        Ok(answer) => {
            if answer_fits(&answer, &env.limits.documents) {
                server.state = State::Streaming;
                server.answer = Some(answer);
                server.sequence = 0;
                server.requests.push(http::Request::Respond(http::Response {
                    status: 200,
                    headers: Box::new([Header {
                        name: bytes::copy_of(b"content-type"),
                        value: bytes::copy_of(b"text/event-stream"),
                    }]),
                    body: http::Body::Chunked,
                    close: false,
                }));
                next_event(server, service, env, above, below);
            } else {
                response_error(server, service, env, api::Error::InvalidRequest);
            }
        }
        Err(error) => response_error(server, service, env, error),
    }
    resume(server, service, credential, env, above, below);
}

/// Performs one bounded deferred HTTP/SSE handoff under backpressure; reserve output maxima first.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn resume(
    server: &mut Server,
    service: &mut Service,
    credential: &skein_llm::Credential,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    for _route in 0..ROUTES {
        if below.room() < 2 || above.room() < 2 {
            break;
        }
        if let Some(event) = server.writer_events.pop() {
            match event {
                sse::Event::Sent => next_event(server, service, env, above, below),
                sse::Event::Refused(_) | sse::Event::Failed(_) => close(server, service, env, above, below),
                sse::Event::Closed => {}
            }
        } else if let Some(event) = server.events.pop() {
            http_event(server, service, credential, env, event, above, below);
        } else if let Some(request) = server.requests.pop() {
            http_down(server, env, request, below);
        } else {
            break;
        }
    }
}

/// Starts orderly connection closure; outstanding lower work still requires its terminal.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn close(
    server: &mut Server,
    service: &mut Service,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if server.state == State::Closing || server.state == State::Closed {
        return;
    }
    if let Some(id) = server.call.take()
        && let Some(call) = service.calls.get_mut(id)
        && call.active
    {
        call.active = false;
        service.calls.retire(id);
    }
    for _slot in 0..server.events.capacity() {
        if server.events.pop().is_none() {
            break;
        }
    }
    for _slot in 0..server.requests.capacity() {
        if server.requests.pop().is_none() {
            break;
        }
    }
    server.answer = None;
    server.reply = None;
    server.body.clear();
    if !server.writer_closed {
        writer_down(server, env, sse::Request::Close);
        server.writer_closed = true;
    }
    for _slot in 0..server.requests.capacity() {
        if server.requests.pop().is_none() {
            break;
        }
    }
    http_down(server, env, http::Request::Close, below);
    server.state = State::Closing;
    above.push(Event::Close);
}

/// Consumes the lower layer's close terminal and emits the owner's single Closed observation.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn closed(
    server: &mut Server,
    service: &mut Service,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if server.state == State::Closed {
        return;
    }
    if server.state != State::Closing {
        close(server, service, env, above, below);
    }
    server.state = State::Closed;
    above.push(Event::Closed);
}

fn entrance(server: &mut Server, service: &mut Service, credential: &skein_llm::Credential, call: http::Call) {
    let identity = next_response(&mut service.count);
    server.response_id = identity.unwrap_or(0);
    server.refused = if identity.is_some() && call.method == Method::Post && call.target == service.config.path {
        None
    } else {
        Some(api::Error::InvalidRequest)
    };
    let mut authorization = None;
    let mut account = None;
    let mut content_type = 0_u32;
    for header in &call.headers {
        if header.is(b"authorization") {
            if authorization.is_some() {
                server.refused = Some(api::Error::Unauthorized);
            }
            authorization = Some(header.value.as_ref());
        }
        if header.is(b"chatgpt-account-id") {
            if account.is_some() {
                server.refused = Some(api::Error::Unauthorized);
            }
            account = Some(header.value.as_ref());
        }
        if header.is(b"content-type") {
            content_type = content_type.saturating_add(1);
            if header.value.as_ref() != b"application/json" {
                server.refused = Some(api::Error::InvalidRequest);
            }
        }
    }
    if content_type != 1 {
        server.refused = Some(api::Error::InvalidRequest);
    }
    let token = match authorization {
        Some(value) => value.strip_prefix(b"Bearer "),
        None => None,
    };
    let authorized = match token {
        Some(token) => {
            token == credential.access_token.as_ref()
                && match service.config.provider {
                    documents::Provider::Anthropic => true,
                    documents::Provider::OpenAi => {
                        account.is_some()
                            && account == Some(credential.account_id.as_ref())
                            && !credential.account_id.is_empty()
                    }
                }
        }
        None => false,
    };
    for expected in &service.config.headers {
        let mut count = 0_u32;
        for header in &call.headers {
            if header.is(&expected.name) {
                count = count.saturating_add(1);
                if header.value != expected.value {
                    server.refused = Some(api::Error::InvalidRequest);
                }
            }
        }
        if count != 1 {
            server.refused = Some(api::Error::InvalidRequest);
        }
    }
    if !authorized {
        server.refused = Some(api::Error::Unauthorized);
    }
    server.state = State::Body;
    server.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
}

fn next_response(count: &mut u64) -> Option<u64> {
    let next = count.checked_add(1)?;
    *count = next;
    Some(next)
}

fn http_down(server: &mut Server, env: &Env<Limits>, request: http::Request, below: &mut Queue<Down>) {
    let child = Env { now: env.now, wall: env.wall, limits: env.limits.http };
    http::down(&mut server.http, &child, request, &mut server.events, below);
}

fn writer_down(server: &mut Server, env: &Env<Limits>, request: sse::Request) {
    let child = Env { now: env.now, wall: env.wall, limits: env.limits.sse };
    let mut down = Queue::with_capacity(1);
    sse::down(&mut server.writer, &child, request, &mut server.writer_events, &mut down);
    if let Some(down) = down.pop() {
        server.requests.push(http::Request::Reply(down));
    }
}

// HTTP Done has already settled this response's downstream demand. Close the
// old writer explicitly before replacing it; its withdrawal belongs to that
// completed response and must not be sent into HTTP's WaitingNext state.
fn reset_writer(server: &mut Server, env: &Env<Limits>) {
    let child = Env { now: env.now, wall: env.wall, limits: env.limits.sse };
    let mut down = Queue::with_capacity(1);
    if !server.writer_closed {
        sse::down(&mut server.writer, &child, sse::Request::Close, &mut server.writer_events, &mut down);
    }
    for _slot in 0..server.writer_events.capacity() {
        if server.writer_events.pop().is_none() {
            break;
        }
    }
    server.writer = sse::Writer::new(&env.limits.sse);
    server.writer_closed = false;
}

fn http_event(
    server: &mut Server,
    service: &mut Service,
    credential: &skein_llm::Credential,
    env: &Env<Limits>,
    event: http::Event,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if server.state == State::Closing || server.state == State::Closed {
        return;
    }
    match event {
        http::Event::Body(Up::Bytes(data)) => {
            if data.len() > usize::try_from(server.body.room()).expect("u32 fits usize") {
                close(server, service, env, above, below);
                return;
            }
            for byte in data {
                server.body.push(byte).expect("checked body cap");
            }
            server.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
        }
        http::Event::Body(Up::End) => requested(server, service, env, above),
        http::Event::Reply(event) => match server.state {
            State::Streaming => {
                let child = Env { now: env.now, wall: env.wall, limits: env.limits.sse };
                let mut down = Queue::with_capacity(2);
                sse::up(&mut server.writer, &child, event, &mut server.writer_events, &mut down);
                for _slot in 0..2_u32 {
                    if let Some(down) = down.pop() {
                        server.requests.push(http::Request::Reply(down));
                    }
                }
            }
            State::Erring => match event {
                Up::Room => send_error(server, env),
                Up::Failed(_) | Up::Bytes(_) | Up::End => close(server, service, env, above, below),
            },
            State::New | State::Reading | State::Body | State::Waiting | State::Closing | State::Closed => {}
        },
        http::Event::Done(reuse) => {
            server.answer = None;
            server.reply = None;
            match reuse {
                http::Reuse::Keep => {
                    reset_writer(server, env);
                    server.state = State::Reading;
                    server.requests.push(http::Request::Next);
                }
                http::Reuse::Close => close(server, service, env, above, below),
            }
        }
        http::Event::Call(call) => entrance(server, service, credential, call),
        http::Event::Ended
        | http::Event::Failed(_)
        | http::Event::Refused(_)
        | http::Event::Body(Up::Failed(_) | Up::Room) => close(server, service, env, above, below),
        http::Event::Closed => {}
    }
}

fn requested(server: &mut Server, service: &mut Service, env: &Env<Limits>, above: &mut Queue<Event>) {
    if let Some(error) = server.refused.take() {
        server.body.clear();
        response_error(server, service, env, error);
        return;
    }
    let query = documents::request(service.config.provider, server.body.as_slice(), &env.limits.documents);
    server.body.clear();
    match query {
        Ok(query) => {
            let Ok(id) = service.calls.insert(Call { server: server.owner, active: true }) else {
                response_error(server, service, env, api::Error::Overloaded);
                return;
            };
            server.call = Some(id);
            server.state = State::Waiting;
            above.push(Event::Domain(domain::Event::Call { reply_to: ReplyTo::new(id.token()), query }));
        }
        Err(documents::Error::Malformed | documents::Error::TooLarge) => {
            response_error(server, service, env, api::Error::InvalidRequest);
        }
    }
}

fn next_event(
    server: &mut Server,
    service: &mut Service,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if server.state != State::Streaming {
        return;
    }
    let Some(answer) = &server.answer else {
        return;
    };
    match documents::event(service.config.provider, answer, server.sequence, server.response_id, &env.limits.documents)
    {
        Ok(Some(event)) => {
            let Some(next) = server.sequence.checked_add(1) else {
                close(server, service, env, above, below);
                return;
            };
            server.sequence = next;
            writer_down(server, env, sse::Request::Event(event));
        }
        Ok(None) => writer_down(server, env, sse::Request::Finish),
        Err(documents::Error::Malformed | documents::Error::TooLarge) => close(server, service, env, above, below),
    }
}

fn response_error(server: &mut Server, service: &Service, env: &Env<Limits>, error: api::Error) {
    let (status, kind, retry) = match error {
        api::Error::Overloaded => (503, b"overloaded_error".as_slice(), None),
        api::Error::RateLimited { retry_after } => (429, b"rate_limit_error".as_slice(), Some(retry_after)),
        api::Error::Unavailable => (500, b"server_error".as_slice(), None),
        api::Error::ContextTooLong => (400, b"context_length_exceeded".as_slice(), None),
        api::Error::Unauthorized => (401, b"authentication_error".as_slice(), None),
        api::Error::Exhausted { retry_after } => (429, b"usage_limit_reached".as_slice(), Some(retry_after)),
        api::Error::InvalidRequest => (400, b"invalid_request_error".as_slice(), None),
    };
    let body = match service.config.provider {
        documents::Provider::Anthropic => {
            let (kind, message) = match error {
                api::Error::Unavailable => (b"api_error".as_slice(), b"api_error".as_slice()),
                api::Error::ContextTooLong => (b"invalid_request_error".as_slice(), b"prompt is too long".as_slice()),
                api::Error::Exhausted { .. } => (b"rate_limit_error".as_slice(), b"rate_limit_error".as_slice()),
                api::Error::Overloaded
                | api::Error::RateLimited { .. }
                | api::Error::Unauthorized
                | api::Error::InvalidRequest => (kind, kind),
            };
            let error = anthropic::ProviderError {
                kind: bytes::copy_of(kind),
                message: bytes::copy_of(message),
                resets_in_seconds: None,
                resets_at: None,
            };
            match openai::encode_error(&error, &env.limits.documents.anthropic) {
                Ok(body) => body,
                Err(
                    anthropic::DecodeError::Malformed
                    | anthropic::DecodeError::Missing
                    | anthropic::DecodeError::WrongType
                    | anthropic::DecodeError::TooLarge,
                ) => bytes::copy_of(b"{}"),
            }
        }
        documents::Provider::OpenAi => {
            let error = openai::ProviderError {
                kind: bytes::copy_of(kind),
                message: bytes::copy_of(kind),
                resets_in_seconds: None,
                resets_at: None,
            };
            match openai::encode_error(&error, &env.limits.documents.openai) {
                Ok(body) => body,
                Err(
                    openai::DecodeError::Malformed
                    | openai::DecodeError::Missing
                    | openai::DecodeError::WrongType
                    | openai::DecodeError::TooLarge,
                ) => bytes::copy_of(b"{}"),
            }
        }
    };
    let mut headers = List::with_capacity(3);
    headers
        .push(Header { name: bytes::copy_of(b"content-type"), value: bytes::copy_of(b"application/json") })
        .expect("two headers");
    if let Some(delay) = retry {
        let seconds = Decimal::of(delay.as_nanos().div_euclid(1_000_000_000));
        headers
            .push(Header { name: bytes::copy_of(b"retry-after"), value: bytes::copy_of(seconds.as_bytes()) })
            .expect("second header");
    }
    if service.config.provider == documents::Provider::Anthropic {
        match error {
            api::Error::Exhausted { .. } => headers
                .push(Header {
                    name: bytes::copy_of(b"anthropic-ratelimit-unified-status"),
                    value: bytes::copy_of(b"rejected"),
                })
                .expect("third header"),
            api::Error::Overloaded
            | api::Error::RateLimited { .. }
            | api::Error::Unavailable
            | api::Error::ContextTooLong
            | api::Error::Unauthorized
            | api::Error::InvalidRequest => {}
        }
    }
    server.requests.push(http::Request::Respond(http::Response {
        status,
        headers: headers.into_boxed(),
        body: http::Body::Length(u64::try_from(body.len()).expect("bounded document")),
        close: false,
    }));
    server.reply = Some(body);
    server.reply_offset = 0;
    server.state = State::Erring;
    server.requests.push(http::Request::Reply(Down::Demand { read: Read::Nothing, room: env.limits.http.send }));
}

fn send_error(server: &mut Server, env: &Env<Limits>) {
    let Some(body) = &server.reply else {
        return;
    };
    let end = server
        .reply_offset
        .saturating_add(usize::try_from(env.limits.http.send).expect("u32 fits usize"))
        .min(body.len());
    let Some(piece) = body.get(server.reply_offset..end) else {
        return;
    };
    server.requests.push(http::Request::Reply(Down::Send(bytes::copy_of(piece))));
    server.reply_offset = end;
    if end == body.len() {
        server.requests.push(http::Request::Reply(Down::Finish));
    } else {
        server.requests.push(http::Request::Reply(Down::Demand { read: Read::Nothing, room: env.limits.http.send }));
    }
}

fn answer_fits(answer: &api::Answer, limits: &documents::Limits) -> bool {
    if answer.parts.len() > usize::try_from(limits.openai.parts.min(limits.anthropic.parts)).expect("u32 fits usize") {
        return false;
    }
    let mut length = Some(0_usize);
    for part in &answer.parts {
        let size = match part {
            api::Part::Text { text } | api::Part::Opaque { bytes: text } => Some(text.len()),
            api::Part::ToolCall { id, name, arguments } => match id.len().checked_add(name.len()) {
                Some(length) => length.checked_add(arguments.len()),
                None => None,
            },
            api::Part::ToolOutput { .. } => return false,
        };
        length = match length {
            Some(length) => match size {
                Some(size) => length.checked_add(size),
                None => None,
            },
            None => None,
        };
    }
    match length {
        Some(length) => {
            length
                <= usize::try_from(limits.openai.answer_bytes.max(limits.anthropic.answer_bytes))
                    .expect("u32 fits usize")
        }
        None => false,
    }
}

/// Checked maximum owned bytes under limits, or `None` when counters or containers cannot be bounded.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let request = limits.documents.openai.request_bytes.max(limits.documents.anthropic.request_bytes);
    if limits.http.body < u64::from(request)
        || limits.sse.chunk > limits.http.send
        || limits.documents.model_ceiling == 0
    {
        return None;
    }
    u64::try_from(size_of::<Server>())
        .ok()?
        .checked_add(u64::try_from(size_of::<Service>()).ok()?)?
        .checked_add(u64::from(limits.http.headers).checked_mul(u64::try_from(size_of::<Header>()).ok()?)?)?
        .checked_add(Slab::<Call>::worst_case(limits.calls)?)?
        .checked_add(http::worst_case(&limits.http)?)?
        .checked_add(sse::worst_case(&limits.sse)?)?
        .checked_add(openai::worst_case(&limits.documents.openai)?)?
        .checked_add(anthropic::worst_case(&limits.documents.anthropic)?)?
        .checked_add(List::<u8>::worst_case(request)?)?
        .checked_add(
            u64::from(limits.documents.openai.answer_bytes.max(limits.documents.anthropic.answer_bytes))
                .checked_mul(8)?,
        )?
        .checked_add(Queue::<http::Event>::worst_case(EVENTS)?)?
        .checked_add(Queue::<http::Request>::worst_case(REQUESTS)?)?
        .checked_add(Queue::<sse::Event>::worst_case(1)?)?
        .checked_add(Queue::<Down>::worst_case(2)?)?
        .checked_add(u64::from(limits.http.head).checked_mul(3)?)
}

#[cfg(test)]
mod tests {
    #[test]
    fn final_identity_is_unique_and_exhaustion_refuses_further_identities() {
        let mut count = u64::MAX - 1;
        assert_eq!(super::next_response(&mut count), Some(u64::MAX));
        assert_eq!(super::next_response(&mut count), None);
        assert_eq!(super::next_response(&mut count), None);
        assert_eq!(count, u64::MAX);
    }
}
