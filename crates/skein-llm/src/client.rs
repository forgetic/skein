//! One LLM exchange over a caller-owned plaintext stream.
//!
//! Reserve [`MAX_OUT`] before each entry point and drain [`resume`] while
//! [`Client::has_work`] holds. `Next` demands one data event; terminal faults
//! need no demand. On `Close`, close the real stream and acknowledge its
//! settlement with [`closed`]. The caller owns deadlines and retry policy.

use alloc::boxed::Box;
use core::mem;
use core::mem::size_of;

use skein_http::{Header, MaxOut, Method, client as http, sse};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Decimal, Env, List, Queue, Token, Writer, bytes};

use crate::{
    Block, Call, Completion, Delta, Endpoint, Error, Failure, Provider, anthropic, dialect, openai, translate,
};

/// Startup bounds, unchanged for a client's lifetime and reuse.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub http: http::Limits,
    pub sse: sse::Limits,
    pub dialect: crate::DocumentLimits,
    /// A non-success response body. At its cap the connection is closed.
    pub error_bytes: u32,
}

const HTTP_EVENTS: u32 = 4;
const SSE_EVENTS: u32 = 2;
const REQUESTS: u32 = 4;
const ROUTES: u32 = 8;

/// Each entry point's bound. Buffered output requires another `resume`.
pub const MAX_OUT: MaxOut = MaxOut { above: 4, below: 20 };
pub const UP_MAX_OUT: MaxOut = MAX_OUT;
pub const DOWN_MAX_OUT: MaxOut = MAX_OUT;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Request {
    Start,
    /// One delta, block or completion. A duplicate outstanding demand is inert.
    Next,
    Cancel,
    /// An unfinished call is cancelled; an idle/terminal one is just closed.
    Close,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Evidence {
    Unsent,
    Unknown,
    Response,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    Delta {
        owner: Token,
        delta: Delta,
    },
    Block {
        owner: Token,
        block: Block,
    },
    Completed {
        owner: Token,
        completion: Completion,
    },
    Failed {
        owner: Token,
        failure: Failure,
        evidence: Evidence,
        detail: Box<[u8]>,
    },
    Cancelled {
        owner: Token,
    },
    /// The HTTP body has drained. Only now may the binding be reused.
    Reusable,
    /// The owner must close the actual stream and later call `closed`.
    Close,
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    Start,
    /// Local routing work must run before external input is accepted.
    Ready,
    /// Upload room or a response head, as the HTTP machine describes.
    Http(http::Waiting),
    Next,
    Response,
    Draining,
    Idle,
    Closing,
    Nothing,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum State {
    Prepared,
    Head,
    Streaming,
    ErrorBody,
    Draining,
    Idle,
    Closing,
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Outcome {
    Pending,
    Terminal,
    Cancelled,
}

/// A single bound call; no socket, TLS object or OAuth refresh state.
#[expect(missing_debug_implementations, reason = "the HTTP machine retains bearer credentials in its request head")]
pub struct Client {
    owner: Token,
    provider: Provider,
    authority: Box<[u8]>,
    limits: Limits,
    state: State,
    outcome: Outcome,
    http: http::Client,
    sse: sse::Reader,
    decoder: Option<dialect::Decoder>,
    content: Option<List<Block>>,
    content_bytes: u64,
    call: Option<http::Call>,
    upload: Option<Box<[u8]>>,
    upload_offset: usize,
    http_events: Queue<http::Event>,
    sse_events: Queue<sse::Event>,
    outputs: Queue<dialect::Output>,
    requests: Queue<http::Request>,
    sse_below: Queue<Down>,
    error: List<u8>,
    status: u16,
    rate: openai::RateLimit,
    asked: bool,
    close_sent: bool,
    sse_closed: bool,
    evidence: Evidence,
    activity: u64,
}

impl Client {
    /// Validates and measures before touching a connection or holding the
    /// final encoded body. The caller binds its transport to this endpoint.
    pub fn prepare(input: Call, limits: &Limits) -> Result<Client, Error> {
        if worst_case(limits).is_none() {
            return Err(Error::Limit);
        }
        let provider = input.endpoint.provider;
        let (headers, body) = match provider {
            Provider::OpenAiCodex => {
                let request = translate::request(input.prompt, provider, &limits.dialect)?;
                let length = match openai::measure_request(&request, &limits.dialect) {
                    Ok(length) => length,
                    Err(error) => return Err(admission(error)),
                };
                let headers = headers(&input.endpoint, &input.credential, limits, length)?;
                let body = match openai::encode_request(&request, &limits.dialect) {
                    Ok(body) => body,
                    Err(error) => return Err(admission(error)),
                };
                (headers, body)
            }
            Provider::Anthropic => {
                let length = anthropic::measure_request(&input.prompt, &limits.dialect)?;
                let headers = headers(&input.endpoint, &input.credential, limits, length)?;
                let body = anthropic::encode_request(&input.prompt, &limits.dialect)?;
                (headers, body)
            }
        };
        let call = http::Call {
            method: Method::Post,
            target: input.endpoint.target,
            headers,
            body: http::Body::Length(u64::try_from(body.len()).expect("a body length fits u64")),
            close: false,
        };
        Ok(Client {
            owner: input.owner,
            provider: input.endpoint.provider,
            authority: input.endpoint.authority,
            limits: *limits,
            state: State::Prepared,
            outcome: Outcome::Pending,
            http: http::Client::new(&limits.http),
            sse: sse::Reader::new(&limits.sse),
            decoder: Some(dialect::Decoder::new(provider, &limits.dialect)),
            content: Some(List::with_capacity(limits.dialect.parts)),
            content_bytes: 0,
            call: Some(call),
            upload: Some(body),
            upload_offset: 0,
            http_events: Queue::with_capacity(HTTP_EVENTS),
            sse_events: Queue::with_capacity(SSE_EVENTS),
            outputs: Queue::with_capacity(dialect::MAX_OUT),
            requests: Queue::with_capacity(REQUESTS),
            sse_below: Queue::with_capacity(1),
            error: List::with_capacity(limits.error_bytes),
            status: 0,
            rate: openai::RateLimit { retry_after: None, reset: None, exhausted: false },
            asked: false,
            close_sent: false,
            sse_closed: false,
            evidence: Evidence::Unsent,
            activity: 0,
        })
    }

    #[must_use]
    pub const fn owner(&self) -> Token {
        self.owner
    }

    /// The number of complete SSE messages read for this call, including
    /// provider pings and extension events that consume no `Next` demand.
    #[must_use]
    pub const fn activity(&self) -> u64 {
        self.activity
    }

    /// The response head has arrived, whether or not the caller demanded its
    /// first output. The connection owner clears its head deadline here.
    #[must_use]
    pub fn response_received(&self) -> bool {
        self.evidence == Evidence::Response
    }

    /// True only for runnable work, never for output blocked on `Next`.
    #[must_use]
    pub fn has_work(&self) -> bool {
        if self.state == State::Closing || self.state == State::Closed {
            return false;
        }
        if !self.outputs.is_empty() {
            return self.asked;
        }
        if !self.http_events.is_empty() || !self.sse_events.is_empty() || !self.requests.is_empty() {
            return true;
        }
        if self.state == State::Streaming && self.asked {
            if let Some(decoder) = &self.decoder
                && decoder.has_ready()
            {
                return true;
            }
            return self.sse.waiting() == sse::Waiting::Next;
        }
        false
    }

    #[must_use]
    pub fn waiting(&self) -> Waiting {
        if self.has_work() {
            return Waiting::Ready;
        }
        match self.state {
            State::Prepared => Waiting::Start,
            State::Head => Waiting::Http(self.http.waiting()),
            State::Streaming if self.asked => Waiting::Response,
            State::Streaming => Waiting::Next,
            State::ErrorBody => Waiting::Response,
            State::Draining => Waiting::Draining,
            State::Idle => Waiting::Idle,
            State::Closing => Waiting::Closing,
            State::Closed => Waiting::Nothing,
        }
    }

    /// Reuses the HTTP machine and its carry-over, on the same authority and
    /// provider, only after `Reusable`. Refusal returns the prepared owner.
    #[expect(clippy::result_large_err, reason = "the owned admitted call is returned intact on refusal")]
    pub fn next_call(&mut self, mut prepared: Client) -> Result<(), Client> {
        if self.state != State::Idle
            || self.has_work()
            || self.http.waiting() != http::Waiting::Call
            || prepared.state != State::Prepared
            || self.provider != prepared.provider
            || self.authority != prepared.authority
            || self.limits != prepared.limits
        {
            return Err(prepared);
        }
        mem::swap(&mut self.http, &mut prepared.http);
        *self = prepared;
        Ok(())
    }
}

pub fn down(
    client: &mut Client,
    env: &Env<Limits>,
    request: Request,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    check_env(client, env);
    match request {
        Request::Start => {
            if client.state == State::Prepared {
                let call = client.call.take().expect("a prepared client holds its request");
                client.state = State::Head;
                http_down(client, env, http::Request::Call(call), below);
                client
                    .requests
                    .push(http::Request::Upload(Down::Demand { read: Read::Nothing, room: env.limits.http.send }));
            }
        }
        Request::Next => {
            if client.outcome == Outcome::Pending
                && (client.state == State::Prepared || client.state == State::Head || client.state == State::Streaming)
            {
                client.asked = true;
            }
        }
        Request::Cancel | Request::Close => {
            if client.outcome == Outcome::Pending {
                client.outcome = Outcome::Cancelled;
            }
            closing(client, env, above, below);
        }
    }
    resume(client, env, above, below);
}

pub fn up(client: &mut Client, env: &Env<Limits>, event: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    check_env(client, env);
    match client.state {
        State::Prepared | State::Closing | State::Closed => return,
        State::Head | State::Streaming | State::ErrorBody | State::Draining | State::Idle => {}
    }
    let transport_failed = match &event {
        Up::Failed(_) => true,
        Up::Bytes(_) | Up::End | Up::Room => false,
    };
    let child = Env { now: env.now, wall: env.wall, limits: env.limits.http };
    let before = below.len();
    http::up(&mut client.http, &child, event, &mut client.http_events, below);
    sent_evidence(client, below, before);
    // A peer failure ends the accepted attempt even when ordered provider
    // output is parked on an undemanded Next. Data backpressure must not
    // postpone faults or settlement of the actual lower binding.
    let failure = if transport_failed { Some(Failure::Unavailable) } else { queued_http_failure(client) };
    if let Some(failure) = failure {
        fail(client, failure, bytes::copy_of(b"provider transport or HTTP framing failed"), above);
        closing(client, env, above, below);
        return;
    }
    resume(client, env, above, below);
    if client.state == State::Idle && client.http.waiting() == http::Waiting::Close {
        closing(client, env, above, below);
    }
}

/// At most eight routing operations. The owner schedules another turn when
/// `has_work` is true; an undemanded output stays bounded in the client.
pub fn resume(client: &mut Client, env: &Env<Limits>, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    check_env(client, env);
    if client.state == State::Closing || client.state == State::Closed {
        return;
    }
    for _route in 0..ROUTES {
        if below.room() < 2 || above.room() < 3 {
            break;
        }
        if !client.outputs.is_empty() {
            if !client.asked {
                break;
            }
            let event = client.outputs.pop().expect("the output queue is nonempty");
            output(client, event, env, above, below);
        } else if let Some(event) = client.sse_events.pop() {
            sse_event(client, event, env, above, below);
        } else if let Some(event) = client.http_events.pop() {
            http_event(client, event, env, above, below);
        } else if let Some(request) = client.requests.pop() {
            http_down(client, env, request, below);
        } else if client.state == State::Streaming && client.asked {
            let decoder = client.decoder.as_mut().expect("a streaming client has its decoder");
            if decoder.has_ready() {
                decoder.ready(&env.limits.dialect, &mut client.outputs);
            } else if client.sse.waiting() == sse::Waiting::Next {
                sse_down(client, env, sse::Request::Next);
            } else {
                break;
            }
        } else {
            break;
        }
        if client.state == State::Closing || client.state == State::Closed {
            break;
        }
    }
}

/// Report a caller-owned deadline or transport failure. One failure is
/// emitted before closing; already terminal/cancelled calls keep their result.
pub fn abort(
    client: &mut Client,
    env: &Env<Limits>,
    failure: Failure,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    check_env(client, env);
    fail(client, failure, bytes::copy_of(b"call aborted by its owner"), above);
    closing(client, env, above, below);
}

/// Actual lower settlement, not merely the HTTP/SSE machines saying closed.
pub fn closed(client: &mut Client, env: &Env<Limits>, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    check_env(client, env);
    if client.state == State::Closed {
        return;
    }
    if client.outcome == Outcome::Pending {
        fail(client, Failure::Unavailable, bytes::copy_of(b"stream closed before completion"), above);
    }
    close_machines(client, env, below);
    if client.outcome == Outcome::Cancelled {
        above.push(Event::Cancelled { owner: client.owner });
        client.outcome = Outcome::Terminal;
    }
    client.state = State::Closed;
    above.push(Event::Closed);
}

fn check_env(client: &Client, env: &Env<Limits>) {
    assert!(client.limits == env.limits, "limits are fixed for the client's lifetime");
}

fn http_down(client: &mut Client, env: &Env<Limits>, request: http::Request, below: &mut Queue<Down>) {
    let child = Env { now: env.now, wall: env.wall, limits: env.limits.http };
    let before = below.len();
    http::down(&mut client.http, &child, request, &mut client.http_events, below);
    sent_evidence(client, below, before);
}

fn sent_evidence(client: &mut Client, below: &Queue<Down>, before: u32) {
    if client.evidence == Evidence::Unsent {
        for down in below.iter().skip(usize::try_from(before).expect("u32 fits usize")) {
            match down {
                Down::Send(_) => client.evidence = Evidence::Unknown,
                Down::Demand { .. } | Down::Finish => {}
            }
        }
    }
}

fn sse_down(client: &mut Client, env: &Env<Limits>, request: sse::Request) {
    let child = Env { now: env.now, wall: env.wall, limits: env.limits.sse };
    sse::down(&mut client.sse, &child, request, &mut client.sse_events, &mut client.sse_below);
    route_sse_down(client);
}

fn route_sse_down(client: &mut Client) {
    if let Some(request) = client.sse_below.pop() {
        client.requests.push(http::Request::Body(request));
    }
}

fn http_event(
    client: &mut Client,
    event: http::Event,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    match event {
        http::Event::Response(response) => response_head(client, response, env, above, below),
        http::Event::Upload(Up::Room) => {
            if let Some(body) = &client.upload {
                let end = client
                    .upload_offset
                    .saturating_add(usize::try_from(env.limits.http.send).expect("u32 fits usize"))
                    .min(body.len());
                let piece =
                    body.get(client.upload_offset..end).expect("the upload offset stays within the measured body");
                client.requests.push(http::Request::Upload(Down::Send(bytes::copy_of(piece))));
                client.upload_offset = end;
                if end == body.len() {
                    client.upload = None;
                    client.requests.push(http::Request::Upload(Down::Finish));
                } else {
                    client
                        .requests
                        .push(http::Request::Upload(Down::Demand { read: Read::Nothing, room: env.limits.http.send }));
                }
            }
        }
        http::Event::Upload(Up::Failed(_)) => client.upload = None,
        http::Event::Upload(Up::Bytes(_) | Up::End) => {
            fail(client, Failure::Protocol, bytes::copy_of(b"invalid upload event"), above);
            closing(client, env, above, below);
        }
        http::Event::Body(event) => body_event(client, event, env, above, below),
        http::Event::Done(reuse) => {
            if client.outcome == Outcome::Pending {
                if client.state == State::ErrorBody {
                    error_end(client, env, above);
                } else {
                    fail(client, Failure::Protocol, bytes::copy_of(b"provider stream has no terminal"), above);
                }
            }
            match reuse {
                http::Reuse::Keep
                    if client.state == State::Draining && client.http.waiting() == http::Waiting::Call =>
                {
                    client.state = State::Idle;
                    above.push(Event::Reusable);
                }
                http::Reuse::Keep | http::Reuse::Close => closing(client, env, above, below),
            }
        }
        http::Event::Failed(error) => {
            let failure = http_failure(error);
            fail(client, failure, bytes::copy_of(b"HTTP exchange failed"), above);
            closing(client, env, above, below);
        }
        http::Event::Closed => {}
    }
}

fn queued_http_failure(client: &Client) -> Option<Failure> {
    for event in &client.http_events {
        match event {
            http::Event::Failed(error) => return Some(http_failure(*error)),
            http::Event::Response(_)
            | http::Event::Upload(_)
            | http::Event::Body(_)
            | http::Event::Done(_)
            | http::Event::Closed => {}
        }
    }
    None
}

fn http_failure(error: http::Error) -> Failure {
    match error {
        http::Error::Refused(_) => Failure::Invalid,
        http::Error::Closed(_) | http::Error::Stream(_) => Failure::Unavailable,
        http::Error::Truncated { .. }
        | http::Error::Status
        | http::Error::Version
        | http::Error::Header
        | http::Error::Framing
        | http::Error::ChunkSize
        | http::Error::Chunk
        | http::Error::Trailer
        | http::Error::Upgrade => Failure::Protocol,
        http::Error::HeadTooLong | http::Error::TooManyHeaders => Failure::Limit,
    }
}

fn response_head(
    client: &mut Client,
    response: http::Response,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    client.evidence = Evidence::Response;
    client.status = response.status;
    for header in &response.headers {
        client.rate.observe(&header.name, &header.value);
    }
    if !identity_encoding(&response.headers) {
        fail(client, Failure::Protocol, bytes::copy_of(b"unsupported content encoding"), above);
        closing(client, env, above, below);
    } else if (200..300).contains(&response.status) {
        // The Codex subscription route can omit Content-Type on a valid SSE
        // response. Still reject explicit wrong or duplicate media types, and
        // require Anthropic's documented event-stream type.
        if media_type(&response.headers, b"text/event-stream", client.provider == Provider::OpenAiCodex) {
            client.state = State::Streaming;
        } else {
            fail(client, Failure::Protocol, bytes::copy_of(b"response is not an event stream"), above);
            closing(client, env, above, below);
        }
    } else {
        client.state = State::ErrorBody;
        client.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
    }
}

fn body_event(client: &mut Client, event: Up, env: &Env<Limits>, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    match client.state {
        State::Streaming => {
            let child = Env { now: env.now, wall: env.wall, limits: env.limits.sse };
            sse::up(&mut client.sse, &child, event, &mut client.sse_events, &mut client.sse_below);
            route_sse_down(client);
        }
        State::ErrorBody => match event {
            Up::Bytes(data) => {
                let room = usize::try_from(client.error.room()).expect("u32 fits usize");
                for &byte in data.iter().take(room) {
                    client.error.push(byte).expect("error bytes are within their cap");
                }
                if data.len() > room || client.error.room() == 0 {
                    error_end(client, env, above);
                    closing(client, env, above, below);
                } else {
                    client.requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
                }
            }
            Up::End => error_end(client, env, above),
            Up::Failed(_) | Up::Room => {
                fail(client, Failure::Unavailable, bytes::copy_of(b"error body failed"), above);
                closing(client, env, above, below);
            }
        },
        State::Prepared | State::Head | State::Draining | State::Idle | State::Closing | State::Closed => {}
    }
}

fn sse_event(
    client: &mut Client,
    event: sse::Event,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if client.state != State::Streaming {
        return;
    }
    match event {
        sse::Event::Message(message) => {
            client.activity = client.activity.checked_add(1).expect("an SSE message count fits u64");
            let decoder = client.decoder.as_mut().expect("the streaming state has its decoder");
            let parsed =
                decoder.event(&message, &env.limits.dialect, env.wall, client.status, client.rate, &mut client.outputs);
            if let Err(error) = parsed {
                let failure = match error {
                    openai::DecodeError::TooLarge => Failure::Limit,
                    openai::DecodeError::Malformed | openai::DecodeError::Missing | openai::DecodeError::WrongType => {
                        Failure::Protocol
                    }
                };
                fail(client, failure, bytes::copy_of(b"malformed provider event"), above);
                closing(client, env, above, below);
            }
        }
        sse::Event::Ended => {
            let decoder = client.decoder.as_mut().expect("the streaming state has its decoder");
            decoder.end(&env.limits.dialect, &mut client.outputs);
        }
        sse::Event::Failed(error) => {
            let failure = match error {
                sse::Error::LineTooLong | sse::Error::EventTooLong | sse::Error::FieldTooLong => Failure::Limit,
                sse::Error::Stream(_) => Failure::Unavailable,
            };
            fail(client, failure, bytes::copy_of(b"event stream failed"), above);
            closing(client, env, above, below);
        }
        sse::Event::Closed => {}
    }
}

fn output(
    client: &mut Client,
    event: dialect::Output,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if client.state != State::Streaming {
        return;
    }
    match event {
        dialect::Output::Part(part) => match part {
            Ok(block) => {
                let size = block_size(&block);
                let next = client.content_bytes.checked_add(size);
                match next {
                    Some(next) if next <= u64::from(env.limits.dialect.answer_bytes) => client.content_bytes = next,
                    Some(_) | None => {
                        fail(client, Failure::Limit, bytes::copy_of(b"completion exceeds its byte limit"), above);
                        closing(client, env, above, below);
                        return;
                    }
                }
                let content = client.content.as_mut().expect("a pending call holds its completion");
                if content.push(block.clone()).is_err() {
                    fail(client, Failure::Limit, bytes::copy_of(b"completion exceeds its part limit"), above);
                    closing(client, env, above, below);
                } else {
                    client.asked = false;
                    above.push(Event::Block { owner: client.owner, block });
                }
            }
            Err(error) => {
                let failure = match error {
                    Error::Limit => Failure::Limit,
                    Error::Invalid | Error::Unsupported => Failure::Protocol,
                };
                fail(client, failure, bytes::copy_of(b"completion translation failed"), above);
                closing(client, env, above, below);
            }
        },
        dialect::Output::TextDelta { index, content_index, text } => {
            client.asked = false;
            above.push(Event::Delta { owner: client.owner, delta: Delta::Text { index, content_index, text } });
        }
        dialect::Output::ArgumentsDelta { index, delta } => {
            client.asked = false;
            above.push(Event::Delta { owner: client.owner, delta: Delta::ToolArguments { index, delta } });
        }
        dialect::Output::ReasoningDelta { index, summary_index, text } => {
            client.asked = false;
            above.push(Event::Delta { owner: client.owner, delta: Delta::Reasoning { index, summary_index, text } });
        }
        dialect::Output::Completed { stop, usage } => {
            let content = client.content.take().expect("a pending call holds its completion");
            client.outcome = Outcome::Terminal;
            client.asked = false;
            above.push(Event::Completed {
                owner: client.owner,
                completion: Completion { content: content.into_boxed(), stop, usage },
            });
            client.state = State::Draining;
            close_sse(client, env);
            clear_requests(client);
            client.requests.push(http::Request::Discard);
        }
        dialect::Output::Failed { failure, detail } => {
            fail(client, failure, detail, above);
            closing(client, env, above, below);
        }
        dialect::Output::Progress => {}
    }
}

fn fail(client: &mut Client, failure: Failure, detail: Box<[u8]>, above: &mut Queue<Event>) {
    if client.outcome != Outcome::Pending {
        return;
    }
    client.outcome = Outcome::Terminal;
    client.asked = false;
    client.content = None;
    let detail = openai::clip_detail(&detail, client.limits.dialect.detail_bytes);
    above.push(Event::Failed { owner: client.owner, failure, evidence: client.evidence, detail });
}

fn error_end(client: &mut Client, env: &Env<Limits>, above: &mut Queue<Event>) {
    let mut limits = env.limits.dialect;
    limits.document_bytes = env.limits.error_bytes;
    let error = match openai::Json::from_bytes(client.error.as_slice(), &limits) {
        Ok(json) => openai::decode_error(&json, &limits).ok(),
        Err(_) => None,
    };
    let failure = translate::failure(openai::classify(client.status, error.as_ref(), client.rate, env.wall));
    let detail = match error {
        Some(error) => error.message,
        None => bytes::copy_of(b"provider HTTP error"),
    };
    client.error.clear();
    fail(client, failure, detail, above);
}

fn clear_requests(client: &mut Client) {
    for _request in 0..client.requests.capacity() {
        if client.requests.pop().is_none() {
            break;
        }
    }
}

fn close_sse(client: &mut Client, env: &Env<Limits>) {
    if client.sse_closed {
        return;
    }
    for _event in 0..client.sse_events.capacity() {
        drop(client.sse_events.pop());
    }
    clear_requests(client);
    sse_down(client, env, sse::Request::Close);
    client.sse_closed = true;
}

fn close_machines(client: &mut Client, env: &Env<Limits>, below: &mut Queue<Down>) {
    if client.close_sent {
        return;
    }
    close_sse(client, env);
    clear_requests(client);
    for _event in 0..client.http_events.capacity() {
        drop(client.http_events.pop());
    }
    for _event in 0..client.outputs.capacity() {
        drop(client.outputs.pop());
    }
    client.upload = None;
    client.call = None;
    client.content = None;
    client.decoder = None;
    client.error.clear();
    http_down(client, env, http::Request::Close, below);
    for _event in 0..client.http_events.capacity() {
        drop(client.http_events.pop());
    }
    client.close_sent = true;
    client.asked = false;
}

fn closing(client: &mut Client, env: &Env<Limits>, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    if client.state == State::Closing || client.state == State::Closed {
        return;
    }
    close_machines(client, env, below);
    client.state = State::Closing;
    above.push(Event::Close);
}

#[must_use]
pub fn largest_read(limits: &Limits) -> u32 {
    http::largest_read(&limits.http)
}

#[must_use]
pub fn largest_room(limits: &Limits) -> u32 {
    http::largest_room(&limits.http)
}

/// A conservative bound on held and temporary storage, including output
/// copies until handed to the caller; excludes the caller's input prompt.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.error_bytes == 0
        || limits.http.headers < 6
        || limits.dialect.parts == 0
        || limits.dialect.depth == 0
        || limits.dialect.tokens == 0
        || sse::largest_demand(&limits.sse) > limits.http.read
    {
        return None;
    }
    // Every opaque item was charged by its serialized length in the
    // dialect's answer budget. A JSON token occupies at least one wire byte.
    // Metadata envelopes are also charged in the held completion. Include
    // token wrappers for all retained replay values and their emission copy.
    let replay_tokens = u64::from(limits.dialect.parts)
        .checked_mul(u64::from(limits.dialect.tokens))?
        .min(u64::from(limits.dialect.answer_bytes));
    let replay_storage =
        replay_tokens.checked_mul(u64::try_from(size_of::<skein_json::Token>()).ok()?)?.checked_mul(2)?;
    u64::try_from(size_of::<Client>())
        .ok()?
        .checked_add(replay_storage)?
        .checked_add(http::worst_case(&limits.http)?)?
        .checked_add(sse::worst_case(&limits.sse)?)?
        .checked_add(openai::worst_case(&limits.dialect)?.max(anthropic::worst_case(&limits.dialect)?))?
        .checked_add(List::<Block>::worst_case(limits.dialect.parts)?.checked_mul(3)?)?
        .checked_add(u64::from(limits.dialect.answer_bytes).checked_mul(4)?)?
        .checked_add(u64::from(limits.dialect.request_bytes).checked_mul(4)?)?
        .checked_add(u64::from(limits.http.request).checked_mul(4)?)?
        .checked_add(u64::from(limits.sse.event).checked_mul(4)?)?
        .checked_add(u64::from(limits.error_bytes).checked_mul(4)?)?
        .checked_add(Queue::<http::Event>::worst_case(HTTP_EVENTS)?)?
        .checked_add(Queue::<sse::Event>::worst_case(SSE_EVENTS)?)?
        .checked_add(Queue::<dialect::Output>::worst_case(dialect::MAX_OUT)?)?
        .checked_add(Queue::<openai::Output>::worst_case(openai::MAX_OUT)?)?
        .checked_add(Queue::<anthropic::Output>::worst_case(anthropic::MAX_OUT)?)?
        .checked_add(Queue::<http::Request>::worst_case(REQUESTS)?)?
        .checked_add(Queue::<Down>::worst_case(1)?)
}

fn admission(error: openai::DecodeError) -> Error {
    match error {
        openai::DecodeError::TooLarge => Error::Limit,
        openai::DecodeError::Malformed | openai::DecodeError::Missing | openai::DecodeError::WrongType => {
            Error::Invalid
        }
    }
}

fn headers(
    endpoint: &Endpoint,
    credential: &crate::Credential,
    limits: &Limits,
    body_length: u32,
) -> Result<Box<[Header]>, Error> {
    if endpoint.authority.is_empty()
        || credential.access_token.is_empty()
        || !header_value(&credential.access_token)
        || !header_value(&credential.account_id)
        || !valid_authority(&endpoint.authority)
        || endpoint.target.first() != Some(&b'/')
    {
        return Err(Error::Invalid);
    }
    match endpoint.provider {
        Provider::OpenAiCodex if credential.account_id.is_empty() => return Err(Error::Invalid),
        Provider::Anthropic if !credential.account_id.is_empty() => return Err(Error::Invalid),
        Provider::OpenAiCodex | Provider::Anthropic => {}
    }
    let fields = provider_headers(endpoint, credential);
    let mut fixed_count: usize = 5;
    for &byte in &endpoint.target {
        if !(0x21..=0x7e).contains(&byte) || byte == b'#' {
            return Err(Error::Invalid);
        }
    }
    if endpoint.headers.len() > usize::try_from(limits.http.headers).expect("u32 fits usize") {
        return Err(Error::Limit);
    }
    let mut total = endpoint.authority.len().checked_add(endpoint.target.len()).ok_or(Error::Limit)?;
    total = total.checked_add(credential.access_token.len()).ok_or(Error::Limit)?;
    total = total.checked_add(credential.account_id.len()).ok_or(Error::Limit)?;
    for (index, header) in endpoint.headers.iter().enumerate() {
        if reserved(&header.name) || !header_name(&header.name) || !header_value(&header.value) {
            return Err(Error::Invalid);
        }
        for previous in endpoint.headers.get(..index).ok_or(Error::Invalid)? {
            if previous.is(&header.name) {
                return Err(Error::Invalid);
            }
        }
        total = total.checked_add(header.name.len()).ok_or(Error::Limit)?;
        total = total.checked_add(header.value.len()).ok_or(Error::Limit)?;
        total = total.checked_add(4).ok_or(Error::Limit)?;
    }
    // Exact wire length: the measured body fixes the Content-Length digits.
    // Check it before allocating any request head or credential copy.
    for fixed in [
        b"POST ".as_slice(),
        b" HTTP/1.1\r\n",
        b"Host: \r\n",
        b"Content-Type: application/json\r\n",
        b"Accept: text/event-stream\r\n",
        b"Accept-Encoding: identity\r\n",
        b"Authorization: Bearer \r\n",
        b"Content-Length: \r\n",
        b"\r\n",
    ] {
        total = total.checked_add(fixed.len()).ok_or(Error::Limit)?;
    }
    for (name, value) in fields.iter().flatten() {
        fixed_count = fixed_count.checked_add(1).ok_or(Error::Limit)?;
        total = total.checked_add(name.len()).ok_or(Error::Limit)?;
        // Codex account bytes were counted with the credential above.
        if !name.eq_ignore_ascii_case(b"chatgpt-account-id") {
            total = total.checked_add(value.len()).ok_or(Error::Limit)?;
        }
        total = total.checked_add(4).ok_or(Error::Limit)?;
    }
    let length = Decimal::of(u64::from(body_length));
    total = total.checked_add(length.as_bytes().len()).ok_or(Error::Limit)?;
    if total > usize::try_from(limits.http.request).expect("u32 fits usize")
        || endpoint.headers.len().saturating_add(fixed_count)
            > usize::try_from(limits.http.headers).expect("u32 fits usize")
    {
        return Err(Error::Limit);
    }
    let mut headers = List::with_capacity(limits.http.headers);
    for (name, value) in [
        (b"Host".as_slice(), endpoint.authority.as_ref()),
        (b"Content-Type".as_slice(), b"application/json".as_slice()),
        (b"Accept".as_slice(), b"text/event-stream".as_slice()),
        (b"Accept-Encoding".as_slice(), b"identity".as_slice()),
    ] {
        headers
            .push(Header { name: bytes::copy_of(name), value: bytes::copy_of(value) })
            .expect("fixed headers fit the checked cap");
    }
    for (name, value) in fields.iter().flatten() {
        headers
            .push(Header { name: bytes::copy_of(name), value: bytes::copy_of(value) })
            .expect("provider headers fit the checked cap");
    }
    let len = credential.access_token.len().checked_add(7).ok_or(Error::Limit)?;
    let mut bearer = Writer::new(len);
    bearer.put(b"Bearer ").expect("the bearer prefix was measured");
    bearer.put(&credential.access_token).expect("the token was measured");
    headers
        .push(Header { name: bytes::copy_of(b"Authorization"), value: bearer.finish() })
        .expect("six headers fit the checked cap");
    for header in &endpoint.headers {
        headers.push(header.clone()).expect("extra headers fit the checked cap");
    }
    Ok(headers.into_boxed())
}

fn provider_headers<'a>(
    endpoint: &Endpoint,
    credential: &'a crate::Credential,
) -> [Option<(&'static [u8], &'a [u8])>; 2] {
    match endpoint.provider {
        Provider::OpenAiCodex => [Some((b"chatgpt-account-id", &credential.account_id)), None],
        Provider::Anthropic => [
            default_header(endpoint, b"anthropic-version", b"2023-06-01"),
            default_header(endpoint, b"anthropic-beta", b"oauth-2025-04-20"),
        ],
    }
}
fn default_header(
    endpoint: &Endpoint,
    name: &'static [u8],
    value: &'static [u8],
) -> Option<(&'static [u8], &'static [u8])> {
    for header in &endpoint.headers {
        if header.is(name) {
            return None;
        }
    }
    Some((name, value))
}

fn valid_authority(value: &[u8]) -> bool {
    for &byte in value {
        if !byte.is_ascii_alphanumeric() && !b".-:[]".contains(&byte) {
            return false;
        }
    }
    true
}

fn header_name(name: &[u8]) -> bool {
    if name.is_empty() {
        return false;
    }
    for &byte in name {
        if !byte.is_ascii_alphanumeric() && !b"!#$%&'*+-.^_`|~".contains(&byte) {
            return false;
        }
    }
    true
}

fn header_value(value: &[u8]) -> bool {
    for &byte in value {
        if !(0x20..=0x7e).contains(&byte) && byte != b'\t' {
            return false;
        }
    }
    true
}

fn reserved(name: &[u8]) -> bool {
    for reserved in [
        b"host".as_slice(),
        b"authorization",
        b"chatgpt-account-id",
        b"x-api-key",
        b"content-type",
        b"accept",
        b"accept-encoding",
        b"content-length",
        b"transfer-encoding",
        b"connection",
    ] {
        if name.eq_ignore_ascii_case(reserved) {
            return true;
        }
    }
    false
}

fn identity_encoding(headers: &[Header]) -> bool {
    let mut seen = false;
    for header in headers {
        if header.is(b"content-encoding") {
            if seen || !trim(&header.value).eq_ignore_ascii_case(b"identity") {
                return false;
            }
            seen = true;
        }
    }
    true
}

fn media_type(headers: &[Header], expected: &[u8], allow_missing: bool) -> bool {
    let mut found = false;
    for header in headers {
        if header.is(b"content-type") {
            if found {
                return false;
            }
            let end = match bytes::find(&header.value, b";") {
                Some(end) => end,
                None => header.value.len(),
            };
            if !trim(header.value.get(..end).expect("the delimiter is within the value")).eq_ignore_ascii_case(expected)
            {
                return false;
            }
            found = true;
        }
    }
    found || allow_missing
}

fn trim(mut value: &[u8]) -> &[u8] {
    for _byte in 0..value.len() {
        if value.first() == Some(&b' ') || value.first() == Some(&b'\t') {
            value = value.get(1..).expect("a nonempty slice loses one byte");
        } else {
            break;
        }
    }
    for _byte in 0..value.len() {
        if value.last() == Some(&b' ') || value.last() == Some(&b'\t') {
            value = value.get(..value.len().saturating_sub(1)).expect("a nonempty slice loses one byte");
        } else {
            break;
        }
    }
    value
}

fn block_size(block: &Block) -> u64 {
    // Replay values have already passed the dialect's document cap. Count
    // their token payload without allocating another serialization.
    let mut bytes = 0_u64;
    let replay = match block {
        Block::Text { text, replay } | Block::Refusal { text, replay } => {
            bytes = u64::try_from(text.len()).expect("a slice length fits u64");
            replay.as_ref()
        }
        Block::ToolCall { id, name, arguments, replay } => {
            for part in [id, name, arguments] {
                bytes = bytes.saturating_add(u64::try_from(part.len()).expect("a slice length fits u64"));
            }
            replay.as_ref()
        }
        Block::ToolResult { id, text, .. } => {
            return u64::try_from(id.len())
                .expect("a slice length fits u64")
                .saturating_add(u64::try_from(text.len()).expect("a slice length fits u64"));
        }
        Block::Reasoning { replay } => Some(replay),
    };
    if let Some(replay) = replay {
        for token in replay.value.as_tokens() {
            let length = match token {
                skein_json::Token::Key(value) | skein_json::Token::String(value) | skein_json::Token::Number(value) => {
                    value.len()
                }
                skein_json::Token::ObjectStart
                | skein_json::Token::ObjectEnd
                | skein_json::Token::ArrayStart
                | skein_json::Token::ArrayEnd
                | skein_json::Token::True
                | skein_json::Token::False
                | skein_json::Token::Null => 1,
            };
            bytes = bytes.saturating_add(u64::try_from(length).expect("a slice length fits u64"));
        }
    }
    bytes
}
