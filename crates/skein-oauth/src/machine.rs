//! One bounded OAuth exchange. HTTP/TLS and persistence belong to the caller.

use crate::{ClaimSelector, DecodeError, Failure, Json, Limits, OAuthError, RefreshState, SavedToken, TokenResponse};
use crate::{classify, decode_error, decode_response, pkce, rotate};
use alloc::boxed::Box;
use skein_json::writer::Encoder;
use skein_lib::{Duration, Queue, Time, Wall, Writer, bytes};

/// Maximum requests emitted by one step.
pub const MAX_OUT: u32 = 1;

/// Startup bounds for one client and exchange.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ClientLimits {
    pub document: Limits,
    pub uri_bytes: u32,
    pub scope_bytes: u32,
    pub state_bytes: u32,
    pub code_bytes: u32,
    pub url_bytes: u32,
    pub request_bytes: u32,
    pub sign_in_time: Duration,
    pub request_time: Duration,
    pub backoff_base: Duration,
    pub backoff_ceiling: Duration,
    pub max_attempts: u32,
}

/// Upper bound for retained registration, exchange, request and answer bytes.
#[must_use]
pub fn client_worst_case(limits: &ClientLimits) -> Option<u64> {
    crate::worst_case(&limits.document)?
        .checked_add(u64::from(limits.uri_bytes).checked_mul(4)?)?
        .checked_add(u64::from(limits.scope_bytes))?
        .checked_add(u64::from(limits.state_bytes).checked_mul(2)?)?
        .checked_add(u64::from(limits.code_bytes))?
        .checked_add(u64::from(limits.url_bytes))?
        .checked_add(u64::from(limits.request_bytes).checked_mul(2)?)?
        .checked_add(4096)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WireFormat {
    Json,
    Form,
}

/// Caller-provided registration, with no provider identity or discovery.
#[expect(missing_debug_implementations, reason = "registration may hold a client secret")]
pub struct Registration {
    pub authorization_url: Box<[u8]>,
    pub token_endpoint: Box<[u8]>,
    pub client_id: Box<[u8]>,
    pub redirect_uri: Box<[u8]>,
    pub scope: Box<[u8]>,
    pub wire: WireFormat,
    pub client_secret: Option<Box<[u8]>>,
    pub pkce_for_confidential: bool,
    pub metadata_claim: Option<ClaimSelector>,
}

/// A complete token POST for the caller's HTTP/TLS stack.
#[expect(missing_debug_implementations, reason = "request body contains credentials")]
pub struct HttpRequest {
    pub id: u64,
    pub endpoint: Box<[u8]>,
    pub content_type: &'static [u8],
    pub body: Box<[u8]>,
    pub deadline: Time,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum HttpEvidence {
    Unsent,
    Unknown,
    Response,
}

/// Terminal HTTP answer, including bounded body and transport evidence.
#[expect(missing_debug_implementations, reason = "answer may contain tokens")]
pub struct HttpResponse {
    pub id: u64,
    pub status: u16,
    pub body: Box<[u8]>,
    pub retry_after: Duration,
    pub evidence: HttpEvidence,
    pub now: Time,
    pub wall: Wall,
}

#[expect(missing_debug_implementations, reason = "events can contain secrets")]
pub enum Event {
    SignIn {
        registration: Registration,
        key: u32,
        generation: u64,
        state: Box<[u8]>,
        verifier: Option<Box<[u8]>>,
        now: Time,
    },
    Redirected {
        uri: Box<[u8]>,
        state: Box<[u8]>,
        code: Option<Box<[u8]>>,
        error: Option<Box<[u8]>>,
        now: Time,
    },
    Refresh {
        registration: Registration,
        prior: RefreshState,
        now: Time,
    },
    Http(HttpResponse),
    Tick {
        now: Time,
    },
    Cancel,
    Reset,
}

#[expect(missing_debug_implementations, reason = "requests can contain secrets")]
pub enum Request {
    Visit { url: Box<[u8]> },
    Http(HttpRequest),
    Tokens { record: SavedToken },
    Failed { failure: Failure },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Phase {
    Idle,
    AwaitRedirect,
    AwaitHttp { id: u64, deadline: Time },
    Backoff { due: Time },
    Done,
}

enum Grant {
    SignIn { key: u32, generation: u64, code: Option<Box<[u8]>> },
    Refresh { prior: RefreshState },
}

struct Exchange {
    registration: Registration,
    grant: Grant,
    state: Box<[u8]>,
    verifier: Option<Box<[u8]>>,
    deadline: Time,
    attempts: u32,
}

/// One sans-IO sign-in or refresh. The caller reserves `MAX_OUT` output slots.
#[expect(missing_debug_implementations, reason = "machine retains secrets")]
pub struct Client {
    limits: ClientLimits,
    phase: Phase,
    active: Option<Exchange>,
    next_id: u64,
}

impl Client {
    pub fn new(limits: ClientLimits) -> Result<Client, Failure> {
        if limits.max_attempts == 0
            || limits.request_time == Duration::ZERO
            || limits.sign_in_time == Duration::ZERO
            || client_worst_case(&limits).is_none()
        {
            return Err(Failure::Limit);
        }
        Ok(Client { limits, phase: Phase::Idle, active: None, next_id: 1 })
    }

    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The next time the caller must send `Tick`, or none while idle or done.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        match self.phase {
            Phase::Idle | Phase::Done => None,
            Phase::AwaitRedirect => Some(self.active.as_ref()?.deadline),
            Phase::AwaitHttp { deadline, .. } => Some(deadline),
            Phase::Backoff { due } => Some(due),
        }
    }

    pub fn step(&mut self, event: Event, out: &mut Queue<Request>) {
        assert!(out.room() >= MAX_OUT, "caller reserves one OAuth output");
        match event {
            Event::SignIn { registration, key, generation, state, verifier, now } => {
                if self.phase != Phase::Idle {
                    return;
                }
                let result = self.sign_in(registration, key, generation, state, verifier, now);
                self.emit_start(result, out);
            }
            Event::Refresh { registration, prior, now } => {
                if self.phase != Phase::Idle {
                    return;
                }
                let result = self.refresh(registration, prior, now);
                self.emit_start(result, out);
            }
            Event::Redirected { uri, state, code, error, now } => self.redirected(&uri, &state, code, error, now, out),
            Event::Http(answer) => self.http(answer, out),
            Event::Tick { now } => self.tick(now, out),
            Event::Cancel => {
                if self.phase != Phase::Idle && self.phase != Phase::Done {
                    self.finish(Failure::Cancelled, out);
                }
            }
            Event::Reset => {
                if self.phase == Phase::Done {
                    self.active = None;
                    self.phase = Phase::Idle;
                }
            }
        }
    }

    fn emit_start(&mut self, result: Result<(Exchange, Request, Phase), Failure>, out: &mut Queue<Request>) {
        match result {
            Ok((exchange, request, phase)) => {
                self.active = Some(exchange);
                self.phase = phase;
                out.push(request);
            }
            Err(failure) => self.finish(failure, out),
        }
    }

    fn sign_in(
        &mut self,
        registration: Registration,
        key: u32,
        generation: u64,
        state: Box<[u8]>,
        verifier: Option<Box<[u8]>>,
        now: Time,
    ) -> Result<(Exchange, Request, Phase), Failure> {
        validate_registration(&registration, &self.limits, true)?;
        if state.len() < 16 || state.len() > usize::try_from(self.limits.state_bytes).expect("u32 fits") {
            return Err(Failure::Limit);
        }
        let pkce_required = registration.client_secret.is_none() || registration.pkce_for_confidential;
        let challenge = match (&verifier, pkce_required) {
            (Some(value), true) => Some(match pkce::challenge(value) {
                Ok(challenge) => challenge,
                Err(error) => return Err(admit(error)),
            }),
            (None, false) => None,
            (Some(_), false) | (None, true) => return Err(Failure::Malformed),
        };
        let url = authorization_url(&registration, &state, challenge.as_deref(), &self.limits)?;
        let deadline = now.checked_add(self.limits.sign_in_time).ok_or(Failure::Limit)?;
        let exchange = Exchange {
            registration,
            grant: Grant::SignIn { key, generation, code: None },
            state,
            verifier,
            deadline,
            attempts: 0,
        };
        Ok((exchange, Request::Visit { url }, Phase::AwaitRedirect))
    }

    fn refresh(
        &mut self,
        registration: Registration,
        prior: RefreshState,
        now: Time,
    ) -> Result<(Exchange, Request, Phase), Failure> {
        validate_registration(&registration, &self.limits, false)?;
        if prior.refresh_token.is_empty()
            || prior.refresh_token.len() > usize::try_from(self.limits.document.token_bytes).expect("u32 fits")
        {
            return Err(Failure::Limit);
        }
        let deadline = now.checked_add(self.limits.sign_in_time).ok_or(Failure::Limit)?;
        let mut exchange = Exchange {
            registration,
            grant: Grant::Refresh { prior },
            state: bytes::copy_of(b""),
            verifier: None,
            deadline,
            attempts: 0,
        };
        let (request, phase) = self.http_request(&mut exchange, now)?;
        Ok((exchange, Request::Http(request), phase))
    }

    fn redirected(
        &mut self,
        uri: &[u8],
        state: &[u8],
        code: Option<Box<[u8]>>,
        error: Option<Box<[u8]>>,
        now: Time,
        out: &mut Queue<Request>,
    ) {
        if self.phase != Phase::AwaitRedirect {
            return;
        }
        let Some(mut exchange) = self.active.take() else {
            self.finish(Failure::Malformed, out);
            return;
        };
        if now >= exchange.deadline
            || uri != exchange.registration.redirect_uri.as_ref()
            || state.len() != exchange.state.len()
            || !constant_time_equal(state, &exchange.state)
        {
            self.finish(Failure::InvalidRedirect, out);
            return;
        }
        if error.is_some() {
            self.finish(Failure::Refused, out);
            return;
        }
        let Some(code) = code else {
            self.finish(Failure::Malformed, out);
            return;
        };
        if code.is_empty() || code.len() > usize::try_from(self.limits.code_bytes).expect("u32 fits") {
            self.finish(Failure::Limit, out);
            return;
        }
        match &mut exchange.grant {
            Grant::SignIn { code: slot, .. } => *slot = Some(code),
            Grant::Refresh { .. } => {
                self.finish(Failure::Malformed, out);
                return;
            }
        }
        match self.http_request(&mut exchange, now) {
            Ok((request, phase)) => {
                self.phase = phase;
                self.active = Some(exchange);
                out.push(Request::Http(request));
            }
            Err(failure) => self.finish(failure, out),
        }
    }

    fn http(&mut self, answer: HttpResponse, out: &mut Queue<Request>) {
        let Phase::AwaitHttp { id, deadline } = self.phase else {
            return;
        };
        if answer.id != id {
            return;
        }
        let Some(exchange) = self.active.take() else {
            self.finish(Failure::Malformed, out);
            return;
        };
        if answer.now >= deadline || answer.now >= exchange.deadline {
            self.finish(Failure::TimedOut, out);
            return;
        }
        if answer.evidence == HttpEvidence::Unsent {
            self.retry_or_fail(exchange, Failure::Unavailable, answer.now, answer.retry_after, true, out);
            return;
        }
        if answer.body.len() > usize::try_from(self.limits.document.document_bytes).expect("u32 fits") {
            self.finish(Failure::Limit, out);
            return;
        }
        if (200..300).contains(&answer.status) {
            let response = match Json::from_bytes(&answer.body, &self.limits.document) {
                Ok(value) => decode_response(&value, &self.limits.document),
                Err(error) => Err(error),
            };
            match response {
                Ok(tokens) => match self.record(&exchange, &tokens, answer.wall) {
                    Ok(record) => {
                        self.phase = Phase::Done;
                        out.push(Request::Tokens { record });
                    }
                    Err(failure) => self.finish(failure, out),
                },
                Err(error) => self.finish(admit(error), out),
            }
            return;
        }
        let error: Result<Option<OAuthError>, DecodeError> = if answer.body.is_empty() {
            Ok(None)
        } else {
            match Json::from_bytes(&answer.body, &self.limits.document) {
                Ok(value) => match decode_error(&value, &self.limits.document) {
                    Ok(error) => Ok(Some(error)),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        };
        let failure = match error {
            Ok(value) => classify(answer.status, value.as_ref(), answer.retry_after),
            Err(_) => Failure::Malformed,
        };
        let safe = answer.status == 429 && answer.evidence == HttpEvidence::Response;
        self.retry_or_fail(exchange, failure, answer.now, answer.retry_after, safe, out);
    }

    fn record(&self, exchange: &Exchange, response: &TokenResponse, wall: Wall) -> Result<SavedToken, Failure> {
        let prior = match &exchange.grant {
            Grant::Refresh { prior } => prior.clone(),
            Grant::SignIn { key, generation, .. } => {
                let Some(refresh) = &response.refresh_token else {
                    return Err(Failure::Malformed);
                };
                RefreshState { key: *key, generation: *generation, refresh_token: refresh.clone() }
            }
        };
        match rotate(&prior, response, exchange.registration.metadata_claim.as_ref(), wall, &self.limits.document) {
            Ok(record) => Ok(record),
            Err(error) => Err(admit(error)),
        }
    }

    fn retry_or_fail(
        &mut self,
        exchange: Exchange,
        failure: Failure,
        now: Time,
        retry_after: Duration,
        safe: bool,
        out: &mut Queue<Request>,
    ) {
        let transient = match failure {
            Failure::Unavailable | Failure::TimedOut | Failure::RateLimited { .. } => true,
            Failure::Refused
            | Failure::ClientRejected
            | Failure::Malformed
            | Failure::InvalidRedirect
            | Failure::Limit
            | Failure::Cancelled => false,
        };
        if safe && transient && exchange.attempts < self.limits.max_attempts {
            let shift = u64::from(exchange.attempts.saturating_sub(1)).min(63);
            let factor = 1_u64.checked_shl(u32::try_from(shift).expect("shift fits")).unwrap_or(u64::MAX);
            let delay =
                self.limits.backoff_base.saturating_mul(factor).min(self.limits.backoff_ceiling).max(retry_after);
            if let Some(due) = now.checked_add(delay)
                && due < exchange.deadline
            {
                self.phase = Phase::Backoff { due };
                self.active = Some(exchange);
                return;
            }
        }
        self.finish(failure, out);
    }

    fn tick(&mut self, now: Time, out: &mut Queue<Request>) {
        match self.phase {
            Phase::AwaitRedirect => {
                if let Some(value) = &self.active
                    && now >= value.deadline
                {
                    self.finish(Failure::TimedOut, out);
                }
            }
            Phase::AwaitHttp { deadline, .. } => {
                if now >= deadline {
                    self.finish(Failure::TimedOut, out);
                }
            }
            Phase::Backoff { due } => {
                if now < due {
                    return;
                }
                let Some(mut exchange) = self.active.take() else {
                    self.finish(Failure::Malformed, out);
                    return;
                };
                match self.http_request(&mut exchange, now) {
                    Ok((request, phase)) => {
                        self.phase = phase;
                        self.active = Some(exchange);
                        out.push(Request::Http(request));
                    }
                    Err(failure) => self.finish(failure, out),
                }
            }
            Phase::Idle | Phase::Done => {}
        }
    }

    fn http_request(&mut self, exchange: &mut Exchange, now: Time) -> Result<(HttpRequest, Phase), Failure> {
        if now >= exchange.deadline || exchange.attempts >= self.limits.max_attempts {
            return Err(Failure::TimedOut);
        }
        let deadline = now.checked_add(self.limits.request_time).ok_or(Failure::Limit)?.min(exchange.deadline);
        let body = token_body(exchange, &self.limits)?;
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).ok_or(Failure::Limit)?;
        exchange.attempts = exchange.attempts.checked_add(1).ok_or(Failure::Limit)?;
        let content_type = match exchange.registration.wire {
            WireFormat::Json => b"application/json".as_slice(),
            WireFormat::Form => b"application/x-www-form-urlencoded".as_slice(),
        };
        let request = HttpRequest {
            id,
            endpoint: bytes::copy_of(&exchange.registration.token_endpoint),
            content_type,
            body,
            deadline,
        };
        Ok((request, Phase::AwaitHttp { id, deadline }))
    }

    fn finish(&mut self, failure: Failure, out: &mut Queue<Request>) {
        self.active = None;
        self.phase = Phase::Done;
        out.push(Request::Failed { failure });
    }
}

fn admit(error: DecodeError) -> Failure {
    match error {
        DecodeError::TooLarge => Failure::Limit,
        DecodeError::Malformed | DecodeError::Missing | DecodeError::WrongType | DecodeError::Version => {
            Failure::Malformed
        }
    }
}

fn validate_registration(registration: &Registration, limits: &ClientLimits, sign_in: bool) -> Result<(), Failure> {
    for value in [&registration.authorization_url, &registration.token_endpoint, &registration.redirect_uri] {
        if value.is_empty()
            || value.len() > usize::try_from(limits.uri_bytes).expect("u32 fits")
            || value.contains(&b'#')
        {
            return Err(Failure::Limit);
        }
    }
    if !registration.authorization_url.starts_with(b"https://") || !registration.token_endpoint.starts_with(b"https://")
    {
        return Err(Failure::Malformed);
    }
    if sign_in {
        if registration.client_secret.is_none() && !loopback(&registration.redirect_uri) {
            return Err(Failure::InvalidRedirect);
        }
        if registration.client_secret.is_some() && !registration.redirect_uri.starts_with(b"https://") {
            return Err(Failure::InvalidRedirect);
        }
    }
    if registration.client_id.is_empty()
        || registration.client_id.len() > usize::try_from(limits.document.client_bytes).expect("u32 fits")
        || registration.scope.len() > usize::try_from(limits.scope_bytes).expect("u32 fits")
    {
        return Err(Failure::Limit);
    }
    if let Some(secret) = &registration.client_secret
        && (secret.is_empty() || secret.len() > usize::try_from(limits.document.client_bytes).expect("u32 fits"))
    {
        return Err(Failure::Limit);
    }
    Ok(())
}

fn loopback(uri: &[u8]) -> bool {
    let rest = if let Some(value) = uri.strip_prefix(b"http://127.0.0.1:") {
        value
    } else if let Some(value) = uri.strip_prefix(b"http://[::1]:") {
        value
    } else {
        return false;
    };
    let Some(path_at) = bytes::find(rest, b"/") else { return false };
    let Some(port) = rest.get(..path_at) else { return false };
    if port.is_empty() || port.len() > 5 {
        return false;
    }
    let mut number = 0_u32;
    for &digit in port {
        if !digit.is_ascii_digit() {
            return false;
        }
        let Some(tens) = number.checked_mul(10_u32) else { return false };
        let Some(value) = tens.checked_add(u32::from(digit.wrapping_sub(b'0'))) else { return false };
        number = value;
    }
    number > 0 && u16::try_from(number).is_ok()
}

fn constant_time_equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (&left, &right) in a.iter().zip(b) {
        diff |= left ^ right;
    }
    diff == 0
}

fn authorization_url(
    registration: &Registration,
    state: &[u8],
    challenge: Option<&[u8]>,
    limits: &ClientLimits,
) -> Result<Box<[u8]>, Failure> {
    let fields = [
        Some((b"response_type".as_slice(), b"code".as_slice())),
        Some((b"client_id".as_slice(), registration.client_id.as_ref())),
        Some((b"redirect_uri".as_slice(), registration.redirect_uri.as_ref())),
        Some((b"state".as_slice(), state)),
        if registration.scope.is_empty() { None } else { Some((b"scope".as_slice(), registration.scope.as_ref())) },
        match challenge {
            Some(value) => Some((b"code_challenge".as_slice(), value)),
            None => None,
        },
        match challenge {
            Some(_) => Some((b"code_challenge_method".as_slice(), b"S256".as_slice())),
            None => None,
        },
    ];
    let query = form_fields(&fields, limits.url_bytes)?;
    let prefix = registration.authorization_url.len().checked_add(1).ok_or(Failure::Limit)?;
    let length = prefix.checked_add(query.len()).ok_or(Failure::Limit)?;
    if length > usize::try_from(limits.url_bytes).expect("u32 fits") {
        return Err(Failure::Limit);
    }
    let mut writer = Writer::new(length);
    writer.put(&registration.authorization_url).expect("measured URL");
    let separator = if registration.authorization_url.contains(&b'?') { b"&" } else { b"?" };
    writer.put(separator).expect("measured URL");
    writer.put(&query).expect("measured URL");
    Ok(writer.finish())
}

fn token_body(exchange: &Exchange, limits: &ClientLimits) -> Result<Box<[u8]>, Failure> {
    let registration = &exchange.registration;
    let (grant_type, name, value, redirect, verifier) = match &exchange.grant {
        Grant::SignIn { code: Some(code), .. } => (
            b"authorization_code".as_slice(),
            b"code".as_slice(),
            code.as_ref(),
            Some(registration.redirect_uri.as_ref()),
            exchange.verifier.as_deref(),
        ),
        Grant::SignIn { code: None, .. } => return Err(Failure::Malformed),
        Grant::Refresh { prior } => {
            (b"refresh_token".as_slice(), b"refresh_token".as_slice(), prior.refresh_token.as_ref(), None, None)
        }
    };
    let fields = [
        Some((b"grant_type".as_slice(), grant_type)),
        Some((b"client_id".as_slice(), registration.client_id.as_ref())),
        Some((name, value)),
        match redirect {
            Some(item) => Some((b"redirect_uri".as_slice(), item)),
            None => None,
        },
        match verifier {
            Some(item) => Some((b"code_verifier".as_slice(), item)),
            None => None,
        },
        match registration.client_secret.as_deref() {
            Some(item) => Some((b"client_secret".as_slice(), item)),
            None => None,
        },
    ];
    match registration.wire {
        WireFormat::Form => form_fields(&fields, limits.request_bytes),
        WireFormat::Json => json_fields(&fields, limits),
    }
}

fn json_fields(fields: &[Option<(&[u8], &[u8])>], limits: &ClientLimits) -> Result<Box<[u8]>, Failure> {
    let writer_limits = skein_json::writer::Limits { depth: 2, length: limits.request_bytes };
    let mut measure = Encoder::measure(&writer_limits);
    measure.object_start();
    for field in fields {
        let Some((key, value)) = field else { continue };
        measure.key(key);
        measure.string(value);
    }
    measure.object_end();
    let length = match measure.measured() {
        Ok(length) => length,
        Err(skein_json::writer::Refusal::TooLong | skein_json::writer::Refusal::TooDeep) => return Err(Failure::Limit),
        Err(skein_json::writer::Refusal::Text | skein_json::writer::Refusal::Number) => return Err(Failure::Malformed),
    };
    let mut writer = Encoder::write(length, &writer_limits);
    writer.object_start();
    for field in fields {
        let Some((key, value)) = field else { continue };
        writer.key(key);
        writer.string(value);
    }
    writer.object_end();
    Ok(writer.finish())
}

fn form_fields(fields: &[Option<(&[u8], &[u8])>], cap: u32) -> Result<Box<[u8]>, Failure> {
    let mut length = 0_usize;
    let mut count = 0_usize;
    for field in fields {
        let Some((key, value)) = field else { continue };
        if count != 0 {
            length = length.checked_add(1).ok_or(Failure::Limit)?;
        }
        length = length.checked_add(encoded_len(key)?).ok_or(Failure::Limit)?;
        length = length.checked_add(1).ok_or(Failure::Limit)?;
        length = length.checked_add(encoded_len(value)?).ok_or(Failure::Limit)?;
        count = count.checked_add(1).ok_or(Failure::Limit)?;
    }
    if length > usize::try_from(cap).expect("u32 fits") {
        return Err(Failure::Limit);
    }
    let mut writer = Writer::new(length);
    let mut first = true;
    for field in fields {
        let Some((key, value)) = field else { continue };
        if !first {
            writer.put(b"&").expect("measured fields");
        }
        first = false;
        write_encoded(&mut writer, key);
        writer.put(b"=").expect("measured fields");
        write_encoded(&mut writer, value);
    }
    Ok(writer.finish())
}

fn encoded_len(value: &[u8]) -> Result<usize, Failure> {
    let mut length = 0_usize;
    for &byte in value {
        length = length.checked_add(if plain(byte) { 1 } else { 3 }).ok_or(Failure::Limit)?;
    }
    Ok(length)
}

fn write_encoded(writer: &mut Writer, value: &[u8]) {
    const HEX: &[u8] = b"0123456789ABCDEF";
    for &byte in value {
        if plain(byte) {
            writer.put(&[byte]).expect("measured encoding");
        } else {
            let high = *HEX.get(usize::from(byte >> 4_u32)).expect("hex digit");
            let low = *HEX.get(usize::from(byte & 15_u8)).expect("hex digit");
            writer.put(&[b'%', high, low]).expect("measured encoding");
        }
    }
}

const fn plain(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.' || byte == b'_' || byte == b'~'
}
