use crate::openai::{Input, Tool};
use alloc::boxed::Box;
use skein_json::{Token, tokenizer, writer};
use skein_lib::{Duration, List, Wall, bytes};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub request_bytes: u32,
    pub document_bytes: u32,
    pub string_bytes: u32,
    pub depth: u32,
    pub tokens: u32,
    pub parts: u32,
    pub input_bytes: u32,
    pub opaque_bytes: u32,
    pub answer_bytes: u32,
    pub detail_bytes: u32,
}
impl Limits {
    #[must_use]
    pub const fn writer_limits(&self) -> writer::Limits {
        writer::Limits { depth: self.depth, length: self.document_bytes }
    }
    #[must_use]
    pub const fn tokenizer_limits(&self) -> tokenizer::Limits {
        tokenizer::Limits {
            depth: self.depth,
            string: self.string_bytes,
            number: 32,
            chunk: 256,
            length: self.document_bytes,
        }
    }
}
/// A conservative per-exchange bound including one event's tokens, temporary
/// tokenizer/writer storage and a completion being handed to its owner.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let tokens = List::<Token>::worst_case(limits.tokens)?;
    // Translation validates the aggregate request before it clones it.
    // Every JSON token costs at least one byte in that request budget.
    // The caller's prompt and its translated copy may coexist until encoding.
    let request_count = u64::from(limits.tokens)
        .checked_mul(u64::from(limits.parts))?
        .checked_mul(2)?
        .min(u64::from(limits.request_bytes));
    let request_tokens = request_count.checked_mul(u64::try_from(size_of::<Token>()).ok()?)?.checked_mul(2)?;
    let request_slots = List::<Input>::worst_case(limits.parts)?
        .checked_add(List::<Tool>::worst_case(limits.parts)?)?
        .checked_mul(2)?;
    let documents = u64::from(limits.document_bytes).checked_mul(8)?;
    let answer = u64::from(limits.answer_bytes).checked_mul(4)?;
    tokens
        .checked_mul(4)?
        .checked_add(request_tokens)?
        .checked_add(request_slots)?
        .checked_add(documents)?
        .checked_add(answer)?
        .checked_add(u64::from(limits.request_bytes))?
        .checked_add(tokenizer::worst_case(&limits.tokenizer_limits())?)?
        .checked_add(writer::worst_case(&limits.writer_limits())?)?
        .checked_add(crate::openai::response::decoder_worst_case(limits)?)
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum DecodeError {
    Malformed,
    Missing,
    WrongType,
    TooLarge,
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Stop {
    EndTurn,
    ToolUse,
    MaxTokens,
    Refusal,
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}
impl Usage {
    pub const ZERO: Usage = Usage { input_tokens: 0, output_tokens: 0, cache_read_tokens: 0, cache_write_tokens: 0 };
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Failure {
    /// Provider bytes violate the dialect or end before its terminal event.
    Protocol,
    /// A caller-configured local resource limit was exceeded.
    Limit,
    Unauthorized,
    Exhausted {
        retry_after: Duration,
    },
    RateLimited {
        retry_after: Duration,
    },
    Overloaded,
    Unavailable,
    ContextTooLong,
    Invalid,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ProviderError {
    pub kind: Box<[u8]>,
    pub message: Box<[u8]>,
    pub resets_in_seconds: Option<u64>,
    pub resets_at: Option<u64>,
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct RateLimit {
    pub retry_after: Option<Duration>,
    pub reset: Option<u64>,
    pub exhausted: bool,
}
impl RateLimit {
    pub const NONE: RateLimit = RateLimit { retry_after: None, reset: None, exhausted: false };
    pub fn observe(&mut self, name: &[u8], value: &[u8]) {
        if name.eq_ignore_ascii_case(b"retry-after") {
            self.retry_after = None;
            if let Some(n) = decimal(value) {
                self.retry_after = Some(Duration::from_secs(n));
            }
        }
        if name.eq_ignore_ascii_case(b"anthropic-ratelimit-unified-reset") {
            self.reset = decimal(value);
        }
        if name.eq_ignore_ascii_case(b"anthropic-ratelimit-unified-status") && value == b"rejected" {
            self.exhausted = true;
        }
    }
    #[must_use]
    pub fn delay(self, error: Option<&ProviderError>, wall: Wall) -> Duration {
        if let Some(delay) = self.retry_after {
            return delay;
        }
        if let Some(error) = error {
            if let Some(seconds) = error.resets_in_seconds {
                return Duration::from_secs(seconds);
            }
            if let Some(reset) = error.resets_at {
                return Duration::from_secs(reset.saturating_sub(wall.as_secs()));
            }
        }
        match self.reset {
            Some(reset) => Duration::from_secs(reset.saturating_sub(wall.as_secs())),
            None => Duration::ZERO,
        }
    }
}
#[must_use]
pub fn classify(status: u16, error: Option<&ProviderError>, rate: RateLimit, wall: Wall) -> Failure {
    let kind = match error {
        Some(error) => error.kind.as_ref(),
        None => b"",
    };
    let delay = rate.delay(error, wall);
    if status == 401 || kind == b"authentication_error" || kind == b"invalid_api_key" {
        return Failure::Unauthorized;
    }
    if status == 403 {
        return Failure::Invalid;
    }
    if status == 429 || kind == b"rate_limit_error" || kind == b"rate_limit_exceeded" || kind == b"usage_limit_reached"
    {
        return if rate.exhausted || kind == b"usage_limit_reached" {
            Failure::Exhausted { retry_after: delay }
        } else {
            Failure::RateLimited { retry_after: delay }
        };
    }
    if status == 529 || status == 503 || kind == b"overloaded_error" {
        return Failure::Overloaded;
    }
    if status == 500 || status == 502 || status == 504 || kind == b"api_error" || kind == b"server_error" {
        return Failure::Unavailable;
    }
    let context = kind == b"context_length_exceeded"
        || kind == b"request_too_large"
        || match error {
            Some(error) => bytes::find(&error.message, b"prompt is too long").is_some(),
            None => false,
        };
    if (status == 400 || status == 413 || status == 0) && context {
        return Failure::ContextTooLong;
    }
    match status {
        400 | 404 | 413 | 422 => Failure::Invalid,
        _ => Failure::Unavailable,
    }
}
pub(crate) fn decimal(bytes: &[u8]) -> Option<u64> {
    let mut n: u64 = 0;
    if bytes.is_empty() {
        return None;
    }
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(u64::from(b.wrapping_sub(b'0')))?;
    }
    Some(n)
}
pub(crate) fn append(out: &mut List<u8>, bytes: &[u8]) -> Result<(), DecodeError> {
    if bytes.len() > usize::try_from(out.room()).expect("u32 fits usize") {
        return Err(DecodeError::TooLarge);
    }
    for &b in bytes {
        if out.push(b).is_err() {
            return Err(DecodeError::TooLarge);
        }
    }
    Ok(())
}
pub(crate) fn clipped(bytes: &[u8], limit: u32) -> Box<[u8]> {
    let mut count = bytes.len().min(usize::try_from(limit).expect("u32 fits usize"));
    // Tokenizer strings are UTF-8. A prefix ends before a continuation byte,
    // so truncating diagnostics never creates a new malformed string.
    for _back in 0..4_u32 {
        match bytes.get(count) {
            Some(byte) if byte & 0xc0 == 0x80 => count = count.saturating_sub(1),
            Some(_) | None => break,
        }
    }
    bytes::copy_of(bytes.get(..count).expect("within bytes"))
}

pub(crate) fn measured(encoder: writer::Encoder) -> Result<u32, DecodeError> {
    match encoder.measured() {
        Ok(len) => Ok(len),
        Err(writer::Refusal::TooLong | writer::Refusal::TooDeep) => Err(DecodeError::TooLarge),
        Err(writer::Refusal::Text | writer::Refusal::Number) => Err(DecodeError::Malformed),
    }
}
