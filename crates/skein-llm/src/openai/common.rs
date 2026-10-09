use crate::openai::{Input, Tool};
use alloc::boxed::Box;
use skein_json::{Compact, collector, document, tokenizer, writer};
use skein_lib::{Duration, List, Wall, bytes};

/// Native codec bounds for one endpoint; each dialect receives its own value.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub request: u32,
    pub retained: u32,
    pub strings: u32,
    pub depth: u32,
    pub tokens: u32,
    pub output_items: u32,
    pub tools: u32,
    pub history_items: u32,
    pub metadata: u32,
    pub receiving: u32,
    pub skip: u32,
    pub input: u32,
    pub reasoning: u32,
    pub answer: u32,
    pub detail_bytes: u32,
}
impl Limits {
    /// Neutral admission for a request or standalone native fixture.
    #[must_use]
    pub const fn document(&self) -> crate::DocumentLimits {
        crate::DocumentLimits { bytes: self.skip, strings: self.strings, depth: self.depth, tokens: self.tokens }
    }
    /// Caller input is bounded by the whole request, without a sent-string cap.
    #[must_use]
    pub const fn request_document(&self) -> crate::DocumentLimits {
        crate::DocumentLimits { bytes: self.request, strings: self.request, depth: self.depth, tokens: self.request }
    }
    #[must_use]
    pub const fn metadata_document(&self) -> crate::DocumentLimits {
        crate::DocumentLimits { bytes: self.metadata, strings: self.strings, depth: self.depth, tokens: self.tokens }
    }
    #[must_use]
    pub const fn reasoning_document(&self) -> crate::DocumentLimits {
        crate::DocumentLimits { bytes: self.reasoning, strings: self.strings, depth: self.depth, tokens: self.tokens }
    }
    #[must_use]
    pub const fn writer_limits(&self) -> writer::Limits {
        writer::Limits { depth: self.depth, length: self.skip }
    }
    #[must_use]
    pub const fn tokenizer_limits(&self) -> tokenizer::Limits {
        tokenizer::Limits { depth: self.depth, string: self.strings, number: 32, chunk: 256, length: self.skip }
    }
}
/// A conservative bound for native admission, request encoding and decoded
/// output. The client's selective collector prices streamed events separately.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    // Native temporary values belong to the request, answer or replay
    // budgets. A discarded wire event contributes no document-sized buffer.
    let temporary = limits.request.max(limits.answer).max(limits.input).max(limits.reasoning).max(limits.metadata);
    let tokens = document::worst_case(&document::Limits { tokens: limits.tokens, text: temporary })?;
    // Translation validates the aggregate request before it clones it.
    // Every JSON token costs at least one byte in that request budget.
    // The caller's prompt and its translated copy may coexist until encoding.
    let request_count = u64::from(limits.request);
    let request_tokens = request_count.checked_mul(u64::try_from(size_of::<Compact>()).ok()?)?.checked_mul(2)?;
    let answer_records = u64::from(limits.output_items)
        .checked_mul(u64::from(limits.tokens))?
        .min(u64::from(limits.answer))
        .checked_mul(u64::try_from(size_of::<Compact>()).ok()?)?
        .checked_mul(2)?;
    // Whole-value collection remains a neutral JSON admission entrance for
    // schemas and replay metadata. The client prices its error body separately.
    let admission = collector::worst_case(
        &collector::Limits {
            tokenizer: limits.tokenizer_limits(),
            tokens: limits.tokens,
            text: temporary,
            skip: u64::from(temporary),
        },
        &[],
        &collector::Filter { root: collector::Keep::Value },
    )?;
    let request_slots = List::<Input>::worst_case(limits.history_items)?
        .checked_add(List::<Tool>::worst_case(limits.tools)?)?
        .checked_mul(2)?;
    let documents = u64::from(temporary).checked_mul(8)?;
    let answer = u64::from(limits.answer).checked_mul(4)?;
    tokens
        .checked_mul(4)?
        .checked_add(request_tokens)?
        .checked_add(answer_records)?
        .checked_add(admission)?
        .checked_add(request_slots)?
        .checked_add(documents)?
        .checked_add(answer)?
        .checked_add(u64::from(limits.request))?
        .checked_add(tokenizer::worst_case(&limits.tokenizer_limits())?)?
        .checked_add(writer::worst_case(&writer::Limits { depth: limits.depth, length: temporary })?)?
        .checked_add(crate::openai::response::decoder_worst_case(limits)?)
}
/// A bounded document entrance refused its grammar, shape or receiving limits.
/// These errors can arise during local admission or an active response decode.
/// See `docs/design/llm.md`, Vocabulary and ownership.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum DecodeError {
    /// Tokens, bytes or required framing do not form the admitted grammar.
    Malformed,
    /// A required field is absent from an otherwise bounded document.
    Missing,
    /// A field or root value has a type the entrance cannot accept.
    WrongType,
    /// Input, tokens, nesting or measured output exceed caller-supplied limits.
    TooLarge { which: crate::Cap, bound: u64 },
}
impl DecodeError {
    pub(crate) fn limit(which: crate::Cap, bound: u32) -> DecodeError {
        DecodeError::TooLarge { which, bound: u64::from(bound) }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Stop {
    EndTurn,
    ToolUse,
    MaxTokens,
    Refusal,
}
/// Optional native token reports; absent accounting never becomes a report of zero.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Usage {
    pub input: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
    /// Completion tokens, including reasoning.
    pub output: Option<u64>,
    /// The part of output spent on reasoning, never added to output.
    pub reasoning: Option<u64>,
}
impl Usage {
    /// All counts are unreported until the provider supplies them.
    pub const NONE: Usage = Usage { input: None, cache_read: None, cache_write: None, output: None, reasoning: None };
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Failure {
    /// Provider bytes violate the dialect or end before its terminal event.
    Protocol,
    /// A caller-configured local resource limit was exceeded.
    Limit {
        which: crate::Cap,
        bound: u64,
    },
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
pub(crate) fn append(out: &mut List<u8>, bytes: &[u8], cap: crate::Cap) -> Result<(), DecodeError> {
    if bytes.len() > usize::try_from(out.room()).expect("u32 fits usize") {
        return Err(DecodeError::limit(cap, out.capacity()));
    }
    for &b in bytes {
        if out.push(b).is_err() {
            return Err(DecodeError::limit(cap, out.capacity()));
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

pub(crate) fn measured(encoder: writer::Encoder, limits: writer::Limits, cap: crate::Cap) -> Result<u32, DecodeError> {
    match encoder.measured() {
        Ok(len) => Ok(len),
        Err(writer::Refusal::TooLong) => Err(DecodeError::limit(cap, limits.length)),
        Err(writer::Refusal::TooDeep) => Err(DecodeError::limit(crate::Cap::Depth, limits.depth)),
        Err(writer::Refusal::Text | writer::Refusal::Number) => Err(DecodeError::Malformed),
    }
}

/// The typed cap is the source of a local limit's diagnostic.
pub(crate) fn limit_detail(which: crate::Cap, bound: u64) -> Box<[u8]> {
    let digits = skein_lib::Decimal::of(bound);
    let length = which
        .name()
        .len()
        .checked_add(digits.as_bytes().len())
        .expect("fixed diagnostic prefix fits usize")
        .checked_add(b" exceeds bound ".len())
        .expect("fixed diagnostic fits usize");
    let mut out = skein_lib::Writer::new(length);
    out.put(which.name()).expect("measured cap name");
    out.put(b" exceeds bound ").expect("measured separator");
    out.put(digits.as_bytes()).expect("measured bound");
    out.finish()
}
