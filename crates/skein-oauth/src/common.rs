use alloc::boxed::Box;
use skein_json::{Token, tokenizer, writer};
use skein_lib::{Duration, List, bytes};

/// Bounds for one token exchange and its saved record (oauth.md §5).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub document_bytes: u32,
    pub string_bytes: u32,
    pub token_bytes: u32,
    pub client_bytes: u32,
    pub detail_bytes: u32,
    pub record_bytes: u32,
    pub depth: u32,
    pub tokens: u32,
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
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum DecodeError {
    Malformed,
    Missing,
    WrongType,
    TooLarge,
    Version,
}
/// Exchange failures, without secret-bearing transport details.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Failure {
    Unavailable,
    TimedOut,
    RateLimited { retry_after: Duration },
    Refused,
    ClientRejected,
    Malformed,
    InvalidRedirect,
    Limit,
    Cancelled,
}
/// Classifies an answered refresh. Server failures are ambiguous after sending;
/// only a transport that proves nothing was sent may use `Unavailable`.
#[must_use]
pub fn classify(status: u16, error: Option<&crate::OAuthError>, retry_after: Duration) -> Failure {
    let code = match error {
        Some(error) => error.code.as_ref(),
        None => b"",
    };
    if status == 429 || code == b"rate_limit_exceeded" || code == b"slow_down" {
        return Failure::RateLimited { retry_after };
    }
    if code == b"invalid_grant" {
        return Failure::Refused;
    }
    if code == b"invalid_client" || code == b"unauthorized_client" {
        return Failure::ClientRejected;
    }
    if status >= 500 || code == b"server_error" || code == b"temporarily_unavailable" {
        return Failure::TimedOut;
    }
    match status {
        401 | 403 => Failure::ClientRejected,
        400 | 404 | 405 | 422 => Failure::Malformed,
        _ => Failure::TimedOut,
    }
}
/// Includes an exchange, tokenized JWT payload, two saved generations and a
/// candidate/record being handed to the durability owner. Checked on startup.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    List::<Token>::worst_case(limits.tokens)?
        .checked_mul(3)?
        .checked_add(u64::from(limits.document_bytes).checked_mul(6)?)?
        .checked_add(u64::from(limits.token_bytes).checked_mul(12)?)?
        .checked_add(u64::from(limits.client_bytes))?
        .checked_add(u64::from(limits.detail_bytes).checked_mul(2)?)?
        .checked_add(u64::from(limits.record_bytes).checked_mul(2)?)?
        .checked_add(crate::json::validation_worst_case(limits)?)?
        .checked_add(tokenizer::worst_case(&limits.tokenizer_limits())?)?
        .checked_add(writer::worst_case(&limits.writer_limits())?)
}
pub(crate) fn measured(encoder: writer::Encoder) -> Result<u32, DecodeError> {
    match encoder.measured() {
        Ok(len) => Ok(len),
        Err(writer::Refusal::TooLong | writer::Refusal::TooDeep) => Err(DecodeError::TooLarge),
        Err(writer::Refusal::Text | writer::Refusal::Number) => Err(DecodeError::Malformed),
    }
}
pub(crate) fn bounded(input: &[u8], cap: u32) -> Result<(), DecodeError> {
    if input.len() > usize::try_from(cap).expect("u32 fits usize") {
        return Err(DecodeError::TooLarge);
    }
    if input.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(())
}
pub(crate) fn bearer(input: &[u8], limits: &Limits) -> Result<(), DecodeError> {
    bounded(input, limits.token_bytes)?;
    let mut padding = false;
    let mut body = false;
    for &byte in input {
        if byte == b'=' {
            if !body {
                return Err(DecodeError::Malformed);
            }
            padding = true;
        } else if padding || !(byte.is_ascii_alphanumeric() || b"-._~+/".contains(&byte)) {
            return Err(DecodeError::Malformed);
        } else {
            body = true;
        }
    }
    Ok(())
}
pub(crate) fn text(input: &[u8]) -> Result<(), DecodeError> {
    let limits = writer::Limits { depth: 1, length: u32::MAX };
    let mut measure = writer::Encoder::measure(&limits);
    measure.string(input);
    let _length = measured(measure)?;
    Ok(())
}
pub(crate) fn clipped(input: &[u8], cap: u32) -> Box<[u8]> {
    let mut count = input.len().min(usize::try_from(cap).expect("u32 fits usize"));
    for _back in 0..4_u32 {
        match input.get(count) {
            Some(byte) if byte & 0xc0 == 0x80 => count = count.saturating_sub(1),
            Some(_) | None => break,
        }
    }
    bytes::copy_of(input.get(..count).expect("within input"))
}
