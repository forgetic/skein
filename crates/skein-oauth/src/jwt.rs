//! Payload metadata only: this decoder does not authenticate JWT signatures.
//! The owner obtains tokens from its authenticated TLS token endpoint; claims
//! supply caller-selected metadata, never a new authentication decision.
use crate::{ClaimSelector, DecodeError, Json, Limits, common, json};
use alloc::boxed::Box;
use skein_lib::{Duration, Wall, Writer, bytes};

#[derive(Clone, PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub struct Claims {
    pub metadata: Box<[u8]>,
    pub expires_at: Option<Wall>,
    pub valid: Option<Duration>,
}
/// Reads caller-selected metadata once, at refresh, against an injected wall clock.
/// `valid` is only metadata; the refresh's `expires_in` remains an independent cap.
pub fn read_claims(token: &[u8], selector: &ClaimSelector, wall: Wall, limits: &Limits) -> Result<Claims, DecodeError> {
    common::bearer(token, limits)?;
    common::bounded(&selector.object, limits.string_bytes)?;
    common::bounded(&selector.field, limits.string_bytes)?;
    let first = bytes::find(token, b".").ok_or(DecodeError::Malformed)?;
    let header = token.get(..first).ok_or(DecodeError::Malformed)?;
    let rest = token.get(first.checked_add(1).ok_or(DecodeError::TooLarge)?..).ok_or(DecodeError::Malformed)?;
    let second = bytes::find(rest, b".").ok_or(DecodeError::Malformed)?;
    let payload = rest.get(..second).ok_or(DecodeError::Malformed)?;
    let signature = rest.get(second.checked_add(1).ok_or(DecodeError::TooLarge)?..).ok_or(DecodeError::Malformed)?;
    let _header_len = decoded_len(header)?;
    let _signature_len = decoded_len(signature)?;
    let decoded = decode(payload, limits.document_bytes)?;
    let value = Json::from_bytes(&decoded, limits)?;
    let tokens = value.as_tokens();
    let auth = json::value_at(tokens, json::required(tokens, &selector.object)?)?;
    let id = json::text_ref(json::value_at(auth, json::required(auth, &selector.field)?)?)?;
    check_metadata(id, limits)?;
    let expires_at = match json::optional_at(tokens, json::field(tokens, b"exp")?)? {
        Some(value) => {
            let seconds = json::unsigned(value)?;
            Some(Wall::from_nanos(seconds.checked_mul(1_000_000_000).ok_or(DecodeError::TooLarge)?))
        }
        None => None,
    };
    let mut valid = None;
    if let Some(expires_at) = expires_at {
        valid = Some(Duration::from_nanos(expires_at.as_nanos().saturating_sub(wall.as_nanos())));
    }
    Ok(Claims { metadata: bytes::copy_of(id), expires_at, valid })
}
pub(crate) fn check_metadata(id: &[u8], limits: &Limits) -> Result<(), DecodeError> {
    common::bounded(id, limits.token_bytes)?;
    for &byte in id {
        if !(0x21..=0x7e).contains(&byte) {
            return Err(DecodeError::Malformed);
        }
    }
    Ok(())
}
fn sextet(byte: u8) -> Result<u8, DecodeError> {
    match byte {
        b'A'..=b'Z' => Ok(byte.wrapping_sub(b'A')),
        b'a'..=b'z' => Ok(byte.wrapping_sub(b'a').wrapping_add(26)),
        b'0'..=b'9' => Ok(byte.wrapping_sub(b'0').wrapping_add(52)),
        b'-' => Ok(62),
        b'_' => Ok(63),
        _ => Err(DecodeError::Malformed),
    }
}
fn decoded_len(input: &[u8]) -> Result<usize, DecodeError> {
    if input.is_empty() {
        return Err(DecodeError::Malformed);
    }
    for &byte in input {
        let _value = sextet(byte)?;
    }
    let remainder = input.len().rem_euclid(4);
    let extra: usize = match remainder {
        0 => 0,
        2 => {
            if sextet(*input.last().ok_or(DecodeError::Malformed)?)? & 0x0f != 0 {
                return Err(DecodeError::Malformed);
            }
            1
        }
        3 => {
            if sextet(*input.last().ok_or(DecodeError::Malformed)?)? & 0x03 != 0 {
                return Err(DecodeError::Malformed);
            }
            2
        }
        _ => return Err(DecodeError::Malformed),
    };
    input
        .len()
        .div_euclid(4)
        .checked_mul(3)
        .ok_or(DecodeError::TooLarge)?
        .checked_add(extra)
        .ok_or(DecodeError::TooLarge)
}
fn decode(input: &[u8], cap: u32) -> Result<Box<[u8]>, DecodeError> {
    let len = decoded_len(input)?;
    if len > usize::try_from(cap).expect("u32 fits usize") {
        return Err(DecodeError::TooLarge);
    }
    let mut out = Writer::new(len);
    for group in input.chunks(4) {
        let decoded: [u8; 3];
        let count: usize;
        match group {
            [a, b, c, d] => {
                let a = sextet(*a)?;
                let b = sextet(*b)?;
                let c = sextet(*c)?;
                let d = sextet(*d)?;
                decoded = [(a << 2_u32) | (b >> 4_u32), (b << 4_u32) | (c >> 2_u32), (c << 6_u32) | d];
                count = 3;
            }
            [a, b, c] => {
                let a = sextet(*a)?;
                let b = sextet(*b)?;
                let c = sextet(*c)?;
                decoded = [(a << 2_u32) | (b >> 4_u32), (b << 4_u32) | (c >> 2_u32), 0];
                count = 2;
            }
            [a, b] => {
                let a = sextet(*a)?;
                let b = sextet(*b)?;
                decoded = [(a << 2_u32) | (b >> 4_u32), 0, 0];
                count = 1;
            }
            _ => return Err(DecodeError::Malformed),
        }
        out.put(decoded.get(..count).ok_or(DecodeError::Malformed)?).expect("base64 length measured exactly");
    }
    Ok(out.finish())
}
