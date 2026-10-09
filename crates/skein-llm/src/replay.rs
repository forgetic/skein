//! Bounded durable envelopes for provider-owned replay metadata.
//! Applications keep these bytes without knowing a provider's JSON fields.

use crate::{DocumentLimits, Error, Json, Provider, Replay};
use alloc::boxed::Box;
use skein_lib::{Reader, Writer};

const VERSION: u16 = 1;
/// Fixed version, provider tag and length overhead, beyond the raw opaque cap.
/// Receivers reserve `replay_bytes` so every admitted raw replay fits its envelope.
pub const REPLAY_HEADER_BYTES: u32 = 7;

/// Maximum complete durable envelope for the supplied raw metadata allowance.
/// Checked overflow refuses configuration rather than reducing replay storage.
#[must_use]
pub const fn replay_bytes(limits: &DocumentLimits) -> Option<u32> {
    limits.bytes.checked_add(REPLAY_HEADER_BYTES)
}

/// Transit heap for encoding/decoding, including simultaneous raw/output bytes
/// and bounded JSON machines/token storage. Caller-owned input storage is separate.
#[must_use]
pub fn replay_worst_case(limits: &DocumentLimits) -> Option<u64> {
    crate::document::worst_case(limits)?
        .checked_add(u64::from(limits.bytes).checked_mul(2)?)?
        .checked_add(u64::from(REPLAY_HEADER_BYTES))
}

impl Replay {
    /// Encodes complete metadata under the raw opaque cap plus the exported header.
    /// Unknown provider-owned fields stay in the envelope. Its interpretation
    /// and compatibility are checked again by the receiving Client.
    pub fn to_bytes(&self, limits: &DocumentLimits) -> Result<Box<[u8]>, Error> {
        let value = match Json::from_view(self.value.view(), limits) {
            Ok(value) => value,
            Err(error) => return Err(crate::translate::decode(error)),
        };
        let data = match value.to_bytes(limits) {
            Ok(data) => data,
            Err(error) => return Err(crate::translate::decode(error)),
        };
        let length = u32::try_from(data.len()).or(Err(Error::limit(crate::Cap::Retained, limits.bytes)))?;
        let total = length.checked_add(REPLAY_HEADER_BYTES).ok_or(Error::limit(crate::Cap::Retained, limits.bytes))?;
        if length > limits.bytes {
            return Err(Error::limit(crate::Cap::Retained, limits.bytes));
        }
        let mut out = Writer::new(usize::try_from(total).expect("u32 fits usize"));
        out.put(&VERSION.to_be_bytes()).expect("measured replay header");
        let provider = match self.provider {
            Provider::OpenAiCodex => 1_u8,
            Provider::Anthropic => 2_u8,
        };
        out.put(&[provider]).expect("measured provider tag");
        out.put(&length.to_be_bytes()).expect("measured replay length");
        out.put(&data).expect("measured replay payload");
        Ok(out.finish())
    }

    /// Admits one complete bounded replay envelope. No trailing bytes, unknown
    /// version/tag or malformed JSON is accepted; Client checks block/dialect
    /// compatibility when the replay is used in a prompt.
    pub fn from_bytes(data: &[u8], limits: &DocumentLimits) -> Result<Replay, Error> {
        let maximum = replay_bytes(limits).ok_or(Error::limit(crate::Cap::Retained, limits.bytes))?;
        if data.len() > usize::try_from(maximum).expect("u32 fits usize") {
            return Err(Error::limit(crate::Cap::Retained, limits.bytes));
        }
        let mut input = Reader::new(data);
        if input.u16() != Some(VERSION) {
            return Err(Error::Unsupported);
        }
        let provider = match input.u8() {
            Some(1) => Provider::OpenAiCodex,
            Some(2) => Provider::Anthropic,
            Some(_) | None => return Err(Error::Unsupported),
        };
        let length = input.u32().ok_or(Error::Invalid)?;
        if length > limits.bytes {
            return Err(Error::limit(crate::Cap::Retained, limits.bytes));
        }
        let data = input.bytes(length).ok_or(Error::Invalid)?;
        if input.remaining() != 0 {
            return Err(Error::Invalid);
        }
        let value = match Json::from_bytes(data, limits) {
            Ok(value) => value,
            Err(error) => return Err(crate::translate::decode(error)),
        };
        Ok(Replay { provider, value })
    }
}
