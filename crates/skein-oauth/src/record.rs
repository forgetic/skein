use crate::{DecodeError, Limits, TokenResponse, common, documents, jwt};
use alloc::boxed::Box;
use skein_lib::{Duration, Reader, Wall, Writer, bytes};

pub const RECORD_VERSION: u16 = 1;
const MAGIC: &[u8] = b"SKOT";
const FIXED_BYTES: u32 = 38;
/// Caller-selected nested JWT claim to keep as opaque metadata.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ClaimSelector {
    pub object: Box<[u8]>,
    pub field: Box<[u8]>,
}
/// Metadata needed for Starting or the next refresh; access tokens are optional
/// in the owner's table and are not required to ask for the first refresh.
#[derive(Clone, PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub struct RefreshState {
    pub key: u32,
    pub generation: u64,
    pub refresh_token: Box<[u8]>,
}
/// Kept as one versioned record before `Refreshed` crosses a domain boundary.
/// `expires_at` is restart metadata, never a cross-host grant deadline.
#[derive(Clone, PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub struct SavedToken {
    pub key: u32,
    pub generation: u64,
    pub access_token: Box<[u8]>,
    pub refresh_token: Box<[u8]>,
    pub metadata: Option<Box<[u8]>>,
    pub expires_at: Wall,
}
impl SavedToken {
    #[must_use]
    pub fn refresh_state(&self) -> RefreshState {
        RefreshState { key: self.key, generation: self.generation, refresh_token: self.refresh_token.clone() }
    }
    /// Call at grant encoding or durability completion, not at refresh start.
    #[must_use]
    pub fn remaining(&self, wall: Wall) -> Duration {
        Duration::from_nanos(self.expires_at.as_nanos().saturating_sub(wall.as_nanos()))
    }
}
/// Builds the next candidate; its owner must keep it successfully before
/// granting it. An omitted `refresh_token` preserves the previous token.
pub fn rotate(
    previous: &RefreshState,
    response: &TokenResponse,
    claim: Option<&ClaimSelector>,
    wall: Wall,
    limits: &Limits,
) -> Result<SavedToken, DecodeError> {
    common::bounded(&previous.refresh_token, limits.token_bytes)?;
    common::text(&previous.refresh_token)?;
    documents::validate_response(response, limits)?;
    let generation = previous.generation.checked_add(1).ok_or(DecodeError::TooLarge)?;
    let nanos = response.expires_in.checked_mul(1_000_000_000).ok_or(DecodeError::TooLarge)?;
    let mut expires_at = Wall::from_nanos(wall.as_nanos().checked_add(nanos).ok_or(DecodeError::TooLarge)?);
    let metadata = match claim {
        None => None,
        Some(selector) => {
            let claims = jwt::read_claims(&response.access_token, selector, wall, limits)?;
            if let Some(expiry) = claims.expires_at {
                expires_at = expires_at.min(expiry);
            }
            Some(claims.metadata)
        }
    };
    let refresh_token = match &response.refresh_token {
        Some(refresh) => refresh.clone(),
        None => previous.refresh_token.clone(),
    };
    let candidate = SavedToken {
        key: previous.key,
        generation,
        access_token: response.access_token.clone(),
        refresh_token,
        metadata,
        expires_at,
    };
    validate(&candidate, limits)?;
    Ok(candidate)
}
fn validate(record: &SavedToken, limits: &Limits) -> Result<(), DecodeError> {
    common::bearer(&record.access_token, limits)?;
    common::bounded(&record.refresh_token, limits.token_bytes)?;
    common::text(&record.refresh_token)?;
    if let Some(id) = &record.metadata {
        jwt::check_metadata(id, limits)?;
    }
    let _length = measured(record, limits)?;
    Ok(())
}
fn measured(record: &SavedToken, limits: &Limits) -> Result<u32, DecodeError> {
    let id_len = match &record.metadata {
        Some(id) => u32::try_from(id.len()).or(Err(DecodeError::TooLarge))?,
        None => 0,
    };
    let access_len = u32::try_from(record.access_token.len()).or(Err(DecodeError::TooLarge))?;
    let refresh_len = u32::try_from(record.refresh_token.len()).or(Err(DecodeError::TooLarge))?;
    let len = FIXED_BYTES
        .checked_add(access_len)
        .ok_or(DecodeError::TooLarge)?
        .checked_add(refresh_len)
        .ok_or(DecodeError::TooLarge)?
        .checked_add(id_len)
        .ok_or(DecodeError::TooLarge)?;
    if len > limits.record_bytes {
        return Err(DecodeError::TooLarge);
    }
    Ok(len)
}
pub fn encode_record(record: &SavedToken, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    validate(record, limits)?;
    let len = measured(record, limits)?;
    let mut out = Writer::new(usize::try_from(len).expect("u32 fits usize"));
    out.put(MAGIC).expect("record measured");
    out.put(&RECORD_VERSION.to_be_bytes()).expect("record measured");
    out.put(&record.key.to_be_bytes()).expect("record measured");
    out.put(&record.generation.to_be_bytes()).expect("record measured");
    out.put(&record.expires_at.as_nanos().to_be_bytes()).expect("record measured");
    write_field(&mut out, &record.access_token);
    write_field(&mut out, &record.refresh_token);
    match &record.metadata {
        Some(id) => write_field(&mut out, id),
        None => write_field(&mut out, b""),
    }
    Ok(out.finish())
}
fn write_field(out: &mut Writer, value: &[u8]) {
    out.put(&u32::try_from(value.len()).expect("measured field fits u32").to_be_bytes()).expect("record measured");
    out.put(value).expect("record measured");
}
pub fn decode_record(input: &[u8], limits: &Limits) -> Result<SavedToken, DecodeError> {
    if input.len() > usize::try_from(limits.record_bytes).expect("u32 fits usize") {
        return Err(DecodeError::TooLarge);
    }
    let mut reader = Reader::new(input);
    if reader.bytes(4).ok_or(DecodeError::Malformed)? != MAGIC {
        return Err(DecodeError::Malformed);
    }
    if reader.u16().ok_or(DecodeError::Malformed)? != RECORD_VERSION {
        return Err(DecodeError::Version);
    }
    let key = reader.u32().ok_or(DecodeError::Malformed)?;
    let generation = reader.u64().ok_or(DecodeError::Malformed)?;
    let expires_at = Wall::from_nanos(reader.u64().ok_or(DecodeError::Malformed)?);
    let access_token = read_field(&mut reader, limits.token_bytes)?;
    let refresh_token = read_field(&mut reader, limits.token_bytes)?;
    let id = read_field(&mut reader, limits.token_bytes)?;
    let metadata = if id.is_empty() { None } else { Some(id) };
    if !reader.is_empty() {
        return Err(DecodeError::Malformed);
    }
    let record = SavedToken { key, generation, access_token, refresh_token, metadata, expires_at };
    validate(&record, limits)?;
    Ok(record)
}
fn read_field(reader: &mut Reader<'_>, cap: u32) -> Result<Box<[u8]>, DecodeError> {
    let len = reader.u32().ok_or(DecodeError::Malformed)?;
    if len > cap {
        return Err(DecodeError::TooLarge);
    }
    let value = reader.bytes(len).ok_or(DecodeError::Malformed)?;
    Ok(bytes::copy_of(value))
}
