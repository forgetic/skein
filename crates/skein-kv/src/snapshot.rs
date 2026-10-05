//! Checksummed snapshot format. Chunks are independently validated; a
//! complete trailer is required before any snapshot is accepted.

#![expect(clippy::disallowed_types, reason = "snapshot chunks and recovery are bounded by configured limits")]

use crate::Row;
use crate::crc::crc32c;
use crate::frame::{u16_at, u32_at, u64_at};
use alloc::boxed::Box;
use alloc::vec::Vec;

const MAGIC: &[u8; 4] = b"SKVS";
const END: &[u8; 4] = b"END!";
const TRAILER: &[u8; 4] = b"SNPE";
const VERSION: u16 = 1;
pub(crate) const HEADER: usize = 18;
pub(crate) const TRAILER_LEN: usize = 28;

#[must_use]
pub(crate) fn header(start: u64) -> Box<[u8]> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&start.to_be_bytes());
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    bytes.into_boxed_slice()
}

pub(crate) fn chunk(rows: &[Row], limit: u32) -> Option<Box<[u8]>> {
    if rows.is_empty() {
        return None;
    }
    let mut body = Vec::new();
    for row in rows {
        body.extend_from_slice(&u32::try_from(row.key.len()).ok()?.to_be_bytes());
        body.extend_from_slice(&u32::try_from(row.value.len()).ok()?.to_be_bytes());
        body.extend_from_slice(&row.key);
        body.extend_from_slice(&row.value);
    }
    let total = body.len().checked_add(12)?;
    if total > usize::try_from(limit).ok()? {
        return None;
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&u32::try_from(rows.len()).ok()?.to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(body.len()).ok()?.to_be_bytes());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    Some(bytes.into_boxed_slice())
}

#[must_use]
pub(crate) fn trailer(rows: u64, bytes: u64) -> Box<[u8]> {
    let mut trailer = Vec::new();
    trailer.extend_from_slice(TRAILER);
    trailer.extend_from_slice(&rows.to_be_bytes());
    trailer.extend_from_slice(&bytes.to_be_bytes());
    trailer.extend_from_slice(&crc32c(&trailer).to_be_bytes());
    trailer.extend_from_slice(END);
    trailer.into_boxed_slice()
}

pub(crate) fn decode(bytes: &[u8], chunk_limit: u32) -> Option<(u64, Box<[Row]>)> {
    if bytes.len() < HEADER.checked_add(TRAILER_LEN)?
        || bytes.get(..4)? != MAGIC
        || u16_at(bytes, 4)? != VERSION
        || u32_at(bytes, 14)? != crc32c(bytes.get(..14)?)
    {
        return None;
    }
    let start = u64_at(bytes, 6)?;
    if start == 0 {
        return None;
    }
    let mut at = HEADER;
    let mut rows: Vec<Row> = Vec::new();
    let mut payload = 0_u64;
    let mut previous: Option<Box<[u8]>> = None;
    loop {
        if bytes.get(at..at.checked_add(4)?)? == TRAILER {
            let end = at.checked_add(TRAILER_LEN)?;
            if end != bytes.len() || bytes.get(end.checked_sub(4)?..end)? != END {
                return None;
            }
            if u32_at(bytes, at.checked_add(20)?)? != crc32c(bytes.get(at..at.checked_add(20)?)?) {
                return None;
            }
            if u64_at(bytes, at.checked_add(4)?)? != u64::try_from(rows.len()).ok()?
                || u64_at(bytes, at.checked_add(12)?)? != payload
            {
                return None;
            }
            return Some((start, rows.into_boxed_slice()));
        }
        let count = usize::try_from(u32_at(bytes, at)?).ok()?;
        let len = usize::try_from(u32_at(bytes, at.checked_add(4)?)?).ok()?;
        let total = len.checked_add(12)?;
        if count == 0 || total > usize::try_from(chunk_limit).ok()? {
            return None;
        }
        let end = at.checked_add(total)?;
        if end > bytes.len() {
            return None;
        }
        let crc_at = end.checked_sub(4)?;
        if u32_at(bytes, crc_at)? != crc32c(bytes.get(at..crc_at)?) {
            return None;
        }
        let mut cursor = at.checked_add(8)?;
        for _ in 0..count {
            let key_len = usize::try_from(u32_at(bytes, cursor)?).ok()?;
            cursor = cursor.checked_add(4)?;
            let value_len = usize::try_from(u32_at(bytes, cursor)?).ok()?;
            cursor = cursor.checked_add(4)?;
            let key_end = cursor.checked_add(key_len)?;
            let key: Box<[u8]> = Box::from(bytes.get(cursor..key_end)?);
            cursor = key_end;
            let value_end = cursor.checked_add(value_len)?;
            let value = Box::from(bytes.get(cursor..value_end)?);
            cursor = value_end;
            if let Some(old) = previous.as_ref()
                && old.as_ref() >= key.as_ref()
            {
                return None;
            }
            previous = Some(key.clone());
            payload = payload.checked_add(u64::try_from(key_len.checked_add(value_len)?).ok()?)?;
            rows.push(Row { key, value });
        }
        if cursor != crc_at {
            return None;
        }
        at = end;
    }
}
