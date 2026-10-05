//! Versioned, checksummed commit frames.

#![expect(clippy::disallowed_types, reason = "encoded frames are bounded by the commit limit")]

use crate::Op;
use crate::crc::crc32c;
use alloc::boxed::Box;
use alloc::vec::Vec;

const MAGIC: &[u8; 4] = b"SKVC";
const VERSION: u16 = 1;
pub(crate) const HEADER: usize = 22;

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Frame {
    pub(crate) number: u64,
    pub(crate) ops: Box<[Op]>,
}

pub(crate) fn encoded_len(ops: &[Op]) -> Option<usize> {
    let mut total = HEADER.checked_add(4)?;
    for op in ops {
        total = total.checked_add(1)?.checked_add(4)?.checked_add(op.key().len())?;
        if let Op::Put { value, .. } = op {
            total = total.checked_add(4)?.checked_add(value.len())?;
        }
    }
    Some(total)
}

pub(crate) fn encode(number: u64, ops: &[Op], limit: u32) -> Option<Box<[u8]>> {
    let total = encoded_len(ops)?;
    if total > usize::try_from(limit).ok()? {
        return None;
    }
    let count = u32::try_from(ops.len()).ok()?;
    let capacity = total.checked_sub(HEADER)?.checked_sub(4)?;
    let mut body = Vec::with_capacity(capacity);
    for op in ops {
        match op {
            Op::Put { key, value } => {
                body.push(1);
                body.extend_from_slice(&u32::try_from(key.len()).ok()?.to_be_bytes());
                body.extend_from_slice(key);
                body.extend_from_slice(&u32::try_from(value.len()).ok()?.to_be_bytes());
                body.extend_from_slice(value);
            }
            Op::Erase { key } => {
                body.push(2);
                body.extend_from_slice(&u32::try_from(key.len()).ok()?.to_be_bytes());
                body.extend_from_slice(key);
            }
        }
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&number.to_be_bytes());
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(body.len()).ok()?.to_be_bytes());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    Some(bytes.into_boxed_slice())
}

#[must_use]
pub(crate) fn frame_len(head: &[u8], max: u32) -> Option<usize> {
    if head.len() < HEADER || head.get(..4)? != MAGIC || u16_at(head, 4)? != VERSION {
        return None;
    }
    let body = usize::try_from(u32_at(head, 18)?).ok()?;
    let total = HEADER.checked_add(body)?.checked_add(4)?;
    if total > usize::try_from(max).ok()? {
        return None;
    }
    Some(total)
}

pub(crate) fn decode(bytes: &[u8], max: u32) -> Option<Frame> {
    let total = frame_len(bytes, max)?;
    if bytes.len() != total {
        return None;
    }
    let crc_at = total.checked_sub(4)?;
    if u32_at(bytes, crc_at)? != crc32c(bytes.get(..crc_at)?) {
        return None;
    }
    let number = u64_at(bytes, 6)?;
    let count = usize::try_from(u32_at(bytes, 14)?).ok()?;
    let mut ops = Vec::new();
    let mut pos = HEADER;
    while pos < crc_at {
        if ops.len() >= count {
            return None;
        }
        let kind = *bytes.get(pos)?;
        pos = pos.checked_add(1)?;
        let key_len = usize::try_from(u32_at(bytes, pos)?).ok()?;
        pos = pos.checked_add(4)?;
        let key_end = pos.checked_add(key_len)?;
        let key = Box::from(bytes.get(pos..key_end)?);
        pos = key_end;
        match kind {
            1 => {
                let value_len = usize::try_from(u32_at(bytes, pos)?).ok()?;
                pos = pos.checked_add(4)?;
                let value_end = pos.checked_add(value_len)?;
                let value = Box::from(bytes.get(pos..value_end)?);
                pos = value_end;
                ops.push(Op::Put { key, value });
            }
            2 => ops.push(Op::Erase { key }),
            _ => return None,
        }
    }
    if pos != crc_at || ops.len() != count {
        return None;
    }
    Some(Frame { number, ops: ops.into_boxed_slice() })
}

pub(crate) fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    let end = at.checked_add(2)?;
    Some(u16::from_be_bytes(bytes.get(at..end)?.try_into().ok()?))
}

pub(crate) fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    Some(u32::from_be_bytes(bytes.get(at..end)?.try_into().ok()?))
}

pub(crate) fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    let end = at.checked_add(8)?;
    Some(u64::from_be_bytes(bytes.get(at..end)?.try_into().ok()?))
}
