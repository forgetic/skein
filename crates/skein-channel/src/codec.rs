//! Frozen common framing and direct final allocation (channel.md §§1,3,7).
//! Typed consumers measure first, then write into `FrameWriter` without a body copy.
use crate::{Opening, Refusal, Schema};
use alloc::boxed::Box;
use skein_lib::{Reader, Writer};

/// Exactly eight-byte big-endian header; reserved u16 must be zero (§1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Header {
    /// Raw wire kind, validated against mechanical phase and schema before body.
    pub kind: u16,
    /// Announced body length, checked before allocation.
    pub body_bytes: u32,
}

/// Decode exactly eight bytes, with no accepted trailing or reserved bits (§1).
#[must_use]
pub fn framing(bytes: &[u8]) -> Option<Header> {
    if bytes.len() != 8 {
        return None;
    }
    let mut reader = Reader::new(bytes);
    let kind = reader.u16()?;
    if reader.u16()? != 0 {
        return None;
    }
    let body_bytes = reader.u32()?;
    Some(Header { kind, body_bytes })
}

/// One sealed exact final frame allocation, private validated metadata (§3).
#[derive(Debug)]
pub struct Encoded {
    pub(crate) kind: u16,
    pub(crate) version: u16,
    pub(crate) body_bytes: u32,
    pub(crate) bytes: Box<[u8]>,
}

impl Encoded {
    /// Exact immutable frame bytes; observation does not confer send credit.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Validated service kind; common encoders remain internal (§3).
    #[must_use]
    pub const fn kind(&self) -> u16 {
        self.kind
    }

    /// Required service version; common frames use frozen version zero (§3).
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }
}

/// A checked measure creates one final allocation; writes never grow it (§3).
#[derive(Debug)]
pub struct FrameWriter {
    kind: u16,
    version: u16,
    body_bytes: u32,
    writer: Writer,
}

impl FrameWriter {
    /// Validate kind/channel/send direction, version, bound and complete frame
    /// fit before allocation. Wrapper must first check its service phase (§3).
    #[must_use]
    pub fn new(schema: &Schema, version: u16, kind: u16, body_bytes: u32) -> Option<FrameWriter> {
        if !schema.flows(kind, false) || body_bytes > schema.rule(version, kind)?.body_bytes {
            return None;
        }
        Self::common(kind, version, body_bytes, schema.limits().queued_bytes)
    }

    pub(crate) fn common(kind: u16, version: u16, body_bytes: u32, cap: u32) -> Option<FrameWriter> {
        let length = body_bytes.checked_add(8)?;
        if length > cap {
            return None;
        }
        let mut writer = Writer::new(usize::try_from(length).ok()?);
        writer.put(&kind.to_be_bytes()).ok()?;
        writer.put(&0_u16.to_be_bytes()).ok()?;
        writer.put(&body_bytes.to_be_bytes()).ok()?;
        Some(FrameWriter { kind, version, body_bytes, writer })
    }

    /// Append measured body bytes directly; a failed write changes nothing.
    pub fn put(&mut self, bytes: &[u8]) -> Option<()> {
        self.writer.put(bytes).ok()
    }

    /// Seal only exact consumption; short encoding is data, never a panic (§3).
    #[must_use]
    pub fn finish(self) -> Option<Encoded> {
        if self.writer.room() != 0 {
            return None;
        }
        Some(Encoded {
            kind: self.kind,
            version: self.version,
            body_bytes: self.body_bytes,
            bytes: self.writer.finish(),
        })
    }
}

pub(crate) fn open(schema: &Schema, opening: &Opening) -> Option<Encoded> {
    let name = u32::try_from(opening.name.len()).ok()?;
    let secret = u32::try_from(opening.secret.len()).ok()?;
    if name > schema.profile().name_bytes || secret > schema.profile().secret_bytes {
        return None;
    }
    let body = 17_u32.checked_add(name)?.checked_add(secret)?;
    let mut writer = FrameWriter::common(1, 0, body, schema.limits().queued_bytes)?;
    writer.put(&schema.profile().magic)?;
    writer.put(&[opening.channel])?;
    writer.put(&opening.lowest.to_be_bytes())?;
    writer.put(&opening.highest.to_be_bytes())?;
    writer.put(&name.to_be_bytes())?;
    writer.put(&opening.name)?;
    writer.put(&secret.to_be_bytes())?;
    writer.put(&opening.secret)?;
    writer.finish()
}

pub(crate) fn short(schema: &Schema, kind: u16, value: u16) -> Option<Encoded> {
    let mut writer = FrameWriter::common(kind, 0, 2, schema.limits().queued_bytes)?;
    writer.put(&value.to_be_bytes())?;
    writer.finish()
}

pub(crate) fn refusal(schema: &Schema, refusal: &Refusal) -> Option<Encoded> {
    let text = u32::try_from(refusal.text.len()).ok()?;
    if text > schema.limits().refuse_bytes {
        return None;
    }
    let mut writer = FrameWriter::common(3, 0, 6_u32.checked_add(text)?, schema.limits().queued_bytes)?;
    writer.put(&refusal.reason.to_be_bytes())?;
    writer.put(&text.to_be_bytes())?;
    writer.put(&refusal.text)?;
    writer.finish()
}

pub(crate) fn terms_length(schema: &Schema, version: u16) -> Option<u32> {
    let mut count = 0_u32;
    for rule in schema.rules() {
        if rule.version == version && schema.flows(rule.kind, true) {
            count = count.checked_add(1)?;
        }
    }
    4_u32.checked_add(count.checked_mul(6)?)
}

pub(crate) fn terms(schema: &Schema, version: u16) -> Option<Encoded> {
    let length = terms_length(schema, version)?;
    let count = length.checked_sub(4)?.checked_div(6)?;
    let mut writer = FrameWriter::common(16, 0, length, schema.limits().queued_bytes)?;
    writer.put(&count.to_be_bytes())?;
    for rule in schema.rules() {
        if rule.version == version && schema.flows(rule.kind, true) {
            writer.put(&rule.kind.to_be_bytes())?;
            writer.put(&rule.body_bytes.to_be_bytes())?;
        }
    }
    writer.finish()
}

pub(crate) fn read_open(schema: &Schema, bytes: &[u8]) -> Option<Opening> {
    let mut reader = Reader::new(bytes);
    if reader.bytes(4)? != schema.profile().magic {
        return None;
    }
    let channel = reader.u8()?;
    let lowest = reader.u16()?;
    let highest = reader.u16()?;
    if channel != schema.profile().channel || lowest > highest {
        return None;
    }
    let name_length = reader.u32()?;
    if name_length > schema.profile().name_bytes {
        return None;
    }
    let name = reader.bytes(name_length)?;
    let secret_length = reader.u32()?;
    if secret_length > schema.profile().secret_bytes {
        return None;
    }
    let secret = reader.bytes(secret_length)?;
    if !reader.is_empty() {
        return None;
    }
    Some(Opening { channel, lowest, highest, name: Box::from(name), secret: Box::from(secret) })
}

pub(crate) fn read_refusal(schema: &Schema, bytes: &[u8]) -> Option<Refusal> {
    let mut reader = Reader::new(bytes);
    let reason = reader.u16()?;
    let length = reader.u32()?;
    if length > schema.limits().refuse_bytes {
        return None;
    }
    let text = reader.bytes(length)?;
    if !reader.is_empty() {
        return None;
    }
    Some(Refusal { reason, text: Box::from(text) })
}

pub(crate) fn read_short(bytes: &[u8]) -> Option<u16> {
    let mut reader = Reader::new(bytes);
    let value = reader.u16()?;
    if !reader.is_empty() {
        return None;
    }
    Some(value)
}

pub(crate) fn check_terms(schema: &Schema, version: u16, bytes: &[u8]) -> Option<()> {
    let mut reader = Reader::new(bytes);
    let count = reader.u32()?;
    if count > schema.limits().terms || count.checked_mul(6)? != reader.remaining() {
        return None;
    }
    // No term array allocation: revisit the already bounded raw body to check
    // uniqueness/coverage while preserving actual source-order advertisement.
    for index in 0..count {
        let kind = reader.u16()?;
        let body = reader.u32()?;
        if !schema.flows(kind, false) || body < schema.rule(version, kind)?.body_bytes {
            return None;
        }
        let mut previous = Reader::new(bytes);
        previous.skip(4)?;
        for _ in 0..index {
            if previous.u16()? == kind {
                return None;
            }
            previous.skip(4)?;
        }
    }
    let mut required = 0_u32;
    for row in schema.rules() {
        if row.version == version && schema.flows(row.kind, false) {
            required = required.checked_add(1)?;
        }
    }
    if required != count {
        return None;
    }
    Some(())
}
