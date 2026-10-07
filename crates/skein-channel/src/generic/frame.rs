//! Eight-byte framing and the six channel messages (channel.md, sections 3
//! and 5.1). A writer owns one measured allocation. This module never
//! interprets application bodies or credentials; `parse_header` and
//! `decode_control` check bytes before allocating variable fields.
use alloc::boxed::Box;
use skein_lib::{List, Reader, Writer};

use super::Limits;

/// A checked eight-byte header with zero flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub kind: u16,
    pub body_len: u32,
}

/// Why a frame could not be measured, encoded or decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// A length exceeds the representable frame or a configured bound.
    TooLarge,
    /// The given body did not match its frozen layout.
    Malformed,
    /// The supplied kind has no channel message layout.
    UnknownKind,
    /// The measured writer was not filled exactly.
    Incomplete,
}

/// An encoded frame whose header and measured body occupy one box.
#[derive(Debug)]
pub struct Frame {
    kind: u16,
    body_len: u32,
    bytes: Box<[u8]>,
}

impl Frame {
    /// The wire bytes, including the header.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The validated kind in the header.
    #[must_use]
    pub const fn kind(&self) -> u16 {
        self.kind
    }

    /// The measured body length, excluding the header.
    #[must_use]
    pub const fn body_len(&self) -> u32 {
        self.body_len
    }

    /// The whole frame length, including the header.
    #[must_use]
    pub fn wire_len(&self) -> u32 {
        self.body_len.checked_add(8).expect("the frame length was checked before allocation")
    }
}

/// A writer that appends a measured body after an already written header.
#[derive(Debug)]
pub struct FrameWriter {
    kind: u16,
    body_len: u32,
    writer: Writer,
}

/// Makes one exact frame allocation after checking its total length.
pub fn frame_writer(kind: u16, body_len: u32) -> Result<FrameWriter, FrameError> {
    let length = body_len.checked_add(8).ok_or(FrameError::TooLarge)?;
    let length = usize::try_from(length).ok().ok_or(FrameError::TooLarge)?;
    isize::try_from(length).ok().ok_or(FrameError::TooLarge)?;
    let mut writer = Writer::new(length);
    writer.put(&kind.to_be_bytes()).ok().ok_or(FrameError::TooLarge)?;
    writer.put(&0_u16.to_be_bytes()).ok().ok_or(FrameError::TooLarge)?;
    writer.put(&body_len.to_be_bytes()).ok().ok_or(FrameError::TooLarge)?;
    Ok(FrameWriter { kind, body_len, writer })
}

impl FrameWriter {
    /// Appends bytes only if all of them fit the measured body.
    pub fn put(&mut self, bytes: &[u8]) -> Result<(), FrameError> {
        self.writer.put(bytes).ok().ok_or(FrameError::TooLarge)
    }

    /// Returns the frame only when exactly its measured length was written.
    pub fn finish(self) -> Result<Frame, FrameError> {
        if self.writer.room() != 0 {
            return Err(FrameError::Incomplete);
        }
        Ok(Frame { kind: self.kind, body_len: self.body_len, bytes: self.writer.finish() })
    }
}

/// Parses exactly one frozen header, rejecting nonzero flags.
pub fn parse_header(bytes: &[u8]) -> Result<Header, FrameError> {
    if bytes.len() != 8 {
        return Err(FrameError::Malformed);
    }
    let mut reader = Reader::new(bytes);
    let kind = reader.u16().ok_or(FrameError::Malformed)?;
    let flags = reader.u16().ok_or(FrameError::Malformed)?;
    let body_len = reader.u32().ok_or(FrameError::Malformed)?;
    if flags != 0 {
        return Err(FrameError::Malformed);
    }
    Ok(Header { kind, body_len })
}

/// One receive term: a kind and the largest body the peer takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Term {
    pub kind: u16,
    pub largest: u32,
}

/// One of the six fixed channel messages, sent by either peer as allowed by phase.
#[derive(Debug)]
pub enum Control {
    /// The initiator's protocol magic, version offer and opaque credential.
    Open { magic: [u8; 4], lowest: u16, highest: u16, features: u16, credential: Box<[u8]> },
    /// The responder's selected version and features.
    Accept { version: u16, features: u16 },
    /// A terminal reason and bounded operator text.
    Refuse { reason: u16, text: Box<[u8]> },
    /// The sender's bounded receive terms.
    Terms { entries: List<Term> },
    /// A frame used for liveness after Open.
    Ping,
    /// A kind the sender skipped after its terms.
    Unsupported { kind: u16 },
}

/// Encodes a fixed message, checking variable lengths before allocation.
pub fn control_frame(control: &Control, limits: &Limits) -> Result<Frame, FrameError> {
    match control {
        Control::Open { magic, lowest, highest, features, credential } => {
            let credential_len = u32::try_from(credential.len()).ok().ok_or(FrameError::TooLarge)?;
            if credential_len > limits.credential {
                return Err(FrameError::TooLarge);
            }
            let body_len = 14_u32.checked_add(credential_len).ok_or(FrameError::TooLarge)?;
            let mut writer = frame_writer(1, body_len)?;
            writer.put(magic)?;
            writer.put(&lowest.to_be_bytes())?;
            writer.put(&highest.to_be_bytes())?;
            writer.put(&features.to_be_bytes())?;
            writer.put(&credential_len.to_be_bytes())?;
            writer.put(credential)?;
            writer.finish()
        }
        Control::Accept { version, features } => {
            let mut writer = frame_writer(2, 4)?;
            writer.put(&version.to_be_bytes())?;
            writer.put(&features.to_be_bytes())?;
            writer.finish()
        }
        Control::Refuse { reason, text } => {
            let text_len = u32::try_from(text.len()).ok().ok_or(FrameError::TooLarge)?;
            if text_len > 256 {
                return Err(FrameError::TooLarge);
            }
            let body_len = 6_u32.checked_add(text_len).ok_or(FrameError::TooLarge)?;
            let mut writer = frame_writer(3, body_len)?;
            writer.put(&reason.to_be_bytes())?;
            writer.put(&text_len.to_be_bytes())?;
            writer.put(text)?;
            writer.finish()
        }
        Control::Terms { entries } => {
            if entries.len() > limits.kinds {
                return Err(FrameError::TooLarge);
            }
            let entries_len = entries.len().checked_mul(6).ok_or(FrameError::TooLarge)?;
            let body_len = entries_len.checked_add(4).ok_or(FrameError::TooLarge)?;
            let mut writer = frame_writer(4, body_len)?;
            writer.put(&entries.len().to_be_bytes())?;
            for entry in entries {
                writer.put(&entry.kind.to_be_bytes())?;
                writer.put(&entry.largest.to_be_bytes())?;
            }
            writer.finish()
        }
        Control::Ping => frame_writer(5, 0)?.finish(),
        Control::Unsupported { kind } => {
            let mut writer = frame_writer(6, 2)?;
            writer.put(&kind.to_be_bytes())?;
            writer.finish()
        }
    }
}

/// Decodes a control body whose header has already been checked.
pub fn decode_control(kind: u16, body: &[u8], limits: &Limits) -> Result<Control, FrameError> {
    if u32::try_from(body.len()).is_err() {
        return Err(FrameError::TooLarge);
    }
    let mut reader = Reader::new(body);
    let control = match kind {
        1 => {
            let source = reader.bytes(4).ok_or(FrameError::Malformed)?;
            let magic: [u8; 4] = source.try_into().ok().ok_or(FrameError::Malformed)?;
            let lowest = reader.u16().ok_or(FrameError::Malformed)?;
            let highest = reader.u16().ok_or(FrameError::Malformed)?;
            let features = reader.u16().ok_or(FrameError::Malformed)?;
            let size = reader.u32().ok_or(FrameError::Malformed)?;
            if size > limits.credential {
                return Err(FrameError::TooLarge);
            }
            let credential = Box::from(reader.bytes(size).ok_or(FrameError::Malformed)?);
            Control::Open { magic, lowest, highest, features, credential }
        }
        2 => {
            let version = reader.u16().ok_or(FrameError::Malformed)?;
            let features = reader.u16().ok_or(FrameError::Malformed)?;
            Control::Accept { version, features }
        }
        3 => {
            let reason = reader.u16().ok_or(FrameError::Malformed)?;
            let size = reader.u32().ok_or(FrameError::Malformed)?;
            if size > 256 {
                return Err(FrameError::TooLarge);
            }
            let text = Box::from(reader.bytes(size).ok_or(FrameError::Malformed)?);
            Control::Refuse { reason, text }
        }
        4 => {
            let count = reader.u32().ok_or(FrameError::Malformed)?;
            if count > limits.kinds {
                return Err(FrameError::TooLarge);
            }
            if reader.remaining() != count.checked_mul(6).ok_or(FrameError::TooLarge)? {
                return Err(FrameError::Malformed);
            }
            let mut entries = List::with_capacity(count);
            for _ in 0..count {
                let entry = Term {
                    kind: reader.u16().ok_or(FrameError::Malformed)?,
                    largest: reader.u32().ok_or(FrameError::Malformed)?,
                };
                entries.push(entry).ok().ok_or(FrameError::TooLarge)?;
            }
            Control::Terms { entries }
        }
        5 => Control::Ping,
        6 => Control::Unsupported { kind: reader.u16().ok_or(FrameError::Malformed)? },
        _ => return Err(FrameError::UnknownKind),
    };
    if !reader.is_empty() {
        return Err(FrameError::Malformed);
    }
    Ok(control)
}
