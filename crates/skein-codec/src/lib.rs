//! Shared bounded reads for schema-generated codecs (codec.md, sections 2 and 4).
//! The reader owns no state beyond its cursor; this crate knows no schema or
//! field path. Generated decoders call these helpers after choosing a limit.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::boxed::Box;
use skein_lib::Reader;

#[cfg(test)]
mod tests;

/// Why decoding a record failed; a generated problem adds its field path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reason {
    /// The bytes ended before the field did.
    Short,
    /// A length or count passed its field's limit.
    Bound,
    /// An enumeration's tag has no variant.
    Tag,
    /// A boolean was neither zero nor one.
    Bool,
    /// Text was not UTF-8.
    Utf8,
    /// The record has a version this decoder does not read.
    Version,
    /// Bytes remained after the record.
    Trailing,
}

/// Reads a length and checks both its limit and the available bytes.
pub fn read_len(reader: &mut Reader<'_>, limit: u32) -> Result<u32, Reason> {
    let length = reader.u32().ok_or(Reason::Short)?;
    if length > limit {
        return Err(Reason::Bound);
    }
    if length > reader.remaining() {
        return Err(Reason::Short);
    }
    Ok(length)
}

/// Reads a list count and checks its limit. Item reads check remaining bytes.
pub fn read_count(reader: &mut Reader<'_>, limit: u32) -> Result<u32, Reason> {
    let count = reader.u32().ok_or(Reason::Short)?;
    if count > limit {
        return Err(Reason::Bound);
    }
    Ok(count)
}

/// Reads bounded, UTF-8 text into a box allocated at its final length.
pub fn read_text(reader: &mut Reader<'_>, limit: u32) -> Result<Box<[u8]>, Reason> {
    let length = read_len(reader, limit)?;
    let bytes = reader.bytes(length).ok_or(Reason::Short)?;
    if !text_is_valid(bytes) {
        return Err(Reason::Utf8);
    }
    Ok(Box::from(bytes))
}

/// Reads a versioned record's leading version without decoding the record.
pub fn version_of(bytes: &[u8]) -> Result<u16, Reason> {
    let mut reader = Reader::new(bytes);
    reader.u16().ok_or(Reason::Short)
}

/// Reports whether a complete text field is UTF-8 (RFC 3629, section 4).
#[must_use]
pub fn text_is_valid(bytes: &[u8]) -> bool {
    let mut more = 0_u8;
    let mut low = 0_u8;
    let mut high = 0_u8;
    for &byte in bytes {
        if more > 0 {
            if byte < low || byte > high {
                return false;
            }
            more = more.checked_sub(1).expect("a continuation remains");
            low = 0x80;
            high = 0xbf;
        } else {
            match byte {
                0x00..=0x7f => {}
                0xc2..=0xdf => {
                    more = 1;
                    low = 0x80;
                    high = 0xbf;
                }
                0xe0 => {
                    more = 2;
                    low = 0xa0;
                    high = 0xbf;
                }
                0xe1..=0xec | 0xee..=0xef => {
                    more = 2;
                    low = 0x80;
                    high = 0xbf;
                }
                0xed => {
                    more = 2;
                    low = 0x80;
                    high = 0x9f;
                }
                0xf0 => {
                    more = 3;
                    low = 0x90;
                    high = 0xbf;
                }
                0xf1..=0xf3 => {
                    more = 3;
                    low = 0x80;
                    high = 0xbf;
                }
                0xf4 => {
                    more = 3;
                    low = 0x80;
                    high = 0x8f;
                }
                0x80..=0xc1 | 0xf5..=0xff => return false,
            }
        }
    }
    more == 0
}
