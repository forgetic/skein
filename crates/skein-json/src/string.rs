//! A string's text, decoded as it arrives (RFC 8259, section 7; json.md,
//! 3): escapes undone, every byte checked as UTF-8, a control character
//! refused, and the result held under the string limit.
//!
//! The tokenizer scans to the next quote, so a piece ends at a quote or at
//! the scan's maximum, anywhere in a character or an escape: the state
//! between two pieces is a [`Text`].

use skein_lib::List;

use crate::tokenizer::Error;
use crate::utf8::Utf8;

/// Where the decoding of a string's text is, between two of its bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Text {
    /// Between characters: a character, an escape or the closing quote
    /// comes next.
    Plain,
    /// Within a character of UTF-8, whose first byte is in the text.
    Char { more: u8, low: u8, high: u8 },
    /// After a backslash: what it escapes comes next.
    Escape,
    /// Within a `\u` escape: `digits` of its four hex digits read, making
    /// `unit`.
    Unicode { digits: u8, unit: u16 },
    /// After the `\u` escape of a high surrogate, which only the escape of
    /// a low one may follow: its backslash comes next.
    High { high: u16 },
    /// After that backslash: its `u` comes next.
    HighEscape { high: u16 },
    /// Within the low surrogate's `\u` escape, after `high`.
    Low { high: u16, digits: u8, unit: u16 },
}

/// How a piece of a string's text left the string.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Piece {
    /// It goes on, in this state.
    More(Text),
    /// The closing quote was the piece's last byte: the string is whole.
    Closed,
}

/// Decodes `piece`, the next bytes of a string's text after `text`, onto
/// `out`, which may hold at most `limit` bytes.
///
/// A piece is a scan to the next quote, so a quote can only be its last
/// byte; one that closes the string anywhere else is the side below's
/// bug, asserted.
pub(crate) fn decode(text: Text, piece: &[u8], out: &mut List<u8>, limit: u32) -> Result<Piece, Error> {
    let mut text = text;
    // Bounded by the piece, which is at most the scan's maximum.
    for (at, &byte) in piece.iter().enumerate() {
        match next(text, byte, out, limit)? {
            Some(after) => text = after,
            None => {
                assert!(at.checked_add(1) == Some(piece.len()), "a scan ends at its first quote");
                return Ok(Piece::Closed);
            }
        }
    }
    Ok(Piece::More(text))
}

/// The state after `byte`, or `None` when it closes the string.
fn next(text: Text, byte: u8, out: &mut List<u8>, limit: u32) -> Result<Option<Text>, Error> {
    match text {
        Text::Plain => plain(byte, out, limit),
        Text::Char { more, low, high } => {
            let after = Utf8::Within { more, low, high }.next(byte).ok_or(Error::Utf8)?;
            // The room for the whole character was made at its first byte.
            put(out, &[byte]);
            Ok(Some(character(after)))
        }
        Text::Escape => escape(byte, out, limit),
        Text::Unicode { digits, unit } => {
            let unit = hex(unit, byte)?;
            match count(digits) {
                4 => unit_escaped(unit, out, limit),
                digits => Ok(Some(Text::Unicode { digits, unit })),
            }
        }
        Text::High { high } => match byte {
            b'\\' => Ok(Some(Text::HighEscape { high })),
            _ => Err(Error::Surrogate),
        },
        Text::HighEscape { high } => match byte {
            b'u' => Ok(Some(Text::Low { high, digits: 0, unit: 0 })),
            _ => Err(Error::Surrogate),
        },
        Text::Low { high, digits, unit } => {
            let unit = hex(unit, byte)?;
            match count(digits) {
                4 => pair_escaped(high, unit, out, limit),
                digits => Ok(Some(Text::Low { high, digits, unit })),
            }
        }
    }
}

/// A byte between characters.
fn plain(byte: u8, out: &mut List<u8>, limit: u32) -> Result<Option<Text>, Error> {
    match byte {
        b'"' => Ok(None),
        b'\\' => Ok(Some(Text::Escape)),
        0x00..=0x1F => Err(Error::Control),
        _ => {
            let after = Utf8::Between.next(byte).ok_or(Error::Utf8)?;
            // The whole character must fit, decided at its first byte, so
            // that whether it is too long does not wait on whether it is
            // UTF-8.
            let width = match after {
                Utf8::Between => 1,
                Utf8::Within { more, .. } => u32::from(more).checked_add(1).expect("at most four bytes"),
            };
            room(out, limit, width)?;
            put(out, &[byte]);
            Ok(Some(character(after)))
        }
    }
}

/// The state within or after a character of UTF-8.
fn character(utf8: Utf8) -> Text {
    match utf8 {
        Utf8::Between => Text::Plain,
        Utf8::Within { more, low, high } => Text::Char { more, low, high },
    }
}

/// The byte after a backslash.
fn escape(byte: u8, out: &mut List<u8>, limit: u32) -> Result<Option<Text>, Error> {
    let unescaped = match byte {
        b'"' => b'"',
        b'\\' => b'\\',
        b'/' => b'/',
        b'b' => 0x08,
        b'f' => 0x0C,
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'u' => return Ok(Some(Text::Unicode { digits: 0, unit: 0 })),
        _ => return Err(Error::Escape),
    };
    room(out, limit, 1)?;
    put(out, &[unescaped]);
    Ok(Some(Text::Plain))
}

/// A `\u` escape's four digits read, outside a surrogate pair.
fn unit_escaped(unit: u16, out: &mut List<u8>, limit: u32) -> Result<Option<Text>, Error> {
    match unit {
        0xD800..=0xDBFF => Ok(Some(Text::High { high: unit })),
        0xDC00..=0xDFFF => Err(Error::Surrogate),
        _ => {
            scalar(u32::from(unit), out, limit)?;
            Ok(Some(Text::Plain))
        }
    }
}

/// The low surrogate's four digits read, after `high`.
fn pair_escaped(high: u16, low: u16, out: &mut List<u8>, limit: u32) -> Result<Option<Text>, Error> {
    if !(0xDC00..=0xDFFF).contains(&low) {
        return Err(Error::Surrogate);
    }
    // Ten bits from each half, above the basic plane.
    let bits = (u32::from(high) & 0x3FF).wrapping_shl(10) | (u32::from(low) & 0x3FF);
    scalar(bits.checked_add(0x1_0000).expect("at most U+10FFFF"), out, limit)?;
    Ok(Some(Text::Plain))
}

/// Puts the UTF-8 of the scalar value `point`, which an escape spelled.
fn scalar(point: u32, out: &mut List<u8>, limit: u32) -> Result<(), Error> {
    let character = char::from_u32(point).expect("neither a surrogate nor past U+10FFFF");
    let mut buffer = [0; 4];
    let bytes = character.encode_utf8(&mut buffer).as_bytes();
    room(out, limit, u32::try_from(bytes.len()).expect("at most four bytes"))?;
    put(out, bytes);
    Ok(())
}

/// `unit` with the hex digit `byte` appended.
fn hex(unit: u16, byte: u8) -> Result<u16, Error> {
    let value = match byte {
        b'0'..=b'9' => byte.wrapping_sub(b'0'),
        b'a'..=b'f' => byte.wrapping_sub(b'a').wrapping_add(10),
        b'A'..=b'F' => byte.wrapping_sub(b'A').wrapping_add(10),
        _ => return Err(Error::Escape),
    };
    // At most three digits are in `unit`, so nothing is shifted out.
    Ok(unit.wrapping_shl(4) | u16::from(value))
}

/// The digits read, one more.
fn count(digits: u8) -> u8 {
    digits.checked_add(1).expect("at most four digits")
}

/// Whether `n` more bytes fit the string's limit.
fn room(out: &List<u8>, limit: u32, n: u32) -> Result<(), Error> {
    match out.len().checked_add(n) {
        Some(len) if len <= limit => Ok(()),
        Some(_) | None => Err(Error::StringTooLong),
    }
}

/// Appends bytes there is room for.
fn put(out: &mut List<u8>, bytes: &[u8]) {
    for &byte in bytes {
        out.push(byte).expect("room was made under the limit, which the text holds");
    }
}
