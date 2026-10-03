//! UTF-8, checked a byte at a time (RFC 3629, section 4).
//!
//! The tokenizer meets a string's text in pieces cut anywhere, so a
//! character may span two of them; the writer checks the text it is given.
//! Both check it here rather than with `core::str::from_utf8`, which the
//! subset keeps out of step code (json.md, 7).

/// Where a check of UTF-8 is: between characters, or within one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Utf8 {
    /// Between characters: any byte that can begin one comes next.
    Between,
    /// Within a character: `more` continuation bytes are still to come, at
    /// least one, and the next is in `low..=high`.
    Within { more: u8, low: u8, high: u8 },
}

impl Utf8 {
    /// The state after `byte`, or `None` when UTF-8 has no such byte here.
    #[must_use]
    pub(crate) fn next(self, byte: u8) -> Option<Utf8> {
        match self {
            Utf8::Between => lead(byte),
            Utf8::Within { more, low, high } => {
                if !(low..=high).contains(&byte) {
                    return None;
                }
                match more.checked_sub(1).expect("within a character, a continuation byte is still to come") {
                    0 => Some(Utf8::Between),
                    left => Some(Utf8::Within { more: left, low: 0x80, high: 0xBF }),
                }
            }
        }
    }
}

/// The state after the first byte of a character. The ranges of the byte
/// after it keep out overlong forms, the surrogates and what lies past
/// U+10FFFF (RFC 3629, section 4).
fn lead(byte: u8) -> Option<Utf8> {
    let (more, low, high) = match byte {
        0x00..=0x7F => return Some(Utf8::Between),
        0xC2..=0xDF => (1, 0x80, 0xBF),
        0xE0 => (2, 0xA0, 0xBF),
        0xE1..=0xEC | 0xEE..=0xEF => (2, 0x80, 0xBF),
        0xED => (2, 0x80, 0x9F),
        0xF0 => (3, 0x90, 0xBF),
        0xF1..=0xF3 => (3, 0x80, 0xBF),
        0xF4 => (3, 0x80, 0x8F),
        0x80..=0xC1 | 0xF5..=0xFF => return None,
    };
    Some(Utf8::Within { more, low, high })
}
