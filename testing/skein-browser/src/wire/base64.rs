//! Bounded base64 decoding for screenshot PNGs.

#![expect(clippy::disallowed_types, reason = "a decoded screenshot is one owned byte buffer")]

use alloc::boxed::Box;
use alloc::vec::Vec;

/// An invalid or overlong screenshot body.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Invalid,
    TooLong,
}

/// Decode standard padded base64 under a decoded byte limit.
pub fn decode(input: &[u8], limit: u32) -> Result<Box<[u8]>, Error> {
    if !input.len().is_multiple_of(4) {
        return Err(Error::Invalid);
    }
    let mut output = Vec::new();
    for (index, quartet) in input.chunks_exact(4).enumerate() {
        let last = index.checked_add(1).ok_or(Error::TooLong)? == input.len() / 4;
        let a = digit(*quartet.first().ok_or(Error::Invalid)?).ok_or(Error::Invalid)?;
        let b = digit(*quartet.get(1).ok_or(Error::Invalid)?).ok_or(Error::Invalid)?;
        let c = quartet.get(2).copied().ok_or(Error::Invalid)?;
        let d = quartet.get(3).copied().ok_or(Error::Invalid)?;
        if (c == b'=' || d == b'=') && !last {
            return Err(Error::Invalid);
        }
        if c == b'=' && d != b'=' {
            return Err(Error::Invalid);
        }
        let c_value = if c == b'=' { 0 } else { digit(c).ok_or(Error::Invalid)? };
        let d_value = if d == b'=' { 0 } else { digit(d).ok_or(Error::Invalid)? };
        if c == b'=' && (b & 0x0f) != 0 || d == b'=' && c != b'=' && (c_value & 0x03) != 0 {
            return Err(Error::Invalid);
        }
        let first = (a << 2_u32) | (b >> 4_u32);
        push(&mut output, first, limit)?;
        if c != b'=' {
            let second = (b << 4_u32) | (c_value >> 2_u32);
            push(&mut output, second, limit)?;
        }
        if d != b'=' {
            let third = (c_value << 6_u32) | d_value;
            push(&mut output, third, limit)?;
        }
    }
    Ok(output.into_boxed_slice())
}

fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => byte.checked_sub(b'A'),
        b'a'..=b'z' => byte.checked_sub(b'a')?.checked_add(26),
        b'0'..=b'9' => byte.checked_sub(b'0')?.checked_add(52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn push(output: &mut Vec<u8>, byte: u8, limit: u32) -> Result<(), Error> {
    let Ok(limit) = usize::try_from(limit) else {
        return Err(Error::TooLong);
    };
    if output.len() >= limit {
        return Err(Error::TooLong);
    }
    output.push(byte);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Error, decode};

    #[test]
    fn padded_and_unpadded_groups() {
        assert_eq!(decode(b"YQ==", 1).expect("one byte").as_ref(), b"a");
        assert_eq!(decode(b"YWI=", 2).expect("two bytes").as_ref(), b"ab");
        assert_eq!(decode(b"YWJj", 3).expect("three bytes").as_ref(), b"abc");
    }

    #[test]
    fn rejects_bad_padding_and_limit() {
        assert_eq!(decode(b"YQ=A", 3), Err(Error::Invalid));
        assert_eq!(decode(b"YQ==YWJj", 3), Err(Error::Invalid));
        assert_eq!(decode(b"YR==", 3), Err(Error::Invalid));
        assert_eq!(decode(b"YWJj", 2), Err(Error::TooLong));
    }
}
