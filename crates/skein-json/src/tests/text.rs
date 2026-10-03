//! The checks of text: UTF-8 a byte at a time against the standard
//! library's, and a number's grammar against a plain reading of RFC 8259,
//! section 6.

use crate::number::is_number;
use crate::utf8::Utf8;

/// Whether `bytes` is UTF-8, whole, by the check under test.
fn checked(bytes: &[u8]) -> bool {
    let mut state = Utf8::Between;
    for &byte in bytes {
        match state.next(byte) {
            Some(next) => state = next,
            None => return false,
        }
    }
    state == Utf8::Between
}

#[expect(clippy::disallowed_methods, reason = "the check is compared with the standard library's")]
fn standard(bytes: &[u8]) -> bool {
    core::str::from_utf8(bytes).is_ok()
}

#[test]
fn every_one_and_two_byte_sequence_is_judged_as_the_standard_library_judges_it() {
    for first in 0..=u8::MAX {
        assert_eq!(checked(&[first]), standard(&[first]), "{first:#04x}");
        for second in 0..=u8::MAX {
            let bytes = [first, second];
            assert_eq!(checked(&bytes), standard(&bytes), "{}", bytes.escape_ascii());
        }
    }
}

#[test]
fn three_and_four_byte_sequences_are_judged_at_every_boundary() {
    // Each byte after the second at the edges of the continuation range,
    // and just past them.
    let edges = [0x00, 0x7F, 0x80, 0x8F, 0x90, 0x9F, 0xA0, 0xBF, 0xC0, 0xFF];
    for lead in 0xC0..=u8::MAX {
        for second in 0..=u8::MAX {
            for third in edges {
                let bytes = [lead, second, third];
                assert_eq!(checked(&bytes), standard(&bytes), "{}", bytes.escape_ascii());
                for fourth in edges {
                    let bytes = [lead, second, third, fourth];
                    assert_eq!(checked(&bytes), standard(&bytes), "{}", bytes.escape_ascii());
                }
            }
        }
    }
}

/// A plain reading of the grammar: an optional minus, an integer without a
/// leading zero, an optional fraction, an optional exponent.
fn naive(text: &[u8]) -> bool {
    fn digits(text: &[u8]) -> usize {
        let mut count: usize = 0;
        for byte in text {
            if !byte.is_ascii_digit() {
                break;
            }
            count = count.checked_add(1).unwrap();
        }
        count
    }
    let rest = text.strip_prefix(b"-").unwrap_or(text);
    let integer = digits(rest);
    if integer == 0 || (integer > 1 && rest.first() == Some(&b'0')) {
        return false;
    }
    let mut rest = &rest[integer..];
    if let Some(fraction) = rest.strip_prefix(b".") {
        let count = digits(fraction);
        if count == 0 {
            return false;
        }
        rest = &fraction[count..];
    }
    if let Some(exponent) = rest.strip_prefix(b"e").or(rest.strip_prefix(b"E")) {
        let exponent = exponent.strip_prefix(b"+").or(exponent.strip_prefix(b"-")).unwrap_or(exponent);
        let count = digits(exponent);
        if count == 0 {
            return false;
        }
        rest = &exponent[count..];
    }
    rest.is_empty()
}

#[test]
fn every_short_text_of_a_numbers_bytes_is_judged_as_the_grammar_reads() {
    let alphabet = b"01.eE+-x";
    let mut text = [0_u8; 6];
    for len in 0..=text.len() {
        let mut indices = [0_usize; 6];
        'texts: loop {
            for (at, &index) in indices[..len].iter().enumerate() {
                text[at] = alphabet[index];
            }
            let candidate = &text[..len];
            assert_eq!(is_number(candidate), naive(candidate), "{}", candidate.escape_ascii());
            // The next text of this length, as a counter in the alphabet.
            for index in &mut indices[..len] {
                *index = index.checked_add(1).unwrap();
                if *index < alphabet.len() {
                    continue 'texts;
                }
                *index = 0;
            }
            break;
        }
    }
}
