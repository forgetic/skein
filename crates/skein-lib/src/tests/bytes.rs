//! Bytes (lib.md, 6): the byte search, against a naive one too; the reader,
//! the writer, and a count's decimal digits.

use alloc::boxed::Box;

use crate::bytes::{count, find, find_from};
use crate::{Decimal, List, Overflow, Reader, Rng, Writer};

#[test]
fn find_gives_the_first_occurrence() {
    assert_eq!(find(b"abacabad", b"aba"), Some(0));
    assert_eq!(find(b"abacabad", b"cab"), Some(3));
    assert_eq!(find(b"abacabad", b"abad"), Some(4));
    assert_eq!(find(b"abacabad", b"abac"), Some(0));
    assert_eq!(find(b"abacabad", b"abd"), None);
    assert_eq!(find(b"aaaaab", b"aab"), Some(3));
    assert_eq!(find(b"ab", b"abc"), None, "a needle longer than the haystack");
    assert_eq!(find(b"", b"a"), None);
    assert_eq!(find(b"abc", b"abc"), Some(0));
}

#[test]
fn find_from_starts_where_it_is_told() {
    assert_eq!(find_from(b"abcabc", b"abc", 1), Some(3));
    assert_eq!(find_from(b"abcabc", b"abc", 3), Some(3));
    assert_eq!(find_from(b"abcabc", b"abc", 4), None);
    assert_eq!(find_from(b"abc", b"c", 7), None, "past the end");
}

#[test]
fn an_empty_needle_occurs_everywhere() {
    assert_eq!(find(b"", b""), Some(0));
    assert_eq!(find(b"abc", b""), Some(0));
    assert_eq!(find_from(b"abc", b"", 3), Some(3));
    assert_eq!(find_from(b"abc", b"", 4), None);
    assert_eq!(count(b"abc", b"", 10), 4);
    assert_eq!(count(b"abc", b"", 2), 2);
}

#[test]
fn count_does_not_overlap_and_stops_at_its_cap() {
    assert_eq!(count(b"aaaaa", b"aa", 10), 2);
    assert_eq!(count(b"abcabcabc", b"abc", 10), 3);
    assert_eq!(count(b"abcabcabc", b"abc", 2), 2);
    assert_eq!(count(b"abcabcabc", b"abc", 0), 0);
    assert_eq!(count(b"abcabcabc", b"abd", 2), 0);
    assert_eq!(count(b"x = 1;\ny = 1;\n", b" = 1;", 2), 2);
}

/// `len` bytes of `a` that end in a `b`.
fn a_then_b(len: u32) -> Box<[u8]> {
    let mut bytes = List::with_capacity(len);
    for _ in 1..len {
        bytes.push(b'a').expect("room");
    }
    bytes.push(b'b').expect("room");
    bytes.into_boxed()
}

#[test]
fn a_long_periodic_needle_is_found_without_quadratic_work() {
    // A naive search compares close to the whole needle at every position:
    // billions of comparisons here.
    let haystack = a_then_b(200_000);
    let mut needle = a_then_b(20_000);
    assert_eq!(find(&haystack, &needle), Some(180_000));
    assert_eq!(count(&haystack, &needle, 2), 1);
    needle[0] = b'b';
    assert_eq!(find(&haystack, &needle), None);
}

/// The first occurrence at or after `from`, comparing at every position.
fn naive(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    let last = haystack.len().checked_sub(needle.len())?;
    for at in from..=last {
        if haystack[at..].starts_with(needle) {
            return Some(at);
        }
    }
    None
}

fn naive_count(haystack: &[u8], needle: &[u8], cap: u32) -> u32 {
    let mut found = 0;
    let mut from = 0;
    while found < cap {
        let Some(at) = naive(haystack, needle, from) else { break };
        found = found.checked_add(1).expect("a count below its cap fits a u32");
        from = at.checked_add(needle.len().max(1)).expect("an index within a slice fits a usize");
    }
    found
}

/// A length up to `max`, at random.
fn up_to(rng: &mut Rng, max: usize) -> usize {
    usize::try_from(rng.between(0, u64::try_from(max).expect("fits"))).expect("fits")
}

/// Fills `text` with letters from the first `letters` of the alphabet: few
/// letters make many partial matches.
fn fill(rng: &mut Rng, text: &mut [u8], letters: u64) {
    for byte in text {
        *byte = b'a'.checked_add(u8::try_from(rng.below(letters)).expect("a few")).expect("a letter");
    }
}

#[test]
fn the_search_agrees_with_a_naive_one() {
    let mut rng = Rng::new(0x5EA2_C4ED);
    let mut haystack_buffer = [0_u8; 64];
    let mut needle_buffer = [0_u8; 12];
    for round in 0_u32..20_000 {
        let letters = rng.between(1, 4);
        let len = up_to(&mut rng, haystack_buffer.len());
        fill(&mut rng, &mut haystack_buffer[..len], letters);
        let haystack = &haystack_buffer[..len];
        let needle: &[u8] = if rng.chance(500) && !haystack.is_empty() {
            // A piece of the haystack, so that it occurs at least once.
            let start = up_to(&mut rng, haystack.len() - 1);
            &haystack[start..start + 1 + up_to(&mut rng, haystack.len() - start - 1)]
        } else {
            let len = up_to(&mut rng, needle_buffer.len());
            fill(&mut rng, &mut needle_buffer[..len], letters);
            &needle_buffer[..len]
        };
        for from in 0..=haystack.len() + 1 {
            assert_eq!(find_from(haystack, needle, from), naive(haystack, needle, from), "round {round}");
        }
        for cap in [0, 1, 2, 3, u32::MAX] {
            assert_eq!(count(haystack, needle, cap), naive_count(haystack, needle, cap), "round {round}");
        }
    }
}

#[test]
fn integers_are_big_endian() {
    let mut reader = Reader::new(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    assert_eq!(reader.u8(), Some(1));
    assert_eq!(reader.u16(), Some(0x0203));
    assert_eq!(reader.u32(), Some(0x0405_0607));
    assert_eq!(reader.u64(), Some(0x0809_0A0B_0C0D_0E0F));
    assert!(reader.is_empty());
    assert_eq!(reader.u8(), None);
}

#[test]
fn a_short_read_consumes_nothing() {
    let mut reader = Reader::new(&[1, 2, 3]);
    assert_eq!(reader.u64(), None);
    assert_eq!(reader.u32(), None);
    assert_eq!(reader.bytes(4), None);
    assert_eq!(reader.skip(4), None);
    assert_eq!(reader.remaining(), 3);
    assert_eq!(reader.u16(), Some(0x0102));
    assert_eq!(reader.u16(), None);
    assert_eq!(reader.remaining(), 1);
    assert_eq!(reader.u8(), Some(3));
}

#[test]
fn bytes_and_skip_move_the_cursor() {
    let input = *b"head:body";
    let mut reader = Reader::new(&input);
    assert_eq!(reader.bytes(4), Some(&b"head"[..]));
    assert_eq!(reader.skip(1), Some(()));
    assert_eq!(reader.bytes(0), Some(&b""[..]));
    assert_eq!(reader.bytes(4), Some(&b"body"[..]));
    assert!(reader.is_empty());
}

/// The bytes borrow what the reader is over, not the reader, so they
/// outlive it.
fn second_word(input: &[u8]) -> Option<&[u8]> {
    let mut reader = Reader::new(input);
    reader.skip(5)?;
    reader.bytes(4)
}

#[test]
fn bytes_outlive_their_reader() {
    assert_eq!(second_word(b"head:body"), Some(&b"body"[..]));
    assert_eq!(second_word(b"head:bo"), None);
}

#[test]
fn an_empty_reader_reads_nothing_but_nothing() {
    let mut reader = Reader::new(&[]);
    assert_eq!((reader.remaining(), reader.is_empty()), (0, true));
    assert_eq!(reader.u8(), None);
    assert_eq!(reader.bytes(0), Some(&b""[..]));
    assert_eq!(reader.skip(0), Some(()));
    assert_eq!(reader.skip(u32::MAX), None);
}

#[test]
fn a_writer_builds_its_box_from_slices() {
    let mut writer = Writer::new(11);
    assert_eq!(writer.put(b"hello"), Ok(()));
    assert_eq!(writer.put(b""), Ok(()));
    assert_eq!(writer.put(b" "), Ok(()));
    assert_eq!((writer.written(), writer.room()), (6, 5));
    assert_eq!(writer.put(b"world"), Ok(()));
    assert_eq!(&*writer.finish(), b"hello world");
}

#[test]
fn a_put_that_does_not_fit_writes_nothing() {
    let mut writer = Writer::new(4);
    assert_eq!(writer.put(b"abc"), Ok(()));
    assert_eq!(writer.put(b"de"), Err(Overflow));
    assert_eq!(writer.written(), 3);
    assert_eq!(writer.put(b"d"), Ok(()));
    assert_eq!(writer.put(b"e"), Err(Overflow));
    assert_eq!(&*writer.finish(), b"abcd");
}

#[test]
fn an_empty_writer_is_finished_at_once() {
    let mut writer = Writer::new(0);
    assert_eq!(writer.put(b"a"), Err(Overflow));
    assert!(writer.finish().is_empty());
}

#[test]
#[should_panic(expected = "a writer is finished full")]
fn finishing_short_is_a_bug() {
    let mut writer = Writer::new(2);
    writer.put(b"a").expect("room");
    drop(writer.finish());
}

#[test]
fn a_number_is_its_digits_without_leading_zeros() {
    assert_eq!(Decimal::of(0).as_bytes(), b"0");
    assert_eq!(Decimal::of(7).as_bytes(), b"7");
    assert_eq!(Decimal::of(10).as_bytes(), b"10");
    assert_eq!(Decimal::of(105).as_bytes(), b"105");
    assert_eq!(Decimal::of(1_234_567_890).as_bytes(), b"1234567890");
    assert_eq!(Decimal::of(u64::from(u32::MAX)).as_bytes(), b"4294967295");
    assert_eq!(Decimal::of(u64::MAX).as_bytes(), b"18446744073709551615");
}

#[test]
fn every_power_of_ten_adds_a_digit() {
    let mut power: u64 = 1;
    for zeros in 0..20 {
        let digits = Decimal::of(power);
        let (lead, rest) = digits.as_bytes().split_first().unwrap();
        assert_eq!((*lead, rest.len()), (b'1', zeros), "{power}");
        assert_eq!(rest, &[b'0'; 19][..zeros], "{power}");
        let below = Decimal::of(power - 1);
        if zeros > 0 {
            assert_eq!(below.as_bytes(), &[b'9'; 19][..zeros], "{power} - 1");
        }
        power = power.saturating_mul(10);
    }
}

#[test]
fn a_count_is_measured_then_written() {
    let count = Decimal::of(1234);
    let mut writer = Writer::new(b"[".len() + count.as_bytes().len() + b" bytes cut]".len());
    for piece in [&b"["[..], count.as_bytes(), b" bytes cut]"] {
        writer.put(piece).unwrap();
    }
    assert_eq!(&*writer.finish(), b"[1234 bytes cut]");
}

#[test]
#[expect(clippy::disallowed_macros, reason = "the digits are checked against the standard library's")]
fn numbers_drawn_at_every_width_match_the_standard_library() {
    let mut rng = Rng::new(0x0DEC_1A11);
    for _ in 0..10_000_u32 {
        let width = u32::try_from(rng.below(64)).unwrap();
        let n = rng.next_u64().checked_shr(width).unwrap();
        assert_eq!(Decimal::of(n).as_bytes(), format!("{n}").as_bytes());
    }
}
