//! Streams (lib.md, 7): delimiters, and the intake meeting fills and scans
//! under every split of its input, against a plain reference.

#![expect(clippy::disallowed_types, reason = "the reference and the model are Vecs, grown as a test goes")]

use alloc::boxed::Box;
use alloc::vec::Vec;

use super::ROUNDS;
use crate::stream::{Delimiter, Read};
use crate::{Intake, Overflow, Rng};

#[test]
fn a_delimiter_is_one_to_four_bytes() {
    assert_eq!(Delimiter::new(b""), None);
    assert_eq!(Delimiter::new(b"abcde"), None);
    assert_eq!(Delimiter::new(b"\n"), Some(Delimiter::LF));
    assert_eq!(Delimiter::new(b"\r\n"), Some(Delimiter::CRLF));
    assert_eq!(Delimiter::new(b"\r\n\r\n"), Some(Delimiter::CRLF_CRLF));
    let three = Delimiter::new(b"abc").expect("three bytes");
    assert_eq!(three.as_bytes(), b"abc");
}

#[test]
fn the_consts_are_their_bytes() {
    assert_eq!(Delimiter::LF.as_bytes(), b"\n");
    assert_eq!(Delimiter::CRLF.as_bytes(), b"\r\n");
    assert_eq!(Delimiter::CRLF_CRLF.as_bytes(), b"\r\n\r\n");
}

#[test]
fn a_delimiter_with_zero_bytes_is_not_a_shorter_one() {
    let zero = Delimiter::new(b"\0").expect("one byte");
    let zeros = Delimiter::new(b"\0\0").expect("two bytes");
    assert_ne!(zero, zeros);
    assert_eq!(zeros.as_bytes(), b"\0\0");
}

fn scan(until: Delimiter, max: u32) -> Read {
    Read::Scan { until, max }
}

fn boxed(bytes: &[u8]) -> Box<[u8]> {
    Box::from(bytes)
}

#[test]
fn a_fill_is_met_once_its_bytes_are_buffered() {
    let mut intake = Intake::with_capacity(8);
    assert_eq!(intake.append(b"ab"), Ok(()));
    assert_eq!(intake.meet(Read::Fill(3)), None);
    assert_eq!(intake.append(b"cde"), Ok(()));
    assert_eq!(intake.meet(Read::Fill(3)), Some(boxed(b"abc")));
    assert_eq!(intake.len(), 2);
    assert_eq!(intake.meet(Read::Fill(2)), Some(boxed(b"de")));
    assert!(intake.is_empty());
}

#[test]
fn nothing_delivers_nothing() {
    let mut intake = Intake::with_capacity(4);
    assert_eq!(intake.append(b"abcd"), Ok(()));
    assert_eq!(intake.meet(Read::Nothing), None);
    assert_eq!(intake.len(), 4);
}

#[test]
fn an_empty_fill_is_an_empty_box() {
    let mut intake = Intake::with_capacity(0);
    assert_eq!(intake.meet(Read::Fill(0)), Some(boxed(b"")));
}

#[test]
fn a_scan_delivers_through_the_first_delimiter() {
    let mut intake = Intake::with_capacity(16);
    assert_eq!(intake.append(b"GET /\r\nHost"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 16)), Some(boxed(b"GET /\r\n")));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 16)), None);
    assert_eq!(intake.append(b": a\r\n\r\n"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 16)), Some(boxed(b"Host: a\r\n")));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 16)), Some(boxed(b"\r\n")));
    assert!(intake.is_empty());
}

#[test]
fn a_delimiter_split_across_appends_is_found() {
    let mut intake = Intake::with_capacity(16);
    let head = scan(Delimiter::CRLF_CRLF, 16);
    for piece in [&b"a\r"[..], b"\n", b"\r"] {
        assert_eq!(intake.append(piece), Ok(()));
        assert_eq!(intake.meet(head), None);
    }
    assert_eq!(intake.append(b"\nb"), Ok(()));
    assert_eq!(intake.meet(head), Some(boxed(b"a\r\n\r\n")));
    assert_eq!(intake.meet(Read::Fill(1)), Some(boxed(b"b")));
}

#[test]
fn an_intake_knows_when_it_ends_partway_through_a_delimiter() {
    let mut intake = Intake::with_capacity(16);
    assert!(!intake.ends_partway(Delimiter::CRLF), "nothing buffered");
    assert_eq!(intake.append(b"ab"), Ok(()));
    assert!(!intake.ends_partway(Delimiter::CRLF));
    assert_eq!(intake.append(b"\r"), Ok(()));
    assert!(intake.ends_partway(Delimiter::CRLF), "its first byte");
    assert!(!intake.ends_partway(Delimiter::LF), "a delimiter of one byte has no part");
    assert_eq!(intake.append(b"\n"), Ok(()));
    assert!(!intake.ends_partway(Delimiter::CRLF), "all of it is no part of it");
    assert!(intake.ends_partway(Delimiter::CRLF_CRLF), "its first two bytes");
    assert_eq!(intake.append(b"\r"), Ok(()));
    assert!(intake.ends_partway(Delimiter::CRLF_CRLF), "its first three");
    assert_eq!(intake.meet(Read::Fill(4)), Some(boxed(b"ab\r\n")));
    assert!(intake.ends_partway(Delimiter::CRLF_CRLF), "what is left, from the front");
    assert_eq!(intake.meet(Read::Fill(1)), Some(boxed(b"\r")));
    assert!(!intake.ends_partway(Delimiter::CRLF_CRLF), "empty again");
}

#[test]
fn a_delimiter_ending_at_max_is_found() {
    let mut intake = Intake::with_capacity(8);
    assert_eq!(intake.append(b"abc\r\nde"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 5)), Some(boxed(b"abc\r\n")));
}

#[test]
fn a_delimiter_ending_past_max_is_not() {
    let mut intake = Intake::with_capacity(8);
    assert_eq!(intake.append(b"abcd\r"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 5)), Some(boxed(b"abcd\r")));
    assert_eq!(intake.append(b"\n"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 5)), None);
    assert_eq!(intake.len(), 1);
}

#[test]
fn max_bytes_without_a_delimiter_are_delivered_as_they_are() {
    let mut intake = Intake::with_capacity(8);
    assert_eq!(intake.append(b"abc"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::LF, 4)), None);
    assert_eq!(intake.append(b"defg"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::LF, 4)), Some(boxed(b"abcd")));
    assert_eq!(intake.meet(Read::Fill(3)), Some(boxed(b"efg")));
}

#[test]
fn a_scan_as_short_as_its_delimiter_is_met() {
    let mut intake = Intake::with_capacity(4);
    assert_eq!(intake.append(b"\r\n\r"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 2)), Some(boxed(b"\r\n")));
    assert_eq!(intake.append(b"x"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 2)), Some(boxed(b"\rx")));
}

#[test]
fn the_cap_refuses_an_append_whole_and_room_counts_down() {
    let mut intake = Intake::with_capacity(4);
    assert_eq!((intake.capacity(), intake.room()), (4, 4));
    assert_eq!(intake.append(b"abc"), Ok(()));
    assert_eq!(intake.room(), 1);
    assert_eq!(intake.append(b"de"), Err(Overflow));
    assert_eq!(intake.len(), 3);
    assert_eq!(intake.append(b"d"), Ok(()));
    assert_eq!(intake.room(), 0);
    assert_eq!(intake.append(b"e"), Err(Overflow));
    assert_eq!(intake.append(b""), Ok(()));
    assert_eq!(intake.meet(Read::Fill(2)), Some(boxed(b"ab")));
    assert_eq!(intake.room(), 2);
    assert_eq!(intake.append(b"ef"), Ok(()));
    assert_eq!(intake.meet(Read::Fill(4)), Some(boxed(b"cdef")));
    assert_eq!(intake.room(), 4);
}

#[test]
fn a_full_intake_meets_any_demand_it_can_hold() {
    let mut intake = Intake::with_capacity(4);
    assert_eq!(intake.append(b"abcd"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::LF, 4)), Some(boxed(b"abcd")));
    assert_eq!(Intake::worst_case(4), Some(4));
}

#[test]
fn interleaved_demands_take_from_the_front_in_turn() {
    let mut intake = Intake::with_capacity(32);
    assert_eq!(intake.append(b"4\r\nabcd\r\n0\r\n\r\n"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 8)), Some(boxed(b"4\r\n")));
    assert_eq!(intake.meet(Read::Fill(4)), Some(boxed(b"abcd")));
    assert_eq!(intake.meet(Read::Fill(2)), Some(boxed(b"\r\n")));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 8)), Some(boxed(b"0\r\n")));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 8)), Some(boxed(b"\r\n")));
    assert!(intake.is_empty());
}

#[test]
fn a_search_for_one_delimiter_does_not_hide_another() {
    let mut intake = Intake::with_capacity(16);
    assert_eq!(intake.append(b"ab\ncd\r"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 16)), None);
    assert_eq!(intake.meet(scan(Delimiter::LF, 16)), Some(boxed(b"ab\n")));
    assert_eq!(intake.meet(scan(Delimiter::LF, 16)), None);
    assert_eq!(intake.append(b"\n"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::CRLF, 16)), Some(boxed(b"cd\r\n")));
}

#[test]
fn a_shorter_scan_after_a_longer_one_still_stops_at_its_max() {
    let mut intake = Intake::with_capacity(16);
    assert_eq!(intake.append(b"abcdef"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::LF, 16)), None);
    assert_eq!(intake.meet(scan(Delimiter::LF, 4)), Some(boxed(b"abcd")));
    assert_eq!(intake.append(b"\n"), Ok(()));
    assert_eq!(intake.meet(scan(Delimiter::LF, 16)), Some(boxed(b"ef\n")));
}

/// What `read` takes from the front of `stream` when all of it is
/// buffered, found the plain way: how many bytes, or `None`.
fn reference(stream: &[u8], read: Read) -> Option<usize> {
    match read {
        Read::Nothing => None,
        Read::Fill(n) => {
            let n = usize::try_from(n).unwrap();
            (stream.len() >= n).then_some(n)
        }
        Read::Scan { until, max } => {
            let max = usize::try_from(max).unwrap();
            let needle = until.as_bytes();
            let window = &stream[..stream.len().min(max)];
            for at in 0..window.len() {
                if window[at..].starts_with(needle) {
                    return Some(at.checked_add(needle.len()).unwrap());
                }
            }
            (stream.len() >= max).then_some(max)
        }
    }
}

/// Feeds `input` to an intake of `capacity` in pieces of the lengths in
/// `pieces` (cycled), cut further where a piece does not fit, meeting
/// `demands` in turn as soon as each can be met; checks every delivery
/// against the reference, and that the demands met are those the whole
/// input meets, returning how many that is.
fn check(input: &[u8], demands: &[Read], capacity: u32, pieces: &[usize]) -> usize {
    let mut expected = Vec::new();
    let mut rest = input;
    for &read in demands {
        if read == Read::Nothing {
            expected.push(None);
            continue;
        }
        let Some(n) = reference(rest, read) else { break };
        let (taken, after) = rest.split_at_checked(n).unwrap();
        expected.push(Some(taken));
        rest = after;
    }

    let mut intake = Intake::with_capacity(capacity);
    let mut unfed = input;
    let mut pieces = pieces.iter().cycle();
    for (index, (&read, &expected)) in demands.iter().zip(&expected).enumerate() {
        let delivered = loop {
            let delivered = intake.meet(read);
            if delivered.is_some() || read == Read::Nothing {
                break delivered;
            }
            assert!(!unfed.is_empty(), "demand {index} ({read:?}) is met by the whole input");
            assert!(intake.room() > 0, "demand {index} ({read:?}) fits the cap, so a full intake meets it");
            let want = (*pieces.next().unwrap()).clamp(1, unfed.len());
            let fits = want.min(usize::try_from(intake.room()).unwrap());
            if fits < want {
                let before = intake.len();
                assert_eq!(intake.append(&unfed[..want]), Err(Overflow));
                assert_eq!(intake.len(), before, "a refused append appends nothing");
            }
            let (piece, after) = unfed.split_at_checked(fits).unwrap();
            assert_eq!(intake.append(piece), Ok(()));
            unfed = after;
        };
        assert_eq!(delivered.as_deref(), expected, "demand {index}: {read:?}");
    }
    // Nothing more is met than the whole input meets.
    if let Some(&read) = demands.get(expected.len()) {
        intake.append(unfed).unwrap();
        assert_eq!(intake.meet(read), None, "demand {} is not met by the whole input", expected.len());
    }
    expected.len()
}

/// A head, a line, a two-byte fill, a line ending exactly at its max, a
/// line cut at its max, and the byte after it.
const INPUT: &[u8] = b"a\r\n\r\nb\r\ncde\nf\r\n";

const DEMANDS: [Read; 7] = [
    Read::Scan { until: Delimiter::CRLF_CRLF, max: 8 },
    Read::Scan { until: Delimiter::CRLF, max: 3 },
    Read::Nothing,
    Read::Fill(2),
    Read::Scan { until: Delimiter::LF, max: 3 },
    Read::Scan { until: Delimiter::CRLF, max: 2 },
    Read::Fill(1),
];

#[test]
fn every_split_of_the_input_meets_the_same_demands() {
    let cuts = INPUT.len() - 1;
    for chosen in 0_u32..1 << cuts {
        let mut pieces = Vec::new();
        let mut start = 0;
        for at in 1..INPUT.len() {
            if chosen & (1 << (at - 1)) != 0 {
                pieces.push(at - start);
                start = at;
            }
        }
        pieces.push(INPUT.len() - start);
        assert_eq!(check(INPUT, &DEMANDS, 8, &pieces), DEMANDS.len(), "split {chosen:b}");
    }
}

#[test]
fn one_byte_at_a_time_meets_the_same_demands() {
    for capacity in [8, 9, 16] {
        assert_eq!(check(INPUT, &DEMANDS, capacity, &[1]), DEMANDS.len());
    }
}

#[test]
fn random_splits_demands_and_caps_meet_what_the_reference_meets() {
    let delimiters = [Delimiter::LF, Delimiter::CRLF, Delimiter::CRLF_CRLF, Delimiter::new(b"a\r").unwrap()];
    let mut rng = Rng::new(0x1_47A4E);
    for _ in 0_u32..ROUNDS {
        let capacity = u32::try_from(rng.between(4, 24)).unwrap();
        let mut input = Vec::new();
        for _ in 0..rng.below(96) {
            input.push(*[b'a', b'b', b'\r', b'\n'].get(usize::try_from(rng.below(4)).unwrap()).unwrap());
        }
        let mut demands = Vec::new();
        for _ in 0..rng.between(1, 40) {
            let read = match rng.below(5) {
                0 => Read::Nothing,
                1 => Read::Fill(u32::try_from(rng.below(u64::from(capacity) + 1)).unwrap()),
                _ => {
                    let until = delimiters[usize::try_from(rng.below(4)).unwrap()];
                    let shortest = u64::try_from(until.as_bytes().len()).unwrap();
                    scan(until, u32::try_from(rng.between(shortest, u64::from(capacity))).unwrap())
                }
            };
            demands.push(read);
        }
        let mut pieces = Vec::new();
        for _ in 0_u32..8 {
            pieces.push(usize::try_from(rng.between(1, 12)).unwrap());
        }
        check(&input, &demands, capacity, &pieces);
    }
}

/// Takes `n` bytes from the front of the model, as a delivery does.
fn drain(model: &mut Vec<u8>, n: usize) {
    drop(model.drain(..n));
}

#[test]
fn every_meet_takes_what_the_buffer_holds_demands_changing_or_not() {
    // Few letters and overlapping delimiters make many partial matches;
    // a demand left unmet is often followed by a different one.
    let delimiters: [&[u8]; 11] =
        [b"a", b"aa", b"aaa", b"aaaa", b"ab", b"aba", b"abab", b"aab", b"aaab", b"abaa", b"bab"];
    let mut rng = Rng::new(0x3E_E7);
    for round in 0_u32..ROUNDS {
        let capacity = u32::try_from(rng.between(0, 12)).unwrap();
        let cap = usize::try_from(capacity).unwrap();
        let mut intake = Intake::with_capacity(capacity);
        let mut model = Vec::new();
        for op in 0_u32..60 {
            if rng.chance(500) {
                let mut bytes = [0_u8; 6];
                let len = usize::try_from(rng.below(7)).unwrap();
                for byte in &mut bytes[..len] {
                    *byte = if rng.chance(500) { b'a' } else { b'b' };
                }
                let fits = model.len().checked_add(len).unwrap() <= cap;
                let appended = intake.append(&bytes[..len]);
                assert_eq!(appended.is_ok(), fits, "round {round}, op {op}");
                if fits {
                    model.extend_from_slice(&bytes[..len]);
                }
            } else {
                let read = match rng.below(4) {
                    0 => Read::Nothing,
                    1 => Read::Fill(u32::try_from(rng.below(u64::from(capacity) + 1)).unwrap()),
                    _ => {
                        let until = delimiters[usize::try_from(rng.below(11)).unwrap()];
                        let shortest = u32::try_from(until.len()).unwrap();
                        if shortest > capacity {
                            continue;
                        }
                        let max = u32::try_from(rng.between(u64::from(shortest), u64::from(capacity))).unwrap();
                        scan(Delimiter::new(until).unwrap(), max)
                    }
                };
                let expected = reference(&model, read);
                let delivered = intake.meet(read);
                match expected {
                    Some(n) => {
                        assert_eq!(delivered.as_deref(), Some(&model[..n]), "round {round}, op {op}: {read:?}");
                        drain(&mut model, n);
                    }
                    None => assert_eq!(delivered, None, "round {round}, op {op}: {read:?}"),
                }
            }
            let len = u32::try_from(model.len()).unwrap();
            assert_eq!(
                (intake.len(), intake.room()),
                (len, capacity.checked_sub(len).unwrap()),
                "round {round}, op {op}"
            );
            assert_eq!(intake.is_empty(), model.is_empty());
        }
    }
}

#[test]
#[should_panic(expected = "a fill past the intake's cap would never be met")]
fn a_fill_past_the_cap_is_a_bug() {
    let mut intake = Intake::with_capacity(4);
    drop(intake.meet(Read::Fill(5)));
}

#[test]
#[should_panic(expected = "a scan past the intake's cap would never be met")]
fn a_scan_past_the_cap_is_a_bug() {
    let mut intake = Intake::with_capacity(4);
    drop(intake.meet(scan(Delimiter::LF, 5)));
}

#[test]
#[should_panic(expected = "a scan holds its delimiter")]
fn a_scan_shorter_than_its_delimiter_is_a_bug() {
    let mut intake = Intake::with_capacity(4);
    drop(intake.meet(scan(Delimiter::CRLF, 1)));
}
