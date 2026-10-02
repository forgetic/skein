//! The carry-over of a stream (programming-style.md, 3.3; overview.md,
//! section 4): bytes received but not yet demanded, held by the side below
//! under its cap.

#![expect(
    clippy::disallowed_types,
    reason = "an intake is a VecDeque allocated once, at its cap, and a delivery is built in a Vec of its exact length"
)]

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;

use crate::Overflow;
use crate::stream::{Delimiter, Read};

/// The bytes a stream has received and the side above has not yet demanded,
/// up to a cap fixed when it is made.
///
/// The side below appends what arrives while there is [`room`](Intake::room),
/// and meets the side above's [`Read`] with [`meet`](Intake::meet) as soon as
/// it can. The buffer is allocated once, at the cap, and never grows; each
/// delivery is a box of exactly the demanded length.
#[derive(Debug)]
pub struct Intake {
    bytes: VecDeque<u8>,
    capacity: u32,
    /// How far the last scan has searched, so that the next one for the same
    /// delimiter does not search the same bytes again.
    searched: Searched,
}

/// No occurrence of `until` begins in the first `clear` bytes buffered.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Searched {
    until: Delimiter,
    clear: u32,
}

impl Intake {
    #[must_use]
    pub fn with_capacity(capacity: u32) -> Intake {
        Intake {
            bytes: VecDeque::with_capacity(index(capacity)),
            capacity,
            searched: Searched { until: Delimiter::LF, clear: 0 },
        }
    }

    /// The heap an intake of `capacity` takes: its buffer, or `None` past a
    /// `u64`. The boxes it delivers are their new owners' to count.
    #[must_use]
    pub fn worst_case(capacity: u32) -> Option<u64> {
        Some(u64::from(capacity))
    }

    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    /// The bytes buffered.
    #[must_use]
    pub fn len(&self) -> u32 {
        u32::try_from(self.bytes.len()).expect("no longer than its capacity")
    }

    /// Whether nothing is buffered: at the end of the stream, whether the
    /// peer stopped between messages or within one.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// How many more bytes can be appended. Receiving stops at zero.
    #[must_use]
    pub fn room(&self) -> u32 {
        self.capacity.checked_sub(self.len()).expect("no longer than its capacity")
    }

    /// Appends received bytes, or refuses them whole when they do not fit,
    /// appending nothing.
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), Overflow> {
        if bytes.len() > index(self.room()) {
            return Err(Overflow);
        }
        self.bytes.extend(bytes);
        Ok(())
    }

    /// The bytes `read` demands, taken from the front in a box of exactly
    /// their length, or `None` until enough are buffered:
    ///
    /// - `Fill(n)`: exactly `n` bytes, once `n` are buffered;
    /// - `Scan { until, max }`: the bytes up to and including the first
    ///   `until`, if it ends within the first `max`; otherwise, once `max`
    ///   are buffered, exactly `max` bytes, which do not end with `until`;
    /// - `Nothing`: nothing.
    ///
    /// A demand the cap can never meet, a fill or a scan past the capacity,
    /// is the caller's bug, as is a scan too short to hold its delimiter.
    pub fn meet(&mut self, read: Read) -> Option<Box<[u8]>> {
        match read {
            Read::Nothing => None,
            Read::Fill(n) => {
                assert!(n <= self.capacity, "a fill past the intake's cap would never be met");
                if self.len() < n {
                    return None;
                }
                Some(self.take(n))
            }
            Read::Scan { until, max } => {
                assert!(max <= self.capacity, "a scan past the intake's cap would never be met");
                assert!(index(max) >= until.as_bytes().len(), "a scan holds its delimiter");
                let n = self.scan(until, max)?;
                Some(self.take(n))
            }
        }
    }

    /// How many bytes a scan for `until` within `max` delivers, if it can be
    /// met yet.
    fn scan(&mut self, until: Delimiter, max: u32) -> Option<u32> {
        let needle = until.as_bytes();
        if self.searched.until != until {
            self.searched = Searched { until, clear: 0 };
        }
        let end = self.len().min(max);
        // The last position at which a delimiter can begin and end by `end`.
        let last = index(end).checked_sub(needle.len())?;
        let first = index(self.searched.clear);
        // Bounded by `max`: each position is searched once per delimiter, as
        // `clear` moves past it.
        for at in first..=last {
            if self.occurs_at(needle, at) {
                return Some(count(at.checked_add(needle.len()).expect("within the buffer")));
            }
        }
        self.searched.clear = count(last.checked_add(1).expect("within the buffer")).max(self.searched.clear);
        if end == max {
            return Some(max);
        }
        None
    }

    /// Whether `needle` occurs in the buffer at `at`.
    fn occurs_at(&self, needle: &[u8], at: usize) -> bool {
        for (offset, expected) in needle.iter().enumerate() {
            let position = at.checked_add(offset).expect("within the buffer");
            if self.bytes.get(position) != Some(expected) {
                return false;
            }
        }
        true
    }

    /// The first `n` bytes buffered, which there are, moved into a box of
    /// exactly their length.
    fn take(&mut self, n: u32) -> Box<[u8]> {
        let len = index(n);
        let (front, back) = self.bytes.as_slices();
        let from_front = front.len().min(len);
        let from_back = len.checked_sub(from_front).expect("no more than the front holds");
        let mut taken = Vec::with_capacity(len);
        taken.extend_from_slice(front.get(..from_front).expect("within the front"));
        taken.extend_from_slice(back.get(..from_back).expect("the rest is buffered"));
        drop(self.bytes.drain(..len));
        self.searched.clear = self.searched.clear.saturating_sub(n);
        taken.into_boxed_slice()
    }
}

fn index(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits in a usize")
}

fn count(n: usize) -> u32 {
    u32::try_from(n).expect("a position in the buffer fits its u32 capacity")
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use alloc::vec::Vec;

    use super::Intake;
    use crate::stream::{Delimiter, Read};
    use crate::{Overflow, Rng};

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
        for _ in 0_u32..20_000 {
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
}
