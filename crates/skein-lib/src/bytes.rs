//! Owned bytes (5.1): a `Box<[u8]>` allocated at its final length, moved from
//! owner to owner, never shared; and searching them.

use alloc::boxed::Box;
use core::cmp::Ordering;

/// A copy of `bytes` in a box of exactly their length.
///
/// This is "copy at emission" (5.3): data a layer keeps and also sends goes out
/// as a copy made when the request is emitted.
#[must_use]
pub fn copy_of(bytes: &[u8]) -> Box<[u8]> {
    Box::from(bytes)
}

/// Where `needle` first occurs in `haystack`, or `None`. An empty needle
/// occurs at 0.
///
/// The search takes time linear in both lengths and allocates nothing, so a
/// whole file can be searched for a long snippet.
#[must_use]
pub fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    find_from(haystack, needle, 0)
}

/// Where `needle` first occurs in `haystack` at or after `from`, or `None`, as
/// when `from` is past the end. An empty needle occurs at `from`.
///
/// The next occurrence that does not overlap one at `at` is
/// `find_from(haystack, needle, at + needle.len())`.
#[must_use]
pub fn find_from(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if from > haystack.len() {
        return None;
    }
    if needle.is_empty() {
        return Some(from);
    }
    TwoWay::of(needle).find(haystack, needle, from)
}

/// How many times `needle` occurs in `haystack` without overlapping, counting
/// from the start, as a run of [`find_from`]s past each match would, and
/// stopping at `cap`: "none, one, or more" is `count(haystack, needle, 2)`.
///
/// An empty needle occurs at every position, `haystack.len() + 1` times.
#[must_use]
pub fn count(haystack: &[u8], needle: &[u8], cap: u32) -> u32 {
    if needle.is_empty() {
        let positions = u32::try_from(haystack.len()).unwrap_or(u32::MAX).saturating_add(1);
        return positions.min(cap);
    }
    let needle_split = TwoWay::of(needle);
    let mut found = 0;
    let mut from = 0;
    // Bounded by the cap, and by the haystack: each match moves past itself.
    while found < cap {
        let Some(at) = needle_split.find(haystack, needle, from) else {
            break;
        };
        found = add32(found, 1);
        from = add(at, needle.len());
    }
    found
}

/// A needle split for the two-way search (Crochemore and Perrin, 1991), which
/// runs in linear time and constant space.
///
/// The needle is split at a critical position, found from its greatest
/// suffixes. A search compares the right part left to right, then the left
/// part right to left. A mismatch in the right part shifts the right part past
/// the mismatched byte; one in the left part shifts the needle by its period.
#[derive(Debug)]
struct TwoWay {
    /// Where the left part ends and the right part begins.
    split: usize,
    /// How far a mismatch in the left part shifts the needle.
    period: usize,
    /// Whether `period` is the needle's period, rather than a lower bound on
    /// it. After a shift by an exact period, the needle's first
    /// `len - period` bytes are known to match, and are not compared again.
    exact: bool,
}

impl TwoWay {
    /// The split of a needle that is not empty.
    fn of(needle: &[u8]) -> TwoWay {
        let (ascending, ascending_period) = greatest_suffix(needle, false);
        let (descending, descending_period) = greatest_suffix(needle, true);
        let (split, period) =
            if ascending > descending { (ascending, ascending_period) } else { (descending, descending_period) };
        // The right part's period is the whole needle's when the left part
        // recurs one period on.
        let left = needle.get(..split).expect("the split is within the needle");
        let repeated = match period.checked_add(split) {
            Some(end) => needle.get(period..end),
            None => None,
        };
        if repeated == Some(left) {
            return TwoWay { split, period, exact: true };
        }
        let right = sub(needle.len(), split);
        TwoWay { split, period: add(split.max(right), 1), exact: false }
    }

    /// Where `needle`, which this splits, first occurs in `haystack` at or
    /// after `from`.
    fn find(&self, haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
        let mut position = from;
        // How many of the needle's first bytes are known to match at
        // `position`: only ever more than none after an exact period's shift.
        let mut known = 0;
        // Bounded by the haystack: every round that does not return shifts the
        // needle on, and the window runs out at its end.
        while let Some(window) = window(haystack, position, needle.len()) {
            let start = self.split.max(known);
            if let Some(mismatch) = mismatch(needle, window, start) {
                position = add(position, add(sub(mismatch, self.split), 1));
                known = 0;
                continue;
            }
            let stop = known.min(self.split);
            if !agree_backwards(needle, window, stop, self.split) {
                position = add(position, self.period);
                if self.exact {
                    known = sub(needle.len(), self.period);
                }
                continue;
            }
            return Some(position);
        }
        None
    }
}

/// Where the greatest suffix of `needle` begins, under the byte order or, when
/// `reversed`, under its reverse, and that suffix's period.
fn greatest_suffix(needle: &[u8], reversed: bool) -> (usize, usize) {
    // The greatest suffix so far begins at `start`, with period `period`; the
    // one at `candidate` agrees with it for `offset` bytes.
    let mut start = 0;
    let mut candidate = 1;
    let mut offset = 0;
    let mut period = 1;
    // Bounded by three times the needle's length: every round raises
    // `start + candidate + offset`, which stays below it.
    while let Some(&next) = needle.get(add(candidate, offset)) {
        let &known = needle.get(add(start, offset)).expect("start is before candidate");
        let order = if reversed { known.cmp(&next) } else { next.cmp(&known) };
        match order {
            // The candidate is smaller: the period of the greatest suffix
            // runs at least to here.
            Ordering::Less => {
                candidate = add(candidate, add(offset, 1));
                offset = 0;
                period = sub(candidate, start);
            }
            Ordering::Equal => {
                if add(offset, 1) == period {
                    candidate = add(candidate, period);
                    offset = 0;
                } else {
                    offset = add(offset, 1);
                }
            }
            // The candidate is greater: it is the greatest suffix so far.
            Ordering::Greater => {
                start = candidate;
                candidate = add(candidate, 1);
                offset = 0;
                period = 1;
            }
        }
    }
    (start, period)
}

/// The `len` bytes of `haystack` at `position`, if it has that many.
fn window(haystack: &[u8], position: usize, len: usize) -> Option<&[u8]> {
    haystack.get(position..position.checked_add(len)?)
}

/// The first index from `start` on at which `needle` and `window`, of the
/// same length, differ.
fn mismatch(needle: &[u8], window: &[u8], start: usize) -> Option<usize> {
    let needle_part = needle.get(start..).expect("the start is within the needle");
    let window_part = window.get(start..).expect("the window is the needle's length");
    for (offset, (expected, found)) in needle_part.iter().zip(window_part).enumerate() {
        if expected != found {
            return Some(add(start, offset));
        }
    }
    None
}

/// Whether `needle` and `window` agree on `start..end`, compared from the end.
fn agree_backwards(needle: &[u8], window: &[u8], start: usize, end: usize) -> bool {
    let needle_part = needle.get(start..end).expect("the range is within the needle");
    let window_part = window.get(start..end).expect("the window is the needle's length");
    for (expected, found) in needle_part.iter().zip(window_part).rev() {
        if expected != found {
            return false;
        }
    }
    true
}

/// Index arithmetic within a slice, which cannot overflow.
fn add(a: usize, b: usize) -> usize {
    a.checked_add(b).expect("an index within a slice fits a usize")
}

fn sub(a: usize, b: usize) -> usize {
    a.checked_sub(b).expect("an index within a slice is not negative")
}

fn add32(a: u32, b: u32) -> u32 {
    a.checked_add(b).expect("a count below its cap fits a u32")
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use super::{add, add32, count, find, find_from};
    use crate::{List, Rng};

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
            found = add32(found, 1);
            from = add(at, needle.len().max(1));
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
                &haystack[start..add(start, 1 + up_to(&mut rng, haystack.len() - start - 1))]
            } else {
                let len = up_to(&mut rng, needle_buffer.len());
                fill(&mut rng, &mut needle_buffer[..len], letters);
                &needle_buffer[..len]
            };
            for from in 0..=add(haystack.len(), 1) {
                assert_eq!(find_from(haystack, needle, from), naive(haystack, needle, from), "round {round}");
            }
            for cap in [0, 1, 2, 3, u32::MAX] {
                assert_eq!(count(haystack, needle, cap), naive_count(haystack, needle, cap), "round {round}");
            }
        }
    }
}
