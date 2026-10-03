//! Owned bytes (programming-model.md, 6.1): a `Box<[u8]>` allocated at its
//! final length, moved from owner to owner, never shared; and searching them.

use alloc::boxed::Box;
use core::cmp::Ordering;

/// A copy of `bytes` in a box of exactly their length.
///
/// This is "copy at emission" (programming-model.md, 6.2): data a layer keeps
/// and also sends goes out as a copy made when the request is emitted.
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
