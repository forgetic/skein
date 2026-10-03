//! The byte search's naive counterparts (lib.md, 6): every position
//! compared, and random haystacks and needles of few letters.

use skein_lib::Rng;

/// The first occurrence at or after `from`, comparing at every position.
#[must_use]
pub fn naive(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    let last = haystack.len().checked_sub(needle.len())?;
    for at in from..=last {
        if haystack[at..].starts_with(needle) {
            return Some(at);
        }
    }
    None
}

/// How many times `needle` occurs in `haystack`, without overlapping, up
/// to `cap`, found with [`naive`].
#[must_use]
pub fn naive_count(haystack: &[u8], needle: &[u8], cap: u32) -> u32 {
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
pub fn up_to(rng: &mut Rng, max: usize) -> usize {
    usize::try_from(rng.between(0, u64::try_from(max).expect("fits"))).expect("fits")
}

/// Fills `text` with letters from the first `letters` of the alphabet: few
/// letters make many partial matches.
pub fn fill(rng: &mut Rng, text: &mut [u8], letters: u64) {
    for byte in text {
        *byte = b'a'.checked_add(u8::try_from(rng.below(letters)).expect("a few")).expect("a letter");
    }
}
