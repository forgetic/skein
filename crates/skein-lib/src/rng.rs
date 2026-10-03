//! Randomness is injected state (programming-model.md, section 9).

/// A deterministic pseudo-random generator (`SplitMix64`), seeded by the shell or
/// by the simulator and kept in its layer's state. Not for secrets.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    #[must_use]
    pub const fn new(seed: u64) -> Rng {
        Rng { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ z.wrapping_shr(31)
    }

    /// A number in `0..bound`, or 0 when `bound` is 0. The bias is at most
    /// `bound / 2^64`, which no use here can observe.
    pub fn below(&mut self, bound: u64) -> u64 {
        let wide = u128::from(self.next_u64()).wrapping_mul(u128::from(bound));
        u64::try_from(wide.wrapping_shr(64)).expect("the high half of a u128 fits in a u64")
    }

    /// A number in `low..=high`, or `low` when `high < low`.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        let Some(span) = high.checked_sub(low) else {
            return low;
        };
        let offset = match span.checked_add(1) {
            Some(count) => self.below(count),
            None => self.next_u64(),
        };
        low.checked_add(offset).expect("low + offset <= high")
    }

    /// True `per_mille` times in a thousand.
    pub fn chance(&mut self, per_mille: u32) -> bool {
        self.below(1000) < u64::from(per_mille)
    }
}

#[cfg(test)]
mod tests {
    use super::Rng;

    #[test]
    fn the_same_seed_gives_the_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0_u32..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn draws_stay_in_range() {
        let mut rng = Rng::new(7);
        for _ in 0_u32..1000 {
            assert!(rng.below(10) < 10);
            let n = rng.between(5, 9);
            assert!((5_u64..=9).contains(&n));
        }
        assert_eq!(rng.below(0), 0);
        assert_eq!(rng.between(9, 5), 9_u64);
        let _: u64 = rng.between(0, u64::MAX);
        assert!(!rng.chance(0));
        assert!(rng.chance(1000));
    }
}
