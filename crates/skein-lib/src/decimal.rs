//! Counts written as text (programming-model.md, section 8): the decimal digits
//! of a number, for a line a step writes into its own bytes without formatting.

/// The decimal digits of a `u64`, without leading zeros, `0` for zero: what a
/// step puts into a [`Writer`](crate::Writer) to write a count, after
/// measuring the text by their length.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Decimal {
    /// The digits, right-aligned, with zeros before the first.
    digits: [u8; 20],
    /// Where the first digit is.
    first: usize,
}

impl Decimal {
    /// `n` in decimal digits.
    #[must_use]
    pub fn of(n: u64) -> Decimal {
        let mut digits = [b'0'; 20];
        let mut rest = n;
        // The leftmost digit that is not a leading zero; the last, for zero.
        let mut first = 19;
        for (index, digit) in digits.iter_mut().enumerate().rev() {
            let value = u8::try_from(rest.checked_rem(10).unwrap_or(0)).expect("a digit fits in a byte");
            *digit = b'0'.saturating_add(value);
            if value != 0 {
                first = index;
            }
            rest = rest.checked_div(10).unwrap_or(0);
        }
        Decimal { digits, first }
    }

    /// The digits, most significant first.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.digits.get(self.first..).expect("the first digit is within the digits")
    }
}

#[cfg(test)]
mod tests {
    use super::Decimal;
    use crate::{Rng, Writer};

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
}
