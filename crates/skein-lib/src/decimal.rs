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
