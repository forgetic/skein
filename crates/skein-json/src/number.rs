//! A number's text, checked a byte at a time against JSON's grammar
//! (RFC 8259, section 6):
//!
//! ```text
//! number = [ "-" ] ( "0" / 1-9 *DIGIT ) [ "." 1*DIGIT ] [ ( "e" / "E" ) [ "+" / "-" ] 1*DIGIT ]
//! ```
//!
//! No number is converted: the tokenizer sends its text up, and the writer
//! writes the text it is given.

/// Where a number's text is in the grammar, after the bytes read so far.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Number {
    /// `-`: a digit comes next.
    Minus,
    /// A leading `0`, which no digit may follow.
    Zero,
    /// The digits of the integer part, the first not `0`.
    Integer,
    /// `.`: a digit comes next.
    Point,
    /// The digits of the fraction.
    Fraction,
    /// `e` or `E`: a sign or a digit comes next.
    Exponent,
    /// The exponent's sign: a digit comes next.
    Sign,
    /// The digits of the exponent.
    Power,
}

/// What a byte does to a number.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum After {
    /// It continues the number.
    More(Number),
    /// It is one of a number's bytes (a digit, `.`, `e`, `E`, `+` or `-`)
    /// where the grammar allows none of them: `01`, `1.e`, `1-`, `--1`.
    Invalid,
    /// It is no number's byte, so the number ended before it.
    Ended,
}

impl Number {
    /// The state after a number's first byte, or `None` when no number
    /// starts with it.
    #[must_use]
    pub(crate) fn start(byte: u8) -> Option<Number> {
        match byte {
            b'-' => Some(Number::Minus),
            b'0' => Some(Number::Zero),
            b'1'..=b'9' => Some(Number::Integer),
            _ => None,
        }
    }

    /// What `byte` does to a number in this state.
    #[must_use]
    pub(crate) fn next(self, byte: u8) -> After {
        let class = Class::of(byte);
        match self {
            Number::Minus => match class {
                Class::Zero => After::More(Number::Zero),
                Class::Digit => After::More(Number::Integer),
                Class::Point | Class::Exponent | Class::Sign => After::Invalid,
                Class::Other => After::Ended,
            },
            Number::Zero => match class {
                Class::Point => After::More(Number::Point),
                Class::Exponent => After::More(Number::Exponent),
                Class::Zero | Class::Digit | Class::Sign => After::Invalid,
                Class::Other => After::Ended,
            },
            Number::Integer => match class {
                Class::Zero | Class::Digit => After::More(Number::Integer),
                Class::Point => After::More(Number::Point),
                Class::Exponent => After::More(Number::Exponent),
                Class::Sign => After::Invalid,
                Class::Other => After::Ended,
            },
            Number::Point => match class {
                Class::Zero | Class::Digit => After::More(Number::Fraction),
                Class::Point | Class::Exponent | Class::Sign => After::Invalid,
                Class::Other => After::Ended,
            },
            Number::Fraction => match class {
                Class::Zero | Class::Digit => After::More(Number::Fraction),
                Class::Exponent => After::More(Number::Exponent),
                Class::Point | Class::Sign => After::Invalid,
                Class::Other => After::Ended,
            },
            Number::Exponent => match class {
                Class::Sign => After::More(Number::Sign),
                Class::Zero | Class::Digit => After::More(Number::Power),
                Class::Point | Class::Exponent => After::Invalid,
                Class::Other => After::Ended,
            },
            Number::Sign | Number::Power => match class {
                Class::Zero | Class::Digit => After::More(Number::Power),
                Class::Point | Class::Exponent | Class::Sign => After::Invalid,
                Class::Other => After::Ended,
            },
        }
    }

    /// Whether the number may end here.
    #[must_use]
    pub(crate) fn is_complete(self) -> bool {
        match self {
            Number::Zero | Number::Integer | Number::Fraction | Number::Power => true,
            Number::Minus | Number::Point | Number::Exponent | Number::Sign => false,
        }
    }
}

/// Whether `text` is a number, whole.
#[must_use]
pub(crate) fn is_number(text: &[u8]) -> bool {
    let Some((&first, rest)) = text.split_first() else {
        return false;
    };
    let Some(mut state) = Number::start(first) else {
        return false;
    };
    for &byte in rest {
        match state.next(byte) {
            After::More(next) => state = next,
            After::Invalid | After::Ended => return false,
        }
    }
    state.is_complete()
}

/// The bytes a number is made of, by what they do in the grammar.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Class {
    Zero,
    /// `1` to `9`.
    Digit,
    Point,
    /// `e` or `E`.
    Exponent,
    /// `+` or `-`.
    Sign,
    /// Every other byte: none of a number's.
    Other,
}

impl Class {
    fn of(byte: u8) -> Class {
        match byte {
            b'0' => Class::Zero,
            b'1'..=b'9' => Class::Digit,
            b'.' => Class::Point,
            b'e' | b'E' => Class::Exponent,
            b'+' | b'-' => Class::Sign,
            _ => Class::Other,
        }
    }
}
