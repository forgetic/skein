//! The byte vocabulary every stream boundary shares (programming-model.md,
//! 4.3; lib.md, 7): reading is a demand, writing is a move.
//!
//! The side above says what it needs with a [`Down::Demand`] and moves its
//! output down in [`Down::Send`]; the side below answers with [`Up`]. The side
//! below holds the bytes received but not yet demanded, in an
//! [`Intake`](crate::Intake).

use alloc::boxed::Box;

/// What a state needs from the stream below before it can go on.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Read {
    /// Nothing: no `Bytes` are delivered.
    Nothing,
    /// Exactly this many bytes. The count is at most the cap of the side
    /// below; a larger one could never be met, and is the caller's bug,
    /// asserted below.
    Fill(u32),
    /// The bytes up to and including the first `until`, if it ends within
    /// the first `max` bytes; otherwise exactly `max` bytes, which the side
    /// above, seeing no delimiter at their end, treats as a framing error.
    /// `max` is at least the delimiter's length and at most the cap of the
    /// side below; breaking either is the caller's bug, asserted below.
    Scan { until: Delimiter, max: u32 },
}

/// To the side below.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Down {
    /// What this state needs, and the output room it wants.
    Demand { read: Read, room: u32 },
    /// Bytes moved down, within the room granted.
    Send(Box<[u8]>),
    /// Nothing more to send: flush, then end the stream.
    Finish,
}

/// From the side below.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Up {
    /// Exactly the demand.
    Bytes(Box<[u8]>),
    /// The room asked for is free.
    Room,
    /// The other side will send nothing more.
    End,
    /// The stream is broken: no more `Bytes` or `Room` follow.
    Failed(Fault),
}

/// Why a stream broke, as far as the side above can act on it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Fault {
    /// The peer reset the stream: it is gone, and what was in flight with it.
    Reset,
    /// The side below could not make sense of the peer's data, as when a TLS
    /// record fails to decrypt: the peer is broken or hostile.
    Invalid,
    /// Anything else: the side below failed for a reason of its own, such as a
    /// kernel error io does not name.
    Other,
}

/// The bytes a scan ends at: one to four of them, held inline so that a
/// demand is a plain value (programming-model.md, 4.4).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Delimiter {
    /// The delimiter's bytes, then zeros.
    bytes: [u8; 4],
    len: u8,
}

impl Delimiter {
    /// A line feed, `\n`.
    pub const LF: Delimiter = Delimiter { bytes: *b"\n\0\0\0", len: 1 };
    /// A carriage return and line feed, `\r\n`: the end of a line in HTTP.
    pub const CRLF: Delimiter = Delimiter { bytes: *b"\r\n\0\0", len: 2 };
    /// Two of them, `\r\n\r\n`: the end of an HTTP head.
    pub const CRLF_CRLF: Delimiter = Delimiter { bytes: *b"\r\n\r\n", len: 4 };

    /// The delimiter `bytes`, or `None` unless there are one to four.
    #[must_use]
    pub const fn new(bytes: &[u8]) -> Option<Delimiter> {
        match *bytes {
            [a] => Some(Delimiter { bytes: [a, 0, 0, 0], len: 1 }),
            [a, b] => Some(Delimiter { bytes: [a, b, 0, 0], len: 2 }),
            [a, b, c] => Some(Delimiter { bytes: [a, b, c, 0], len: 3 }),
            [a, b, c, d] => Some(Delimiter { bytes: [a, b, c, d], len: 4 }),
            [] | [_, _, _, _, _, ..] => None,
        }
    }

    /// The delimiter's one to four bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..usize::from(self.len)).expect("a delimiter is at most four bytes")
    }
}
