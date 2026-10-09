//! The carry-over of a stream (programming-model.md, 4.3; lib.md,
//! 7): bytes received but not yet demanded, held by the side below
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
    until: Until,
    clear: u32,
}

/// What a scan ends at.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Until {
    /// A delimiter's bytes, in order.
    Delimiter(Delimiter),
    /// A line's end: a CR or an LF, whichever comes first.
    LineEnd,
}

impl Until {
    fn len(self) -> usize {
        match self {
            Until::Delimiter(delimiter) => delimiter.as_bytes().len(),
            Until::LineEnd => 1,
        }
    }
}

impl Intake {
    #[must_use]
    pub fn with_capacity(capacity: u32) -> Intake {
        Intake {
            bytes: VecDeque::with_capacity(index(capacity)),
            capacity,
            searched: Searched { until: Until::LineEnd, clear: 0 },
        }
    }

    /// The heap an intake of `capacity` takes: its buffer, so always
    /// `Some(capacity)`, an `Option` to match the other containers. The boxes
    /// it delivers are their new owners' to count.
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

    /// Drops buffered bytes while keeping the fixed capacity; resets scan progress.
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.searched.clear = 0;
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
    /// - `Line { max }`: the same, to the first CR or LF;
    /// - `Nothing`: nothing.
    ///
    /// A demand the cap can never meet, a fill or a scan past the capacity,
    /// is the caller's bug, as is a scan too short to hold its delimiter.
    pub fn meet(&mut self, read: Read) -> Option<Box<[u8]>> {
        let (until, max) = match read {
            Read::Nothing => return None,
            Read::Fill(n) => {
                assert!(n <= self.capacity, "a fill past the intake's cap would never be met");
                if self.len() < n {
                    return None;
                }
                return Some(self.take(n));
            }
            Read::Scan { until, max } => (Until::Delimiter(until), max),
            Read::Line { max } => (Until::LineEnd, max),
        };
        assert!(max <= self.capacity, "a scan past the intake's cap would never be met");
        assert!(index(max) >= until.len(), "a scan holds its delimiter");
        let n = self.scan(until, max)?;
        Some(self.take(n))
    }

    /// Whether what is buffered ends partway through `until`: with its first
    /// byte, or its first bytes, but not all of them. A side below that
    /// fills its own intake from another stream reads one byte at a time
    /// while it does, so that it never reads past a delimiter that its next
    /// bytes complete.
    #[must_use]
    pub fn ends_partway(&self, until: Delimiter) -> bool {
        let needle = until.as_bytes();
        let len = self.bytes.len();
        // Bounded by the delimiter's length, at most four.
        for part in 1..needle.len() {
            let Some(start) = len.checked_sub(part) else { break };
            if self.occurs_at(needle.get(..part).expect("a part of the delimiter"), start) {
                return true;
            }
        }
        false
    }

    /// How many bytes a scan for `until` within `max` delivers, if it can be
    /// met yet.
    fn scan(&mut self, until: Until, max: u32) -> Option<u32> {
        let len = until.len();
        if self.searched.until != until {
            self.searched = Searched { until, clear: 0 };
        }
        let end = self.len().min(max);
        // The last position at which a delimiter can begin and end by `end`.
        let last = index(end).checked_sub(len)?;
        let first = index(self.searched.clear);
        // Bounded by `max`: each position is searched once per delimiter, as
        // `clear` moves past it.
        for at in first..=last {
            let found = match until {
                Until::Delimiter(delimiter) => self.occurs_at(delimiter.as_bytes(), at),
                Until::LineEnd => match self.bytes.get(at) {
                    Some(b'\r' | b'\n') => true,
                    Some(_) | None => false,
                },
            };
            if found {
                return Some(count(at.checked_add(len).expect("within the buffer")));
            }
        }
        self.searched.clear = count(last.checked_add(1).expect("within the buffer")).max(self.searched.clear);
        if end == max {
            return Some(max);
        }
        None
    }

    /// Whether `needle` occurs in the buffer at `at`. Not `bytes::find_from`,
    /// which wants one slice: the deque may wrap, and a needle is at most four
    /// bytes.
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
