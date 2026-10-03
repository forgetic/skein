//! Sized writing (programming-model.md, 6.2 and section 8): bytes built into a
//! box of a length computed first.

#![expect(clippy::disallowed_types, reason = "a writer fills a Vec allocated once, at its final length")]

use alloc::boxed::Box;
use alloc::vec::Vec;

/// Builds a `Box<[u8]>` of a length its caller computed first, from slices
/// put one after another: an encoded message, a prompt rendered from
/// fragments, a file with one span replaced.
///
/// The box is allocated once, at that length, and never grows; its bytes are
/// the caller's to count, as any payload's. A put that does not fit what is
/// left is refused whole and writes nothing. The length is the caller's own
/// (programming-model.md, section 8): a caller that computed it expects every
/// put to fit, and finishing short is a bug, which [`Writer::finish`] asserts.
#[derive(Debug)]
pub struct Writer {
    bytes: Vec<u8>,
    len: usize,
}

/// What a [`Writer`] or an [`Intake`](crate::Intake) refuses: bytes past the
/// length or the cap it was made for. Nothing was written.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Overflow;

impl Writer {
    /// A writer of exactly `len` bytes.
    #[must_use]
    pub fn new(len: usize) -> Writer {
        Writer { bytes: Vec::with_capacity(len), len }
    }

    /// The bytes put so far.
    #[must_use]
    pub fn written(&self) -> usize {
        self.bytes.len()
    }

    /// How many more bytes fit.
    #[must_use]
    pub fn room(&self) -> usize {
        self.len.checked_sub(self.bytes.len()).expect("a writer never passes its length")
    }

    /// Appends `bytes`, or refuses them whole when they do not fit.
    pub fn put(&mut self, bytes: &[u8]) -> Result<(), Overflow> {
        if bytes.len() > self.room() {
            return Err(Overflow);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    /// The bytes written, in a box of exactly the length the writer was made
    /// for, which they must fill.
    #[must_use]
    pub fn finish(self) -> Box<[u8]> {
        assert!(self.bytes.len() == self.len, "a writer is finished full");
        self.bytes.into_boxed_slice()
    }
}
