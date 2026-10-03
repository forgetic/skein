//! Bounds-checked reading (programming-model.md, 4.3 and section 8): the
//! bytes a stream delivered, decoded without trusting any length in them.

/// A cursor over bytes, read from the front.
///
/// Every read returns `Option`, and a read that fails consumes nothing: a
/// short message is a framing error for the caller to handle, never a panic.
/// Integers are big-endian: byte order is chosen, never the host's. Counts
/// are `u32`, as every length a demand delivers is, so a reader is over at
/// most `u32::MAX` bytes.
#[derive(Clone, Debug)]
pub struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    /// A reader over `bytes`, which are at most `u32::MAX` long.
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Reader<'a> {
        assert!(u32::try_from(bytes.len()).is_ok(), "a reader is over at most u32::MAX bytes");
        Reader { rest: bytes }
    }

    /// The bytes not yet read.
    #[must_use]
    pub fn remaining(&self) -> u32 {
        u32::try_from(self.rest.len()).expect("a reader is over at most u32::MAX bytes")
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }

    pub fn u8(&mut self) -> Option<u8> {
        let (&byte, rest) = self.rest.split_first()?;
        self.rest = rest;
        Some(byte)
    }

    /// A big-endian `u16`.
    pub fn u16(&mut self) -> Option<u16> {
        let (bytes, rest) = self.rest.split_first_chunk::<2>()?;
        self.rest = rest;
        Some(u16::from_be_bytes(*bytes))
    }

    /// A big-endian `u32`.
    pub fn u32(&mut self) -> Option<u32> {
        let (bytes, rest) = self.rest.split_first_chunk::<4>()?;
        self.rest = rest;
        Some(u32::from_be_bytes(*bytes))
    }

    /// A big-endian `u64`.
    pub fn u64(&mut self) -> Option<u64> {
        let (bytes, rest) = self.rest.split_first_chunk::<8>()?;
        self.rest = rest;
        Some(u64::from_be_bytes(*bytes))
    }

    /// The next `n` bytes, borrowed from what the reader is over.
    pub fn bytes(&mut self, n: u32) -> Option<&'a [u8]> {
        let (bytes, rest) = self.rest.split_at_checked(usize::try_from(n).ok()?)?;
        self.rest = rest;
        Some(bytes)
    }

    /// Passes over the next `n` bytes, or over none when there are fewer.
    pub fn skip(&mut self, n: u32) -> Option<()> {
        let _: &[u8] = self.bytes(n)?;
        Some(())
    }
}
