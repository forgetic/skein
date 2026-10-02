//! Bounds-checked reading (programming-style.md, 3.3 and section 7): the
//! bytes a stream delivered, decoded without trusting any length in them.

/// A cursor over bytes, read from the front.
///
/// Every read returns `Option`, and a read that fails consumes nothing: a
/// short message is a framing error for the caller to handle, never a panic.
/// Integers are big-endian: byte order is chosen, never the host's. Counts
/// are `u32`, as every length a demand delivers is, so a reader is over at
/// most `u32::MAX` bytes.
#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    use super::Reader;

    #[test]
    fn integers_are_big_endian() {
        let mut reader = Reader::new(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(reader.u8(), Some(1));
        assert_eq!(reader.u16(), Some(0x0203));
        assert_eq!(reader.u32(), Some(0x0405_0607));
        assert_eq!(reader.u64(), Some(0x0809_0A0B_0C0D_0E0F));
        assert!(reader.is_empty());
        assert_eq!(reader.u8(), None);
    }

    #[test]
    fn a_short_read_consumes_nothing() {
        let mut reader = Reader::new(&[1, 2, 3]);
        assert_eq!(reader.u64(), None);
        assert_eq!(reader.u32(), None);
        assert_eq!(reader.bytes(4), None);
        assert_eq!(reader.skip(4), None);
        assert_eq!(reader.remaining(), 3);
        assert_eq!(reader.u16(), Some(0x0102));
        assert_eq!(reader.u16(), None);
        assert_eq!(reader.remaining(), 1);
        assert_eq!(reader.u8(), Some(3));
    }

    #[test]
    fn bytes_and_skip_move_the_cursor() {
        let input = *b"head:body";
        let mut reader = Reader::new(&input);
        assert_eq!(reader.bytes(4), Some(&b"head"[..]));
        assert_eq!(reader.skip(1), Some(()));
        assert_eq!(reader.bytes(0), Some(&b""[..]));
        assert_eq!(reader.bytes(4), Some(&b"body"[..]));
        assert!(reader.is_empty());
    }

    /// The bytes borrow what the reader is over, not the reader, so they
    /// outlive it.
    fn second_word(input: &[u8]) -> Option<&[u8]> {
        let mut reader = Reader::new(input);
        reader.skip(5)?;
        reader.bytes(4)
    }

    #[test]
    fn bytes_outlive_their_reader() {
        assert_eq!(second_word(b"head:body"), Some(&b"body"[..]));
        assert_eq!(second_word(b"head:bo"), None);
    }

    #[test]
    fn an_empty_reader_reads_nothing_but_nothing() {
        let mut reader = Reader::new(&[]);
        assert_eq!((reader.remaining(), reader.is_empty()), (0, true));
        assert_eq!(reader.u8(), None);
        assert_eq!(reader.bytes(0), Some(&b""[..]));
        assert_eq!(reader.skip(0), Some(()));
        assert_eq!(reader.skip(u32::MAX), None);
    }
}
