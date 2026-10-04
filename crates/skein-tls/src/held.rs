//! Bytes held in one slice, for rustls, which deciphers and joins the
//! records it reads in place, and writes its own output into a slice it is
//! given (tls.md, 3).

use alloc::boxed::Box;

use skein_lib::{Overflow, Writer, bytes};

/// Bytes held at the front of one box, allocated once at a capacity fixed
/// when it is made: the ciphertext received and not yet discarded, or the
/// output TLS owes the stream below.
///
/// Not lib's `Intake`, whose deque may wrap: rustls reads and writes one
/// slice. What is taken from the front moves the rest down, as rustls's own
/// buffers do.
#[derive(Debug)]
pub(crate) struct Held {
    buffer: Box<[u8]>,
    len: u32,
}

impl Held {
    pub(crate) fn with_capacity(capacity: u32) -> Held {
        Held { buffer: bytes::zeroed(index(capacity)), len: 0 }
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// How many more bytes fit.
    pub(crate) fn room(&self) -> u32 {
        self.capacity().checked_sub(self.len).expect("no longer than its capacity")
    }

    fn capacity(&self) -> u32 {
        u32::try_from(self.buffer.len()).expect("allocated at a u32 capacity")
    }

    /// The bytes held, for rustls to read and change in place.
    pub(crate) fn filled_mut(&mut self) -> &mut [u8] {
        self.buffer.get_mut(..index(self.len)).expect("no longer than its capacity")
    }

    /// The room after them, for rustls to write into; [`wrote`](Held::wrote)
    /// then counts what it wrote.
    pub(crate) fn spare_mut(&mut self) -> &mut [u8] {
        self.buffer.get_mut(index(self.len)..).expect("no longer than its capacity")
    }

    /// Counts `n` bytes written into the room after those held.
    pub(crate) fn wrote(&mut self, n: usize) {
        let n = u32::try_from(n).expect("written within the room");
        assert!(n <= self.room(), "written within the room");
        self.len = self.len.checked_add(n).expect("within the capacity");
    }

    /// Appends `bytes`, or refuses them whole when they do not fit.
    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), Overflow> {
        let Some(target) = self.spare_mut().get_mut(..bytes.len()) else {
            return Err(Overflow);
        };
        for (to, from) in target.iter_mut().zip(bytes) {
            *to = *from;
        }
        self.wrote(bytes.len());
        Ok(())
    }

    /// Drops the first `n` bytes held, at most all of them, and moves the
    /// rest to the front.
    pub(crate) fn discard(&mut self, n: usize) {
        let len = index(self.len);
        assert!(n <= len, "rustls discards only what it was given");
        self.buffer.copy_within(n..len, 0);
        self.len = u32::try_from(len.checked_sub(n).expect("checked above")).expect("no longer than before");
    }

    /// The first `n` bytes held, at most all of them, moved out into a box
    /// of exactly their length.
    pub(crate) fn take(&mut self, n: u32) -> Box<[u8]> {
        let n = n.min(self.len);
        let mut writer = Writer::new(index(n));
        let front = self.buffer.get(..index(n)).expect("no more than is held");
        writer.put(front).expect("a writer of their length");
        self.discard(index(n));
        writer.finish()
    }

    /// Drops everything held.
    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }

    /// The bytes held.
    pub(crate) fn filled(&self) -> &[u8] {
        self.buffer.get(..index(self.len)).expect("no longer than its capacity")
    }
}

fn index(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits in a usize")
}
