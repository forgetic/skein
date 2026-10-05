//! Order-preserving composite byte keys.

#![expect(clippy::disallowed_types, reason = "an encoded key is bounded by KeyWriter's limit")]

use alloc::boxed::Box;
use alloc::vec::Vec;

#[derive(Debug)]
pub struct KeyWriter {
    bytes: Vec<u8>,
    limit: u32,
}

impl KeyWriter {
    #[must_use]
    pub fn new(limit: u32) -> KeyWriter {
        KeyWriter { bytes: Vec::new(), limit }
    }

    pub fn tag(&mut self, tag: u8) -> &mut Self {
        self.bytes.push(tag);
        self
    }

    pub fn u64(&mut self, number: u64) -> &mut Self {
        self.bytes.extend_from_slice(&number.to_be_bytes());
        self
    }

    pub fn rev_u64(&mut self, number: u64) -> &mut Self {
        self.u64(!number)
    }

    pub fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
        for &byte in bytes {
            self.bytes.push(byte);
            if byte == 0 {
                self.bytes.push(0xff);
            }
        }
        self.bytes.extend_from_slice(&[0, 0]);
        self
    }

    #[must_use]
    pub fn finish(self) -> Option<Box<[u8]>> {
        if self.bytes.len() > usize::try_from(self.limit).ok()? {
            return None;
        }
        Some(self.bytes.into_boxed_slice())
    }
}

#[derive(Debug)]
pub struct KeyReader<'a> {
    rest: &'a [u8],
}

impl<'a> KeyReader<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> KeyReader<'a> {
        KeyReader { rest: bytes }
    }

    #[must_use]
    pub fn tag(&mut self) -> Option<u8> {
        let (&first, rest) = self.rest.split_first()?;
        self.rest = rest;
        Some(first)
    }

    #[must_use]
    pub fn u64(&mut self) -> Option<u64> {
        let bytes = self.rest.get(..8)?;
        let fixed: [u8; 8] = bytes.try_into().ok()?;
        self.rest = self.rest.get(8..)?;
        Some(u64::from_be_bytes(fixed))
    }

    #[must_use]
    pub fn rev_u64(&mut self) -> Option<u64> {
        Some(!self.u64()?)
    }

    #[must_use]
    pub fn bytes(&mut self) -> Option<Box<[u8]>> {
        let mut result = Vec::new();
        let mut at: usize = 0;
        loop {
            let byte = *self.rest.get(at)?;
            at = at.checked_add(1)?;
            if byte != 0 {
                result.push(byte);
                continue;
            }
            let escaped = *self.rest.get(at)?;
            at = at.checked_add(1)?;
            match escaped {
                0 => {
                    self.rest = self.rest.get(at..)?;
                    return Some(result.into_boxed_slice());
                }
                0xff => result.push(0),
                _ => return None,
            }
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }
}
