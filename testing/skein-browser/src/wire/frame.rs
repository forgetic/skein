//! NUL terminated CDP documents, across arbitrary pipe deliveries.

#![expect(clippy::disallowed_types, reason = "a push returns the messages completed by one delivery")]

use alloc::boxed::Box;
use alloc::vec::Vec;

/// Why the pipe cannot continue carrying messages.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameError {
    TooLong,
    EndedMidMessage,
    Failed,
}

/// A reader for Chromium's NUL terminated messages.
#[derive(Debug)]
pub struct Framer {
    limit: u32,
    pending: Vec<u8>,
    failed: bool,
}

impl Framer {
    /// `limit` is the maximum JSON document length, excluding NUL.
    #[must_use]
    pub fn new(limit: u32) -> Framer {
        Framer { limit, pending: Vec::new(), failed: false }
    }

    /// Accept bytes from one pipe delivery; complete documents exclude NUL.
    /// The first overlong document makes the framer terminal.
    #[expect(clippy::mem_replace_with_default, reason = "the step subset uses an explicit placeholder")]
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Box<[u8]>>, FrameError> {
        if self.failed {
            return Err(FrameError::Failed);
        }
        let mut complete = Vec::new();
        for byte in bytes {
            if *byte == 0 {
                complete.push(core::mem::replace(&mut self.pending, Vec::new()).into_boxed_slice());
            } else {
                let Ok(len) = u32::try_from(self.pending.len()) else {
                    return Err(FrameError::TooLong);
                };
                if len >= self.limit {
                    self.failed = true;
                    return Err(FrameError::TooLong);
                }
                self.pending.push(*byte);
            }
        }
        Ok(complete)
    }

    /// End the pipe. A partial final document is a framing fault.
    pub fn end(&mut self) -> Result<(), FrameError> {
        if self.failed {
            return Err(FrameError::Failed);
        }
        if !self.pending.is_empty() {
            self.failed = true;
            return Err(FrameError::EndedMidMessage);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{FrameError, Framer};

    #[test]
    fn split_and_combined_messages() {
        let mut framer = Framer::new(4);
        assert!(framer.push(b"ab").expect("valid chunk").is_empty());
        let messages = framer.push(b"cd\0x\0").expect("two valid messages");
        assert_eq!(messages.first().expect("first message").as_ref(), b"abcd");
        assert_eq!(messages.get(1).expect("second message").as_ref(), b"x");
        assert_eq!(framer.end(), Ok(()));
    }

    #[test]
    fn message_past_limit() {
        let mut framer = Framer::new(2);
        assert_eq!(framer.push(b"abc\0"), Err(FrameError::TooLong));
        assert_eq!(framer.push(b"x\0"), Err(FrameError::Failed));
    }

    #[test]
    fn ending_half_a_message() {
        let mut framer = Framer::new(2);
        assert!(framer.push(b"a").expect("valid chunk").is_empty());
        assert_eq!(framer.end(), Err(FrameError::EndedMidMessage));
    }
}
