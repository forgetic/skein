//! Requests and their terminal events.

#![expect(clippy::disallowed_methods, reason = "prefix successor needs a mutable copy of the caller's bytes")]

use alloc::boxed::Box;
use skein_io::kernel::Fd;
use skein_lib::Token;

#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    Open { owner: Token, root: Fd },
    Commit { owner: Token, ops: Box<[Op]> },
    Get { owner: Token, key: Box<[u8]> },
    Load { owner: Token, range: Range, max: Page },
    Close { owner: Token },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Op {
    Put { key: Box<[u8]>, value: Box<[u8]> },
    Erase { key: Box<[u8]> },
}

impl Op {
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put { key, .. } | Op::Erase { key } => key,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Range {
    pub start: Box<[u8]>,
    pub end: Option<Box<[u8]>>,
}

impl Range {
    #[must_use]
    pub fn prefix(prefix: &[u8]) -> Range {
        let mut end = prefix.to_vec();
        let mut successor = None;
        while let Some(last) = end.pop() {
            if last < u8::MAX {
                end.push(last.checked_add(1).expect("a byte below 255 has a successor"));
                successor = Some(end.into_boxed_slice());
                break;
            }
        }
        Range { start: Box::from(prefix), end: successor }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Page {
    pub rows: u32,
    pub bytes: u32,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Row {
    pub key: Box<[u8]>,
    pub value: Box<[u8]>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    Opened {
        owner: Token,
        last: u64,
    },
    Committed {
        owner: Token,
        number: u64,
    },
    Refused {
        owner: Token,
        refusal: Refusal,
    },
    Failed {
        owner: Token,
        number: u64,
    },
    Got {
        owner: Token,
        value: Option<Box<[u8]>>,
    },
    Loaded {
        owner: Token,
        rows: Box<[Row]>,
        next: Option<Box<[u8]>>,
    },
    Closed {
        owner: Token,
    },
    OpenFailed {
        owner: Token,
        failure: Failure,
    },
    /// A fatal file failure was repaired; the recovered commit prefix is usable.
    Recovered {
        last: u64,
    },
    /// Recovery after a previously opened store could not complete.
    RecoveryFailed {
        failure: Failure,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Refusal {
    TooLarge,
    Full,
    Busy,
    Unavailable,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Failure {
    Io,
    Corrupt,
    Full,
}
