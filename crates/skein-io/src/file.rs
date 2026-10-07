//! File requests and terminal events for step machines above io.
//!
//! A file request carries the owner's token and produces exactly one event.
//! Files remain open across positioned operations; names stay beneath a root.

use crate::digest::Digest;
use crate::kernel::Fd;
use alloc::boxed::Box;
use skein_lib::Token;

#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    Create {
        owner: Token,
        root: Fd,
        name: Box<[u8]>,
        mode: u32,
    },
    /// Creates a file without following a symbolic link in any parent path part.
    CreateNoFollow {
        owner: Token,
        root: Fd,
        name: Box<[u8]>,
        mode: u32,
    },
    OpenRead {
        owner: Token,
        root: Fd,
        name: Box<[u8]>,
    },
    /// Opens a file for reading without following any symbolic link.
    OpenReadNoFollow {
        owner: Token,
        root: Fd,
        name: Box<[u8]>,
    },
    /// Opens a directory beneath a root and gives back a file token for it.
    OpenDirectory {
        owner: Token,
        root: Fd,
        name: Box<[u8]>,
        no_follow: bool,
    },
    /// Loads one whole file through a root token, refusing content above `max` bytes.
    Load {
        owner: Token,
        root: Token,
        path: Box<[u8]>,
        max: u32,
        no_follow: bool,
    },
    /// Scans a directory, retaining its first entries in name order within both bounds.
    Scan {
        owner: Token,
        root: Token,
        path: Box<[u8]>,
        max: u32,
        /// Maximum owned bytes of entry cells and names in the result.
        max_bytes: u64,
        no_follow: bool,
    },
    /// Replaces one file after rechecking its content version, through a synced temporary.
    Store {
        owner: Token,
        root: Token,
        path: Box<[u8]>,
        bytes: Box<[u8]>,
        expected: Option<Digest>,
        no_follow: bool,
    },
    /// States an open file or directory.
    Stat {
        owner: Token,
        file: Token,
    },
    WriteAt {
        owner: Token,
        file: Token,
        offset: u64,
        bytes: Box<[u8]>,
    },
    ReadAt {
        owner: Token,
        file: Token,
        offset: u64,
        max: u32,
    },
    Sync {
        owner: Token,
        file: Token,
    },
    Close {
        owner: Token,
        file: Token,
    },
    SyncDirectory {
        owner: Token,
        root: Fd,
    },
    Rename {
        owner: Token,
        root: Fd,
        from: Box<[u8]>,
        to: Box<[u8]>,
    },
    Remove {
        owner: Token,
        root: Fd,
        name: Box<[u8]>,
    },
    List {
        owner: Token,
        root: Fd,
    },
}

#[derive(PartialEq, Eq, Debug)]
pub struct Entry {
    pub name: Box<[u8]>,
    pub kind: crate::kernel::Kind,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    Opened {
        owner: Token,
        file: Token,
        len: u64,
    },
    /// A whole file loaded within its requested byte bound.
    Loaded {
        owner: Token,
        bytes: Box<[u8]>,
    },
    /// The first bounded directory entries in name order, and the number omitted.
    Scanned {
        owner: Token,
        entries: Box<[Entry]>,
        more: u64,
    },
    /// A whole-file replacement is durable and has this content digest.
    Stored {
        owner: Token,
        digest: Digest,
    },
    /// The target did not have the expected content version at the recheck.
    Conflict {
        owner: Token,
        now: Option<Digest>,
    },
    /// A load exceeded its requested byte bound; `size` is the observed size.
    TooLarge {
        owner: Token,
        size: u64,
    },
    /// Metadata of the open file or directory.
    Stated {
        owner: Token,
        stat: crate::kernel::Stat,
    },
    Written {
        owner: Token,
    },
    Read {
        owner: Token,
        bytes: Box<[u8]>,
    },
    Synced {
        owner: Token,
    },
    Closed {
        owner: Token,
    },
    Renamed {
        owner: Token,
    },
    Removed {
        owner: Token,
    },
    Listed {
        owner: Token,
        entries: Box<[Entry]>,
    },
    Failed {
        owner: Token,
        error: crate::kernel::Error,
    },
    /// The owner stopped a file request before it completed.
    Cancelled {
        owner: Token,
    },
}

impl Event {
    #[must_use]
    pub const fn owner(&self) -> Token {
        match self {
            Event::Opened { owner, .. }
            | Event::Loaded { owner, .. }
            | Event::Scanned { owner, .. }
            | Event::Stored { owner, .. }
            | Event::Conflict { owner, .. }
            | Event::TooLarge { owner, .. }
            | Event::Stated { owner, .. }
            | Event::Written { owner }
            | Event::Read { owner, .. }
            | Event::Synced { owner }
            | Event::Closed { owner }
            | Event::Renamed { owner }
            | Event::Removed { owner }
            | Event::Listed { owner, .. }
            | Event::Failed { owner, .. }
            | Event::Cancelled { owner } => *owner,
        }
    }
}
