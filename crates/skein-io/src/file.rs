//! File requests and terminal events for step machines above io.
//!
//! A file request carries the owner's token and produces exactly one event.
//! Files remain open across positioned operations; names stay beneath a root.

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
}

impl Event {
    #[must_use]
    pub const fn owner(&self) -> Token {
        match self {
            Event::Opened { owner, .. }
            | Event::Stated { owner, .. }
            | Event::Written { owner }
            | Event::Read { owner, .. }
            | Event::Synced { owner }
            | Event::Closed { owner }
            | Event::Renamed { owner }
            | Event::Removed { owner }
            | Event::Listed { owner, .. }
            | Event::Failed { owner, .. } => *owner,
        }
    }
}
