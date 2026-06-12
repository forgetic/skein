//! Linux reactor placeholders.

use super::{Events, Interest, Reactor, Registration};
use std::io;

/// io_uring-based reactor (not yet implemented).
#[derive(Debug, Default)]
pub struct IoUringReactor;

impl IoUringReactor {
    /// Create a new io_uring reactor.
    pub fn new() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IoUringReactor is not implemented yet",
        ))
    }
}

impl Reactor for IoUringReactor {
    fn register(&self, _source: &dyn super::Source, _token: super::Token, _interest: Interest) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IoUringReactor is not implemented yet",
        ))
    }

    fn deregister(&self, _token: super::Token) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IoUringReactor is not implemented yet",
        ))
    }

    fn poll(&self, _events: &mut Events, _timeout: Option<std::time::Duration>) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IoUringReactor is not implemented yet",
        ))
    }

    fn wake(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IoUringReactor is not implemented yet",
        ))
    }
}

/// epoll-based reactor (not yet implemented).
#[derive(Debug, Default)]
pub struct EpollReactor;

impl EpollReactor {
    /// Create a new epoll reactor.
    pub fn new() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "EpollReactor is not implemented yet",
        ))
    }
}

impl Reactor for EpollReactor {
    fn register(&self, _source: &dyn super::Source, _token: super::Token, _interest: Interest) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "EpollReactor is not implemented yet",
        ))
    }

    fn deregister(&self, _token: super::Token) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "EpollReactor is not implemented yet",
        ))
    }

    fn poll(&self, _events: &mut Events, _timeout: Option<std::time::Duration>) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "EpollReactor is not implemented yet",
        ))
    }

    fn wake(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "EpollReactor is not implemented yet",
        ))
    }
}
