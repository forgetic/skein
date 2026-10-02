//! The simulator's own tests (testing-pyramid.md, section 8): the records of
//! `skein_io::kernel`, submitted by hand, against what the contract allows.

extern crate alloc;

#[cfg(test)]
mod support;

#[cfg(test)]
mod exchange;

#[cfg(test)]
mod sockets;

#[cfg(test)]
mod cancel;

#[cfg(test)]
mod time;

#[cfg(test)]
mod invariants;
