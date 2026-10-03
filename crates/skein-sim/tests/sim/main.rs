//! The simulator's own tests (simulator.md, 6): the records of
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
