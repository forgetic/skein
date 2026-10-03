//! io, the lowest step layer of every service (io.md).
//!
//! So far only the records of the kernel boundary below it, for sockets
//! (kernel.md): [`kernel`].

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod kernel;
#[cfg(test)]
mod tests;
