//! io, the lowest step layer of every service (overview.md, section 5).
//!
//! So far only the records of the kernel boundary below it, for sockets
//! (overview.md, section 6): [`kernel`].

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod kernel;
