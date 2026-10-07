//! Generated sample family for codec.md, sections 4 and 6.
//! The types hold bounded fields. They know no peer or transport. Tests drive
//! their constructors and, later, wire entry points.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

#[rustfmt::skip]
#[path = "generated/v1.rs"]
pub mod v1;
