//! A bounded browser testing machine over Chromium's `DevTools` pipe.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod boundary;
pub mod command;
pub mod limits;
pub mod wire;

pub use boundary::{Below, Down, Event, Request};
pub use limits::{Limits, largest_read, largest_room, worst_case};
