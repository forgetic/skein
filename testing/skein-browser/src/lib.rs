//! A bounded browser testing machine over Chromium's `DevTools` pipe.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod boundary;
mod browser;
pub mod command;
pub mod limits;
mod params;
pub mod wire;

pub use boundary::{Below, Down, Event, Request};
pub use browser::{Browser, DOWN_MAX_OUT, FIRE_MAX_OUT, MaxOut, UP_MAX_OUT, down, fire, up};
pub use limits::{Limits, largest_read, largest_room, worst_case};
