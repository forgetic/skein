//! A bounded generic framed channel (channel.md, sections 1–10).
//!
//! A `Machine` keeps a checked application schema, opening phase, one
//! partial input frame, the peer's terms, a bounded output queue, and one
//! independent output right. It never knows credential policy, application
//! body meaning, transport type, timers, or call correlation. `Machine::new`
//! checks the schema; `down`, `up`, and `poll` move owned records between the
//! owner and streams. `frame_writer` measures and seals a frame before Send.
//! The owner reserves `MAX_UP` and `MAX_DOWN` slots at each entry point.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod frame;
mod limits;
mod machine;
mod opening;
mod read;
mod schema;
mod write;

pub use boundary::{
    Closed, Event, Lower, LowerEvent, MAX_DOWN, MAX_UP, ReadWait, Request, Room, Unsent, Waiting, WriteWait,
};
pub use frame::{
    Control, Frame, FrameError, FrameWriter, Header, Term, control_frame, decode_control, frame_writer, parse_header,
};
pub use limits::worst_case;
pub use machine::{Machine, StreamMode};
pub use schema::{Direction, Kind, Limits, Role, Schema, SchemaError, Version};

#[cfg(test)]
mod tests;
