//! The generic framed channel's immutable schema and frozen wire vocabulary
//! (channel.md, sections 3–5). The schema owns bounded version and kind tables;
//! frames own their bytes. This layer knows no application body meaning,
//! credential policy, stream lifetime or deadlines. `Schema::new` checks the
//! tables; `frame_writer`, `control_frame` and `decode_control` handle bytes.

mod boundary;
mod frame;
mod machine;
mod opening;
mod schema;

pub use boundary::{Closed, Event, Lower, LowerEvent, MAX_DOWN, MAX_UP, ReadWait, Request, Waiting, WriteWait};
pub use frame::{
    Control, Frame, FrameError, FrameWriter, Header, Term, control_frame, decode_control, frame_writer, parse_header,
};
pub use machine::Machine;
pub use schema::{Direction, Kind, Limits, Role, Schema, SchemaError, Version};

#[cfg(test)]
mod tests;
