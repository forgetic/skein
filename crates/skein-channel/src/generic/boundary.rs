//! Owned requests and events around a generic channel (channel.md, sections
//! 5 and 9). They keep no transport handle, clock or application body meaning.
use alloc::boxed::Box;
use skein_lib::{List, Token, stream};

use super::{Frame, Term};

/// The most owner events one entry point emits.
pub const MAX_UP: u32 = 3;

/// The most lower records one entry point emits.
pub const MAX_DOWN: u32 = 3;

/// A request from the channel's owner.
#[derive(Debug)]
pub enum Request {
    /// The initiator starts with an opaque bounded credential.
    Open { credential: Box<[u8]> },
    /// The responder selects a version offered by both peers.
    Accept { version: u16 },
    /// Either owner ends with a bounded reason and text.
    Refuse { reason: u16, text: Box<[u8]> },
    /// The owner grants one application frame read.
    Read,
    /// The owner submits one measured application frame.
    Send { token: Token, frame: Frame },
    /// The owner asks to send a liveness frame.
    Ping,
    /// The owner sends its last word.
    Finish,
    /// The owner stops the channel now.
    Close,
}

/// Why one terminal closed the channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Closed {
    /// The owner requested close.
    Owner,
    /// This side sent a refusal with the reason.
    RefusedHere(u16),
    /// The peer refused with the reason.
    RefusedPeer(u16),
    /// The peer sent a malformed or out-of-phase frame.
    Framing,
    /// A stream failed.
    Stream,
    /// Input ended inside a frame or before ready.
    Truncated,
}

/// Why an owner's measured frame was refused before queue admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unsent {
    /// This kind is absent or belongs to the other sender.
    WrongDirection,
    /// The peer omitted this kind from its receive terms.
    PeerDoesNotTake,
    /// The body exceeds the peer's or this side's bound.
    TooLarge,
    /// The application output cap has no byte or frame room.
    Full,
    /// The channel has not become ready or is ending.
    Closed,
}

/// Remaining application output credit, excluding control reserves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Room {
    pub bytes: u32,
    pub frames: u32,
}

/// An event delivered to the channel's owner.
#[derive(Debug)]
pub enum Event {
    /// The responder heard an opening and asks its owner to choose.
    Opening { credential: Box<[u8]>, lowest: u16, highest: u16 },
    /// Opening completed with the peer's receive terms.
    Ready { version: u16, terms: List<Term> },
    /// One application body was received.
    Body { kind: u16, body: Box<[u8]> },
    /// The peer sent a liveness frame.
    Ping,
    /// The peer reported a kind it skipped.
    Unsupported { kind: u16 },
    /// The peer sent a refusal.
    Refused { reason: u16, text: Box<[u8]> },
    /// Input ended between frames.
    Ended,
    /// An owner's frame was admitted to the queue.
    Sent { token: Token },
    /// An owner's frame was refused at the entrance.
    Unsent { token: Token, why: Unsent },
    /// A queued frame was handed to the stream below.
    Drained,
    /// The write stream failed while input may continue.
    OutputFailed,
    /// The one terminal, after lower rights settle.
    Closed { why: Closed },
}

/// One record to the read or independent write stream.
#[derive(Debug)]
pub enum Lower {
    /// A read-only demand or a finish on the read stream.
    Read(stream::Down),
    /// A named independent output operation on the write stream.
    Write(stream::OutputDown),
}

/// One response from the read or independent write stream.
#[derive(Debug)]
pub enum LowerEvent {
    /// Bytes, end or failure from the read stream.
    Read(stream::Up),
    /// The named output reservation settled.
    Write(stream::OutputUp),
}

/// What the read side currently needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadWait {
    /// The automatic opening is in progress.
    Opening,
    /// The owner has asked for a frame.
    Frame,
    /// No read is wanted.
    Nothing,
}

/// What the write side currently needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteWait {
    /// At least one frame awaits a grant or handoff.
    Frames,
    /// No frame is queued.
    Nothing,
}

/// The two independent waits used by the owner's deadlines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Waiting {
    pub read: ReadWait,
    pub write: WriteWait,
}
