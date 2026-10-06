//! Independent output reservations beside a classic read (lib.md, 7.1).

use alloc::boxed::Box;

use crate::Token;

use super::Fault;

/// Requests to a native independent output face (lib.md, 7.1).
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum OutputDown {
    /// A caller-unique local token and positive bytes within the lower cap.
    /// Only an open writable endpoint with idle output admits this right;
    /// classic room ownership cannot coexist. One `Settled` ends it (lib.md, 7.1).
    Room {
        /// Generated and never reused by the caller on this stream (lib.md, 7.1).
        right: Token,
        /// Positive, at most the lower output cap, with one Send slot (lib.md, 7.1).
        bytes: u32,
    },
    /// Cancel a pending right; an emitted terminal remains its winner (lib.md, 7.1).
    Cancel {
        /// The pending right; another or retired token is inert (lib.md, 7.1).
        right: Token,
    },
    /// Move one box within a matching grant, spending its whole reservation.
    /// An empty box also consumes the grant. A stale/no-grant Send is inert;
    /// a matching oversized box is the caller's bug, checked before effects (lib.md, 7.1).
    Send {
        /// The currently granted right, consumed once (lib.md, 7.1).
        right: Token,
        /// At most the bytes granted by the matching terminal (lib.md, 7.1).
        bytes: Box<[u8]>,
    },
    /// Consume an unused matching grant without moving any bytes (lib.md, 7.1).
    Release {
        /// The currently granted right; another or retired token is inert (lib.md, 7.1).
        right: Token,
    },
}

/// The native lower face's exactly-once terminal for an admitted output right (lib.md, 7.1).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum OutputUp {
    /// Generated only for an admitted right; it neither answers nor withdraws
    /// a classic read. A queued terminal remains the winner across Cancel (lib.md, 7.1).
    Settled {
        /// The request's exact locally generated identity (lib.md, 7.1).
        right: Token,
        /// Actual grant, cancellation or failure; never peer receipt (lib.md, 7.1).
        outcome: OutputOutcome,
    },
}

/// What consumed the output demand (lib.md, 7.1).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum OutputOutcome {
    /// Bytes and one Send slot are reserved; one matching Send, Release,
    /// Finish or actual close/failure retires the resulting affine grant (lib.md, 7.1).
    Granted,
    /// Actual cancellation before grant, including Close/Abort before Closed (lib.md, 7.1).
    Cancelled,
    /// Actual failure before grant, emitted before classic stream Failed (lib.md, 7.1).
    Failed(
        /// The actual lower failure, before classic Failed (lib.md, 7.1).
        Fault,
    ),
}
