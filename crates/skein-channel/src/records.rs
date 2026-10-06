//! Owned public entrances and outcomes (channel.md §§3–6). Every outward
//! call reserves `MAX_UP/MAX_DOWN`; raw Body is internal wrapper scratch.
use crate::Encoded;
use alloc::boxed::Box;
use skein_lib::{Token, stream};

/// Maximum aggregate upper records for consume/resolve/one final poll (§5).
pub const MAX_UP: u32 = 2;

/// Maximum aggregate lower records, including retirement (§§5–6).
pub const MAX_DOWN: u32 = 3;

/// Mechanical framing phase; service phases belong in concrete wrappers (§2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// Waiting for Open or Accept.
    Opening,
    /// Parent must Accept or Refuse the opaque Opening.
    Authorizing,
    /// Selected version; local Terms admitted, peer Terms outstanding.
    Terms,
    /// Negotiated; immutable first gate may still be pending.
    Ready,
    /// No new sender admission; existing queue drains.
    Finished,
    /// Logical drain/stop; native rights and physical lifecycle still retire.
    Closing,
    /// Physical resource closed; a missing named terminal can still be owed.
    Closed,
}

/// Immediate logical failure; never a transport commitment (§§1,6).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fault {
    /// Malformed bytes or illegal mechanical phase/direction.
    Framing,
    /// Peer terms or checked local identity/storage limits.
    Limits,
    /// Well-ordered offers have no intersection, or invalid Accept version.
    Version,
    /// Unexpected actual input `EOF`; service-specific allowed `EOF` uses `read_end`.
    End,
    /// Actual lower stream failure.
    Stream(
        /// The lower fault, preserved without inventing another cause.
        stream::Fault,
    ),
    /// Explicit Close, peer refusal, or actual output lifecycle loss.
    Closed,
    /// Failed live sender encoding or queue byte/slot admission.
    OutputFull,
}

/// Opaque bounded credentials and offer, consumed by Open/Opening (§§1–2).
#[derive(Debug)]
pub struct Opening {
    /// Exact frozen channel byte.
    pub channel: u8,
    /// Lowest offered version; peer offers may include zero.
    pub lowest: u16,
    /// Highest offered version; must be at least lowest.
    pub highest: u16,
    /// Opaque name, bounded by actual `OpeningProfile.name_bytes`.
    pub name: Box<[u8]>,
    /// Opaque secret, bounded by actual `OpeningProfile.secret_bytes`.
    pub secret: Box<[u8]>,
}

/// Opaque refusal code/text; no normalization of unknown reason codes (§1).
#[derive(Debug)]
pub struct Refusal {
    /// Exact wire u16, interpreted only by the service.
    pub reason: u16,
    /// Exact opaque text, at most actual `refuse_bytes` and frozen 506.
    pub text: Box<[u8]>,
}

/// Exact body receipt resolution, after bounded typed decode and policy (§3).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Disposition {
    /// Invalid tag/shape/trailing bytes; does not increment `received_frames`.
    InvalidBody,
    /// Publish using the already admitted Read, then pause input.
    DecodedPause,
    /// Count this decoded record and continue input without a new Read.
    DecodedContinue,
    /// Count a fully decoded record, then close for semantic rejection.
    DecodedReject,
}

/// Entrance staging; neither consume nor resolve hides an ordinary poll (§5).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Step {
    /// Resolve synchronously before the single permitted final poll.
    NeedDecode {
        /// Exact local receipt; cannot be reused or settle another body.
        receipt: Token,
    },
    /// One ordinary final poll is permitted, with reserved aggregate outputs.
    NeedPoll,
    /// No same-call ordinary poll; fatal retirement may already have emitted.
    Halt,
}

/// Parent requests; sender checks precede encoding in the concrete wrapper (§3).
#[derive(Debug)]
pub enum Request {
    /// Initiator sends its first bounded offer; disposition echoes owner.
    Open {
        /// Admission identity only, never a write/peer/durable acknowledgment.
        owner: Token,
        /// Owned credentials and a subset of supported versions.
        opening: Opening,
    },
    /// Parent authorization of a held Opening.
    Accept {
        /// Within both supported range and peer offer.
        version: u16,
    },
    /// Queue/drain a refusal; no public Sent for this internal control.
    Refuse {
        /// Owned bounded opaque reason/text.
        refusal: Refusal,
    },
    /// Permit one next record; cannot overwrite `AwaitDecode` or accumulate credit.
    Read,
    /// State-only resolution of exactly one received body.
    Resolve {
        /// Matching local body receipt; stale/late values are inert.
        receipt: Token,
        /// Result of exact typed decode and service phase validation.
        disposition: Disposition,
    },
    /// Admission of an already checked exact owning frame.
    Send {
        /// Echoed in exactly one Sent/Unsent on admission/capacity paths.
        owner: Token,
        /// None means live typed encoding failure only after wrapper checks.
        encoded: Option<Encoded>,
    },
    /// Explicit bounded Ping, with queue-admission disposition.
    Ping {
        /// Admission identity, never a lower output right.
        owner: Token,
    },
    /// Wrapper marks outgoing-final after actual Sent admission, before poll.
    Finish,
    /// Immediate logical closure and bounded retirement; no ordinary poll.
    Close,
}

/// Parent events, bounded by two aggregate records per entrance (§§3,5).
#[derive(Debug)]
pub enum Event {
    /// `AskParent` pauses until Accept/Refuse; credentials have no shared meaning.
    Opening {
        /// Actual decoded bounded offer.
        opening: Opening,
    },
    /// Local Terms admitted and peer Terms validated, not transmitted.
    Ready {
        /// Selected supported version; first gates already installed.
        version: u16,
    },
    /// One exact known raw body; wrapper owns at most one decoded record.
    Body {
        /// Never-reused receipt blocking another body until resolution.
        receipt: Token,
        /// Selected version for typed decode.
        version: u16,
        /// Validated kind/channel/direction; service phase remains above.
        kind: u16,
        /// Exact body allocation, counted until wrapper drops it.
        bytes: Box<[u8]>,
    },
    /// Received status is delivered once and pauses input like service payload.
    Unsupported {
        /// Exact status payload, not the frame's common kind 17.
        kind: u16,
    },
    /// Exact peer refusal followed by immediate Closed.
    Refused {
        /// Opaque unnormalized reason.
        reason: u16,
        /// Actual bounded decoded text.
        text: Box<[u8]>,
    },
    /// Queue admission, with no transport or durable acknowledgment implied.
    Sent {
        /// Parent-supplied frame admission identity.
        owner: Token,
    },
    /// Rejected owning frame is dropped, no box returned.
    Unsent {
        /// Parent-supplied frame admission identity.
        owner: Token,
    },
    /// Wrapper-approved actual input `EOF`; output remains independent.
    ReadEnded,
    /// Immediate logical notification; owner must still await `is_retired`.
    Closed {
        /// Exact framing fault; emitted at most once.
        fault: Fault,
    },
}

/// Native lower requests; no classic room or classic Send is manufactured (§4).
#[derive(Debug)]
pub enum LowerRequest {
    /// Read-only Demand, withdrawal, or final Finish (lib.md §7).
    Stream(
        /// Actual stream request; Demand always has room zero.
        stream::Down,
    ),
    /// Whole-cap affine output right (lib.md §7.1).
    Output(
        /// Actual named native reservation operation.
        stream::OutputDown,
    ),
}

/// Actual lower facts, including separate physical lifecycle (§6).
#[derive(Debug)]
pub enum LowerEvent {
    /// Exact read answer/`EOF`/failure; classic Room is invalid on this face.
    Stream(
        /// Native lower stream event.
        stream::Up,
    ),
    /// Actual winner of one locally named whole-cap output request.
    Output(
        /// Matching native terminal; stale terminals are inert.
        stream::OutputUp,
    ),
    /// Actual enclosing transport entered closing; no anonymous right terminal.
    Closing,
    /// Actual enclosing resource closed; a named terminal still must arrive.
    Closed,
}
