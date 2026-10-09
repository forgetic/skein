//! Requests and events at the owner's and io's boundaries
//! (llm-connection.md, sections 2 and 4). The owner translates its domain
//! vocabulary and keeps io's token with each component token.

use alloc::boxed::Box;
use skein_lib::{Duration, Token};
use skein_llm::client::Evidence;
use skein_llm::{self as llm, Block, Completion, Credential, Delta, Failure, Prompt};

/// Optional per-call deadlines, measured from the phase in their names.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Deadlines {
    pub connect: Option<Duration>,
    pub handshake: Option<Duration>,
    pub head: Option<Duration>,
    pub idle: Option<Duration>,
    pub whole: Option<Duration>,
}

impl Deadlines {
    /// No per-call deadlines.
    #[must_use]
    pub const fn none() -> Deadlines {
        Deadlines { connect: None, handshake: None, head: None, idle: None, whole: None }
    }
}

/// A request from the owning protocol layer.
#[expect(missing_debug_implementations, reason = "Start holds a bearer credential")]
#[expect(clippy::large_enum_variant, reason = "the owning protocol layer moves each Start directly into the component")]
pub enum Request {
    /// Admit a call or refuse it at once; accepted calls have one terminal.
    /// The caller explicitly selects reasoning dropping for this model/call.
    Start {
        call: Token,
        endpoint: u32,
        prompt: Prompt,
        credential: Credential,
        deadlines: Deadlines,
        drop_reasoning: bool,
    },
    /// Demand one more answer event from an accepted call.
    Next { call: Token },
    /// End an accepted call at the owner's request.
    Cancel { call: Token },
    /// Drain admitted calls and close every socket; Closed is its terminal.
    Close,
    /// Cancel admitted calls and abort every socket; Closed is its terminal.
    Abort,
}

/// Why a Start was refused before an accepted call existed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// The owner has requested close or abort and settlement is still pending.
    Closed,
    /// The endpoint index does not name configured endpoint.
    Endpoint,
    /// The owner already has its declared number of conversations outstanding.
    Calls { bound: u32 },
    /// No wait can admit a call larger than the whole pool.
    Memory { reservation: u64, bound: u64 },
    /// The LLM client rejected the call before touching the stream.
    Client(llm::Error),
}

/// An answer to the owning protocol layer.
#[derive(Debug)]
pub enum Event {
    /// The owner close or abort settled every call and physical socket, last.
    Closed,
    /// Admission failed; no terminal follows.
    Refused { call: Token, why: Refusal },
    /// A fragment of the provider's output for an outstanding demand.
    Delta { call: Token, delta: Delta },
    /// A completed provider block for an outstanding demand.
    Block { call: Token, block: Block },
    /// The accepted call completed successfully.
    Completed { call: Token, completion: Completion },
    /// The accepted call failed with the client's sending evidence.
    Failed { call: Token, failure: Failure, evidence: Evidence, detail: Box<[u8]> },
    /// The accepted call ended at the owner's request.
    Cancelled { call: Token },
}

/// A component-issued io request; the owner translates its token to io's.
pub type Lower = skein_io::Request;

/// An io event routed back by the owner using the component's token.
pub type LowerEvent = skein_io::Event;
