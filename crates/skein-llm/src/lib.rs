//! Provider-neutral LLM calls over an externally owned plaintext stream.
//!
//! Supports `ChatGPT`'s Codex Responses and Anthropic Messages subscription routes.
//! Socket/TLS ownership, OAuth sign-in and renewal, deadlines and retry
//! policy belong to the caller. See `docs/design/llm.md` for the contract.
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod affinity;
pub mod anthropic;
pub mod client;
mod dialect;
mod document;
mod filter;
pub mod openai;
mod replay;
mod translate;
mod types;

pub use document::DocumentLimits;
/// Bounded document admission errors, independent of the configured wire dialect.
/// Callers use `document_error` to retain limit versus invalid-input refusal.
/// See `docs/design/llm.md`, Vocabulary and ownership.
pub use openai::DecodeError as DocumentError;
pub use openai::Json;
pub use replay::{REPLAY_HEADER_BYTES, replay_bytes, replay_worst_case};
pub use translate::decode as document_error;
pub use types::{
    Affinity, Block, Call, Cap, Completion, Credential, Delta, Endpoint, Error, Failure, Message, Phase, Prompt,
    Provider, Replay, Role, Stop, Tool, ToolChoice, Usage,
};
