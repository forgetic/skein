//! Provider-neutral LLM calls over an externally owned plaintext stream.
//!
//! Supports `ChatGPT`'s Codex Responses and Anthropic Messages subscription routes.
//! Socket/TLS ownership, OAuth sign-in and renewal, deadlines and retry
//! policy belong to the caller. See `docs/design/llm.md` for the contract.
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod anthropic;
pub mod client;
mod dialect;
pub mod openai;
mod translate;
mod types;

pub use openai::Json;
pub use types::{
    Block, Call, Completion, Credential, Delta, Endpoint, Error, Failure, Message, Prompt, Provider, Replay, Role,
    Stop, Tool, Usage,
};
