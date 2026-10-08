//! Loopback io faces for scripted peers (fake-llm.md, section 3; oauth.md,
//! section 5). Each peer keeps its independent machine, bounded connections,
//! observations and io queues; it never knows a client's internal state or
//! application policy. Constructors start listening; `Host::iterate` drives
//! the passes, and `shutdown` closes admission and every connection.
//!
//! These testing step machines use ordinary Rust (programming-model.md,
//! section 10.2). Plaintext replays; TLS uses the fixed Skein test chain and
//! real rustls cryptography (testing-strategy.md, section 4.4).
#![forbid(unsafe_code)]

mod face;
pub mod llm;
pub mod oauth;
mod transport;

pub use face::{Error, Limits};
pub use transport::Transport;
