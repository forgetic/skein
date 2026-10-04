//! Protocol worlds (testing-strategy.md, 2.5; testing.md, 2): both ends of
//! an LLM streaming exchange, built as two services would build them, their
//! bottoms joined by an in-memory stream in each direction, cut and joined
//! at random.
//!
//! - [`client_end`]: the client's stack, the HTTP client, the event stream
//!   reader on its body, and a JSON tokenizer for each event's data; its
//!   request body written by the JSON writer; a scripted user at its top.
//! - [`server_end`]: the server's stack, the HTTP server, a JSON tokenizer
//!   on each request's body, and the event stream writer on its reply,
//!   each event's data written by the JSON writer; a scripted user at its
//!   top.
//! - [`wire`]: the bytes between them, and each end's stream below.
//! - [`world`]: the loop, the contracts as it goes, replay, and the
//!   referee.
//! - [`scenario`]: what each world sets up, in the ends' own terms.
//!
//! The world harness `skein-world` drives processes' `iterate` over the
//! simulator, through the kernel's records; a protocol world has no kernel
//! and joins two stacks of machines by bytes, so it keeps a small harness
//! of its own, of the same shape: one loop, the contracts as it goes, a
//! referee watching what the users saw, safety at every observation and
//! liveness once settled. The worlds run in plaintext.
//!
//! The focused tests are `tests/*.rs`; the sweeps over many seeds are
//! `tests/fuzzy_*.rs` (testing-strategy.md, 8).

pub mod client_end;
pub mod scenario;
pub mod server_end;
pub mod wire;
pub mod world;
