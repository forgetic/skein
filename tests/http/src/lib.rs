//! The HTTP client's and the event stream reader's machine worlds
//! (testing-strategy.md, 2.4; http.md, 6), and the two stacked.
//!
//! What the test binaries share:
//!
//! - [`client_world`]: one client from a seed, for one exchange after
//!   another, between a server's stream below that cuts its bytes at
//!   random, grants room late, ends early or fails, and a user above that
//!   uploads, reads slowly, withdraws, discards, stops, and closes in every
//!   state. It checks both of the client's streams as it goes, and each
//!   exchange against the reference reader.
//! - [`sse_world`]: one reader from a seed, between a body below and a
//!   user above that asks for events slowly, stops, and closes in every
//!   state, checked against the reference reader.
//! - [`reference`]: simple readers of a whole response and a whole event
//!   stream, held in memory, which the machines are checked against.
//! - [`generate`]: calls, responses and event streams from a seed, valid
//!   and mutated.
//! - [`transcript`]: the transcripts kept in `transcripts/`, each with what
//!   it must decode to.
//!
//! The focused tests are `tests/*.rs`; the sweeps over many seeds are
//! `tests/fuzzy_*.rs` (testing-strategy.md, 8).

pub mod client_world;
pub mod generate;
pub mod reference;
pub mod sse_world;
pub mod transcript;
