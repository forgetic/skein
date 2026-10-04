//! The machine worlds of skein-http (testing-strategy.md, 2.4; http.md, 6):
//! the client's and the server's, the event stream reader's and the
//! writer's, and the client and the reader stacked.
//!
//! What the test binaries share:
//!
//! - [`client_world`]: one client from a seed, for one exchange after
//!   another, between a server's stream below that cuts its bytes at
//!   random, grants room late, ends early or fails, and a user above that
//!   uploads, reads slowly, withdraws, discards, stops, and closes in every
//!   state. It checks both of the client's streams as it goes, and each
//!   exchange against the reference reader.
//! - [`server_world`]: one server from a seed, for one request after
//!   another, between a client's stream below that cuts its bytes at
//!   random, pipelines or waits, holds a body back for a 100 (Continue),
//!   grants room late, ends and fails, and a service above that reads,
//!   discards, withdraws, and responds at every moment. It checks both of
//!   the server's sides as it goes, each call against the reference reader
//!   of requests, and what it wrote against a writer of the test's own.
//! - [`sse_world`]: one reader from a seed, between a body below and a
//!   user above that asks for events slowly, stops, and closes in every
//!   state, checked against the reference reader.
//! - [`writer_world`]: one writer from a seed, between a body below that
//!   grants room late and fails and a user above that writes events and
//!   comments, read back by the reference reader and the reader's world.
//! - [`reference`]: simple readers of a whole response, a whole request
//!   and a whole event stream, held in memory, which the machines are
//!   checked against.
//! - [`generate`]: calls, responses and event streams from a seed, valid
//!   and mutated; [`requests`]: requests, valid and corrupted, the
//!   responses a service gives them, and a writer of the head the server
//!   must write.
//! - [`transcript`] and [`request_transcript`]: the transcripts kept in
//!   `transcripts/` and `transcripts/requests/`, each with what it must
//!   decode to.
//!
//! The focused tests are `tests/*.rs`; the sweeps over many seeds are
//! `tests/fuzzy_*.rs` (testing-strategy.md, 8).

pub mod client_world;
pub mod generate;
pub mod reference;
pub mod request_transcript;
pub mod requests;
pub mod server_world;
pub mod sse_world;
pub mod transcript;
pub mod writer_world;
