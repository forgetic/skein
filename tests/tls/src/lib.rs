//! The TLS client's machine worlds (testing-strategy.md, 2.4 and 4.4;
//! tls.md, 5).
//!
//! What the test binaries share:
//!
//! - [`pki`]: the test root, an intermediate, the server's certificates and
//!   key, kept in `fixtures/`, and the configurations made of them.
//! - [`server`]: a rustls server in memory, over byte slices.
//! - [`drive`]: a prompt side below, that server's ciphertext delivered as
//!   soon as it meets a demand, room granted at once: for tests that aim at
//!   one outcome.
//! - [`world`]: one client, from a seed, between that server's ciphertext
//!   below, cut at random, room granted late, ending or failing, and a user
//!   above that reads slowly, writes within the room granted, finishes, and
//!   closes in every state.
//!
//! The worlds are not deterministic: rustls draws its randoms and keys from
//! the kernel, so a record's length, and what is cut where, change from run
//! to run of a seed. They assert only what does not depend on it: what
//! each side received of the other's plaintext, the events and their order,
//! the errors, and the contracts of both streams.
//!
//! The focused tests are `tests/*.rs`; the sweeps over many seeds are
//! `tests/fuzzy_*.rs` (testing-strategy.md, 8).

pub mod drive;
pub mod pki;
pub mod server;
pub mod world;
