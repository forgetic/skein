//! The simulator's own tests (simulator.md, 6): the records of
//! `skein_io::kernel`, submitted by hand, against what the contract allows.
//!
//! What the test binaries share: [`World`], a small harness that submits one
//! record and reaps what came of it, and [`exchange`], a client and a server
//! scripted as io will drive them. The focused tests are `tests/*.rs`; the
//! sweep under chaos is `tests/fuzzy_exchange.rs` (testing-strategy.md, 8).

pub mod exchange;
mod support;

pub use support::{World, local, received, recv_op};
