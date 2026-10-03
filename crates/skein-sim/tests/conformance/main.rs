//! The conformance suite against the simulator (kernel.md,
//! 8): every scenario of `skein_sim::conformance`, over many
//! seeds, in a calm world and with the faults loopback can show. The same
//! scenarios run against the ring in `skein-shell`'s tests.
//!
//! A failing seed is named, with the end of its trace.

extern crate alloc;

#[cfg(test)]
mod suite;
