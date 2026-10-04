//! TLS for a connection's stack of protocol machines (tls.md): the client,
//! over rustls's unbuffered connection.
//!
//! - [`client`] is a step machine (programming-model.md, 4) between two
//!   streams (lib.md, 7): the ciphertext below, a socket's or a pipe's, and
//!   the plaintext above, which the machine stacked on it cannot tell from a
//!   socket.
//! - [`Config`] is what every connection shares, made at startup from the
//!   roots a service trusts and the protocols it offers by ALPN; [`Name`]
//!   is each connection's server.
//!
//! It is the one exception of programming-model.md, section 3: the only
//! step code that depends on a crate from outside skein and the service,
//! rustls with ring beneath it, and the only step code that is not
//! deterministic, as rustls draws its randoms and keys from the kernel
//! through ring. It reads no clock: certificates are checked against
//! `env.wall`. The server side is not built (tls.md, 7).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod client;
mod config;
mod held;
mod session;
#[cfg(test)]
mod tests;

pub use config::{ALPN, Config, Name, Refusal};
/// What [`Config::new`] takes: the roots a service trusts, which its shell
/// reads at startup, and the certificates they are made of.
pub use rustls::RootCertStore;
pub use rustls::pki_types::CertificateDer;

/// The most an entry point emits in one call, into each of its two queues.
/// Whoever calls it reserves this much room in each first
/// (programming-model.md, 2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    /// Events for the side above.
    pub above: u32,
    /// Requests for the side below.
    pub below: u32,
}
