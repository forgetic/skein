//! HTTP/1.1 and server-sent events for a connection's stack of protocol
//! machines (http.md): the client side of HTTP/1.1, and the reader side of
//! an event stream.
//!
//! - [`client`] is a step machine (programming-model.md, 4). Below it, a
//!   `lib::stream` to the server: TLS's plaintext, a socket or a pipe.
//!   Above it, the service's protocol layer, which makes one exchange at a
//!   time, writes the request body as a stream, and reads the response
//!   body as a stream, or stacks a machine on it.
//! - [`sse`] is a step machine over a body stream: lines, fields, and an
//!   event at each blank line, each under a maximum. An event's data goes
//!   up whole, and [`sse::Data`] reads it to a machine stacked above, a
//!   JSON tokenizer.
//!
//! The server and the event writer are not built yet (http.md, 9).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod body;
pub mod client;
mod header;
mod message;
pub mod sse;
#[cfg(test)]
mod tests;

pub use header::Header;
pub use message::{Method, Version};

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
