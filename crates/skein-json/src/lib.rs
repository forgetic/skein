//! JSON for a connection's stack of protocol machines (json.md): a bounded
//! tokenizer, pulled by demand, and a sized writer.
//!
//! - [`tokenizer`] is a step machine (programming-model.md, 4). Below it, a
//!   `lib::stream` carrying one document; above it, the service's decoder,
//!   which demands one [`Token`] at a time. Nesting is held in a
//!   `lib::Stack`, strings and numbers are under maximum lengths, and
//!   numbers go up as validated text, never as floats.
//! - [`writer`] encodes a document from the caller's own calls, run twice:
//!   once to measure it, once to write it, escaped, into a box of exactly
//!   its length.
//!
//! Both speak [`Token`]: what the tokenizer reads, the writer writes back.
//! Decoding documents into a domain's types is the application's
//! (json.md, 5).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod number;
mod string;
#[cfg(test)]
mod tests;
mod token;
pub mod tokenizer;
mod utf8;
pub mod writer;

pub use token::Token;
