//! Bounded HTTP/SSE byte peers for the shared scripted LLM domain.
//!
//! `provider` owns server and response progress; `documents` uses shared LLM
//! dialect codecs. Explicit caller credentials are borrowed for fake admission;
//! no sign-in, refresh, JWT claims or credential store belongs to this package.
//! Constructors and entries accept immutable bounds, injected clocks and lower
//! stream events. Actual lower close remains outstanding until `closed`.
//! These peers never know application tool authority, policy or checkout state.
//! Contract: docs/design/llm.md; programming-model.md, sections 4.4 and 6.3.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]
extern crate alloc;

pub mod documents;
pub mod provider;
