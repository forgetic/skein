//! The domain layer of a fake LLM provider, for application worlds.
//!
//! A provider seen from the inside: calls come up from its protocol layer as
//! [`Event::Call`], and each is answered with exactly one [`Request::Reply`]
//! after a latency drawn from the configuration. What it answers follows a
//! script (see the `respond` module): configured chances of failing, a
//! configured number of tool rounds, then a final answer; or, for a
//! conversation a world scripted ([`api::Script`]), the answers it wrote,
//! one after another. It rejects conversations a real provider would reject,
//! so it also checks its clients.
//!
//! Its vocabulary ([`api`]) is its own: it shares nothing with the agent's
//! domain. Between the two sits a protocol layer on each side, or a world
//! translating (testing-strategy.md, section 4).
//!
//! It follows the programming model as any other step crate does.

//!
//! It keeps bounded pending calls, reply rights, scripts, RNG state and due
//! alarms, never agent state, credential secrets, tool authority, checkout
//! effects or live network behavior. [`step`] accepts calls and [`fire`]
//! settles due work under injected time; callers reserve [`MAX_OUT`] output
//! slots and receive one reply per accepted call (docs/design/fake-llm.md,
//! sections 2–5; programming-model.md,
//! sections 4.4, 5.3 and 7; testing-strategy.md, section 4).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod api;
mod domain;
mod limits;
mod respond;

pub use limits::worst_case;

pub use domain::{Config, Domain, Event, MAX_OUT, Request, fire, step};
