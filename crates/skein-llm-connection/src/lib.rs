//! Bounded LLM calls owned by a service's protocol layer (llm-connection.md,
//! sections 1 to 7). This component holds configured endpoints, physical
//! connections, live calls and bounded routing work. It never knows the
//! service's domain, credential store or retry policy. [`Component::down`]
//! takes calls; [`Component::up`] takes io events by the component's token;
//! [`Component::fire`] drains ready work and closes idle bindings. The owner
//! calls [`Component::reclaim`] at its iteration's reclaim point. Each entry
//! reserves [`MAX_OUT`] in both output queues first.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod call;
mod component;
mod deadlines;
mod endpoint;
mod limits;
#[cfg(test)]
mod tests;

pub use boundary::{Deadlines, Event, Lower, LowerEvent, Refusal, Request};
pub use component::{Component, MAX_OUT, MaxOut};
pub use endpoint::{Endpoint, EndpointError};
pub use limits::{Limits, worst_case};
