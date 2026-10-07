//! Bounded LLM calls owned by a service's protocol layer (llm-connection.md,
//! sections 1 to 7). This component holds configured endpoints, live calls,
//! admission bounds. It never knows the service's domain, credential store
//! or retry policy. [`Component::admit`] prepares one call without touching
//! the stream. The connection machine will own admitted calls and deadlines.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod component;
mod endpoint;
mod limits;
#[cfg(test)]
mod tests;

pub use boundary::{Deadlines, Event, Lower, LowerEvent, Refusal, Request};
pub use component::Component;
pub use endpoint::{Endpoint, EndpointError};
pub use limits::{Limits, worst_case};
