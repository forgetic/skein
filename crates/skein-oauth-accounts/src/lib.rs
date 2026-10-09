//! The owner's OAuth accounts (oauth.md, sections 6.1–6.4 and 6.6–6.7).
//! Keeps bounded records, held grants, refresh exchanges and monotonic deadlines;
//! knows no account names or durable owner-store implementation. `down` accepts
//! owner requests and keeper terminals, `up` routes socket events, `fire` advances
//! one child or deadline, and `next_deadline` lets the owner schedule it.
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]
extern crate alloc;

mod accounts;
mod boundary;
mod component;
mod exchange;
mod limits;

pub use accounts::{Account, Endpoint, Keeper, Transport};
pub use boundary::{Asked, Ends, Event, Failure, Keeping, Refusal, Request};
pub use component::{Component, Lower, LowerEvent, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_UP, MaxOut, Unusable};
pub use limits::{Limits, worst_case};

#[cfg(test)]
mod tests;
