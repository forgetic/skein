//! The owner's OAuth accounts (oauth.md, sections 6.1–6.3 and 6.6–6.7).
//! Keeps bounded account records, holds and monotonic expiry deadlines; knows no
//! account names or credential source policy. `down` handles owner requests,
//! `fire` runs one due account, and `next_deadline` lets the owner schedule it.
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]
extern crate alloc;

mod accounts;
mod boundary;
mod component;
mod limits;

pub use accounts::Account;
pub use boundary::{Asked, Ends, Event, Failure, Refusal, Request};
pub use component::{Component, MAX_OUT_DOWN, MAX_OUT_FIRE, MaxOut, Unusable};
pub use limits::{Limits, worst_case};

#[cfg(test)]
mod tests;
