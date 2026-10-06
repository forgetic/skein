//! Shared bounded framed channels (channel.md §§1–7).
//!
//! A Machine owns an immutable checked Schema, one header/body/skip cell, one
//! raw-body receipt awaiting wrapper decode, a fixed frame queue, its C/P/S
//! admission ledger and one native output right that survives logical stop in
//! a bounded retirement cell. `KnownKind` covers foreign and newer kinds; First
//! gates and source-order receive Terms are frozen before Ready (§§2–4).
//!
//! This crate knows no authentication, typed service-body meaning, domain
//! transition, timer, provider or kernel write progress. Concrete wrappers
//! validate payload version, legal service phase and exact typed body shape;
//! transports supply actual lib.md §§7–7.1 read answers/native right winners.
//! `IO`/`TLS` lifetime and buffers are the enclosing owner's responsibility (§6).
//!
//! `Schema::new` validates slices and representability before owned copies;
//! `Machine::new` takes that schema. `up` consumes one lower fact, `down` admits
//! one parent request, and `resolve` settles exactly one body receipt. Their
//! Step tells the wrapper to decode first, run one final `poll`, or halt.
//! Neither consume nor resolve secretly runs an ordinary poll. `read_end`
//! consumes a wrapper-approved actual `EOF`, drops partial input and returns
//! Halt without a same-call poll or output/resource settlement (§§3,5).
//!
//! The caller reserves `MAX_UP=2` upper records and `MAX_DOWN=3` lower records for
//! the aggregate consume/decode/resolve/single-poll entrance. A normal poll
//! emits at most one native Send, one read-only Demand and one whole-cap Room;
//! fatal retirement never stacks another normal poll. Frame owner Sent means
//! queue admission; the native grant is affine and promises no peer/durable
//! receipt. Logical Closed is immediate; `is_retired` additionally requires the
//! actual named winner and enclosing resource Closed (§§4–6).
//!
//! `FrameWriter` measures/checks before one exact final frame allocation and
//! writes header/body directly into it. `Schema::worst_case` uses checked exact
//! owning schema/queue layouts, C frame bytes, the all-kind raw maximum, one
//! max(8,chunk) delivery, bounded common decode and actual outward scratch.
//! Inline host state, caller typed/Encoded ownership and `IO`/`TLS` buffers are
//! priced separately; raw+decoded+encoded can coexist and require sums (§7).
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod codec;
mod machine;
mod records;
mod schema;

pub use codec::{Encoded, FrameWriter, Header, framing};
pub use machine::{Machine, down, poll, resolve, up};
pub use records::{
    Disposition, Event, Fault, LowerEvent, LowerRequest, MAX_DOWN, MAX_UP, Opening, Phase, Refusal, Request, Step,
};
pub use schema::{
    Direction, First, KindRule, KnownKind, Limits, OpeningMode, OpeningProfile, Role, Schema, VersionRule,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_support;
