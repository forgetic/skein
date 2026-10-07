//! A checked heap bound for one channel (channel.md, section 10).
//! It prices the owning schema, queue slots and bytes, an in-progress body,
//! bounded control reservations, terms, and the caller's event/lower queues.
//! It never counts a transport's intake or buffers as channel ownership.
use core::mem::size_of;
use skein_lib::{List, Queue};

use crate::machine::Queued;
use crate::{Event, Kind, Limits, Lower, MAX_DOWN, MAX_UP, Machine, Schema, Term, Version};

/// The most channel-owned heap at the configured ceilings.
#[must_use]
pub fn worst_case(schema: &Schema, limits: &Limits) -> Option<u64> {
    schema.check(limits).ok()?;
    let queue_capacity = limits.output_frames.checked_add(3)?;
    let mut schema_heap = List::<Version>::worst_case(schema.versions.capacity())?;
    let mut largest_body = 0_u32;
    for version in &schema.versions {
        schema_heap = schema_heap.checked_add(List::<Kind>::worst_case(version.kinds.capacity())?)?;
        for kind in &version.kinds {
            largest_body = largest_body.max(kind.largest);
        }
    }
    let terms_body = limits.kinds.checked_mul(6)?.checked_add(4)?;
    let open_body = limits.credential.checked_add(14)?;
    let control_body = open_body.max(terms_body).max(262);
    let control_frame = u64::from(control_body.checked_add(8)?);
    let control_reserve = control_frame.checked_mul(3)?;
    let body = u64::from(largest_body.max(control_body));
    let terms = List::<Term>::worst_case(limits.kinds)?.checked_mul(2)?;
    let queues = Queue::<Queued>::worst_case(queue_capacity)?
        .checked_add(Queue::<Event>::worst_case(MAX_UP)?)?
        .checked_add(Queue::<Lower>::worst_case(MAX_DOWN)?)?;
    let state = u64::try_from(size_of::<Machine>()).ok()?;
    schema_heap
        .checked_add(queues)?
        .checked_add(u64::from(limits.output_bytes))?
        .checked_add(control_reserve)?
        .checked_add(body)?
        .checked_add(u64::from(limits.chunk.max(8)))?
        .checked_add(u64::from(limits.credential))?
        .checked_add(terms)?
        .checked_add(state)
}
