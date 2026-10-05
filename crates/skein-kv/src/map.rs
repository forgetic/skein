//! Budgeted ordered map. The fixed charge per entry includes B-tree nodes.

#![expect(clippy::disallowed_types, reason = "returned pages are bounded by Page")]
#![expect(clippy::disallowed_methods, reason = "pure lookups and bounded admission scans have no hidden effects")]
#![expect(clippy::arithmetic_side_effects, reason = "the signed delta sums at most the bounded commit queue")]

use crate::{Limits, Op, Page, Range, Refusal, Row};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::ops::Bound;
use skein_lib::Map;

/// A conservative charge for tree nodes, key and value boxes and allocator
/// metadata. Payload lengths are charged separately.
pub(crate) const ENTRY: u64 = 128;

#[derive(Debug)]
pub(crate) struct PageRows {
    pub(crate) rows: Box<[Row]>,
    pub(crate) next: Option<Box<[u8]>>,
}

#[derive(Debug)]
pub(crate) struct KvMap {
    entries: Map<Box<[u8]>, Box<[u8]>>,
    used: u64,
}

impl KvMap {
    pub(crate) fn new(limits: &Limits) -> KvMap {
        let possible = limits.budget / ENTRY;
        let capacity = u32::try_from(possible).unwrap_or(u32::MAX);
        KvMap { entries: Map::with_capacity(capacity), used: 0 }
    }

    #[must_use]
    pub(crate) fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries.get(key).map(Box::as_ref)
    }

    #[must_use]
    pub(crate) fn used(&self) -> u64 {
        self.used
    }

    #[must_use]
    pub(crate) fn len(&self) -> u32 {
        self.entries.len()
    }

    pub(crate) fn after<'a>(
        &'a self,
        cursor: Option<&'a [u8]>,
    ) -> impl Iterator<Item = (&'a Box<[u8]>, &'a Box<[u8]>)> {
        let lower = match cursor {
            Some(key) => Bound::Excluded(key),
            None => Bound::Unbounded,
        };
        self.entries.range::<[u8], _>((lower, Bound::Unbounded))
    }

    pub(crate) fn check(&self, ops: &[Op], limits: &Limits) -> Result<u64, Refusal> {
        let mut after = i128::from(self.used);
        let mut count = i128::from(self.entries.len());
        for (index, op) in ops.iter().enumerate() {
            let key = op.key();
            if key.len() > usize::try_from(limits.key).expect("u32 fits usize") {
                return Err(Refusal::TooLarge);
            }
            if let Op::Put { value, .. } = op
                && value.len() > usize::try_from(limits.value).expect("u32 fits usize")
            {
                return Err(Refusal::TooLarge);
            }
            // Only the last operation on a key determines its final charge.
            if ops.get(index + 1..).expect("index is within ops").iter().any(|later| later.key() == key) {
                continue;
            }
            if let Some(old) = self.entries.get(key) {
                let charge = ENTRY
                    .checked_add(u64::try_from(key.len()).expect("length fits u64"))
                    .and_then(|v| v.checked_add(u64::try_from(old.len()).expect("length fits u64")))
                    .ok_or(Refusal::Full)?;
                after -= i128::from(charge);
                count -= 1;
            }
            if let Op::Put { value, .. } = op {
                let charge = ENTRY
                    .checked_add(u64::try_from(key.len()).expect("length fits u64"))
                    .and_then(|v| v.checked_add(u64::try_from(value.len()).expect("length fits u64")))
                    .ok_or(Refusal::Full)?;
                after += i128::from(charge);
                count += 1;
            }
        }
        if after < 0 || after > i128::from(limits.budget) || count > i128::from(self.entries.capacity()) {
            return Err(Refusal::Full);
        }
        u64::try_from(after).map_err(|_| Refusal::Full)
    }

    pub(crate) fn apply(&mut self, ops: &[Op], used: u64) {
        // Remove first so an atomic batch whose final map fits is never
        // refused by a transient extra entry. Only each key's last op wins.
        for (index, op) in ops.iter().enumerate() {
            if ops.get(index + 1..).expect("index is within ops").iter().any(|later| later.key() == op.key()) {
                continue;
            }
            drop(self.entries.remove(op.key()));
        }
        for (index, op) in ops.iter().enumerate() {
            if ops.get(index + 1..).expect("index is within ops").iter().any(|later| later.key() == op.key()) {
                continue;
            }
            if let Op::Put { key, value } = op {
                let old = self.entries.insert(key.clone(), value.clone());
                assert!(old.is_ok(), "a checked commit fits the map");
            }
        }
        self.used = used;
    }

    pub(crate) fn page(&self, range: &Range, max: Page) -> Result<PageRows, Refusal> {
        let mut rows = Vec::new();
        let mut bytes: u32 = 0;
        let mut next = None;
        let end = match range.end.as_ref() {
            Some(key) => Bound::Excluded(key.as_ref()),
            None => Bound::Unbounded,
        };
        for (key, value) in self.entries.range::<[u8], _>((Bound::Included(range.start.as_ref()), end)) {
            let size = u32::try_from(key.len().saturating_add(value.len())).unwrap_or(u32::MAX);
            if rows.len() >= usize::try_from(max.rows).expect("u32 fits usize")
                || bytes.checked_add(size).is_none_or(|total| total > max.bytes)
            {
                if rows.is_empty() {
                    return Err(Refusal::TooLarge);
                }
                next = Some(key.clone());
                break;
            }
            bytes = bytes.checked_add(size).expect("checked row size");
            rows.push(Row { key: key.clone(), value: value.clone() });
        }
        Ok(PageRows { rows: rows.into_boxed_slice(), next })
    }
}
