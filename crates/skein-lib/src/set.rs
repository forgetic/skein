//! Bounded ordered sets (programming-model.md, 6.1): a [`Map`](crate::Map)
//! without values.

use alloc::collections::BTreeSet;
use alloc::collections::btree_set;
use core::borrow::Borrow;
use core::mem::{align_of, size_of};

use crate::btree;

/// An ordered set that holds at most a capacity fixed when it is made.
///
/// Inserting a new key into a full set hands it back; inserting one already
/// present always works. Like a [`Map`](crate::Map), it allocates tree nodes
/// as it fills, and [`Set::worst_case`] counts them.
#[derive(Debug)]
pub struct Set<K> {
    keys: BTreeSet<K>,
    capacity: u32,
}

impl<K: Ord> Set<K> {
    #[must_use]
    pub const fn with_capacity(capacity: u32) -> Set<K> {
        Set { keys: BTreeSet::new(), capacity }
    }

    /// The most heap a set of `capacity` keys takes, its tree nodes included,
    /// or `None` past a `u64`. What the keys own is the owner's to count.
    #[must_use]
    pub fn worst_case(capacity: u32) -> Option<u64> {
        // A set is a map whose values take no room.
        btree::worst_case(capacity, size_of::<K>(), 0, align_of::<K>())
    }

    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    #[must_use]
    pub fn len(&self) -> u32 {
        u32::try_from(self.keys.len()).expect("no more keys than the capacity")
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[must_use]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.keys.contains(key)
    }

    /// Adds `key`, saying whether it is new, or hands it back when it is new
    /// and the set is full. A key already present stays, and the one given is
    /// dropped.
    pub fn insert(&mut self, key: K) -> Result<bool, K> {
        if self.len() >= self.capacity && !self.keys.contains(&key) {
            return Err(key);
        }
        Ok(self.keys.insert(key))
    }

    /// Removes `key`, saying whether it was present.
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.keys.remove(key)
    }

    /// The least key.
    #[must_use]
    pub fn first(&self) -> Option<&K> {
        self.keys.first()
    }

    /// The greatest key.
    #[must_use]
    pub fn last(&self) -> Option<&K> {
        self.keys.last()
    }

    /// Removes the least key, and hands it over.
    pub fn pop_first(&mut self) -> Option<K> {
        self.keys.pop_first()
    }

    /// The keys, in order.
    pub fn iter(&self) -> btree_set::Iter<'_, K> {
        self.keys.iter()
    }
}

impl<'a, K> IntoIterator for &'a Set<K> {
    type Item = &'a K;
    type IntoIter = btree_set::Iter<'a, K>;

    fn into_iter(self) -> btree_set::Iter<'a, K> {
        self.keys.iter()
    }
}
