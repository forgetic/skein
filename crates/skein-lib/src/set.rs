//! Bounded ordered sets (5.1): a [`Map`](crate::Map) without values.

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

#[cfg(test)]
mod tests {
    use super::Set;

    #[test]
    fn a_full_set_refuses_new_keys_only() {
        let mut set = Set::with_capacity(2);
        assert_eq!(set.insert(2_u8), Ok(true));
        assert_eq!(set.insert(1), Ok(true));
        assert_eq!(set.insert(3), Err(3));
        assert_eq!(set.insert(1), Ok(false), "a key already present takes no new room");
        assert!(set.contains(&2));
        assert_eq!((set.first(), set.last()), (Some(&1), Some(&2)));
        for (expected, key) in (1_u8..).zip(&set) {
            assert_eq!(*key, expected);
        }
        assert!(set.remove(&1));
        assert!(!set.remove(&1));
        assert_eq!(set.insert(3), Ok(true), "removing makes room");
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn the_least_key_is_taken_first_and_makes_room() {
        let mut set = Set::with_capacity(2);
        assert_eq!(set.pop_first(), None);
        assert_eq!(set.insert(5_u8), Ok(true));
        assert_eq!(set.insert(4), Ok(true));
        assert_eq!(set.pop_first(), Some(4));
        assert_eq!(set.insert(6), Ok(true), "taking a key makes room");
        assert_eq!(set.pop_first(), Some(5));
        assert_eq!(set.pop_first(), Some(6));
        assert!(set.is_empty());
    }

    #[test]
    fn a_zero_capacity_set_refuses_everything() {
        let mut set = Set::with_capacity(0);
        assert_eq!(set.insert('a'), Err('a'));
        assert!(set.is_empty());
    }
}
