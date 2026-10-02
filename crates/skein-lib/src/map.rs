//! Bounded ordered maps (5.1): domain tables keyed by value, up to a limit.

use alloc::collections::BTreeMap;
use alloc::collections::btree_map::{self, Entry};
use core::borrow::Borrow;
use core::mem::{align_of, size_of};

use crate::btree;

/// An ordered map that holds at most a capacity fixed when it is made.
///
/// Inserting a new key into a full map hands the key and value back: that is
/// the refusal. Replacing the value of a key already present always works.
/// Lookups take the key borrowed, so a map keyed by `Box<[u8]>` is queried
/// with a `&[u8]`.
///
/// The capacity bounds the map, but unlike a slab or a list it is not
/// allocated up front: the B-tree allocates nodes as it fills (5.1), and
/// [`Map::worst_case`] counts those nodes.
#[derive(Debug)]
pub struct Map<K, V> {
    entries: BTreeMap<K, V>,
    capacity: u32,
}

impl<K: Ord, V> Map<K, V> {
    #[must_use]
    pub const fn with_capacity(capacity: u32) -> Map<K, V> {
        Map { entries: BTreeMap::new(), capacity }
    }

    /// The most heap a map of `capacity` entries takes, its tree nodes
    /// included, or `None` past a `u64`. What the keys and values own, such as
    /// the bytes of a `Box<[u8]>`, is the owner's to count.
    #[must_use]
    pub fn worst_case(capacity: u32) -> Option<u64> {
        btree::worst_case(capacity, size_of::<K>(), size_of::<V>(), align_of::<(K, V)>())
    }

    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    #[must_use]
    pub fn len(&self) -> u32 {
        u32::try_from(self.entries.len()).expect("no more entries than the capacity")
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.entries.contains_key(key)
    }

    #[must_use]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.entries.get(key)
    }

    #[must_use]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.entries.get_mut(key)
    }

    /// Maps `key` to `value`, returning the value it replaces, if any. Hands
    /// both back when `key` is new and the map is full. A replaced entry keeps
    /// the key it had, and drops the one given.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>, (K, V)> {
        let full = self.len() >= self.capacity;
        match self.entries.entry(key) {
            Entry::Occupied(mut entry) => Ok(Some(entry.insert(value))),
            Entry::Vacant(entry) => {
                if full {
                    return Err((entry.into_key(), value));
                }
                let _: &mut V = entry.insert(value);
                Ok(None)
            }
        }
    }

    /// Removes `key`, returning its value if it was present.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.entries.remove(key)
    }

    /// The entry with the least key.
    #[must_use]
    pub fn first(&self) -> Option<(&K, &V)> {
        self.entries.first_key_value()
    }

    /// The entry with the greatest key.
    #[must_use]
    pub fn last(&self) -> Option<(&K, &V)> {
        self.entries.last_key_value()
    }

    /// The entries, in key order.
    pub fn iter(&self) -> btree_map::Iter<'_, K, V> {
        self.entries.iter()
    }
}

impl<'a, K, V> IntoIterator for &'a Map<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = btree_map::Iter<'a, K, V>;

    fn into_iter(self) -> btree_map::Iter<'a, K, V> {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use super::Map;

    #[test]
    fn a_full_map_refuses_new_keys_but_replaces_values() {
        let mut map = Map::with_capacity(2);
        assert_eq!(map.insert('b', 2_u8), Ok(None));
        assert_eq!(map.insert('a', 1), Ok(None));
        assert_eq!(map.insert('c', 3), Err(('c', 3)));
        assert_eq!(map.insert('a', 10), Ok(Some(1)));
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&'a'), Some(&10));
        assert!(!map.contains_key(&'c'));
        assert_eq!(map.remove(&'b'), Some(2));
        assert_eq!(map.remove(&'b'), None);
        assert_eq!(map.insert('c', 3), Ok(None), "removing makes room");
    }

    #[test]
    fn entries_come_out_in_key_order() {
        let mut map = Map::with_capacity(4);
        for key in [3_u8, 1, 4, 2] {
            map.insert(key, u32::from(key) * 10).expect("room");
        }
        for (expected, (key, value)) in (1_u8..).zip(&map) {
            assert_eq!((*key, *value), (expected, u32::from(expected) * 10));
        }
        assert_eq!(map.first(), Some((&1, &10)));
        assert_eq!(map.last(), Some((&4, &40)));
        if let Some(value) = map.get_mut(&4) {
            *value = 0;
        }
        assert_eq!(map.iter().last(), Some((&4, &0)));
    }

    #[test]
    fn a_map_keyed_by_bytes_is_queried_by_a_slice() {
        let mut map: Map<Box<[u8]>, u8> = Map::with_capacity(1);
        map.insert(Box::from(&b"src/lib.rs"[..]), 7).expect("room");
        assert_eq!(map.get(&b"src/lib.rs"[..]), Some(&7));
        assert!(map.contains_key(&b"src/lib.rs"[..]));
        assert_eq!(map.get(&b"src"[..]), None);
        assert_eq!(map.remove(&b"src/lib.rs"[..]), Some(7));
    }

    #[test]
    fn a_zero_capacity_map_refuses_everything() {
        let mut map = Map::with_capacity(0);
        assert_eq!(map.insert(1_u8, ()), Err((1, ())));
        assert!(map.is_empty());
        assert_eq!(map.first(), None);
    }
}
