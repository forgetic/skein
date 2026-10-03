//! Entity storage (programming-model.md, 5.1 and 6.1): a slab sized at startup,
//! whose capacity is the admission limit for its entity kind.

#![expect(
    clippy::disallowed_types,
    reason = "a slab's slots and lists are Vecs allocated once, at their final capacity"
)]

use alloc::vec::Vec;
use core::mem::size_of;

use crate::Id;

/// Entities of one kind, named by `Id<T>`.
///
/// Inserting into a full slab hands the value back: that is the refusal.
/// Retiring an entity marks it; it stays reachable through its handle until
/// `reclaim`, which the loop calls once per iteration, so every entity present
/// when an iteration begins can be looked up until the iteration ends.
#[derive(Debug)]
pub struct Slab<T> {
    slots: Vec<Slot<T>>,
    /// Vacant slots, the next one to fill last.
    free: Vec<u32>,
    /// Slots retired since the last reclaim.
    retired: Vec<u32>,
    capacity: u32,
    len: u32,
}

#[derive(Debug)]
enum Slot<T> {
    /// The next entity here gets `generation`.
    Vacant {
        generation: u32,
    },
    Occupied {
        generation: u32,
        retired: bool,
        value: T,
    },
    /// Its generation would wrap: never used again, so no handle is reused.
    Spent,
}

impl<T> Slab<T> {
    #[must_use]
    pub fn with_capacity(capacity: u32) -> Slab<T> {
        let size = index(capacity);
        let mut slots = Vec::with_capacity(size);
        let mut free = Vec::with_capacity(size);
        for _ in 0..capacity {
            slots.push(Slot::Vacant { generation: 0 });
        }
        for slot in (0..capacity).rev() {
            free.push(slot);
        }
        Slab { slots, free, retired: Vec::with_capacity(size), capacity, len: 0 }
    }

    /// The heap a slab of `capacity` takes itself, its slots and its lists of
    /// free and retired slots, or `None` past a `u64`. What its entities own
    /// is theirs to count.
    #[must_use]
    pub fn worst_case(capacity: u32) -> Option<u64> {
        let slot = u64::try_from(size_of::<Slot<T>>()).ok()?;
        let lists = u64::try_from(size_of::<u32>()).ok()?.checked_mul(2)?;
        u64::from(capacity).checked_mul(slot.checked_add(lists)?)
    }

    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Entities present, retired ones included until they are reclaimed.
    #[must_use]
    pub const fn len(&self) -> u32 {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether an insert would be refused.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.free.is_empty()
    }

    /// Stores `value` and names it, or hands it back when the slab is full.
    pub fn insert(&mut self, value: T) -> Result<Id<T>, T> {
        let Some(slot) = self.free.pop() else {
            return Err(value);
        };
        let entry = self.slots.get_mut(index(slot)).expect("free slots are in range");
        let generation = match entry {
            Slot::Vacant { generation } => *generation,
            Slot::Occupied { .. } | Slot::Spent => unreachable!("the free list holds vacant slots only"),
        };
        *entry = Slot::Occupied { generation, retired: false, value };
        self.len = self.len.checked_add(1).expect("no more entities than slots");
        Ok(Id::new(slot, generation))
    }

    /// The entity `id` names, or `None` if it has been reclaimed.
    #[must_use]
    pub fn get(&self, id: Id<T>) -> Option<&T> {
        match self.slots.get(index(id.slot()))? {
            Slot::Occupied { generation, retired: _, value } => (*generation == id.generation()).then_some(value),
            Slot::Vacant { .. } | Slot::Spent => None,
        }
    }

    /// The entity `id` names, or `None` if it has been reclaimed.
    #[must_use]
    pub fn get_mut(&mut self, id: Id<T>) -> Option<&mut T> {
        match self.slots.get_mut(index(id.slot()))? {
            Slot::Occupied { generation, retired: _, value } => (*generation == id.generation()).then_some(value),
            Slot::Vacant { .. } | Slot::Spent => None,
        }
    }

    /// Marks a live entity for reclaiming. It stays reachable until
    /// `reclaim`. Retiring anything else is a bug.
    pub fn retire(&mut self, id: Id<T>) {
        let entry = self.slots.get_mut(index(id.slot())).expect("a handle names a slot of its slab");
        match entry {
            Slot::Occupied { generation, retired, value: _ } => {
                assert!(*generation == id.generation(), "only a live entity is retired");
                assert!(!*retired, "an entity is retired once");
                *retired = true;
            }
            Slot::Vacant { .. } | Slot::Spent => unreachable!("only a live entity is retired"),
        }
        self.retired.push(id.slot());
    }

    /// Frees the slots of every retired entity: the reclaim point, once per
    /// iteration, after every step.
    pub fn reclaim(&mut self) {
        // Bounded: each slot is retired at most once between reclaims.
        while let Some(slot) = self.retired.pop() {
            let entry = self.slots.get_mut(index(slot)).expect("retired slots are in range");
            let next = match entry {
                Slot::Occupied { generation, retired: true, value: _ } => generation.checked_add(1),
                Slot::Occupied { retired: false, .. } | Slot::Vacant { .. } | Slot::Spent => {
                    unreachable!("the retired list holds retired entities only")
                }
            };
            *entry = match next {
                Some(generation) => {
                    self.free.push(slot);
                    Slot::Vacant { generation }
                }
                None => Slot::Spent,
            };
            self.len = self.len.checked_sub(1).expect("a retired entity was counted");
        }
    }
}

fn index(slot: u32) -> usize {
    usize::try_from(slot).expect("a u32 fits in a usize")
}

#[cfg(test)]
mod tests {
    use super::Slab;

    #[test]
    fn a_full_slab_hands_the_value_back() {
        let mut slab = Slab::with_capacity(1);
        let id = slab.insert('a').expect("room for one");
        assert_eq!(slab.insert('b'), Err('b'));
        assert_eq!(slab.get(id), Some(&'a'));
        assert!(slab.is_full());
    }

    #[test]
    fn a_retired_entity_is_reachable_until_reclaimed_and_its_handle_goes_stale() {
        let mut slab = Slab::with_capacity(1);
        let old = slab.insert('a').expect("room for one");
        slab.retire(old);
        assert_eq!(slab.get(old), Some(&'a'));
        assert_eq!(slab.len(), 1);
        slab.reclaim();
        assert_eq!(slab.get(old), None);
        assert!(slab.is_empty());
        let new = slab.insert('b').expect("the slot was freed");
        assert_ne!(new, old);
        assert_eq!(slab.get(old), None);
        assert_eq!(slab.get_mut(new), Some(&mut 'b'));
    }

    #[test]
    #[should_panic(expected = "an entity is retired once")]
    fn retiring_twice_is_a_bug() {
        let mut slab = Slab::with_capacity(1);
        let id = slab.insert(()).expect("room for one");
        slab.retire(id);
        slab.retire(id);
    }

    #[test]
    fn a_zero_capacity_slab_refuses_everything() {
        let mut slab = Slab::with_capacity(0);
        assert_eq!(slab.insert(1_u8), Err(1));
    }
}
