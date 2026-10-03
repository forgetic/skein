//! Bounded sequences: what a step appends to, up to a limit.

#![expect(clippy::disallowed_types, reason = "a list is a Vec allocated once, at its final capacity")]

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::mem::size_of;
use core::slice;

/// A sequence that grows up to a capacity fixed when it is made, and refuses
/// past it. The capacity is allocated up front, so pushing never reallocates.
#[derive(Debug)]
pub struct List<T> {
    items: Vec<T>,
    capacity: u32,
}

impl<T> List<T> {
    #[must_use]
    pub fn with_capacity(capacity: u32) -> List<T> {
        let size = usize::try_from(capacity).expect("a u32 fits in a usize");
        List { items: Vec::with_capacity(size), capacity }
    }

    /// The heap a list of `capacity` takes for its items, or `None` past a
    /// `u64`. What the items own is theirs to count.
    #[must_use]
    pub fn worst_case(capacity: u32) -> Option<u64> {
        u64::try_from(size_of::<T>()).ok()?.checked_mul(u64::from(capacity))
    }

    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    #[must_use]
    pub fn len(&self) -> u32 {
        u32::try_from(self.items.len()).expect("no longer than its capacity")
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// How many more items fit.
    #[must_use]
    pub fn room(&self) -> u32 {
        self.capacity.checked_sub(self.len()).expect("no longer than its capacity")
    }

    /// Appends an item, or hands it back when the list is full.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.room() == 0 {
            return Err(item);
        }
        self.items.push(item);
        Ok(())
    }

    /// The item at `index`, counted from the first pushed.
    #[must_use]
    pub fn get(&self, index: u32) -> Option<&T> {
        self.items.get(usize::try_from(index).ok()?)
    }

    /// The item at `index`, to change in place: a list of slots pushed in
    /// order can be filled in any order.
    #[must_use]
    pub fn get_mut(&mut self, index: u32) -> Option<&mut T> {
        self.items.get_mut(usize::try_from(index).ok()?)
    }

    #[must_use]
    pub fn last(&self) -> Option<&T> {
        self.items.last()
    }

    /// Empties the list, keeping its capacity: a list refilled in place
    /// allocates nothing.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.items
    }

    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.items.iter()
    }

    /// The items, moved into a box of exactly their number.
    #[must_use]
    pub fn into_boxed(self) -> Box<[T]> {
        self.items.into_boxed_slice()
    }

    /// A copy of the items in a box of exactly their number: copy at emission.
    #[must_use]
    pub fn to_boxed(&self) -> Box<[T]>
    where
        T: Clone,
    {
        Box::from(self.items.as_slice())
    }
}

impl<'a, T> IntoIterator for &'a List<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> slice::Iter<'a, T> {
        self.items.iter()
    }
}
