//! Bounded nesting (programming-model.md, section 8): what a parser keeps
//! instead of recursing, since the depth of nested input is the peer's choice.

#![expect(clippy::disallowed_types, reason = "a stack is a Vec allocated once, at its final capacity")]

use alloc::vec::Vec;
use core::mem::size_of;

/// A last-in, first-out stack that never grows past its capacity, and
/// refuses past it: input nested deeper than the capacity is the caller's to
/// refuse.
#[derive(Debug)]
pub struct Stack<T> {
    items: Vec<T>,
    capacity: u32,
}

impl<T> Stack<T> {
    #[must_use]
    pub fn with_capacity(capacity: u32) -> Stack<T> {
        let size = usize::try_from(capacity).expect("a u32 fits in a usize");
        Stack { items: Vec::with_capacity(size), capacity }
    }

    /// The heap a stack of `capacity` takes for its items, or `None` past a
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

    /// Pushes an item on top, or hands it back when the stack is full.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.len() == self.capacity {
            return Err(item);
        }
        self.items.push(item);
        Ok(())
    }

    /// Takes the top item.
    pub fn pop(&mut self) -> Option<T> {
        self.items.pop()
    }

    /// The top item: the innermost level.
    #[must_use]
    pub fn top(&self) -> Option<&T> {
        self.items.last()
    }

    /// The top item, to change in place.
    #[must_use]
    pub fn top_mut(&mut self) -> Option<&mut T> {
        self.items.last_mut()
    }
}
