//! Bounded queues (consumers.md, 4): the records between stages, sized at
//! startup.

#![expect(clippy::disallowed_types, reason = "a queue is a VecDeque allocated once, at its final capacity")]

use alloc::collections::VecDeque;
use alloc::collections::vec_deque;
use core::mem::size_of;

/// A first-in, first-out queue that never grows past its capacity.
#[derive(Debug)]
pub struct Queue<T> {
    items: VecDeque<T>,
    capacity: u32,
}

impl<T> Queue<T> {
    #[must_use]
    pub fn with_capacity(capacity: u32) -> Queue<T> {
        let size = usize::try_from(capacity).expect("a u32 fits in a usize");
        Queue { items: VecDeque::with_capacity(size), capacity }
    }

    /// The heap a queue of `capacity` takes for its items, or `None` past a
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

    /// Appends an item to a queue that has room for it.
    ///
    /// This is how a step emits: the loop reserved `MAX_OUT` slots before
    /// calling it, so a full queue here is a bug, not an outcome.
    pub fn push(&mut self, item: T) {
        assert!(self.room() > 0, "the loop reserves room for every output");
        self.items.push_back(item);
    }

    /// Appends an item, or hands it back when the queue is full.
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        if self.room() == 0 {
            return Err(item);
        }
        self.items.push_back(item);
        Ok(())
    }

    /// Takes the oldest item.
    pub fn pop(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    /// The items, oldest first.
    #[must_use]
    pub fn iter(&self) -> vec_deque::Iter<'_, T> {
        self.items.iter()
    }
}

impl<'a, T> IntoIterator for &'a Queue<T> {
    type Item = &'a T;
    type IntoIter = vec_deque::Iter<'a, T>;

    fn into_iter(self) -> vec_deque::Iter<'a, T> {
        self.items.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::Queue;

    #[test]
    fn a_queue_is_fifo_and_bounded() {
        let mut queue = Queue::with_capacity(2);
        queue.push(1_u8);
        assert_eq!(queue.try_push(2), Ok(()));
        assert_eq!(queue.try_push(3), Err(3));
        assert_eq!(queue.room(), 0);
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    #[should_panic(expected = "the loop reserves room for every output")]
    fn pushing_past_the_reservation_is_a_bug() {
        let mut queue = Queue::with_capacity(0);
        queue.push(());
    }
}
