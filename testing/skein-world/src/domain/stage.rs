//! Domain entrances driven under output backpressure (programming-model.md,
//! section 2; testing-strategy.md, section 2.2). Worlds supply time and call
//! the real step; this helper owns only input and output queues.

use alloc::collections::VecDeque;

use skein_lib::{Env, Queue, Time, Wall};

/// A domain as the shell drives it (programming-model.md, section 2): its
/// environment, the events on their way to it, and the queue its steps and
/// alarms emit into. A step or an alarm runs only while the queue has room for
/// the most one may emit, so a slow consumer holds events back instead of
/// overflowing the queue.
///
/// The world calls the domain itself, with `env` and `out`: for each event
/// [`Stage::next_event`] hands it, then for each alarm due while
/// [`Stage::has_room`]; and takes what the steps emitted from `out` at the end
/// of the iteration, before the domain's reclaim point.
#[derive(Debug)]
pub struct Stage<L, E, R> {
    /// Time and immutable domain limits supplied by the world for each step.
    pub env: Env<L>,
    /// FIFO events awaiting output room; the world bounds incoming work.
    pub inbox: VecDeque<E>,
    /// Owned requests emitted by steps, bounded by the constructor capacity.
    pub out: Queue<R>,
    /// The most one step or alarm may emit; keep no greater than `out.capacity()`.
    pub max_out: u32,
}

impl<L, E, R> Stage<L, E, R> {
    /// A stage for a domain under `limits`, one step or alarm of which emits
    /// at most `max_out` requests, with a queue of `capacity`.
    #[must_use]
    pub fn new(limits: L, max_out: u32, capacity: u32) -> Stage<L, E, R> {
        assert!(capacity >= max_out, "the queue has room for what one step may emit");
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
        Stage { env, inbox: VecDeque::new(), out: Queue::with_capacity(capacity), max_out }
    }

    /// Starts an iteration at `now`: every step in it sees the same time.
    pub fn tick(&mut self, now: Time) {
        self.env.now = now;
    }

    /// Sends `event` to the domain, after those on their way already.
    pub fn push(&mut self, event: E) {
        self.inbox.push_back(event);
    }

    /// The next event for the domain, if there is one and room for what its
    /// step may emit.
    pub fn next_event(&mut self) -> Option<E> {
        if self.has_room() { self.inbox.pop_front() } else { None }
    }

    /// Whether the queue has room for what one more step or alarm may emit.
    #[must_use]
    pub fn has_room(&self) -> bool {
        self.out.room() >= self.max_out
    }

    /// Whether events are waiting for the domain.
    #[must_use]
    pub fn has_events(&self) -> bool {
        !self.inbox.is_empty()
    }
}
