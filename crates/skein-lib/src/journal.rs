//! A bounded commit barrier for writes and outputs (lib.md, section 11).
//!
//! The journal keeps accepted commits, their numbers, and outputs held for
//! durability. It never inspects writes or outputs and never knows whether a
//! store has made a commit durable until its owner says so. Admission and
//! acceptance are separate: a decision reserves its room before the owner
//! changes state, then acceptance makes at most one commit.

use crate::Queue;

/// Fixed capacities for a journal (lib.md, section 11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalLimits {
    /// Commits made but not yet answered.
    pub commits: u32,
    /// Writes in one commit.
    pub writes: u32,
    /// Outputs held across decisions.
    pub held: u32,
    /// Untagged outputs at the door.
    pub now: u32,
    /// Outputs released by one call.
    pub release: u32,
}

/// The most one decision may write and hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalRoom {
    /// The decision's maximum writes.
    pub writes: u32,
    /// The decision's maximum held outputs.
    pub held: u32,
}

/// One accepted decision's writes, numbered for the store.
#[derive(Debug)]
pub struct Commit<W> {
    /// The journal's increasing commit number.
    pub number: u64,
    /// The writes the store applies together, in order.
    pub writes: Queue<W>,
}

/// One decision's reserved writes and held outputs.
#[must_use]
#[derive(Debug)]
pub struct Decision<W, O> {
    room: JournalRoom,
    writes: Queue<W>,
    held: Queue<O>,
    overrun: bool,
}

impl<W, O> Decision<W, O> {
    /// Adds a write, returning it and marking an overrun past the room.
    pub fn write(&mut self, write: W) -> Result<(), W> {
        let result = self.writes.try_push(write);
        if result.is_err() {
            self.overrun = true;
        }
        result
    }

    /// Holds an output, returning it and marking an overrun past the room.
    pub fn hold(&mut self, output: O) -> Result<(), O> {
        let result = self.held.try_push(output);
        if result.is_err() {
            self.overrun = true;
        }
        result
    }
}

/// A counted, bounded commit barrier (lib.md, section 11).
#[derive(Debug)]
pub struct Journal<W, O> {
    limits: JournalLimits,
    pending: Queue<Commit<W>>,
    outstanding: Queue<u64>,
    held: Queue<O>,
    reserved: Option<JournalRoom>,
    last_number: u64,
    stopped: bool,
}

impl<W, O> Journal<W, O> {
    /// Allocates the journal's fixed-capacity containers.
    #[must_use]
    pub fn new(limits: &JournalLimits) -> Journal<W, O> {
        Journal {
            limits: *limits,
            pending: Queue::with_capacity(limits.commits),
            outstanding: Queue::with_capacity(limits.commits),
            held: Queue::with_capacity(limits.held),
            reserved: None,
            last_number: 0,
            stopped: false,
        }
    }

    /// Checks a decision's room without changing the journal.
    #[must_use]
    pub fn takes(&self, room: &JournalRoom) -> bool {
        if self.stopped || self.reserved.is_some() || room.writes > self.limits.writes {
            return false;
        }
        if room.held > self.held.room() {
            return false;
        }
        if room.writes > 0 && self.outstanding.room() == 0 {
            return false;
        }
        room.writes == 0 || self.last_number < u64::MAX
    }

    /// Reserves the room for one decision, or refuses before state changes.
    pub fn decision(&mut self, room: &JournalRoom) -> Option<Decision<W, O>> {
        if !self.takes(room) {
            return None;
        }
        self.reserved = Some(*room);
        Some(Decision {
            room: *room,
            writes: Queue::with_capacity(room.writes),
            held: Queue::with_capacity(room.held),
            overrun: false,
        })
    }

    /// Accepts one decision; an overrun stops the journal before emission.
    pub fn accept(&mut self, mut decision: Decision<W, O>) {
        assert_eq!(self.reserved, Some(decision.room), "accept the reserved decision");
        self.reserved = None;
        if decision.overrun {
            self.stopped = true;
            return;
        }
        if !decision.writes.is_empty() {
            self.last_number = self.last_number.checked_add(1).expect("admission checked numbering");
            self.outstanding.push(self.last_number);
            self.pending.push(Commit { number: self.last_number, writes: decision.writes });
        }
        while let Some(output) = decision.held.pop() {
            self.held.push(output);
        }
    }

    /// Takes the next numbered commit for the store, in order.
    pub fn commit(&mut self) -> Option<Commit<W>> {
        self.pending.pop()
    }

    /// Whether an overrun has stopped admission and emission.
    #[must_use]
    pub const fn stopped(&self) -> bool {
        self.stopped
    }

    /// Prices fixed containers and one decision's reserved buffers.
    #[must_use]
    pub fn worst_case(limits: &JournalLimits) -> Option<u64> {
        let commits = Queue::<Commit<W>>::worst_case(limits.commits)?;
        let outstanding = Queue::<u64>::worst_case(limits.commits)?;
        let held = Queue::<O>::worst_case(limits.held)?;
        let writes = Queue::<W>::worst_case(limits.writes)?.checked_mul(u64::from(limits.commits).checked_add(1)?)?;
        let decision_held = Queue::<O>::worst_case(limits.held)?;
        commits.checked_add(outstanding)?.checked_add(held)?.checked_add(writes)?.checked_add(decision_held)
    }
}
