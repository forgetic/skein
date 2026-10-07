//! A bounded commit barrier for writes and outputs (lib.md, section 11).
//!
//! The journal keeps accepted commits, their numbers, outputs held for
//! durability, and untagged outputs at its door. It never inspects writes or
//! outputs, and it learns durability only when its owner answers a commit.
//! `takes` and `decision` reserve room before the owner changes state;
//! `accept` makes at most one commit. The owner sends `commit` values to its
//! store, answers them with `committed` or `failed`, then calls `release`.
//! `from_durable` resumes numbering after a durable commit on restart.
//!
//! Commit transitions (lib.md, section 11):
//!
//! | State | Event | Next | What becomes available |
//! |---|---|---|---|
//! | open | `commit` | sent | the writes go to the owner |
//! | sent | `committed(n)` | durable | output tagged at most `n` |
//! | sent | `failed(n)` | failed | nothing tagged at or after `n`; admission stops |
//!
//! A decision without writes makes no commit. Its outputs follow the last
//! commit made, or number zero, which is already durable.

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

/// Whether release moved outputs, found none ready, or found a stopped journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Released {
    /// At least one output moved to the caller's queue.
    Some,
    /// No output was ready or the caller's queue had no room.
    None,
    /// The journal stopped after a failure or decision overrun.
    Stopped,
}

#[derive(Debug)]
struct Held<O> {
    after: u64,
    output: O,
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
    held: Queue<Held<O>>,
    now: Queue<O>,
    reserved: Option<JournalRoom>,
    last_number: u64,
    sent: u64,
    durable: u64,
    stopped: bool,
}

impl<W, O> Journal<W, O> {
    /// Allocates the journal's fixed-capacity containers.
    #[must_use]
    pub fn new(limits: &JournalLimits) -> Journal<W, O> {
        Self::from_durable(limits, 0)
    }

    /// Allocates an empty journal after the supplied durable commit number.
    #[must_use]
    pub fn from_durable(limits: &JournalLimits, durable: u64) -> Journal<W, O> {
        Journal {
            limits: *limits,
            pending: Queue::with_capacity(limits.commits),
            outstanding: Queue::with_capacity(limits.commits),
            held: Queue::with_capacity(limits.held),
            now: Queue::with_capacity(limits.now),
            reserved: None,
            last_number: durable,
            sent: durable,
            durable,
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

    /// Adds an untagged output at the door, returning it if the door is full.
    pub fn now(&mut self, output: O) -> Result<(), O> {
        if self.stopped {
            return Err(output);
        }
        self.now.try_push(output)
    }

    /// Accepts one decision; an overrun stops the journal before emission.
    pub fn accept(&mut self, mut decision: Decision<W, O>) {
        assert_eq!(self.reserved, Some(decision.room), "accept the reserved decision");
        self.reserved = None;
        if self.stopped {
            return;
        }
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
            self.held.push(Held { after: self.last_number, output });
        }
    }

    /// Takes the next numbered commit for the store, in order.
    pub fn commit(&mut self) -> Option<Commit<W>> {
        if self.stopped {
            return None;
        }
        let commit = self.pending.pop()?;
        self.sent = commit.number;
        Some(commit)
    }

    /// Marks a sent commit and every earlier one durable; invalid answers stop.
    pub fn committed(&mut self, number: u64) {
        if self.stopped {
            return;
        }
        if number <= self.durable || number > self.sent {
            self.stopped = true;
            return;
        }
        while let Some(front) = self.outstanding.iter().next() {
            if *front > number {
                break;
            }
            self.outstanding.pop().expect("front exists");
        }
        self.durable = number;
    }

    /// Stops after a failed sent commit; nothing tagged with it or later leaves.
    pub fn failed(&mut self, number: u64) {
        if self.stopped {
            return;
        }
        if number <= self.durable || number > self.sent {
            self.stopped = true;
            return;
        }
        self.stopped = true;
    }

    /// Moves at most the release limit of ready outputs into `out`.
    pub fn release(&mut self, out: &mut Queue<O>) -> Released {
        if self.stopped {
            return Released::Stopped;
        }
        let mut moved = 0;
        while moved < self.limits.release && out.room() > 0 {
            let held_ready = match self.held.iter().next() {
                Some(held) => held.after <= self.durable,
                None => false,
            };
            if let Some(output) = self.now.pop() {
                out.push(output);
                moved = moved.checked_add(1).expect("bounded by the release limit");
            } else if held_ready {
                let held = self.held.pop().expect("ready front exists");
                out.push(held.output);
                moved = moved.checked_add(1).expect("bounded by the release limit");
            } else {
                break;
            }
        }
        if moved > 0 { Released::Some } else { Released::None }
    }

    /// Whether an overrun has stopped admission and emission.
    #[must_use]
    pub const fn stopped(&self) -> bool {
        self.stopped
    }

    /// Whether the running journal has no open decision, commit, or output.
    #[must_use]
    pub fn idle(&self) -> bool {
        !self.stopped
            && self.reserved.is_none()
            && self.pending.is_empty()
            && self.outstanding.is_empty()
            && self.held.is_empty()
            && self.now.is_empty()
    }

    /// Prices fixed containers and one decision's reserved buffers.
    #[must_use]
    pub fn worst_case(limits: &JournalLimits) -> Option<u64> {
        let commits = Queue::<Commit<W>>::worst_case(limits.commits)?;
        let outstanding = Queue::<u64>::worst_case(limits.commits)?;
        let held = Queue::<Held<O>>::worst_case(limits.held)?;
        let now = Queue::<O>::worst_case(limits.now)?;
        let writes = Queue::<W>::worst_case(limits.writes)?.checked_mul(u64::from(limits.commits).checked_add(1)?)?;
        let decision_held = Queue::<O>::worst_case(limits.held)?;
        commits
            .checked_add(outstanding)?
            .checked_add(held)?
            .checked_add(now)?
            .checked_add(writes)?
            .checked_add(decision_held)
    }
}
