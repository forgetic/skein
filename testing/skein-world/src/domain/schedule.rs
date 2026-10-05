//! Deterministic delivery order and cancellation (testing-strategy.md,
//! sections 3 and 6). Worlds own payloads and clock; this scheduler names
//! each delivery and emits it at most once, without performing IO.

use alloc::collections::BTreeMap;

use skein_lib::Time;

/// Where a delivery stands on a [`Schedule`]: the time it is due at, and its
/// place among those due then. It withdraws the delivery while it is in
/// flight.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Key {
    /// Delivery deadline from the world’s injected clock.
    pub at: Time,
    /// Unique order issued by its schedule; use only the returned key to withdraw.
    pub serial: u64,
}

/// Deliveries in flight, each handed over once it is due: by time, and those
/// due at the same time in the order they were sent.
///
/// The count that orders them also names whatever else a world names
/// (sessions, calls, roots), so that names follow the order things happened
/// in, and a seed replays to the same names.
#[derive(Debug)]
pub struct Schedule<D> {
    due: BTreeMap<Key, D>,
    serial: u64,
}

impl<D> Schedule<D> {
    /// Creates an empty schedule whose first issued name is one.
    #[must_use]
    pub fn new() -> Schedule<D> {
        Schedule { due: BTreeMap::new(), serial: 0 }
    }

    /// A name not given before.
    pub fn name(&mut self) -> u64 {
        self.serial = self.serial.checked_add(1).expect("schedule names are not exhausted");
        self.serial
    }

    /// Sends `delivery`, due at `at`, after those sent for then already.
    /// Returns its key, to withdraw it by.
    pub fn send(&mut self, at: Time, delivery: D) -> Key {
        let key = Key { at, serial: self.name() };
        self.due.insert(key, delivery);
        key
    }

    /// Withdraws the delivery of `key`, if it is still in flight.
    pub fn withdraw(&mut self, key: Key) -> Option<D> {
        self.due.remove(&key)
    }

    /// Takes the next delivery due by `now`.
    pub fn next(&mut self, now: Time) -> Option<D> {
        let entry = self.due.first_entry()?;
        if entry.key().at > now {
            return None;
        }
        Some(entry.remove())
    }

    /// Whether a delivery is due by `now`.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        self.next_time().is_some_and(|at| at <= now)
    }

    /// When the next delivery is due.
    #[must_use]
    pub fn next_time(&self) -> Option<Time> {
        self.due.first_key_value().map(|(key, _)| key.at)
    }

    /// Whether nothing is in flight.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.due.is_empty()
    }

    /// How many deliveries are in flight: what a world caps, so that a run
    /// that never settles fails fast instead of growing.
    #[must_use]
    pub fn len(&self) -> usize {
        self.due.len()
    }
}

impl<D> Default for Schedule<D> {
    fn default() -> Schedule<D> {
        Schedule::new()
    }
}
