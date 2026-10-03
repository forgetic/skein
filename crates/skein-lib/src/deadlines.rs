//! The deadline table each layer keeps for its own timers
//! (programming-model.md, section 9).

use crate::{Map, Set, Time};

/// Timers, each named by a key of the owning layer's choosing, with at most
/// one timer per key.
///
/// Arm and cancel are synchronous. A stage fires its expired timers after its
/// input events, one `expire` at a time; firing removes the timer before its
/// handler runs, so a timer ends exactly once, fired or cancelled, and
/// cancelling one that already fired is a stale name, ignored.
///
/// Timers that fall due at the same time fire in key order.
///
/// The capacity bounds the table, but unlike a slab or a queue it is not
/// allocated up front: its two indexes are B-trees that allocate nodes as they
/// fill (programming-model.md, 6.1), and [`Deadlines::worst_case`] counts those
/// nodes.
#[derive(Debug)]
pub struct Deadlines<K> {
    by_time: Set<(Time, K)>,
    by_key: Map<K, Time>,
}

impl<K: Ord + Copy> Deadlines<K> {
    #[must_use]
    pub fn with_capacity(capacity: u32) -> Deadlines<K> {
        Deadlines { by_time: Set::with_capacity(capacity), by_key: Map::with_capacity(capacity) }
    }

    /// The most heap a table of `capacity` timers takes, or `None` past a
    /// `u64`.
    #[must_use]
    pub fn worst_case(capacity: u32) -> Option<u64> {
        Set::<(Time, K)>::worst_case(capacity)?.checked_add(Map::<K, Time>::worst_case(capacity)?)
    }

    #[must_use]
    pub fn len(&self) -> u32 {
        self.by_key.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Arms the timer `key` to fall due at `at`, moving it if it is already
    /// armed. Hands the key back when the table is full.
    pub fn arm(&mut self, key: K, at: Time) -> Result<(), K> {
        match self.by_key.insert(key, at) {
            Ok(Some(old)) => {
                let armed = self.by_time.remove(&(old, key));
                assert!(armed, "both indexes hold every timer");
            }
            Ok(None) => {}
            Err((key, _)) => return Err(key),
        }
        let Ok(fresh) = self.by_time.insert((at, key)) else {
            unreachable!("both indexes have the same capacity");
        };
        assert!(fresh, "both indexes hold every timer");
        Ok(())
    }

    /// Cancels the timer `key`, if it is armed.
    pub fn cancel(&mut self, key: K) {
        if let Some(at) = self.by_key.remove(&key) {
            let armed = self.by_time.remove(&(at, key));
            assert!(armed, "both indexes hold every timer");
        }
    }

    /// When the earliest timer falls due.
    #[must_use]
    pub fn next(&self) -> Option<Time> {
        let (at, _) = self.by_time.first()?;
        Some(*at)
    }

    /// Removes the earliest timer due at `now` and returns its key.
    pub fn expire(&mut self, now: Time) -> Option<K> {
        let &(at, key) = self.by_time.first()?;
        if at > now {
            return None;
        }
        let armed = self.by_time.remove(&(at, key));
        assert!(armed, "the first timer is the one removed");
        let armed = self.by_key.remove(&key);
        assert!(armed == Some(at), "both indexes hold every timer");
        Some(key)
    }
}

#[cfg(test)]
mod tests {
    use super::Deadlines;
    use crate::Time;

    fn at(nanos: u64) -> Time {
        Time::from_nanos(nanos)
    }

    #[test]
    fn timers_fire_in_order_once_due() {
        let mut timers = Deadlines::with_capacity(3);
        timers.arm('b', at(20)).expect("room");
        timers.arm('a', at(10)).expect("room");
        timers.arm('c', at(10)).expect("room");
        assert_eq!(timers.next(), Some(at(10)));
        assert_eq!(timers.expire(at(9)), None);
        assert_eq!(timers.expire(at(15)), Some('a'));
        assert_eq!(timers.expire(at(15)), Some('c'));
        assert_eq!(timers.expire(at(15)), None);
        assert_eq!(timers.expire(at(20)), Some('b'));
        assert!(timers.is_empty());
    }

    #[test]
    fn rearming_moves_a_timer_and_cancelling_is_idempotent() {
        let mut timers = Deadlines::with_capacity(1);
        timers.arm('a', at(10)).expect("room");
        timers.arm('a', at(30)).expect("re-arming takes no new room");
        assert_eq!(timers.arm('b', at(5)), Err('b'));
        assert_eq!(timers.expire(at(10)), None);
        timers.cancel('a');
        timers.cancel('a');
        assert_eq!(timers.next(), None);
    }
}
