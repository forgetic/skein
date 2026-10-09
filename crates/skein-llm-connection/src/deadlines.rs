//! Per-call deadlines (llm-connection.md, section 6). This table stores the
//! five absolute monotonic times and the durations needed to rearm idleness.
//! It never reads a clock: the owner supplies one `Env` snapshot to every
//! entry point. `due` names a phase that must end through the LLM client.

use skein_lib::Time;

use crate::boundary::Deadlines;

/// The call or connection phase, selecting all armed deadlines in one place.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Phase {
    Waiting,
    Connecting,
    Handshaking,
    Head,
    Streaming,
    Draining,
    Idle,
    Closing,
    Closed,
}

/// Which configured wait expired. All produce the client's timed-out failure.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Due {
    Connect,
    Handshake,
    Head,
    Idle,
    Whole,
    Keep,
}

/// One call's absolute deadlines and arming state.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Table {
    config: Deadlines,
    connect: Option<Time>,
    handshake: Option<Time>,
    head: Option<Time>,
    idle: Option<Time>,
    whole: Option<Time>,
    keep: Option<Time>,
}

impl Table {
    pub(crate) fn new(config: Deadlines, now: Time) -> Table {
        Table {
            config,
            connect: at(now, config.connect),
            handshake: None,
            head: None,
            idle: None,
            whole: at(now, config.whole),
            keep: None,
        }
    }

    /// Reconcile the complete deadline set after every phase transition.
    pub(crate) fn arm(&mut self, phase: Phase, now: Time, idle_keep: skein_lib::Duration) {
        let (connect, handshake, head, idle, whole, keep) = match phase {
            Phase::Waiting => (false, false, false, false, true, false),
            Phase::Connecting => (true, false, false, false, true, false),
            Phase::Handshaking => (false, true, false, false, true, false),
            Phase::Head => (false, false, true, false, true, false),
            Phase::Streaming => (false, false, false, true, true, false),
            Phase::Draining => (false, false, false, true, false, false),
            Phase::Idle => (false, false, false, false, false, true),
            Phase::Closing | Phase::Closed => (false, false, false, false, false, false),
        };
        self.connect = selected(self.connect, connect, now, self.config.connect);
        self.handshake = selected(self.handshake, handshake, now, self.config.handshake);
        if !head {
            self.head = None;
        }
        self.idle = selected(self.idle, idle, now, self.config.idle);
        if !whole {
            self.whole = None;
        }
        self.keep = selected(self.keep, keep, now, Some(idle_keep));
    }

    pub(crate) fn sent(&mut self, now: Time) {
        self.head = at(now, self.config.head);
    }

    pub(crate) fn activity(&mut self, now: Time) {
        self.idle = at(now, self.config.idle);
    }

    pub(crate) fn next(&self) -> Option<Time> {
        let mut earliest = self.connect;
        earliest = earlier(earliest, self.handshake);
        earliest = earlier(earliest, self.head);
        earliest = earlier(earliest, self.idle);
        earliest = earlier(earliest, self.whole);
        earlier(earliest, self.keep)
    }

    pub(crate) fn due(&self, now: Time) -> Option<Due> {
        if expired(self.connect, now) {
            Some(Due::Connect)
        } else if expired(self.handshake, now) {
            Some(Due::Handshake)
        } else if expired(self.head, now) {
            Some(Due::Head)
        } else if expired(self.idle, now) {
            Some(Due::Idle)
        } else if expired(self.whole, now) {
            Some(Due::Whole)
        } else if expired(self.keep, now) {
            Some(Due::Keep)
        } else {
            None
        }
    }
}

#[expect(clippy::manual_map, reason = "the step subset excludes closures")]
fn at(now: Time, duration: Option<skein_lib::Duration>) -> Option<Time> {
    match duration {
        Some(duration) => Some(now.saturating_add(duration)),
        None => None,
    }
}

fn earlier(left: Option<Time>, right: Option<Time>) -> Option<Time> {
    match left {
        Some(left) => match right {
            Some(right) => Some(left.min(right)),
            None => Some(left),
        },
        None => right,
    }
}

fn expired(deadline: Option<Time>, now: Time) -> bool {
    match deadline {
        Some(deadline) => now >= deadline,
        None => false,
    }
}

fn selected(previous: Option<Time>, enabled: bool, now: Time, duration: Option<skein_lib::Duration>) -> Option<Time> {
    if enabled {
        match previous {
            Some(previous) => Some(previous),
            None => at(now, duration),
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_lib::Duration;

    #[test]
    fn every_phase_selects_its_complete_deadline_set() {
        let duration = Some(Duration::from_secs(1));
        let config =
            Deadlines { connect: duration, handshake: duration, head: duration, idle: duration, whole: duration };
        for (phase, expected) in [
            (Phase::Waiting, [false, false, false, false, true, false]),
            (Phase::Connecting, [true, false, false, false, true, false]),
            (Phase::Handshaking, [false, true, false, false, true, false]),
            (Phase::Head, [false, false, true, false, true, false]),
            (Phase::Streaming, [false, false, false, true, true, false]),
            (Phase::Draining, [false, false, false, true, false, false]),
            (Phase::Idle, [false, false, false, false, false, true]),
            (Phase::Closing, [false; 6]),
            (Phase::Closed, [false; 6]),
        ] {
            let mut table = Table::new(config, Time::ZERO);
            table.sent(Time::ZERO);
            table.arm(phase, Time::ZERO, Duration::from_secs(10));
            assert_eq!(
                [
                    table.connect.is_some(),
                    table.handshake.is_some(),
                    table.head.is_some(),
                    table.idle.is_some(),
                    table.whole.is_some(),
                    table.keep.is_some()
                ],
                expected,
                "{phase:?}"
            );
        }
    }

    #[test]
    fn a_terminal_keeps_drain_idleness_and_closing_disarms_even_past_due_times() {
        let mut table = Table::new(
            Deadlines { idle: Some(Duration::from_secs(1)), whole: Some(Duration::from_secs(2)), ..Deadlines::none() },
            Time::ZERO,
        );
        table.arm(Phase::Streaming, Time::ZERO, Duration::from_secs(10));
        table.arm(Phase::Draining, Time::ZERO, Duration::from_secs(10));
        let later = Time::from_nanos(3_000_000_000);
        assert_eq!(table.due(later), Some(Due::Idle));
        table.arm(Phase::Closing, later, Duration::from_secs(10));
        assert_eq!(table.next(), None);
        assert_eq!(table.due(later), None);
    }
    #[test]
    fn connecting_after_waiting_arms_connect_now_and_retains_the_original_whole() {
        let mut table = Table::new(
            Deadlines {
                connect: Some(Duration::from_secs(1)),
                whole: Some(Duration::from_secs(5)),
                ..Deadlines::none()
            },
            Time::ZERO,
        );
        table.arm(Phase::Waiting, Time::ZERO, Duration::from_secs(10));
        assert_eq!(table.next(), Some(Time::from_nanos(5_000_000_000)));
        table.arm(Phase::Connecting, Time::from_nanos(3_000_000_000), Duration::from_secs(10));
        assert_eq!(table.connect, Some(Time::from_nanos(4_000_000_000)));
        assert_eq!(table.whole, Some(Time::from_nanos(5_000_000_000)));
    }
}
