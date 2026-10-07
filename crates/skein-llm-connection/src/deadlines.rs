//! Per-call deadlines (llm-connection.md, section 6). This table stores the
//! five absolute monotonic times and the durations needed to rearm idleness.
//! It never reads a clock: the owner supplies one `Env` snapshot to every
//! entry point. `due` names a phase that must end through the LLM client.

use skein_lib::Time;

use crate::boundary::Deadlines;

/// Which configured wait expired. All produce the client's timed-out failure.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Due {
    Connect,
    Handshake,
    Head,
    Idle,
    Whole,
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
    request_started: bool,
    response_seen: bool,
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
            request_started: false,
            response_seen: false,
        }
    }

    pub(crate) fn connected(&mut self, now: Time) {
        self.connect = None;
        self.handshake = at(now, self.config.handshake);
    }

    pub(crate) fn ready(&mut self) {
        self.handshake = None;
    }

    pub(crate) fn sent(&mut self, now: Time) {
        if !self.request_started {
            self.request_started = true;
            self.head = at(now, self.config.head);
        }
    }

    pub(crate) fn response(&mut self, now: Time) {
        if !self.response_seen {
            self.response_seen = true;
            self.head = None;
            self.idle = at(now, self.config.idle);
        }
    }

    pub(crate) fn activity(&mut self, now: Time) {
        if self.response_seen {
            self.idle = at(now, self.config.idle);
        }
    }

    pub(crate) fn terminal(&mut self) {
        self.connect = None;
        self.handshake = None;
        self.head = None;
        self.idle = None;
        self.whole = None;
    }

    pub(crate) fn next(&self) -> Option<Time> {
        let mut earliest = self.connect;
        earliest = earlier(earliest, self.handshake);
        earliest = earlier(earliest, self.head);
        earliest = earlier(earliest, self.idle);
        earlier(earliest, self.whole)
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
