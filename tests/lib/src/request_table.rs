//! Request-table promises checked from the owner's and store's observations.
//!
//! This model is an obligation ledger, not the table's state machine. It
//! uses an ordinary map, stable wall time and lower/upper retry bounds; it
//! knows no slab, ready state, monotonic anchor or generated jitter. The
//! store applies output records in order, restarts restore only what it
//! saved, and the peer checks one key always names the same opaque request.
//! Terminal uniqueness is checked within each ownership episode; restoring
//! a still-saved record starts another episode. A peer's keyed outcome is
//! preserved across those restarts.

use std::collections::{BTreeMap, BTreeSet};

use skein_lib::{
    Duration, RequestAnswered, RequestEnvelope, RequestKey, RequestLimits, RequestOut, RequestProgress, RequestRecord,
    RequestTable, Rng, Time, Token, Wall,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Terminal {
    Final,
    Unknown,
}

#[derive(Debug)]
struct Obligation {
    record: RequestRecord<u64>,
    size: u64,
    attempt: Option<Token>,
    earliest_retry: u64,
    latest_retry: u64,
    retry_span: u64,
    terminal: Option<Terminal>,
    erased: bool,
    restored: bool,
    expiry_witness: Option<u64>,
}

#[derive(Debug)]
enum Observed {
    Save(RequestRecord<u64>),
    Erase(RequestKey),
    Send { attempt: Token, key: RequestKey, request: u64 },
}

#[derive(Default, Debug)]
struct Coverage {
    asks: u32,
    refused: u32,
    saves: u32,
    sends: u32,
    envelopes: [u32; 4],
    stale: u32,
    offline: u32,
    confirmed: u32,
    restarts: u32,
    restored: u32,
    unknown: u32,
}

struct World {
    table: RequestTable<u64>,
    limits: RequestLimits,
    held: BTreeMap<RequestKey, Obligation>,
    saved: BTreeMap<RequestKey, RequestRecord<u64>>,
    issued: BTreeMap<RequestKey, u64>,
    decided: BTreeMap<RequestKey, u64>,
    history: Vec<Token>,
    seen_attempts: BTreeSet<Token>,
    up: bool,
    scope: Option<u64>,
    retention: Option<u64>,
    clock: u64,
    origin: u64,
    next_owner: u64,
    seed: u64,
    digest: u64,
}

impl World {
    fn new(seed: u64, limits: RequestLimits) -> World {
        World {
            table: RequestTable::new(&limits, seed),
            limits,
            held: BTreeMap::new(),
            saved: BTreeMap::new(),
            issued: BTreeMap::new(),
            decided: BTreeMap::new(),
            history: Vec::new(),
            seen_attempts: BTreeSet::new(),
            up: false,
            scope: None,
            retention: None,
            clock: 0,
            origin: 0,
            next_owner: 1,
            seed,
            digest: 0,
        }
    }

    fn now(&self) -> Time {
        Time::from_nanos(self.clock - self.origin)
    }

    fn wall(&self) -> Wall {
        Wall::from_nanos(self.clock)
    }

    fn trace(&mut self, value: u64) {
        self.digest = self.digest.rotate_left(7) ^ value;
    }

    fn bytes(&self) -> u64 {
        self.held.values().map(|entry| entry.size).sum()
    }

    fn ask(&mut self, scope: u64, size: u64, coverage: &mut Coverage) {
        let owner = self.next_owner;
        self.next_owner += 1;
        let fits = self.held.len() < usize::try_from(self.limits.requests).expect("small count")
            && self.bytes().checked_add(size).is_some_and(|bytes| bytes <= self.limits.bytes);
        match self.table.ask(Token::new(owner), scope, size, owner) {
            Ok(key) => {
                assert!(fits, "accepted past admission limits");
                assert!(self.issued.insert(key, owner).is_none(), "new asks never reuse keys, even after a restart");
                self.held.insert(
                    key,
                    Obligation {
                        record: RequestRecord { key, scope, first_sent: None, request: owner },
                        size,
                        attempt: None,
                        earliest_retry: 0,
                        latest_retry: 0,
                        retry_span: self.limits.first.min(self.limits.most).as_nanos(),
                        terminal: None,
                        erased: false,
                        restored: false,
                        expiry_witness: None,
                    },
                );
                coverage.asks += 1;
            }
            Err(request) => {
                assert!(!fits, "refusal when count and bytes fit");
                assert_eq!(request, owner, "refusal returns ownership");
                coverage.refused += 1;
            }
        }
    }

    fn park_ineligible(&mut self) {
        for entry in self.held.values_mut() {
            if !self.up || self.scope != Some(entry.record.scope) || (entry.restored && self.retention.is_none()) {
                entry.attempt = None;
                entry.earliest_retry = 0;
                entry.latest_retry = 0;
            }
        }
    }

    fn link(&mut self, up: bool, coverage: &mut Coverage) {
        self.up = up;
        self.table.link(up);
        self.park_ineligible();
        if !up {
            coverage.offline += 1;
        }
    }

    fn confirm(&mut self, scope: Option<u64>, coverage: &mut Coverage) {
        self.scope = scope;
        self.table.confirmed(scope);
        self.park_ineligible();
        coverage.confirmed += 1;
    }

    fn retention(&mut self, retention: u64) {
        self.retention = Some(retention);
        self.table.retention(Duration::from_nanos(retention));
    }

    fn expired(&self, entry: &Obligation) -> bool {
        if let Some(cutoff) = entry.expiry_witness {
            assert!(self.clock > cutoff, "witness records a strict retention crossing");
            return true;
        }
        match (self.retention, entry.record.first_sent) {
            (Some(retention), Some(first)) => {
                self.clock.saturating_sub(first.as_nanos()) > retention.saturating_sub(self.limits.margin.as_nanos())
            }
            (Some(_), None) | (None, Some(_) | None) => false,
        }
    }

    fn fire(&mut self) {
        let clock = self.clock;
        let retention = self.retention;
        let margin = self.limits.margin.as_nanos();
        for entry in self.held.values_mut() {
            if let (Some(retention), Some(first)) = (retention, entry.record.first_sent)
                && clock.saturating_sub(first.as_nanos()) > retention.saturating_sub(margin)
            {
                entry.attempt = None;
                entry.expiry_witness.get_or_insert(first.as_nanos() + retention.saturating_sub(margin));
            }
        }
        self.table.fire(self.now(), self.wall());
    }

    fn answer(&mut self, attempt: Token, envelope: RequestEnvelope, coverage: &mut Coverage) {
        let current = self
            .held
            .iter()
            .find_map(|(key, entry)| (entry.terminal.is_none() && entry.attempt == Some(attempt)).then_some(*key));
        let expected = if let Some(key) = current {
            let entry = self.held.get_mut(&key).expect("current obligation");
            entry.attempt = None;
            let which = match envelope {
                RequestEnvelope::Final => 0,
                RequestEnvelope::Again => 1,
                RequestEnvelope::Lost => 2,
                RequestEnvelope::SignedOut => 3,
            };
            coverage.envelopes[which] += 1;
            match envelope {
                RequestEnvelope::Final => {
                    entry.terminal = Some(Terminal::Final);
                    if let Some(previous) = self.decided.insert(key, entry.record.request) {
                        assert_eq!(previous, entry.record.request, "one key names one effect across restarts");
                    }
                    RequestAnswered::Final(Token::new(entry.record.request))
                }
                RequestEnvelope::Again | RequestEnvelope::Lost => {
                    entry.earliest_retry = self.clock + entry.retry_span / 2;
                    entry.latest_retry = self.clock + entry.retry_span;
                    entry.retry_span = (entry.retry_span * 2).min(self.limits.most.as_nanos());
                    RequestAnswered::Pending
                }
                RequestEnvelope::SignedOut => {
                    self.scope = None;
                    self.park_ineligible();
                    RequestAnswered::Pending
                }
            }
        } else {
            coverage.stale += 1;
            RequestAnswered::Stale
        };
        assert_eq!(
            self.table.answered(self.now(), attempt, envelope),
            expected,
            "envelope routing and terminal uniqueness"
        );
        self.trace(attempt.raw());
    }

    fn take(&mut self, prefix: u32, coverage: &mut Coverage) {
        self.fire();
        let mut observed = Vec::new();
        {
            let mut visit = self.table.take(self.now(), self.wall());
            for _ in 0..prefix {
                let Some(out) = visit.next_out() else {
                    break;
                };
                observed.push(match out {
                    RequestOut::Save(record) => Observed::Save(record.clone()),
                    RequestOut::Erase(key) => Observed::Erase(key),
                    RequestOut::Send { attempt, key, request } => Observed::Send { attempt, key, request: *request },
                });
            }
            if prefix > self.limits.out {
                assert!(visit.next_out().is_none(), "cursor never exceeds out");
            }
        }
        assert!(observed.len() <= usize::try_from(self.limits.out).expect("small output bound"));
        let mut sending = false;
        for out in observed {
            match out {
                Observed::Save(record) => {
                    assert!(!sending, "all records precede sends in each take");
                    let entry = self.held.get_mut(&record.key).expect("saved request is held");
                    assert_eq!(record.scope, entry.record.scope);
                    assert_eq!(record.request, entry.record.request);
                    assert!(entry.terminal.is_none(), "no save resurrects a retired request");
                    if entry.record.first_sent.is_some() {
                        assert_eq!(record.first_sent, entry.record.first_sent, "first time never changes");
                    } else if let Some(first) = record.first_sent {
                        assert_eq!(
                            first,
                            Wall::from_nanos(self.clock),
                            "first-send record uses the supplied wall clock"
                        );
                    }
                    entry.record = record.clone();
                    let key = record.key;
                    self.saved.insert(key, record);
                    for byte in key {
                        self.trace(u64::from(byte));
                    }
                    coverage.saves += 1;
                }
                Observed::Erase(key) => {
                    assert!(!sending, "erases are store records too");
                    let entry = self.held.get(&key).expect("erased request is held");
                    assert!(
                        entry.terminal.is_some() || self.expired(entry),
                        "erase only after final or past retention"
                    );
                    let entry = self.held.get_mut(&key).expect("held");
                    assert!(!entry.erased, "erase once per ownership episode");
                    entry.erased = true;
                    self.saved.remove(&key);
                    for byte in key {
                        self.trace(u64::from(byte));
                    }
                }
                Observed::Send { attempt, key, request } => {
                    sending = true;
                    let record = self.saved.get(&key).expect("a send's record has been applied to the store");
                    assert_eq!(record.request, request);
                    assert!(record.first_sent.is_some(), "first-send record applied before send");
                    let entry = self.held.get(&key).expect("sent request is held");
                    assert!(self.up, "no send on a down link");
                    assert_eq!(self.scope, Some(entry.record.scope), "only confirmed scope sends");
                    assert!(!entry.restored || self.retention.is_some(), "restoration waits for retention");
                    assert!(!self.expired(entry), "nothing sent past retention minus margin");
                    assert!(entry.terminal.is_none(), "no send after terminal");
                    assert!(entry.attempt.is_none(), "one attempt at a time");
                    assert!(
                        self.clock >= entry.earliest_retry,
                        "retry obeys the first/doubled span's lower jitter bound"
                    );
                    assert_ne!(
                        attempt,
                        Token::new(entry.record.request),
                        "attempt has its own token, distinct from its owner's"
                    );
                    assert!(self.seen_attempts.insert(attempt), "attempt tokens never reused in one process");
                    self.history.push(attempt);
                    self.held.get_mut(&key).expect("held").attempt = Some(attempt);
                    coverage.sends += 1;
                    self.trace(request ^ attempt.raw());
                    for byte in key {
                        self.trace(u64::from(byte));
                    }
                }
            }
        }
    }

    fn progress(&mut self, coverage: &mut Coverage) {
        while let Some((owner, progress)) = self.table.progress() {
            let key = *self
                .held
                .iter()
                .find(|(_, entry)| entry.record.request == owner.raw())
                .expect("progress routed to a held owner")
                .0;
            let entry = self.held.get(&key).expect("held");
            match progress {
                RequestProgress::InFlight => {
                    assert!(entry.attempt.is_some());
                    assert!(entry.terminal.is_none());
                }
                RequestProgress::Retrying | RequestProgress::Parked => {
                    assert!(entry.attempt.is_none());
                    assert!(entry.terminal.is_none());
                }
                RequestProgress::Unknown => {
                    assert!(
                        self.expired(entry),
                        "unknown only past retention minus margin: seed {}, clock {}, retention {:?}, entry {entry:?}",
                        self.seed,
                        self.clock,
                        self.retention
                    );
                    assert!(entry.terminal.is_none(), "one terminal per ownership episode");
                    self.held.get_mut(&key).expect("held").terminal = Some(Terminal::Unknown);
                    coverage.unknown += 1;
                }
            }
            self.trace(owner.raw());
        }
        assert!(!self.table.progress_pending());
    }

    fn reclaim(&mut self) {
        self.table.reclaim();
        self.held.retain(|_, entry| entry.terminal.is_none() || !entry.erased);
    }

    fn verify(&self) {
        assert_eq!(
            usize::try_from(self.table.len()).expect("small count"),
            self.held.len(),
            "count matches obligations"
        );
        assert_eq!(self.table.bytes(), self.bytes(), "declared bytes match obligations");
        assert!(self.table.len() <= self.limits.requests);
        assert!(self.table.bytes() <= self.limits.bytes);
        for (key, entry) in &self.held {
            assert_eq!(self.table.owner(*key), Some(Token::new(entry.record.request)));
            assert_eq!(self.table.declared_size(*key), Some(entry.size));
            assert_eq!(self.issued.get(key), Some(&entry.record.request));
        }
    }

    fn restart(&mut self, coverage: &mut Coverage) {
        self.seed = self.seed.wrapping_add(1);
        self.table = RequestTable::new(&self.limits, self.seed);
        self.up = false;
        self.scope = None;
        self.retention = None;
        self.origin = self.clock;
        self.held.clear();
        self.history.clear();
        self.seen_attempts.clear();
        for record in self.saved.values() {
            let size = record.request % 4;
            self.table
                .restore(Token::new(record.request), size, record.clone())
                .expect("saved records fit the same limits");
            self.held.insert(
                record.key,
                Obligation {
                    record: record.clone(),
                    size,
                    attempt: None,
                    earliest_retry: 0,
                    latest_retry: 0,
                    retry_span: self.limits.first.min(self.limits.most).as_nanos(),
                    terminal: None,
                    erased: false,
                    restored: true,
                    expiry_witness: None,
                },
            );
            coverage.restored += 1;
        }
        coverage.restarts += 1;
    }

    fn flush(&mut self, coverage: &mut Coverage) {
        for _ in 0..3 * self.limits.requests + 2 {
            self.take(self.limits.out + 1, coverage);
            self.progress(coverage);
            self.reclaim();
            if !self.table.pending() {
                break;
            }
        }
        assert!(!self.table.pending(), "bounded store/send work settles");
        for entry in self.held.values() {
            if self.up
                && self.scope == Some(entry.record.scope)
                && (!entry.restored || self.retention.is_some())
                && entry.terminal.is_none()
                && !self.expired(entry)
                && self.clock >= entry.latest_retry
            {
                assert!(entry.attempt.is_some(), "eligible request sent by its maximum retry span after outputs drain");
            }
        }
    }

    fn settle(&mut self, coverage: &mut Coverage) {
        self.retention(200);
        self.link(true, coverage);
        for scope in 0..3 {
            self.confirm(Some(scope), coverage);
            self.clock += self.limits.most.as_nanos();
            self.flush(coverage);
            let current: Vec<_> = self.held.values().filter_map(|entry| entry.attempt).collect();
            for attempt in current {
                self.answer(attempt, RequestEnvelope::Final, coverage);
            }
            self.flush(coverage);
        }
        assert!(self.held.is_empty(), "every remaining episode ended final or unknown");
        assert!(self.saved.is_empty(), "all retired records erased");
        assert!(self.table.is_empty());
        assert_eq!(self.table.next_deadline(), None);
        assert!(!self.table.progress_pending());
    }
}

/// Drive independent obligation checks over seeded asks, envelopes and restarts.
/// Returns a trace fingerprint for replay checks; panics on a broken promise.
#[must_use]
pub fn check(seed: u64, rounds: u32) -> u64 {
    let mut rng = Rng::new(seed);
    let mut coverage = Coverage::default();
    let mut trace = 0_u64;
    for _ in 0..rounds {
        let limits = RequestLimits {
            requests: u32::try_from(rng.below(6)).expect("small count"),
            bytes: rng.below(8),
            out: u32::try_from(rng.below(5)).expect("small output") + 1,
            first: Duration::from_nanos(4),
            most: Duration::from_nanos(32),
            margin: Duration::from_nanos(2),
        };
        let mut world = World::new(rng.next_u64(), limits);
        for _ in 0..96 {
            match rng.below(11) {
                0..=3 => {
                    let scope = rng.below(3);
                    let size = world.next_owner % 4;
                    world.ask(scope, size, &mut coverage);
                }
                4 => {
                    let attempt = if world.history.is_empty() {
                        Token::new(u64::MAX)
                    } else {
                        world.history[usize::try_from(
                            rng.below(u64::try_from(world.history.len()).expect("small history")),
                        )
                        .expect("small index")]
                    };
                    let envelope = match rng.below(4) {
                        0 => RequestEnvelope::Final,
                        1 => RequestEnvelope::Again,
                        2 => RequestEnvelope::Lost,
                        3 => RequestEnvelope::SignedOut,
                        _ => unreachable!("draw below four"),
                    };
                    world.answer(attempt, envelope, &mut coverage);
                }
                5 => world.link(rng.chance(500), &mut coverage),
                6 => {
                    let scope = match rng.below(4) {
                        0 => None,
                        n => Some(n - 1),
                    };
                    world.confirm(scope, &mut coverage);
                }
                7 => world.retention(rng.below(200) + 2),
                8 => world.clock += rng.below(20) + 1,
                9 => {
                    if rng.chance(300) {
                        world.restart(&mut coverage);
                    } else {
                        world.clock += 1;
                    }
                }
                10 => {}
                _ => unreachable!("draw below eleven"),
            }
            let prefix = u32::try_from(rng.below(u64::from(limits.out) + 2)).expect("small prefix");
            world.take(prefix, &mut coverage);
            if rng.chance(700) {
                world.progress(&mut coverage);
            }
            if rng.chance(700) {
                world.reclaim();
            }
            world.verify();
        }
        world.settle(&mut coverage);
        trace = trace.rotate_left(1) ^ world.digest;
    }
    if rounds >= 64 {
        assert!(coverage.asks > 0 && coverage.refused > 0 && coverage.saves > 0 && coverage.sends > 0);
        assert!(coverage.envelopes.iter().all(|count| *count > 0), "all envelope transitions covered: {coverage:?}");
        assert!(coverage.stale > 0 && coverage.offline > 0 && coverage.confirmed > 0);
        assert!(
            coverage.restarts > 0 && coverage.restored > 0 && coverage.unknown > 0,
            "restart and retention transitions covered: {coverage:?}"
        );
    }
    trace
}

#[cfg(test)]
mod tests {
    use super::{Coverage, World};
    use skein_lib::{Duration, RequestLimits};

    #[test]
    fn delayed_unknown_keeps_its_expiry_witness_when_retention_grows() {
        let limits = RequestLimits {
            requests: 2,
            bytes: 4,
            out: 4,
            first: Duration::from_nanos(4),
            most: Duration::from_nanos(32),
            margin: Duration::from_nanos(2),
        };
        let mut world = World::new(1, limits);
        let mut coverage = Coverage::default();
        world.retention(5);
        world.link(true, &mut coverage);
        world.confirm(Some(1), &mut coverage);
        world.ask(1, 1, &mut coverage);
        world.flush(&mut coverage);
        world.clock = 4;
        world.take(5, &mut coverage);
        // The unknown notification remains untaken while the frame announces
        // a longer retention. An already retired request stays unknown.
        world.retention(100);
        world.progress(&mut coverage);
        world.reclaim();
        world.verify();
        assert_eq!(coverage.unknown, 1);
        assert!(world.table.is_empty());
    }
}
