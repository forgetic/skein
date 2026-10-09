//! Bounded keyed requests and their store barrier (lib.md, section 12).
//!
//! The table owns requests, opaque scopes and owner tokens, declared byte
//! counts, seeded keys and attempt tokens. It never reads a request. `ask`
//! and `restore` admit values; `link` and `confirmed` control eligibility.
//! `take` lends a bounded cursor: apply every `Save` before handling a
//! `Send`. Dropping a cursor leaves untaken work in the table.
//!
//! | State | Event | Next | Emits |
//! |---|---|---|---|
//! | absent | ask | parked or ready | save, then eligible send |
//! | absent | restore | parked | initial parked progress |
//! | parked | link up, matching confirmation, retention known if restored | ready | send after first-send save if needed |
//! | ready | take send | in flight | fresh attempt, in-flight progress |
//! | in flight | final envelope | retired | owner token, erase |
//! | in flight | again or lost, eligible | retrying | retry deadline, retrying progress |
//! | in flight | again or lost, ineligible | parked | parked progress |
//! | in flight | signed out | parked | clear confirmation, parked progress |
//! | retrying | retry deadline fires | ready | eligible send |
//! | ready, in flight, retrying | link down or scope changes away | parked | invalidate attempt, parked progress |
//! | any live state | retention cutoff passes | retired | erase, unknown progress |
//! | any state | stale envelope | unchanged | nothing |
//! | retired | erase and terminal progress taken, reclaim | absent | release count and bytes |
//!
//! The link starts down and no scope is confirmed. Restoration also waits
//! for known retention. A first-send save records wall time; restored ages
//! are converted once to monotonic time. Later wall-clock jumps do not move
//! deadlines. The cutoff is strict: exactly retention minus margin is still
//! eligible, and one nanosecond beyond is unknown. Retry jitter is uniform
//! over half to all of each exponentially growing span, capped at `most`.
//!
//! `fire` expires retention and retries; `take` also calls it before lending
//! outputs. Input answers may be processed before `fire`, so a final answer
//! in that iteration wins. `progress` lends the latest display change per
//! owner, coalescing untaken changes; unknown is held until taken. After
//! outputs and progress, call `reclaim` at the iteration's reclaim point.

use crate::{Duration, Id, Map, Rng, Slab, Time, Token, Wall};

/// A request's stable key, preserved across attempts and restoration.
pub type RequestKey = [u8; 16];

/// Fixed admission, output and retry bounds (lib.md, section 12).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestLimits {
    /// Requests held, including work whose store output has not been taken.
    pub requests: u32,
    /// Sum of the owners' declared payload sizes.
    pub bytes: u64,
    /// Store records and sends taken through one cursor.
    pub out: u32,
    /// First retry span.
    pub first: Duration,
    /// Greatest retry span before jitter.
    pub most: Duration,
    /// Safety span deducted from the answer retention.
    pub margin: Duration,
}

/// A request's complete store record, without its process-local owner token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestRecord<R> {
    pub key: RequestKey,
    pub scope: u64,
    /// Absent until the first sending is prepared for the store.
    pub first_sent: Option<Wall>,
    pub request: R,
}

/// One output borrowed from the table; the caller applies it before advancing.
#[derive(Debug)]
pub enum RequestOut<'a, R> {
    /// Persist this record before any send taken afterwards.
    Save(&'a RequestRecord<R>),
    /// Erase the retired request from the store.
    Erase(RequestKey),
    /// Send the held request under a fresh attempt token.
    Send { attempt: Token, key: RequestKey, request: &'a R },
}

/// The protocol layer's classification of an attempt's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestEnvelope {
    /// Durable answer; its payload goes directly to the owner.
    Final,
    /// Busy or not ready; retry under the same key.
    Again,
    /// No answer arrived; retry if reachable, otherwise park.
    Lost,
    /// Sign-in ended; await renewed scope confirmation.
    SignedOut,
}

/// Whether an envelope ends the request, changes its lifecycle or is stale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum RequestAnswered {
    /// Route the final answer directly to this owner.
    Final(Token),
    /// The request will retry or remain parked.
    Pending,
    /// This attempt has been superseded or retired.
    Stale,
}

/// The latest request state its owner may display.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestProgress {
    /// An attempt is in flight.
    InFlight,
    /// A retry is waiting for its deadline or its send output.
    Retrying,
    /// Link, scope or restored retention prevents sending.
    Parked,
    /// Retention passed; whether the request was made is unknown.
    Unknown,
}

#[derive(Debug)]
enum State {
    Parked,
    Ready,
    InFlight(Token),
    Retrying(Time),
    Retired { erase: bool },
}

#[derive(Debug)]
struct Anchor {
    now: Time,
    age: Duration,
}

#[derive(Debug)]
struct Entry<R> {
    record: RequestRecord<R>,
    owner: Token,
    size: u64,
    restored: bool,
    save: bool,
    state: State,
    progress: Option<RequestProgress>,
    changed: bool,
    backoff: Duration,
    anchor: Option<Anchor>,
}

impl<R> Entry<R> {
    fn progress(&mut self, progress: RequestProgress) {
        if self.progress != Some(progress) {
            self.progress = Some(progress);
            self.changed = true;
        }
    }

    fn erase_pending(&self) -> bool {
        match self.state {
            State::Retired { erase } => erase,
            State::Parked | State::Ready | State::InFlight(_) | State::Retrying(_) => false,
        }
    }

    fn unknown(&mut self) {
        self.state = State::Retired { erase: true };
        self.save = false;
        self.progress(RequestProgress::Unknown);
    }
}

/// Keyed requests whose opaque payloads are kept before sending.
#[derive(Debug)]
pub struct RequestTable<R> {
    limits: RequestLimits,
    entries: Slab<Entry<R>>,
    order: Map<u64, Id<Entry<R>>>,
    index: Map<RequestKey, Id<Entry<R>>>,
    bytes: u64,
    draws: u64,
    namespace: u64,
    serial: u64,
    attempt: u64,
    up: bool,
    scope: Option<u64>,
    retention: Option<Duration>,
    retries: Rng,
}

/// A transient bounded visit, borrowing the table only while outputs are taken.
#[derive(Debug)]
pub struct RequestTake<'a, R> {
    table: &'a mut RequestTable<R>,
    remaining: u32,
    now: Time,
    wall: Wall,
}

impl<R> RequestTable<R> {
    /// Construct fixed-capacity storage; the seed must be fresh at each start.
    #[must_use]
    pub fn new(limits: &RequestLimits, seed: u64) -> RequestTable<R> {
        RequestTable {
            limits: *limits,
            entries: Slab::with_capacity(limits.requests),
            order: Map::with_capacity(limits.requests),
            index: Map::with_capacity(limits.requests),
            bytes: 0,
            draws: 0,
            namespace: seed,
            serial: 0,
            attempt: 0,
            up: false,
            scope: None,
            retention: None,
            retries: Rng::new(seed ^ 0x5245_5452_4945_5321),
        }
    }

    /// Admit a request, or return ownership without changing anything at a limit.
    pub fn ask(&mut self, owner: Token, scope: u64, size: u64, request: R) -> Result<RequestKey, R> {
        if !self.fits(size) || self.serial == u64::MAX || self.fresh_attempt(owner).is_none() {
            return Err(request);
        }
        let Some((key, draws)) = self.fresh_key() else {
            return Err(request);
        };
        self.draws = draws;
        let record = RequestRecord { key, scope, first_sent: None, request };
        self.insert(owner, size, record, false);
        Ok(key)
    }

    /// Restore a saved request with its owner's current token and declared size.
    /// Duplicate keys and capacity overruns return the record unchanged.
    pub fn restore(&mut self, owner: Token, size: u64, record: RequestRecord<R>) -> Result<(), RequestRecord<R>> {
        if !self.fits(size) || self.serial == u64::MAX || self.contains(record.key) {
            return Err(record);
        }
        self.insert(owner, size, record, true);
        Ok(())
    }

    /// Tell the table whether its peer is reachable.
    pub fn link(&mut self, up: bool) {
        self.up = up;
        self.synchronize();
    }

    /// Permit only requests belonging to this opaque scope.
    pub fn confirmed(&mut self, scope: Option<u64>) {
        self.scope = scope;
        self.synchronize();
    }

    /// Supply the answer retention before restored requests can leave.
    pub fn retention(&mut self, span: Duration) {
        self.retention = Some(span);
        self.synchronize();
    }

    /// Visit at most `out` records and sends, with every record before sends.
    pub fn take(&mut self, now: Time, wall: Wall) -> RequestTake<'_, R> {
        self.fire(now, wall);
        RequestTake { remaining: self.limits.out, table: self, now, wall }
    }

    /// Apply an envelope for the one current attempt; stale tokens change nothing.
    pub fn answered(&mut self, now: Time, attempt: Token, envelope: RequestEnvelope) -> RequestAnswered {
        let mut found = None;
        for (_, id) in &self.order {
            let entry = self.entries.get(*id).expect("order names a held entry");
            match entry.state {
                State::InFlight(current) => {
                    if current == attempt {
                        found = Some(*id);
                        break;
                    }
                }
                State::Parked | State::Ready | State::Retrying(_) | State::Retired { .. } => {}
            }
        }
        let Some(id) = found else {
            return RequestAnswered::Stale;
        };
        let entry = self.entries.get_mut(id).expect("attempt names a held entry");
        match envelope {
            RequestEnvelope::Final => {
                entry.state = State::Retired { erase: true };
                entry.save = false;
                entry.changed = false;
                return RequestAnswered::Final(entry.owner);
            }
            RequestEnvelope::Again | RequestEnvelope::Lost => {
                let span = entry.backoff.as_nanos();
                let jitter = self.retries.between(span.div_euclid(2), span);
                entry.state = State::Retrying(now.saturating_add(Duration::from_nanos(jitter)));
                entry.backoff = entry.backoff.saturating_mul(2).min(self.limits.most);
                entry.progress(RequestProgress::Retrying);
            }
            RequestEnvelope::SignedOut => {
                self.confirmed(None);
            }
        }
        RequestAnswered::Pending
    }

    /// Expire retry and retention deadlines using the supplied clocks.
    pub fn fire(&mut self, now: Time, wall: Wall) {
        for (_, id) in &self.order {
            let entry = self.entries.get_mut(*id).expect("order names a held entry");
            match entry.state {
                State::Retired { .. } => continue,
                State::Parked | State::Ready | State::InFlight(_) | State::Retrying(_) => {}
            }
            if entry.anchor.is_none()
                && let Some(first) = entry.record.first_sent
            {
                let age = Duration::from_nanos(wall.as_nanos().saturating_sub(first.as_nanos()));
                entry.anchor = Some(Anchor { now, age });
            }
            if let Some(retention) = self.retention
                && let Some(anchor) = &entry.anchor
            {
                let age = anchor.age.saturating_add(now.saturating_since(anchor.now));
                let allowance = retention.as_nanos().saturating_sub(self.limits.margin.as_nanos());
                if age.as_nanos() > allowance {
                    entry.unknown();
                    continue;
                }
            }
            match entry.state {
                State::Retrying(deadline) => {
                    if deadline <= now {
                        entry.state = State::Ready;
                    }
                }
                State::Parked | State::Ready | State::InFlight(_) | State::Retired { .. } => {}
            }
        }
    }

    /// The earliest retry or strict retention cutoff, including parked requests.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut next = None;
        for (_, id) in &self.order {
            let entry = self.entries.get(*id).expect("order names a held entry");
            match entry.state {
                State::Retrying(deadline) => earliest(&mut next, deadline),
                State::Retired { .. } => continue,
                State::Parked | State::Ready | State::InFlight(_) => {}
            }
            if let Some(retention) = self.retention
                && let Some(anchor) = &entry.anchor
            {
                let allowance = retention.as_nanos().saturating_sub(self.limits.margin.as_nanos());
                if anchor.age.as_nanos() > allowance {
                    earliest(&mut next, anchor.now);
                } else {
                    let remaining = allowance.checked_sub(anchor.age.as_nanos()).expect("age is within allowance");
                    if let Some(remaining) = remaining.checked_add(1)
                        && let Some(deadline) = anchor.now.checked_add(Duration::from_nanos(remaining))
                    {
                        earliest(&mut next, deadline);
                    }
                }
            }
        }
        next
    }

    /// Take the next owner's latest display change, in admission order.
    pub fn progress(&mut self) -> Option<(Token, RequestProgress)> {
        for (_, id) in &self.order {
            let entry = self.entries.get_mut(*id).expect("order names a held entry");
            if entry.changed {
                entry.changed = false;
                return Some((entry.owner, entry.progress.expect("a change has a progress value")));
            }
        }
        None
    }

    /// Whether a display change remains, including a terminal unknown outcome.
    #[must_use]
    pub fn progress_pending(&self) -> bool {
        for (_, id) in &self.order {
            if self.entries.get(*id).expect("order names a held entry").changed {
                return true;
            }
        }
        false
    }

    /// Release retired requests whose erase and terminal progress have been taken.
    pub fn reclaim(&mut self) {
        for _ in 0..self.limits.requests {
            let mut found = None;
            for (number, id) in &self.order {
                let entry = self.entries.get(*id).expect("order names a held entry");
                match entry.state {
                    State::Retired { .. } => {
                        if !entry.erase_pending() && !entry.changed {
                            found = Some((*number, *id));
                            break;
                        }
                    }
                    State::Parked | State::Ready | State::InFlight(_) | State::Retrying(_) => {}
                }
            }
            let Some((number, id)) = found else {
                break;
            };
            let entry = self.entries.get(id).expect("retired entry is held");
            self.bytes = self.bytes.checked_sub(entry.size).expect("held entry's size was counted");
            let removed = self.index.remove(&entry.record.key);
            assert!(removed == Some(id), "one index entry per request");
            let removed = self.order.remove(&number);
            assert!(removed == Some(id), "one order entry per request");
            self.entries.retire(id);
        }
        self.entries.reclaim();
    }

    /// Whether store records or eligible sends remain to take.
    #[must_use]
    pub fn pending(&self) -> bool {
        for (_, id) in &self.order {
            let entry = self.entries.get(*id).expect("order names a held entry");
            if entry.save || entry.erase_pending() || self.eligible(entry) {
                return true;
            }
        }
        false
    }

    /// Requests held, whether parked, ready or in flight.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.entries.len()
    }

    /// Whether no requests are held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Payload bytes declared by owners; the table never measures them.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The current owner's opaque token for a held key.
    #[must_use]
    pub fn owner(&self, key: RequestKey) -> Option<Token> {
        let id = *self.index.get(&key)?;
        Some(self.entries.get(id).expect("index names a held entry").owner)
    }

    /// The owner's declared size for a held key.
    #[must_use]
    pub fn declared_size(&self, key: RequestKey) -> Option<u64> {
        let id = *self.index.get(&key)?;
        Some(self.entries.get(id).expect("index names a held entry").size)
    }

    /// Container heap at the configured count, excluding owned payload bytes.
    #[must_use]
    pub fn worst_case(limits: &RequestLimits) -> Option<u64> {
        Slab::<Entry<R>>::worst_case(limits.requests)?
            .checked_add(Map::<u64, Id<Entry<R>>>::worst_case(limits.requests)?)?
            .checked_add(Map::<RequestKey, Id<Entry<R>>>::worst_case(limits.requests)?)
    }

    fn fits(&self, size: u64) -> bool {
        if self.entries.is_full() {
            return false;
        }
        match self.bytes.checked_add(size) {
            Some(total) => total <= self.limits.bytes,
            None => false,
        }
    }

    fn contains(&self, key: RequestKey) -> bool {
        self.index.contains_key(&key)
    }

    fn fresh_key(&self) -> Option<(RequestKey, u64)> {
        let mut draws = self.draws;
        // SplitMix64 permutes all u64 states; before its period it never
        // repeats. Restored keys may collide, so at most held + 1 draws suffice.
        for _ in 0..=self.limits.requests {
            let mut key = [0; 16];
            for (slot, byte) in key.iter_mut().zip(self.namespace.to_be_bytes()) {
                *slot = byte;
            }
            draws = draws.checked_add(1)?;
            let state = self.namespace.wrapping_add(draws.wrapping_sub(1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let mut rng = Rng::new(state);
            let draw = rng.next_u64();
            for (slot, byte) in key.get_mut(8..).expect("second half").iter_mut().zip(draw.to_be_bytes()) {
                *slot = byte;
            }
            if !self.contains(key) {
                return Some((key, draws));
            }
        }
        None
    }

    fn insert(&mut self, owner: Token, size: u64, record: RequestRecord<R>, restored: bool) {
        let key = record.key;
        let eligible = self.up && self.scope == Some(record.scope) && (!restored || self.retention.is_some());
        let entry = Entry {
            record,
            owner,
            size,
            restored,
            save: !restored,
            state: if eligible { State::Ready } else { State::Parked },
            progress: if eligible { None } else { Some(RequestProgress::Parked) },
            changed: !eligible,
            backoff: self.limits.first.min(self.limits.most),
            anchor: None,
        };
        let Ok(id) = self.entries.insert(entry) else {
            unreachable!("admission reserved an entry");
        };
        let replaced = self.index.insert(key, id).expect("one key per slab slot");
        assert!(replaced.is_none(), "admission checked a fresh key");
        self.serial = self.serial.checked_add(1).expect("admission checked numbering");
        let replaced = self.order.insert(self.serial, id).expect("one order entry per slab slot");
        assert!(replaced.is_none(), "admission numbers are fresh");
        self.bytes = self.bytes.checked_add(size).expect("admission checked declared bytes");
    }

    fn synchronize(&mut self) {
        for (_, id) in &self.order {
            let entry = self.entries.get_mut(*id).expect("order names a held entry");
            let eligible =
                self.up && self.scope == Some(entry.record.scope) && (!entry.restored || self.retention.is_some());
            match entry.state {
                State::Retired { .. } => {}
                State::Parked => {
                    if eligible {
                        entry.state = State::Ready;
                    }
                }
                State::Ready | State::InFlight(_) | State::Retrying(_) => {
                    if !eligible {
                        entry.state = State::Parked;
                        entry.progress(RequestProgress::Parked);
                    }
                }
            }
        }
    }

    fn fresh_attempt(&self, owner: Token) -> Option<Token> {
        let mut raw = self.attempt.checked_add(1)?;
        if raw == owner.raw() {
            raw = raw.checked_add(1)?;
        }
        Some(Token::new(raw))
    }

    fn eligible(&self, entry: &Entry<R>) -> bool {
        match entry.state {
            State::Ready => {
                self.up
                    && self.scope == Some(entry.record.scope)
                    && (!entry.restored || self.retention.is_some())
                    && self.fresh_attempt(entry.owner).is_some()
            }
            State::Parked | State::InFlight(_) | State::Retrying(_) | State::Retired { .. } => false,
        }
    }
}

impl<R> RequestTake<'_, R> {
    /// Take the next output; an untaken output stays held when the visit ends.
    pub fn next_out(&mut self) -> Option<RequestOut<'_, R>> {
        if self.remaining == 0 {
            return None;
        }
        let mut record = None;
        let mut send = None;
        for (_, id) in &self.table.order {
            let entry = self.table.entries.get(*id).expect("order names a held entry");
            let eligible = self.table.eligible(entry);
            if entry.save || entry.erase_pending() || (eligible && entry.record.first_sent.is_none()) {
                record = Some(*id);
                break;
            }
            if send.is_none() && eligible {
                send = Some(*id);
            }
        }
        if let Some(id) = record {
            let eligible = self.table.eligible(self.table.entries.get(id).expect("held entry"));
            let entry = self.table.entries.get_mut(id).expect("held entry");
            self.remaining = self.remaining.checked_sub(1).expect("visit has room");
            if entry.erase_pending() {
                entry.state = State::Retired { erase: false };
                return Some(RequestOut::Erase(entry.record.key));
            }
            if eligible && entry.record.first_sent.is_none() {
                entry.record.first_sent = Some(self.wall);
                entry.anchor = Some(Anchor { now: self.now, age: Duration::ZERO });
            }
            entry.save = false;
            return Some(RequestOut::Save(&entry.record));
        }
        let id = send?;
        let owner = self.table.entries.get(id).expect("held entry").owner;
        let attempt = self.table.fresh_attempt(owner).expect("eligibility checked numbering");
        self.table.attempt = attempt.raw();
        let entry = self.table.entries.get_mut(id).expect("held entry");
        entry.state = State::InFlight(attempt);
        entry.progress(RequestProgress::InFlight);
        self.remaining = self.remaining.checked_sub(1).expect("visit has room");
        Some(RequestOut::Send { attempt, key: entry.record.key, request: &entry.record.request })
    }
}

fn earliest(next: &mut Option<Time>, deadline: Time) {
    match *next {
        Some(current) => {
            if deadline < current {
                *next = Some(deadline);
            }
        }
        None => *next = Some(deadline),
    }
}
