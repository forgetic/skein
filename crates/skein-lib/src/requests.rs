//! Bounded keyed requests and their store barrier (lib.md, section 12).
//!
//! The table owns requests, opaque scopes and owner tokens, declared byte
//! counts, seeded keys and attempt tokens. It never reads a request. `ask`
//! and `restore` admit values; `link` and `confirmed` control eligibility.
//! `take` lends a bounded cursor: apply every `Save` before handling a
//! `Send`. Dropping a cursor leaves untaken work in the table.
//!
//! | State | Event | Next | Emits through take |
//! |---|---|---|---|
//! | absent | ask | parked or ready | save, then eligible send |
//! | absent | restore | parked | nothing until eligible |
//! | parked | link up and matching confirmation | ready | first-send save if needed, send |
//! | ready | take send | in flight | one attempt under the held key |
//! | in flight | take | in flight | nothing |
//!
//! The link starts down and no scope is confirmed. Restoration also waits
//! for known retention. First sending is recorded on the supplied wall
//! clock; monotonic time is supplied separately for lifecycle deadlines.

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
    /// Send the held request under a fresh attempt token.
    Send { attempt: Token, key: RequestKey, request: &'a R },
}

#[derive(Debug)]
struct Entry<R> {
    record: RequestRecord<R>,
    owner: Token,
    size: u64,
    restored: bool,
    save: bool,
    attempt: Option<Token>,
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
}

/// A transient bounded visit, borrowing the table only while outputs are taken.
#[derive(Debug)]
pub struct RequestTake<'a, R> {
    table: &'a mut RequestTable<R>,
    remaining: u32,
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
        }
    }

    /// Admit a request, or return ownership without changing anything at a limit.
    pub fn ask(&mut self, owner: Token, scope: u64, size: u64, request: R) -> Result<RequestKey, R> {
        if !self.fits(size) || self.serial == u64::MAX {
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
    }

    /// Permit only requests belonging to this opaque scope.
    pub fn confirmed(&mut self, scope: Option<u64>) {
        self.scope = scope;
    }

    /// Supply the answer retention before restored requests can leave.
    pub fn retention(&mut self, span: Duration) {
        self.retention = Some(span);
    }

    /// Visit at most `out` records and sends, with every record before sends.
    pub fn take(&mut self, _now: Time, wall: Wall) -> RequestTake<'_, R> {
        for (_, id) in &self.order {
            let entry = self.entries.get_mut(*id).expect("order names a held entry");
            if self.up
                && self.scope == Some(entry.record.scope)
                && (!entry.restored || self.retention.is_some())
                && entry.attempt.is_none()
                && entry.record.first_sent.is_none()
            {
                entry.record.first_sent = Some(wall);
                entry.save = true;
            }
        }
        RequestTake { remaining: self.limits.out, table: self }
    }

    /// Whether store records or eligible sends remain to take.
    #[must_use]
    pub fn pending(&self) -> bool {
        for (_, id) in &self.order {
            let entry = self.entries.get(*id).expect("order names a held entry");
            if entry.save || self.eligible(entry) {
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
        let entry = Entry { record, owner, size, restored, save: !restored, attempt: None };
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

    fn eligible(&self, entry: &Entry<R>) -> bool {
        self.up
            && self.scope == Some(entry.record.scope)
            && entry.attempt.is_none()
            && (!entry.restored || self.retention.is_some())
            && self.attempt < u64::MAX
    }
}

impl<R> RequestTake<'_, R> {
    /// Take the next output; an untaken output stays held when the visit ends.
    pub fn next_out(&mut self) -> Option<RequestOut<'_, R>> {
        if self.remaining == 0 {
            return None;
        }
        let mut save = None;
        let mut send = None;
        for (_, id) in &self.table.order {
            let entry = self.table.entries.get(*id).expect("order names a held entry");
            if entry.save {
                save = Some(*id);
                break;
            }
            if send.is_none() && self.table.eligible(entry) {
                send = Some(*id);
            }
        }
        if let Some(id) = save {
            let entry = self.table.entries.get_mut(id).expect("held entry");
            entry.save = false;
            self.remaining = self.remaining.checked_sub(1).expect("visit has room");
            return Some(RequestOut::Save(&entry.record));
        }
        let id = send?;
        self.table.attempt = self.table.attempt.checked_add(1).expect("eligibility checked numbering");
        let attempt = Token::new(self.table.attempt);
        let entry = self.table.entries.get_mut(id).expect("held entry");
        entry.attempt = Some(attempt);
        self.remaining = self.remaining.checked_sub(1).expect("visit has room");
        Some(RequestOut::Send { attempt, key: entry.record.key, request: &entry.record.request })
    }
}
