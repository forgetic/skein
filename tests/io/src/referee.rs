//! The referee (testing-strategy.md, 7): a scenario's expectations, as a
//! small step machine beside the world. It watches from outside, what the
//! owners saw, never io's state. Safety is checked on every observation and
//! fails at once; liveness is a deadline, and one that passes fails the test
//! with what is still pending.

use std::collections::BTreeMap;
use std::fmt;

use skein_io::Error;
use skein_lib::Time;

use crate::owner::{Conn, Owner};

/// What a scenario expects of a connection, named by its process and its
/// plan's name.
#[derive(Clone)]
pub enum Expect {
    /// Safety: what it received is a prefix of `bytes`, at every
    /// observation. Liveness: all of them by `by`, unless its stream broke.
    Receives { at: usize, conn: &'static str, bytes: Box<[u8]>, by: Time },
    /// Safety only: what it received is a prefix of `bytes`, at every
    /// observation; met once it is closed by `by`, or if it is never made (a
    /// server's, when its client's connect failed).
    Prefix { at: usize, conn: &'static str, bytes: Box<[u8]>, by: Time },
    /// Liveness: it is closed by `by`; or, if `optional`, it was never made.
    Closed { at: usize, conn: &'static str, by: Time, optional: bool },
    /// Liveness: its connect fails with one of `errors` by `by`.
    Fails { at: usize, conn: &'static str, errors: &'static [Error], by: Time },
    /// Safety: until `until`, it has handed io no more than `most` bytes.
    Holds { at: usize, conn: &'static str, most: u64, until: Time },
}

/// A scenario's expectations, each either pending or met.
#[derive(Debug)]
pub struct Referee {
    seed: u64,
    pending: BTreeMap<usize, Expect>,
    met: Vec<Expect>,
}

impl Referee {
    #[must_use]
    pub fn new(seed: u64, expectations: Vec<Expect>) -> Referee {
        Referee { seed, pending: expectations.into_iter().enumerate().collect(), met: Vec::new() }
    }

    /// Checks every expectation about process `at` against what its owner
    /// saw: safety at once, liveness when met.
    pub fn observe(&mut self, now: Time, at: usize, owner: &Owner) {
        let mut met = Vec::new();
        for (key, expect) in &self.pending {
            if expect.at() == at && self.check(now, expect, owner) {
                met.push(*key);
            }
        }
        self.meet(met);
    }

    fn meet(&mut self, keys: Vec<usize>) {
        for key in keys {
            let expect = self.pending.remove(&key).expect("pending");
            self.met.push(expect);
        }
    }

    /// Whether `expect` is met; panics if it is broken.
    fn check(&self, now: Time, expect: &Expect, owner: &Owner) -> bool {
        let seed = self.seed;
        let conn = |name: &str| -> Option<&Conn> { owner.conn(name) };
        match expect {
            Expect::Receives { conn: name, bytes, .. } | Expect::Prefix { conn: name, bytes, .. } => {
                let Some(conn) = conn(name) else { return false };
                let same = common(&conn.received, bytes);
                assert!(
                    same == conn.received.len(),
                    "seed {seed}: {name} received what its peer sent, in order: {same} bytes of {}, then {:?}",
                    bytes.len(),
                    &conn.received[same..conn.received.len().min(same + 16)],
                );
                let whole = matches!(expect, Expect::Receives { .. }) && conn.received.len() == bytes.len();
                let closed = conn.closed_at.is_some() && (conn.broken() || matches!(expect, Expect::Prefix { .. }));
                whole || closed
            }
            Expect::Closed { conn: name, .. } => conn(name).is_some_and(|conn| conn.closed_at.is_some()),
            Expect::Fails { conn: name, errors, .. } => {
                let Some(conn) = conn(name) else { return false };
                if let Some(error) = conn.error {
                    assert!(errors.contains(&error), "seed {seed}: {name} failed with {error:?}, not {errors:?}");
                    return true;
                }
                assert!(conn.closed_at.is_none(), "seed {seed}: {name} closed without failing");
                false
            }
            Expect::Holds { conn: name, most, until, .. } => {
                if now >= *until {
                    return true;
                }
                if let Some(conn) = conn(name) {
                    assert!(
                        conn.handed <= *most,
                        "seed {seed}: {name} handed io {} bytes before {} ns, past {most}: backpressure failed",
                        conn.handed,
                        until.as_nanos()
                    );
                }
                false
            }
        }
    }

    /// The expectations that a connection never made meets, met: the world
    /// settled, or their deadline came, without it.
    fn absent(&mut self, owners: &[&Owner], due: Option<Time>) {
        let mut met = Vec::new();
        for (key, expect) in &self.pending {
            let optional = match expect {
                Expect::Prefix { .. } | Expect::Closed { optional: true, .. } => true,
                Expect::Closed { optional: false, .. }
                | Expect::Receives { .. }
                | Expect::Fails { .. }
                | Expect::Holds { .. } => false,
            };
            let reached = due.is_none_or(|now| expect.deadline() <= now);
            if optional && reached && owners[expect.at()].conn(expect.conn()).is_none() {
                met.push(*key);
            }
        }
        self.meet(met);
    }

    /// The world settled: nothing more will be made.
    pub fn settled(&mut self, owners: &[&Owner]) {
        self.absent(owners, None);
    }

    /// When the earliest pending expectation falls due.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.pending.values().map(Expect::deadline).min()
    }

    /// Fails the test if an expectation fell due unmet by `now`.
    pub fn overdue(&mut self, now: Time, owners: &[&Owner], trace: &dyn Fn() -> String) {
        self.absent(owners, Some(now));
        let overdue: Vec<&Expect> = self.pending.values().filter(|expect| expect.deadline() <= now).collect();
        assert!(
            overdue.is_empty(),
            "seed {}: at {} ns, unmet: {overdue:?}\nstill pending: {:?}\n{}",
            self.seed,
            now.as_nanos(),
            self.pending.values().collect::<Vec<_>>(),
            trace()
        );
    }

    /// Whether every expectation is met.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.pending.is_empty()
    }
}

impl Expect {
    const fn at(&self) -> usize {
        match self {
            Expect::Receives { at, .. }
            | Expect::Prefix { at, .. }
            | Expect::Closed { at, .. }
            | Expect::Fails { at, .. }
            | Expect::Holds { at, .. } => *at,
        }
    }

    const fn conn(&self) -> &'static str {
        match self {
            Expect::Receives { conn, .. }
            | Expect::Prefix { conn, .. }
            | Expect::Closed { conn, .. }
            | Expect::Fails { conn, .. }
            | Expect::Holds { conn, .. } => conn,
        }
    }

    const fn deadline(&self) -> Time {
        match self {
            Expect::Receives { by, .. }
            | Expect::Prefix { by, .. }
            | Expect::Closed { by, .. }
            | Expect::Fails { by, .. } => *by,
            Expect::Holds { until, .. } => *until,
        }
    }
}

/// Briefly: the bytes by their length.
impl fmt::Debug for Expect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (at, conn, when) = (self.at(), self.conn(), self.deadline().as_nanos());
        match self {
            Expect::Receives { bytes, .. } => write!(f, "{conn}@{at} receives {} bytes by {when} ns", bytes.len()),
            Expect::Prefix { bytes, .. } => {
                write!(f, "{conn}@{at} receives a prefix of {} bytes, closed by {when} ns", bytes.len())
            }
            Expect::Closed { optional, .. } => write!(f, "{conn}@{at} closed by {when} ns (optional: {optional})"),
            Expect::Fails { errors, .. } => write!(f, "{conn}@{at} fails with one of {errors:?} by {when} ns"),
            Expect::Holds { most, .. } => write!(f, "{conn}@{at} hands io no more than {most} bytes until {when} ns"),
        }
    }
}

fn common(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}
