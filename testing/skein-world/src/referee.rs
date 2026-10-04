//! The referee (testing-strategy.md, 7): a scenario's expectations, a step
//! machine beside the processes, at every tier. It watches from outside,
//! what the processes show of themselves (a fake's observations, a service's
//! public face), never their state. Safety is checked on every observation
//! and fails at once; liveness is a deadline, and one that passes unmet
//! fails the test with what is still pending. It ends the test once every
//! expectation is met and the world has settled, and it may inject what
//! belongs to no process: a shutdown, a directory of addresses.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::{self, Debug, Write};
use core::marker::PhantomData;

use skein_lib::Time;

/// A scenario's referee, over its processes `P`.
pub trait Referee<P> {
    /// Injects what belongs to no process, before they run in an iteration.
    /// It changes only what a process lets be changed between iterations (a
    /// flag it acts on in its next one), and allocates nothing of theirs.
    fn act(&mut self, now: Time, procs: &mut [P]);

    /// Watches every process, after one of them iterated: a broken safety
    /// expectation panics, naming the seed.
    fn observe(&mut self, now: Time, procs: &[P]);

    /// The earliest of its expectations' deadlines and of what it will act
    /// on, for the world to wake at.
    fn next_deadline(&self) -> Option<Time>;

    /// What is overdue at `now`, if anything: the test fails with it.
    fn overdue(&self, now: Time) -> Option<String>;

    /// Whether every expectation is met.
    fn passed(&self) -> bool;
}

/// One expectation of a scenario's, about its processes `P`.
pub trait Expectation<P>: Debug {
    /// Whether it is met by what the processes show at `now`; `Err`, saying
    /// why, if a safety rule is broken.
    fn check(&self, now: Time, procs: &[P]) -> Result<bool, String>;

    /// When it must be met by.
    fn deadline(&self) -> Time;
}

/// A scenario's expectations, each pending until met: what a referee keeps.
pub struct Expectations<P, X> {
    seed: u64,
    pending: Vec<X>,
    met: Vec<X>,
    procs: PhantomData<fn(&[P])>,
}

impl<P, X: Expectation<P>> Expectations<P, X> {
    #[must_use]
    pub fn new(seed: u64, expectations: Vec<X>) -> Expectations<P, X> {
        Expectations { seed, pending: expectations, met: Vec::new(), procs: PhantomData }
    }

    /// Checks every pending expectation: a broken one panics, naming the
    /// seed; a met one is set aside.
    pub fn observe(&mut self, now: Time, procs: &[P]) {
        let mut at = 0;
        while let Some(expectation) = self.pending.get(at) {
            match expectation.check(now, procs) {
                Ok(true) => {
                    let met = self.pending.remove(at);
                    self.met.push(met);
                }
                Ok(false) => at = at.checked_add(1).expect("fewer expectations than memory"),
                Err(why) => crate::fail(&broke(self.seed, now, expectation, &why)),
            }
        }
    }

    /// When the earliest pending expectation falls due.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut next: Option<Time> = None;
        for expectation in &self.pending {
            let at = expectation.deadline();
            next = Some(match next {
                Some(next) => next.min(at),
                None => at,
            });
        }
        next
    }

    /// The pending expectations past their deadline at `now`, with all that
    /// is pending, if any is.
    #[must_use]
    #[expect(clippy::use_debug, reason = "what is overdue is printed for a person reading a failure")]
    pub fn overdue(&self, now: Time) -> Option<String> {
        let mut overdue = String::new();
        for expectation in &self.pending {
            if expectation.deadline() <= now {
                writeln!(overdue, "  unmet: {expectation:?}").expect("writing to a String");
            }
        }
        if overdue.is_empty() {
            return None;
        }
        writeln!(overdue, "  pending: {:?}", self.pending).expect("writing to a String");
        Some(overdue)
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.pending.is_empty()
    }

    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }
}

impl<P, X: Debug> Debug for Expectations<P, X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Expectations").field("seed", &self.seed).field("pending", &self.pending).finish_non_exhaustive()
    }
}

/// What a broken safety expectation fails the world with.
fn broke(seed: u64, now: Time, expectation: &dyn Debug, why: &str) -> String {
    format!("seed {seed}: at {} ns, {expectation:?} broke: {why}", now.as_nanos())
}
