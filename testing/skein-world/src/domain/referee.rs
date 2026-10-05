//! Observation-based safety, liveness and fault injection (testing-strategy.md,
//! section 7). Scenario expectations see boundary observations, not domain
//! state; the world handles emitted stimuli before continuing its iteration.

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};

use alloc::collections::{BTreeMap, BTreeSet};
use core::fmt::{self, Debug, Display};

use skein_lib::{Duration, Time};

use super::Schedule;

/// A scenario's expectations (testing-strategy.md, section 7), which a [`Referee`] holds a
/// world to. Each world writes its own: what its referee observes, the names
/// of what it expects to happen, what it injects, and what it checks of each
/// observation.
pub trait Expectations {
    /// What the referee observes: what the fakes see, and the facts the service
    /// emits; never a domain's state.
    type Seen;
    /// Names a liveness expectation while it is pending, and in the list of
    /// those pending when the test fails.
    type Name: Ord + Clone + Debug;
    /// What the referee injects that belongs to no fake: a channel dropping,
    /// a component restarting.
    type Stimulus;

    /// Checks `seen` against the safety expectations, and arms or meets the
    /// liveness ones, through `judge`.
    fn observe(&mut self, seen: Self::Seen, judge: &mut Judge<Self::Name, Self::Stimulus>);
}

/// A scenario's expectations as a step machine of the world's loop
/// (testing-strategy.md, section 7): observations in, stimuli out, and a deadline table of
/// its own, whose earliest deadline the loop covers as it covers every
/// domain's. It steps only on what it observes and on its own deadlines, and
/// its [`Verdict`] ends the test: passed, failed, or stopped early by the
/// scenario.
///
/// A stimulus comes out at a moment of the scenario, from [`Referee::fire`]
/// once it is due; or at once, from the [`Referee::observe`] whose
/// observation called for it, so that a world can inject it between two
/// things it does (the engine restarting after a write and before the
/// next). A world handles what comes out of either before it goes on.
///
/// The harness keeps the boundary contracts and the invariants once a world
/// settles (section 6); the referee keeps what the scenario expects of the
/// system as a whole.
#[derive(Debug)]
pub struct Referee<X: Expectations> {
    expectations: X,
    judge: Judge<X::Name, X::Stimulus>,
}

impl<X: Expectations> Referee<X> {
    /// Starts with no pending obligations or stimuli; scenario observations arm them.
    #[must_use]
    pub fn new(expectations: X) -> Referee<X> {
        Referee { expectations, judge: Judge::new() }
    }

    /// The expectations, for what they counted.
    #[must_use]
    pub fn expectations(&self) -> &X {
        &self.expectations
    }

    /// Injects `stimulus` at `at`: a moment of the scenario set up before it
    /// starts.
    pub fn inject(&mut self, at: Time, stimulus: X::Stimulus) {
        self.judge.inject(at, stimulus);
    }

    /// The referee observes `seen` at `now`; what it injects at once goes
    /// to `out`.
    pub fn observe(&mut self, now: Time, seen: X::Seen, out: &mut Vec<X::Stimulus>) {
        self.judge.now = now;
        self.expectations.observe(seen, &mut self.judge);
        out.append(&mut self.judge.now_out);
    }

    /// The referee's earliest deadline: a liveness expectation's, or a
    /// stimulus's moment.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let liveness = self.judge.deadlines.first().map(|(at, _)| *at);
        [liveness, self.judge.stimuli.next_time()].into_iter().flatten().min()
    }

    /// Whether a deadline of the referee's is due by `now`.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        self.next_deadline().is_some_and(|at| at <= now)
    }

    /// Fires what is due by `now`: a liveness expectation still pending past
    /// its deadline fails the test, and is no longer pending; the stimuli due
    /// go to `out`, in the order they were injected.
    pub fn fire(&mut self, now: Time, out: &mut Vec<X::Stimulus>) {
        self.judge.now = now;
        while let Some((at, name)) = self.judge.deadlines.first().cloned() {
            if at > now {
                break;
            }
            self.judge.deadlines.pop_first();
            self.judge.pending.remove(&name);
            self.judge.fail(format_args!("{name:?} was not met by {}", Moment(at)));
        }
        while let Some(stimulus) = self.judge.stimuli.next(now) {
            out.push(stimulus);
        }
    }

    /// Where the test stands.
    #[must_use]
    pub fn verdict(&self) -> Verdict {
        if let Some(failure) = &self.judge.failure {
            return Verdict::Failed(failure.clone());
        }
        if self.judge.stopped {
            return Verdict::Stopped { pending: self.judge.pending_list() };
        }
        if self.judge.pending.is_empty() && self.judge.stimuli.is_empty() {
            Verdict::Passed
        } else {
            Verdict::Open { pending: self.judge.pending_list() }
        }
    }

    /// Ends the test early if an expectation was broken: panics with why, and
    /// with `seed`, which replays it.
    pub fn assert_holding(&self, seed: u64) {
        if let Some(failure) = &self.judge.failure {
            crate::fail(&format!("seed {seed}: {failure}"));
        }
    }

    /// Whether the scenario has stopped the test: the world ends its run
    /// there, whatever is still in flight.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.judge.stopped && self.judge.failure.is_none()
    }

    /// Ends the test: panics unless every expectation was met and nothing is
    /// left to inject, or the scenario stopped it.
    pub fn assert_passed(&self, seed: u64) {
        match self.verdict() {
            Verdict::Passed | Verdict::Stopped { .. } => {}
            Verdict::Open { pending } => {
                crate::fail(&format!("seed {seed}: the referee still expects {}", List(&pending)))
            }
            Verdict::Failed(failure) => crate::fail(&format!("seed {seed}: {failure}")),
        }
    }

    /// How many safety checks the referee made, and how many liveness
    /// expectations it saw met (not those withdrawn, nor those whose
    /// deadline passed): that it judged something.
    #[must_use]
    pub fn judged(&self) -> (u64, u64) {
        (self.judge.checks, self.judge.met)
    }
}

/// What the expectations judge with: the safety checks, the liveness
/// expectations pending with their deadlines, and the stimuli to inject.
#[derive(Debug)]
pub struct Judge<N, S> {
    now: Time,
    /// The liveness expectations pending, by name with their deadlines, and
    /// by deadline.
    pending: BTreeMap<N, Time>,
    deadlines: BTreeSet<(Time, N)>,
    /// The stimuli to inject at their moments, and those to inject at once,
    /// with the observation being judged.
    stimuli: Schedule<S>,
    now_out: Vec<S>,
    /// The first expectation broken, and whether the scenario stopped the
    /// test.
    failure: Option<Failure>,
    stopped: bool,
    checks: u64,
    met: u64,
}

impl<N: Ord + Clone + Debug, S> Judge<N, S> {
    fn new() -> Judge<N, S> {
        Judge {
            now: Time::ZERO,
            pending: BTreeMap::new(),
            deadlines: BTreeSet::new(),
            stimuli: Schedule::new(),
            now_out: Vec::new(),
            failure: None,
            stopped: false,
            checks: 0,
            met: 0,
        }
    }

    /// When the observation being judged was made.
    #[must_use]
    pub fn now(&self) -> Time {
        self.now
    }

    /// A safety expectation: it `holds`, or the test fails for `why`.
    pub fn check<D: Display>(&mut self, holds: bool, why: D) {
        self.checks = self.checks.checked_add(1).expect("referee safety count is representable");
        if !holds {
            self.fail(why);
        }
    }

    /// A safety expectation broken: the test fails for `why`, unless it has
    /// failed already.
    pub fn fail<D: Display>(&mut self, why: D) {
        if self.failure.is_none() {
            let pending = self.pending_list();
            self.failure = Some(Failure { at: self.now, why: why.to_string(), pending });
        }
    }

    /// A liveness expectation: `name` is to be met within `within` of now, or
    /// the test fails. No other pending has its name.
    pub fn expect(&mut self, name: N, within: Duration) {
        let at = self.now.saturating_add(within);
        assert!(
            self.pending.insert(name.clone(), at).is_none(),
            "each expectation pending has a name of its own: {name:?}"
        );
        self.deadlines.insert((at, name));
    }

    /// Arms the liveness expectation `name` again, to be met within `within`
    /// of now, whether or not it is pending: after a restart, say, the work
    /// it waits for starts over.
    pub fn rearm(&mut self, name: N, within: Duration) {
        if let Some(at) = self.pending.remove(&name) {
            self.deadlines.remove(&(at, name.clone()));
        }
        self.expect(name, within);
    }

    /// Withdraws the liveness expectation `name`, if it is pending, and says
    /// whether it was: what it waited for will not happen, as the scenario
    /// meant (work cancelled). It is not counted as met.
    pub fn withdraw(&mut self, name: &N) -> bool {
        let Some(at) = self.pending.remove(name) else {
            return false;
        };
        self.deadlines.remove(&(at, name.clone()));
        true
    }

    /// Meets the liveness expectation `name`, if it is pending, and says
    /// whether it was.
    pub fn meet(&mut self, name: &N) -> bool {
        let Some(at) = self.pending.remove(name) else {
            return false;
        };
        self.deadlines.remove(&(at, name.clone()));
        self.met = self.met.checked_add(1).expect("referee liveness count is representable");
        true
    }

    /// Whether `name` is pending.
    #[must_use]
    pub fn is_pending(&self, name: &N) -> bool {
        self.pending.contains_key(name)
    }

    /// Injects `stimulus` at `at`, after those injected for then already: it
    /// comes out of the referee's `fire` once due.
    pub fn inject(&mut self, at: Time, stimulus: S) {
        self.stimuli.send(at, stimulus);
    }

    /// Injects `stimulus` at once: it comes out of the `observe` judging
    /// this observation, before the world goes on.
    pub fn inject_now(&mut self, stimulus: S) {
        self.now_out.push(stimulus);
    }

    /// Stops the test: the scenario has seen what it wanted, and the world
    /// ends its run here. What is still pending is listed, not failed; a
    /// failure, before or after, still fails the test.
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    /// The liveness expectations pending, by deadline.
    fn pending_list(&self) -> Vec<String> {
        self.deadlines.iter().map(|(at, name)| format!("{name:?} by {}", Moment(*at))).collect()
    }
}

/// Where a test stands, by its referee.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Verdict {
    /// Every expectation was met, and nothing is left to inject.
    Passed,
    /// Liveness expectations are pending, or stimuli are still to come.
    Open {
        /// Named liveness obligations in deadline order; stimuli may also remain.
        pending: Vec<String>,
    },
    /// The scenario stopped the test, with these liveness expectations still
    /// pending.
    Stopped {
        /// Obligations intentionally left open when the scenario stopped.
        pending: Vec<String>,
    },
    /// An expectation was broken.
    Failed(
        /// First broken expectation and its boundary-observation time.
        Failure,
    ),
}

/// Why a test failed: a safety expectation an observation broke, or a
/// liveness expectation whose deadline passed; when, and what was still
/// pending then.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Failure {
    /// World time when the first safety or liveness failure was observed.
    pub at: Time,
    /// Scenario-provided reason, retained without interpreting service policy.
    pub why: String,
    /// Liveness obligations still open when the failure was recorded.
    pub pending: Vec<String>,
}

impl Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "at {}, {}", Moment(self.at), self.why)?;
        if !self.pending.is_empty() {
            write!(f, "; still pending: {}", List(&self.pending))?;
        }
        Ok(())
    }
}

/// A time, in seconds.
struct Moment(Time);

impl Display for Moment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nanos = self.0.as_nanos();
        write!(f, "{}.{:09}s", nanos / 1_000_000_000, nanos % 1_000_000_000)
    }
}

/// Lines, joined by commas.
struct List<'a>(&'a [String]);

impl Display for List<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join(", "))
    }
}
