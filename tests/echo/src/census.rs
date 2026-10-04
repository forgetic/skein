//! What a run's trace shows of the faults (testing-strategy.md, 3): a fault
//! configured that never falls tests nothing.

use std::collections::BTreeSet;

use skein_sim::{Entry, Event, Fault};

use crate::proc::Proc;
use crate::scenarios::IDLE;

/// Every fault of the simulator.
pub const FAULTS: [Fault; 10] = [
    Fault::Latency,
    Fault::ShortRecv,
    Fault::ShortSend,
    Fault::Reset,
    Fault::Refuse,
    Fault::NoBuffer,
    Fault::TimedOut,
    Fault::CancelRace,
    Fault::CancelUnsubmitted,
    Fault::LateReset,
];

/// What a connection's attempts came to, as its fake client saw them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Outcome {
    /// Every line answered.
    Served,
    /// Told `busy`, at the domain's entrance.
    Busy,
    /// Ended without a word, unanswered: rejected at the protocol layer's
    /// entrance, or idled out before a line.
    Silent,
    /// Its stream broke, by a reset.
    Broken,
    /// Its line past the limit told `too long`.
    TooLong,
    /// Ended by the server once idle past its deadline.
    Idled,
}

/// Every outcome a connection can come to.
pub const OUTCOMES: [Outcome; 6] =
    [Outcome::Served, Outcome::Busy, Outcome::Silent, Outcome::Broken, Outcome::TooLong, Outcome::Idled];

/// Every outcome the fake clients among `procs` saw.
#[must_use]
pub fn outcomes(procs: &[Proc]) -> BTreeSet<Outcome> {
    let mut seen = BTreeSet::new();
    for client in procs.iter().filter_map(Proc::as_client) {
        for conn in 0..client.conns() {
            let facts = client.seen(conn);
            let idled = match (facts.ended, facts.progress) {
                (Some(ended), Some(progress)) => ended.saturating_since(progress) >= IDLE,
                (Some(_) | None, _) => false,
            };
            let had = [
                (facts.complete && !facts.too_long, Outcome::Served),
                (facts.busy > 0, Outcome::Busy),
                (facts.silent > 0, Outcome::Silent),
                (facts.broken > 0, Outcome::Broken),
                (facts.too_long, Outcome::TooLong),
                (idled, Outcome::Idled),
            ];
            for (happened, outcome) in had {
                if happened {
                    seen.insert(outcome);
                }
            }
        }
    }
    seen
}

/// The faults that fell.
#[must_use]
pub fn faults(trace: &[Entry]) -> BTreeSet<Fault> {
    let mut seen = BTreeSet::new();
    for entry in trace {
        if let Event::Fault(fault) = entry.event {
            seen.insert(fault);
        }
    }
    seen
}
