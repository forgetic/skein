//! What a run's trace shows of the races (testing-strategy.md, 3): each
//! cancel io made, by what it cancelled and what its answer said, and each
//! fault that fell.

use std::collections::{BTreeMap, BTreeSet};

use skein_io::kernel::{Done, Error};
use skein_lib::Token;
use skein_sim::{Entry, Event, Fault, Pid, Summary};

/// What a cancel's completion said about its target (kernel.md, 5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Answer {
    Stopped,
    TooLate,
    Unsubmitted,
}

/// The operations io cancels: those that wait.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Target {
    Accept,
    Connect,
    Recv,
    Send,
}

/// The pairings of what was cancelled and what the cancel answered.
#[must_use]
pub fn cancels(trace: &[Entry]) -> BTreeSet<(Target, Answer)> {
    let mut submitted: BTreeMap<(Pid, Token), Summary> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for entry in trace {
        match entry.event {
            Event::Submit { op, kind } => {
                submitted.insert((entry.pid, op), kind);
            }
            Event::Complete { kind: Summary::Cancel { target }, result, .. } => {
                let target = match submitted.get(&(entry.pid, target)) {
                    Some(Summary::Accept { .. }) => Target::Accept,
                    Some(Summary::Connect { .. }) => Target::Connect,
                    Some(Summary::Recv { .. }) => Target::Recv,
                    Some(Summary::Send { .. }) => Target::Send,
                    other => panic!("io cancels only what waits: {other:?}"),
                };
                let answer = match result {
                    Ok(Done::Nothing) => Answer::Stopped,
                    Err(Error::TooLate) => Answer::TooLate,
                    Err(Error::Other(_) | Error::InvalidArgument) => Answer::Unsubmitted,
                    other => panic!("a cancel answers stopped, too late or unsubmitted: {other:?}"),
                };
                seen.insert((target, answer));
            }
            Event::Complete { .. } | Event::Fault(_) => {}
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
