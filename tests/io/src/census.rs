//! What a run's trace shows of the races (testing-strategy.md, 3): each
//! cancel io made, by what it cancelled and what its answer said, and each
//! fault that fell.

use std::collections::{BTreeMap, BTreeSet};

use skein_io::kernel::{Done, Error, Fd};
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
        match &entry.event {
            Event::Submit { op, kind } => {
                submitted.insert((entry.pid, *op), *kind);
            }
            Event::Complete { kind: Summary::Cancel { target }, result, .. } => {
                let target = match submitted.get(&(entry.pid, *target)) {
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

/// The operations whose completion failed with `error`, by their kind as the
/// trace names them: an accept or a socket out of descriptors, say.
#[must_use]
pub fn failures(trace: &[Entry], error: Error) -> Vec<Summary> {
    let mut seen = Vec::new();
    for entry in trace {
        if let Event::Complete { kind, result: Err(failed), .. } = entry.event
            && failed == error
        {
            seen.push(kind);
        }
    }
    seen
}

/// How many accepted sockets io discarded: closed by the process that
/// accepted them, with nothing ever received on them.
#[must_use]
pub fn discards(trace: &[Entry]) -> u32 {
    let mut accepted: BTreeSet<(Pid, Fd)> = BTreeSet::new();
    let mut discarded = 0;
    for entry in trace {
        match entry.event {
            Event::Complete { kind: Summary::Accept { .. }, result: Ok(Done::Accepted { fd, .. }), .. } => {
                accepted.insert((entry.pid, fd));
            }
            Event::Submit { kind: Summary::Recv { fd, .. }, .. } => {
                accepted.remove(&(entry.pid, fd));
            }
            Event::Submit { kind: Summary::Close { fd }, .. } => {
                if accepted.remove(&(entry.pid, fd)) {
                    discarded += 1;
                }
            }
            Event::Submit { .. } | Event::Complete { .. } | Event::Fault(_) => {}
        }
    }
    discarded
}

/// Whether a listener's first accept waited an iteration for the accept
/// batch: armed in a later iteration than its listen completed in. `marks`
/// are where each iteration began in the trace.
#[must_use]
pub fn batched(trace: &[Entry], marks: &[usize]) -> bool {
    let iteration = |at: usize| marks.partition_point(|mark| *mark <= at);
    let mut listened: BTreeMap<(Pid, Fd), usize> = BTreeMap::new();
    for (at, entry) in trace.iter().enumerate() {
        match entry.event {
            Event::Complete { kind: Summary::Listen { fd, .. }, result: Ok(_), .. } => {
                listened.insert((entry.pid, fd), iteration(at));
            }
            Event::Submit { kind: Summary::Accept { fd }, .. } => {
                if let Some(when) = listened.remove(&(entry.pid, fd))
                    && iteration(at) > when
                {
                    return true;
                }
            }
            Event::Submit { .. } | Event::Complete { .. } | Event::Fault(_) => {}
        }
    }
    false
}
