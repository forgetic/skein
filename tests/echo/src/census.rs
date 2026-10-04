//! What a run's trace shows of the faults (testing-strategy.md, 3): a fault
//! configured that never falls tests nothing.

use std::collections::BTreeSet;

use skein_sim::{Entry, Event, Fault};

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
