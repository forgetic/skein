//! Memory at every iteration (simulator.md, 5; testing-strategy.md, 6): the
//! heap each hosted process holds, against its own worst case.
//!
//! The processes, the simulator and the harness share one thread, so the
//! counting allocator sees one heap. Each process's part is told apart by
//! metering around its own calls (building it, and each `iterate`) with a
//! [`Span`]: what the heap grew by within them is that process's. Nothing
//! one process allocates is freed by another, or by the simulator, which
//! hands every buffer back and never drops, copies or replaces one
//! (`skein_io::kernel`); so everything else (the simulator's trace and
//! network, the referee's notes, the harness's own) is left out, and each
//! process is checked against its own worst case. The check is of the most
//! a process held at once within each call: what it held before the call,
//! and the call's peak growth.

use alloc::format;
use alloc::vec::Vec;
use core::hint;

use skein_heap::{Grown, Span};

/// Whether a world checks memory: only a test binary that declares the
/// counting allocator its global allocator can (testing.md, 6).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Memory {
    /// The heap checked at every iteration; the world fails to start unless
    /// the counting allocator counts.
    Checked,
    /// No check: the system allocator.
    Unchecked,
}

/// Each process's heap, metered, by its index in the world.
#[derive(Debug)]
pub(crate) struct Heap {
    procs: Vec<Held>,
}

/// One process's heap.
#[derive(Clone, Copy, Debug)]
struct Held {
    /// What it holds: what grew within its calls.
    now: i64,
    /// The most it held at once.
    most: i64,
    /// Its worst case.
    bound: i64,
}

impl Heap {
    /// The processes' heap, from nothing, once the counting allocator is seen
    /// to count.
    pub(crate) fn new() -> Heap {
        let span = Span::start();
        let probe = hint::black_box(Box::new([0_u8; 64]));
        assert!(
            span.end().net >= 64,
            "a world that checks memory runs under the counting allocator: #[global_allocator] static HEAP: skein_heap::Counting"
        );
        drop(probe);
        Heap { procs: Vec::new() }
    }

    /// Makes the next process, metered, and admits it with its worst case:
    /// what it holds once made must be within it.
    pub(crate) fn admit<T, F: FnOnce() -> T>(&mut self, make: F, worst_case: fn(&T) -> u64) -> T {
        let span = Span::start();
        let proc = make();
        let grown = span.end();
        let bound = i64::try_from(worst_case(&proc)).expect("a worst case within an i64");
        self.procs.push(Held { now: 0, most: 0, bound });
        let at = self.procs.len().checked_sub(1).expect("just pushed");
        self.check(at, grown);
        proc
    }

    /// Runs one of process `at`'s calls, metered: what the heap grew by is
    /// the process's, and the most it held at once within the call must be
    /// within its worst case.
    pub(crate) fn around<T, F: FnOnce() -> T>(&mut self, at: usize, call: F) -> T {
        let span = Span::start();
        let result = call();
        self.check(at, span.end());
        result
    }

    fn check(&mut self, at: usize, grown: Grown) {
        let held = self.procs.get_mut(at).expect("a process admitted");
        let most = held.now.checked_add(grown.peak).expect("a heap within an i64");
        if most > held.bound {
            let bound = held.bound;
            crate::fail(&format!(
                "process {at} held {most} bytes at once, past its worst case, {bound} (programming-model.md, 6.3)"
            ));
        }
        held.most = held.most.max(most);
        held.now = held.now.checked_add(grown.net).expect("a heap within an i64");
    }

    /// The most each process held at once, and its worst case, by index.
    pub(crate) fn report(&self) -> Vec<(u64, u64)> {
        let mut report = Vec::with_capacity(self.procs.len());
        for held in &self.procs {
            report.push((u64::try_from(held.most).unwrap_or(0), u64::try_from(held.bound).unwrap_or(0)));
        }
        report
    }
}
