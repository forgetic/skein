//! Memory at every iteration (simulator.md, 5; testing-strategy.md, 6): the
//! heap the hosted processes hold, against the sum of their worst cases.
//!
//! The processes, the simulator and the harness share one thread, so the
//! counting allocator sees one heap (testing.md, 9). The processes' part is
//! told apart by metering around their own calls (building each, and each
//! `iterate`) with a [`Span`]: what the heap grew by within them is theirs,
//! and everything else (the simulator's trace and network, the referee's
//! notes, the harness's own) is left out. The check is of the most they held
//! at once within each call: what they held before it, and the call's peak
//! growth.

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

/// The processes' heap, metered.
#[derive(Debug)]
pub(crate) struct Heap {
    /// What the processes hold: what grew within their calls.
    held: i64,
    /// The sum of their worst cases.
    bound: i64,
    /// The most they held at once.
    most: i64,
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
        Heap { held: 0, bound: 0, most: 0 }
    }

    /// Makes a process, metered, and admits its worst case: what it holds
    /// once made must be within it.
    pub(crate) fn admit<T, F: FnOnce() -> T>(&mut self, make: F, worst_case: fn(&T) -> u64) -> T {
        let span = Span::start();
        let proc = make();
        let grown = span.end();
        let worst_case = i64::try_from(worst_case(&proc)).expect("a worst case within an i64");
        self.bound = self.bound.checked_add(worst_case).expect("worst cases within an i64");
        self.check(grown);
        proc
    }

    /// Runs one of a process's calls, metered: what the heap grew by is the
    /// processes', and the most they held at once within it must be within
    /// the sum of their worst cases.
    pub(crate) fn around<T, F: FnOnce() -> T>(&mut self, call: F) -> T {
        let span = Span::start();
        let result = call();
        self.check(span.end());
        result
    }

    fn check(&mut self, grown: Grown) {
        let most = self.held.checked_add(grown.peak).expect("a heap within an i64");
        assert!(
            most <= self.bound,
            "the processes held {most} bytes at once, past the sum of their worst cases, {} (programming-model.md, 6.3)",
            self.bound
        );
        self.most = self.most.max(most);
        self.held = self.held.checked_add(grown.net).expect("a heap within an i64");
    }

    /// The most the processes held at once.
    pub(crate) fn most(&self) -> u64 {
        u64::try_from(self.most).unwrap_or(0)
    }

    /// The sum of their worst cases.
    pub(crate) fn bound(&self) -> u64 {
        u64::try_from(self.bound).unwrap_or(0)
    }
}
