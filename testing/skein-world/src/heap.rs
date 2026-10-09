//! Memory at every iteration (simulator.md, 5; testing-strategy.md, 6).
//!
//! The harness checks each hosted process's heap against its own worst
//! case. One thread, one heap: a process's part is what grew within the
//! calls that run its code (making it, and each `iterate`), metered with a
//! [`Span`], at its peak within each call. The simulator's submit and reap,
//! the referee and the harness run between those calls, so the simulator's
//! trace and network and the harness's heap are left out. This holds while
//! nothing a process owns is allocated or freed outside its calls: the
//! backend hands every buffer back and never copies or replaces one
//! (`skein_io::kernel`); a crash cut destroys its original records under
//! their owner's span. The queues it fills and drains are the process's
//! own, bounded and made with it; and the referee changes a process only
//! through flags that allocate nothing. Once settled, the harness checks it:
//! each process, dropped, frees exactly what was metered as its own
//! ([`Outcome`](crate::Outcome)), which also finds a leak.

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
    /// The largest provision across this process's incarnations, for reports.
    largest_bound: i64,
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
        self.procs.push(Held { now: 0, most: 0, bound, largest_bound: bound });
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

    /// Drops a terminated process under its own span and removes its meter.
    pub(crate) fn release<T>(&mut self, at: usize, proc: T) -> (u64, u64) {
        let held = self.procs.remove(at);
        let span = Span::start();
        drop(proc);
        let freed = span.end().net.checked_neg().expect("a heap within an i64");
        if freed != held.now {
            crate::fail(&format!(
                "terminated process {at} freed {freed} bytes, not the {} it held of its own: a leak, or heap made or freed outside its calls",
                held.now
            ));
        }
        (
            u64::try_from(held.most).expect("a nonnegative peak"),
            u64::try_from(held.largest_bound).expect("a nonnegative bound"),
        )
    }

    /// Drops a cut process and meters its replacement in the same slot.
    pub(crate) fn restart<T, F: FnOnce() -> T>(&mut self, at: usize, proc: T, make: F, worst_case: fn(&T) -> u64) -> T {
        let old = *self.procs.get(at).expect("a process admitted");
        let span = Span::start();
        drop(proc);
        let freed = span.end().net.checked_neg().expect("heap fits i64");
        assert_eq!(freed, old.now, "the cut process releases exactly its remaining owned heap");
        let span = Span::start();
        let proc = make();
        let grown = span.end();
        let bound = i64::try_from(worst_case(&proc)).expect("worst case fits i64");
        *self.procs.get_mut(at).expect("the same process slot") = Held { now: 0, most: 0, bound, largest_bound: bound };
        self.check(at, grown);
        let held = self.procs.get_mut(at).expect("the replacement's meter");
        held.most = held.most.max(old.most);
        held.largest_bound = held.largest_bound.max(old.largest_bound);
        proc
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

    /// What each process holds of its own now, by index.
    pub(crate) fn held(&self) -> Vec<i64> {
        let mut held = Vec::with_capacity(self.procs.len());
        for proc in &self.procs {
            held.push(proc.now);
        }
        held
    }

    /// The most each process held at once, and its worst case, by index.
    pub(crate) fn report(&self) -> Vec<(u64, u64)> {
        let mut report = Vec::with_capacity(self.procs.len());
        for held in &self.procs {
            report.push((u64::try_from(held.most).unwrap_or(0), u64::try_from(held.largest_bound).unwrap_or(0)));
        }
        report
    }
}
