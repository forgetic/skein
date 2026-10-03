//! The counting allocator (testing-strategy.md, 6): it counts the live heap
//! of each thread and its peak, so that a test can check the most a part
//! held at once in each step against its worst case (programming-model.md,
//! 6.3). skein supplies it to a service's worlds as to its own (testing.md,
//! 5). Each memory test binary declares it its global allocator, and
//! measures each step with a [`Meter`]:
//!
//! ```ignore
//! #[global_allocator]
//! static HEAP: skein_heap::Counting = skein_heap::Counting;
//! ```
//!
//! Counts are kept for each thread, so that tests running side by side do not
//! see each other's.
//!
//! # A step's own
//!
//! What a step hands out in its requests is their receivers' to count: a
//! payload moved into a request is no longer the step's (programming-model.md,
//! 6.2), and the layer that holds it next counts it in its own worst case. So
//! a step is checked at the most it held of its own: at each moment, what was
//! live less what it had handed out by then. Which blocks it handed out is
//! known only once the test drops its requests, after the step; so the meter
//! numbers every allocation, in a header before the block, and keeps the
//! moments the step's heap reached a new high, by the allocation that reached
//! it (no other moment can hold more of the step's own, as what it hands out
//! only grows). A block freed after the step was handed out by each of those
//! moments that its number is no later than.
//!
//! # The `unsafe`
//!
//! A global allocator is an `unsafe impl`: beside the ring adapter, the one
//! `unsafe` in skein, allowed because this crate is test-only and never
//! linked into a service (testing.md, 6). It only counts; `System`
//! allocates. On an allocation's path it never panics: where an invariant of
//! its own breaks, it aborts, as a global allocator must not unwind.

use core::alloc::{GlobalAlloc, Layout};
use core::cell::{Cell, RefCell};
use core::fmt::Debug;
use core::ptr;
use std::alloc::System;
use std::process;

/// The allocator: System's, counted.
#[derive(Debug)]
pub struct Counting;

/// A moment a step's heap reached a new high: the last allocation made by
/// then, by number, what was live, and of what the step handed out, how much
/// was allocated after the high before it and by this one (so that what it
/// handed out by a high is the sum of these up to it). A high that stands
/// for several moments (see [`Highs`]) is the first's, with the heap of the
/// last, and `low` the first's.
#[derive(Clone, Copy, Debug)]
struct High {
    made: u64,
    low: i64,
    live: i64,
    handed: u64,
}

/// The highs of the step measured last. Beyond its room, two adjacent highs
/// become one that counts the later's heap at the earlier's moment, the two
/// whose moments it then spans the least: it holds no less of the step's own
/// than any moment it stands for, so a check stays sound, and errs by no
/// more than the heap its moments span.
#[derive(Debug)]
struct Highs {
    len: usize,
    at: [High; KEPT],
}

/// The highs kept before two become one.
const KEPT: usize = 256;

/// The bytes of a block's number, in the header before it.
const NUMBER: usize = size_of::<u64>();

/// Whether a step is being measured, or what it handed out counted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Idle,
    /// A step runs: its highs are kept.
    Stepping,
    /// A step has ended: what is freed from now on that was allocated since
    /// the meter's base, numbered after `before`, was handed out.
    Handing {
        before: u64,
    },
}

// Const-initialised and without a destructor: reaching them never allocates
// and never fails, so the allocator can use them. Nothing allocates while it
// borrows the highs.
thread_local! {
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
    // Allocations made so far, which numbers them from one.
    static MADE: Cell<u64> = const { Cell::new(0) };
    static PHASE: Cell<Phase> = const { Cell::new(Phase::Idle) };
    static HIGHS: RefCell<Highs> = const { RefCell::new(Highs::NONE) };
}

/// Counts a block of `layout` allocated, and numbers it. The heap's counts
/// are signed, as a block freed on another thread than its own takes that
/// thread's below what it allocated, and wrap rather than trap: a panic
/// here would allocate.
fn allocated(layout: Layout) -> u64 {
    let made = MADE.get().wrapping_add(1);
    MADE.set(made);
    let live = LIVE.get().wrapping_add(size(layout));
    LIVE.set(live);
    if live > PEAK.get() {
        PEAK.set(live);
        if PHASE.get() == Phase::Stepping {
            with_highs(|highs| highs.push(High { made, low: live, live, handed: 0 }));
        }
    }
    made
}

/// Counts the block of `layout` numbered `number` freed.
fn freed(layout: Layout, number: u64) {
    LIVE.set(LIVE.get().wrapping_sub(size(layout)));
    let Phase::Handing { before } = PHASE.get() else {
        return;
    };
    if number > before {
        let bytes = u64::try_from(layout.size()).unwrap_or(u64::MAX);
        with_highs(|highs| highs.hand(number, bytes));
    }
}

fn size(layout: Layout) -> i64 {
    i64::try_from(layout.size()).unwrap_or(i64::MAX)
}

/// The highs, on the allocator's paths. They are never borrowed already, as
/// nothing allocates while it borrows them.
fn with_highs<F: FnOnce(&mut Highs)>(change: F) {
    HIGHS.with(|highs| match highs.try_borrow_mut() {
        Ok(mut highs) => change(&mut highs),
        Err(_) => broken(),
    });
}

/// Where an invariant of the allocator's own breaks: it aborts. A global
/// allocator must not unwind, which is undefined behaviour, and a test target
/// unwinds on a panic; a panic would allocate besides.
fn broken() -> ! {
    process::abort()
}

impl Highs {
    const NONE: Highs = Highs { len: 0, at: [High { made: 0, low: 0, live: 0, handed: 0 }; KEPT] };

    fn kept(&self) -> &[High] {
        let Some(kept) = self.at.get(..self.len) else { broken() };
        kept
    }

    fn kept_mut(&mut self) -> &mut [High] {
        let Some(kept) = self.at.get_mut(..self.len) else { broken() };
        kept
    }

    /// The number of the last allocation made before the step began, which
    /// names it.
    fn started(&self) -> Option<u64> {
        Some(self.kept().first()?.made)
    }

    fn push(&mut self, high: High) {
        if self.len == KEPT {
            self.merge();
        }
        let Some(slot) = self.at.get_mut(self.len) else { broken() };
        *slot = high;
        let Some(len) = self.len.checked_add(1) else { broken() };
        self.len = len;
    }

    /// Makes room: two adjacent highs become one.
    fn merge(&mut self) {
        let mut first = 0;
        let mut least = i64::MAX;
        for (at, pair) in self.kept().windows(2).enumerate() {
            let [earlier, later] = pair else { broken() };
            let span = later.live.saturating_sub(earlier.low);
            if span < least {
                (first, least) = (at, span);
            }
        }
        let Some(from) = self.kept_mut().get_mut(first..) else { broken() };
        let [earlier, later, ..] = from else { broken() };
        earlier.live = later.live;
        // The later goes: turned to the end of the highs kept, which then
        // end before it.
        let Some(after) = from.get_mut(1..) else { broken() };
        after.rotate_left(1);
        let Some(len) = self.len.checked_sub(1) else { broken() };
        self.len = len;
    }

    /// The block numbered `number`, of `bytes`, was handed out: by each high
    /// it was allocated by, from the first on, as the highs are in the order
    /// of their numbers.
    fn hand(&mut self, number: u64, bytes: u64) {
        let kept = self.kept_mut();
        let first = kept.partition_point(|high| high.made < number);
        if let Some(high) = kept.get_mut(first) {
            high.handed = high.handed.saturating_add(bytes);
        }
    }

    /// The most the step held of its own at a high, measured since `meter`'s
    /// base: what was live less what it had handed out by then. `None` if it
    /// had handed out more than was live, which the counts rule out.
    fn own(&self, meter: &Meter) -> Option<u64> {
        let mut own = 0;
        let mut handed: u64 = 0;
        for high in self.kept() {
            handed = handed.saturating_add(high.handed);
            own = meter.since(high.live)?.checked_sub(handed)?.max(own);
        }
        Some(own)
    }
}

/// The block System allocates for one of `layout`, and how far into it the
/// caller's begins: room before it for its number, as far as its alignment
/// puts it and no less than a number takes. `None` past what a layout can
/// describe.
fn padded(layout: Layout) -> Option<(Layout, usize)> {
    let offset = layout.align().max(NUMBER);
    let padded = Layout::from_size_align(layout.size().checked_add(offset)?, offset).ok()?;
    Some((padded, offset))
}

#[expect(
    unsafe_code,
    reason = "a global allocator is an unsafe impl; it only counts, and System allocates (testing.md, 6)"
)]
// SAFETY: each block is `offset` bytes into one of System's, which is aligned
// to `offset`, a multiple of the block's alignment, and `offset` bytes longer
// than the block: the block has its layout, within System's. Its number is in
// the bytes just before it, which no one else reaches. Counting touches no
// memory the caller sees. realloc and alloc_zeroed keep their default bodies,
// which call these two: a realloc counts the old block and the new at once,
// as they are both live while it copies.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some((padded, offset)) = padded(layout) else {
            return ptr::null_mut();
        };
        // SAFETY: `padded` is not empty: it has room for a number at least.
        let base = unsafe { System.alloc(padded) };
        if base.is_null() {
            return base;
        }
        let number = allocated(layout);
        let block = base.wrapping_add(offset);
        // SAFETY: the number's bytes are the last of the `offset` before the
        // block, no fewer than a number takes: within System's block.
        unsafe { block.wrapping_sub(NUMBER).cast::<u64>().write_unaligned(number) };
        block
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // It was allocated with `layout`, which padded then.
        let Some((padded, offset)) = padded(layout) else {
            return;
        };
        // SAFETY: `ptr` came from `alloc` with `layout`, above: its number is
        // in the bytes just before it.
        let number = unsafe { ptr.wrapping_sub(NUMBER).cast::<u64>().read_unaligned() };
        // SAFETY: System's block begins `offset` bytes before `ptr`, and was
        // allocated with `padded`.
        unsafe { System.dealloc(ptr.wrapping_sub(offset), padded) };
        freed(layout, number);
    }
}

/// What one step held, measured by a [`Meter`]: the most at once while it
/// ran, and what it held when it ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Measured {
    peak: u64,
    held: u64,
    /// The allocations made before it started, which names it.
    started: u64,
}

impl Measured {
    /// The most heap live at once while the step ran, since the meter's base,
    /// with what it handed out, which [`Meter::check`] takes off.
    #[must_use]
    pub const fn peak(&self) -> u64 {
        self.peak
    }

    /// The heap live when the step ended, since the meter's base.
    #[must_use]
    pub const fn held(&self) -> u64 {
        self.held
    }
}

/// Measures the heap of what a test builds after it: a domain, a container.
#[derive(Debug)]
pub struct Meter {
    base: i64,
    /// The allocations made before it.
    before: u64,
}

impl Meter {
    /// Measures from now: what is allocated after this, and not freed, is
    /// the measured's.
    #[must_use]
    pub fn new() -> Meter {
        Meter { base: LIVE.get(), before: MADE.get() }
    }

    /// Bytes allocated since the base and not freed.
    #[must_use]
    pub fn held(&self) -> u64 {
        self.since(LIVE.get()).expect("nothing freed that was not allocated since the base")
    }

    /// Starts a step: the highs from now on are the step's, the first being
    /// what is live now.
    pub fn start(&self) {
        let (made, live) = (MADE.get(), LIVE.get());
        PEAK.set(live);
        HIGHS.with_borrow_mut(|highs| {
            highs.len = 0;
            highs.push(High { made, low: live, live, handed: 0 });
        });
        PHASE.set(Phase::Stepping);
    }

    /// Ends a step: the most held at once since it started, and what is held
    /// now. What is freed from now on was handed out by it, until the next
    /// starts.
    #[must_use]
    pub fn end(&self) -> Measured {
        assert!(PHASE.get() == Phase::Stepping, "a step ends after it starts");
        PHASE.set(Phase::Handing { before: self.before });
        let started = HIGHS.with_borrow(Highs::started).expect("a step's first high is its start");
        let peak = self.since(PEAK.get()).expect("nothing freed that was not allocated since the base");
        Measured { peak, held: self.held(), started }
    }

    /// Checks `step` against `bound`, a worst case, and returns the most it
    /// held of its own at once: what was live less what it had handed out by
    /// then, which its receivers count. What it handed out is what has been
    /// freed since it ended, as the test took its requests and dropped them:
    /// the test frees nothing else in between, and checks before the next
    /// step starts. `what` names the step if it fails.
    pub fn check(&self, step: Measured, bound: u64, what: &dyn Debug) -> u64 {
        let started = HIGHS.with_borrow(Highs::started);
        assert!(
            started == Some(step.started) && PHASE.get() != Phase::Stepping,
            "a step is checked once it has ended, before the next starts"
        );
        let own = HIGHS.with_borrow(|highs| highs.own(self));
        let own = own.expect("what a step handed out by a high was live at it");
        assert!(own <= bound, "{what:?}: {own} bytes held at the peak of a step, more than the worst case of {bound}");
        own
    }

    /// `live`, a count of the live heap, since the base.
    fn since(&self, live: i64) -> Option<u64> {
        u64::try_from(live.checked_sub(self.base)?).ok()
    }
}

impl Default for Meter {
    fn default() -> Meter {
        Meter::new()
    }
}
