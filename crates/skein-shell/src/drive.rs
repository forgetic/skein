//! A service loop and its adapter (shell.md, sections 12 and 13).
//! The host keeps every service entity and queue; the loop knows only its
//! settlement, work and deadlines, never its domain state.

use skein_io::kernel::{Complete, Exit, Submit};
use skein_lib::{Queue, Time, Wall};

/// A process a world hosts: a service, or a fake, each a step machine with a
/// loop of its own (programming-model.md, section 2). The harness reaps its
/// completions into [`Host::completions`], calls [`Host::iterate`], and
/// submits [`Host::submissions`], as its shell would.
pub trait Host {
    /// Drain shell diagnostics once, after iteration and before submission.
    fn drain(&mut self) {}

    /// One turn of its loop, between its reap and its submit: both passes
    /// and the reclaim point, at `now` and `wall`.
    fn iterate(&mut self, now: Time, wall: Wall);

    fn completions(&mut self) -> &mut Queue<Complete>;

    fn submissions(&mut self) -> &mut Queue<Submit>;

    /// Whether `iterate` has work at `now` without the kernel.
    fn work_pending(&self, now: Time) -> bool;

    /// Its earliest deadline, over every layer.
    fn next_deadline(&self) -> Option<Time>;

    /// Its earliest policy deadline, excluding io close, retry and file stalls
    /// (testing-strategy.md, section 6). Every host states it explicitly.
    fn next_policy_deadline(&self) -> Option<Time>;

    /// Whether it holds nothing: every slab empty, nothing in flight, every
    /// queue empty (testing-strategy.md, 6).
    fn is_empty(&self) -> bool;

    /// A hosted child's terminal exit, after it closes its resources. Hosts
    /// with a failure status override the successful empty-host default.
    fn exit(&self) -> Option<Exit> {
        if self.is_empty() { Some(Exit::Code(0)) } else { None }
    }

    /// The most heap it may hold (programming-model.md, 6.3).
    fn worst_case(&self) -> u64;

    /// The most operations it has in flight at once: the size of its ring.
    fn operations(&self) -> u32;
}

/// Runs a service until its entities and kernel operations have settled.
/// Its exit belongs to the host, after its final drain (shell.md, 13).
pub fn drive(kernel: &mut crate::Kernel, clock: &crate::Clock, host: &mut impl Host) -> Exit {
    loop {
        kernel.reap(host.completions());
        let crate::Now { now, wall } = clock.now();
        host.iterate(now, wall);
        host.drain();
        if host.is_empty() {
            assert!(host.completions().is_empty(), "an empty host has no completions");
            assert!(host.submissions().is_empty(), "an empty host has no submissions");
            assert_eq!(kernel.in_flight(), 0, "an empty host has no kernel operation in flight");
            return host.exit().expect("an empty host has a terminal exit");
        }
        let wait = if host.work_pending(now) {
            crate::Wait::No
        } else {
            match host.next_deadline() {
                Some(deadline) => crate::Wait::Until(deadline),
                None => crate::Wait::Forever,
            }
        };
        kernel.submit(host.submissions(), wait);
    }
}
