//! What the harness needs of a process.

use skein_io::kernel::{Complete, Exit, Submit};
use skein_lib::{Queue, Time, Wall};

/// A process a world hosts: a service, or a fake, each a step machine with a
/// loop of its own (programming-model.md, section 2). The harness reaps its
/// completions into [`Host::completions`], calls [`Host::iterate`], and
/// submits [`Host::submissions`], as its shell would.
pub trait Host {
    /// One turn of its loop, between its reap and its submit: both passes
    /// and the reclaim point, at `now` and `wall`.
    fn iterate(&mut self, now: Time, wall: Wall);

    fn completions(&mut self) -> &mut Queue<Complete>;

    fn submissions(&mut self) -> &mut Queue<Submit>;

    /// Whether `iterate` has work at `now` without the kernel.
    fn work_pending(&self, now: Time) -> bool;

    /// Its earliest deadline, over every layer.
    fn next_deadline(&self) -> Option<Time>;

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
