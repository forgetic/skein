//! The real loop (testing-strategy.md, 2.8): the same processes and referee
//! as a simulated world, in one thread and one loop, whose calls go to the
//! real kernel through the shell's rings, on loopback, with deadlines on the
//! real clock. Each process has a ring of its own, as it would as a process:
//! its tokens are its own. It does not replay.
//!
//! One thread cannot block on several rings at once, so each turn enters
//! every ring without waiting, and only when no process has work does the
//! loop block, on one ring, until the earliest deadline and at most
//! [`TICK`], so that the others' completions are reaped soon after.

use alloc::format;
use alloc::vec::Vec;

use skein_lib::{Duration, Time};
use skein_shell::{Clock, Config, Kernel, Now, Wait};

use crate::host::Host;
use crate::referee::Referee;

/// The longest the loop blocks on one ring while others may have
/// completions.
pub const TICK: Duration = Duration::from_millis(1);

/// What a real run left.
#[derive(Debug)]
pub struct Outcome<P> {
    /// The processes as the run left them, settled.
    pub procs: Vec<P>,
    pub iterations: u32,
    /// When the run began and settled, on the real clock.
    pub start: Time,
    pub end: Time,
}

/// Runs `procs` on the real kernel, each on a ring of its own, with
/// `referee` beside them, until the referee passed and everything settled,
/// within `patience` of `clock`'s now. A machine where `io_uring` cannot be
/// used fails here, saying so, rather than passing in silence.
#[must_use]
pub fn run<P: Host, R: Referee<P>>(mut procs: Vec<P>, mut referee: R, clock: &Clock, patience: Duration) -> Outcome<P> {
    let mut kernels = Vec::with_capacity(procs.len());
    for proc in &procs {
        match Kernel::open(Config { operations: proc.operations() }) {
            Ok(kernel) => kernels.push(kernel),
            Err(error) => crate::fail(&format!("io_uring is not usable here, so the real loop cannot run: {error}")),
        }
    }
    let start = clock.now().now;
    let deadline = start.saturating_add(patience);
    let mut iterations: u32 = 0;
    loop {
        iterations = iterations.checked_add(1).expect("fewer than 2^32 iterations");
        let Now { now, wall } = clock.now();
        assert!(now < deadline, "the real loop settles within {} ms", patience.as_nanos().div_euclid(1_000_000));
        referee.act(now, &mut procs);
        for (proc, kernel) in procs.iter_mut().zip(&mut kernels) {
            kernel.reap(proc.completions());
            proc.iterate(now, wall);
            kernel.submit(proc.submissions(), Wait::No);
        }
        referee.observe(now, &procs);
        if procs.iter().any(|proc| proc.work_pending(now)) {
            continue;
        }
        let settled = procs.iter().zip(&kernels).all(|(proc, kernel)| proc.is_empty() && kernel.in_flight() == 0);
        if settled && referee.passed() {
            return Outcome { procs, iterations, start, end: now };
        }
        if let Some(why) = referee.overdue(now) {
            let at = now.saturating_since(start).as_nanos().div_euclid(1_000_000);
            crate::fail(&format!("at {at} ms, the referee failed the real loop:\n{why}"));
        }
        let mut until = now.saturating_add(TICK);
        let deadlines = procs.iter().map(Host::next_deadline).chain([referee.next_deadline()]);
        for at in deadlines.flatten() {
            until = until.min(at);
        }
        let (Some(proc), Some(kernel)) = (procs.first_mut(), kernels.first_mut()) else {
            crate::fail("a real loop runs at least one process");
        };
        kernel.submit(proc.submissions(), Wait::Until(until));
    }
}
