//! The real loop (testing-strategy.md, 2.8): the same processes and referee
//! as a simulated world, in one thread and one loop, whose calls go to the
//! real kernel through one ring, on loopback, with deadlines on the real
//! clock (examples.md, section 6). It keeps token bindings and each host
//! operation limit, never service state. `run` blocks on the shared ring
//! only when every process is idle. It does not replay.

use alloc::format;
use alloc::vec::Vec;

use alloc::collections::{BTreeMap, VecDeque};

use skein_io::kernel::{Complete, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token};
use skein_shell::{Clock, Config, Kernel, Now, Wait};

use crate::host::Host;
use crate::referee::Referee;

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

/// Runs the processes on one ring, renaming their tokens and enforcing each
/// host's operation limit, until the referee passes and everything settles.
#[must_use]
pub fn run<P: Host, R: Referee<P>>(mut procs: Vec<P>, mut referee: R, clock: &Clock, patience: Duration) -> Outcome<P> {
    assert!(!procs.is_empty(), "a real loop runs at least one process");
    let limits: Vec<u32> = procs.iter().map(Host::operations).collect();
    let operations = limits.iter().try_fold(0_u32, |sum, limit| sum.checked_add(*limit)).expect("world operations fit");
    let mut ring = Ring::new(operations, limits);
    let start = clock.now().now;
    let deadline = start.saturating_add(patience);
    let mut iterations: u32 = 0;
    loop {
        iterations = iterations.checked_add(1).expect("fewer than 2^32 iterations");
        let Now { now, wall } = clock.now();
        assert!(now < deadline, "the real loop settles within {} ms", patience.as_nanos().div_euclid(1_000_000));
        referee.act(now, &mut procs);
        ring.reap();
        for (at, proc) in procs.iter_mut().enumerate() {
            ring.deliver(at, proc.completions());
            proc.iterate(now, wall);
            ring.submit(at, proc.submissions());
        }
        referee.observe(now, &procs);
        if procs.iter().any(|proc| proc.work_pending(now)) || ring.has_ready() {
            continue;
        }
        if procs.iter().all(Host::is_empty) && ring.is_empty() && referee.passed() {
            return Outcome { procs, iterations, start, end: now };
        }
        if let Some(why) = referee.overdue(now) {
            let at = now.saturating_since(start).as_nanos().div_euclid(1_000_000);
            crate::fail(&format!("at {at} ms, the referee failed the real loop:\n{why}"));
        }
        let mut until = deadline;
        let deadlines = procs.iter().map(Host::next_deadline).chain([referee.next_deadline()]);
        for at in deadlines.flatten() {
            until = until.min(at);
        }
        ring.wait(until);
    }
}

/// One host's original operation identity, including a cancel's target.
struct Binding {
    host: usize,
    local: Token,
    cancel: Option<Token>,
}

/// The shared kernel and the bindings kept until records return to their host.
struct Ring {
    kernel: Kernel,
    next: u64,
    limits: Vec<u32>,
    counts: Vec<u32>,
    bindings: BTreeMap<Token, Binding>,
    tokens: BTreeMap<(usize, Token), Token>,
    ready: Vec<VecDeque<Complete>>,
    submits: Queue<Submit>,
    arrived: Queue<Complete>,
}

impl Ring {
    fn new(operations: u32, limits: Vec<u32>) -> Self {
        let kernel = match Kernel::open(Config { operations }) {
            Ok(kernel) => kernel,
            Err(error) => crate::fail(&format!("io_uring is not usable here, so the real loop cannot run: {error}")),
        };
        Self {
            kernel,
            next: 0,
            counts: vec![0; limits.len()],
            ready: (0..limits.len()).map(|_| VecDeque::new()).collect(),
            limits,
            bindings: BTreeMap::new(),
            tokens: BTreeMap::new(),
            submits: Queue::with_capacity(operations),
            arrived: Queue::with_capacity(operations),
        }
    }

    fn reap(&mut self) {
        self.kernel.reap(&mut self.arrived);
        while let Some(mut complete) = self.arrived.pop() {
            let binding = self.bindings.remove(&complete.op).expect("a world token has its binding");
            self.tokens.remove(&(binding.host, binding.local)).expect("a local token has its binding");
            complete.op = binding.local;
            if let Some(target) = binding.cancel {
                complete.kind = Op::Cancel { target };
            }
            self.ready.get_mut(binding.host).expect("a queue per host").push_back(complete);
        }
    }

    fn deliver(&mut self, host: usize, out: &mut Queue<Complete>) {
        while out.room() > 0 {
            let Some(complete) = self.ready.get_mut(host).expect("a queue per host").pop_front() else { break };
            let count = self.counts.get_mut(host).expect("a count per host");
            *count = count.checked_sub(1).expect("a returned operation was in flight");
            out.push(complete);
        }
    }

    fn submit(&mut self, host: usize, records: &mut Queue<Submit>) {
        while let Some(mut record) = records.pop() {
            assert!(
                self.counts.get(host).expect("a count per host") < self.limits.get(host).expect("a limit per host"),
                "host {host} exceeds its own operation limit"
            );
            assert!(!self.tokens.contains_key(&(host, record.op)), "a host never reuses a token in flight");
            self.next = self.next.checked_add(1).expect("world tokens do not wrap");
            let token = Token::new(self.next);
            let local = record.op;
            let cancel = if let Op::Cancel { target } = &mut record.kind {
                let original = *target;
                // An already-reaped target cannot name any future operation.
                *target = self.tokens.get(&(host, original)).copied().unwrap_or(Token::new(0));
                Some(original)
            } else {
                None
            };
            self.tokens.insert((host, local), token);
            self.bindings.insert(token, Binding { host, local, cancel });
            let count = self.counts.get_mut(host).expect("a count per host");
            *count = count.checked_add(1).expect("within the host limit");
            record.op = token;
            self.submits.push(record);
        }
        self.kernel.submit(&mut self.submits, Wait::No);
        assert!(self.submits.is_empty(), "the ring is provisioned for every host's limit");
    }

    fn has_ready(&self) -> bool {
        self.ready.iter().any(|ready| !ready.is_empty())
    }

    fn is_empty(&self) -> bool {
        self.kernel.in_flight() == 0 && !self.has_ready()
    }

    fn wait(&mut self, until: Time) {
        self.kernel.submit(&mut self.submits, Wait::Until(until));
    }
}
