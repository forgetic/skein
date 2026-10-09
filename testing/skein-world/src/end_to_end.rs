//! Shipped binaries beside the scripted processes of a real world
//! (testing-strategy.md, section 2.9; examples.md, section 6). This module
//! keeps the child's pidfd, captured stderr and pending kernel operations;
//! it never knows the service's state. `Binary::start` opens its pipes or
//! controlling terminal, `take_streams` transfers the scripted ends for
//! adoption, and the `Host` entry points wait, signal, read and close on
//! the world's shared ring. The referee observes stderr and `exit_status`.
//! A supervised tree retains its keeper after leader exit; `signal_tree` queues
//! member-pidfd delivery by the keeper, while the referee owns the grace.

use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use skein_io::kernel::{Complete, Done, Error, Exit, Fd, Op, Pipe, Signal, Spawn, Submit, Target, Way};
use skein_lib::{Duration, Queue, Time, Token, Wall, bytes};
use skein_shell::{Config, Kernel, OpenError, Wait};

use crate::Host;
use crate::tree::{self, Tree};
pub use crate::tree::{Cleanup, Counts, Expectation, Method, PeakScope, TreeStatus};

/// A scenario's binary command, with an exact environment and working directory.
#[derive(Debug)]
pub struct Command {
    pub program: PathBuf,
    pub arguments: Vec<OsString>,
    pub environment: Vec<(OsString, OsString)>,
    pub directory: PathBuf,
}

/// How the scenario connects its scripted person to the child.
#[derive(Clone, Copy, Debug)]
pub enum Mode {
    /// Separate stdin/stdout pipes, transferred to the scripted hosts.
    Pipes,
    /// One controlling terminal, transferred to the scripted host.
    Terminal,
}

/// Startup failed before a binary joined the loop; the scenario reports why.
#[derive(Debug)]
pub enum StartError {
    /// The real ring is unusable, so no child was started.
    Ring(OpenError),
    /// Opening the command's working directory failed with this errno.
    Directory(i32),
    /// Preparing tree containment failed before spawn.
    Tree(Error),
    /// Making pipes, the terminal or executing the child failed.
    Child(Error),
}

/// Scripted descriptors transferred by the child to their owning hosts.
#[derive(Debug)]
pub enum Streams {
    /// The person's writing end of stdin and reading end of stdout.
    Pipes { input: Fd, output: Fd },
    /// The person's bidirectional terminal master; the child owns the slave.
    Terminal { stream: Fd },
}

impl Streams {
    /// The descriptors a real world declares when admitting the scripted host.
    #[must_use]
    pub fn descriptors(&self) -> Vec<Fd> {
        match *self {
            Self::Pipes { input, output } => vec![input, output],
            Self::Terminal { stream } => vec![stream],
        }
    }
}

/// An external child's observer, owned by a real world until exit and closure.
#[derive(Debug)]
pub struct Binary {
    pidfd: Option<Fd>,
    stderr: Option<Fd>,
    streams: Option<Streams>,
    error_bytes: Vec<u8>,
    error_limit: usize,
    status: Option<Exit>,
    wait_started: bool,
    reading: bool,
    signal: Option<(Signal, Target)>,
    tree_signal: Option<Signal>,
    tree_sweep: Option<Signal>,
    tree: Option<Box<Tree>>,
    tree_status: Box<TreeStatus>,
    counts: Option<Counts>,
    expectation: Expectation,
    poll_at: Option<Time>,
    signalling: bool,
    closing: u32,
    next: u64,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
}

impl Binary {
    /// Starts a binary after probing `io_uring`; temporary startup resources
    /// close before returning. `stderr_limit` bounds captured bytes, and an
    /// overflow fails the scenario. Environment entries are not inherited.
    pub fn start(command: Command, mode: Mode, stderr_limit: usize) -> Result<Self, StartError> {
        Self::start_with_tree(command, mode, stderr_limit, Expectation::EndsWithBinary, false)
    }

    /// Starts a binary with an explicit tree expectation; `force_walk` exercises
    /// the subreaper path even on a machine with delegated cgroups. The walk
    /// requires one observer per test process, since its counts are process-wide.
    pub fn start_with_tree(
        command: Command,
        mode: Mode,
        stderr_limit: usize,
        expectation: Expectation,
        force_walk: bool,
    ) -> Result<Self, StartError> {
        let mut startup = Kernel::open(Config { operations: 1 }).map_err(StartError::Ring)?;
        let until = skein_shell::Clock::new().now().now.saturating_add(Duration::from_secs(2));
        let baseline = tree::call(&mut startup, Op::Usage, until).expect("startup usage succeeds");
        let Done::Usage(baseline) = baseline else { unreachable!("Usage returns usage") };
        let mut tree = Tree::prepare(force_walk, baseline).map_err(StartError::Tree)?;
        let root = skein_shell::open_root(&command.directory).map_err(StartError::Directory)?;
        let terminal = matches!(mode, Mode::Terminal);
        let mut pipes = Vec::new();
        if !terminal {
            pipes.push(Pipe { child: 0, way: Way::In, parent: None });
            pipes.push(Pipe { child: 1, way: Way::Out, parent: None });
        }
        pipes.push(Pipe { child: 2, way: Way::Out, parent: None });
        let mut spawn = Spawn {
            root,
            dir: Box::from([]),
            program: Box::from(command.program.as_os_str().as_bytes()),
            args: command.arguments.iter().map(|argument| Box::from(argument.as_bytes())).collect(),
            env: command
                .environment
                .iter()
                .map(|(name, value)| {
                    let mut entry = name.as_bytes().to_vec();
                    entry.push(b'=');
                    entry.extend_from_slice(value.as_bytes());
                    entry.into_boxed_slice()
                })
                .collect(),
            pipes: pipes.into_boxed_slice(),
        };
        let child = match tree.cgroup() {
            Some(cgroup) => skein_shell::start_binary_in_cgroup(&mut spawn, terminal, cgroup),
            None => skein_shell::start_binary(&mut spawn, terminal),
        };
        let mut submits = Queue::with_capacity(1);
        let mut completes = Queue::with_capacity(1);
        submits.push(Submit { op: Token::new(1), kind: Op::Close { fd: root } });
        startup.submit(&mut submits, Wait::No);
        while completes.is_empty() {
            startup.reap(&mut completes);
            if completes.is_empty() {
                startup.submit(&mut submits, Wait::Forever);
            }
        }
        let closed = completes.pop().expect("startup root close completed");
        assert!(matches!(closed.kind, Op::Close { .. }), "startup closes only its root");
        let (pidfd, master) = child.map_err(StartError::Child)?;
        tree.started(pidfd);
        let stderr = spawn.pipes.last().and_then(|pipe| pipe.parent).expect("stderr was requested");
        let streams = match master {
            Some(stream) => Streams::Terminal { stream },
            None => Streams::Pipes {
                input: spawn.pipes.first().and_then(|pipe| pipe.parent).expect("stdin was requested"),
                output: spawn.pipes.get(1).and_then(|pipe| pipe.parent).expect("stdout was requested"),
            },
        };
        Ok(Self {
            pidfd: Some(pidfd),
            stderr: Some(stderr),
            streams: Some(streams),
            error_bytes: Vec::new(),
            error_limit: stderr_limit,
            status: None,
            wait_started: false,
            reading: false,
            signal: None,
            tree_signal: None,
            tree_sweep: None,
            tree: Some(Box::new(tree)),
            tree_status: Box::new(TreeStatus::new()),
            counts: None,
            expectation,
            poll_at: Some(Time::ZERO),
            signalling: false,
            closing: 0,
            next: 0,
            completions: Queue::with_capacity(8),
            submissions: Queue::with_capacity(8),
        })
    }

    /// Transfers stdin/stdout or the terminal for adoption by scripted hosts.
    /// Unclaimed ends close on the binary observer's first iteration.
    pub fn take_streams(&mut self) -> Option<Streams> {
        self.streams.take()
    }

    /// The descriptors declared by the real world when it admits this observer.
    #[must_use]
    pub fn descriptors(&self) -> Vec<Fd> {
        self.pidfd.into_iter().chain(self.stderr).chain(self.streams.iter().flat_map(Streams::descriptors)).collect()
    }

    /// Queues a referee's pidfd signal for the next shared-ring iteration.
    pub fn signal(&mut self, signal: Signal) {
        assert!(self.status.is_none(), "the referee signals a child before its exit");
        assert!(self.signal.is_none(), "one pending referee signal at a time");
        self.signal = Some((signal, Target::Child));
    }

    /// Queues a group signal while the binary's leader is retained.
    pub fn signal_group(&mut self, signal: Signal) {
        assert!(self.pidfd.is_some(), "the leader remains owned until tree settlement");
        assert!(self.signal.is_none(), "one pending referee signal at a time");
        self.signal = Some((signal, Target::Group));
    }

    /// Queues a keeper signal to every discovered live tree member, including
    /// detached descendants after leader exit. `Supervise` leaves its grace and
    /// escalation deadlines to the referee. Membership is refreshed at delivery.
    pub fn signal_tree(&mut self, signal: Signal) {
        assert_eq!(self.expectation, Expectation::Supervise, "whole-tree policy belongs to a supervised observer");
        assert!(self.tree.is_some(), "a signal needs a retained tree keeper");
        assert!(self.tree_signal.is_none(), "one pending tree signal at a time");
        self.tree_signal = Some(signal);
    }

    /// The supervised tree's last poll and delivered cleanup signals. Its
    /// history remains available after `counts` reports genuine settlement.
    #[must_use]
    pub fn tree_status(&self) -> &TreeStatus {
        &self.tree_status
    }

    /// Ends a measured command; final counts arrive after its tree settles.
    pub fn end(&mut self) {
        match self.expectation {
            Expectation::Supervise => self.signal_tree(Signal::Kill),
            Expectation::EndsWithBinary | Expectation::Measure => self.signal_group(Signal::Kill),
        }
    }

    /// The terminal counts, present only once the binary and every descendant settled.
    #[must_use]
    pub const fn counts(&self) -> Option<Counts> {
        self.counts
    }

    /// What the child wrote to stderr; it is outside observation, not service state.
    #[must_use]
    pub fn stderr(&self) -> &[u8] {
        &self.error_bytes
    }

    /// The ring's child-exit event, available before final descriptor closure.
    #[must_use]
    pub const fn exit_status(&self) -> Option<Exit> {
        self.status
    }

    fn submit(&mut self, kind: Op) {
        self.next = self.next.checked_add(1).expect("binary operation tokens do not wrap");
        self.submissions.push(Submit { op: Token::new(self.next), kind });
    }

    fn close(&mut self, fd: Fd) {
        self.closing = self.closing.checked_add(1).expect("a small number of descriptors");
        self.submit(Op::Close { fd });
    }

    fn completed(&mut self, complete: Complete) {
        if let Op::Wait { .. } = complete.kind {
            let Ok(Done::Exit(status)) = complete.result else {
                crate::fail(&format!("binary wait failed: {complete:?}"))
            };
            self.status = Some(status);
        } else if let Op::PipeRead { buf, .. } = complete.kind {
            self.reading = false;
            let Ok(Done::Count(count)) = complete.result else {
                crate::fail(&format!("binary stderr read failed: {:?}", complete.result))
            };
            if count == 0 {
                let fd = self.stderr.take().expect("stderr remains open through its read");
                self.close(fd);
            } else {
                let count = usize::try_from(count).expect("a read count fits memory");
                let total = self.error_bytes.len().checked_add(count).expect("stderr length fits memory");
                assert!(
                    total <= self.error_limit,
                    "binary stderr exceeds its scenario limit of {} bytes",
                    self.error_limit
                );
                self.error_bytes
                    .extend_from_slice(buf.get(..count).expect("the kernel returned no more than requested"));
            }
        } else if let Op::Signal { .. } = complete.kind {
            self.signalling = false;
            if let Err(error) = complete.result {
                // Exit and a referee's signal may cross in the kernel.
                assert!(error == Error::NotFound || error == Error::Other(3), "binary signal failed: {error:?}");
            }
        } else if let Op::Close { .. } = complete.kind {
            self.closing = self.closing.checked_sub(1).expect("a submitted close completed");
        } else {
            unreachable!("the binary observer submits only wait, read, signal and close");
        }
    }
}

// Ordinary test code may unwind (programming-model.md, section 10.2).
// A failed scenario must not leave a shipped service running after its
// observer is discarded; a normally settled observer owns no descriptors.
impl Drop for Binary {
    fn drop(&mut self) {
        if let Some(tree) = &mut self.tree
            && let Some(pidfd) = self.pidfd
        {
            tree.abandon(pidfd);
        }
        let descriptors = self.descriptors();
        if !descriptors.is_empty() {
            skein_shell::abandon_binary(self.pidfd.take(), &descriptors);
        }
    }
}

impl Host for Binary {
    fn iterate(&mut self, now: Time, _wall: Wall) {
        let poll_tree = self.poll_at.is_some_and(|at| at <= now);
        if poll_tree {
            if let Some(tree) = &mut self.tree {
                tree.refresh();
            }
            self.poll_at = Some(now.saturating_add(Duration::from_millis(10)));
        }
        while let Some(complete) = self.completions.pop() {
            self.completed(complete);
        }
        if self.expectation == Expectation::Supervise
            && (poll_tree || self.status.is_some() && self.tree_status.live_at_leader_exit.is_none())
            && let Some(tree) = &mut self.tree
        {
            self.tree_status.live_pids = tree.survey(self.pidfd.expect("a retained tree has its leader pidfd"));
            if self.status.is_some() && self.tree_status.live_at_leader_exit.is_none() {
                self.tree_status.live_at_leader_exit = Some(tree.leader_exited(&self.tree_status.live_pids));
            }
        }
        if let Some(signal) = self.tree_signal.take().or(if poll_tree { self.tree_sweep } else { None }) {
            if signal == Signal::Kill {
                self.tree_sweep = Some(Signal::Kill);
            }
            let tree = self.tree.as_mut().expect("a pending signal has a tree");
            let (delivered, forced) = tree.signal(self.pidfd.expect("a retained tree has its leader pidfd"), signal);
            self.tree_status.forced_cleanup |= forced;
            let history = match signal {
                Signal::Terminate => &mut self.tree_status.terminated_pids,
                Signal::Kill => &mut self.tree_status.killed_pids,
            };
            history.extend(delivered);
            history.sort_unstable();
            history.dedup();
            self.tree_status.live_pids = tree.survey(self.pidfd.expect("a retained tree has its leader pidfd"));
        }
        if let Some(streams) = self.streams.take() {
            for fd in streams.descriptors() {
                self.close(fd);
            }
        }
        if !self.wait_started {
            self.wait_started = true;
            self.submit(Op::Wait { pidfd: self.pidfd.expect("a child has its pidfd"), reap: false });
        }
        if !self.reading
            && let Some(fd) = self.stderr
        {
            self.reading = true;
            self.submit(Op::PipeRead { fd, buf: bytes::zeroed(4096) });
        }
        if !self.signalling
            && let Some((signal, to)) = self.signal.take()
        {
            self.signalling = true;
            self.submit(Op::Signal { pidfd: self.pidfd.expect("a running child has its pidfd"), signal, to });
        }
        if self.status.is_some()
            && !self.signalling
            && let Some(pidfd) = self.pidfd
        {
            if self.expectation == Expectation::Supervise {
                let tree = self.tree.as_mut().expect("a retained leader has a tree");
                if !self.tree_status.live_pids.is_empty() || !tree.unpopulated() {
                    return;
                }
            }
            if let Some(tree) = &mut self.tree {
                self.counts = Some(tree.finish(pidfd, self.expectation, true).unwrap_or_else(|why| crate::fail(&why)));
            }
            self.tree = None;
            self.pidfd = None;
            self.poll_at = None;
            self.close(pidfd);
        }
    }

    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }

    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }

    fn work_pending(&self, now: Time) -> bool {
        self.poll_at.is_some_and(|at| at <= now)
            || !self.completions.is_empty()
            || !self.wait_started
            || self.streams.is_some()
            || (self.signal.is_some() && !self.signalling)
            || self.tree_signal.is_some()
            || (self.stderr.is_some() && !self.reading)
            || (self.expectation != Expectation::Supervise
                && self.status.is_some()
                && self.pidfd.is_some()
                && !self.signalling)
    }

    fn next_deadline(&self) -> Option<Time> {
        self.poll_at
    }

    fn is_empty(&self) -> bool {
        self.pidfd.is_none()
            && self.stderr.is_none()
            && self.streams.is_none()
            && self.closing == 0
            && !self.signalling
            && self.signal.is_none()
            && self.tree_signal.is_none()
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }

    fn worst_case(&self) -> u64 {
        u64::try_from(self.error_limit)
            .expect("stderr cap fits u64")
            .checked_add(32 * 1024)
            .expect("observer memory fits u64")
    }

    fn operations(&self) -> u32 {
        8
    }
}
