//! Shipped binaries beside the scripted processes of a real world
//! (testing-strategy.md, section 2.9; examples.md, section 6). This module
//! keeps the child's pidfd, captured stderr and pending kernel operations;
//! it never knows the service's state. `Binary::start` opens its pipes or
//! controlling terminal, `take_streams` transfers the scripted ends for
//! adoption, and the `Host` entry points wait, signal, read and close on
//! the world's shared ring. The referee observes stderr and `exit_status`.

use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use skein_io::kernel::{Complete, Done, Error, Exit, Fd, Op, Pipe, Signal, Spawn, Submit, Way};
use skein_lib::{Queue, Time, Token, Wall, bytes};
use skein_shell::{Config, Kernel, OpenError, Wait};

use crate::Host;

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
    signal: Option<Signal>,
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
        let mut startup = Kernel::open(Config { operations: 1 }).map_err(StartError::Ring)?;
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
        let child = skein_shell::start_binary(&mut spawn, terminal);
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
        self.signal = Some(signal);
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

impl Host for Binary {
    fn iterate(&mut self, _now: Time, _wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            self.completed(complete);
        }
        if let Some(streams) = self.streams.take() {
            for fd in streams.descriptors() {
                self.close(fd);
            }
        }
        if !self.wait_started {
            self.wait_started = true;
            self.submit(Op::Wait { pidfd: self.pidfd.expect("a child has its pidfd") });
        }
        if !self.reading
            && let Some(fd) = self.stderr
        {
            self.reading = true;
            self.submit(Op::PipeRead { fd, buf: bytes::zeroed(4096) });
        }
        if !self.signalling
            && let Some(signal) = self.signal.take()
        {
            self.signalling = true;
            self.submit(Op::Signal { pidfd: self.pidfd.expect("a running child has its pidfd"), signal });
        }
        if self.status.is_some()
            && !self.signalling
            && let Some(pidfd) = self.pidfd.take()
        {
            self.close(pidfd);
        }
    }

    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }

    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }

    fn work_pending(&self, _now: Time) -> bool {
        !self.completions.is_empty()
            || !self.wait_started
            || self.streams.is_some()
            || (self.signal.is_some() && !self.signalling)
            || (self.stderr.is_some() && !self.reading)
            || (self.status.is_some() && self.pidfd.is_some() && !self.signalling)
    }

    fn next_deadline(&self) -> Option<Time> {
        None
    }

    fn is_empty(&self) -> bool {
        self.pidfd.is_none()
            && self.stderr.is_none()
            && self.streams.is_none()
            && self.closing == 0
            && !self.signalling
            && self.signal.is_none()
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
