//! Process and pipe records, exercised identically by the simulator and ring.

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;

use skein_io::kernel::{Done, Error, Exit, Fd, Op, Pipe, Signal, Spawn, Way};

use crate::run::{BRIEFLY, Run, unexpected, usize_of};
use crate::{Backend, Check};

const MESSAGE: &[u8] = b"skein pipe echo";

#[derive(Debug)]
pub struct Processes {
    echoed: Vec<u8>,
    eof: Vec<u8>,
    echo_exit: Result<Done, Error>,
    status_exit: Result<Done, Error>,
    never_waited: bool,
    signalled: Result<Done, Error>,
    killed_exit: Result<Done, Error>,
}

impl Check for Processes {
    fn check(&self) {
        assert_eq!(self.echoed, MESSAGE, "the child echoes bytes through chosen descriptors");
        assert!(self.eof.is_empty(), "a pipe reaches EOF after the child exits");
        assert_eq!(self.echo_exit, Ok(Done::Exit(Exit::Code(0))), "echo exits successfully");
        assert_eq!(self.status_exit, Ok(Done::Exit(Exit::Code(42))), "a child's exit status is preserved");
        assert!(self.never_waited, "Wait stays pending while the child is alive");
        assert_eq!(self.signalled, Ok(Done::Nothing), "a signal is delivered by pidfd");
        assert_eq!(self.killed_exit, Ok(Done::Exit(Exit::Signal(9))), "a killed child reports its signal");
    }
}

/// `executable` is a fixture program that accepts `echo INPUT OUTPUT`,
/// `exit CODE`, and `never`. The simulator's machine models the same fixture.
#[must_use]
pub fn processes<B: Backend>(backend: &mut B, executable: &[u8]) -> Processes {
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &[]);

    let spawn = command(
        executable,
        root,
        &[b"echo", b"7", b"9"],
        &[Pipe { child: 7, way: Way::In, parent: None }, Pipe { child: 9, way: Way::Out, parent: None }],
    );
    let (echo_pidfd, pipes) = spawned(run.call(process, Op::Spawn { spawn: Box::new(spawn) }));
    let [input, output] = pipes.as_slice() else { unexpected("one parent descriptor per requested pipe", &pipes) };
    pipe_write_all(&mut run, process, *input, MESSAGE);
    let echoed = pipe_read_exact(&mut run, process, *output, MESSAGE.len());
    run.close(process, *input);
    let echo_exit = run.call(process, Op::Wait { pidfd: echo_pidfd, reap: false }).result;
    let eof = pipe_read(&mut run, process, *output);
    run.close(process, *output);
    let reaped = run.call(process, Op::Wait { pidfd: echo_pidfd, reap: true });
    assert!(matches!(reaped.result, Ok(Done::Exit(_))), "an observed child reaps at once");
    run.close(process, echo_pidfd);

    let mut missing = command(executable, root, &[b"exit", b"42"], &[Pipe { child: 1, way: Way::Out, parent: None }]);
    missing.dir = Box::from(&b"missing-spawn-directory"[..]);
    let failed = run.call(process, Op::Spawn { spawn: Box::new(missing) });
    assert_eq!(
        failed.result,
        Err(Error::NotFound),
        "a failed Spawn makes no usable descriptors (kernel.md, sections 3 and 4)"
    );

    let status = command(executable, root, &[b"exit", b"42"], &[]);
    let (status_pidfd, status_pipes) = spawned(run.call(process, Op::Spawn { spawn: Box::new(status) }));
    assert!(status_pipes.is_empty(), "no pipes were requested of the exit fixture");
    let status_exit = run.call(process, Op::Wait { pidfd: status_pidfd, reap: false }).result;
    let reaped = run.call(process, Op::Wait { pidfd: status_pidfd, reap: true });
    assert!(matches!(reaped.result, Ok(Done::Exit(_))), "an observed child reaps at once");
    run.close(process, status_pidfd);

    let never = command(executable, root, &[b"never"], &[]);
    let (never_pidfd, never_pipes) = spawned(run.call(process, Op::Spawn { spawn: Box::new(never) }));
    assert!(never_pipes.is_empty(), "no pipes were requested of the never fixture");
    let waiting = run.start(process, Op::Wait { pidfd: never_pidfd, reap: false });
    let never_waited = run.within(process, waiting, BRIEFLY).is_none();
    assert!(never_waited, "the never fixture stays alive until signalled");
    let signalled = run
        .call(process, Op::Signal { pidfd: never_pidfd, signal: Signal::Kill, to: skein_io::kernel::Target::Child })
        .result;
    let killed_exit = run.wait(process, waiting).result;
    let reaped = run.call(process, Op::Wait { pidfd: never_pidfd, reap: true });
    assert!(matches!(reaped.result, Ok(Done::Exit(_))), "an observed child reaps at once");
    run.close(process, never_pidfd);
    run.close(process, root);
    run.finish();
    Processes { echoed, eof, echo_exit, status_exit, never_waited, signalled, killed_exit }
}

fn spawned(complete: skein_io::kernel::Complete) -> (Fd, Vec<Fd>) {
    match (complete.kind, complete.result) {
        (Op::Spawn { spawn }, Ok(Done::Spawned { pidfd })) => {
            (pidfd, spawn.pipes.iter().map(|pipe| pipe.parent.expect("parent pipe slot filled")).collect())
        }
        (_, other) => unexpected("a fixture spawned", &other),
    }
}

fn command(executable: &[u8], root: Fd, args: &[&[u8]], pipes: &[Pipe]) -> Spawn {
    Spawn {
        program: Box::from(executable),
        args: args.iter().map(|arg| Box::from(*arg)).collect(),
        env: Box::new([]),
        root,
        dir: Box::from(&b"."[..]),
        pipes: Box::from(pipes),
    }
}

fn pipe_write_all<B: Backend>(run: &mut Run<'_, B>, process: B::Process, fd: Fd, bytes: &[u8]) {
    let mut from = 0_u32;
    while usize_of(from) < bytes.len() {
        let op = Op::PipeWrite { fd, bytes: Box::from(bytes), from };
        match run.call(process, op).result {
            Ok(Done::Count(n)) => from = from.checked_add(n).expect("no more than the bytes given"),
            other => unexpected("a pipe write accepts at least one byte", &other),
        }
    }
}

fn pipe_read<B: Backend>(run: &mut Run<'_, B>, process: B::Process, fd: Fd) -> Vec<u8> {
    let complete = run.call(process, Op::PipeRead { fd, buf: vec![0; 4096].into_boxed_slice() });
    match (complete.kind, complete.result) {
        (Op::PipeRead { buf, .. }, Ok(Done::Count(n))) => {
            buf.get(..usize_of(n)).expect("a valid pipe read count").to_vec()
        }
        (_, other) => unexpected("a pipe read returns bytes or EOF", &other),
    }
}

fn pipe_read_exact<B: Backend>(run: &mut Run<'_, B>, process: B::Process, fd: Fd, len: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    while bytes.len() < len {
        let next = pipe_read(run, process, fd);
        assert!(!next.is_empty(), "the echo produces every byte before EOF");
        bytes.extend(next);
    }
    bytes
}

/// Observes the exited group leader, then ends the descendant's hold on stdout.
pub fn groups<B: Backend>(backend: &mut B, executable: &[u8]) -> Groups {
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &[]);
    let command = command(executable, root, &[b"fork-exit"], &[Pipe { child: 1, way: Way::Out, parent: None }]);
    let (pidfd, pipes) = spawned(run.call(process, Op::Spawn { spawn: Box::new(command) }));
    let observed = run.call(process, Op::Wait { pidfd, reap: false });
    assert_eq!(observed.result, Ok(Done::Exit(Exit::Code(0))), "the group leader exited");
    let output = *pipes.first().expect("the requested output pipe");
    let read = run.start(process, Op::PipeRead { fd: output, buf: Box::from([0_u8; 1]) });
    assert!(run.within(process, read, BRIEFLY).is_none(), "the descendant holds its leader's pipe after exit");
    let signalled = run.call(process, Op::Signal { pidfd, signal: Signal::Kill, to: skein_io::kernel::Target::Group });
    assert_eq!(signalled.result, Ok(Done::Nothing), "the retained leader names its group");
    assert_eq!(run.wait(process, read).result, Ok(Done::Count(0)), "group kill ends every writer");
    assert_eq!(
        run.call(process, Op::Wait { pidfd, reap: true }).result,
        observed.result,
        "reaping preserves the observed exit"
    );
    run.close(process, output);
    run.close(process, pidfd);
    run.close(process, root);
    run.finish();
    Groups { observed: observed.result, signalled: signalled.result }
}

/// Checks usage before observation, after observation and after reaping one child.
pub fn usage<B: Backend>(backend: &mut B, executable: &[u8]) -> ResourcesCheck {
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &[]);
    let Done::Usage(before) = run.call(process, Op::Usage).result.expect("usage succeeds") else {
        unexpected("a Usage result", &"wrong shape")
    };
    assert_eq!(before.children, skein_io::kernel::Resources::ZERO, "no child was reaped in this process");
    let command = command(executable, root, &[b"exit", b"0"], &[]);
    let (pidfd, _) = spawned(run.call(process, Op::Spawn { spawn: Box::new(command) }));
    run.call(process, Op::Wait { pidfd, reap: false });
    let Done::Usage(observed) = run.call(process, Op::Usage).result.expect("usage succeeds") else {
        unexpected("a Usage result", &"wrong shape")
    };
    assert_eq!(observed.children, before.children, "a zombie has not joined the children's usage");
    run.call(process, Op::Wait { pidfd, reap: true });
    let Done::Usage(after) = run.call(process, Op::Usage).result.expect("usage succeeds") else {
        unexpected("a Usage result", &"wrong shape")
    };
    run.close(process, pidfd);
    run.close(process, root);
    run.finish();
    ResourcesCheck { before, after }
}

/// The process and its one reaped child, checked identically on both backends.
#[derive(Debug)]
pub struct ResourcesCheck {
    before: skein_io::kernel::Usage,
    after: skein_io::kernel::Usage,
}

impl Check for ResourcesCheck {
    fn check(&self) {
        assert!(self.after.children.peak_rss_bytes > 0, "the reaped child's peak is counted");
        assert!(self.after.own.user >= self.before.own.user, "own user CPU never falls");
        assert!(self.after.own.system >= self.before.own.system, "own system CPU never falls");
        assert!(self.after.own.peak_rss_bytes >= self.before.own.peak_rss_bytes, "own resident peak never falls");
    }
}

/// The retained leader's observed exit and its group signal result.
#[derive(Debug)]
pub struct Groups {
    observed: Result<Done, Error>,
    signalled: Result<Done, Error>,
}

impl Check for Groups {
    fn check(&self) {
        assert_eq!(self.observed, Ok(Done::Exit(Exit::Code(0))), "the group leader exits normally");
        assert_eq!(self.signalled, Ok(Done::Nothing), "the signal still reaches the retained leader's group");
    }
}
