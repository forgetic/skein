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
        &[Pipe { child: 7, way: Way::In }, Pipe { child: 9, way: Way::Out }],
    );
    let (echo_pidfd, pipes) = match run.call(process, Op::Spawn { spawn: Box::new(spawn) }).result {
        Ok(Done::Spawned { pidfd, pipes }) => (pidfd, pipes),
        other => unexpected("an echo fixture spawned", &other),
    };
    let [input, output] = pipes.as_ref() else { unexpected("one parent descriptor per requested pipe", &pipes) };
    pipe_write_all(&mut run, process, *input, MESSAGE);
    let echoed = pipe_read_exact(&mut run, process, *output, MESSAGE.len());
    run.close(process, *input);
    let echo_exit = run.call(process, Op::Wait { pidfd: echo_pidfd }).result;
    let eof = pipe_read(&mut run, process, *output);
    run.close(process, *output);
    run.close(process, echo_pidfd);

    let status = command(executable, root, &[b"exit", b"42"], &[]);
    let (status_pidfd, status_pipes) = match run.call(process, Op::Spawn { spawn: Box::new(status) }).result {
        Ok(Done::Spawned { pidfd, pipes }) => (pidfd, pipes),
        other => unexpected("an exit fixture spawned", &other),
    };
    assert!(status_pipes.is_empty(), "no pipes were requested of the exit fixture");
    let status_exit = run.call(process, Op::Wait { pidfd: status_pidfd }).result;
    run.close(process, status_pidfd);

    let never = command(executable, root, &[b"never"], &[]);
    let (never_pidfd, never_pipes) = match run.call(process, Op::Spawn { spawn: Box::new(never) }).result {
        Ok(Done::Spawned { pidfd, pipes }) => (pidfd, pipes),
        other => unexpected("a never fixture spawned", &other),
    };
    assert!(never_pipes.is_empty(), "no pipes were requested of the never fixture");
    let waiting = run.start(process, Op::Wait { pidfd: never_pidfd });
    let never_waited = run.within(process, waiting, BRIEFLY).is_none();
    assert!(never_waited, "the never fixture stays alive until signalled");
    let signalled = run.call(process, Op::Signal { pidfd: never_pidfd, signal: Signal::Kill }).result;
    let killed_exit = run.wait(process, waiting).result;
    run.close(process, never_pidfd);
    run.close(process, root);
    run.finish();
    Processes { echoed, eof, echo_exit, status_exit, never_waited, signalled, killed_exit }
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
