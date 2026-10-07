//! Child programs and their parent-owned pipe and pid descriptors.
#![expect(
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm,
    reason = "descriptor indices and operation routes are checked before these internal lookups"
)]

use alloc::collections::VecDeque;
use alloc::format;
use alloc::vec::Vec;

use skein_io::kernel::{Done, Error, Exit, Fd, Op, Pipe, Signal, Way};
use skein_lib::Token;

use super::{Pid, Sim};
use crate::machine::{Ask, Call, Program, Reply, Ticket};
use crate::trace::Summary;

#[derive(Clone, Copy, Debug)]
pub(super) struct PipeEnd {
    pub child: u64,
    pub index: usize,
}

#[derive(Debug)]
pub(super) struct Child {
    pub program: Program,
    pub exit: Option<Exit>,
    pub waited: bool,
    pub wait: Option<Token>,
    pub pidfd_open: bool,
    pub pipes: Vec<ChildPipe>,
}

#[derive(Debug)]
pub(super) struct ChildPipe {
    pub spec: Pipe,
    pub open: bool,
    pub ended: bool,
    pub bytes: VecDeque<u8>,
    pub waiting: Option<Token>,
}

impl Sim {
    pub(super) fn check_process(&self, pid: Pid, kind: Summary) {
        let process = self.process(pid);
        let fd = kind.fd().expect("a process operation names a descriptor");
        match kind {
            Summary::Spawn { .. } => {
                if process.files.get(&fd).is_none_or(|file| {
                    !matches!(
                        file.how,
                        skein_io::kernel::OpenHow::Directory | skein_io::kernel::OpenHow::DirectoryNoFollow
                    )
                }) {
                    self.fail(pid, &format!("Spawn root {fd:?} is not an open directory"));
                }
            }
            Summary::Wait { .. } | Summary::Signal { .. } => {
                let Some(&child_id) = process.pidfds.get(&fd) else {
                    self.fail(pid, &format!("{kind:?} on {fd:?}, which is not a pidfd"));
                };
                let child = process.children.get(&child_id).expect("pidfd names a child");
                if matches!(kind, Summary::Wait { .. }) && (child.waited || child.wait.is_some()) {
                    self.fail(pid, &format!("a second Wait on {fd:?}"));
                }
            }
            Summary::ReadSignal { .. } => {
                let Some(source) = process.signal_fds.get(&fd) else {
                    self.fail(pid, &format!("{kind:?} on {fd:?}, which is not a signalfd"));
                };
                if source.read.is_some() {
                    self.fail(pid, &format!("a second signal read on {fd:?}"));
                }
            }
            Summary::PipeRead { .. } | Summary::PipeWrite { .. } | Summary::Close { .. } => {
                if let Some(end) = process.pipe_fds.get(&fd) {
                    let pipe = &process.children[&end.child].pipes[end.index];
                    let valid = match kind {
                        Summary::PipeRead { .. } => pipe.spec.way == Way::Out,
                        Summary::PipeWrite { .. } => pipe.spec.way == Way::In,
                        Summary::Close { .. } => true,
                        _ => false,
                    };
                    if !valid {
                        self.fail(pid, &format!("{kind:?} has the wrong direction on pipe {fd:?}"));
                    }
                    for flight in process.flights.values() {
                        if flight.kind.fd() == Some(fd) {
                            self.fail(pid, &format!("{kind:?} beside {:?} on {fd:?}", flight.kind));
                        }
                    }
                } else if matches!(kind, Summary::Close { .. })
                    && (process.pidfds.contains_key(&fd) || process.signal_fds.contains_key(&fd))
                {
                    for flight in process.flights.values() {
                        if flight.kind.fd() == Some(fd) {
                            self.fail(pid, &format!("a Close beside {:?} on {fd:?}", flight.kind));
                        }
                    }
                } else {
                    self.fail(pid, &format!("{kind:?} on {fd:?}, which is not a pipe"));
                }
            }
            _ => self.bug("only process operations are checked here"),
        }
    }

    pub(super) fn process_op(&mut self, pid: Pid, token: Token, serial: u64, op: Op) {
        match op {
            Op::Spawn { spawn } => {
                let wanted = 1_u32.saturating_add(u32::try_from(spawn.pipes.len()).unwrap_or(u32::MAX));
                let open = self
                    .open_fds(pid)
                    .saturating_add(self.process(pid).opening)
                    .saturating_add(self.process(pid).spawning);
                if open.saturating_add(wanted) > self.config.max_fds {
                    self.complete(pid, token, Op::Spawn { spawn }, Err(Error::TooManyOpenFiles));
                    return;
                }
                let root = self.process(pid).files[&spawn.root].handle;
                let ask = Ask::Spawn {
                    root,
                    program: spawn.program.clone(),
                    args: spawn.args.clone(),
                    env: spawn.env.clone(),
                    dir: spawn.dir.clone(),
                    pipes: spawn.pipes.clone(),
                };
                let ticket = Ticket(serial);
                self.spawn_asked.insert(ticket, (pid, token));
                self.process_mut(pid).spawning = self.process(pid).spawning.saturating_add(wanted);
                self.park(pid, token, Op::Spawn { spawn });
                self.calls.push_back(Call { ticket, ask });
            }
            Op::Wait { pidfd } => {
                let child_id = self.process(pid).pidfds[&pidfd];
                let child = self.process_mut(pid).children.get_mut(&child_id).expect("pidfd names child");
                if let Some(exit) = child.exit {
                    child.waited = true;
                    self.complete(pid, token, Op::Wait { pidfd }, Ok(Done::Exit(exit)));
                } else {
                    child.wait = Some(token);
                    self.park(pid, token, Op::Wait { pidfd });
                }
            }
            Op::Signal { pidfd, signal } => {
                let child_id = self.process(pid).pidfds[&pidfd];
                if self.process(pid).children[&child_id].exit.is_none() {
                    let number = match signal {
                        Signal::Terminate => 15,
                        Signal::Kill => 9,
                    };
                    self.exit_child(pid, child_id, Exit::Signal(number));
                }
                self.complete(pid, token, Op::Signal { pidfd, signal }, Ok(Done::Nothing));
            }
            Op::ReadSignal { fd } => {
                let source = self.process_mut(pid).signal_fds.get_mut(&fd).expect("checked signalfd");
                if let Some(signal) = source.pending.pop_front() {
                    self.complete(pid, token, Op::ReadSignal { fd }, Ok(Done::ServiceSignal(signal)));
                } else {
                    source.read = Some(token);
                    self.park(pid, token, Op::ReadSignal { fd });
                }
            }
            Op::PipeRead { fd, .. } => self.recv_pipe(pid, token, op, fd),
            Op::PipeWrite { fd, .. } => self.send_pipe(pid, token, op, fd),
            Op::Close { fd } => {
                if let Some(end) = self.process_mut(pid).pipe_fds.remove(&fd) {
                    let pipe = &mut self.process_mut(pid).children.get_mut(&end.child).expect("pipe names child").pipes
                        [end.index];
                    pipe.open = false;
                    pipe.bytes.clear();
                    if pipe.spec.way == Way::In {
                        self.end_input(pid, end);
                    }
                } else if self.process_mut(pid).signal_fds.remove(&fd).is_some() {
                    // All reads settled before closing this source.
                } else {
                    let child_id = self.process_mut(pid).pidfds.remove(&fd).expect("a pidfd");
                    self.process_mut(pid).children.get_mut(&child_id).expect("pidfd names child").pidfd_open = false;
                }
                self.complete(pid, token, Op::Close { fd }, Ok(Done::Nothing));
            }
            _ => self.bug("a non-process operation was routed here"),
        }
    }

    pub(super) fn answered_spawn(&mut self, ticket: Ticket, result: Result<Reply, Error>) {
        let (pid, token) = self.spawn_asked.remove(&ticket).expect("a spawn call waiting");
        let op = self.unpark(pid, token);
        let Op::Spawn { ref spawn } = op else { self.bug("a spawn call holds Spawn") };
        let reserved = u32::try_from(spawn.pipes.len()).unwrap_or(u32::MAX).saturating_add(1);
        self.process_mut(pid).spawning = self.process(pid).spawning.checked_sub(reserved).expect("reserved spawn fds");
        match result {
            Ok(Reply::Program(program)) => {
                let specs = spawn.pipes.clone();
                let child_id = self.next_child;
                self.next_child = child_id.checked_add(1).expect("fewer than 2^64 children");
                let pipes = specs
                    .iter()
                    .map(|&spec| ChildPipe { spec, open: true, ended: false, bytes: VecDeque::new(), waiting: None })
                    .collect();
                let exit = match program {
                    Program::Exit(code) => Some(Exit::Code(code)),
                    _ => None,
                };
                self.process_mut(pid)
                    .children
                    .insert(child_id, Child { program, exit, waited: false, wait: None, pidfd_open: true, pipes });
                let pidfd = self.new_process_fd(pid);
                self.process_mut(pid).pidfds.insert(pidfd, child_id);
                let mut parent = Vec::with_capacity(specs.len());
                for index in 0..specs.len() {
                    let fd = self.new_process_fd(pid);
                    self.process_mut(pid).pipe_fds.insert(fd, PipeEnd { child: child_id, index });
                    parent.push(fd);
                }
                self.complete(pid, token, op, Ok(Done::Spawned { pidfd, pipes: parent.into_boxed_slice() }));
            }
            Ok(reply) => self.machine(&format!("{reply:?} to a Spawn")),
            Err(error) => self.complete(pid, token, op, Err(error)),
        }
    }

    fn new_process_fd(&mut self, pid: Pid) -> Fd {
        let process = self.process_mut(pid);
        let fd = Fd::new(process.next_fd);
        process.next_fd = process.next_fd.checked_add(1).expect("fewer than 2^31 descriptors");
        fd
    }

    fn recv_pipe(&mut self, pid: Pid, token: Token, mut op: Op, fd: Fd) {
        let end = self.process(pid).pipe_fds[&fd];
        let child = self.process_mut(pid).children.get_mut(&end.child).expect("pipe names child");
        let pipe = &mut child.pipes[end.index];
        let Op::PipeRead { ref mut buf, .. } = op else { self.bug("a Recv holds a buffer") };
        if pipe.bytes.is_empty() && !pipe.ended && child.exit.is_none() {
            pipe.waiting = Some(token);
            self.park(pid, token, op);
            return;
        }
        let n = buf.len().min(pipe.bytes.len());
        for slot in buf.iter_mut().take(n) {
            *slot = pipe.bytes.pop_front().expect("within buffered bytes");
        }
        self.complete(pid, token, op, Ok(Done::Count(u32::try_from(n).expect("a count"))));
        self.wake_pipe(pid, end.child);
    }

    fn send_pipe(&mut self, pid: Pid, token: Token, op: Op, fd: Fd) {
        let end = self.process(pid).pipe_fds[&fd];
        let (program, exit, ended) = {
            let child = &self.process(pid).children[&end.child];
            (child.program, child.exit, child.pipes[end.index].ended)
        };
        if exit.is_some() || ended {
            self.complete(pid, token, op, Err(Error::BrokenPipe));
            return;
        }
        let Op::PipeWrite { ref bytes, from, .. } = op else { self.bug("Send has bytes") };
        let left = &bytes[usize::try_from(from).expect("from fits")..];
        let (n, output) = match program {
            Program::Echo { input, output }
                if self.process(pid).children[&end.child].pipes[end.index].spec.child == input =>
            {
                let child = &self.process(pid).children[&end.child];
                let Some(index) = child.pipes.iter().position(|pipe| pipe.spec.child == output) else {
                    self.bug("echo output descriptor exists")
                };
                let target = &child.pipes[index];
                if target.open {
                    let room =
                        usize::try_from(self.config.buffer).expect("buffer fits").saturating_sub(target.bytes.len());
                    (left.len().min(room), Some(index))
                } else {
                    (left.len().min(usize::try_from(self.config.buffer).expect("buffer fits")), None)
                }
            }
            _ => (left.len().min(usize::try_from(self.config.buffer).expect("buffer fits")), None),
        };
        if n == 0 {
            self.process_mut(pid).children.get_mut(&end.child).expect("child").pipes[end.index].waiting = Some(token);
            self.park(pid, token, op);
            return;
        }
        if let Some(index) = output {
            self.process_mut(pid).children.get_mut(&end.child).expect("child").pipes[index]
                .bytes
                .extend(left[..n].iter().copied());
        }
        self.complete(pid, token, op, Ok(Done::Count(u32::try_from(n).expect("a count"))));
        self.wake_pipe(pid, end.child);
    }

    fn end_input(&mut self, pid: Pid, end: PipeEnd) {
        let child = self.process_mut(pid).children.get_mut(&end.child).expect("child");
        if child.pipes[end.index].ended {
            return;
        }
        child.pipes[end.index].ended = true;
        if let Program::Echo { input, .. } = child.program
            && child.pipes[end.index].spec.child == input
        {
            self.exit_child(pid, end.child, Exit::Code(0));
        }
    }

    fn exit_child(&mut self, pid: Pid, child_id: u64, exit: Exit) {
        let child = self.process_mut(pid).children.get_mut(&child_id).expect("child");
        child.exit = Some(exit);
        for pipe in &mut child.pipes {
            pipe.ended = true;
        }
        if let Some(token) = child.wait.take() {
            child.waited = true;
            let op = self.unpark(pid, token);
            self.complete(pid, token, op, Ok(Done::Exit(exit)));
        }
        self.wake_pipe(pid, child_id);
    }

    fn wake_pipe(&mut self, pid: Pid, child_id: u64) {
        let pending: Vec<(usize, Token)> = self.process(pid).children[&child_id]
            .pipes
            .iter()
            .enumerate()
            .filter_map(|(index, pipe)| pipe.waiting.map(|token| (index, token)))
            .collect();
        for (index, token) in pending {
            let (spec, ended, available, exit) = {
                let child = &self.process(pid).children[&child_id];
                let pipe = &child.pipes[index];
                // Completing an earlier waiter can reenter wake_pipe and
                // consume this waiter before the outer pass reaches it.
                if pipe.waiting != Some(token) {
                    continue;
                }
                (pipe.spec, pipe.ended, !pipe.bytes.is_empty(), child.exit.is_some())
            };
            let ready = match spec.way {
                Way::Out => available || ended || exit,
                Way::In => ended || exit || self.echo_room(pid, child_id, index),
            };
            if ready {
                self.process_mut(pid).children.get_mut(&child_id).expect("child").pipes[index].waiting = None;
                let op = self.unpark(pid, token);
                let fd = self
                    .process(pid)
                    .pipe_fds
                    .iter()
                    .find_map(|(&fd, end)| (end.child == child_id && end.index == index).then_some(fd))
                    .expect("waiting pipe is open");
                match op {
                    Op::PipeRead { .. } => self.recv_pipe(pid, token, op, fd),
                    Op::PipeWrite { .. } => self.send_pipe(pid, token, op, fd),
                    _ => self.bug("a pipe waiter holds Recv or Send"),
                }
            }
        }
    }

    fn echo_room(&self, pid: Pid, child_id: u64, input_index: usize) -> bool {
        let child = &self.process(pid).children[&child_id];
        let Program::Echo { input, output } = child.program else {
            return true;
        };
        if child.pipes[input_index].spec.child != input {
            return true;
        }
        let Some(target) = child.pipes.iter().find(|pipe| pipe.spec.child == output) else {
            return true;
        };
        !target.open || target.bytes.len() < usize::try_from(self.config.buffer).expect("buffer fits")
    }
}

#[cfg(test)]
mod tests {
    use skein_io::kernel::{Done, Error, Exit, Op, Pipe, ServiceSignal, Signal, Spawn, Submit, Way};
    use skein_lib::{Queue, Token};

    use crate::{Answer, Ask, Config, Handle, Program, Reply, Sim};

    #[test]
    fn signal_source_delivers_and_cancels_a_pending_read() {
        let mut sim = Sim::new(2, Config::calm());
        let pid = sim.spawn_process();
        let fd = sim.open_signal_source(pid);
        let mut submits = Queue::with_capacity(2);
        submits.push(Submit { op: Token::new(1), kind: Op::ReadSignal { fd } });
        sim.submit(pid, &mut submits);
        sim.deliver_service_signal(pid, fd, ServiceSignal::Interrupt);
        let mut completes = Queue::with_capacity(3);
        sim.reap(pid, &mut completes);
        assert_eq!(completes.pop().expect("signal read").result, Ok(Done::ServiceSignal(ServiceSignal::Interrupt)));

        submits.push(Submit { op: Token::new(2), kind: Op::ReadSignal { fd } });
        sim.submit(pid, &mut submits);
        submits.push(Submit { op: Token::new(3), kind: Op::Cancel { target: Token::new(2) } });
        sim.submit(pid, &mut submits);
        sim.reap(pid, &mut completes);
        let mut results = [None, None];
        while let Some(complete) = completes.pop() {
            match complete.op.raw() {
                2 => results[0] = Some(complete.result),
                3 => results[1] = Some(complete.result),
                _ => unreachable!("only the pending read and cancel complete"),
            }
        }
        assert_eq!(results[0], Some(Err(Error::Cancelled)));
        assert_eq!(results[1], Some(Ok(Done::Nothing)));

        submits.push(Submit { op: Token::new(4), kind: Op::Close { fd } });
        sim.submit(pid, &mut submits);
        sim.reap(pid, &mut completes);
        assert_eq!(completes.pop().expect("closed signal source").result, Ok(Done::Nothing));
        sim.assert_no_open_fds(pid);
    }

    #[test]
    fn terminating_a_child_wakes_each_waiting_output_pipe_once() {
        let mut sim = Sim::new(1, Config::calm());
        let pid = sim.spawn_process();
        let root = sim.root(pid, Handle::new(1));
        let mut submits = Queue::with_capacity(4);
        submits.push(Submit {
            op: Token::new(0),
            kind: Op::Spawn {
                spawn: Box::new(Spawn {
                    program: b"never".to_vec().into_boxed_slice(),
                    args: Box::new([]),
                    env: Box::new([]),
                    root,
                    dir: Box::new([]),
                    pipes: Box::new([Pipe { child: 1, way: Way::Out }, Pipe { child: 2, way: Way::Out }]),
                }),
            },
        });
        sim.submit(pid, &mut submits);

        let mut calls = Queue::with_capacity(1);
        sim.calls(&mut calls);
        let call = calls.pop().expect("the spawn call");
        assert!(matches!(call.ask, Ask::Spawn { .. }));
        let mut answers = Queue::with_capacity(1);
        answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(Program::Never)) });
        sim.answer(&mut answers);

        let mut completes = Queue::with_capacity(4);
        sim.reap(pid, &mut completes);
        let spawned = completes.pop().expect("the spawn completion");
        let Ok(Done::Spawned { pidfd, pipes }) = spawned.result else {
            unreachable!("the child and its pipes are created")
        };
        assert_eq!(pipes.len(), 2);

        submits.push(Submit { op: Token::new(1), kind: Op::Wait { pidfd } });
        submits.push(Submit { op: Token::new(2), kind: Op::PipeRead { fd: pipes[0], buf: Box::new([0; 1]) } });
        submits.push(Submit { op: Token::new(3), kind: Op::PipeRead { fd: pipes[1], buf: Box::new([0; 1]) } });
        submits.push(Submit { op: Token::new(4), kind: Op::Signal { pidfd, signal: Signal::Terminate } });
        sim.submit(pid, &mut submits);
        sim.reap(pid, &mut completes);

        assert_eq!(completes.len(), 4);
        let mut seen = [false; 4];
        while let Some(complete) = completes.pop() {
            let index = usize::try_from(complete.op.raw() - 1).expect("the submitted token");
            assert!(!seen[index], "one completion per token");
            seen[index] = true;
            match complete.op.raw() {
                1 => assert_eq!(complete.result, Ok(Done::Exit(Exit::Signal(15)))),
                2 | 3 => assert_eq!(complete.result, Ok(Done::Count(0))),
                4 => assert_eq!(complete.result, Ok(Done::Nothing)),
                _ => unreachable!("only the four expected completions"),
            }
        }
        assert!(seen.into_iter().all(|completed| completed));
        assert_eq!(sim.in_flight(pid), 0);
    }
}
