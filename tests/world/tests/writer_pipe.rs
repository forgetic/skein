//! Writer diagnostics are observed on the same hosted stderr pipe in both backends.

use std::io::Write;

use skein_heap::Counting;
use skein_io::kernel::{Complete, Exit, Spawn, Submit};
use skein_lib::{Duration, Queue, Time, Wall};
use skein_sim::{Config, Faults, Handle};
use skein_world::{Host, HostedProgram, Inherited, Memory, PipeHost, PipeWriter, World, real};
use skein_world_tests::hosted::{Act, Judge, RootMachine, Script};

#[global_allocator]
static HEAP: Counting = Counting;

#[derive(Debug)]
struct Diagnostic {
    script: Script,
    writer: PipeWriter,
    said: bool,
}

impl Host for Diagnostic {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.script.iterate(now, wall);
    }
    fn drain(&mut self) {
        self.script.drain();
        if !self.said && !self.script.received.is_empty() {
            writeln!(self.writer, "terminal").expect("bounded terminal diagnostic");
            self.said = true;
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        self.script.completions()
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        self.script.submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.script.work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.script.next_deadline()
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        self.script.next_policy_deadline()
    }

    fn is_empty(&self) -> bool {
        self.script.is_empty()
    }
    fn exit(&self) -> Option<Exit> {
        self.script.exit()
    }
    fn worst_case(&self) -> u64 {
        self.script.worst_case()
    }
    fn operations(&self) -> u32 {
        self.script.operations()
    }
}

#[derive(Debug)]
enum Process {
    Parent(Box<Script>),
    Child(Box<PipeHost<Diagnostic>>),
}

impl Process {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Parent(host) => host.as_ref(),
            Self::Child(host) => host.as_ref(),
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Parent(host) => host.as_mut(),
            Self::Child(host) => host.as_mut(),
        }
    }
    fn parent(&self) -> &Script {
        match self {
            Self::Parent(host) => host,
            Self::Child(_) => panic!("parent"),
        }
    }
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.host_mut().iterate(now, wall);
    }
    fn drain(&mut self) {
        self.host_mut().drain();
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        self.host_mut().completions()
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        self.host_mut().submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.host().work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.host().next_deadline()
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        self.host().next_policy_deadline()
    }

    fn is_empty(&self) -> bool {
        self.host().is_empty()
    }
    fn exit(&self) -> Option<Exit> {
        self.host().exit()
    }
    fn worst_case(&self) -> u64 {
        self.host().worst_case()
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}

fn child(_spawn: &Spawn, inherited: &Inherited) -> Process {
    let stderr = inherited.pipes.iter().find(|(child, _)| *child == 2).expect("stderr pipe").1;
    let (mut writer, capture) = PipeWriter::new(stderr, 64);
    writeln!(writer, "startup").expect("startup before the first iteration");
    let host =
        Diagnostic { script: Script::child(inherited, &[Act::Read(0), Act::Write(1, b"R")]), writer, said: false };
    Process::Child(Box::new(capture.host(host)))
}

fn acts() -> Vec<Act> {
    let mut acts = vec![Act::SpawnStderr, Act::Write(0, b"hello"), Act::Read(1)];
    acts.extend([Act::Read(2); 20]);
    acts.push(Act::Read(1));
    acts.push(Act::Wait);
    acts
}

fn program() -> HostedProgram<Process> {
    HostedProgram { program: Box::from(&b"hosted"[..]), make: child, instances: 1, operations: 9 }
}

fn simulated(seed: u64) -> skein_world::Outcome<Process, RootMachine> {
    let faults = Faults { short_write: 1000, latency: 1000, latency_max: Duration::from_millis(1), ..Faults::NONE };
    let mut world = World::new(seed, Config { buffer: 4, faults, ..Config::calm() }, Judge, Memory::Checked)
        .with_machine(RootMachine);
    world.host(program());
    world.spawn_root(Handle::new(1), |root| Process::Parent(Box::new(Script::parent(root, &acts()))));
    world.run()
}

impl skein_world::Referee<Process> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Process]) {}
    fn observe(&mut self, _now: Time, _procs: &[Process]) {}
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        true
    }
}

#[test]
fn hosted_writer_diagnostics_reach_stderr_and_its_end_before_the_child_exit() {
    for seed in 0..8 {
        let first = simulated(seed);
        let replay = simulated(seed);
        assert_eq!(first.procs[0].parent().received, b"Rstartup\nterminal\n");
        assert_eq!(first.procs[0].parent().child_exit, Some(Exit::Code(0)));
        assert_eq!(first.trace, replay.trace, "short writer transfers replay");
    }
    let mut world = real::World::new_checked(Judge);
    world.host(program());
    let root = skein_shell::open_root(std::path::Path::new(".")).expect("startup root");
    world.spawn_with_fds(vec![root], || Process::Parent(Box::new(Script::parent(root, &acts()))));
    let outcome = world.run(&skein_shell::Clock::new(), Duration::from_secs(1));
    assert_eq!(outcome.procs[0].parent().received, b"Rstartup\nterminal\n");
    assert_eq!(outcome.procs[0].parent().child_exit, Some(Exit::Code(0)));
}

#[test]
fn a_writer_refuses_a_full_capture_and_writes_after_its_pipe_close() {
    let (mut writer, capture) = PipeWriter::new(skein_io::kernel::Fd::new(50), 4);
    assert_eq!(writer.write(b"abcdef").expect("partial capture"), 4);
    assert_eq!(writer.write(b"x").expect_err("bounded capture").kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(writer.flush().expect_err("not yet submitted").kind(), std::io::ErrorKind::WouldBlock);
    let mut host = capture.host(Script::new(&[]));
    host.iterate(Time::ZERO, Wall::from_nanos(0));
    host.drain();
    let write = host.submissions().pop().expect("captured pipe write");
    host.completions().push(Complete {
        op: write.op,
        kind: write.kind,
        result: Err(skein_io::kernel::Error::BrokenPipe),
    });
    host.iterate(Time::ZERO, Wall::from_nanos(0));
    host.drain();
    assert_eq!(writer.write(b"x").expect_err("closed writer").kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(host.failure(), Some(skein_io::kernel::Error::BrokenPipe));
    let close = host.submissions().pop().expect("failed pipe still closes");
    host.completions().push(Complete { op: close.op, kind: close.kind, result: Ok(skein_io::kernel::Done::Nothing) });
    host.iterate(Time::ZERO, Wall::from_nanos(0));
    host.drain();
    assert!(host.is_empty());
    writer.flush().expect("no captured bytes remain");
}
