//! Parent-side conformance and the shared real ring's ownership contracts.

use std::collections::BTreeMap;
use std::path::Path;

use skein_io::kernel::{Complete, Done, Error, Family, Fd, Op, Signal, Spawn, Submit};
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_shell::{Clock, open_root};
use skein_world::{Host, HostedProgram, Inherited, Referee, real};
use skein_world_tests::hosted::{Act, Script};

#[derive(Debug)]
struct Quiet;

impl<P> Referee<P> for Quiet {
    fn act(&mut self, _now: Time, _procs: &mut [P]) {}
    fn observe(&mut self, _now: Time, _procs: &[P]) {}
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

#[derive(Debug)]
struct RecordingParent {
    script: Script,
    descriptors: BTreeMap<Fd, u32>,
    records: Vec<String>,
}

impl RecordingParent {
    fn new(root: Fd) -> Self {
        Self {
            script: Script::parent(
                root,
                &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Close(0), Act::Read(0), Act::Wait],
            ),
            descriptors: BTreeMap::from([(root, 0)]),
            records: Vec::new(),
        }
    }

    fn record(&mut self, complete: &Complete) {
        if let (Op::Spawn { spawn }, Ok(Done::Spawned { pidfd })) = (&complete.kind, &complete.result) {
            self.descriptors.insert(*pidfd, 1);
            for (at, pipe) in spawn.pipes.iter().enumerate() {
                self.descriptors
                    .insert(pipe.parent.expect("successful parent end"), u32::try_from(at + 2).expect("small table"));
            }
        }
        // Fd numbers are kernel identities. Everything else, including the
        // original op token and returned buffers, is compared without alteration.
        let mut normalized = format!("{complete:?}");
        for (fd, logical) in &self.descriptors {
            normalized = normalized.replace(&format!("Fd({})", fd.raw()), &format!("Fd(logical{logical})"));
        }
        self.records.push(normalized);
    }
}

impl Host for RecordingParent {
    fn drain(&mut self) {
        self.script.drain();
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        let mut arrived = Vec::new();
        while let Some(complete) = self.script.completions().pop() {
            self.record(&complete);
            arrived.push(complete);
        }
        for complete in arrived {
            self.script.completions().push(complete);
        }
        self.script.iterate(now, wall);
        let count = self.script.submissions().len();
        for _ in 0..count {
            let mut record = self.script.submissions().pop().expect("counted record");
            if let Op::Spawn { spawn } = &mut record.kind {
                spawn.program = Box::from(env!("CARGO_BIN_EXE_world_process_fixture").as_bytes());
                spawn.args = Box::new([Box::from(&b"echo"[..]), Box::from(&b"0"[..]), Box::from(&b"1"[..])]);
            }
            self.script.submissions().push(record);
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
    fn is_empty(&self) -> bool {
        self.script.is_empty()
    }
    fn worst_case(&self) -> u64 {
        self.script.worst_case()
    }
    fn operations(&self) -> u32 {
        self.script.operations()
    }
}

#[derive(Debug)]
struct OpeningChild {
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    started: bool,
}

impl Host for OpeningChild {
    fn iterate(&mut self, _now: Time, _wall: Wall) {
        if !self.started {
            self.submissions.push(Submit { op: Token::new(1), kind: Op::Socket { family: Family::Ipv4 } });
            self.started = true;
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        !self.started
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        false
    }
    fn worst_case(&self) -> u64 {
        4096
    }
    fn operations(&self) -> u32 {
        1
    }
}

#[derive(Debug)]
enum Proc {
    Parent(RecordingParent),
    Script(Script),
    Opening(OpeningChild),
}

impl Proc {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Parent(parent) => parent,
            Self::Script(script) => script,
            Self::Opening(child) => child,
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Parent(parent) => parent,
            Self::Script(script) => script,
            Self::Opening(child) => child,
        }
    }
}

impl Host for Proc {
    fn drain(&mut self) {
        self.host_mut().drain();
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        self.host_mut().iterate(now, wall);
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
    fn is_empty(&self) -> bool {
        self.host().is_empty()
    }
    fn exit(&self) -> Option<skein_io::kernel::Exit> {
        self.host().exit()
    }
    fn worst_case(&self) -> u64 {
        self.host().worst_case()
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}

fn echo(spawn: &Spawn, inherited: &Inherited) -> Proc {
    assert_eq!(spawn.args.as_ref(), [Box::from(&b"echo"[..]), Box::from(&b"0"[..]), Box::from(&b"1"[..])]);
    Proc::Script(Script::child(inherited, &[Act::Read(0), Act::Write(1, b"hello"), Act::Read(0)]))
}

fn fixture(hosted: bool) -> RecordingParent {
    let mut world = real::World::new(Quiet);
    if hosted {
        world.host(HostedProgram {
            program: Box::from(env!("CARGO_BIN_EXE_world_process_fixture").as_bytes()),
            make: echo,
            instances: 1,
            operations: 8,
        });
    }
    let root = open_root(Path::new(".")).expect("startup root");
    world.spawn_with_fds(vec![root], || Proc::Parent(RecordingParent::new(root)));
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    let Proc::Parent(parent) = outcome.procs.into_iter().next().expect("parent") else {
        panic!("first process is the parent")
    };
    parent
}

#[test]
fn real_conformance_fixture_and_hosted_equivalent_return_the_same_parent_records() {
    let real = fixture(false);
    let hosted = fixture(true);
    assert_eq!(real.script.received, b"hello");
    assert_eq!(hosted.script.received, b"hello");
    assert_eq!(
        real.records, hosted.records,
        "kernel.md record return and io.md section 6: parent cannot distinguish hosting"
    );
}

fn opening(_spawn: &Spawn, _inherited: &Inherited) -> Proc {
    Proc::Opening(OpeningChild {
        completions: Queue::with_capacity(1),
        submissions: Queue::with_capacity(1),
        started: false,
    })
}

#[test]
fn kill_closes_a_descriptor_returned_before_the_child_reaps_it() {
    let before = std::fs::read_dir("/proc/self/fd").expect("descriptor table").count();
    for _ in 0..16 {
        let mut world = real::World::new(Quiet);
        world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make: opening, instances: 1, operations: 1 });
        let root = open_root(Path::new(".")).expect("startup root");
        world.spawn_with_fds(vec![root], || {
            Proc::Script(Script::parent(root, &[Act::Spawn, Act::Signal(Signal::Kill), Act::Read(1), Act::Wait]))
        });
        let outcome = world.run(&Clock::new(), Duration::from_secs(1));
        assert_eq!(outcome.killed.len(), 1);
        assert_eq!(
            std::fs::read_dir("/proc/self/fd").expect("descriptor table").count(),
            before,
            "all inherited and completion-returned descriptors are closed after kill"
        );
    }
}

#[derive(Debug)]
struct CancelReader {
    reader: Fd,
    writer: Fd,
    stage: u32,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    answers: Vec<Complete>,
}

impl CancelReader {
    fn new(reader: Fd, writer: Fd) -> Self {
        Self {
            reader,
            writer,
            stage: 0,
            completions: Queue::with_capacity(4),
            submissions: Queue::with_capacity(4),
            answers: Vec::new(),
        }
    }
}

impl Host for CancelReader {
    fn iterate(&mut self, _now: Time, _wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            self.answers.push(complete);
        }
        match self.stage {
            0 => {
                self.submissions
                    .push(Submit { op: Token::new(1), kind: Op::PipeRead { fd: self.reader, buf: Box::new([0]) } });
                self.submissions.push(Submit { op: Token::new(2), kind: Op::Cancel { target: Token::new(1) } });
                self.stage = 1;
            }
            1 if self.answers.len() == 2 => {
                self.submissions.push(Submit { op: Token::new(3), kind: Op::Close { fd: self.reader } });
                self.submissions.push(Submit { op: Token::new(4), kind: Op::Close { fd: self.writer } });
                self.stage = 2;
            }
            1 | 2 => {}
            _ => panic!("known script stage"),
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        self.stage == 0 || !self.completions.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.answers.len() == 4
    }
    fn worst_case(&self) -> u64 {
        4096
    }
    fn operations(&self) -> u32 {
        4
    }
}

#[test]
fn colliding_host_tokens_and_cancel_targets_return_with_their_original_identity() {
    let mut world = real::World::new(Quiet);
    for _ in 0..2 {
        let (reader, writer) = skein_shell::open_signal_pipe().expect("a real empty pipe");
        world.spawn_with_fds(vec![reader, writer], || CancelReader::new(reader, writer));
    }
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    for host in outcome.procs {
        assert_eq!(host.answers.len(), 4);
        let read = host.answers.iter().find(|complete| complete.op == Token::new(1)).expect("original read token");
        assert_eq!(read.result, Err(Error::Cancelled));
        let cancel = host.answers.iter().find(|complete| complete.op == Token::new(2)).expect("original cancel token");
        assert_eq!(cancel.kind, Op::Cancel { target: Token::new(1) });
        assert_eq!(cancel.result, Ok(Done::Nothing));
    }
}

#[derive(Debug)]
struct Overflow {
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
}

impl Host for Overflow {
    fn iterate(&mut self, _now: Time, _wall: Wall) {
        for token in 1..=2 {
            self.submissions.push(Submit { op: Token::new(token), kind: Op::Socket { family: Family::Ipv4 } });
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        false
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        false
    }
    fn worst_case(&self) -> u64 {
        4096
    }
    fn operations(&self) -> u32 {
        1
    }
}

#[test]
#[should_panic(expected = "exceeds its own operation limit")]
fn shared_ring_room_does_not_allow_a_host_to_exceed_its_own_limit() {
    let _outcome = real::run(
        vec![Overflow { completions: Queue::with_capacity(2), submissions: Queue::with_capacity(2) }],
        Quiet,
        &Clock::new(),
        Duration::from_secs(1),
    );
}
