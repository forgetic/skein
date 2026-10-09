//! The hosted scripts use real pipes and the same factories as simulated worlds.

use skein_io::kernel::{Exit, Signal, Spawn};
use skein_lib::Duration;
use skein_shell::{Clock, open_root};
use skein_world::{HostedProgram, Inherited, real};
use skein_world_tests::hosted::{Act, Judge, Script};

fn exchange(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply")])
}

fn wait_again(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply"), Act::Read(0)])
}

fn run(make: fn(&Spawn, &Inherited) -> Script, acts: &[Act]) -> real::Outcome<Script> {
    let mut world = real::World::new(Judge);
    world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make, instances: 1, operations: 8 });
    let root = open_root(std::path::Path::new(".")).expect("startup root");
    world.spawn_with_fds(vec![root], || Script::parent(root, acts));
    world.run(&Clock::new(), Duration::from_secs(1))
}

#[test]
fn hosted_parent_and_child_exchange_over_real_pipes() {
    let outcome = run(exchange, &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Read(1), Act::Wait]);
    assert_eq!(outcome.procs[0].received, b"reply");
    assert_eq!(outcome.procs[1].received, b"hello");
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Code(0)));
    assert!(outcome.killed.is_empty());
}

#[test]
fn kill_drops_child_and_settles_pending_read_before_parent_wait() {
    let outcome = run(
        wait_again,
        &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Signal(Signal::Kill), Act::Read(1), Act::Wait],
    );
    assert_eq!(outcome.procs.len(), 1);
    assert_eq!(outcome.procs[0].received, b"reply");
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Signal(9)));
    assert_eq!(outcome.killed[0].exit, Exit::Signal(9));
}

fn terminated(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::ReadSignal, Act::Write(1, b"terminated")])
}

#[test]
fn parent_termination_arrives_as_a_signal_record_on_the_child_pipe() {
    let outcome = run(terminated, &[Act::Spawn, Act::Signal(Signal::Terminate), Act::Read(1), Act::Read(1), Act::Wait]);
    assert_eq!(outcome.procs[0].received, b"terminated");
    assert_eq!(outcome.procs[1].signals, [skein_io::kernel::ServiceSignal::Terminate]);
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Code(0)));
}

#[derive(Debug)]
struct SignalJudge {
    control: real::Controls,
    child: bool,
    sent: bool,
}

impl skein_world::Referee<Script> for SignalJudge {
    fn act(&mut self, _now: skein_lib::Time, procs: &mut [Script]) {
        if self.sent {
            return;
        }
        if self.child {
            // This is the parent's public spawn result, not its service state.
            if !procs[0].results.iter().any(|result| matches!(result, Ok(skein_io::kernel::Done::Spawned { .. }))) {
                return;
            }
            self.control.signal(1, skein_io::kernel::ServiceSignal::Interrupt);
        } else {
            self.control.signal(0, skein_io::kernel::ServiceSignal::Terminate);
            self.control.signal(1, skein_io::kernel::ServiceSignal::Interrupt);
        }
        self.sent = true;
    }
    fn observe(&mut self, _now: skein_lib::Time, procs: &[Script]) {
        if self.child
            && !self.sent
            && procs[0].results.iter().any(|result| matches!(result, Ok(skein_io::kernel::Done::Spawned { .. })))
        {
            self.control.signal(1, skein_io::kernel::ServiceSignal::Interrupt);
            self.sent = true;
        }
    }
    fn next_deadline(&self) -> Option<skein_lib::Time> {
        None
    }
    fn overdue(&self, _now: skein_lib::Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        self.sent
    }
}

#[test]
fn referee_real_signal_reaches_only_the_signalfd_reader_and_pipe_signal_reaches_its_host() {
    let mut world = real::World::new_controlled(|control| SignalJudge { control, child: false, sent: false });
    world.spawn_signalfd(|signal| {
        Script::child(&Inherited { roots: vec![], appends: vec![], pipes: vec![], signal }, &[Act::ReadSignal])
    });
    world.spawn_signals(|signal| {
        Script::child(&Inherited { roots: vec![], appends: vec![], pipes: vec![], signal }, &[Act::ReadSignal])
    });
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert_eq!(outcome.procs[0].signals, [skein_io::kernel::ServiceSignal::Terminate]);
    assert_eq!(outcome.procs[1].signals, [skein_io::kernel::ServiceSignal::Interrupt]);
}

#[test]
fn referee_signal_reaches_a_hosted_child_through_its_pipe() {
    let mut world = real::World::new_controlled(|control| SignalJudge { control, child: true, sent: false });
    world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make: terminated, instances: 1, operations: 8 });
    let root = open_root(std::path::Path::new(".")).expect("startup root");
    world.spawn_with_fds(vec![root], || Script::parent(root, &[Act::Spawn, Act::Read(1), Act::Read(1), Act::Wait]));
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert_eq!(outcome.procs[0].received, b"terminated");
    assert_eq!(outcome.procs[1].signals, [skein_io::kernel::ServiceSignal::Interrupt]);
}
