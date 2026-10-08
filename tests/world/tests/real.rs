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
