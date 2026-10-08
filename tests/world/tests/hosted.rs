//! Hosting through the world harness, with scripted services on raw pipes.

use skein_heap::Counting;
use skein_io::kernel::{Exit, Spawn};
use skein_sim::{Config, Handle};
use skein_world::{HostedProgram, Inherited, Memory, World};
use skein_world_tests::hosted::{Act, Judge, RootMachine, Script};

#[global_allocator]
static HEAP: Counting = Counting;

fn child(spawn: &Spawn, inherited: &Inherited) -> Script {
    assert_eq!(&*spawn.args[0], b"argument");
    assert_eq!(&*spawn.env[0], b"KEY=value");
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply")])
}

#[test]
fn a_spawn_hosts_the_child_with_its_arguments_environment_and_pipes() {
    let mut world = World::new(7, Config::calm(), Judge, Memory::Checked).with_machine(RootMachine);
    world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make: child, instances: 1, operations: 8 });
    world.spawn_root(Handle::new(1), |root| {
        Script::parent(root, &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Read(1), Act::Wait])
    });
    let outcome = world.run();
    assert_eq!(outcome.procs.len(), 2);
    assert_eq!(outcome.procs[0].received, b"reply");
    assert_eq!(outcome.procs[1].received, b"hello");
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Code(0)));
}

fn exchange(seed: u64) -> skein_world::Outcome<Script, RootMachine> {
    let faults = skein_sim::Faults {
        latency: 1000,
        latency_max: skein_lib::Duration::from_millis(1),
        ..skein_sim::Faults::NONE
    };
    let mut world =
        World::new(seed, Config { faults, ..Config::calm() }, Judge, Memory::Checked).with_machine(RootMachine);
    world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make: child, instances: 1, operations: 8 });
    world.spawn_root(Handle::new(1), |root| {
        Script::parent(root, &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Read(1), Act::Wait])
    });
    world.run()
}

#[test]
fn hosted_processes_replay_and_each_frees_its_metered_heap() {
    let first = exchange(7);
    let second = exchange(7);
    assert_eq!(first.trace, second.trace, "hosted process records replay with the same seed");
    assert_eq!(first.heap, second.heap, "each process's peaks replay too");
    let heap = first.heap.as_ref().expect("checked memory");
    assert_eq!(heap.len(), 2, "parent and hosted child are separately metered");
    assert!(heap.iter().all(|(peak, bound)| *peak > 0 && peak <= bound), "both process heaps fit their own bounds");
    assert_ne!(first.trace, exchange(8).trace, "another seed changes the delayed completions");
    drop(first);
    drop(second);
}

fn hogging_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let mut script = Script::child(inherited, &[]);
    script.hog = 64 * 1024;
    script
}

fn leaky_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let mut script = Script::child(inherited, &[]);
    script.hog = 1;
    script.leak = true;
    script
}

fn empty_child_without_exit(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let mut script = Script::child(inherited, &[]);
    script.terminal = None;
    script
}

fn simple(make: fn(&Spawn, &Inherited) -> Script, acts: &[Act]) -> skein_world::Outcome<Script, RootMachine> {
    let mut world = World::new(7, Config::calm(), Judge, Memory::Checked).with_machine(RootMachine);
    world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make, instances: 1, operations: 8 });
    world.spawn_root(Handle::new(1), |root| Script::parent(root, acts));
    world.run()
}

#[test]
#[should_panic(expected = "process 1 held")]
fn a_hosted_child_past_its_own_worst_case_fails() {
    let _outcome = simple(hogging_child, &[Act::Spawn, Act::Read(1), Act::Wait]);
}

#[test]
#[should_panic(expected = "a leak")]
fn a_hosted_child_that_leaks_fails_when_dropped() {
    drop(simple(leaky_child, &[Act::Spawn, Act::Read(1), Act::Wait]));
}

#[test]
#[should_panic(expected = "idle, unsettled")]
fn a_hosted_child_without_a_terminal_does_not_settle() {
    let _outcome = simple(empty_child_without_exit, &[Act::Spawn, Act::Read(1)]);
}

#[test]
#[should_panic(expected = "read to its end")]
fn closing_a_hosted_childs_output_without_reading_its_end_does_not_settle() {
    let _outcome = simple(leaky_child, &[Act::Spawn, Act::Wait]);
}
