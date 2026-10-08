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

#[test]
fn a_parent_kills_a_hosted_child_mid_exchange() {
    use skein_world_tests::hosted::{Story, check_story, story};
    let outcome = story(7, Story::Kill);
    check_story(Story::Kill, &outcome);
}

#[test]
fn a_hosted_child_exits_before_its_parent_writes() {
    use skein_world_tests::hosted::{Story, check_story, story};
    let outcome = story(7, Story::EarlyExit);
    check_story(Story::EarlyExit, &outcome);
}

#[test]
fn a_parents_termination_signal_reaches_its_hosted_child() {
    use skein_world_tests::hosted::{Story, check_story, story};
    let outcome = story(7, Story::Terminate);
    check_story(Story::Terminate, &outcome);
}

#[test]
fn a_hosted_child_reaches_its_worst_case_and_still_frees_exactly_what_it_held() {
    use skein_world_tests::hosted::{Story, check_story, story};
    let outcome = story(7, Story::Memory);
    check_story(Story::Memory, &outcome);
}

#[test]
fn every_hosted_story_replays_under_its_own_memory_meter() {
    use skein_world_tests::hosted::{Story, check_story, story};
    for scenario in [Story::Exchange, Story::Kill, Story::EarlyExit, Story::Terminate, Story::Memory] {
        let first = story(19, scenario);
        let second = story(19, scenario);
        check_story(scenario, &first);
        check_story(scenario, &second);
        assert_eq!(first.trace, second.trace, "the hosted story replays its records");
        assert_eq!(first.heap, second.heap, "the hosted story replays its surviving process peaks");
    }
}

fn leaky_killed_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let _leaked = Box::leak(Box::new([0; 64]));
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply"), Act::Read(0)])
}

#[test]
#[should_panic(expected = "terminated process 1 freed")]
fn a_killed_childs_leak_is_checked_when_it_is_dropped() {
    use skein_io::kernel::Signal;
    drop(simple(
        leaky_killed_child,
        &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Signal(Signal::Kill), Act::Read(1), Act::Wait],
    ));
}
