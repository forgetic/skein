//! Independent child roots, startup rollback and per-process metering.

use skein_io::kernel::{Error, Exit};
use skein_world::{HostedProgram, Memory, World};
use skein_world_tests::hosted::{Act, Judge, Script};
use skein_world_tests::roots::{RootFiles, file_child, missing_roots, parent, roots, waiting_child};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn independently_owned_roots_support_child_files_after_parent_close_and_settle_memory() {
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let mut world = World::new(17, skein_sim::Config::calm(), Judge, Memory::Checked).with_machine(machine);
    world.host_roots(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: file_child, instances: 1, operations: 8 },
        roots,
    );
    world.spawn_root(root, |fd| parent(fd, b".", false));
    let mut outcome = world.run();
    assert_eq!(outcome.procs[0].received, b"ready");
    assert_eq!(outcome.procs[1].received, b"gofrom child");
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Code(0)));
    assert!(outcome.heap.as_ref().expect("metered processes").iter().all(|(peak, bound)| peak <= bound));
    outcome.machine.finish();
}

#[test]
fn killed_child_closes_all_startup_roots_under_its_own_memory_span() {
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let mut world = World::new(19, skein_sim::Config::calm(), Judge, Memory::Checked).with_machine(machine);
    world.host_roots(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: waiting_child, instances: 1, operations: 8 },
        roots,
    );
    world.spawn_root(root, |fd| parent(fd, b".", true));
    let mut outcome = world.run();
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Signal(9)));
    assert_eq!(outcome.killed.len(), 1);
    assert!(outcome.killed[0].heap.is_some());
    outcome.machine.finish();
}

#[test]
fn failed_startup_rolls_back_roots_and_never_admits_a_child() {
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let mut world = World::new(23, skein_sim::Config::calm(), Judge, Memory::Checked).with_machine(machine);
    world.host_roots(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: file_child, instances: 1, operations: 8 },
        missing_roots,
    );
    world.spawn_root(root, |fd| {
        let mut script = Script::parent(fd, &[Act::Spawn]);
        script.args = Box::new([Box::from(&b"."[..])]);
        script
    });
    let mut outcome = world.run();
    assert_eq!(outcome.procs.len(), 1);
    assert!(outcome.procs[0].results.contains(&Err(Error::NotFound)));
    outcome.machine.finish();
}

#[test]
fn startup_root_stories_replay_completion_reordering() {
    for (kill, missing) in [(false, false), (true, false), (false, true)] {
        let mut first = skein_world_tests::roots::simulated(31, kill, missing);
        let mut second = skein_world_tests::roots::simulated(31, kill, missing);
        assert_eq!(first.trace, second.trace, "startup roots preserve seeded replay");
        first.machine.finish();
        second.machine.finish();
    }
}

#[test]
fn startup_roots_refuse_a_child_beyond_its_descriptor_limit() {
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let mut world =
        World::new(29, skein_sim::Config { max_fds: 4, ..skein_sim::Config::calm() }, Judge, Memory::Checked)
            .with_machine(machine);
    world.host_roots(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: file_child, instances: 1, operations: 8 },
        roots,
    );
    world.spawn_root(root, |fd| {
        let mut script = Script::parent(fd, &[Act::Spawn]);
        script.args = Box::new([Box::from(&b"."[..])]);
        script
    });
    let mut outcome = world.run();
    assert_eq!(outcome.procs.len(), 1);
    assert!(outcome.procs[0].results.contains(&Err(Error::TooManyOpenFiles)));
    outcome.machine.finish();
}

#[test]
fn startup_append_files_survive_parent_close_and_all_owned_handles_settle() {
    use skein_fake_machine::{How, Opened};
    use skein_world::Machine;
    use skein_world_tests::roots::{append_child, appends};
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let file =
        machine.files.open(Opened::new(root.raw()), b"trace", How::Append { mode: 0o600 }).expect("existing trace");
    machine.files.append(file, b"prefix:").expect("trace prefix");
    machine.files.close(file);
    let mut world = World::new(37, skein_sim::Config::calm(), Judge, Memory::Checked).with_machine(machine);
    world.host_startup(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: append_child, instances: 1, operations: 8 },
        roots,
        appends,
    );
    world.spawn_root(root, |fd| parent(fd, b".", false));
    let mut outcome = world.run();
    assert_eq!(outcome.procs[0].received, b"ready");
    let root = outcome.machine.open_root(b".").expect("inspect the trace");
    let file = outcome.machine.files.open(Opened::new(root.raw()), b"trace", How::Read).expect("trace");
    assert_eq!(outcome.machine.files.read(file, 0, 64).expect("trace bytes"), b"prefix:onetwo");
    outcome.machine.files.close(file);
    outcome.machine.close_root(root);
    outcome.machine.finish();
}

#[test]
fn a_failed_later_startup_append_rolls_back_files_and_roots_before_admission() {
    use skein_world_tests::roots::{append_child, missing_appends};
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let mut world = World::new(41, skein_sim::Config::calm(), Judge, Memory::Checked).with_machine(machine);
    world.host_startup(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: append_child, instances: 1, operations: 8 },
        roots,
        missing_appends,
    );
    world.spawn_root(root, |fd| {
        let mut script = Script::parent(fd, &[Act::Spawn]);
        script.args = Box::new([Box::from(&b"."[..])]);
        script
    });
    let mut outcome = world.run();
    assert_eq!(outcome.procs.len(), 1);
    assert!(outcome.procs[0].results.contains(&Err(Error::NotFound)));
    outcome.machine.finish();
}

#[test]
fn a_killed_child_releases_its_startup_append_files_in_its_own_memory_span() {
    use skein_world_tests::roots::appends;
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let mut world = World::new(43, skein_sim::Config::calm(), Judge, Memory::Checked).with_machine(machine);
    world.host_startup(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: waiting_child, instances: 1, operations: 8 },
        roots,
        appends,
    );
    world.spawn_root(root, |fd| parent(fd, b".", true));
    let mut outcome = world.run();
    assert_eq!(outcome.killed.len(), 1);
    assert!(outcome.killed[0].heap.is_some());
    outcome.machine.finish();
}

#[test]
fn startup_append_descriptors_enter_the_childs_descriptor_limit() {
    use skein_world_tests::roots::{append_child, appends};
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let mut world =
        World::new(47, skein_sim::Config { max_fds: 6, ..skein_sim::Config::calm() }, Judge, Memory::Checked)
            .with_machine(machine);
    world.host_startup(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: append_child, instances: 1, operations: 8 },
        roots,
        appends,
    );
    world.spawn_root(root, |fd| {
        let mut script = Script::parent(fd, &[Act::Spawn]);
        script.args = Box::new([Box::from(&b"."[..])]);
        script
    });
    let mut outcome = world.run();
    assert_eq!(outcome.procs.len(), 1);
    assert!(outcome.procs[0].results.contains(&Err(Error::TooManyOpenFiles)));
    outcome.machine.finish();
}
