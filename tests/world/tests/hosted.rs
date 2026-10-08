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
    let mut world = World::new(7, Config::calm(), Judge, Memory::Unchecked).with_machine(RootMachine);
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
