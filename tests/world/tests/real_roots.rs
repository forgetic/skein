//! The same startup-root factories over a real scratch directory and one ring.

use std::os::unix::ffi::OsStrExt;

use skein_io::kernel::{Error, Exit};
use skein_lib::Duration;
use skein_scratch::Scratch;
use skein_shell::{Clock, open_root};
use skein_world::{HostedProgram, real};
use skein_world_tests::hosted::{Act, Judge, Script};
use skein_world_tests::roots::{file_child, missing_roots, parent, roots, waiting_child};

#[test]
fn child_files_survive_parent_root_close_in_a_real_scratch_directory() {
    let scratch = Scratch::new("hosted-roots");
    let root = open_root(scratch.path()).expect("parent root");
    let mut world = real::World::new(Judge);
    world.host_roots(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: file_child, instances: 1, operations: 8 },
        roots,
    );
    world.spawn_with_fds(vec![root], || parent(root, scratch.path().as_os_str().as_bytes(), false));
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert_scratch_closed(&scratch);
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Code(0)));
    assert_eq!(outcome.procs[1].received, b"gofrom child");
    assert_eq!(std::fs::read(scratch.path().join("note")).expect("child output file"), b"from child");
}

#[test]
fn kill_closes_real_child_roots_and_settles_the_parent_wait() {
    let scratch = Scratch::new("killed-roots");
    let root = open_root(scratch.path()).expect("parent root");
    let mut world = real::World::new(Judge);
    world.host_roots(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: waiting_child, instances: 1, operations: 8 },
        roots,
    );
    world.spawn_with_fds(vec![root], || parent(root, scratch.path().as_os_str().as_bytes(), true));
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert_scratch_closed(&scratch);
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Signal(9)));
    assert_eq!(outcome.killed.len(), 1);
}

#[test]
fn partial_real_startup_failure_closes_roots_without_calling_the_factory() {
    let scratch = Scratch::new("failed-roots");
    let root = open_root(scratch.path()).expect("parent root");
    let mut world = real::World::new(Judge);
    world.host_roots(
        HostedProgram { program: Box::from(&b"hosted"[..]), make: file_child, instances: 1, operations: 8 },
        missing_roots,
    );
    world.spawn_with_fds(vec![root], || {
        let mut script = Script::parent(root, &[Act::Spawn]);
        script.args = Box::new([Box::from(scratch.path().as_os_str().as_bytes())]);
        script
    });
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert_scratch_closed(&scratch);
    assert_eq!(outcome.procs.len(), 1);
    assert!(outcome.procs[0].results.contains(&Err(Error::NotFound)));
}

fn assert_scratch_closed(scratch: &Scratch) {
    for entry in std::fs::read_dir("/proc/self/fd").expect("descriptor observations") {
        let entry = entry.expect("descriptor entry");
        if let Ok(path) = std::fs::read_link(entry.path()) {
            assert!(
                !path.starts_with(scratch.path()),
                "no parent, child or rollback descriptor remains open: {}",
                path.display()
            );
        }
    }
}
