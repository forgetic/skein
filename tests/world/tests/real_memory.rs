//! Checked real-process ownership, including buffers returned after a kill.

use skein_heap::Counting;
use skein_io::kernel::{Exit, Signal, Spawn};
use skein_lib::Duration;
use skein_shell::{Clock, open_root};
use skein_world::{HostedProgram, Inherited, real};
use skein_world_tests::hosted::{Act, Judge, Script};

#[global_allocator]
static HEAP: Counting = Counting;

fn exchange(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply")])
}

fn waiting(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let mut script = Script::child(inherited, &[Act::Read(0), Act::Write(1, b"reply"), Act::Read(0)]);
    script.hog = 60 * 1024;
    script
}

fn run(make: fn(&Spawn, &Inherited) -> Script, acts: &[Act]) -> real::CheckedOutcome<Script> {
    let mut world = real::World::new_checked(Judge);
    world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make, instances: 1, operations: 8 });
    let root = open_root(std::path::Path::new(".")).expect("startup root");
    world.spawn_with_fds(vec![root], || Script::parent(root, acts));
    world.run(&Clock::new(), Duration::from_secs(1))
}

#[test]
fn normal_hosted_exit_preserves_separate_peaks_and_exact_final_release() {
    let outcome = run(exchange, &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Read(1), Act::Wait]);
    assert_eq!(outcome.procs[0].received, b"reply");
    assert_eq!(outcome.procs[1].received, b"hello");
    assert_eq!(outcome.procs[0].child_exit, Some(Exit::Code(0)));
    let peaks = outcome.heap.as_ref().expect("checked processes");
    assert_eq!(peaks.len(), 2);
    for (peak, bound) in peaks {
        assert!(*peak > 0 && peak <= bound);
    }
    assert_eq!(outcome.held.as_ref().expect("final ownership ledger").len(), 2);
    drop(outcome);
}

#[test]
fn kill_returns_pending_buffers_before_exact_child_release() {
    for _ in 0..16 {
        let outcome = run(
            waiting,
            &[Act::Spawn, Act::Write(0, b"hello"), Act::Read(1), Act::Signal(Signal::Kill), Act::Read(1), Act::Wait],
        );
        assert_eq!(outcome.procs.len(), 1);
        assert_eq!(outcome.procs[0].child_exit, Some(Exit::Signal(9)));
        assert_eq!(outcome.killed.len(), 1);
        let (peak, bound) = outcome.killed[0].heap.expect("killed child checked on release");
        assert!(peak >= 60 * 1024 && peak <= bound);
        assert_eq!(outcome.heap.as_ref().expect("surviving root metered").len(), 1);
        drop(outcome);
    }
}

fn hogging(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let mut script = Script::child(inherited, &[]);
    script.hog = 64 * 1024;
    script
}

#[test]
#[should_panic(expected = "past its worst case")]
fn hosted_iteration_cannot_exceed_its_own_bound() {
    drop(run(hogging, &[Act::Spawn, Act::Read(1), Act::Wait]));
}

fn leaky(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let mut script = Script::child(inherited, &[]);
    script.hog = 1;
    script.leak = true;
    script
}

#[test]
#[should_panic(expected = "must release exactly its own metered heap")]
fn settled_child_leak_is_detected_at_final_drop() {
    drop(run(leaky, &[Act::Spawn, Act::Read(1), Act::Wait]));
}

fn leaky_killed(_spawn: &Spawn, inherited: &Inherited) -> Script {
    let _leaked = Box::leak(Box::new([0_u8; 64]));
    Script::child(inherited, &[Act::Read(0)])
}

#[test]
#[should_panic(expected = "a leak")]
fn killed_child_leak_is_detected_after_kernel_settlement() {
    drop(run(leaky_killed, &[Act::Spawn, Act::Signal(Signal::Kill), Act::Read(1), Act::Wait]));
}

#[test]
#[should_panic(expected = "past its worst case")]
fn root_construction_is_inside_its_meter() {
    let mut world = real::World::new_checked(Judge);
    world.spawn(|| {
        let mut script = Script::new(&[]);
        script.held = vec![0; 64 * 1024].into_boxed_slice();
        script
    });
}

#[derive(Debug)]
struct AllocatingJudge {
    storage: Vec<u8>,
}

impl skein_world::Referee<Script> for AllocatingJudge {
    fn act(&mut self, _now: skein_lib::Time, _procs: &mut [Script]) {
        self.storage.resize(128 * 1024, 1);
    }
    fn observe(&mut self, _now: skein_lib::Time, _procs: &[Script]) {}
    fn next_deadline(&self) -> Option<skein_lib::Time> {
        None
    }
    fn overdue(&self, _now: skein_lib::Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        true
    }
}

#[test]
fn referee_storage_is_excluded_from_process_accounting() {
    let mut world = real::World::new_controlled_checked(|_| AllocatingJudge { storage: Vec::new() });
    world.spawn(|| Script::new(&[]));
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert!(outcome.heap.as_ref().expect("root checked")[0].0 < 64 * 1024);
    drop(outcome);
}
