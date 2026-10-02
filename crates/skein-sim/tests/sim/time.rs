//! Time: the world moves it only when asked, to the next thing due.

use skein_io::kernel::{Done, Family, Op};
use skein_lib::{Duration, Time};
use skein_sim::{Config, Faults};

use crate::support::World;

fn slow() -> Config {
    let faults = Faults { latency: 1000, latency_max: Duration::from_millis(10), ..Faults::NONE };
    Config { faults, ..Config::calm() }
}

#[test]
fn latency_delays_a_completion_until_time_moves() {
    let mut world = World::new(1, slow());
    let pid = world.spawn();
    let token = world.submit(pid, Op::Socket { family: Family::Ipv4 });
    assert!(world.reap(pid).is_empty(), "not delivered yet");
    let due = world.sim.next_due().expect("a delivery is due");
    assert!(due > Time::ZERO && due <= Time::ZERO.saturating_add(Duration::from_millis(10)));
    assert!(world.sim.advance());
    assert_eq!(world.sim.now(), due, "time jumps to the delivery");
    assert!(matches!(world.reap_one(pid, token).result, Ok(Done::Fd(_))));
    assert!(!world.sim.advance(), "then the world is idle");
    assert_eq!(world.sim.now(), due, "and time stays");
}

#[test]
fn advancing_to_a_deadline_delivers_what_was_due_before_it() {
    let mut world = World::new(2, slow());
    let pid = world.spawn();
    for _ in 0..8_u32 {
        world.submit(pid, Op::Socket { family: Family::Ipv4 });
    }
    let deadline = Time::ZERO.saturating_add(Duration::from_millis(5));
    world.sim.advance_to(deadline);
    assert_eq!(world.sim.now(), deadline);
    let early = world.reap(pid).len();
    world.sim.advance_to(Time::ZERO.saturating_add(Duration::from_millis(10)));
    assert_eq!(early + world.reap(pid).len(), 8, "every one by the most latency");
    assert_eq!(
        world.sim.wall().as_nanos().checked_sub(Config::calm().wall.as_nanos()),
        Some(10_000_000),
        "wall time follows"
    );
}

#[test]
fn latency_reorders_completions() {
    let mut reordered = false;
    for seed in 0..16_u64 {
        let mut world = World::new(seed, slow());
        let pid = world.spawn();
        let first = world.submit(pid, Op::Socket { family: Family::Ipv4 });
        world.submit(pid, Op::Socket { family: Family::Ipv4 });
        while world.sim.advance() {}
        let got = world.reap(pid);
        reordered |= got[0].op != first;
    }
    assert!(reordered, "completions arrive in any order");
}
