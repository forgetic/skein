//! What the world harness must do and catch (examples.md, 6): time held
//! while a process has work deferred to its next entry; a liveness
//! expectation overdue, and a safety one broken; a process past its worst
//! case, and one that leaks; and a seed that replays. Under the counting
//! allocator, as every world here checks memory.

use skein_heap::Counting;
use skein_lib::Duration;
use skein_sim::{Config, Event, Fault, Faults, Summary};
use skein_world::{Memory, Outcome, World};
use skein_world_tests::{Expect, Judge, Script, WORST, client, ms, server};

#[global_allocator]
static HEAP: Counting = Counting;

const SEED: u64 = 7;

/// Every completion late, by up to a millisecond, and nothing else.
fn late() -> Config {
    let faults = Faults { latency: 1000, latency_max: Duration::from_millis(1), ..Faults::NONE };
    Config { faults, ..Config::calm() }
}

/// A world of a server and a client, each made inside the world, so that its
/// making is metered as its own.
fn world(
    config: Config,
    expect: Vec<Expect>,
    server: impl FnOnce() -> Script,
    client: impl FnOnce() -> Script,
) -> World<Script, Judge> {
    let mut world = World::new(SEED, config, Judge::new(SEED, expect), Memory::Checked);
    world.spawn(server);
    world.spawn(client);
    world
}

fn sender() -> Script {
    client(ms(10), ms(20))
}

fn exchange(config: Config) -> Outcome<Script> {
    let expect = vec![Expect::Receives { at: 0, by: ms(1_000) }];
    world(config, expect, server, sender).run()
}

#[test]
fn a_world_of_two_scripts_settles_and_each_frees_what_it_held() {
    let outcome = exchange(Config::calm());
    let received = &outcome.procs[0].received;
    assert_eq!(received.len(), 1);
    assert_eq!((received[0].0, &*received[0].1), (ms(20), &b"hello"[..]), "received when sent");
    let heap = outcome.heap.as_ref().expect("checked");
    assert!(heap.iter().all(|(most, bound)| *most > 0 && most <= bound), "within each worst case: {heap:?}");
}

/// The client's send, with its own completion late, leaves the server's
/// waiting receive as the only work: deferred to the server's next entry,
/// which the harness must make at once, before time moves on to the late
/// completion. The server's receive is decided, its latency drawn and
/// traced, at the very time of the send.
#[test]
fn time_does_not_move_while_a_process_has_deferred_work() {
    let outcome = exchange(late());
    let (server, client) = (0, 1);
    let sent = outcome
        .trace
        .iter()
        .position(|entry| {
            entry.pid.raw() == client && matches!(entry.event, Event::Submit { kind: Summary::Send { .. }, .. })
        })
        .expect("the client sent");
    let at = outcome.trace[sent].at;
    let decided = outcome.trace[sent..]
        .iter()
        .find(|entry| entry.pid.raw() == server && entry.event == Event::Fault(Fault::Latency))
        .expect("the server's receive was decided");
    assert_eq!(decided.at, at, "the server entered at the time of the send: time did not move");
}

#[test]
#[should_panic(expected = "unmet: Receives")]
fn an_overdue_liveness_expectation_fails_naming_what_is_pending() {
    let expect = vec![Expect::Receives { at: 0, by: ms(5) }];
    let _outcome = world(Config::calm(), expect, server, sender).run();
}

#[test]
#[should_panic(expected = "broke: it received")]
fn a_broken_safety_expectation_fails_at_once() {
    let expect = vec![Expect::Silent { at: 0, until: ms(1_000) }];
    let _outcome = world(Config::calm(), expect, server, sender).run();
}

#[test]
#[should_panic(expected = "past its worst case")]
fn a_process_past_its_worst_case_fails() {
    let hog = usize::try_from(WORST).expect("small");
    let _outcome = world(Config::calm(), Vec::new(), || server().hogging(hog), sender).run();
}

#[test]
#[should_panic(expected = "a leak")]
fn a_process_that_leaks_fails_once_settled() {
    let _outcome =
        world(Config::calm(), vec![Expect::Receives { at: 0, by: ms(1_000) }], || server().leaking(100), sender).run();
}

#[test]
fn the_same_seed_replays_to_the_same_trace() {
    let faults = Faults { short_recv: 500, short_send: 500, ..late().faults };
    let config = Config { faults, ..Config::calm() };
    let first = exchange(config);
    let second = exchange(config);
    assert!(first.trace == second.trace, "the same records, at the same times");
    assert!(first.trace.iter().any(|entry| entry.event == Event::Fault(Fault::Latency)), "faults fell");
    let mut other = World::new(
        SEED + 1,
        config,
        Judge::new(SEED + 1, vec![Expect::Receives { at: 0, by: ms(1_000) }]),
        Memory::Checked,
    );
    other.spawn(server);
    other.spawn(sender);
    assert!(other.run().trace != first.trace, "another seed, another run");
}

#[test]
#[should_panic(expected = "process 0 requires policy deadline 10000000 ns")]
fn a_keep_time_after_the_last_word_fails_the_world_naming_the_process() {
    let mut world = World::new(SEED, Config::calm(), Judge::new(SEED, Vec::new()), Memory::Checked);
    world.spawn(|| Script::new(ms(10), &[]));
    let _outcome = world.run();
}

#[test]
fn a_world_that_ends_by_itself_passes() {
    let mut world = World::new(SEED, Config::calm(), Judge::new(SEED, Vec::new()), Memory::Checked);
    world.spawn(|| Script::new(skein_lib::Time::ZERO, &[]));
    assert_eq!(world.run().end, skein_lib::Time::ZERO);
}

#[test]
fn a_scenario_that_names_a_later_last_word_may_wait_for_it() {
    let referee = skein_world_tests::Later(skein_world::LastWord::After(ms(10)));
    let mut world = World::new(SEED, Config::calm(), referee, Memory::Checked);
    world.spawn(|| Script::new(ms(10), &[]));
    assert_eq!(world.run().end, ms(10));
}

#[test]
#[should_panic(expected = "process 0 requires policy deadline 10000000 ns")]
fn a_named_later_last_word_still_rejects_policy_deadlines_after_it() {
    let referee = skein_world_tests::Later(skein_world::LastWord::After(ms(5)));
    let mut world = World::new(SEED, Config::calm(), referee, Memory::Checked);
    world.spawn(|| Script::new(ms(10), &[]));
    let _outcome = world.run();
}
