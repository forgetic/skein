//! Appended bytes and deadline settlement over the shared simulated world.

use skein_io_world::append::{CONTENTS, Evidence, Story, simulated};
use skein_lib::{Duration, Time};
use skein_sim::{Config, Faults, Summary};
use skein_world::Memory;

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

fn complete(
    seed: u64,
    config: Config,
    story: Story,
) -> skein_world::Outcome<skein_io_world::append::Writer, skein_io_world::append::AppendFiles> {
    let mut outcome = simulated(seed, config, story, Memory::Checked);
    assert_eq!(outcome.machine.bytes(), CONTENTS);
    assert_eq!(outcome.procs[0].failures(), 0);
    assert_eq!(outcome.procs[0].events.last(), Some(&Evidence::Closed));
    assert!(outcome.heap.as_ref().expect("checked owner").iter().all(|(peak, bound)| peak <= bound));
    outcome.machine.finish();
    outcome
}

#[test]
fn sends_within_the_room_granted_land_whole_and_in_order() {
    complete(7, Config::calm(), Story::Finish);
}

#[test]
fn short_appends_are_continued_from_their_count() {
    let config = Config { faults: Faults { short_write: 1000, ..Faults::NONE }, ..Config::calm() };
    let outcome = complete(11, config, Story::Finish);
    assert!(
        outcome.trace.iter().any(|entry| matches!(
            entry.event,
            skein_sim::Event::Submit { kind: Summary::Append { from: 1.., .. }, .. }
        ))
    );
}

#[test]
fn a_close_flushes_what_is_queued() {
    complete(17, Config::calm(), Story::Close);
}

#[test]
fn a_hung_append_is_given_up_at_the_write_deadline_and_fails_the_stream_once() {
    let config = Config { faults: Faults { hung: 1000, ..Faults::NONE }, ..Config::calm() };
    let mut outcome = simulated(19, config, Story::WriteDeadline, Memory::Checked);
    assert_eq!(outcome.end, Time::ZERO.saturating_add(Duration::from_millis(10)));
    assert_eq!(outcome.procs[0].failures(), 1);
    assert_eq!(outcome.machine.bytes(), b"prefix:");
    outcome.machine.finish();
}

#[test]
fn a_hung_append_while_closing_is_given_up_at_the_close_deadline() {
    let config = Config { faults: Faults { hung: 1000, ..Faults::NONE }, ..Config::calm() };
    let mut outcome = simulated(23, config, Story::Close, Memory::Checked);
    assert_eq!(outcome.end, Time::ZERO.saturating_add(Duration::from_millis(100)));
    assert_eq!(outcome.procs[0].failures(), 0);
    assert_eq!(outcome.machine.bytes(), b"prefix:");
    outcome.machine.finish();
}

#[test]
fn abort_settles_without_a_later_append_or_an_extra_terminal() {
    let config = Config { faults: Faults { hung: 1000, ..Faults::NONE }, ..Config::calm() };
    let mut outcome = simulated(29, config, Story::Abort, Memory::Checked);
    assert_eq!(outcome.procs[0].failures(), 0);
    assert_eq!(outcome.machine.bytes(), b"prefix:");
    outcome.machine.finish();
}
