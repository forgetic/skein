//! Close drains every live phase and says Closed once, after io settles.

use skein_llm_connection::{Event, Request};
use skein_llm_connection_world::world::{Judge, Point, World, run};
use skein_world::domain::assert_replays;

#[test]
fn a_close_while_connecting_lets_the_call_end_and_says_closed_last() {
    assert_replays(7, 8, |seed| run(seed, Point::Connecting, false));
}

#[test]
fn a_close_before_the_head_lets_the_call_end_and_says_closed_last() {
    assert_replays(7, 8, |seed| run(seed, Point::Head, false));
}

#[test]
fn a_close_while_streaming_lets_the_call_end_and_says_closed_last() {
    assert_replays(7, 8, |seed| run(seed, Point::Streaming, false));
}

#[test]
fn a_close_while_draining_does_not_wait_for_the_body_end() {
    assert_replays(7, 8, |seed| run(seed, Point::Draining, false));
}

#[test]
fn an_idle_connection_closes_at_once_on_the_close() {
    assert_replays(7, 8, |seed| run(seed, Point::Idle, false));
}

#[test]
fn a_close_while_closing_preserves_the_ordinary_terminal() {
    assert_replays(7, 8, |seed| run(seed, Point::Closing, false));
}

#[test]
fn a_start_after_the_close_is_refused_as_closed() {
    assert_replays(7, 8, |seed| {
        let mut world = World::new(seed, 1);
        world.request(Request::Close);
        world.start(99);
        world.finish();
        assert_eq!(world.judge.refused, 1);
        assert_eq!(world.judge.completed, 1);
        (world.trace, world.events)
    });
}

#[test]
fn an_abort_after_a_close_aborts_what_is_still_closing() {
    assert_replays(7, 8, |seed| run(seed, Point::Closing, true));
}

#[test]
fn a_close_after_an_abort_is_inert() {
    assert_replays(7, 8, |seed| {
        let mut world = World::new(seed, 2);
        world.until(Point::Head);
        world.delay_close = true;
        world.request(Request::Abort);
        world.request(Request::Close);
        world.finish();
        assert_eq!(world.judge.cancelled, 2);
        assert_eq!(world.judge.completed, 0);
        (world.trace, world.events)
    });
}

#[test]
#[should_panic(expected = "nothing follows Closed")]
fn the_referee_rejects_a_second_closed() {
    let mut judge = Judge::default();
    judge.observe(&Event::Closed);
    judge.observe(&Event::Closed);
}

#[test]
#[should_panic(expected = "nothing follows Closed")]
fn the_referee_rejects_an_event_after_closed() {
    let mut judge = Judge::default();
    judge.observe(&Event::Closed);
    judge.observe(&Event::Cancelled { call: skein_lib::Token::new(7) });
}

#[test]
#[should_panic(expected = "every call has its terminal")]
fn the_referee_rejects_a_call_without_a_terminal() {
    let mut judge = Judge::default();
    judge.start(skein_lib::Token::new(7));
    judge.observe(&Event::Closed);
}

#[test]
#[should_panic(expected = "no past-due wake")]
fn the_referee_rejects_a_past_due_deadline_with_only_io_settlement_left() {
    Judge::default().settlement(false, Some(skein_lib::Time::ZERO), skein_lib::Time::ZERO);
}

#[test]
#[should_panic(expected = "not runnable work")]
fn the_referee_rejects_work_with_only_io_settlement_left() {
    Judge::default().settlement(true, None, skein_lib::Time::ZERO);
}
