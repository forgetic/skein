use skein_lib::{Duration, Time, Token};
use skein_llm_connection::{Deadlines, Request};
use skein_llm_connection_world::world::{Point, World, run};
use skein_world::domain::assert_replays;

#[test]
fn more_calls_to_one_endpoint_than_its_connections_each_complete_after_waiting() {
    assert_replays(17, 19, |seed| {
        let mut world = World::configured(seed, 4, 1, 1);
        world.until(Point::Idle);
        assert_eq!(world.connections(), 1);
        world.request(Request::Close);
        world.finish();
        assert_eq!(world.judge.completed, 4);
        assert_eq!(world.judge.refused, 0);
        (world.trace, world.events)
    });
}

#[test]
fn a_wait_past_its_whole_deadline_fails_unsent() {
    let mut world = World::configured(23, 3, 1, 1);
    world.request(Request::Cancel { call: Token::new(9) });
    world.start_at(10, 0, Deadlines { whole: Some(Duration::from_secs(1)), ..Deadlines::none() });
    world.advance(Time::from_nanos(1_000_000_000));
    world.request(Request::Close);
    world.finish();
    assert_eq!(world.judge.failed, 1);
    assert!(
        world
            .events
            .iter()
            .any(|event| event.contains("call: Token(10)") && event.contains("Unsent") && event.contains("TimedOut")),
        "{:?}",
        world.events
    );
    assert_eq!(world.judge.completed, 2);
}

#[test]
fn a_cancel_while_waiting_answers_cancelled_at_once() {
    let mut world = World::configured(29, 2, 1, 1);
    world.request(Request::Cancel { call: Token::new(8) });
    world.tick();
    assert_eq!(world.judge.cancelled, 1);
    world.request(Request::Close);
    world.finish();
    assert_eq!(world.judge.completed, 1);
}

#[test]
fn a_call_to_another_endpoint_is_not_held_back_by_a_full_endpoint() {
    let mut world = World::configured(31, 3, 1, 2);
    world.request(Request::Cancel { call: Token::new(9) });
    world.tick();
    world.start_at(10, 1, Deadlines::none());
    for _ in 0..10 {
        world.tick();
    }
    assert_eq!(world.connections(), 2, "endpoint one connects while endpoint zero still waits");
    assert_eq!(world.judge.completed, 0);
    world.request(Request::Close);
    world.finish();
    assert_eq!(world.judge.completed, 3);
}

#[test]
fn a_close_serves_the_calls_already_waiting() {
    assert_replays(37, 41, |seed| run(seed, Point::Waiting, false));
}

#[test]
fn an_abort_cancels_the_calls_waiting() {
    assert_replays(43, 47, |seed| {
        let mut world = World::configured(seed, 3, 1, 1);
        world.until(Point::Head);
        world.request(Request::Abort);
        world.finish();
        assert_eq!(world.judge.cancelled, 3);
        assert_eq!(world.judge.completed, 0);
        (world.trace, world.events)
    });
}

#[test]
fn an_idle_connection_of_another_endpoint_is_evicted_to_make_room() {
    assert_replays(61, 67, |seed| {
        let mut world = World::configured(seed, 2, 2, 2);
        world.until(Point::Idle);
        assert_eq!(world.connections(), 2);
        world.start_at(10, 1, Deadlines::none());
        for _ in 0..20_000 {
            world.tick();
            if world.judge.completed == 3 {
                break;
            }
        }
        assert_eq!(world.judge.completed, 3);
        assert_eq!(world.judge.refused, 0);
        world.request(Request::Close);
        world.finish();
        (world.trace, world.events)
    });
}

#[test]
fn a_new_call_does_not_pass_an_older_waiter_after_a_record_is_cancelled() {
    let mut world = World::configured(71, 3, 1, 1);
    world.request(Request::Cancel { call: Token::new(8) });
    world.start_at(10, 0, Deadlines::none());
    world.request(Request::Close);
    world.finish();
    let completions: Vec<_> = world.events.iter().filter(|event| event.starts_with("Completed")).collect();
    assert_eq!(completions.len(), 3);
    for (event, token) in completions.iter().zip([7, 9, 10]) {
        assert!(event.contains(&format!("call: Token({token})")), "{event}");
    }
}

#[test]
fn two_endpoints_with_different_client_limits_each_complete_and_replay() {
    assert_replays(73, 79, |seed| {
        let mut world = World::configured(seed, 2, 1, 2);
        world.request(Request::Cancel { call: Token::new(8) });
        world.start_at(10, 1, Deadlines::none());
        world.request(Request::Close);
        world.finish();
        assert_eq!(world.judge.completed, 2);
        assert_eq!(world.judge.cancelled, 1);
        assert_eq!(world.connections(), 2);
        (world.trace, world.events)
    });
}
