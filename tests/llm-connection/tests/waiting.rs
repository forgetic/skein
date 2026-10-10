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
        world.events.iter().any(|event| event.contains("call: Token(10)")
            && event.contains("Unsent")
            && event.contains("TimedOut { phase: Whole }")),
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

fn memory_request(token: u64) -> skein_llm::Call {
    let mut call = skein_llm_world::call(token);
    call.endpoint = skein_llm::Endpoint::codex();
    call.prompt.instructions = b"close-world".as_slice().into();
    call
}

fn request_reservation() -> u64 {
    let mut bounds = skein_llm_world::limits();
    let input = memory_request(7);
    bounds.http.request = skein_llm::client::request_head(
        &input.endpoint,
        &skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
        &bounds,
    )
    .expect("the world request fits its declared endpoint");
    let measured = skein_llm::client::measure(&input.prompt, &input.credential, &input.endpoint, &bounds)
        .expect("the world request fits its declared endpoint");
    skein_llm::client::reservation(measured, &bounds).expect("the world request fits its declared endpoint")
}

#[test]
fn a_full_memory_pool_makes_a_call_wait_for_anothers_terminal() {
    assert_replays(101, 103, |seed| {
        let reservation = request_reservation();
        let memory = 2 * reservation;
        let mut world = World::empty_with_memory(seed, 3, 3, 1, memory);
        for token in 7..10 {
            world.start(token);
        }
        assert_eq!(world.component.reserved(), memory, "two calls fill the pool exactly");
        world.until(Point::Head);
        assert_eq!(world.connections(), 2, "the third call holds no prepared connection");
        assert_eq!(world.judge.completed, 0);
        world.request(Request::Close);
        world.finish();
        assert_eq!(world.judge.completed, 3);
        assert_eq!(world.judge.refused, 0);
        assert_eq!(world.component.reserved(), 0);
        (world.trace, world.events)
    });
}

#[test]
fn a_small_call_does_not_pass_an_older_large_one_in_the_memory_pool() {
    assert_replays(107, 109, |seed| {
        let reservation = request_reservation();
        let mut world = World::empty_with_memory(seed, 3, 3, 2, 2 * reservation);
        world.start(7);
        let mut larger = memory_request(8);
        // The cue stays literal; additional history enlarges only the measured request.
        let mut messages = larger.prompt.messages.to_vec();
        messages.push(skein_llm::Message {
            role: skein_llm::Role::User,
            content: Box::new([skein_llm::Block::Text { text: vec![b'x'; 1024].into(), replay: None }]),
        });
        larger.prompt.messages = messages.into_boxed_slice();
        world.start_prompt(8, 1, larger.prompt, larger.credential, Deadlines::none());
        world.start(9);
        assert_eq!(world.component.reserved(), reservation, "a younger fitting call cannot bypass the large wait");
        world.until(Point::Head);
        assert_eq!(world.connections(), 1);
        world.request(Request::Close);
        world.finish();
        let terminals: Vec<_> = world.events.iter().filter(|event| event.starts_with("Completed")).collect();
        assert_eq!(terminals.len(), 3);
        for (event, token) in terminals.into_iter().zip([7, 8, 9]) {
            assert!(event.contains(&format!("call: Token({token})")), "{event}");
        }
        assert_eq!(world.component.reserved(), 0);
        (world.trace, world.events)
    });
}

#[test]
fn cancel_and_whole_expiry_release_both_waiting_stages() {
    for token in [8, 9] {
        for cancel in [false, true] {
            let reservation = request_reservation();
            let mut world = World::empty_with_memory(113, 3, 1, 1, 2 * reservation);
            world.start(7);
            for waiting in [8, 9] {
                let deadlines = if waiting == token && !cancel {
                    Deadlines { whole: Some(Duration::from_secs(1)), ..Deadlines::none() }
                } else {
                    Deadlines::none()
                };
                world.start_at(waiting, 0, deadlines);
            }
            assert_eq!(world.component.reserved(), 2 * reservation);
            if cancel {
                world.request(Request::Cancel { call: Token::new(token) });
            } else {
                world.advance(Time::from_nanos(1_000_000_000));
            }
            world.request(Request::Close);
            world.finish();
            assert_eq!(world.judge.completed, 2);
            assert_eq!(world.judge.cancelled, u32::from(cancel));
            assert_eq!(world.judge.failed, u32::from(!cancel));
            assert_eq!(world.component.reserved(), 0);
            if !cancel {
                assert!(
                    world.events.iter().any(|event| event.contains(&format!("call: Token({token})"))
                        && event.contains("Unsent")
                        && event.contains("TimedOut { phase: Whole }")),
                    "{:?}",
                    world.events
                );
            }
        }
    }
}

#[test]
fn abort_cancels_memory_and_connection_waits_without_granting_again() {
    let reservation = request_reservation();
    let mut world = World::empty_with_memory(127, 3, 1, 1, 2 * reservation);
    for token in 7..10 {
        world.start(token);
    }
    world.request(Request::Abort);
    world.finish();
    assert_eq!(world.judge.cancelled, 3);
    assert_eq!(world.judge.completed, 0);
    assert_eq!(world.component.reserved(), 0);
}
