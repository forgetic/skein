use super::*;
use skein_lib::{Duration, Env, List, Queue, Time, Wall, bytes};
use skein_oauth::{ClientLimits, SavedToken};

fn limits() -> Limits {
    Limits {
        accounts: 2,
        refresh_lead: Duration::from_secs(10),
        client: ClientLimits {
            document: skein_oauth::Limits {
                document_bytes: 1024,
                string_bytes: 256,
                token_bytes: 64,
                client_bytes: 64,
                detail_bytes: 64,
                record_bytes: 256,
                depth: 8,
                tokens: 64,
            },
            uri_bytes: 256,
            scope_bytes: 64,
            state_bytes: 64,
            code_bytes: 64,
            url_bytes: 1024,
            request_bytes: 1024,
            sign_in_time: Duration::from_secs(120),
            request_time: Duration::from_secs(10),
            backoff_base: Duration::from_secs(2),
            backoff_ceiling: Duration::from_secs(8),
            max_attempts: 3,
        },
    }
}
fn setup() -> (Component, Env<Limits>, Queue<Event>) {
    let limits = limits();
    let mut accounts = List::with_capacity(2);
    accounts.push(Account::HandedIn).expect("account");
    accounts.push(Account::HandedIn).expect("account");
    let component = Component::new(accounts, &limits, 1).expect("component");
    (component, Env { now: Time::ZERO, wall: Wall::EPOCH, limits }, Queue::with_capacity(MAX_OUT_DOWN.above))
}
fn tests_record(generation: u64) -> SavedToken {
    SavedToken {
        key: 0,
        generation,
        access_token: bytes::copy_of(b"access"),
        refresh_token: None,
        metadata: None,
        expires_at: Wall::from_nanos(30_000_000_000),
    }
}
fn hand_in(component: &mut Component, env: &Env<Limits>, out: &mut Queue<Event>, generation: u64) {
    component.down(env, Request::HandIn { account: 0, record: tests_record(generation) }, out);
}
fn assert_event(event: Option<Event>, expected: Event) {
    assert!(event == Some(expected), "the request has its expected typed outcome");
}
fn assert_granted(event: Option<Event>, generation: u64, seconds: u64) {
    assert_event(
        event,
        Event::Granted {
            account: 0,
            token: bytes::copy_of(b"access"),
            generation,
            valid: Duration::from_secs(seconds),
        },
    );
}
fn assert_expired(event: Option<Event>) {
    assert_event(event, Event::Failed { account: 0, ends: Ends::Grant, failure: Failure::Expired });
}
fn assert_refused(event: Option<Event>, account: u32, asked: Asked, why: Refusal) {
    assert_event(event, Event::Refused { account, asked, why });
}

#[test]
fn a_handed_in_record_is_lent_while_it_is_valid() {
    let (mut component, env, mut out) = setup();
    hand_in(&mut component, &env, &mut out, 1);
    assert!(out.is_empty());
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    assert_granted(out.pop(), 1, 30);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    assert_refused(out.pop(), 0, Asked::Grant, Refusal::Held);
    component.down(&env, Request::Release { account: 0 }, &mut out);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    assert_granted(out.pop(), 1, 30);
}

#[test]
fn an_access_only_record_is_announced_expiring_once_and_fails_at_expiry() {
    let (mut component, mut env, mut out) = setup();
    hand_in(&mut component, &env, &mut out, 1);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    drop(out.pop());
    assert_eq!(component.next_deadline(), Some(Time::from_nanos(20_000_000_000)));
    env.now = Time::from_nanos(20_000_000_000);
    // Wall jumps never extend a running grant's deadline.
    env.wall = Wall::from_nanos(100_000_000_000);
    component.fire(&env, &mut out);
    assert_event(out.pop(), Event::Expiring { account: 0, generation: 1 });
    component.fire(&env, &mut out);
    assert!(out.is_empty());
    assert_eq!(component.next_deadline(), Some(Time::from_nanos(30_000_000_000)));
    env.now = Time::from_nanos(30_000_000_000);
    component.fire(&env, &mut out);
    assert_expired(out.pop());
    component.fire(&env, &mut out);
    assert!(out.is_empty());
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    assert_expired(out.pop());
}

#[test]
fn a_rejected_record_fails_until_a_newer_one_is_handed_in() {
    let (mut component, mut env, mut out) = setup();
    hand_in(&mut component, &env, &mut out, 1);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    drop(out.pop());
    component.down(&env, Request::Rejected { account: 0, generation: 0 }, &mut out);
    assert!(out.is_empty());
    component.down(&env, Request::Rejected { account: 0, generation: 1 }, &mut out);
    assert_expired(out.pop());
    hand_in(&mut component, &env, &mut out, 1);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    assert_expired(out.pop());
    env.now = Time::from_nanos(1_000_000_000);
    env.wall = Wall::from_nanos(1_000_000_000);
    hand_in(&mut component, &env, &mut out, 2);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    assert_granted(out.pop(), 2, 29);
}

#[test]
fn a_newer_record_announces_the_new_generation_while_held() {
    let (mut component, env, mut out) = setup();
    hand_in(&mut component, &env, &mut out, 1);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    drop(out.pop());
    hand_in(&mut component, &env, &mut out, 0);
    assert!(out.is_empty());
    hand_in(&mut component, &env, &mut out, 2);
    assert_granted(out.pop(), 2, 30);
}

#[test]
fn a_record_with_a_refresh_token_or_past_its_bounds_is_refused() {
    let (mut component, env, mut out) = setup();
    let mut record = tests_record(1);
    record.refresh_token = Some(bytes::copy_of(b"refresh"));
    component.down(&env, Request::HandIn { account: 0, record }, &mut out);
    assert_refused(out.pop(), 0, Asked::HandIn, Refusal::NotAccessOnly);
    let mut record = tests_record(1);
    record.access_token = bytes::copy_of(&[b'x'; 65]);
    component.down(&env, Request::HandIn { account: 0, record }, &mut out);
    assert_refused(out.pop(), 0, Asked::HandIn, Refusal::Record(skein_oauth::DecodeError::TooLarge));
}

#[test]
fn close_and_abort_settle_once_clear_deadlines_and_refuse_new_work_as_closed() {
    for close in [Request::Close, Request::Abort] {
        let (mut component, env, mut out) = setup();
        hand_in(&mut component, &env, &mut out, 1);
        component.down(&env, close, &mut out);
        assert_event(out.pop(), Event::Closed);
        assert_eq!(component.next_deadline(), None);
        component.down(&env, Request::Close, &mut out);
        component.down(&env, Request::Abort, &mut out);
        component.fire(&env, &mut out);
        assert!(out.is_empty());
        component.down(&env, Request::Grant { account: 9 }, &mut out);
        assert_refused(out.pop(), 9, Asked::Grant, Refusal::Closed);
        hand_in(&mut component, &env, &mut out, 2);
        assert_refused(out.pop(), 0, Asked::HandIn, Refusal::Closed);
    }
}

#[test]
fn a_missing_account_and_missing_record_have_typed_terminals() {
    let (mut component, env, mut out) = setup();
    component.down(&env, Request::Grant { account: 3 }, &mut out);
    assert_refused(out.pop(), 3, Asked::Grant, Refusal::Account);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    assert_expired(out.pop());
}

#[test]
fn a_released_record_still_announces_its_lead_without_an_expiry_failure() {
    let (mut component, mut env, mut out) = setup();
    hand_in(&mut component, &env, &mut out, 1);
    component.down(&env, Request::Grant { account: 0 }, &mut out);
    drop(out.pop());
    component.down(&env, Request::Release { account: 0 }, &mut out);
    env.now = Time::from_nanos(20_000_000_000);
    component.fire(&env, &mut out);
    assert_event(out.pop(), Event::Expiring { account: 0, generation: 1 });
    assert_eq!(component.next_deadline(), None);
}
