//! Attaining caller-menu, script-wrapper and delayed-slot ownership controls.

use std::mem::size_of;

use skein_fake_llm_domain::api::{Error, Finish, InvalidInput, Line, Menu, Message, Part, Query, Role, Script, Turn};
use skein_fake_llm_domain::{Domain, Event, MAX_OUT, Request, fire, step, worst_case};
use skein_heap::{Counting, Meter};
use skein_lib::{Duration, Env, Queue, ReplyTo, Time, Token, Wall};

#[global_allocator]
static HEAP: Counting = Counting;

const ITEMS: usize = 128;

fn scripts() -> Box<[Script]> {
    Box::new([Script {
        cue: b"cue".as_slice().into(),
        turns: Box::new([Turn {
            lines: (0..ITEMS).map(|_| Line::Text { text: Box::new([]) }).collect(),
            finish: Finish::Stop,
            tokens: 1,
        }]),
    }])
}

fn menu() -> Menu {
    Menu {
        arguments: (0..ITEMS).map(|_| Box::<[u8]>::from(b"{}".as_slice())).collect(),
        invalid: (0..ITEMS)
            .map(|_| InvalidInput { name: Some(b"unknown".as_slice().into()), arguments: b"{".as_slice().into() })
            .collect(),
    }
}

fn query() -> Query {
    Query {
        model: Box::new([]),
        system: b"cue".as_slice().into(),
        tools: Box::new([]),
        messages: Box::new([Message { role: Role::User, parts: Box::new([Part::Text { text: Box::new([]) }]) }]),
        max_tokens: 10,
    }
}

fn caps() -> skein_fake_llm_domain::Config {
    let scripts = size_of::<Script>() + 3 + size_of::<Turn>() + ITEMS * size_of::<Line>();
    let menu = ITEMS * (size_of::<Box<[u8]>>() + 2 + size_of::<InvalidInput>() + 7 + 1);
    skein_fake_llm_domain::Config {
        calls: 4,
        script_bytes: u32::try_from(scripts + menu).expect("bounded exact aggregate"),
        answer_bytes: u32::try_from(ITEMS * size_of::<Part>()).expect("bounded exact wrapper answer"),
        query_bytes: u32::try_from(3 + size_of::<Message>() + size_of::<Part>()).expect("bounded exact query"),
        latency_min: Duration::from_secs(1),
        latency_max: Duration::from_secs(1),
        ..skein_llm_world::fake::config()
    }
}

#[test]
fn exact_joint_menu_script_cap_and_all_delayed_slots_include_empty_part_wrappers() {
    let config = caps();
    let bound = worst_case(&config).expect("checked bound");
    let mut out = Queue::with_capacity(MAX_OUT);
    let meter = Meter::new();
    meter.start();
    let mut domain = Domain::configured(&config, 7, scripts(), menu()).expect("exact joint script/menu cap");
    let measured = meter.end();
    meter.check(measured, bound, &"all retained script/menu wrappers");
    let mut env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config };
    for token in 1..=u64::from(config.calls) {
        meter.start();
        step(&mut domain, &env, Event::Call { reply_to: ReplyTo::new(Token::new(token)), query: query() }, &mut out);
        let measured = meter.end();
        assert!(out.pop().is_none(), "every admitted answer waits for its actual timer");
        meter.check(measured, bound, &"empty-part wrapper scratch plus all delayed slots");
    }
    assert_eq!(domain.calls(), config.calls, "every slab and alarm slot is live");
    meter.start();
    step(&mut domain, &env, Event::Call { reply_to: ReplyTo::new(Token::new(99)), query: query() }, &mut out);
    let measured = meter.end();
    let Request::Reply { result, .. } = out.pop().expect("full slab refuses once");
    assert_eq!(result, Err(Error::Overloaded));
    meter.check(measured, bound, &"one-over live-slot refusal");
    env.now = Time::from_nanos(1_000_000_000);
    let mut terminals = 0_u32;
    while domain.is_due(env.now) {
        meter.start();
        fire(&mut domain, &env, &mut out);
        let measured = meter.end();
        let Request::Reply { result, .. } = out.pop().expect("one actual delayed terminal");
        let answer = result.expect("exact maximum answer cap admitted");
        assert_eq!(answer.parts.len(), ITEMS, "every empty Part wrapper reached output");
        assert!(answer.parts.iter().all(|part| matches!(part, Part::Text { text } if text.is_empty())));
        drop(answer);
        meter.check(measured, bound, &"actual delayed terminal output");
        terminals += 1;
    }
    assert_eq!(terminals, config.calls);
    domain.reclaim();
    assert_eq!(domain.calls(), 0);
    drop(domain);
    assert_eq!(meter.held(), 0, "all owned wrappers reclaimed after actual terminals");

    let tight = skein_fake_llm_domain::Config { script_bytes: config.script_bytes - 1, ..config };
    assert!(Domain::configured(&tight, 7, scripts(), menu()).is_err(), "one-byte tighter aggregate rejects");
    let mut env = Env {
        now: Time::ZERO,
        wall: Wall::EPOCH,
        limits: skein_fake_llm_domain::Config { answer_bytes: config.answer_bytes - 1, ..config },
    };
    let mut domain = Domain::configured(&env.limits, 7, scripts(), menu()).expect("same admitted input");
    step(&mut domain, &env, Event::Call { reply_to: ReplyTo::new(Token::new(1)), query: query() }, &mut out);
    env.now = Time::from_nanos(1_000_000_000);
    fire(&mut domain, &env, &mut out);
    let Request::Reply { result, .. } = out.pop().expect("actual refusal terminal");
    assert_eq!(result, Err(Error::ContextTooLong), "empty wrappers count toward answer admission");
}
