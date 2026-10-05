use skein_browser::boundary::{Below, Down, Event, Expect, Query, Refusal, Request, Trouble};
use skein_browser::wire::decode::Document;
use skein_browser::{down, fire, up};
use skein_lib::stream;
use skein_lib::{Time, Token};

use crate::world::World;

#[test]
fn lifecycle_find_and_press() {
    let mut world = World::new();
    world.start();
    world.open();
    world.ask(Request::Find {
        page: Token::new(3),
        op: Token::new(4),
        query: Query {
            role: b"button".to_vec().into_boxed_slice(),
            name: b"Save".to_vec().into_boxed_slice(),
            within: None,
            boxes: true,
        },
    });
    match world.above.pop() {
        Some(Event::Found { op, seen, more }) => {
            assert_eq!(op, Token::new(4));
            assert_eq!(more, 0);
            assert_eq!(seen.len(), 1);
            assert_eq!(seen.get(0).expect("button").node, 7);
            assert!(seen.get(0).expect("button").rect.is_some());
        }
        other => panic!("expected Found, got {other:?}"),
    }
    world.ask(Request::Press { page: Token::new(3), op: Token::new(5), node: 7 });
    assert!(matches!(world.above.pop(), Some(Event::Done { op }) if op == Token::new(5)));
    let mouse = world
        .sent
        .iter()
        .filter(|command| command.windows(b"Input.dispatchMouseEvent".len()).any(|w| w == b"Input.dispatchMouseEvent"))
        .count();
    assert_eq!(mouse, 3);
}

#[test]
fn await_polls_until_present() {
    let mut world = World::new();
    world.start();
    world.open();
    world.ax_visible = false;
    world.ask(Request::Await {
        page: Token::new(3),
        op: Token::new(6),
        query: Query {
            role: b"button".to_vec().into_boxed_slice(),
            name: b"Save".to_vec().into_boxed_slice(),
            within: None,
            boxes: false,
        },
        expect: Expect::Present,
        within: skein_lib::Duration::from_millis(200),
    });
    assert!(world.above.is_empty());
    world.ax_visible = true;
    world.env.now = Time::from_nanos(50_000_000);
    fire(&mut world.browser, &world.env, world.env.now, &mut world.above, &mut world.below);
    world.drive();
    assert!(matches!(world.above.pop(), Some(Event::Met { op, .. }) if op == Token::new(6)));
}

#[test]
fn crash_fails_operation_and_closes_entities() {
    let mut world = World::new();
    world.start();
    world.open();
    world.ax_visible = false;
    world.ask(Request::Await {
        page: Token::new(3),
        op: Token::new(7),
        query: Query {
            role: b"missing".to_vec().into_boxed_slice(),
            name: b"No".to_vec().into_boxed_slice(),
            within: None,
            boxes: false,
        },
        expect: Expect::Present,
        within: skein_lib::Duration::from_secs(1),
    });
    up(&mut world.browser, &world.env, Below::Replies(stream::Up::End), &mut world.above, &mut world.below);
    let mut events = Vec::new();
    while let Some(event) = world.above.pop() {
        events.push(event);
    }
    for _ in 0..4 {
        fire(&mut world.browser, &world.env, world.env.now, &mut world.above, &mut world.below);
        while let Some(event) = world.above.pop() {
            events.push(event);
        }
    }
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::Refused { op, why: Refusal::Crashed } if *op == Token::new(7)))
    );
    assert!(events.iter().any(|event| matches!(event, Event::Closed { owner } if *owner == Token::new(1))));
}

#[test]
fn await_misses_at_its_deadline() {
    let mut world = World::new();
    world.start();
    world.open();
    world.ax_visible = false;
    world.ask(Request::Await {
        page: Token::new(3),
        op: Token::new(8),
        query: Query {
            role: b"button".to_vec().into_boxed_slice(),
            name: b"Save".to_vec().into_boxed_slice(),
            within: None,
            boxes: false,
        },
        expect: Expect::Present,
        within: skein_lib::Duration::from_millis(100),
    });
    world.env.now = Time::from_nanos(100_000_000);
    fire(&mut world.browser, &world.env, world.env.now, &mut world.above, &mut world.below);
    world.drive();
    assert!(
        matches!(world.above.pop(), Some(Event::Missed { op, seen, more: 0 }) if op == Token::new(8) && seen.is_empty())
    );
}

#[test]
fn trouble_is_reported_without_judgment() {
    let mut world = World::new();
    world.start();
    world.open();
    let messages: [&[u8]; 3] = [
        b"{\"method\":\"Runtime.exceptionThrown\",\"sessionId\":\"s1\",\"params\":{\"exceptionDetails\":{\"text\":\"boom\"}}}\0",
        b"{\"method\":\"Runtime.consoleAPICalled\",\"sessionId\":\"s1\",\"params\":{\"type\":\"error\",\"args\":[{\"value\":\"bad\"}]}}\0",
        b"{\"method\":\"Log.entryAdded\",\"sessionId\":\"s1\",\"params\":{\"entry\":{\"level\":\"error\",\"text\":\"blocked\"}}}\0",
    ];
    for message in messages {
        up(
            &mut world.browser,
            &world.env,
            Below::Replies(stream::Up::Bytes(message.into())),
            &mut world.above,
            &mut world.below,
        );
    }
    assert!(
        matches!(world.above.pop(), Some(Event::Trouble { trouble: Trouble::Exception, text, .. }) if text.as_ref() == b"boom")
    );
    assert!(
        matches!(world.above.pop(), Some(Event::Trouble { trouble: Trouble::Console, text, .. }) if text.as_ref() == b"bad")
    );
    assert!(
        matches!(world.above.pop(), Some(Event::Trouble { trouble: Trouble::Log, text, .. }) if text.as_ref() == b"blocked")
    );
    world.ask(Request::Screenshot { page: Token::new(3), op: Token::new(9) });
    assert!(
        matches!(world.above.pop(), Some(Event::Screenshot { op, png }) if op == Token::new(9) && png.starts_with(b"\x89PNG"))
    );
}

#[test]
fn press_refuses_disabled_hidden_and_covered_without_mouse() {
    for (disabled, hidden, covered, why) in [
        (true, false, false, Refusal::Disabled),
        (false, true, false, Refusal::Hidden),
        (false, false, true, Refusal::Covered),
    ] {
        let mut world = World::new();
        world.start();
        world.open();
        world.disabled = disabled;
        world.hidden = hidden;
        world.covered = covered;
        world.ask(Request::Press { page: Token::new(3), op: Token::new(10), node: 7 });
        assert!(
            matches!(world.above.pop(), Some(Event::Refused { op, why: observed }) if op == Token::new(10) && observed == why)
        );
        assert!(!world.sent.iter().any(|command| {
            command.windows(b"Input.dispatchMouseEvent".len()).any(|window| window == b"Input.dispatchMouseEvent")
        }));
    }
}

#[test]
fn unanswered_command_reaches_answer_deadline() {
    let mut world = World::new();
    world.start();
    world.open();
    down(
        &mut world.browser,
        &world.env,
        Request::Snapshot { page: Token::new(3), op: Token::new(11) },
        &mut world.above,
        &mut world.below,
    );
    let id = world
        .below
        .iter()
        .find_map(|record| match record {
            Down::Commands(stream::Down::Send(bytes)) => {
                let document =
                    Document::parse(&bytes[..bytes.len() - 1], world.env.limits.command).expect("valid command");
                document.root().get(b"id").and_then(|id| id.u64())
            }
            _ => None,
        })
        .expect("snapshot command was sent");
    world.env.now = Time::from_nanos(world.env.limits.answer.as_nanos());
    fire(&mut world.browser, &world.env, world.env.now, &mut world.above, &mut world.below);
    assert!(matches!(world.above.pop(), Some(Event::Refused { op, why: Refusal::Timeout }) if op == Token::new(11)));
    let late = format!("{{\"id\":{id},\"result\":{{}}}}\0");
    up(
        &mut world.browser,
        &world.env,
        Below::Replies(stream::Up::Bytes(late.into_bytes().into_boxed_slice())),
        &mut world.above,
        &mut world.below,
    );
    assert!(world.above.is_empty(), "a late reply cannot answer twice or close the browser");
}
