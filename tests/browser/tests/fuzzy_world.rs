//! Seeded scenarios against a faulting in-memory CDP peer.

use skein_browser::boundary::{Event, Expect, Query, Refusal, Request};
use skein_browser_world::referee::Referee;
use skein_browser_world::world::{Fault, World};
use skein_lib::{Time, Token};

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn observe(world: &mut World, referee: &mut Referee) -> Vec<Event> {
    for bytes in &world.sent {
        referee.command(bytes);
    }
    let mut events = Vec::new();
    while let Some(event) = world.above.pop() {
        referee.event(&event, u32::from(world.ax_visible));
        events.push(event);
    }
    events
}

#[test]
fn varied_pages_and_faults_hold_the_contract() {
    let mut faults_seen = [false; 7];
    for case in 0..192_u64 {
        let mut seed = case + 0x9b3d_724a;
        let mut world = World::new();
        world.start();
        world.open();
        let mut referee = Referee::new();
        let op = Token::new(10);
        match next(&mut seed) % 3 {
            0 => {
                world.ax_visible = next(&mut seed) & 1 == 0;
                let expect = if world.ax_visible { Expect::Present } else { Expect::Absent };
                referee.await_op(op, expect);
                world.ask(Request::Await {
                    page: Token::new(3),
                    op,
                    query: Query {
                        role: b"button".to_vec().into_boxed_slice(),
                        name: b"Save".to_vec().into_boxed_slice(),
                        within: None,
                        boxes: false,
                    },
                    expect,
                    within: skein_lib::Duration::from_millis(100),
                });
                let events = observe(&mut world, &mut referee);
                assert!(
                    matches!(events.as_slice(), [Event::Met { op: got, .. }] if *got == op),
                    "seed {case}: {events:?}"
                );
                world.assert_commands_settled();
            }
            1 => {
                world.disabled = next(&mut seed) & 1 != 0;
                world.hidden = !world.disabled && next(&mut seed) & 1 != 0;
                world.covered = !world.disabled && !world.hidden && next(&mut seed) & 1 != 0;
                referee.press(op);
                world.ask(Request::Press { page: Token::new(3), op, node: 7 });
                let events = observe(&mut world, &mut referee);
                assert!(
                    matches!(events.as_slice(), [Event::Done { .. } | Event::Refused { .. }]),
                    "seed {case}: {events:?}"
                );
                world.assert_commands_settled();
            }
            _ => {
                let which = usize::try_from(next(&mut seed) % 7).expect("index");
                faults_seen[which] = true;
                let fault = [
                    Fault::Drop,
                    Fault::Delay,
                    Fault::Error,
                    Fault::Oversize,
                    Fault::Crash,
                    Fault::PageCrash,
                    Fault::Interleave,
                ][which];
                world.fault(b"Accessibility.getFullAXTree", fault);
                world.ask(Request::Snapshot { page: Token::new(3), op });
                if fault == Fault::Delay && next(&mut seed) & 1 == 0 {
                    world.release_delayed();
                } else if fault == Fault::Drop || fault == Fault::Delay {
                    world.tick(Time::from_nanos(world.env.limits.answer.as_nanos()));
                    if fault == Fault::Delay {
                        world.release_delayed();
                    }
                }
                let events = observe(&mut world, &mut referee);
                let terminals = events
                    .iter()
                    .filter(|event| {
                        matches!(event,
                    Event::Snapshot { op: got, .. } | Event::Refused { op: got, .. } if *got == op)
                    })
                    .count();
                assert_eq!(terminals, 1, "seed {case}, fault {fault:?}: {events:?}");
                if fault == Fault::Error {
                    assert!(events.iter().any(|event| matches!(event, Event::Refused { why: Refusal::Gone, .. })));
                }
                world.assert_commands_settled();
            }
        }
    }
    assert!(faults_seen.into_iter().all(|seen| seen), "each fault must actually fall");
}
