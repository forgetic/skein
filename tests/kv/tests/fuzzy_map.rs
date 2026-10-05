//! Seeded model checks for commit, get and ordered pages.

use std::collections::BTreeMap;

use skein_kv::{Event, Limits, Op, Page, Range, Request, Store};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

fn limits() -> Limits {
    Limits {
        key: 8,
        value: 8,
        ops: 4,
        commit: 256,
        queued: 4,
        queued_bytes: 1024,
        budget: 4096,
        segment: 1024,
        snapshot_after: 2048,
        chunk: 128,
        page: Page { rows: 4, bytes: 64 },
        deadline: Duration::from_secs(1),
    }
}

fn draw(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn generated_commits_and_pages_match_the_model() {
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    for initial in 1..=64_u64 {
        let mut seed = initial;
        let mut store = Store::memory(&env.limits);
        let mut model = BTreeMap::new();
        let mut above = Queue::with_capacity(4);
        let mut below = Queue::with_capacity(1);
        for step in 0..100_u64 {
            let key = [u8::try_from(draw(&mut seed) % 24).expect("key byte")];
            let value = [u8::try_from(draw(&mut seed) % 256).expect("value byte")];
            let erase = draw(&mut seed).is_multiple_of(4);
            let op = if erase {
                Op::Erase { key: Box::from(key) }
            } else {
                Op::Put { key: Box::from(key), value: Box::from(value) }
            };
            skein_kv::down(
                &mut store,
                &env,
                Request::Commit { owner: Token::new(step + 1), ops: Box::new([op]) },
                &mut above,
                &mut below,
            );
            match above.pop().expect("one commit outcome") {
                Event::Committed { .. } => {
                    if erase {
                        model.remove(key.as_slice());
                    } else {
                        model.insert(key.to_vec(), value.to_vec());
                    }
                }
                Event::Refused { .. } => {}
                event => panic!("unexpected commit outcome: {event:?}"),
            }
            skein_kv::down(
                &mut store,
                &env,
                Request::Get { owner: Token::new(101), key: Box::from(key) },
                &mut above,
                &mut below,
            );
            let Event::Got { value: found, .. } = above.pop().expect("get answers") else {
                panic!("get terminal");
            };
            assert_eq!(found.as_deref(), model.get(key.as_slice()).map(Vec::as_slice), "seed {initial}, step {step}");
        }
        let mut start = Box::from(&b""[..]);
        let mut found = Vec::new();
        loop {
            skein_kv::down(
                &mut store,
                &env,
                Request::Load {
                    owner: Token::new(102),
                    range: Range { start, end: None },
                    max: Page { rows: 4, bytes: 64 },
                },
                &mut above,
                &mut below,
            );
            let Event::Loaded { rows, next, .. } = above.pop().expect("load answers") else {
                panic!("load terminal");
            };
            found.extend(rows.iter().map(|row| (row.key.to_vec(), row.value.to_vec())));
            let Some(next) = next else { break };
            start = next;
        }
        assert_eq!(found, model.into_iter().collect::<Vec<_>>(), "seed {initial}");
        assert!(below.is_empty());
    }
}
