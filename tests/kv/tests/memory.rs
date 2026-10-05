//! The store's declared bound against the counting allocator.

use skein_heap::{Counting, Meter};
use skein_kv::{Event, Limits, Op, Page, Request, Store, worst_case};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

#[global_allocator]
static HEAP: Counting = Counting;

fn limits() -> Limits {
    Limits {
        key: 32,
        value: 32,
        ops: 4,
        commit: 512,
        queued: 4,
        queued_bytes: 2048,
        budget: 8192,
        segment: 2048,
        snapshot_after: 4096,
        chunk: 128,
        page: Page { rows: 8, bytes: 512 },
        deadline: Duration::from_secs(1),
    }
}

#[test]
fn random_churn_stays_under_declared_worst_case() {
    let limits = limits();
    let bound = worst_case(&limits).expect("usable limits have a bound");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let meter = Meter::new();
    meter.start();
    let mut store = Store::memory(&limits);
    let mut above = Queue::with_capacity(4);
    let mut below = Queue::with_capacity(1);
    let mut seed = 0x764f_32ac_1234_9bde_u64;
    for step in 0..500_u64 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let key = [(seed % 64) as u8];
        let op = if step % 5 == 0 {
            Op::Erase { key: Box::from(key) }
        } else {
            Op::Put { key: Box::from(key), value: Box::from([u8::try_from(step % 256).expect("byte"); 32]) }
        };
        skein_kv::down(
            &mut store,
            &env,
            Request::Commit { owner: Token::new(step + 1), ops: Box::new([op]) },
            &mut above,
            &mut below,
        );
        assert!(matches!(above.pop(), Some(Event::Committed { .. } | Event::Refused { .. })));
        assert!(below.is_empty(), "the memory store has no file work");
        assert!(store.used() <= limits.budget);
    }
    let measured = meter.end();
    let _own = meter.check(measured, bound, &"skein-kv random memory churn");
}
