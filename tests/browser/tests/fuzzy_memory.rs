//! Seeded loads at varied limits, checked against `worst_case`.

use skein_browser::boundary::Request;
use skein_browser::{Limits, worst_case};
use skein_browser_world::world::{Fault, World};
use skein_heap::{Counting, Meter};
use skein_lib::{Time, Token};

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn random_load_stays_within_the_declared_bound() {
    for seed in 0..96_u64 {
        let limits = Limits {
            persons: 2,
            pages: 2,
            ops: 4 + u32::try_from(seed % 8).expect("ops"),
            commands: 24,
            message: 1024 + u32::try_from(seed % 5).expect("message") * 256,
            command: 2048,
            matches: 4 + u32::try_from(seed % 8).expect("matches"),
            text: 64 + u32::try_from(seed % 4).expect("text") * 32,
            snapshot: 512,
            screenshot: 1024,
            stderr: 64,
            ..Limits::default()
        };
        let bound = worst_case(&limits).expect("finite bound");
        let meter = Meter::new();
        meter.start();
        let mut world = World::with_limits(limits);
        world.start();
        world.open();
        world.fault(
            b"Accessibility.getFullAXTree",
            match seed % 4 {
                0 => Fault::Drop,
                1 => Fault::Delay,
                2 => Fault::Error,
                _ => Fault::Interleave,
            },
        );
        world.ask(Request::Snapshot { page: Token::new(3), op: Token::new(4) });
        if seed % 4 <= 1 {
            world.tick(Time::from_nanos(limits.answer.as_nanos()));
            world.release_delayed();
        }
        let measured = meter.end();
        assert!(measured.peak() <= bound, "seed {seed}: peak {} > bound {bound}", measured.peak());
        drop(world);
        assert_eq!(meter.held(), 0, "seed {seed}: all storage released");
    }
}
