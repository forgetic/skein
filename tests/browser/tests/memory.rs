//! Exercise the declared heap bound with a counting allocator.

use skein_browser::boundary::Request;
use skein_browser::{Limits, worst_case};
use skein_browser_world::world::{Fault, World};
use skein_heap::{Counting, Meter};
use skein_lib::Token;

#[global_allocator]
static HEAP: Counting = Counting;

pub fn compact_limits() -> Limits {
    Limits {
        persons: 2,
        pages: 2,
        ops: 8,
        commands: 32,
        message: 4096,
        command: 2048,
        matches: 8,
        text: 128,
        snapshot: 1024,
        screenshot: 2048,
        stderr: 128,
        ..Limits::default()
    }
}

#[test]
fn reports_faults_and_terminals_fit_the_declared_bound() {
    let limits = compact_limits();
    let bound = worst_case(&limits).expect("finite bound");
    let meter = Meter::new();
    meter.start();
    let mut world = World::with_limits(limits);
    world.start();
    world.open();
    world.ask(Request::Screenshot { page: Token::new(3), op: Token::new(4) });
    world.fault(b"Accessibility.getFullAXTree", Fault::Interleave);
    world.ask(Request::Snapshot { page: Token::new(3), op: Token::new(5) });
    world.disabled = true;
    world.ask(Request::Press { page: Token::new(3), op: Token::new(6), node: 7 });
    let measured = meter.end();
    assert!(measured.peak() <= bound, "whole world peak {} exceeds browser bound {bound}", measured.peak());
    drop(world);
    assert_eq!(meter.held(), 0, "all storage is released");
}
