//! A world that checks memory refuses to run without the counting allocator,
//! which this binary does not declare: it would check nothing, and pass.

use skein_sim::Config;
use skein_world::{Memory, World};
use skein_world_tests::{Judge, Script};

#[test]
#[should_panic(expected = "runs under the counting allocator")]
fn a_world_that_checks_memory_refuses_to_run_without_the_counting_allocator() {
    let _world: World<Script, Judge> = World::new(1, Config::calm(), Judge::new(1, Vec::new()), Memory::Checked);
}
