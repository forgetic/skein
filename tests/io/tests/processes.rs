//! io's process-group worlds, with exact replay and per-process heap checking.
use skein_heap::Counting;
use skein_io_world::processes::{Story, chaos, run};
use skein_sim::Config;
use skein_world::Memory;

#[global_allocator]
static HEAP: Counting = Counting;

fn check(story: Story) {
    for seed in 0..4 {
        for config in [Config::calm(), chaos()] {
            let outcome = run(seed, config, story, Memory::Checked);
            assert!(
                outcome.heap.as_ref().expect("memory checked").iter().all(|(peak, bound)| *peak > 0 && peak <= bound)
            );
        }
    }
}

#[test]
fn a_descendant_holding_the_pipe_ends_with_a_signal_to_the_group() {
    check(Story::SignalRunning);
}
#[test]
fn closing_a_child_whose_group_still_runs_ends_the_group() {
    check(Story::CloseExited);
}
#[test]
fn a_signal_to_the_group_after_its_leader_exited_reaches_the_group() {
    check(Story::SignalExited);
}
#[test]
fn the_group_story_replays_its_actual_kernel_trace() {
    let first = run(7, chaos(), Story::SignalExited, Memory::Checked);
    let second = run(7, chaos(), Story::SignalExited, Memory::Checked);
    assert_eq!(first.trace, second.trace);
    assert_eq!(first.heap, second.heap);
    assert_ne!(first.trace, run(8, chaos(), Story::SignalExited, Memory::Checked).trace);
}
