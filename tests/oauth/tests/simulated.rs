//! The actual browser follows the issuer's Location over loopback.
use skein_heap::Counting;
use skein_world::Memory;
#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn sign_in_redirect_and_refresh_run_in_separate_processes_and_replay() {
    let first = skein_oauth_world::simulated::run(43, false, false, Memory::Checked);
    let second = skein_oauth_world::simulated::run(43, false, false, Memory::Checked);
    skein_oauth_world::simulated::assert_replay(&first, &second);
    assert_eq!(first.heap, second.heap);
    assert_eq!(first.heap.as_ref().unwrap().len(), 3);
}

#[test]
fn full_issuer_tokens_plans_codes_and_rotations_fit_the_process_heap() {
    let outcome = skein_oauth_world::simulated::run(47, false, true, Memory::Checked);
    assert!(outcome.heap.as_ref().unwrap().iter().all(|(peak, bound)| *peak > 0 && peak <= bound));
}
