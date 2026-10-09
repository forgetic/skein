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
    assert_clients_settle_before_issuer(&first);
    assert_eq!(first.heap, second.heap);
    assert_eq!(first.heap.as_ref().unwrap().len(), 3);
}

#[test]
fn full_issuer_tokens_plans_codes_and_rotations_fit_the_process_heap() {
    let outcome = skein_oauth_world::simulated::run(47, false, true, Memory::Checked);
    assert!(outcome.heap.as_ref().unwrap().iter().all(|(peak, bound)| *peak > 0 && peak <= bound));
}

fn assert_clients_settle_before_issuer(outcome: &skein_world::Outcome<skein_oauth_world::simulated::Process>) {
    use skein_sim::{Event, Summary};
    let listener = outcome
        .trace
        .iter()
        .find_map(|entry| match entry.event {
            Event::Submit { kind: Summary::Listen { fd, .. }, .. } if entry.pid.raw() == 0 => Some(fd),
            Event::Submit { .. } | Event::Complete { .. } | Event::Fault(_) => None,
        })
        .expect("the issuer listened");
    let clients_closed = outcome
        .trace
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| {
            (entry.pid.raw() != 0
                && matches!(entry.event, Event::Complete { kind: Summary::Close { .. }, result: Ok(_), .. }))
            .then_some(at)
        })
        .max()
        .expect("the client and browser closed their descriptors");
    let issuer_closed = outcome
        .trace
        .iter()
        .position(|entry| {
            entry.pid.raw() == 0
                && matches!(entry.event,
            Event::Submit { kind: Summary::Close { fd }, .. } if fd == listener)
        })
        .expect("the issuer closed its listener");
    assert!(clients_closed < issuer_closed, "the issuer stays live until all client closes have completed");
}
