//! Fragmented simulated loopback runs with seeded short sends and receives.

#[test]
fn scripted_peer_settles_under_short_socket_operations() {
    for seed in 0_u64..16 {
        skein_llm_connection_world::simulated::scenario(seed, true);
    }
}

#[test]
fn batched_uploads_replay_under_actual_short_operations() {
    use skein_fake_peers::Transport;
    use skein_llm_connection_world::simulated::{assert_replay, run_with_input};
    use skein_world::Memory;

    let mut short_recv = false;
    let mut short_send = false;
    for seed in 0..4 {
        let first = run_with_input(seed, true, Transport::Plaintext, false, Memory::Unchecked, 1500);
        let second = run_with_input(seed, true, Transport::Plaintext, false, Memory::Unchecked, 1500);
        assert_replay(&first, &second);
        short_recv |=
            first.trace.iter().any(|entry| matches!(entry.event, skein_sim::Event::Fault(skein_sim::Fault::ShortRecv)));
        short_send |=
            first.trace.iter().any(|entry| matches!(entry.event, skein_sim::Event::Fault(skein_sim::Fault::ShortSend)));
    }
    assert!(short_recv && short_send, "configured short reads and writes actually fell");
}
