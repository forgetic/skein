//! Focused simulated loop with the scripted fake peer.

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn scripted_fake_peer_answers_over_io_and_simulated_loopback() {
    skein_llm_connection_world::simulated::scenario(23, false);
}

#[test]
fn scripted_fake_process_serves_the_actual_tls_connection() {
    drop(skein_llm_connection_world::simulated::run(
        31,
        false,
        skein_fake_peers::Transport::Tls,
        false,
        skein_world::Memory::Unchecked,
    ));
}

#[test]
fn owner_shutdown_settles_live_plaintext_and_tls_peers_and_releases_both_heaps() {
    for transport in [skein_fake_peers::Transport::Plaintext, skein_fake_peers::Transport::Tls] {
        let outcome = skein_llm_connection_world::simulated::run_with_shutdown(
            41,
            false,
            transport,
            true,
            skein_world::Memory::Checked,
            0,
            false,
        );
        assert!(outcome.end < skein_lib::Time::from_nanos(2_000_000_000), "the shipped keep cannot settle the owner");
        assert!(outcome.heap.as_ref().expect("checked heaps").iter().all(|(peak, bound)| peak <= bound));
    }
}

#[test]
fn a_peer_that_ignores_the_half_close_is_settled_by_ios_close_deadline() {
    let first = skein_llm_connection_world::simulated::run_with_shutdown(
        43,
        false,
        skein_fake_peers::Transport::Plaintext,
        false,
        skein_world::Memory::Checked,
        0,
        true,
    );
    let second = skein_llm_connection_world::simulated::run_with_shutdown(
        43,
        false,
        skein_fake_peers::Transport::Plaintext,
        false,
        skein_world::Memory::Checked,
        0,
        true,
    );
    skein_llm_connection_world::simulated::assert_replay(&first, &second);
    assert!(first.end >= skein_lib::Time::from_nanos(1_000_000_000), "io's close deadline bounds the live peer");
    assert!(first.end < skein_lib::Time::from_nanos(2_000_000_000));
}
