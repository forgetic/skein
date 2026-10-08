//! Focused simulated loop with the scripted fake peer.

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
