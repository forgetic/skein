//! Focused simulated loop with the scripted fake peer.

#[test]
fn scripted_fake_peer_answers_over_io_and_simulated_loopback() {
    skein_llm_connection_world::simulated::scenario(23, false);
}
