//! Fragmented simulated loopback runs with seeded short sends and receives.

#[test]
fn scripted_peer_settles_under_short_socket_operations() {
    for seed in 0_u64..16 {
        skein_llm_connection_world::simulated::scenario(seed, true);
    }
}
