//! Seeded short socket operations replay the sign-in and refresh story.
#[test]
fn browser_redirect_and_token_posts_settle_under_short_socket_operations() {
    for seed in 0..16 {
        let first = skein_oauth_world::simulated::run(seed, true, false, skein_world::Memory::Unchecked);
        let second = skein_oauth_world::simulated::run(seed, true, false, skein_world::Memory::Unchecked);
        skein_oauth_world::simulated::assert_replay(&first, &second);
    }
}
