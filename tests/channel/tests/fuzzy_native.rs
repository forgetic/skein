//! Real IO and simulator sweeps under independently configured fragmentation.
use skein_channel_world::native::conversation;
use skein_sim::Config;

#[test]
fn fragmented_native_framing_sweep() {
    for seed in 0..64 {
        let config = Config { buffer: 1, ..Config::calm() };
        let first = conversation(seed, config);
        assert_eq!(first, conversation(seed, config));
    }
}
