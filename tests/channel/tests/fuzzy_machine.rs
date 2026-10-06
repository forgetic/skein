//! Original machine/version sweep dimensions are retained as new core controls.
use skein_channel_world::sweeps::{raw, versions};

#[test]
fn raw_inputs_and_machine_seed_sweep() {
    for seed in 0..128 {
        raw(seed, 8);
    }
    raw(4096, 4096);
}

#[test]
fn bounded_versions_and_version_seed_sweep() {
    for seed in 0..64 {
        versions(seed, 8);
    }
    versions(4096, 4096);
}
