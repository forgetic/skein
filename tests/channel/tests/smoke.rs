//! Cheap deterministic controls; expensive cardinalities stay in fuzzy tests.
use skein_channel_world::sweeps::{raw, versions};

#[test]
fn raw_headers_and_version_offers_smoke() {
    raw(7, 32);
    versions(13, 32);
}
