//! Supplemental actual IO schedules; original 400 TLS runs and 32 positives
//! remain in `fuzzy_world.rs` without edits (tls.md, 3.6; testing-strategy.md, 8).
use skein_lib::Duration;
use skein_sim::{Config, Faults};
use skein_tls::client::Version;
use skein_tls_world::native::{Case, conversation};

#[test]
fn independent_native_reads_and_output_settle_under_actual_short_reordered_io() {
    let config = Config {
        buffer: 7,
        faults: Faults {
            latency: 500,
            latency_max: Duration::from_millis(2),
            short_send: 500,
            short_recv: 500,
            ..Faults::NONE
        },
        ..Config::calm()
    };
    let mut completed = 0;
    for seed in 1..=8 {
        for version in [Version::Tls12, Version::Tls13] {
            let actual = conversation(seed, config, Case::Late, version);
            assert_eq!(actual.server_plaintext, b"ping");
            assert_eq!(actual.reply, b"ok");
            assert!(actual.behind_both_reads);
            assert_eq!(actual.closed, 1);
            completed += 1;
        }
    }
    assert_eq!(completed, 16, "every real conversation completed, no refusal substitutions");
}
