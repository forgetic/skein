//! Exchanges with a rustls server in the client's machine world (tls.md,
//! 5): every split of the ciphertext, room granted late, a slow reader
//! above, the server's endings (`close_notify`, a truncation, a corrupted
//! record), a key update, a finish, and closes in every state.
//!
//! Each runs a few seeds of the world, held to its scenario by
//! `world::check`; what is asserted here does not depend on what rustls
//! draws from the kernel.

use std::collections::BTreeSet;

use skein_lib::Rng;
use skein_lib::stream::Fault;
use skein_tls::client::{Error, Limits, Waiting};
use skein_tls_world::pki::Versions;
use skein_tls_world::world::{self, Ending, Reads, Run, Scenario, Settings};

const LIMITS: Limits = Limits { read: 512, send: 512, records: 2 * skein_tls::client::MAX_RECORD };

fn calm(seed: u64) -> Settings {
    let mut rng = Rng::new(seed);
    let mut settings = Settings::calm(&mut rng, LIMITS);
    settings.finish = false;
    settings
}

fn exchange(seed: u64, request: usize, response: usize) -> Scenario {
    Scenario::exchange(&mut Rng::new(seed), request, response)
}

#[test]
fn every_split_of_the_ciphertext_reads_the_same() {
    for (seed, versions) in [(1, Versions::Tls13), (2, Versions::Tls12)] {
        let mut scenario = exchange(seed, 200, 3_000);
        scenario.server.versions = versions;
        // A byte at a time: every byte boundary is a cut.
        let mut settings = calm(seed);
        settings.piece = 1;
        settings.arrival = 1_000;
        settings.reads = Reads::Bytes;
        let run = world::check(&scenario, &settings, seed);
        assert_eq!(run.received, scenario.response, "{versions:?}: all of it, a byte at a time");
        assert_eq!(run.served, scenario.request);
        assert!(run.ended.is_some());
        // Pieces of every size, read with demands of every shape.
        for seed in 10..14 {
            let settings = calm(seed);
            let run = world::check(&scenario, &settings, seed);
            assert!(run.ended.is_some(), "{versions:?}, seed {seed}");
        }
    }
}

#[test]
fn room_granted_late_and_a_slow_reader_hold_the_stream_below() {
    let scenario = exchange(3, 2_000, 60_000);
    let mut settings = calm(3);
    settings.grant = 20;
    settings.eagerness = 50;
    settings.stall = Some((50, 400));
    settings.limits = Limits { read: 8_192, send: 64, ..LIMITS };
    let run = world::check(&scenario, &settings, 3);
    assert_eq!(run.served, scenario.request, "the request, sent within room granted late");
    assert!(run.fell.stalled_below, "the stream below held all it could while the reader stopped");
    assert!(run.ended.is_some());
}

#[test]
fn close_notify_from_the_server_ends_the_stream() {
    let scenario = exchange(4, 100, 5_000);
    let mut settings = calm(4);
    settings.reads = Reads::Bytes;
    let run = world::check(&scenario, &settings, 4);
    assert_eq!(run.received, scenario.response);
    assert_eq!((run.ended.is_some(), run.failed), (true, None));
}

#[test]
fn a_truncation_delivers_what_was_read_then_fails_never_ends() {
    for versions in [Versions::Tls13, Versions::Tls12] {
        let mut scenario = exchange(5, 100, 5_000);
        scenario.server.versions = versions;
        scenario.ending = Ending::Truncate;
        let mut settings = calm(5);
        settings.reads = Reads::Bytes;
        let run = world::check(&scenario, &settings, 5);
        assert_eq!(run.received, scenario.response, "{versions:?}: every byte deciphered is delivered");
        assert_eq!(run.failed, Some(Error::Truncated), "{versions:?}");
        assert_eq!(run.stream_failed, Some(Fault::Invalid), "{versions:?}: told apart from the end");
        assert_eq!(run.ended, None, "{versions:?}");
    }
}

#[test]
fn a_corrupted_record_fails_the_stream_as_invalid() {
    for (seed, versions) in [(6, Versions::Tls13), (7, Versions::Tls12)] {
        let mut scenario = exchange(seed, 100, 40_000);
        scenario.server.versions = versions;
        scenario.ending = Ending::Corrupt;
        let run = world::check(&scenario, &calm(seed), seed);
        assert_eq!(run.failed, Some(Error::Decrypt), "{versions:?}");
        assert_eq!(run.stream_failed, Some(Fault::Invalid), "{versions:?}");
        assert!(run.received.len() < scenario.response.len(), "{versions:?}: not past the record");
    }
}

#[test]
fn finishing_sends_close_notify_and_the_whole_request() {
    for versions in [Versions::Tls13, Versions::Tls12] {
        let mut scenario = exchange(8, 3_000, 100);
        scenario.server.versions = versions;
        scenario.ending = Ending::Silent;
        let mut settings = calm(8);
        settings.finish = true;
        let run = world::check(&scenario, &settings, 8);
        assert!(run.finished && run.notified, "{versions:?}: close_notify after the request");
        assert_eq!(run.served, scenario.request, "{versions:?}");
    }
}

#[test]
fn a_key_update_from_the_server_is_answered_with_the_next_records() {
    // The server answers at once, asking for a key update midway, while the
    // request still goes: the client's answer goes before its next records.
    let mut scenario = exchange(9, 20_000, 20_000);
    scenario.eager = true;
    scenario.key_update = true;
    scenario.server.versions = Versions::Tls13;
    let mut settings = calm(9);
    settings.grant = 50;
    let run = world::check(&scenario, &settings, 9);
    assert!(run.fell.early_response, "the response came while the request went");
    assert_eq!(run.served, scenario.request, "the server read every record after its key update");
}

#[test]
fn a_retry_and_alpn_in_an_exchange() {
    let mut scenario = exchange(10, 300, 3_000);
    scenario.server.retry = true;
    scenario.alpn = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    scenario.server.alpn = vec![b"http/1.1".to_vec()];
    let run = world::check(&scenario, &calm(10), 10);
    assert_eq!(run.agreed.expect("ready").alpn.as_deref(), Some(&b"http/1.1"[..]));
}

/// The waits the side above closed the client in, over runs closing at
/// every few iterations.
fn closes(scenario: &Scenario, seeds: std::ops::Range<u64>) -> (BTreeSet<String>, Vec<Run>) {
    let mut waits = BTreeSet::new();
    let mut runs = Vec::new();
    for seed in seeds {
        let mut settings = calm(seed);
        settings.close = Some(seed * 9);
        let run = world::check(scenario, &settings, seed);
        waits.insert(format!("{:?}", run.closed_while));
        runs.push(run);
    }
    (waits, runs)
}

#[test]
fn closes_in_every_state() {
    let (waits, runs) = closes(&exchange(11, 2_000, 8_000), 0..60);
    let mut truncated = exchange(12, 0, 100);
    truncated.ending = Ending::Truncate;
    let (failed, _) = closes(&truncated, 0..10);
    for wait in [Waiting::Handshaking, Waiting::Room, Waiting::Bytes, Waiting::Above] {
        assert!(waits.contains(&format!("{wait:?}")), "{wait:?}: {waits:?}");
    }
    assert!(failed.contains(&format!("{:?}", Waiting::Close)), "{failed:?}");
    // A close once the handshake is done sends close_notify (checked by the
    // world): some went; one before it, none.
    assert!(runs.iter().any(|run| run.notified) && runs.iter().any(|run| run.agreed.is_none()));
}
