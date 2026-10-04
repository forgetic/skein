//! The client's machine world, swept (testing-strategy.md, 8; tls.md, 5):
//! exchanges with a rustls server of every version, with retries, ALPN,
//! a big chain, key updates, every ending and refused certificates, under
//! limits and neighbours drawn from each seed, each run held to its
//! scenario. A handshake costs a millisecond or so, which keeps the sweep
//! to a few hundred runs.
//!
//! A sweep asserts that what it injects fell (testing-strategy.md, 3): each
//! outcome, each way the streams end or fail, a close while the client
//! waited for each thing, and each neighbour's oddity.

use std::collections::BTreeMap;

use skein_lib::Rng;
use skein_tls::client::{Error, Version};
use skein_tls_world::pki::{self, Chain, Versions};
use skein_tls_world::world::{self, Ending, Run, Scenario, Settings};

const ROUNDS: u64 = 400;

/// A scenario drawn from `rng`.
fn scenario(rng: &mut Rng) -> Scenario {
    let request = usize::try_from(rng.below(3_000)).expect("fits");
    let most = if rng.chance(300) { 60_000 } else { 3_000 };
    // A byte at least, which an ending may corrupt.
    let response = usize::try_from(rng.between(1, most)).expect("fits");
    let mut scenario = Scenario::exchange(rng, request, response);
    scenario.server.versions = world::pick(rng, &[Versions::Both, Versions::Tls12, Versions::Tls13]);
    scenario.server.retry = rng.chance(150);
    scenario.eager = rng.chance(300);
    scenario.key_update = rng.chance(300);
    scenario.ending = world::pick(rng, &[Ending::CloseNotify, Ending::Truncate, Ending::Corrupt, Ending::Silent]);
    if rng.chance(200) {
        scenario.alpn = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        scenario.server.alpn = vec![b"http/1.1".to_vec()];
    }
    if rng.chance(100) {
        scenario.server.chain = Chain::Big;
        scenario.name = "big.skein.test".into();
    }
    match rng.below(50) {
        0 => scenario.wall = pki::EXPIRED,
        1 => scenario.wall = pki::EARLY,
        2 => scenario.name = "other.test".into(),
        3 => scenario.server.chain = Chain::Untrusted,
        4 => scenario.server.chain = Chain::SelfSigned,
        5 => scenario.server.chain = Chain::CaAsLeaf,
        6 => scenario.server.chain = Chain::ClientOnly,
        _ => {}
    }
    scenario
}

/// How often each thing a sweep injects or reaches fell.
#[derive(Default)]
struct Seen(BTreeMap<String, u32>);

impl Seen {
    fn note(&mut self, what: String) {
        *self.0.entry(what).or_default() += 1;
    }

    fn record(&mut self, scenario: &Scenario, run: &Run) {
        match run.failed {
            Some(Error::Stream(_)) => self.note("failed Stream".into()),
            Some(Error::Certificate(certificate)) => self.note(format!("refused {certificate:?}")),
            Some(error) => self.note(format!("failed {error:?}")),
            None => self.note("no failure".into()),
        }
        if run.ended.is_some() {
            self.note("ended".into());
        }
        if let Some(agreed) = &run.agreed {
            self.note(format!("agreed {:?}", agreed.version));
            if agreed.alpn.is_some() {
                self.note("agreed a protocol".into());
            }
            // The server offers no group the client sent a share for.
            if scenario.server.retry && agreed.version == Version::Tls13 {
                self.note("agreed after a retry".into());
            }
        }
        // rustls had sent close_notify when it read the corrupted record.
        if run.notified && run.failed == Some(Error::Decrypt) {
            self.note("a corrupted record read after close_notify went".into());
        }
        self.note(format!("closed while waiting for {:?}", run.closed_while));
        let fell = run.fell;
        for (flag, what) in [
            (fell.room_first, "room granted while bytes waited"),
            (fell.stalled_below, "the stream below held all it could"),
            (fell.early_demand, "a demand waited for the handshake"),
            (fell.withdrew, "a demand withdrawn at the close"),
            (fell.late_answer, "an answer after the withdrawal"),
            (fell.idle_end, "an end with nothing demanded"),
            (fell.early_response, "a response before the request was sent"),
            (fell.withdrew_after_end, "the read that crossed the end withdrawn"),
            (fell.withdrew_room_after_end, "a demand of room withdrawn after the end"),
            (fell.updated_keys, "records read past the server's key update"),
        ] {
            if flag {
                self.note(what.into());
            }
        }
        if run.notified {
            self.note("close_notify read by the server".into());
        }
    }

    fn assert_fell(&self, expected: &[&str]) {
        for what in expected {
            assert!(self.0.contains_key(*what), "{what} fell: {:#?}", self.0);
        }
    }
}

#[test]
fn exchanges_swept() {
    let mut seen = Seen::default();
    for seed in 0..ROUNDS {
        let mut rng = Rng::new(seed);
        let scenario = scenario(&mut rng);
        let limits = world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits);
        let run = world::check(&scenario, &settings, seed);
        seen.record(&scenario, &run);
    }
    seen.assert_fell(&[
        "no failure",
        "failed Stream",
        "failed Truncated",
        "failed Decrypt",
        "failed TooLong",
        "refused Expired",
        "refused NotYetValid",
        "refused Name",
        "refused Issuer",
        "refused Invalid",
        "ended",
        "agreed Tls12",
        "agreed Tls13",
        "agreed a protocol",
        "agreed after a retry",
        "closed while waiting for Handshaking",
        "closed while waiting for Room",
        "closed while waiting for Bytes",
        "closed while waiting for Above",
        "closed while waiting for Close",
        "room granted while bytes waited",
        "the stream below held all it could",
        "a demand waited for the handshake",
        "a demand withdrawn at the close",
        "an answer after the withdrawal",
        "an end with nothing demanded",
        "a response before the request was sent",
        "the read that crossed the end withdrawn",
        "a demand of room withdrawn after the end",
        "close_notify read by the server",
        "a corrupted record read after close_notify went",
        "records read past the server's key update",
    ]);
}
