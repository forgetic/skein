//! The client's machine world, swept (testing-strategy.md, 8; http.md, 6):
//! many connections of generated exchanges, valid and mutated, and the
//! transcripts, cut and mutated, under limits and neighbours drawn from
//! each seed, each exchange checked against the reference reader. This
//! stands in for the fuzz target, which waits for a nightly toolchain.
//!
//! A sweep asserts that what it injects fell (testing-strategy.md, 3): each
//! way an exchange can fail, each way a stream can end or fail, a close in
//! each state, and every refusal of the side above's.

use std::collections::BTreeMap;

use skein_http::client::Error;
use skein_http_world::client_world::{self, Exchange, Run, Settings};
use skein_http_world::reference::Outcome;
use skein_http_world::{generate, transcript};
use skein_lib::Rng;

const ROUNDS: u64 = 20_000;

/// The transcripts, longer, are swept fewer times.
const TRANSCRIPT_ROUNDS: u64 = 2_000;

/// How often each thing a sweep injects or reaches fell.
#[derive(Default)]
struct Seen(BTreeMap<String, u32>);

impl Seen {
    fn note(&mut self, what: String) {
        *self.0.entry(what).or_default() += 1;
    }

    fn record(&mut self, run: &Run, settings: &Settings) {
        for seen in &run.exchanges {
            match seen.outcome {
                Some(Outcome::Failed(Error::Stream(_))) => self.note("failed Stream".into()),
                Some(Outcome::Failed(Error::Refused(_))) => self.note("failed Refused".into()),
                Some(outcome) => self.note(format!("{outcome:?}")),
                None => self.note("closed before the outcome".into()),
            }
            if seen.discarded {
                self.note("a body discarded".into());
            }
            if seen.upload_failed.is_some() {
                self.note(format!("the upload failed with {:?}", seen.upload_failed));
            }
            if seen.body_failed.is_some() {
                self.note("the body failed".into());
            }
        }
        self.note(format!("closed while waiting for {:?}", run.closed_while));
        let fell = run.fell;
        for (flag, what) in [
            (fell.idle_end, "an end with nothing demanded"),
            (fell.crossed_end, "a demand crossed the end"),
            (fell.room_first, "room granted while a response waited"),
            (fell.early_response, "a response line read mid-upload"),
            (fell.withdrew, "a body demand withdrawn"),
            (fell.late_answer, "an answer after the withdrawal"),
            (fell.failed_after_end, "a failure after the end"),
            (fell.reused, "a connection reused"),
        ] {
            if flag {
                self.note(what.into());
            }
        }
        if let Some(waiting) = fell.failed_while {
            self.note(format!("failed while waiting for {waiting:?}"));
        }
        if let Some(fault) = run.failed {
            self.note(format!("failed with {fault:?}"));
        }
        if settings.cut.is_some() {
            self.note("a stream ended early".into());
        }
    }

    fn assert_fell(&self, expected: &[&str]) {
        for what in expected {
            assert!(self.0.contains_key(*what), "{what} fell: {:#?}", self.0);
        }
    }
}

const EVERY_OUTCOME: [&str; 16] = [
    "Done(Keep)",
    "Done(Close)",
    "Failed(Closed)",
    "Failed(Truncated)",
    "Failed(Status)",
    "Failed(Version)",
    "Failed(Header)",
    "Failed(HeadTooLong)",
    "Failed(TooManyHeaders)",
    "Failed(Framing)",
    "Failed(ChunkSize)",
    "Failed(Chunk)",
    "Failed(Trailer)",
    "failed Stream",
    "failed Refused",
    "closed before the outcome",
];

const EVERY_NEIGHBOUR: [&str; 24] = [
    "a body discarded",
    "the upload failed with Some(Other)",
    "the upload failed with Some(Invalid)",
    "the body failed",
    "closed while waiting for Call",
    "closed while waiting for Room",
    "closed while waiting for Response",
    "closed while waiting for Body",
    "closed while waiting for Above",
    "closed while waiting for Close",
    "an end with nothing demanded",
    "a demand crossed the end",
    "room granted while a response waited",
    "a response line read mid-upload",
    "a body demand withdrawn",
    "an answer after the withdrawal",
    "a failure after the end",
    "a connection reused",
    "failed while waiting for Call",
    "failed while waiting for Room",
    "failed while waiting for Response",
    "failed while waiting for Body",
    "failed while waiting for Above",
    "a stream ended early",
];

/// `count` calls and the server's responses, from `rng`.
fn scenario(rng: &mut Rng, count: usize) -> (Vec<Exchange>, Vec<u8>) {
    let mut exchanges = Vec::new();
    let mut server = Vec::new();
    for index in 0..count {
        let (call, upload) = generate::call(rng);
        let body = generate::body(rng);
        server.extend(generate::response(rng, call.method, &body, index + 1 == count));
        exchanges.push(Exchange { call, upload });
    }
    (exchanges, server)
}

#[test]
fn generated_and_mutated_exchanges_read_as_the_reference_reads_them_whatever_the_neighbours() {
    let mut tally = Seen::default();
    for round in 0..ROUNDS {
        let seed = 0x0070_0000 + round;
        let mut rng = Rng::new(seed);
        let count = usize::try_from(rng.between(1, 4)).unwrap();
        let (exchanges, mut server) = scenario(&mut rng, count);
        if rng.chance(500) {
            server = generate::mutate(&mut rng, &server);
        } else if rng.chance(300) {
            server = generate::corrupt(&mut rng, &server);
        }
        let limits = client_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, server.len());
        let run = client_world::check(&exchanges, &server, &settings, seed);
        tally.record(&run, &settings);
    }
    tally.assert_fell(&EVERY_OUTCOME);
    tally.assert_fell(&EVERY_NEIGHBOUR);
}

#[test]
fn transcripts_cut_mutated_and_failed_read_as_the_reference_reads_them() {
    let transcripts = transcript::all();
    let mut tally = Seen::default();
    for round in 0..TRANSCRIPT_ROUNDS {
        let seed = 0x0071_0000 + round;
        let mut rng = Rng::new(seed);
        let transcript = &transcripts[usize::try_from(rng.below(transcripts.len() as u64)).unwrap()];
        let mut server = transcript.bytes.clone();
        if rng.chance(500) {
            server = generate::mutate(&mut rng, &server);
        }
        let call = skein_http::client::Call {
            method: transcript.method,
            target: b"/".to_vec().into(),
            headers: Box::new([]),
            body: skein_http::client::Body::None,
            close: false,
        };
        let exchanges = [Exchange { call, upload: Vec::new() }];
        let settings = Settings::chaotic(&mut rng, transcript.limits, server.len());
        let run = client_world::check(&exchanges, &server, &settings, seed);
        tally.record(&run, &settings);
    }
    tally.assert_fell(&["Done(Keep)", "Failed(Truncated)", "failed Stream", "closed before the outcome"]);
}
