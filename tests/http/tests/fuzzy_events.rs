//! The event stream reader's machine world, swept (testing-strategy.md, 8;
//! http.md, 6): many streams, generated and mutated, and the bodies of the
//! transcripts that hold events, cut and mutated, under limits and
//! neighbours drawn from each seed, each run checked against the reference
//! reader. This stands in for the fuzz target, which waits for a nightly
//! toolchain.
//!
//! A sweep asserts that what it injects fell (testing-strategy.md, 3): each
//! way a stream can fail, each way it can end or fail below, a close in
//! each state, a CR's LF delivered alone, and a line read in pieces.

use std::collections::BTreeMap;

use skein_http::sse::Error;
use skein_http_world::reference::Ending;
use skein_http_world::sse_world::{self, Run, Settings};
use skein_http_world::{generate, transcript};
use skein_lib::Rng;

const ROUNDS: u64 = 12_000;

#[derive(Default)]
struct Seen(BTreeMap<String, u32>);

impl Seen {
    fn note(&mut self, what: String) {
        *self.0.entry(what).or_default() += 1;
    }

    fn record(&mut self, run: &Run, settings: &Settings) {
        match run.outcome {
            Some(Ending::Failed(Error::Stream(_))) => self.note("failed Stream".into()),
            Some(outcome) => self.note(format!("{outcome:?}")),
            None => self.note("closed before the outcome".into()),
        }
        self.note(format!("closed while waiting for {:?}", run.closed_while));
        let fell = run.fell;
        for (flag, what) in [
            (fell.idle_end, "an end with nothing demanded"),
            (fell.crossed_end, "a demand crossed the end"),
            (fell.late_delivery, "a delivery after the close"),
            (fell.failed_after_end, "a failure after the end"),
            (fell.lone_lf, "a lone LF after a CR"),
            (fell.long_line, "a line longer than a chunk"),
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
        if run.held_back == settings.cap {
            self.note("the stream below filled while the side above stopped".into());
        }
    }

    fn assert_fell(&self, expected: &[&str]) {
        for what in expected {
            assert!(self.0.contains_key(*what), "{what} fell: {:#?}", self.0);
        }
    }
}

const EVERY: [&str; 24] = [
    "Ended",
    "Failed(LineTooLong)",
    "Failed(EventTooLong)",
    "Failed(FieldTooLong)",
    "failed Stream",
    "closed before the outcome",
    "closed while waiting for Next",
    "closed while waiting for Bytes",
    "closed while waiting for Close",
    "an end with nothing demanded",
    "a demand crossed the end",
    "a delivery after the close",
    "a failure after the end",
    "a lone LF after a CR",
    "a line longer than a chunk",
    "failed while waiting for Next",
    "failed while waiting for Bytes",
    "failed while waiting for Close",
    "failed while waiting for Nothing",
    "failed with Reset",
    "failed with Invalid",
    "failed with Other",
    "a stream ended early",
    "the stream below filled while the side above stopped",
];

#[test]
fn generated_and_mutated_streams_read_as_the_reference_reads_them_whatever_the_neighbours() {
    let mut tally = Seen::default();
    for round in 0..ROUNDS {
        let seed = 0x0080_0000 + round;
        let mut rng = Rng::new(seed);
        let mut stream = generate::events(&mut rng);
        if rng.chance(500) {
            stream = generate::mutate(&mut rng, &stream);
        }
        let limits = sse_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, stream.len());
        let run = sse_world::check(&stream, &settings, seed);
        tally.record(&run, &settings);
    }
    tally.assert_fell(&EVERY);
}

#[test]
fn the_transcripts_events_cut_mutated_and_failed_read_as_the_reference_reads_them() {
    let mut bodies = Vec::new();
    for transcript in transcript::all() {
        let decoded = transcript::read(&transcript);
        if let (Some(_), Some(head)) = (decoded.events, decoded.head) {
            let response =
                skein_http_world::reference::response(&transcript.bytes, transcript.method, false, &transcript.limits);
            assert!(head.status > 0);
            bodies.push((response.body, transcript.sse));
        }
    }
    assert!(bodies.len() >= 4, "the transcripts hold event streams");
    let mut tally = Seen::default();
    for round in 0..ROUNDS / 6 {
        let seed = 0x0081_0000 + round;
        let mut rng = Rng::new(seed);
        let (body, limits) = &bodies[usize::try_from(rng.below(bodies.len() as u64)).unwrap()];
        let mut stream = body.clone();
        if rng.chance(500) {
            stream = generate::mutate(&mut rng, &stream);
        }
        let mut limits = *limits;
        limits.chunk = u32::try_from(rng.between(1, 128)).unwrap();
        let settings = Settings::chaotic(&mut rng, limits, stream.len());
        let run = sse_world::check(&stream, &settings, seed);
        tally.record(&run, &settings);
    }
    tally.assert_fell(&["Ended", "failed Stream", "closed before the outcome", "a stream ended early"]);
}
