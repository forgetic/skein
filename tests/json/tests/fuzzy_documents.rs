//! The tokenizer's machine world, swept (testing-strategy.md, 8; json.md,
//! 6): many documents, generated and mutated, and the transcripts, cut and
//! mutated, under limits and neighbours drawn from each seed, each run
//! checked against the reference parser. This stands in for the fuzz
//! target, which waits for a nightly toolchain.
//!
//! A sweep asserts that what it injects fell (testing-strategy.md, 3): each
//! way a document can fail, a stream ending early, idle or failing, and a
//! close in each state.

use std::collections::BTreeMap;

use skein_json::tokenizer::Error;
use skein_json_world::generate::{self, Shape};
use skein_json_world::world::{self, Run, Settings};
use skein_json_world::{Outcome, transcript};
use skein_lib::Rng;

const ROUNDS: u64 = 20_000;

/// How often each thing a sweep injects or reaches fell.
#[derive(Default)]
struct Seen(BTreeMap<String, u32>);

impl Seen {
    fn note(&mut self, what: String) {
        *self.0.entry(what).or_default() += 1;
    }

    fn record(&mut self, run: &Run, settings: &Settings) {
        match run.outcome {
            Some(Outcome::Failed(Error::Stream(_))) => self.note("failed Stream".into()),
            Some(outcome) => self.note(format!("{outcome:?}")),
            None => self.note("closed before the outcome".into()),
        }
        self.note(format!("closed while waiting for {:?}", run.closed_while));
        if run.idle_end {
            self.note("an end with nothing demanded".into());
        }
        if run.crossed_end {
            self.note("a demand crossed the end".into());
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

const EVERY_ERROR: [&str; 13] = [
    "Failed(Unexpected)",
    "Failed(Trailing)",
    "Failed(TooLong)",
    "Failed(TooDeep)",
    "Failed(StringTooLong)",
    "Failed(NumberTooLong)",
    "Failed(Number)",
    "Failed(Escape)",
    "Failed(Surrogate)",
    "Failed(Utf8)",
    "Failed(Control)",
    "Failed(Truncated)",
    "failed Stream",
];

const EVERY_NEIGHBOUR: [&str; 9] = [
    "Done",
    "closed before the outcome",
    "closed while waiting for Next",
    "closed while waiting for Bytes",
    "closed while waiting for Close",
    "an end with nothing demanded",
    "a demand crossed the end",
    "a stream ended early",
    "the stream below filled while the side above stopped",
];

#[test]
fn generated_and_mutated_documents_read_as_the_reference_reads_them_whatever_the_neighbours() {
    let mut tally = Seen::default();
    for round in 0..ROUNDS {
        let seed = 0x0050_0000 + round;
        let mut rng = Rng::new(seed);
        let shape = Shape {
            depth: u32::try_from(rng.between(2, 6)).unwrap(),
            width: u32::try_from(rng.between(2, 8)).unwrap(),
            string: u32::try_from(rng.between(0, 16)).unwrap(),
        };
        let tokens = generate::tokens(&mut rng, shape);
        let mut document = generate::render(&mut rng, &tokens);
        if rng.chance(600) {
            document = generate::mutate(&mut rng, &document);
        }
        let limits = world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, document.len());
        let run = world::check(&document, &settings, seed);
        tally.record(&run, &settings);
    }
    tally.assert_fell(&EVERY_ERROR);
    tally.assert_fell(&EVERY_NEIGHBOUR);
}

#[test]
fn transcripts_cut_mutated_and_failed_read_as_the_reference_reads_them() {
    let (hostile, realistic): (Vec<_>, Vec<_>) =
        transcript::all().into_iter().partition(|transcript| transcript.name.starts_with("hostile-"));
    let mut tally = Seen::default();
    for round in 0..ROUNDS / 4 {
        let seed = 0x0051_0000 + round;
        let mut rng = Rng::new(seed);
        // Mostly the realistic ones, which are long enough to cut and
        // mutate in many places.
        let transcripts = if rng.chance(800) { &realistic } else { &hostile };
        let transcript = &transcripts[usize::try_from(rng.below(transcripts.len() as u64)).unwrap()];
        let mut document = transcript.document.clone();
        if rng.chance(500) {
            document = generate::mutate(&mut rng, &document);
        }
        let mut limits = transcript.limits;
        limits.chunk = u32::try_from(rng.between(1, 128)).unwrap();
        let settings = Settings::chaotic(&mut rng, limits, document.len());
        let run = world::check(&document, &settings, seed);
        tally.record(&run, &settings);
    }
    tally.assert_fell(&EVERY_NEIGHBOUR);
}
