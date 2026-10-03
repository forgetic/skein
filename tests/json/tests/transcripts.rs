//! The transcripts (testing-strategy.md, 4.1; json.md, 6): documents in
//! the shape of an LLM provider's answers and a forge's API responses, and
//! hostile ones, each decoding to its expectation in `transcripts/`.

use std::fmt::Write as _;

use skein_json_world::transcript::{self, Transcript};
use skein_json_world::world::{self, Settings};
use skein_json_world::{Outcome, reference};
use skein_lib::Rng;

/// The scans' maximums a transcript is read with, unless it pins one.
const CHUNKS: [u32; 6] = [1, 2, 3, 7, 64, 4096];

const SEEDS: u64 = 6;

fn transcripts() -> Vec<Transcript> {
    let transcripts = transcript::all();
    assert!(transcripts.len() > 20, "the transcripts are found");
    transcripts
}

#[test]
fn every_transcript_has_an_expectation_and_the_reference_parser_agrees_with_it() {
    let mut drafts = String::new();
    for transcript in transcripts() {
        let decoded = reference::parse(&transcript.document, &transcript.limits);
        if let Some(expected) = &transcript.expected {
            assert_eq!(&decoded, expected, "{}: the reference parser", transcript.name);
        } else {
            let draft = transcript::render(&transcript.limits, &decoded);
            writeln!(drafts, "--- {}.expect\n{draft}", transcript.name).unwrap();
        }
    }
    assert!(
        drafts.is_empty(),
        "transcripts without an expectation; what the reference parser reads, a draft to check against \
         each document by hand before it is kept:\n{drafts}"
    );
}

#[test]
fn every_transcript_decodes_to_its_expectation_whatever_the_cuts_and_scans() {
    for transcript in transcripts() {
        let Some(expected) = &transcript.expected else { continue };
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed);
            let mut limits = transcript.limits;
            if !transcript.pinned_chunk {
                limits.chunk = CHUNKS[usize::try_from(seed).unwrap() % CHUNKS.len()];
            }
            let settings = Settings::calm(&mut rng, limits);
            let run = world::run(&transcript.document, &settings, seed);
            assert_eq!(run.decoded().as_ref(), Some(expected), "{}, seed {seed}: {settings:?}", transcript.name);
        }
    }
}

#[test]
fn the_realistic_transcripts_decode_whole_and_the_hostile_ones_fail() {
    for transcript in transcripts() {
        let expected = transcript.expected.expect("an expectation");
        let hostile = transcript.name.starts_with("hostile-");
        let escapes = transcript.name.starts_with("hostile-escapes-");
        assert_eq!(expected.outcome == Outcome::Done, !hostile || escapes, "{}", transcript.name);
    }
}

#[test]
fn a_transcript_cut_short_anywhere_decodes_as_the_reference_reads_its_prefix() {
    for transcript in transcripts() {
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed);
            let len = transcript.document.len();
            let cut = usize::try_from(rng.below(len as u64 + 1)).unwrap();
            let settings = Settings { cut: Some(cut), ..Settings::calm(&mut rng, transcript.limits) };
            let _ = world::check(&transcript.document, &settings, seed);
        }
    }
}
