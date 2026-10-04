//! The transcripts (testing-strategy.md, 4.1; http.md, 6): responses in
//! the shape of an LLM provider's streams and a forge's API, responses
//! curl accepts, and hostile ones, each decoding to its expectation in
//! `transcripts/`.

use std::fmt::Write as _;

use skein_http::client::{Body, Call};
use skein_http_world::client_world::{self, Exchange, Reads, Settings};
use skein_http_world::reference::{Ending, Outcome};
use skein_http_world::sse_world;
use skein_http_world::transcript::{self, Decoded, Transcript};
use skein_lib::Rng;

const SEEDS: u64 = 4;

fn transcripts() -> Vec<Transcript> {
    let transcripts = transcript::all();
    assert!(transcripts.len() > 40, "the transcripts are found");
    transcripts
}

/// Whether two decodings agree: the events' own fields, not what the
/// reference reads of reconnection, which an expectation does not list.
fn agree(left: &Decoded, right: &Decoded) -> bool {
    let events = match (&left.events, &right.events) {
        (Some(left), Some(right)) => left.events == right.events && left.ending == right.ending,
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    };
    events && left.head == right.head && left.body == right.body && left.outcome == right.outcome
}

fn exchange(transcript: &Transcript) -> [Exchange; 1] {
    let call = Call {
        method: transcript.method,
        target: b"/".to_vec().into(),
        headers: Box::new([]),
        body: Body::None,
        close: false,
    };
    [Exchange { call, upload: Vec::new() }]
}

#[test]
fn every_transcript_has_an_expectation_and_the_reference_readers_agree_with_it() {
    let mut drafts = String::new();
    for transcript in transcripts() {
        let decoded = transcript::read(&transcript);
        if let Some(expected) = &transcript.expected {
            assert!(agree(&decoded, expected), "{}: the reference reads {decoded:#?}", transcript.name);
        } else {
            let draft = transcript::render(&transcript, &decoded);
            writeln!(drafts, "--- {}.expect\n{draft}", transcript.name).unwrap();
        }
    }
    assert!(
        drafts.is_empty(),
        "transcripts without an expectation; what the reference readers read, a draft to check against \
         each response by hand before it is kept:\n{drafts}"
    );
}

#[test]
fn every_transcript_decodes_to_its_expectation_whatever_the_cuts() {
    for transcript in transcripts() {
        let expected = transcript.expected.as_ref().expect("an expectation");
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed);
            // A server that keeps the connection open: no end comes once the
            // response is read, which would make it not persist.
            let settings = Settings {
                reads: Reads::Bytes,
                discard: 0,
                withdraw: 0,
                cross: 0,
                idle_end: 0,
                ..Settings::calm(&mut rng, transcript.limits)
            };
            let run = client_world::check(&exchange(&transcript), &transcript.bytes, &settings, seed);
            let seen = &run.exchanges[0];
            let what = format!("{}, seed {seed}", transcript.name);
            assert_eq!(seen.outcome, Some(expected.outcome), "{what}");
            let head = seen.response.as_ref().map(|response| {
                let headers =
                    response.headers.iter().map(|header| (header.name.to_vec(), header.value.to_vec())).collect();
                skein_http_world::reference::Head {
                    version: response.version,
                    status: response.status,
                    headers,
                    framing: response.framing,
                }
            });
            assert_eq!(head, expected.head, "{what}");
            if let (Some(body), Outcome::Done(_)) = (&expected.body, expected.outcome) {
                assert_eq!(&seen.body, body, "{what}: the body, a byte at a time");
            }
        }
    }
}

#[test]
fn every_event_stream_reads_to_its_expectation_whatever_the_cuts_and_scans() {
    for transcript in transcripts() {
        let Some(events) = transcript.expected.as_ref().and_then(|expected| expected.events.as_ref()) else { continue };
        let body =
            skein_http_world::reference::response(&transcript.bytes, transcript.method, false, &transcript.limits).body;
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed);
            let mut limits = transcript.sse;
            limits.chunk = [1, 7, 64, 4096][usize::try_from(seed).unwrap() % 4];
            let settings = sse_world::Settings::calm(&mut rng, limits);
            let run = sse_world::check(&body, &settings, seed);
            assert_eq!(run.events, events.events, "{}, seed {seed}", transcript.name);
            assert_eq!(run.outcome, Some(events.ending), "{}, seed {seed}", transcript.name);
        }
    }
}

#[test]
fn the_realistic_transcripts_decode_whole_and_the_hostile_ones_fail() {
    for transcript in transcripts() {
        let expected = transcript.expected.expect("an expectation");
        let failed = match (expected.outcome, &expected.events) {
            (Outcome::Failed(_), _) => true,
            (Outcome::Done(_), Some(events)) => events.ending != Ending::Ended,
            (Outcome::Done(_), None) => false,
        };
        assert_eq!(failed, transcript.name.starts_with("hostile-"), "{}", transcript.name);
    }
}

#[test]
fn a_transcript_cut_short_anywhere_decodes_as_the_reference_reads_its_prefix() {
    for transcript in transcripts() {
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed);
            let cut = usize::try_from(rng.below(transcript.bytes.len() as u64 + 1)).unwrap();
            let settings = Settings { cut: Some(cut), ..Settings::calm(&mut rng, transcript.limits) };
            let _ = client_world::check(&exchange(&transcript), &transcript.bytes, &settings, seed);
        }
    }
}
