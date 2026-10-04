//! The request transcripts (testing-strategy.md, 4.1; http.md, 6): each
//! comes to its expectation through the server's machine world under
//! several seeds, read a byte at a time so that every byte of each body is
//! seen, and cut anywhere, as the reference reads the prefix. A transcript
//! without an expectation fails, printing the reference reader's reading
//! as a draft to check.

use std::fmt::Write as _;

use skein_http_world::reference::RequestHead;
use skein_http_world::request_transcript::{self, Entry, Transcript};
use skein_http_world::server_world::{self, Reads, Run, Settings};
use skein_lib::Rng;

/// The settings a transcript is served with: a client of every pace that
/// waits as long as it takes for a 100 (Continue), and ends the stream only
/// once the server reads past its bytes; and a side above that reads a
/// byte at a time and withdraws nothing.
fn settings(transcript: &Transcript, seed: u64) -> Settings {
    let mut rng = Rng::new(seed);
    Settings {
        reads: Reads::Bytes,
        withdraw: 0,
        idle_end: 0,
        patience: 1_000_000,
        ..Settings::calm(&mut rng, transcript.limits)
    }
}

/// `transcript` served from `seed`, by the plan it is served with.
fn serve(transcript: &Transcript, settings: &Settings, seed: u64) -> Run {
    let plans = vec![request_transcript::plan(); 8];
    server_world::check(&transcript.bytes, &plans, settings, seed)
}

/// What a run came to, as a transcript's expectation lists it.
fn entries(run: &Run) -> Vec<Entry> {
    let mut entries = Vec::new();
    for seen in &run.seen {
        let head = seen.call.as_ref().map(|call| RequestHead {
            method: call.method,
            target: call.target.to_vec(),
            version: call.version,
            headers: call.headers.iter().map(|header| (header.name.to_vec(), header.value.to_vec())).collect(),
            body: call.body,
        });
        let outcome = seen.outcome.expect("an outcome for each request: the side above closes only at the end");
        entries.push(Entry { head, body: seen.body.clone(), outcome });
    }
    entries
}

#[test]
fn every_request_transcript_comes_to_its_expectation_whatever_the_pace() {
    let mut drafts = String::new();
    let transcripts = request_transcript::all();
    assert!(transcripts.len() >= 30, "the request transcripts are kept: {}", transcripts.len());
    for transcript in &transcripts {
        let read = request_transcript::read(transcript);
        let Some(expected) = &transcript.expected else {
            let draft = request_transcript::render(transcript, &read);
            writeln!(drafts, "{}.expect:\n{draft}", transcript.name).expect("writing to a String");
            continue;
        };
        assert_eq!(&read, expected, "{}: the reference reads the expectation", transcript.name);
        for seed in 0..5 {
            let run = serve(transcript, &settings(transcript, seed), seed);
            let entries = entries(&run);
            assert_eq!(entries.len(), expected.len(), "{}, seed {seed}: {entries:#?}", transcript.name);
            for (entry, expected) in entries.iter().zip(expected) {
                assert_eq!(entry.head, expected.head, "{}, seed {seed}: the call", transcript.name);
                assert_eq!(entry.body, expected.body, "{}, seed {seed}: the body", transcript.name);
                assert_eq!(entry.outcome, expected.outcome, "{}, seed {seed}: the outcome", transcript.name);
            }
        }
    }
    assert!(drafts.is_empty(), "transcripts without an expectation; drafts:\n{drafts}");
}

#[test]
fn a_request_transcript_cut_short_anywhere_comes_to_what_the_reference_reads_of_its_prefix() {
    for transcript in request_transcript::all() {
        let len = transcript.bytes.len();
        let step = (len / 40).max(1);
        for cut in (0..=len).step_by(step) {
            let seed = cut as u64;
            let settings = Settings { cut: Some(cut), ..settings(&transcript, seed) };
            let _ = serve(&transcript, &settings, seed);
        }
    }
}
