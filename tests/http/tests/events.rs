//! The event stream reader's machine world (testing-strategy.md, 2.4),
//! focused: a few seeds of each neighbour's behaviour, each run checked
//! against the reference reader. The sweeps over many seeds are in
//! `fuzzy_events.rs`.

use std::collections::BTreeSet;

use skein_http::sse::{Error, Limits, Waiting};
use skein_http_world::generate;
use skein_http_world::reference::{self, Ending};
use skein_http_world::sse_world::{self, Settings};
use skein_lib::Rng;
use skein_lib::stream::Fault;

const LIMITS: Limits = Limits { line: 256, event: 1024, field: 32, chunk: 16 };

fn stream(seed: u64) -> Vec<u8> {
    generate::events(&mut Rng::new(seed))
}

#[test]
fn a_stream_cut_and_delivered_at_random_reads_whole() {
    for seed in 0..60 {
        let stream = stream(seed);
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = sse_world::check(&stream, &settings, seed);
        assert_eq!(run.outcome, Some(Ending::Ended), "seed {seed}");
    }
}

#[test]
fn neighbours_that_end_early_fail_or_close_at_any_moment_leave_a_consistent_run() {
    for seed in 0..80 {
        let mut rng = Rng::new(seed);
        let mut stream = stream(seed);
        if rng.chance(500) {
            stream = generate::mutate(&mut rng, &stream);
        }
        let limits = sse_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, stream.len());
        let _ = sse_world::check(&stream, &settings, seed);
    }
}

#[test]
fn the_stream_ending_after_any_byte_reads_as_that_prefix() {
    let bytes = b"\xef\xbb\xbfevent: a\r\ndata: 1\rdata: 2\n\n: c\r\nid: x\nretry: 5\ndata: 3\r\n\r\n";
    for cut in 0..=bytes.len() {
        for seed in 0..3 {
            let mut rng = Rng::new(seed);
            let settings = Settings { cut: Some(cut), ..Settings::calm(&mut rng, LIMITS) };
            let run = sse_world::check(bytes, &settings, seed);
            assert_eq!(run.outcome, Some(Ending::Ended), "cut at {cut}");
        }
    }
}

#[test]
fn the_stream_failing_at_any_moment_fails_the_reader_unless_its_outcome_came_first() {
    let stream = stream(11);
    let mut failed = 0;
    for at in 0..300 {
        let mut rng = Rng::new(at);
        let settings = Settings { failure: Some((at, Fault::Reset)), ..Settings::calm(&mut rng, LIMITS) };
        let run = sse_world::check(&stream, &settings, at);
        if run.outcome == Some(Ending::Failed(Error::Stream(Fault::Reset))) {
            failed += 1;
        }
    }
    assert!(failed > 50, "most failures land before the outcome: {failed}");
}

#[test]
fn the_side_above_closes_in_every_state() {
    let bytes = b"data: one\n\ndata: two\n\ndata: thr";
    let mut seen = BTreeSet::new();
    for at in 0..200 {
        let mut rng = Rng::new(at);
        let settings = Settings { close: Some(at % 60), ..Settings::calm(&mut rng, LIMITS) };
        let run = sse_world::check(bytes, &settings, at);
        seen.insert(format!("{:?}", run.closed_while));
    }
    let every: BTreeSet<String> =
        [Waiting::Next, Waiting::Bytes, Waiting::Close].iter().map(|waiting| format!("{waiting:?}")).collect();
    assert_eq!(seen, every, "closed while waiting for a Next, for bytes, and for the close");
}

#[test]
fn a_stream_of_lines_ended_by_cr_alone_is_read_as_it_comes() {
    let bytes = b"data: 1\r\rdata: 2\r\r: ping\r\rdata: 3\r\r";
    let mut scanned_to_cr = 0;
    for seed in 0..20 {
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, Limits { chunk: 8, ..LIMITS });
        let run = sse_world::check(bytes, &settings, seed);
        assert_eq!(run.events.len(), 3, "seed {seed}");
        if run.fell.scanned_to_cr {
            scanned_to_cr += 1;
        }
    }
    assert_eq!(scanned_to_cr, 20, "after the first CR alone, every scan is to CR");
}

#[test]
fn a_user_that_stops_asking_stops_the_reading_and_the_stream_below_holds_the_rest() {
    let mut bytes = Vec::new();
    for n in 0..60 {
        bytes.extend_from_slice(format!("data: {n}\n\n").as_bytes());
    }
    for seed in 0..8 {
        let mut rng = Rng::new(seed);
        let settings =
            Settings { cap: 24, arrival: 1000, stall: Some((10 + seed, 300)), ..Settings::calm(&mut rng, LIMITS) };
        let run = sse_world::check(&bytes, &settings, seed);
        assert_eq!(run.outcome, Some(Ending::Ended), "seed {seed}");
        assert_eq!(run.held_back, settings.cap, "seed {seed}: the stream below filled to its cap, undemanded");
    }
}

#[test]
fn a_seed_replays_to_the_same_run() {
    for seed in 0..20 {
        let mut rng = Rng::new(seed);
        let stream = generate::mutate(&mut rng, &stream(seed));
        let limits = sse_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, stream.len());
        assert_eq!(sse_world::run(&stream, &settings, seed), sse_world::run(&stream, &settings, seed), "seed {seed}");
    }
}

#[test]
fn the_reference_reader_reads_the_standard_s_examples() {
    // WHATWG HTML, 9.2.6, the examples after the parsing rules.
    let roomy = Limits { line: 1024, event: 4096, field: 64, chunk: 64 };
    let read = reference::events(b"data: YHOO\ndata: +2\ndata: 10\n\n", &roomy);
    assert_eq!(read.events.len(), 1);
    assert_eq!(read.events[0].data, b"YHOO\n+2\n10");
    let read = reference::events(
        b": test stream\n\ndata: first event\nid: 1\n\ndata:second event\nid\n\ndata:  third event\n",
        &roomy,
    );
    let data: Vec<&[u8]> = read.events.iter().map(|event| &event.data[..]).collect();
    assert_eq!(data, [&b"first event"[..], b"second event"]);
    assert_eq!(read.events[0].id, b"1");
    assert_eq!(read.events[1].id, b"", "an id with no value resets it");
    let read = reference::events(b"data\n\ndata\ndata\n\ndata:", &roomy);
    let data: Vec<&[u8]> = read.events.iter().map(|event| &event.data[..]).collect();
    assert_eq!(data, [&b""[..], b"\n"], "the last block has no blank line");
    let read = reference::events(b"data:test\n\ndata: test\n\n", &roomy);
    assert_eq!(read.events[0].data, read.events[1].data, "one space after the colon is dropped");
}
