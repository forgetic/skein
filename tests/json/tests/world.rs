//! The tokenizer's machine world (testing-strategy.md, 2.4), focused: a
//! few seeds of each neighbour's behaviour, each run checked against the
//! reference parser. The sweeps over many seeds are in `fuzzy_documents.rs`.

use std::collections::BTreeSet;

use skein_json::Token;
use skein_json::tokenizer::{Error, Limits, Waiting};
use skein_json_world::generate::{self, Shape};
use skein_json_world::world::{self, Settings};
use skein_json_world::{Outcome, reference};
use skein_lib::Rng;
use skein_lib::stream::Fault;

const LIMITS: Limits = Limits { depth: 8, string: 64, number: 32, chunk: 16 };
const SHAPE: Shape = Shape { depth: 4, width: 4, string: 8 };

/// A document generated from `seed`, rendered with whitespace and escapes.
fn document(seed: u64) -> Vec<u8> {
    let mut rng = Rng::new(seed);
    let tokens = generate::tokens(&mut rng, SHAPE);
    generate::render(&mut rng, &tokens)
}

#[test]
fn a_document_cut_and_delivered_at_random_decodes_whole() {
    for seed in 0..40 {
        let document = document(seed);
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = world::check(&document, &settings, seed);
        assert_eq!(run.outcome, Some(Outcome::Done), "seed {seed}");
    }
}

#[test]
fn neighbours_that_end_early_fail_or_close_at_any_moment_leave_a_consistent_run() {
    for seed in 0..60 {
        let mut rng = Rng::new(seed);
        let mut document = document(seed);
        if rng.chance(500) {
            document = generate::mutate(&mut rng, &document);
        }
        let limits = world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, document.len());
        let _ = world::check(&document, &settings, seed);
    }
}

#[test]
fn the_stream_ending_after_any_byte_decodes_as_that_prefix() {
    let document = b"{\"a\":[1,-2.5e3,\"x\\\"y\\u00e9\",true,null],\"b\":{}}";
    for cut in 0..=document.len() {
        for seed in 0..3 {
            let mut rng = Rng::new(seed);
            let settings = Settings { cut: Some(cut), ..Settings::calm(&mut rng, LIMITS) };
            let run = world::check(document, &settings, seed);
            let expected = if cut == document.len() { Outcome::Done } else { Outcome::Failed(Error::Truncated) };
            assert_eq!(run.outcome, Some(expected), "cut at {cut}");
        }
    }
}

#[test]
fn the_stream_failing_at_any_moment_fails_the_document_unless_its_outcome_came_first() {
    let document = document(7);
    let mut failed = 0;
    for at in 0..400 {
        let mut rng = Rng::new(at);
        let settings = Settings { failure: Some((at, Fault::Reset)), ..Settings::calm(&mut rng, LIMITS) };
        let run = world::check(&document, &settings, at);
        if run.outcome == Some(Outcome::Failed(Error::Stream(Fault::Reset))) {
            failed += 1;
        }
    }
    assert!(failed > 100, "most failures land before the outcome: {failed}");
}

#[test]
fn the_side_above_closes_in_every_state() {
    let document = br#"[ "abc" , 12 , {"k": tr"#;
    let mut seen = BTreeSet::new();
    for at in 0..200 {
        let mut rng = Rng::new(at);
        let settings = Settings { close: Some(at), ..Settings::calm(&mut rng, LIMITS) };
        let run = world::check(document, &settings, at);
        seen.insert(format!("{:?}", run.closed_while));
    }
    let every: BTreeSet<String> =
        [Waiting::Next, Waiting::Bytes, Waiting::Close].iter().map(|waiting| format!("{waiting:?}")).collect();
    assert_eq!(seen, every, "closed while waiting for a Next, for bytes, and for the close");
}

#[test]
fn an_end_with_nothing_demanded_is_held_for_the_next_token() {
    let mut idle_ends = 0;
    for seed in 0..60 {
        let document = document(seed);
        let mut rng = Rng::new(seed);
        let settings = Settings { idle_end: 1000, eagerness: 100, ..Settings::calm(&mut rng, LIMITS) };
        let run = world::check(&document, &settings, seed);
        assert_eq!(run.outcome, Some(Outcome::Done), "seed {seed}");
        if run.idle_end {
            idle_ends += 1;
        }
    }
    assert!(idle_ends > 10, "the end came while idle in {idle_ends} runs");
}

#[test]
fn nesting_past_the_depth_is_refused() {
    let limits = Limits { depth: 16, ..LIMITS };
    for depth in [15, 16, 17, 100] {
        let document = generate::nested(depth);
        let mut rng = Rng::new(u64::from(depth));
        let settings = Settings::calm(&mut rng, limits);
        let run = world::check(&document, &settings, u64::from(depth));
        if depth <= 16 {
            assert_eq!(run.outcome, Some(Outcome::Done), "{depth} deep");
        } else {
            assert_eq!(run.outcome, Some(Outcome::Failed(Error::TooDeep)), "{depth} deep");
            let starts = run.tokens.iter().filter(|token| matches!(token, Token::ArrayStart | Token::ObjectStart));
            assert_eq!(starts.count(), 16, "a token for each container up to the depth");
        }
    }
}

#[test]
fn a_seed_replays_to_the_same_run() {
    for seed in 0..20 {
        let mut rng = Rng::new(seed);
        let document = generate::mutate(&mut rng, &document(seed));
        let limits = world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, document.len());
        assert_eq!(world::run(&document, &settings, seed), world::run(&document, &settings, seed), "seed {seed}");
    }
}

#[test]
fn the_reference_parser_reads_what_the_writer_of_the_generator_meant() {
    // The generator and the reference agree before the tokenizer is held
    // to either.
    for seed in 0..100 {
        let mut rng = Rng::new(seed);
        let tokens = generate::tokens(&mut rng, SHAPE);
        let document = generate::render(&mut rng, &tokens);
        let decoded = reference::parse(&document, &LIMITS);
        assert_eq!(decoded.outcome, Outcome::Done, "seed {seed}: {}", document.escape_ascii());
        assert_eq!(decoded.tokens, tokens, "seed {seed}");
    }
}
