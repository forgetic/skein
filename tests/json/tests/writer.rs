//! The writer against the tokenizer (json.md, 6): what one writes, the
//! other reads back the same, through the machine world. The sweep over
//! many seeds is in `fuzzy_writer.rs`.

use skein_json::Token;
use skein_json::tokenizer::Limits;
use skein_json::writer::{self, Refusal};
use skein_json_world::generate::{self, Shape};
use skein_json_world::world::{self, Settings};
use skein_json_world::{Decoded, Outcome, reference, transcript, write};
use skein_lib::Rng;

const WRITER: writer::Limits = writer::Limits { depth: 64, length: 1 << 20 };
const READER: Limits = Limits { depth: 64, string: 4096, number: 64, chunk: 32, length: 1 << 20 };

/// Reads `document` back through the world from `seed`, whole.
fn read_back(document: &[u8], seed: u64) -> Decoded {
    let mut rng = Rng::new(seed);
    let settings = Settings::calm(&mut rng, READER);
    world::check(document, &settings, seed).decoded().expect("an outcome")
}

#[test]
fn every_transcript_read_whole_is_written_and_read_back_the_same() {
    for transcript in transcript::all() {
        let Some(expected) = transcript.expected else { continue };
        if expected.outcome != Outcome::Done {
            continue;
        }
        let document = write(&expected.tokens, &WRITER).expect("a document read whole is written");
        assert!(document.len() <= transcript.document.len(), "{}: compact, and no longer", transcript.name);
        for seed in 0..4 {
            let decoded = read_back(&document, seed);
            assert_eq!(decoded, expected, "{}, seed {seed}", transcript.name);
        }
    }
}

#[test]
fn generated_documents_are_written_and_read_back_the_same() {
    for seed in 0..60 {
        let mut rng = Rng::new(seed);
        let tokens = generate::tokens(&mut rng, Shape { depth: 5, width: 5, string: 12 });
        let document = write(&tokens, &WRITER).expect("generated text is UTF-8, its numbers numbers");
        let expected = Decoded { tokens, outcome: Outcome::Done };
        assert_eq!(reference::parse(&document, &READER), expected, "seed {seed}: the reference reads it too");
        assert_eq!(read_back(&document, seed), expected, "seed {seed}");
    }
}

#[test]
fn every_byte_a_string_may_hold_is_written_and_read_back() {
    let mut text = Vec::new();
    for byte in 0..0x80 {
        text.push(byte);
    }
    text.extend_from_slice("é€😀\u{7ff}\u{800}\u{ffff}\u{10000}\u{10ffff}".as_bytes());
    let tokens = vec![
        Token::ObjectStart,
        Token::Key(text.clone().into_boxed_slice()),
        Token::String(text.into_boxed_slice()),
        Token::ObjectEnd,
    ];
    let document = write(&tokens, &WRITER).unwrap();
    assert_eq!(read_back(&document, 1), Decoded { tokens, outcome: Outcome::Done });
}

#[test]
fn what_the_tokenizer_refuses_the_writer_refuses_to_write() {
    let not_utf8 = [Token::String(Box::from(&b"\xC3\x28"[..]))];
    assert_eq!(write(&not_utf8, &WRITER), Err(Refusal::Text));
    let not_a_number = [Token::Number(Box::from(&b"01"[..]))];
    assert_eq!(write(&not_a_number, &WRITER), Err(Refusal::Number));
    let deep = generate::nested(65);
    let decoded = reference::parse(&deep, &Limits { depth: 65, ..READER });
    assert_eq!(write(&decoded.tokens, &WRITER), Err(Refusal::TooDeep));
}
