//! The tokenizer's and the writer's worst cases against the counting
//! allocator (json.md, 3.3 and 4; programming-model.md, 6.3): every call of
//! an entry point a step of the meter, and what the machine held of its own
//! never more than its `worst_case`.
//!
//! What the connection holds is made before the meter: the queues, the
//! stream below and its buffer. Each delivery is made between steps, by the
//! stream below, and is the tokenizer's to count from when it is handed over
//! (lib.md, 7). What a step emits, a token's box among it, is handed out:
//! dropped before the check, as its receiver's.

use skein_heap::{Counting, Meter};
use skein_json::Token;
use skein_json::tokenizer::{self as json, Event, Limits, Request, Tokenizer};
use skein_json::writer::{self, Encoder};
use skein_json_world::generate::{self, Shape};
use skein_json_world::reference;
use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue, Rng, Time, Wall};

#[global_allocator]
static HEAP: Counting = Counting;

/// What a run does besides reading the document to its outcome.
#[derive(Clone, Copy, Debug)]
enum Interrupt {
    /// Nothing: read to the outcome, then close.
    Nothing,
    /// Close after this many steps, whatever the tokenizer is doing.
    Close(usize),
    /// Fail the stream after this many steps.
    Fail(usize),
}

/// What a run came to: the most the tokenizer held of its own in a step,
/// the tokens it sent up, and its outcome, if it reached one.
#[derive(Debug)]
struct Ran {
    most: u64,
    tokens: usize,
    outcome: Option<Event>,
}

/// Reads `document` with a tokenizer under `limits`, through a stream that
/// holds all of it and meets each demand from it, each call a step of the
/// meter checked against `worst_case`, and closes it.
fn read(document: &[u8], limits: Limits, interrupt: Interrupt) -> Ran {
    read_demand(document, limits, interrupt, Request::Next)
}

fn read_demand(document: &[u8], limits: Limits, interrupt: Interrupt, first: Request) -> Ran {
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(json::UP_MAX_OUT.above.max(json::DOWN_MAX_OUT.above));
    let mut below = Queue::with_capacity(json::UP_MAX_OUT.below.max(json::DOWN_MAX_OUT.below));
    let cap = u32::try_from(document.len()).expect("a short document").max(json::largest_demand(&limits));
    let mut intake = Intake::with_capacity(cap);
    intake.append(document).expect("the stream holds all of it");
    let what = format!("{limits:?} reading {}", document.escape_ascii());
    let bound = json::worst_case(&limits).expect("the limits are honoured");

    let meter = Meter::new();
    meter.start();
    let mut tokenizer = Tokenizer::new(&limits);
    let mut most = meter.check(meter.end(), bound, &what);
    let mut run = Ran { most: 0, tokens: 0, outcome: None };
    let mut demanded: Option<Read> = None;
    let mut steps = 0;
    loop {
        // Between steps, the stream below makes what it delivers.
        let (ev, rq) = match (interrupt, demanded.take()) {
            (Interrupt::Close(at), _) if steps == at => (None, Request::Close),
            (Interrupt::Fail(at), _) if steps == at => (Some(Up::Failed(Fault::Reset)), Request::Next),
            (_, Some(read)) => match intake.meet(read) {
                Some(bytes) => (Some(Up::Bytes(bytes)), Request::Next),
                None => (Some(Up::End), Request::Next),
            },
            (_, None) if run.outcome.is_some() => (None, Request::Close),
            (_, None) => (None, if steps == 0 { first } else { Request::Next }),
        };
        let closing = ev.is_none() && rq == Request::Close;
        meter.start();
        match ev {
            Some(ev) => json::up(&mut tokenizer, &env, ev, &mut above, &mut below),
            None => json::down(&mut tokenizer, &env, rq, &mut above, &mut below),
        }
        let step = meter.end();
        // What the step emitted is handed out, its tokens dropped.
        let event = above.pop();
        let request = below.pop();
        let closed = matches!(event, Some(Event::Closed));
        match event {
            Some(Event::Token(token)) => {
                run.tokens += 1;
                drop(token);
            }
            Some(outcome @ (Event::Done | Event::Failed(_))) => run.outcome = Some(outcome),
            Some(Event::Long(_) | Event::Skipped(_) | Event::Closed) | None => {}
        }
        match request {
            Some(Down::Demand { read: Read::Nothing, .. }) => assert!(closing, "only a close withdraws"),
            Some(Down::Demand { read, .. }) => demanded = Some(read),
            Some(other) => panic!("the tokenizer sent {other:?}"),
            None => {}
        }
        most = most.max(meter.check(step, bound, &what));
        steps += 1;
        if closed {
            run.most = most;
            return run;
        }
        assert!(steps < 8 * document.len() + 64, "{what}: a document is read in a few steps a byte");
    }
}

/// `[" + text + "]`
fn string_in_an_array(text: &[u8]) -> Vec<u8> {
    let mut document = b"[\"".to_vec();
    document.extend_from_slice(text);
    document.extend_from_slice(b"\"]");
    document
}

#[test]
fn the_delivery_being_read_counts_with_a_full_text() {
    // A full text, and the last scan's quote delivered: the stack, the text
    // and the delivery are held at once.
    let limits = Limits { depth: 4, string: 64, number: 8, chunk: 32, length: 4096 };
    let run = read(&string_in_an_array(&[b'a'; 64]), limits, Interrupt::Nothing);
    assert_eq!(run.outcome, Some(Event::Done));
    assert!(run.most > 4 + 64, "the delivery is held with the stack and the text: {}", run.most);
}

#[test]
fn every_entry_point_holds_no_more_than_its_worst_case_at_its_limits() {
    let limits = Limits { depth: 6, string: 24, number: 12, chunk: 24, length: 4096 };
    let full_text = "aé€😀".repeat(2) + "abcd";
    assert_eq!(full_text.len(), 24);
    let mut deepest = generate::nested(6);
    deepest.push(b' ');
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (string_in_an_array(full_text.as_bytes()), "a string at its limit, a scan of the chunk"),
        (format!("{{\"{full_text}\":[{}]}}", "1".repeat(12)).into_bytes(), "a key and a number at their limits"),
        (deepest, "nesting at its depth"),
        (b"[false,true,null,-1.5e+10]".to_vec(), "literals, the longest demand of four"),
        (generate::nested(7), "nesting past its depth"),
        (string_in_an_array(&[b'a'; 25]), "a string past its limit"),
        (b"[1234567890123]".to_vec(), "a number past its limit"),
        (string_in_an_array(b"\\ud83d\\ude00\\u00e9"), "escapes"),
        (string_in_an_array(&[b'a'; 20]).split_at(12).0.to_vec(), "a document cut short in a string"),
        (b"[\"a\" x]".to_vec(), "a document that fails"),
    ];
    for (document, what) in &cases {
        let whole = read(document, limits, Interrupt::Nothing);
        let expected = reference::parse(document, &limits);
        assert_eq!(whole.tokens, expected.tokens.len(), "{what}: the reference's tokens");
        for at in 0..document.len() + 4 {
            let _ = read(document, limits, Interrupt::Close(at));
            let _ = read(document, limits, Interrupt::Fail(at));
        }
    }
}

#[test]
fn generated_documents_under_tiny_limits_hold_no_more_than_the_worst_case() {
    for seed in 0..200 {
        let mut rng = Rng::new(0x4E_A900 + seed);
        let tokens = generate::tokens(&mut rng, Shape { depth: 4, width: 4, string: 8 });
        let mut document = generate::render(&mut rng, &tokens);
        if rng.chance(500) {
            document = generate::mutate(&mut rng, &document);
        }
        let limits = skein_json_world::world::limits(&mut rng);
        let _ = read(&document, limits, Interrupt::Nothing);
    }
}

/// Writes `tokens` through both passes under `limits`, each call a step of
/// the meter checked against `worst_case`: the most the encoder held of its
/// own in a step. The document `finish` returns is handed out.
fn write(tokens: &[Token], limits: writer::Limits) -> u64 {
    let what = format!("{limits:?} writing {tokens:?}");
    let bound = writer::worst_case(&limits).expect("the limits are priced");
    let meter = Meter::new();
    meter.start();
    let mut measure = Encoder::measure(&limits);
    let mut most = meter.check(meter.end(), bound, &what);
    for token in tokens {
        meter.start();
        measure.token(token);
        most = most.max(meter.check(meter.end(), bound, &what));
    }
    meter.start();
    let len = measure.measured().expect("generated documents are written");
    most = most.max(meter.check(meter.end(), bound, &what));
    meter.start();
    let mut write = Encoder::write(len, &limits);
    most = most.max(meter.check(meter.end(), bound, &what));
    for token in tokens {
        meter.start();
        write.token(token);
        most = most.max(meter.check(meter.end(), bound, &what));
    }
    meter.start();
    let document = write.finish();
    let step = meter.end();
    drop(document);
    most.max(meter.check(step, bound, &what))
}

#[test]
fn the_writer_holds_no_more_than_its_worst_case_at_its_limits() {
    for seed in 0..100 {
        let mut rng = Rng::new(0x4E_A9A0 + seed);
        let tokens = generate::tokens(&mut rng, Shape { depth: 5, width: 5, string: 12 });
        // The document's own depth and length are its limits.
        let roomy = writer::Limits { depth: 8, length: 1 << 20 };
        let len = skein_json_world::write(&tokens, &roomy).expect("generated documents are written").len();
        let mut depth = 0_u32;
        let mut deepest = 0;
        for token in &tokens {
            match token {
                Token::ObjectStart | Token::ArrayStart => depth += 1,
                Token::ObjectEnd | Token::ArrayEnd => depth -= 1,
                Token::Key(_) | Token::String(_) | Token::Number(_) | Token::True | Token::False | Token::Null => {}
            }
            deepest = deepest.max(depth);
        }
        let limits = writer::Limits { depth: deepest, length: u32::try_from(len).unwrap() };
        let most = write(&tokens, limits);
        assert_eq!(most, writer::worst_case(&limits).unwrap(), "seed {seed}: the writing pass holds all of it");
    }
}

#[test]
fn skipped_megabytes_and_long_text_keep_the_same_bounded_buffer() {
    let limits = Limits { depth: 4, string: 8, number: 8, chunk: 32, length: 2 << 20 };
    let mut large = vec![b'a'; 1 << 20];
    large.insert(0, b'"');
    large.push(b'"');
    for request in [Request::Skip, Request::Text(4)] {
        let small_document = format!("\"{}\"", "a".repeat(64));
        let small = read_demand(small_document.as_bytes(), limits, Interrupt::Nothing, request);
        let large = read_demand(&large, limits, Interrupt::Nothing, request);
        assert_eq!(small.outcome, Some(Event::Done));
        assert_eq!(large.outcome, Some(Event::Done));
        assert_eq!(small.tokens, 0);
        assert_eq!(large.tokens, 0);
        assert_eq!(large.most, small.most, "discarded text adds no retained memory");
    }
}

#[test]
fn a_compact_document_at_its_counts_costs_its_text_and_one_record_per_token() {
    use skein_json::document::{self, Limits};
    for seed in 0..100 {
        let mut rng = Rng::new(seed);
        let tokens = generate::tokens(&mut rng, Shape { depth: 5, width: 5, string: 12 });
        let text = tokens
            .iter()
            .map(|token| match token {
                Token::Key(bytes) | Token::String(bytes) | Token::Number(bytes) => bytes.len(),
                Token::ObjectStart
                | Token::ObjectEnd
                | Token::ArrayStart
                | Token::ArrayEnd
                | Token::True
                | Token::False
                | Token::Null => 0,
            })
            .sum::<usize>();
        let limits = Limits { tokens: tokens.len().try_into().unwrap(), text: text.try_into().unwrap() };
        let bound = document::worst_case(&limits).unwrap();
        let meter = Meter::new();
        meter.start();
        let document = skein_json::Document::from_tokens(&tokens, &limits).unwrap();
        let measured = meter.end();
        assert_eq!(meter.check(measured, bound, &"compact document at both counts"), bound);
        drop(document);
    }
}
