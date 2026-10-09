//! Selective collection over generated paths and corrupt peer extensions.
use skein_json::Token;
use skein_json::collector::{Cap, Event, Filter, Keep, Key, Limits, Node};
use skein_json_world::{
    generate::{self, Shape},
    reference,
    world::{self, Settings},
};
use skein_lib::Rng;
use std::collections::BTreeSet;
const CHILDREN: &[Node] = &[
    Node { key: Key::Field(b"keep"), keep: Keep::Value },
    Node { key: Key::Field(b"text"), keep: Keep::Text(Cap::new(0)) },
    Node { key: Key::Field(b"array"), keep: Keep::Into(&[Node { key: Key::Each, keep: Keep::Text(Cap::new(1)) }]) },
    Node {
        key: Key::Field(b"nested"),
        keep: Keep::Into(&[Node { key: Key::Field(b"text"), keep: Keep::Text(Cap::new(2)) }]),
    },
    Node { key: Key::Field(b"absent"), keep: Keep::Value },
    Node { key: Key::Each, keep: Keep::Text(Cap::new(0)) },
];
const FILTERS: &[Keep] =
    &[Keep::Value, Keep::Text(Cap::new(2)), Keep::Text(Cap::new(1)), Keep::Into(&[]), Keep::Into(CHILDREN)];
const NAMES: &[&[u8]] = &[b"keep", b"text", b"array", b"nested", b"omit"];
#[test]
fn ten_thousand_generated_mutated_and_extended_documents_obey_drawn_filters() {
    let mut seen = BTreeSet::new();
    for seed in 0..10_000 {
        let mut rng = Rng::new(0xC011_EC70 + seed);
        let mut tokens = generate::tokens(&mut rng, Shape { depth: 4, width: 4, string: 8 });
        for token in &mut tokens {
            if matches!(token, Token::Key(_)) {
                *token =
                    Token::Key(NAMES[usize::try_from(rng.below(NAMES.len() as u64)).expect("a name index")].into());
            }
        }
        let mut document = generate::render(&mut rng, &tokens);
        let filter =
            Filter { root: FILTERS[usize::try_from(rng.below(FILTERS.len() as u64)).expect("a filter index")] };
        seen.insert(format!("{:?}", filter.root));
        let mut limits = Limits {
            tokenizer: skein_json::tokenizer::Limits {
                depth: 16,
                string: 128,
                number: 64,
                chunk: u32::try_from(rng.between(1, 32)).expect("small chunk"),
                length: 1 << 20,
            },
            tokens: 1024,
            text: 8192,
            skip: 1 << 20,
        };
        if rng.chance(300) {
            match rng.below(3) {
                0 => limits.tokens = u32::try_from(rng.below(12)).expect("small count"),
                1 => limits.text = u32::try_from(rng.below(24)).expect("small count"),
                _ => limits.skip = rng.below(24),
            }
        }
        if rng.chance(200) {
            document = generate::mutate(&mut rng, &document);
        }
        let settings = Settings::calm(&mut rng, limits.tokenizer);
        let run = world::collect(&document, filter, limits, &settings, seed);
        let outcome = run.outcome.expect("test value fits its admitted bounds");
        if let Some((expected, skipped, kinds)) = reference::prune(&document, filter, &limits) {
            for kind in kinds {
                seen.insert(format!("skip {:?}", core::mem::discriminant(&kind)));
            }
            assert_eq!(outcome, expected, "seed {seed}: {}", document.escape_ascii());
            if matches!(outcome, Event::Collected(_)) {
                assert_eq!(run.counts.skipped, skipped, "seed {seed}");
            }
        } else {
            assert!(matches!(outcome, Event::Failed(_)), "seed {seed}: corrupt JSON cannot collect");
        }
        match &outcome {
            Event::Collected(document) => {
                seen.insert("collected".into());
                for i in 0..document.len() {
                    if document.token(i).expect("test value fits its admitted bounds").kind == skein_json::Kind::Long {
                        seen.insert("long".into());
                    }
                }
            }
            Event::Failed(error) => {
                seen.insert(format!("{error:?}"));
            }
            Event::Closed => unreachable!(),
        }
        if seed % 10 == 0 {
            unchanged_extension(&mut rng, seed, limits, &settings);
        }
    }
    for what in ["collected", "long", "TooManyTokens", "TooMuchText { cap: None }", "SkippedTooLong", "Duplicate"] {
        assert!(seen.contains(what), "{what} fell: {seen:?}");
    }
    for value in [
        Token::ObjectStart,
        Token::ArrayStart,
        Token::String(b"".as_slice().into()),
        Token::Number(b"0".as_slice().into()),
        Token::True,
        Token::False,
        Token::Null,
    ] {
        assert!(
            seen.contains(&format!("skip {:?}", core::mem::discriminant(&value))),
            "each skipped value kind fell: {seen:?}"
        );
    }
    for keep in FILTERS {
        assert!(seen.contains(&format!("{keep:?}")));
    }
}

fn unchanged_extension(rng: &mut Rng, seed: u64, limits: Limits, settings: &Settings) {
    let extension =
        generate::render(rng, &generate::tokens(&mut Rng::new(seed), Shape { depth: 3, width: 3, string: 12 }));
    let plain = br#"{"keep":7,"text":"abcd"}"#;
    let mut added = br#"{"keep":7,"text":"abcd","not_named":"#.to_vec();
    added.extend_from_slice(&extension);
    added.push(b'}');
    let roomy = Limits { tokens: 1024, text: 8192, skip: 1 << 20, ..limits };
    let selective = Filter { root: Keep::Into(CHILDREN) };
    assert_eq!(
        world::collect(plain, selective, roomy, settings, seed).outcome,
        world::collect(&added, selective, roomy, settings, seed).outcome
    );
}

#[test]
fn tagged_projections_ignore_random_extensions_and_field_order() {
    use skein_json::collector::{Tagged, Variant};
    const TAGGED: Tagged = Tagged {
        tag: b"type",
        known: &[
            Variant { value: b"small", children: &[Node { key: Key::Field(b"id"), keep: Keep::Value }] },
            Variant { value: b"large", children: &[Node { key: Key::Field(b"body"), keep: Keep::Text(Cap::new(0)) }] },
        ],
        unknown: Cap::new(1),
    };
    let filter = Filter { root: Keep::Tagged(&TAGGED) };
    let mut long = false;
    let mut omitted = false;
    for seed in 0..1_000 {
        let mut rng = Rng::new(seed + 0x007A_66ED);
        let extension = generate::render(
            &mut rng,
            &generate::tokens(&mut Rng::new(seed), Shape { depth: 3, width: 3, string: 12 }),
        );
        let tag = if rng.chance(500) { "small" } else { "large" };
        let body = "x".repeat(usize::try_from(rng.below(32)).expect("small body"));
        let mut fields = [
            format!("\"type\":\"{tag}\""),
            "\"id\":1".into(),
            format!("\"body\":\"{body}\""),
            format!("\"extension\":{}", String::from_utf8(extension).unwrap()),
        ];
        for index in (1..fields.len()).rev() {
            fields.swap(index, usize::try_from(rng.below((index + 1) as u64)).expect("field index"));
        }
        let document = format!("{{{}}}", fields.join(","));
        let limits = Limits {
            tokenizer: skein_json::tokenizer::Limits {
                depth: 8,
                string: 32,
                number: 64,
                chunk: u32::try_from(rng.between(1, 8)).expect("small chunk"),
                length: 4096,
            },
            tokens: 6,
            text: 13,
            skip: 4096,
        };
        let settings = Settings::calm(&mut rng, limits.tokenizer);
        let run = world::collect(document.as_bytes(), filter, limits, &settings, seed);
        let (expected, skipped, _) = reference::prune(document.as_bytes(), filter, &limits).unwrap();
        assert_eq!(run.outcome, Some(expected.clone()), "seed {seed}: {document}");
        if let Event::Collected(document) = expected {
            assert_eq!(run.counts.skipped, skipped, "seed {seed}: {document:?}");
            long |= (0..document.len()).any(|index| document.token(index).unwrap().kind == skein_json::Kind::Long);
            omitted |= tag == "small";
        }
    }
    assert!(long && omitted);
}
