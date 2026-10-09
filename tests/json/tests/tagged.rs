//! Tagged filters across delayed neighbours, against independent span pruning.
use skein_json::collector::{Cap, Event, Filter, Keep, Key, Limits, Node, Tagged, Variant};
use skein_json_world::{
    reference,
    world::{self, Settings},
};
use skein_lib::Rng;

const ITEM: Tagged = Tagged {
    tag: b"type",
    known: &[
        Variant { value: b"small", children: &[Node { key: Key::Field(b"id"), keep: Keep::Value }] },
        Variant { value: b"large", children: &[Node { key: Key::Field(b"body"), keep: Keep::Text(Cap::new(0)) }] },
    ],
    unknown: Cap::new(1),
};
const FILTER: Filter = Filter { root: Keep::Tagged(&ITEM) };

#[test]
fn tagged_objects_settle_with_late_known_unknown_and_missing_tags() {
    let limits = Limits {
        tokenizer: skein_json::tokenizer::Limits { depth: 8, string: 32, number: 8, chunk: 3, length: 4096 },
        tokens: 32,
        text: 64,
        skip: 4096,
    };
    for (index, bytes) in [
        br#"{"type":"small","body":"extension","id":1}"#.as_slice(),
        br#"{"body":"extension","id":1,"type":"small"}"#,
        br#"{"body":"abcdef","type":"large"}"#,
        br#"{"type":"other","body":1}"#,
        br#"{"x":1}"#,
    ]
    .iter()
    .enumerate()
    {
        for seed in 0..8 {
            let settings = Settings::calm(&mut Rng::new(seed), limits.tokenizer);
            let run = world::collect(bytes, FILTER, limits, &settings, seed);
            let (expected, skipped, _) = reference::prune(bytes, FILTER, &limits).unwrap();
            assert_eq!(run.outcome, Some(expected.clone()), "case {index} seed {seed}");
            if matches!(expected, Event::Collected(_)) {
                assert_eq!(run.counts.skipped, skipped);
            }
        }
    }
}
