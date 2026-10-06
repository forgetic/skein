//! Independent common codec literals at the lowest tier.
use crate::{
    Direction, First, KindRule, KnownKind, OpeningMode, OpeningProfile, Role, Schema, VersionRule, codec, tests_support,
};

#[test]
fn terms_source_literal_keeps_exact_common_frame() {
    let schema = tests_support::schema();
    let frame = codec::terms(&schema, 1).expect("bounded Terms");
    assert_eq!(
        frame.bytes(),
        &[0, 16, 0, 0, 0, 0, 0, 10, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1],
        "independent single-term bytes"
    );
}

#[test]
fn all_thirty_two_terms_fit_exact_frozen_two_hundred_four_bytes() {
    let mut known = [KnownKind { kind: 257, channel: 1, direction: Direction::InitiatorToResponder }; 32];
    let mut rules = [KindRule { version: 1, kind: 257, body_bytes: 1 }; 32];
    for offset in 0..32_u16 {
        let kind = offset.checked_add(257).expect("bounded literal kind");
        known[usize::from(offset)].kind = kind;
        rules[usize::from(offset)].kind = kind;
    }
    let mut limits = *tests_support::schema().limits();
    limits.known_kinds = 32;
    limits.schema_rows = 32;
    let schema = Schema::new(
        Role::Responder,
        OpeningProfile {
            magic: *b"tmpr",
            channel: 1,
            name_bytes: 64,
            secret_bytes: 64,
            mode: OpeningMode::AskParent,
            ping_allowed: true,
        },
        limits,
        &known,
        &rules,
        &[VersionRule {
            version: 1,
            unknown_body_bytes: 512,
            first: First { receive: None, send: None, initial_read: true, ping_before_receive: false },
        }],
    )
    .expect("maximum Terms schema");
    let frame = codec::terms(&schema, 1).expect("all maximum terms");
    assert_eq!(frame.bytes().len(), 204, "exact frozen maximum Terms frame");
    assert_eq!(
        &frame.bytes()[..12],
        &[0, 16, 0, 0, 0, 0, 0, 196, 0, 0, 0, 32],
        "independent maximum Terms header/count"
    );
    assert_eq!(&frame.bytes()[198..], &[1, 32, 0, 0, 0, 1], "independent last kind and body bound");
}
