use super::{Constants, dialect, relationships};
use crate::{Declared, ESCAPE, Rule, TOKEN_BYTES, Violation};
use skein_json::{Compact, document};

// Explicit synthetic arithmetic inputs, not provider constants or captures.
const SYNTHETIC: Constants = Constants {
    response_head: 128,
    response_fields: 12,
    error_bytes: 64,
    detail_bytes: 32,
    metadata: 16,
    smallest_tool: 77,
    smallest_item: 12,
    fixed_tokens: 32,
    fixed_text: 64,
    event_field: 64,
    depth: 16,
};

const DECLARED: Declared =
    Declared { window: 64, output: 32, reasoning_item: 48, tool_payload: 8, calls_per_response: 3, conversations: 2 };

#[test]
fn each_rule_follows_the_declared_quantities_and_explicit_constants() {
    let limits = dialect(&DECLARED, &SYNTHETIC).unwrap();
    assert_eq!(limits.request, 3072);
    assert_eq!(limits.answer, 256);
    assert_eq!(limits.input, 48);
    assert_eq!(limits.reasoning, 48);
    assert_eq!(limits.output_items, 8);
    assert_eq!(limits.strings, 256);
    assert_eq!(limits.tokens, 80);
    assert_eq!(limits.retained, 320);
    let event = 80 * u32::try_from(size_of::<Compact>()).unwrap() + 320;
    assert_eq!(limits.receiving, 256 + 8 * 48 + event);
    assert_eq!(limits.skip, 3072 + 6 * limits.receiving);
    assert_eq!(limits.tools, 3072 / 77);
    assert_eq!(limits.history_items, 256);
    assert_eq!(limits.metadata, 16);
    assert_eq!(limits.depth, 16);
    assert_eq!(limits.http.head, 128);
    assert_eq!(limits.http.headers, 12);
    assert_eq!(limits.sse.line, limits.skip);
    assert_eq!(limits.sse.event, limits.skip);
    assert_eq!(limits.sse.field, 64);
    assert_eq!(limits.error_bytes, 64);
    assert_eq!(limits.detail_bytes, 32);
    assert_eq!(limits.declared_output_tokens, DECLARED.output);
    assert!(!limits.drop_reasoning);
}

#[test]
fn pieces_heads_and_policy_remain_for_endpoint_composition() {
    let limits = dialect(&DECLARED, &SYNTHETIC).unwrap();
    assert_eq!((limits.http.request, limits.http.read, limits.http.send, limits.sse.chunk), (0, 0, 0, 0));
    let other = dialect(&Declared { conversations: 99, ..DECLARED }, &SYNTHETIC).unwrap();
    assert_eq!(limits, other, "conversations belongs to the owner's pool");
}

#[test]
fn overflowing_declarations_name_the_first_rule_without_wrapping() {
    for (declared, rule) in [
        (Declared { window: u32::MAX, ..DECLARED }, Rule::Request),
        (Declared { output: u32::MAX, ..DECLARED }, Rule::Answer),
        (Declared { tool_payload: u32::MAX, ..DECLARED }, Rule::Input),
        (Declared { calls_per_response: u32::MAX, ..DECLARED }, Rule::OutputItems),
        (Declared { reasoning_item: u32::MAX, ..DECLARED }, Rule::Tokens),
        (Declared { calls_per_response: 1_000_000_000, reasoning_item: 3, ..DECLARED }, Rule::Receiving),
        (Declared { window: u32::MAX / (TOKEN_BYTES * ESCAPE), ..DECLARED }, Rule::Skip),
    ] {
        assert_eq!(dialect(&declared, &SYNTHETIC), Err(Violation::Overflow { rule }));
    }
    assert_eq!(
        dialect(&DECLARED, &Constants { fixed_text: u32::MAX, ..SYNTHETIC }),
        Err(Violation::Overflow { rule: Rule::Retained })
    );
    assert_eq!(
        dialect(&DECLARED, &Constants { smallest_tool: 0, ..SYNTHETIC }),
        Err(Violation::Overflow { rule: Rule::Tools })
    );
    assert_eq!(
        dialect(&DECLARED, &Constants { smallest_item: 0, ..SYNTHETIC }),
        Err(Violation::Overflow { rule: Rule::HistoryItems })
    );
}

#[test]
fn retained_event_records_are_priced_before_the_receiving_bound_is_narrowed() {
    let declared = Declared { reasoning_item: u32::MAX / 2, ..DECLARED };
    assert_eq!(dialect(&declared, &SYNTHETIC), Err(Violation::Overflow { rule: Rule::Receiving }));
}

#[test]
fn every_startup_relationship_keeps_its_values_in_the_violation() {
    let limits = dialect(&DECLARED, &SYNTHETIC).unwrap();
    for (changed, expected) in [
        (crate::client::Limits { input: 257, ..limits }, Violation::InputAnswer { input: 257, answer: 256 }),
        (
            crate::client::Limits { output_items: 2, ..limits },
            Violation::OutputItemsCalls { output_items: 2, calls: 3 },
        ),
        (crate::client::Limits { strings: 47, ..limits }, Violation::StringsInput { strings: 47, input: 48 }),
        (
            crate::client::Limits { strings: 48, reasoning: 49, ..limits },
            Violation::StringsReasoning { strings: 48, reasoning: 49 },
        ),
    ] {
        assert_eq!(relationships(&DECLARED, &changed), Err(expected));
    }
    assert_eq!(
        dialect(&Declared { output: 1, tool_payload: 2, ..DECLARED }, &SYNTHETIC),
        Err(Violation::InputAnswer { input: 12, answer: 8 })
    );
}

#[test]
fn drawn_declarations_keep_every_relationship_and_the_whole_event_reservation() {
    for seed in 1..=512 {
        let mut random = skein_lib::Rng::new(seed);
        let output = u32::try_from(random.next_u64() % 1000 + 1).unwrap();
        let declared = Declared {
            window: u32::try_from(random.next_u64() % 1000 + 1).unwrap(),
            output,
            reasoning_item: u32::try_from(random.next_u64() % 1000).unwrap(),
            tool_payload: u32::try_from(random.next_u64() % u64::from(output * TOKEN_BYTES / ESCAPE + 1)).unwrap(),
            calls_per_response: u32::try_from(random.next_u64() % 100).unwrap(),
            conversations: u32::try_from(random.next_u64() % 16).unwrap(),
        };
        let limits = dialect(&declared, &SYNTHETIC).unwrap();
        assert!(limits.input <= limits.answer);
        assert!(limits.output_items >= declared.calls_per_response);
        assert!(limits.strings >= limits.input);
        assert!(limits.strings >= limits.reasoning);
        let event = document::worst_case(&document::Limits { tokens: limits.tokens, text: limits.retained }).unwrap();
        assert!(u64::from(limits.receiving) >= event + u64::from(limits.answer));
        assert!(u64::from(limits.skip) >= u64::from(limits.request) + u64::from(limits.receiving) * u64::from(ESCAPE));
    }
}
