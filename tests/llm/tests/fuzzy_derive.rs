//! Seeded declarations hold each relationship and independently priced reservation.
use skein_lib::Rng;
use skein_llm::{Declared, Limits, Provider, Violation};

#[test]
fn ten_thousand_declarations_preserve_all_client_relationships() {
    let mut rng = Rng::new(0xdec1_a4ed);
    for _ in 0..10_000 {
        let declared = Declared {
            window: u32::try_from(rng.between(1, 65536)).unwrap(),
            output: u32::try_from(rng.between(0, 4096)).unwrap(),
            reasoning_item: u32::try_from(rng.between(0, 4096)).unwrap(),
            tool_payload: u32::try_from(rng.between(0, 8192)).unwrap(),
            calls_per_response: u32::try_from(rng.between(0, 32)).unwrap(),
            conversations: u32::try_from(rng.between(0, 65536)).unwrap(),
        };
        match Limits::derive(&declared) {
            Ok(derived) => {
                for provider in [Provider::OpenAiCodex, Provider::Anthropic] {
                    let bounds = derived.dialect(provider);
                    assert!(bounds.input <= bounds.answer);
                    assert!(bounds.output_items >= declared.calls_per_response);
                    assert!(bounds.strings >= bounds.input);
                    assert!(bounds.strings >= bounds.reasoning);
                    let event = skein_json::document::worst_case(&skein_json::document::Limits {
                        tokens: bounds.tokens,
                        text: bounds.retained,
                    })
                    .unwrap();
                    assert_eq!(
                        u64::from(bounds.receiving),
                        u64::from(bounds.answer) + u64::from(bounds.output_items) * u64::from(bounds.reasoning) + event
                    );
                    assert_eq!(u64::from(bounds.skip), u64::from(bounds.request) + 6 * u64::from(bounds.receiving));
                }
            }
            Err(Violation::InputAnswer { input, answer }) => {
                assert_eq!(input, declared.tool_payload * 6);
                assert_eq!(answer, declared.output * 8);
                assert!(input > answer);
            }
            Err(
                Violation::Overflow { .. }
                | Violation::OutputItemsCalls { .. }
                | Violation::StringsInput { .. }
                | Violation::StringsReasoning { .. },
            ) => panic!("bounded declarations cannot reach this failure"),
        }
    }
}
