//! Declared models derive both dialects before endpoint transport composition.
use skein_llm::{Cap, Declared, Endpoint, Error, Limits, Provider, Rule, Violation, client};
use skein_llm_world::{TINY_DECL, World, call, limits_for};

#[test]
fn declared_quantities_price_the_retained_event_and_leave_transport_to_the_owner() {
    let derived = Limits::derive(&TINY_DECL).expect("declaration");
    for (provider, head, fields, fixed_tokens, fixed_text, metadata, tool) in
        [(Provider::OpenAiCodex, 1935, 42, 30, 196, 86, 78), (Provider::Anthropic, 1435, 31, 29, 202, 0, 47)]
    {
        let limits = derived.dialect(provider);
        assert_eq!(limits.http.head, head);
        assert_eq!(limits.http.headers, fields);
        assert_eq!(limits.http.request, 0);
        assert_eq!(limits.http.read, 0);
        assert_eq!(limits.http.send, 0);
        assert_eq!(limits.sse.chunk, 0);
        assert_eq!(limits.request, TINY_DECL.window * 8 * 6);
        assert_eq!(limits.answer, TINY_DECL.output * 8);
        assert_eq!(limits.input, TINY_DECL.tool_payload * 6);
        assert_eq!(limits.reasoning, TINY_DECL.reasoning_item);
        assert_eq!(limits.output_items, TINY_DECL.calls_per_response * 2 + 2);
        assert_eq!(limits.strings, limits.answer.max(limits.reasoning));
        assert_eq!(limits.tokens, fixed_tokens + limits.reasoning);
        assert_eq!(limits.retained, limits.answer.max(limits.input).max(limits.reasoning) + fixed_text);
        let event = skein_json::document::worst_case(&skein_json::document::Limits {
            tokens: limits.tokens,
            text: limits.retained,
        })
        .expect("event document");
        assert_eq!(
            u64::from(limits.receiving),
            u64::from(limits.answer) + u64::from(limits.output_items) * u64::from(limits.reasoning) + event
        );
        assert_eq!(limits.skip, limits.request + 6 * limits.receiving);
        assert_eq!(limits.sse.line, limits.skip);
        assert_eq!(limits.sse.event, limits.skip);
        assert_eq!(limits.tools, limits.request / tool);
        assert_eq!(limits.history_items, limits.request / 12);
        assert_eq!(limits.metadata, metadata);
        assert_eq!(limits.error_bytes, 16 * 1024);
        assert_eq!(limits.detail_bytes, 1024);
        assert_eq!(limits.declared_output_tokens, TINY_DECL.output);
        assert!(!limits.drop_reasoning);
    }
    assert_eq!(Limits::derive(&Declared { conversations: u32::MAX, ..TINY_DECL }), Ok(derived));
    assert_eq!(
        Limits::derive(&Declared { window: u32::MAX, ..TINY_DECL }),
        Err(Violation::Overflow { rule: Rule::Request })
    );
}

#[test]
fn only_the_messages_endpoint_requires_a_nonzero_native_output_cap() {
    let credential = client::CredentialLimits { access_token: 32, account_id: 32 };
    let declared = Declared { output: 0, tool_payload: 0, ..TINY_DECL };
    let derived = Limits::derive(&declared).expect("zero output can describe Codex");
    assert!(client::request_head(&Endpoint::codex(), &credential, &derived.codex).is_ok());
    assert_eq!(
        client::request_head(&Endpoint::anthropic(), &credential, &derived.anthropic),
        Err(Error::Limit { which: Cap::Output, bound: 0 })
    );
}

fn capture(provider: Provider, scenario: &str, file: &str) -> Vec<u8> {
    let dialect = match provider {
        Provider::OpenAiCodex => "openai",
        Provider::Anthropic => "anthropic",
    };
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/skein-llm/src")
            .join(dialect)
            .join("fixtures")
            .join(scenario)
            .join(file),
    )
    .expect("historical capture")
}

fn captured_head(provider: Provider, scenario: &str) -> (Vec<u8>, u32) {
    let source = capture(provider, scenario, "response.headers");
    // Admission for the inert fixture wrapper, independent of client limits.
    let parsed = skein_llm::Json::from_bytes(
        &source,
        &skein_llm::DocumentLimits { bytes: 16384, strings: 16384, tokens: 4096, depth: 16 },
    )
    .expect("capture wrapper");
    let document = parsed.document();
    let mut head = b"HTTP/1.1 200 OK\r\n".to_vec();
    let mut fields = 0;
    for index in 0..document.len() {
        let token = document.token(index).expect("record");
        if token.kind != skein_json::Kind::Key {
            continue;
        }
        let value = document.token(index + 1).expect("value");
        match document.text(token).expect("key") {
            b"status" => assert_eq!(document.text(value), Some(b"200".as_slice())),
            b"name" => {
                fields += 1;
                head.extend_from_slice(document.text(value).expect("name"));
                head.extend_from_slice(b": ");
            }
            b"value" => {
                head.extend_from_slice(document.text(value).expect("value"));
                head.extend_from_slice(b"\r\n");
            }
            _ => {}
        }
    }
    head.extend_from_slice(b"\r\n");
    (head, fields)
}

#[test]
fn archived_heads_reach_each_derived_bound_and_complete_with_derived_limits() {
    for provider in [Provider::OpenAiCodex, Provider::Anthropic] {
        let bounds = limits_for(provider);
        let mut maximum = 0;
        let mut maximum_fields = 0;
        let scenarios: &[&str] = match provider {
            Provider::OpenAiCodex => &["single-text", "tool-call", "tool-result-final"],
            Provider::Anthropic => &["single-text", "tool-call", "parallel-tool-calls", "tool-result-final"],
        };
        for scenario in scenarios {
            let (mut wire, fields) = captured_head(provider, scenario);
            maximum = maximum.max(u32::try_from(wire.len()).expect("capture length"));
            maximum_fields = maximum_fields.max(fields);
            wire.extend_from_slice(&capture(provider, scenario, "response.sse"));
            let mut input = call(1);
            if provider == Provider::Anthropic {
                input.endpoint = Endpoint::anthropic();
                input.credential = skein_llm::Credential::anthropic(b"archive-test".as_slice().into());
                input.prompt.affinity = None;
                input.prompt.max_output_tokens = Some(TINY_DECL.output);
            }
            let mut world = World::new(input, bounds, wire, 17);
            world.fragmentation(31, 1);
            world.request(client::Request::Start);
            world.run();
            world.assert_once();
            assert!(
                world.seen.iter().any(|event| matches!(event, client::Event::Completed { .. })),
                "{provider:?} {scenario}: {:?}",
                world.seen
            );
        }
        assert_eq!(maximum, bounds.http.head);
        assert_eq!(maximum_fields, bounds.http.headers);
    }
}
