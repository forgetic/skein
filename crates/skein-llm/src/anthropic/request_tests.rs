use super::{DEFAULT_MAX_TOKENS, encode_request, measure_request};
use crate::{Block, Error, Json, Message, Prompt, Provider, Replay, Role, Tool, openai};
use alloc::boxed::Box;
use skein_lib::bytes;

const LIMITS: openai::Limits = openai::Limits {
    request_bytes: 8192,
    document_bytes: 8192,
    string_bytes: 2048,
    depth: 16,
    tokens: 1024,
    parts: 16,
    input_bytes: 2048,
    opaque_bytes: 2048,
    answer_bytes: 4096,
    detail_bytes: 256,
};

fn prompt(role: Role, block: Block) -> Prompt {
    Prompt {
        model: bytes::copy_of(b"claude-sonnet-4-6"),
        instructions: bytes::copy_of(b""),
        tools: Box::new([]),
        messages: Box::new([Message { role, content: Box::new([block]) }]),
        max_output_tokens: None,
        reasoning_effort: None,
        cache_key: None,
    }
}

fn text(value: &[u8]) -> Block {
    Block::Text { text: bytes::copy_of(value), replay: None }
}

fn reasoning(value: &[u8]) -> Block {
    Block::Reasoning {
        replay: Replay {
            provider: Provider::Anthropic,
            value: Json::from_bytes(value, &LIMITS).expect("reasoning JSON"),
        },
    }
}

#[test]
fn minimal_request_is_native_streaming_messages_with_a_bounded_output_cap() {
    let prompt = prompt(Role::User, text(b"hello\n\"Claude\""));
    let wire = encode_request(&prompt, &LIMITS).expect("bounded request");
    assert_eq!(
        wire.as_ref(),
        br#"{"model":"claude-sonnet-4-6","max_tokens":4096,"stream":true,"messages":[{"role":"user","content":[{"type":"text","text":"hello\n\"Claude\""}]}]}"#
    );
    assert_eq!(DEFAULT_MAX_TOKENS, 4096);
    assert_eq!(
        usize::try_from(measure_request(&prompt, &LIMITS).expect("measurement")).expect("u32 fits usize"),
        wire.len()
    );
}

#[test]
fn signed_and_redacted_reasoning_tool_calls_and_native_error_results_replay_in_order() {
    let signed = br#"{"type":"thinking","thinking":"consider","signature":"opaque-signature"}"#;
    let redacted = br#"{"type":"redacted_thinking","data":"opaque-redacted"}"#;
    let mut prompt = prompt(Role::User, text(b"read"));
    prompt.instructions = bytes::copy_of(b"actual client instructions");
    prompt.max_output_tokens = Some(9000);
    let schema = Json::from_bytes(
        br#"{"type":"object","properties":{"path":{"type":"string"}},"additionalProperties":false}"#,
        &LIMITS,
    )
    .expect("tool schema");
    prompt.tools =
        Box::new([Tool { name: bytes::copy_of(b"read"), description: bytes::copy_of(b"read a file"), schema }]);
    prompt.messages = Box::new([
        Message { role: Role::User, content: Box::new([text(b"read")]) },
        Message {
            role: Role::Assistant,
            content: Box::new([
                reasoning(signed),
                reasoning(redacted),
                text(b"checking"),
                Block::ToolCall {
                    id: bytes::copy_of(b"toolu_1"),
                    name: bytes::copy_of(b"read"),
                    arguments: bytes::copy_of(br#"{"path":"file"}"#),
                    replay: None,
                },
            ]),
        },
        Message {
            role: Role::User,
            content: Box::new([Block::ToolResult {
                id: bytes::copy_of(b"toolu_1"),
                text: bytes::copy_of(b"denied"),
                is_error: true,
            }]),
        },
    ]);
    let wire = encode_request(&prompt, &LIMITS).expect("replay encodes");
    assert_eq!(wire.as_ref(), br#"{"model":"claude-sonnet-4-6","max_tokens":9000,"stream":true,"system":"actual client instructions","tools":[{"name":"read","description":"read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}},"additionalProperties":false}}],"messages":[{"role":"user","content":[{"type":"text","text":"read"}]},{"role":"assistant","content":[{"type":"thinking","thinking":"consider","signature":"opaque-signature"},{"type":"redacted_thinking","data":"opaque-redacted"},{"type":"text","text":"checking"},{"type":"tool_use","id":"toolu_1","name":"read","input":{"path":"file"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"denied","is_error":true}]}]}"#);
}

#[test]
fn effort_is_explicit_and_unsupported_fields_are_rejected() {
    for effort in [b"low".as_slice(), b"medium", b"high", b"max"] {
        let mut prompt = prompt(Role::User, text(b"hello"));
        prompt.reasoning_effort = Some(bytes::copy_of(effort));
        let wire = encode_request(&prompt, &LIMITS).expect("supported adaptive effort");
        assert!(bytes::find(&wire, br#""thinking":{"type":"adaptive"}"#).is_some(), "effort enables adaptive thinking");
        assert!(bytes::find(&wire, br#""output_config":{"effort":""#).is_some(), "effort goes into output_config");
    }
    let mut prompt = prompt(Role::User, text(b"hello"));
    prompt.reasoning_effort = Some(bytes::copy_of(b"off"));
    let wire = encode_request(&prompt, &LIMITS).expect("thinking disabled");
    assert!(bytes::find(&wire, br#""thinking":{"type":"disabled"}"#).is_some(), "off disables thinking");
    assert_eq!(bytes::find(&wire, b"output_config"), None);
    prompt.reasoning_effort = Some(bytes::copy_of(b"unknown"));
    assert_eq!(measure_request(&prompt, &LIMITS), Err(Error::Unsupported));
    prompt.reasoning_effort = None;
    prompt.cache_key = Some(bytes::copy_of(b"affinity"));
    assert_eq!(measure_request(&prompt, &LIMITS), Err(Error::Unsupported));
    prompt.cache_key = None;
    prompt.max_output_tokens = Some(0);
    assert_eq!(measure_request(&prompt, &LIMITS), Err(Error::Invalid));
}

#[test]
fn malformed_unsigned_unknown_and_duplicate_reasoning_cannot_replay() {
    for value in [
        br#"{"type":"thinking","thinking":"a"}"#.as_slice(),
        br#"{"type":"thinking","thinking":"a","signature":""}"#,
        br#"{"type":"thinking","thinking":"a","signature":"s","unknown":"discarded"}"#,
        br#"{"type":"thinking","thinking":"a","signature":"s","signature":"t"}"#,
        br#"{"type":"thinking","thinking":1,"signature":"s"}"#,
        br#"{"type":"redacted_thinking","data":""}"#,
        br#"{"type":"redacted_thinking","data":"opaque","unknown":null}"#,
        br#"{"type":"reasoning","encrypted_content":"other-provider"}"#,
    ] {
        assert_eq!(measure_request(&prompt(Role::Assistant, reasoning(value)), &LIMITS), Err(Error::Invalid));
    }
    let replay = Replay {
        provider: Provider::OpenAiCodex,
        value: Json::from_bytes(br#"{"type":"thinking","thinking":"a","signature":"s"}"#, &LIMITS).expect("JSON"),
    };
    assert_eq!(
        measure_request(&prompt(Role::Assistant, Block::Reasoning { replay }), &LIMITS),
        Err(Error::Unsupported)
    );
}

#[test]
fn roles_utf8_object_arguments_and_tool_identifiers_are_checked_before_encoding() {
    let tool = Block::ToolCall {
        id: bytes::copy_of(b"id"),
        name: bytes::copy_of(b"read"),
        arguments: bytes::copy_of(b"{}"),
        replay: None,
    };
    assert_eq!(measure_request(&prompt(Role::User, tool), &LIMITS), Err(Error::Invalid));
    let result = Block::ToolResult { id: bytes::copy_of(b"id"), text: bytes::copy_of(b"result"), is_error: false };
    assert_eq!(measure_request(&prompt(Role::Assistant, result), &LIMITS), Err(Error::Invalid));
    let signed = reasoning(br#"{"type":"thinking","thinking":"a","signature":"s"}"#);
    assert_eq!(measure_request(&prompt(Role::User, signed), &LIMITS), Err(Error::Invalid));
    assert_eq!(measure_request(&prompt(Role::User, text(&[0xff])), &LIMITS), Err(Error::Invalid));
    for arguments in [b"broken".as_slice(), b"[]", b"null"] {
        let tool = Block::ToolCall {
            id: bytes::copy_of(b"id"),
            name: bytes::copy_of(b"read"),
            arguments: bytes::copy_of(arguments),
            replay: None,
        };
        assert_eq!(measure_request(&prompt(Role::Assistant, tool), &LIMITS), Err(Error::Invalid));
    }
    for id in [b"bad|id".as_slice(), b""] {
        let tool = Block::ToolCall {
            id: bytes::copy_of(id),
            name: bytes::copy_of(b"read"),
            arguments: bytes::copy_of(b"{}"),
            replay: None,
        };
        assert_eq!(measure_request(&prompt(Role::Assistant, tool), &LIMITS), Err(Error::Invalid));
    }
    let replay = Replay { provider: Provider::Anthropic, value: Json::from_bytes(b"{}", &LIMITS).expect("JSON") };
    let block = Block::Text { text: bytes::copy_of(b"a"), replay: Some(replay) };
    assert_eq!(measure_request(&prompt(Role::Assistant, block), &LIMITS), Err(Error::Unsupported));
}

#[test]
fn escaped_wire_bytes_string_tokens_blocks_arguments_and_depth_have_hard_bounds() {
    let request = prompt(Role::User, text(b"\\\"\n"));
    let length = measure_request(&request, &LIMITS).expect("full request");
    let mut limits = LIMITS;
    limits.request_bytes = length;
    assert_eq!(measure_request(&request, &limits), Ok(length));
    limits.request_bytes = length.checked_sub(1).expect("nonempty request");
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    limits = LIMITS;
    limits.string_bytes = 2;
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    limits = LIMITS;
    limits.parts = 0;
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    limits = LIMITS;
    limits.depth = 3;
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    let tool = Block::ToolCall {
        id: bytes::copy_of(b"id"),
        name: bytes::copy_of(b"read"),
        arguments: bytes::copy_of(br#"{"a":{}}"#),
        replay: None,
    };
    let prompt = prompt(Role::Assistant, tool);
    limits = LIMITS;
    limits.input_bytes = 2;
    assert_eq!(measure_request(&prompt, &limits), Err(Error::Limit));
    limits = LIMITS;
    limits.tokens = 2;
    assert_eq!(measure_request(&prompt, &limits), Err(Error::Limit));
}

#[test]
fn prebuilt_replay_and_schema_obey_the_current_admission_limits() {
    let signed = br#"{"type":"thinking","thinking":"a","signature":"s"}"#;
    let mut request = prompt(Role::Assistant, reasoning(signed));
    let mut limits = LIMITS;
    limits.opaque_bytes = 10;
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    limits = LIMITS;
    limits.tokens = 2;
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    request.messages = Box::new([Message { role: Role::User, content: Box::new([text(b"hello")]) }]);
    request.tools = Box::new([Tool {
        name: bytes::copy_of(b"custom_tool"),
        description: bytes::copy_of(b"custom description"),
        schema: Json::from_bytes(
            br#"{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}"#,
            &LIMITS,
        )
        .expect("schema JSON"),
    }]);
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    limits = LIMITS;
    limits.document_bytes = 2;
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
}

#[test]
fn assistant_refusal_is_a_native_text_block_and_empty_history_is_invalid() {
    let mut request = prompt(Role::Assistant, Block::Refusal { text: bytes::copy_of(b"no"), replay: None });
    let wire = encode_request(&request, &LIMITS).expect("refusal can appear in assistant history");
    assert!(
        bytes::find(&wire, br#"{"type":"text","text":"no"}"#).is_some(),
        "Anthropic refusals have native text content"
    );
    request.messages = Box::new([]);
    assert_eq!(measure_request(&request, &LIMITS), Err(Error::Invalid));
    request.messages = Box::new([Message { role: Role::User, content: Box::new([]) }]);
    assert_eq!(measure_request(&request, &LIMITS), Err(Error::Invalid));
}

#[test]
fn historical_identity_is_explicit_and_is_the_first_separate_system_block() {
    let extra = b"actual\n\"instructions\"";
    let instructions = super::identity::instructions(extra).expect("bounded identity instructions");
    assert_eq!(
        instructions.as_ref(),
        b"You are Claude Code, Anthropic's official CLI for Claude.\n\nactual\n\"instructions\""
    );
    let mut request = prompt(Role::User, text(b"hello"));
    request.instructions = instructions;
    let wire = encode_request(&request, &LIMITS).expect("explicit identity request");
    assert!(bytes::find(&wire, br#""system":[{"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."},{"type":"text","text":"actual\n\"instructions\""}]"#).is_some(), "identity precedes unchanged caller instructions in a separate block");
    request.instructions = super::identity::instructions(b"").expect("identity alone");
    assert_eq!(request.instructions.as_ref(), super::identity::CLAUDE_CODE_SYSTEM_IDENTITY);
    let wire = encode_request(&request, &LIMITS).expect("identity-only request");
    assert!(
        bytes::find(
            &wire,
            br#""system":[{"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."}]"#
        )
        .is_some(),
        "an empty extra prompt has only the identity block"
    );
    request.instructions = bytes::copy_of(b"You are Claude Code, Anthropic's official CLI for Claude.\nsimilar prefix");
    let wire = encode_request(&request, &LIMITS).expect("generic instructions");
    assert!(
        bytes::find(&wire, br#""system":"You are Claude Code, Anthropic's official CLI for Claude.\nsimilar prefix""#)
            .is_some(),
        "only the exact opt-in separator selects identity blocks"
    );
    request.instructions = bytes::copy_of(extra);
    let wire = encode_request(&request, &LIMITS).expect("actual client instructions");
    assert!(
        bytes::find(&wire, br#""system":"actual\n\"instructions\"""#).is_some(),
        "generic instructions retain their string representation"
    );
    assert_eq!(bytes::find(&wire, super::identity::CLAUDE_CODE_SYSTEM_IDENTITY), None);
}

#[test]
fn explicit_identity_blocks_remain_subject_to_instruction_and_wire_limits() {
    let mut request = prompt(Role::User, text(b"hello"));
    request.instructions = super::identity::instructions(b"extra").expect("identity instructions");
    let length = measure_request(&request, &LIMITS).expect("identity request measurement");
    let mut limits = LIMITS;
    limits.request_bytes = length;
    assert_eq!(measure_request(&request, &limits), Ok(length));
    limits.request_bytes = length.checked_sub(1).expect("nonempty identity request");
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    limits = LIMITS;
    limits.string_bytes = u32::try_from(request.instructions.len().checked_sub(1).expect("nonempty instructions"))
        .expect("small instructions");
    assert_eq!(measure_request(&request, &limits), Err(Error::Limit));
    request.instructions = super::identity::instructions(&[0xff]).expect("owned instructions");
    assert_eq!(measure_request(&request, &LIMITS), Err(Error::Invalid));
}
