//! Bridge between the provider-neutral vocabulary and Codex documents.
use crate::{Block, Error, Failure, Prompt, Provider, Replay, Role, Stop, Usage, openai};
use alloc::boxed::Box;
use skein_json::{Token, writer};
use skein_lib::{List, Writer, bytes};

const TOOL_ERROR: &[u8] = b"Error: ";

pub(crate) fn request(prompt: Prompt, provider: Provider, limits: &openai::Limits) -> Result<openai::Request, Error> {
    if provider != Provider::OpenAiCodex || prompt.max_output_tokens.is_some() {
        return Err(Error::Unsupported);
    }
    validate(&prompt, provider, limits)?;
    let mut tools = List::with_capacity(limits.parts);
    for tool in &prompt.tools {
        let raw = openai::Tool {
            name: tool.name.clone(),
            description: tool.description.clone(),
            schema: tool.schema.clone(),
        };
        if tools.push(raw).is_err() {
            return Err(Error::Limit);
        }
    }
    let mut input = List::with_capacity(limits.parts);
    for message in &prompt.messages {
        for block in &message.content {
            let item = input_block(block, message.role, limits)?;
            if input.push(item).is_err() {
                return Err(Error::Limit);
            }
        }
    }
    let raw = openai::Request {
        model: prompt.model,
        instructions: prompt.instructions,
        tools: tools.into_boxed(),
        input: input.into_boxed(),
        effort: prompt.reasoning_effort,
        prompt_cache_key: prompt.cache_key,
    };
    match openai::measure_request(&raw, limits) {
        Ok(_) => Ok(raw),
        Err(error) => Err(decode(error)),
    }
}

fn validate(prompt: &Prompt, provider: Provider, limits: &openai::Limits) -> Result<(), Error> {
    let count = usize::try_from(limits.parts).expect("u32 fits usize");
    if prompt.tools.len() > count || prompt.messages.len() > count || prompt.model.is_empty() {
        return Err(if prompt.model.is_empty() { Error::Invalid } else { Error::Limit });
    }
    let mut budget: u64 = 0;
    charge_text(&prompt.model, &mut budget, limits)?;
    charge_text(&prompt.instructions, &mut budget, limits)?;
    if let Some(value) = &prompt.reasoning_effort {
        charge_text(value, &mut budget, limits)?;
    }
    if let Some(value) = &prompt.cache_key {
        charge_text(value, &mut budget, limits)?;
    }
    for tool in &prompt.tools {
        if tool.name.is_empty() || tool.schema.as_tokens().first() != Some(&Token::ObjectStart) {
            return Err(Error::Invalid);
        }
        charge_text(&tool.name, &mut budget, limits)?;
        charge_text(&tool.description, &mut budget, limits)?;
        charge_json(&tool.schema, &mut budget, limits)?;
    }
    let mut blocks: usize = 0;
    for message in &prompt.messages {
        blocks = blocks.checked_add(message.content.len()).ok_or(Error::Limit)?;
        if blocks > count {
            return Err(Error::Limit);
        }
        for block in &message.content {
            match block {
                Block::Text { text, replay } => {
                    charge_text(text, &mut budget, limits)?;
                    charge_replay(replay.as_ref(), provider, &mut budget, limits)?;
                    metadata(replay.as_ref(), b"id", b"phase")?;
                    if replay.is_some() && message.role != Role::Assistant {
                        return Err(Error::Invalid);
                    }
                }
                Block::Refusal { text, replay } => {
                    if message.role != Role::Assistant {
                        return Err(Error::Invalid);
                    }
                    charge_text(text, &mut budget, limits)?;
                    charge_replay(replay.as_ref(), provider, &mut budget, limits)?;
                    metadata(replay.as_ref(), b"id", b"phase")?;
                }
                Block::ToolCall { id, name, arguments, replay } => {
                    if message.role != Role::Assistant || id.is_empty() || name.is_empty() {
                        return Err(Error::Invalid);
                    }
                    charge_text(id, &mut budget, limits)?;
                    charge_text(name, &mut budget, limits)?;
                    charge_text(arguments, &mut budget, limits)?;
                    charge_replay(replay.as_ref(), provider, &mut budget, limits)?;
                    metadata(replay.as_ref(), b"item_id", b"item_id")?;
                }
                Block::ToolResult { id, text, is_error } => {
                    if message.role != Role::User || id.is_empty() {
                        return Err(Error::Invalid);
                    }
                    charge_text(id, &mut budget, limits)?;
                    charge_text(text, &mut budget, limits)?;
                    if *is_error {
                        let len = text.len().checked_add(TOOL_ERROR.len()).ok_or(Error::Limit)?;
                        if len > usize::try_from(limits.string_bytes).expect("u32 fits usize") {
                            return Err(Error::Limit);
                        }
                        charge(TOOL_ERROR.len(), &mut budget, limits)?;
                    }
                }
                Block::Reasoning { replay } => {
                    if message.role != Role::Assistant {
                        return Err(Error::Invalid);
                    }
                    charge_replay(Some(replay), provider, &mut budget, limits)?;
                }
            }
        }
    }
    Ok(())
}
fn charge_text(value: &[u8], budget: &mut u64, limits: &openai::Limits) -> Result<(), Error> {
    if value.len() > usize::try_from(limits.string_bytes).expect("u32 fits usize") {
        return Err(Error::Limit);
    }
    let bounded = writer::Limits { depth: limits.depth, length: limits.request_bytes };
    let mut text = writer::Encoder::measure(&bounded);
    text.string(value);
    match text.measured() {
        Ok(_) => charge(value.len(), budget, limits),
        Err(writer::Refusal::Text | writer::Refusal::Number) => Err(Error::Invalid),
        Err(writer::Refusal::TooLong | writer::Refusal::TooDeep) => Err(Error::Limit),
    }
}
fn charge(size: usize, budget: &mut u64, limits: &openai::Limits) -> Result<(), Error> {
    *budget = budget.checked_add(u64::try_from(size).expect("usize fits u64")).ok_or(Error::Limit)?;
    if *budget > u64::from(limits.request_bytes) {
        return Err(Error::Limit);
    }
    Ok(())
}
fn charge_json(value: &openai::Json, budget: &mut u64, limits: &openai::Limits) -> Result<(), Error> {
    if value.as_tokens().len() > usize::try_from(limits.tokens).expect("u32 fits usize") {
        return Err(Error::Limit);
    }
    let mut measure = writer::Encoder::measure(&limits.writer_limits());
    for token in value.as_tokens() {
        // Even empty key/string payloads own a token wrapper after cloning.
        charge(1, budget, limits)?;
        match token {
            Token::Key(text) | Token::String(text) | Token::Number(text) => charge_text(text, budget, limits)?,
            Token::ObjectStart
            | Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::True
            | Token::False
            | Token::Null => {}
        }
        measure.token(token);
    }
    match measure.measured() {
        Ok(_) => Ok(()),
        Err(writer::Refusal::TooLong | writer::Refusal::TooDeep) => Err(Error::Limit),
        Err(writer::Refusal::Text | writer::Refusal::Number) => Err(Error::Invalid),
    }
}
fn charge_replay(
    replay: Option<&Replay>,
    provider: Provider,
    budget: &mut u64,
    limits: &openai::Limits,
) -> Result<(), Error> {
    if let Some(replay) = replay {
        if replay.provider != provider {
            return Err(Error::Unsupported);
        }
        if replay.value.as_tokens().first() != Some(&Token::ObjectStart) {
            return Err(Error::Invalid);
        }
        charge_json(&replay.value, budget, limits)?;
    }
    Ok(())
}
// A replay envelope is tied to the block that owns it. Unknown envelope
// fields cannot be silently discarded; reasoning documents remain opaque.
fn metadata(replay: Option<&Replay>, first: &[u8], second: &[u8]) -> Result<(), Error> {
    let Some(replay) = replay else {
        return Ok(());
    };
    for token in replay.value.as_tokens() {
        match token {
            Token::Key(name) if name.as_ref() == first || name.as_ref() == second => {}
            Token::Key(_) => return Err(Error::Invalid),
            Token::ObjectStart
            | Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::String(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => {}
        }
    }
    for name in [first, second] {
        let tokens = replay.value.as_tokens();
        let field = match openai::json::field(tokens, name) {
            Ok(at) => at,
            Err(error) => return Err(decode(error)),
        };
        if let Some(at) = field {
            match openai::json::value_at(tokens, at) {
                Ok([Token::String(_) | Token::Null]) => {}
                Ok(_) => return Err(Error::Invalid),
                Err(error) => return Err(decode(error)),
            }
        }
    }
    Ok(())
}
fn input_block(block: &Block, role: Role, limits: &openai::Limits) -> Result<openai::Input, Error> {
    match block {
        Block::Text { text, replay } | Block::Refusal { text, replay } => {
            let id = replay_text(replay.as_ref(), b"id")?;
            let phase = replay_text(replay.as_ref(), b"phase")?;
            let refusal = match block {
                Block::Refusal { .. } => true,
                Block::Text { .. } => false,
                Block::ToolCall { .. } | Block::ToolResult { .. } | Block::Reasoning { .. } => {
                    unreachable!("text variants entered this arm")
                }
            };
            Ok(openai::Input::Message {
                role: match role {
                    Role::User => openai::Role::User,
                    Role::Assistant => openai::Role::Assistant,
                },
                text: text.clone(),
                id,
                phase,
                refusal,
            })
        }
        Block::ToolCall { id, name, arguments, replay } => {
            let value = match openai::Json::from_bytes(arguments, limits) {
                Ok(value) => value,
                Err(error) => return Err(decode(error)),
            };
            if value.as_tokens().first() != Some(&Token::ObjectStart) {
                return Err(Error::Invalid);
            }
            Ok(openai::Input::FunctionCall {
                call_id: id.clone(),
                item_id: replay_text(replay.as_ref(), b"item_id")?,
                name: name.clone(),
                arguments: arguments.clone(),
            })
        }
        Block::ToolResult { id, text, is_error } => {
            let output = if *is_error {
                let len = text.len().checked_add(TOOL_ERROR.len()).ok_or(Error::Limit)?;
                let mut output = Writer::new(len);
                output.put(TOOL_ERROR).expect("the error prefix was measured");
                output.put(text).expect("the error result was measured");
                output.finish()
            } else {
                text.clone()
            };
            Ok(openai::Input::FunctionOutput { call_id: id.clone(), output })
        }
        Block::Reasoning { replay } => {
            let tokens = replay.value.as_tokens();
            let kind = match openai::json::required(tokens, b"type") {
                Ok(at) => match openai::json::value_at(tokens, at) {
                    Ok(value) => value,
                    Err(error) => return Err(decode(error)),
                },
                Err(error) => return Err(decode(error)),
            };
            match kind {
                [Token::String(kind)] if kind.as_ref() == b"reasoning" => {
                    Ok(openai::Input::Opaque { value: replay.value.clone() })
                }
                _ => Err(Error::Invalid),
            }
        }
    }
}
fn replay_text(replay: Option<&Replay>, name: &[u8]) -> Result<Option<Box<[u8]>>, Error> {
    let Some(replay) = replay else {
        return Ok(None);
    };
    let tokens = replay.value.as_tokens();
    let field = match openai::json::field(tokens, name) {
        Ok(at) => at,
        Err(error) => return Err(decode(error)),
    };
    let Some(at) = field else {
        return Ok(None);
    };
    match openai::json::value_at(tokens, at) {
        Ok([Token::String(text)]) => Ok(Some(text.clone())),
        Ok([Token::Null]) => Ok(None),
        Ok(_) => Err(Error::Invalid),
        Err(error) => Err(decode(error)),
    }
}

pub(crate) fn part(value: openai::Part, limits: &openai::Limits) -> Result<Block, Error> {
    match value {
        openai::Part::Text { id, phase, text, refusal } => {
            let mut tokens = List::with_capacity(6);
            push(&mut tokens, Token::ObjectStart)?;
            push(&mut tokens, Token::Key(bytes::copy_of(b"id")))?;
            push(&mut tokens, Token::String(id))?;
            if let Some(phase) = phase {
                push(&mut tokens, Token::Key(bytes::copy_of(b"phase")))?;
                push(&mut tokens, Token::String(phase))?;
            }
            push(&mut tokens, Token::ObjectEnd)?;
            let replay = Some(replay(tokens.as_slice(), limits)?);
            if refusal { Ok(Block::Refusal { text, replay }) } else { Ok(Block::Text { text, replay }) }
        }
        openai::Part::Opaque { bytes } => {
            let value = match openai::Json::from_bytes(&bytes, limits) {
                Ok(value) => value,
                Err(error) => return Err(decode(error)),
            };
            Ok(Block::Reasoning { replay: Replay { provider: Provider::OpenAiCodex, value } })
        }
        openai::Part::ToolCall { id, name, input, too_large } => {
            if too_large {
                return Err(Error::Limit);
            }
            let (call_id, replay) = match bytes::find(&id, b"|") {
                Some(at) => {
                    let item_at = at.checked_add(1).ok_or(Error::Limit)?;
                    let item_id = id.get(item_at..).ok_or(Error::Invalid)?;
                    let mut tokens = List::with_capacity(4);
                    push(&mut tokens, Token::ObjectStart)?;
                    push(&mut tokens, Token::Key(bytes::copy_of(b"item_id")))?;
                    push(&mut tokens, Token::String(bytes::copy_of(item_id)))?;
                    push(&mut tokens, Token::ObjectEnd)?;
                    (bytes::copy_of(id.get(..at).ok_or(Error::Invalid)?), Some(replay(tokens.as_slice(), limits)?))
                }
                None => (id, None),
            };
            Ok(Block::ToolCall { id: call_id, name, arguments: input, replay })
        }
    }
}
fn push(tokens: &mut List<Token>, token: Token) -> Result<(), Error> {
    match tokens.push(token) {
        Ok(()) => Ok(()),
        Err(_) => Err(Error::Limit),
    }
}
fn replay(tokens: &[Token], limits: &openai::Limits) -> Result<Replay, Error> {
    match openai::Json::from_tokens(tokens, limits) {
        Ok(value) => Ok(Replay { provider: Provider::OpenAiCodex, value }),
        Err(error) => Err(decode(error)),
    }
}
pub(crate) const fn decode(error: openai::DecodeError) -> Error {
    match error {
        openai::DecodeError::TooLarge => Error::Limit,
        openai::DecodeError::Malformed | openai::DecodeError::Missing | openai::DecodeError::WrongType => {
            Error::Invalid
        }
    }
}
pub(crate) const fn stop(value: openai::Stop) -> Stop {
    match value {
        openai::Stop::EndTurn => Stop::EndTurn,
        openai::Stop::ToolUse => Stop::ToolUse,
        openai::Stop::MaxTokens => Stop::MaxTokens,
        openai::Stop::Refusal => Stop::Refusal,
    }
}
pub(crate) const fn usage(value: openai::Usage) -> Usage {
    Usage {
        input_tokens: value.input_tokens,
        output_tokens: value.output_tokens,
        cache_read_tokens: value.cache_read_tokens,
        cache_write_tokens: value.cache_write_tokens,
    }
}
pub(crate) const fn failure(value: openai::Failure) -> Failure {
    match value {
        openai::Failure::Limit => Failure::Limit,
        openai::Failure::Protocol => Failure::Protocol,
        openai::Failure::Unauthorized => Failure::Unauthorized,
        openai::Failure::Exhausted { retry_after } => Failure::Exhausted { retry_after },
        openai::Failure::RateLimited { retry_after } => Failure::RateLimited { retry_after },
        openai::Failure::Overloaded => Failure::Overloaded,
        openai::Failure::Unavailable => Failure::Unavailable,
        openai::Failure::ContextTooLong => Failure::ContextTooLong,
        openai::Failure::Invalid => Failure::Invalid,
    }
}

#[cfg(test)]
mod tests {
    use super::{failure, part, request, stop, usage};
    use crate::{Block, Error, Failure, Message, Prompt, Provider, Replay, Role, Stop, Usage, openai};
    use alloc::boxed::Box;
    use skein_lib::{Duration, bytes};
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
            model: bytes::copy_of(b"test"),
            instructions: bytes::copy_of(b""),
            tools: Box::new([]),
            messages: Box::new([Message { role, content: Box::new([block]) }]),
            reasoning_effort: None,
            cache_key: None,
            max_output_tokens: None,
        }
    }
    #[test]
    fn text_replays_message_identity_phase_and_refusal() {
        for refusal in [false, true] {
            let block = part(
                openai::Part::Text {
                    id: bytes::copy_of(b"message"),
                    phase: Some(bytes::copy_of(b"commentary")),
                    text: bytes::copy_of(b"answer"),
                    refusal,
                },
                &LIMITS,
            )
            .expect("bounded completed text");
            let raw =
                request(prompt(Role::Assistant, block), Provider::OpenAiCodex, &LIMITS).expect("assistant replay");
            assert_eq!(
                raw.input.as_ref(),
                &[openai::Input::Message {
                    role: openai::Role::Assistant,
                    text: bytes::copy_of(b"answer"),
                    id: Some(bytes::copy_of(b"message")),
                    phase: Some(bytes::copy_of(b"commentary")),
                    refusal,
                }]
            );
            let wire = openai::encode_request(&raw, &LIMITS).expect("replay wire");
            let value = openai::Json::from_bytes(&wire, &LIMITS).expect("valid document");
            assert_eq!(openai::decode_request(&value, &LIMITS).expect("replay decodes"), raw);
        }
    }
    #[test]
    fn tool_replay_separates_call_and_item_identity() {
        let block = part(
            openai::Part::ToolCall {
                id: bytes::copy_of(b"call|item"),
                name: bytes::copy_of(b"read"),
                input: bytes::copy_of(br#"{"path":"a"}"#),
                too_large: false,
            },
            &LIMITS,
        )
        .expect("bounded tool");
        let raw = request(prompt(Role::Assistant, block), Provider::OpenAiCodex, &LIMITS).expect("tool replay");
        assert_eq!(
            raw.input.as_ref(),
            &[openai::Input::FunctionCall {
                call_id: bytes::copy_of(b"call"),
                item_id: Some(bytes::copy_of(b"item")),
                name: bytes::copy_of(b"read"),
                arguments: bytes::copy_of(br#"{"path":"a"}"#),
            }]
        );
        let result = Block::ToolResult {
            id: bytes::copy_of(b"call"),
            text: bytes::copy_of(b"permission denied"),
            is_error: true,
        };
        let raw = request(prompt(Role::User, result), Provider::OpenAiCodex, &LIMITS).expect("tool output replay");
        assert_eq!(
            raw.input.as_ref(),
            &[openai::Input::FunctionOutput {
                call_id: bytes::copy_of(b"call"),
                output: bytes::copy_of(b"Error: permission denied"),
            }]
        );
    }
    #[test]
    fn tool_error_flag_is_visible_on_wire_and_success_is_unchanged() {
        for is_error in [false, true] {
            let block =
                Block::ToolResult { id: bytes::copy_of(b"call"), text: bytes::copy_of(b"result\ntext"), is_error };
            let raw = request(prompt(Role::User, block), Provider::OpenAiCodex, &LIMITS).expect("tool result");
            let wire = openai::encode_request(&raw, &LIMITS).expect("encoded result");
            let json = openai::Json::from_bytes(&wire, &LIMITS).expect("request JSON");
            let decoded = openai::decode_request(&json, &LIMITS).expect("request replay");
            let output = if is_error { b"Error: result\ntext".as_slice() } else { b"result\ntext".as_slice() };
            assert_eq!(
                decoded.input.as_ref(),
                &[openai::Input::FunctionOutput { call_id: bytes::copy_of(b"call"), output: bytes::copy_of(output) }]
            );
        }
        let mut limits = LIMITS;
        limits.string_bytes = 8;
        let block = Block::ToolResult { id: bytes::copy_of(b"call"), text: bytes::copy_of(b"no"), is_error: true };
        assert_eq!(request(prompt(Role::User, block), Provider::OpenAiCodex, &limits), Err(Error::Limit));
    }
    #[test]
    fn malformed_tool_arguments_are_preserved_received_and_rejected_sent() {
        let block = part(
            openai::Part::ToolCall {
                id: bytes::copy_of(b"call|item"),
                name: bytes::copy_of(b"read"),
                input: bytes::copy_of(b"broken"),
                too_large: false,
            },
            &LIMITS,
        )
        .expect("received arguments need not be valid JSON");
        assert_eq!(request(prompt(Role::Assistant, block), Provider::OpenAiCodex, &LIMITS), Err(Error::Invalid));
    }
    #[test]
    fn encrypted_reasoning_is_exactly_replayed() {
        let value = br#"{"type":"reasoning","id":"r","encrypted_content":"opaque","summary":[]}"#;
        let block = part(openai::Part::Opaque { bytes: bytes::copy_of(value) }, &LIMITS).expect("bounded reasoning");
        let raw = request(prompt(Role::Assistant, block), Provider::OpenAiCodex, &LIMITS).expect("reasoning replay");
        assert_eq!(
            raw.input.as_ref(),
            &[openai::Input::Opaque { value: openai::Json::from_bytes(value, &LIMITS).expect("reasoning value") }]
        );
        let replay = Replay {
            provider: Provider::OpenAiCodex,
            value: openai::Json::from_bytes(br#"{"type":"message"}"#, &LIMITS).expect("JSON object"),
        };
        assert_eq!(
            request(prompt(Role::Assistant, Block::Reasoning { replay }), Provider::OpenAiCodex, &LIMITS),
            Err(Error::Invalid)
        );
    }
    #[test]
    fn invalid_text_roles_and_bounds_reject_admission() {
        let invalid = Block::Text { text: bytes::copy_of(&[0xff]), replay: None };
        assert_eq!(request(prompt(Role::User, invalid), Provider::OpenAiCodex, &LIMITS), Err(Error::Invalid));
        let refusal = Block::Refusal { text: bytes::copy_of(b"no"), replay: None };
        assert_eq!(request(prompt(Role::User, refusal), Provider::OpenAiCodex, &LIMITS), Err(Error::Invalid));
        let text = Block::Text { text: bytes::copy_of(b"long text"), replay: None };
        let mut limits = LIMITS;
        limits.string_bytes = 4;
        assert_eq!(request(prompt(Role::User, text), Provider::OpenAiCodex, &limits), Err(Error::Limit));
        let mut limits = LIMITS;
        limits.parts = 0;
        assert_eq!(
            request(
                prompt(Role::User, Block::Text { text: bytes::copy_of(b""), replay: None }),
                Provider::OpenAiCodex,
                &limits
            ),
            Err(Error::Limit)
        );
    }
    #[test]
    fn replay_metadata_cannot_be_silently_lost() {
        for value in [
            br#"{"id":"m","unknown":"value"}"#.as_slice(),
            br#"{"id":7}"#.as_slice(),
            br#"{"id":"a","id":"b"}"#.as_slice(),
        ] {
            let replay = Replay {
                provider: Provider::OpenAiCodex,
                value: openai::Json::from_bytes(value, &LIMITS).expect("valid object"),
            };
            let block = Block::Text { text: bytes::copy_of(b"answer"), replay: Some(replay) };
            assert_eq!(request(prompt(Role::Assistant, block), Provider::OpenAiCodex, &LIMITS), Err(Error::Invalid));
        }
    }
    #[test]
    fn common_terminal_vocabulary_preserves_usage_stop_and_retry() {
        assert_eq!(stop(openai::Stop::Refusal), Stop::Refusal);
        assert_eq!(stop(openai::Stop::MaxTokens), Stop::MaxTokens);
        assert_eq!(stop(openai::Stop::ToolUse), Stop::ToolUse);
        assert_eq!(stop(openai::Stop::EndTurn), Stop::EndTurn);
        let counts = openai::Usage { input_tokens: 10, output_tokens: 3, cache_read_tokens: 5, cache_write_tokens: 2 };
        assert_eq!(
            usage(counts),
            Usage { input_tokens: 10, output_tokens: 3, cache_read_tokens: 5, cache_write_tokens: 2 }
        );
        assert_eq!(
            failure(openai::Failure::RateLimited { retry_after: Duration::from_secs(7) }),
            Failure::RateLimited { retry_after: Duration::from_secs(7) }
        );
    }
}
