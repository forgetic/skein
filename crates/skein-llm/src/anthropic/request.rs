//! Measured native Messages requests and shape-derived cache markers.
//! Keeps no state; replay is never modified. Entrances validate, measure and encode.
//! Contract: llm.md, sections 4.5 and 4.7.
use super::identity::CLAUDE_CODE_SYSTEM_IDENTITY;
use crate::{Block, Error, Json, Prompt, Provider, Replay, Role, Tool, openai};
use alloc::boxed::Box;
use skein_json::{Document, Kind, writer};

/// Encode the request under the endpoint's nonzero declared output ceiling.
pub fn encode_request(prompt: &Prompt, declared_output: u32, limits: &openai::Limits) -> Result<Box<[u8]>, Error> {
    let length = measure_request(prompt, declared_output, limits)?;
    let bounded = writer::Limits { depth: limits.depth, length: limits.request_bytes };
    let mut out = writer::Encoder::write(length, &bounded);
    write_request(&mut out, prompt, declared_output, limits)?;
    Ok(out.finish())
}

/// Validates the prompt and measures its escaped wire representation before allocating it.
pub fn measure_request(prompt: &Prompt, declared_output: u32, limits: &openai::Limits) -> Result<u32, Error> {
    validate(prompt, declared_output, limits)?;
    let bounded = writer::Limits { depth: limits.depth, length: limits.request_bytes };
    let mut out = writer::Encoder::measure(&bounded);
    write_request(&mut out, prompt, declared_output, limits)?;
    measured(out, bounded, crate::Cap::Request)
}

pub(super) fn validate(prompt: &Prompt, declared_output: u32, limits: &openai::Limits) -> Result<(), Error> {
    if declared_output == 0 {
        return Err(Error::Invalid);
    }
    if prompt.max_output_tokens.unwrap_or(declared_output) > declared_output {
        return Err(Error::limit(crate::Cap::Output, declared_output));
    }
    let count = usize::try_from(limits.parts).expect("u32 fits usize");
    if prompt.model.is_empty() || prompt.messages.is_empty() || prompt.max_output_tokens == Some(0) {
        return Err(Error::Invalid);
    }
    if prompt.tools.len() > count || prompt.messages.len() > count {
        return Err(Error::limit(crate::Cap::Parts, limits.parts));
    }
    crate::translate::validate_choice(&prompt.choice, &prompt.tools)?;
    let mut budget: u64 = 0;
    match &prompt.choice {
        crate::ToolChoice::Auto | crate::ToolChoice::None => {}
        crate::ToolChoice::Only(names) => {
            for name in names {
                text(name, &mut budget, limits)?;
            }
        }
    }
    text(&prompt.model, &mut budget, limits)?;
    text(&prompt.instructions, &mut budget, limits)?;
    if let Some(effort) = &prompt.reasoning_effort {
        text(effort, &mut budget, limits)?;
        match effort.as_ref() {
            b"off" | b"low" | b"medium" | b"high" | b"max" => {}
            _ => return Err(Error::Unsupported),
        }
    }
    for tool in &prompt.tools {
        identifier(&tool.name, 128)?;
        text(&tool.name, &mut budget, limits)?;
        text(&tool.description, &mut budget, limits)?;
        json(&tool.schema, &mut budget, limits)?;
        if openai::json::kind(tool.schema.view(), 0) != Some(Kind::ObjectStart) {
            return Err(Error::Invalid);
        }
    }
    let mut blocks: usize = 0;
    for message in &prompt.messages {
        if message.content.is_empty() {
            return Err(Error::Invalid);
        }
        blocks = blocks.checked_add(message.content.len()).ok_or(Error::limit(crate::Cap::Parts, limits.parts))?;
        if blocks > count {
            return Err(Error::limit(crate::Cap::Parts, limits.parts));
        }
        for block in &message.content {
            validate_block(block, message.role, &mut budget, limits)?;
        }
    }
    Ok(())
}

fn validate_block(block: &Block, role: Role, budget: &mut u64, limits: &openai::Limits) -> Result<(), Error> {
    match block {
        Block::Oversize { .. } | Block::Cut { .. } => Err(Error::Invalid),
        Block::Dropped { .. } => Ok(()),
        Block::Text { text: value, replay } => {
            if role == Role::User && replay.is_some() {
                return Err(Error::Invalid);
            }
            unsupported_replay(replay.as_ref())?;
            text(value, budget, limits)
        }
        Block::Refusal { text: value, replay } => {
            if role != Role::Assistant {
                return Err(Error::Invalid);
            }
            unsupported_replay(replay.as_ref())?;
            text(value, budget, limits)
        }
        Block::ToolCall { id, name, arguments, replay } => {
            if role != Role::Assistant {
                return Err(Error::Invalid);
            }
            unsupported_replay(replay.as_ref())?;
            identifier(id, 64)?;
            identifier(name, 128)?;
            text(id, budget, limits)?;
            text(name, budget, limits)?;
            if arguments.len() > usize::try_from(limits.string_bytes).expect("u32 fits usize") {
                return Err(Error::limit(crate::Cap::String, limits.string_bytes));
            }
            charge(arguments.len(), budget, limits)
        }
        Block::ToolResult { id, text: value, is_error: _ } => {
            if role != Role::User {
                return Err(Error::Invalid);
            }
            identifier(id, 64)?;
            text(id, budget, limits)?;
            text(value, budget, limits)
        }
        Block::Reasoning { replay } => {
            if role != Role::Assistant {
                return Err(Error::Invalid);
            }
            if replay.provider != Provider::Anthropic {
                return Err(Error::Unsupported);
            }
            json(&replay.value, budget, limits)?;
            let bounded = writer::Limits { depth: limits.depth, length: limits.opaque_bytes };
            let mut measure = writer::Encoder::measure(&bounded);
            replay.value.write(&mut measure);
            let _length = measured(measure, bounded, crate::Cap::Opaque)?;
            validate_reasoning(&replay.value)
        }
    }
}

fn unsupported_replay(replay: Option<&Replay>) -> Result<(), Error> {
    if replay.is_some() { Err(Error::Unsupported) } else { Ok(()) }
}

fn identifier(value: &[u8], maximum: usize) -> Result<(), Error> {
    if value.is_empty() || value.len() > maximum {
        return Err(Error::Invalid);
    }
    for byte in value {
        if !byte.is_ascii_alphanumeric() && *byte != b'_' && *byte != b'-' {
            return Err(Error::Invalid);
        }
    }
    Ok(())
}

fn validate_reasoning(value: &Json) -> Result<(), Error> {
    let tokens = value.view();
    let kind = field_text(tokens, b"type")?;
    match kind {
        b"thinking" => {
            let _thinking = field_text(tokens, b"thinking")?;
            if field_text(tokens, b"signature")?.is_empty() {
                return Err(Error::Invalid);
            }
        }
        b"redacted_thinking" if field_text(tokens, b"data")?.is_empty() => return Err(Error::Invalid),
        b"" | b"text" | b"tool_use" | b"tool_result" => return Err(Error::Invalid),
        _ => {}
    }
    // Unknown provider-owned extension fields remain in the bounded JSON value.
    // Field access checks the required known fields and rejects their duplicate
    // names. Collector checks structure, UTF-8 and the supplied limits; unknown
    // extension keys, including their duplicates, stay uninterpreted.

    Ok(())
}

fn field_text<'a>(tokens: (&'a Document, openai::json::Span), key: &[u8]) -> Result<&'a [u8], Error> {
    let at = match openai::json::required(tokens, key) {
        Ok(at) => at,
        Err(error) => return Err(crate::translate::decode(error)),
    };
    let value = match openai::json::value_at(tokens, at) {
        Ok(value) => value,
        Err(error) => return Err(crate::translate::decode(error)),
    };
    match openai::json::text_ref(value) {
        Ok(value) => Ok(value),
        Err(error) => Err(crate::translate::decode(error)),
    }
}

fn charge(size: usize, budget: &mut u64, limits: &openai::Limits) -> Result<(), Error> {
    *budget = budget
        .checked_add(u64::try_from(size).expect("usize fits u64"))
        .ok_or(Error::limit(crate::Cap::Request, limits.request_bytes))?;
    if *budget > u64::from(limits.request_bytes) {
        Err(Error::limit(crate::Cap::Request, limits.request_bytes))
    } else {
        Ok(())
    }
}

fn text(value: &[u8], budget: &mut u64, limits: &openai::Limits) -> Result<(), Error> {
    if value.len() > usize::try_from(limits.string_bytes).expect("u32 fits usize") {
        return Err(Error::limit(crate::Cap::String, limits.string_bytes));
    }
    let mut measure = writer::Encoder::measure(&writer::Limits { depth: limits.depth, length: limits.request_bytes });
    measure.string(value);
    let _length =
        measured(measure, writer::Limits { depth: limits.depth, length: limits.request_bytes }, crate::Cap::Request)?;
    charge(value.len(), budget, limits)
}

fn json(value: &Json, budget: &mut u64, limits: &openai::Limits) -> Result<(), Error> {
    if value.document().len() > limits.tokens {
        return Err(Error::limit(crate::Cap::Tokens, limits.tokens));
    }
    let view = value.view();
    for index in 0..openai::json::len(view) {
        charge(1, budget, limits)?;
        match openai::json::kind(view, index).expect("admitted record") {
            Kind::Key | Kind::String | Kind::Number => {
                text(openai::json::record_text(view, index).expect("admitted text"), budget, limits)?;
            }
            Kind::ObjectStart
            | Kind::ObjectEnd
            | Kind::ArrayStart
            | Kind::ArrayEnd
            | Kind::True
            | Kind::False
            | Kind::Null => {}
            Kind::Long => unreachable!("admitted Json contains no Long"),
        }
    }
    let mut measure = writer::Encoder::measure(&limits.writer_limits());
    value.write(&mut measure);
    let _length = measured(measure, limits.writer_limits(), crate::Cap::Document)?;
    Ok(())
}

fn measured(out: writer::Encoder, limits: writer::Limits, cap: crate::Cap) -> Result<u32, Error> {
    match out.measured() {
        Ok(length) => Ok(length),
        Err(writer::Refusal::TooLong) => Err(Error::limit(cap, limits.length)),
        Err(writer::Refusal::TooDeep) => Err(Error::limit(crate::Cap::Depth, limits.depth)),
        Err(writer::Refusal::Text | writer::Refusal::Number) => Err(Error::Invalid),
    }
}

fn write_request(
    out: &mut writer::Encoder,
    prompt: &Prompt,
    declared_output: u32,
    limits: &openai::Limits,
) -> Result<(), Error> {
    let marks = breakpoints(prompt);
    out.object_start();
    out.key(b"model");
    out.string(&prompt.model);
    out.key(b"max_tokens");
    out.unsigned(u64::from(prompt.max_output_tokens.unwrap_or(declared_output)));
    out.key(b"stream");
    out.boolean(true);
    if !prompt.instructions.is_empty() {
        out.key(b"system");
        write_system(out, &prompt.instructions, marks.system);
    }
    if !prompt.tools.is_empty() {
        out.key(b"tools");
        write_tools(out, &prompt.tools);
    }
    match &prompt.choice {
        crate::ToolChoice::Auto | crate::ToolChoice::Only(_) => {}
        crate::ToolChoice::None => {
            out.key(b"tool_choice");
            out.object_start();
            out.key(b"type");
            out.string(b"none");
            out.object_end();
        }
    }
    out.key(b"messages");
    out.array_start();
    for (message_index, message) in prompt.messages.iter().enumerate() {
        if !has_native_block(&message.content) {
            continue;
        }
        out.object_start();
        out.key(b"role");
        out.string(match message.role {
            Role::User => b"user",
            Role::Assistant => b"assistant",
        });
        out.key(b"content");
        out.array_start();
        for (block_index, block) in message.content.iter().enumerate() {
            let marked = match &marks.tail {
                Some(tail) => tail.message == message_index && tail.block == block_index,
                None => false,
            };
            write_block(out, block, marked, limits)?;
        }
        out.array_end();
        out.object_end();
    }
    out.array_end();
    if let Some(effort) = &prompt.reasoning_effort {
        out.key(b"thinking");
        out.object_start();
        out.key(b"type");
        out.string(if effort.as_ref() == b"off" { b"disabled" } else { b"adaptive" });
        out.object_end();
        if effort.as_ref() != b"off" {
            out.key(b"output_config");
            out.object_start();
            out.key(b"effort");
            out.string(effort);
            out.object_end();
        }
    }
    out.object_end();
    Ok(())
}

struct Breakpoints {
    system: bool,
    tail: Option<Tail>,
}

struct Tail {
    message: usize,
    block: usize,
}

fn breakpoints(prompt: &Prompt) -> Breakpoints {
    let mut tail = None;
    if let Some(message) = prompt.messages.last() {
        for (index, block) in message.content.iter().enumerate() {
            match block {
                Block::Text { .. } | Block::Refusal { .. } | Block::ToolCall { .. } | Block::ToolResult { .. } => {
                    tail = Some(Tail {
                        message: prompt.messages.len().checked_sub(1).expect("last message"),
                        block: index,
                    });
                }
                Block::Reasoning { .. } | Block::Dropped { .. } | Block::Oversize { .. } | Block::Cut { .. } => {}
            }
        }
    }
    Breakpoints { system: !prompt.instructions.is_empty(), tail }
}

fn write_system(out: &mut writer::Encoder, instructions: &[u8], marked: bool) {
    let extra = if instructions == CLAUDE_CODE_SYSTEM_IDENTITY {
        Some(b"".as_slice())
    } else {
        match instructions.strip_prefix(CLAUDE_CODE_SYSTEM_IDENTITY) {
            Some(remaining) => remaining.strip_prefix(b"\n\n"),
            None => None,
        }
    };
    out.array_start();
    match extra {
        Some(extra) => {
            write_system_text(out, CLAUDE_CODE_SYSTEM_IDENTITY, marked && extra.is_empty());
            if !extra.is_empty() {
                write_system_text(out, extra, marked);
            }
        }
        None => write_system_text(out, instructions, marked),
    }
    out.array_end();
}

fn write_system_text(out: &mut writer::Encoder, text: &[u8], marked: bool) {
    out.object_start();
    out.key(b"type");
    out.string(b"text");
    out.key(b"text");
    out.string(text);
    cache_control(out, marked);
    out.object_end();
}

fn cache_control(out: &mut writer::Encoder, marked: bool) {
    if marked {
        out.key(b"cache_control");
        out.object_start();
        out.key(b"type");
        out.string(b"ephemeral");
        out.object_end();
    }
}

fn write_tools(out: &mut writer::Encoder, tools: &[Tool]) {
    out.array_start();
    for tool in tools {
        out.object_start();
        out.key(b"name");
        out.string(&tool.name);
        out.key(b"description");
        out.string(&tool.description);
        out.key(b"input_schema");
        tool.schema.write(out);
        out.object_end();
    }
    out.array_end();
}

fn write_block(out: &mut writer::Encoder, block: &Block, marked: bool, limits: &openai::Limits) -> Result<(), Error> {
    match block {
        Block::Oversize { .. } | Block::Cut { .. } => return Err(Error::Invalid),
        Block::Dropped { .. } => {}
        Block::Reasoning { replay } => replay.value.write(out),
        Block::Text { text, replay: _ } | Block::Refusal { text, replay: _ } => {
            out.object_start();
            out.key(b"type");
            out.string(b"text");
            out.key(b"text");
            out.string(text);
            cache_control(out, marked);
            out.object_end();
        }
        Block::ToolCall { id, name, arguments, replay: _ } => {
            let value = match Json::from_bytes(arguments, limits) {
                Ok(value) => value,
                Err(error) => return Err(crate::translate::decode(error)),
            };
            if openai::json::kind(value.view(), 0) != Some(Kind::ObjectStart) {
                return Err(Error::Invalid);
            }
            out.object_start();
            out.key(b"type");
            out.string(b"tool_use");
            out.key(b"id");
            out.string(id);
            out.key(b"name");
            out.string(name);
            out.key(b"input");
            value.write(out);
            cache_control(out, marked);
            out.object_end();
        }
        Block::ToolResult { id, text, is_error } => {
            out.object_start();
            out.key(b"type");
            out.string(b"tool_result");
            out.key(b"tool_use_id");
            out.string(id);
            out.key(b"content");
            out.string(text);
            out.key(b"is_error");
            out.boolean(*is_error);
            cache_control(out, marked);
            out.object_end();
        }
    }
    Ok(())
}

fn has_native_block(content: &[Block]) -> bool {
    for block in content {
        match block {
            Block::Dropped { .. } => {}
            Block::Text { .. }
            | Block::Refusal { .. }
            | Block::ToolCall { .. }
            | Block::ToolResult { .. }
            | Block::Reasoning { .. }
            | Block::Oversize { .. }
            | Block::Cut { .. } => return true,
        }
    }
    false
}
