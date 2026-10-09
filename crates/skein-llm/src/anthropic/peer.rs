//! Bounded document entrances for independent byte peers.
//! These helpers own the wire grammar, never application tool schemas or policy.

use super::{BlockStart, Delta, Event, UsagePatch};
use crate::openai::{self, DecodeError, Json, Limits, json};
use crate::{Block, Message, Prompt, Provider, Replay, Role, Tool};
use alloc::boxed::Box;
use skein_json::{Document, Kind, writer::Encoder};
use skein_lib::{List, bytes};

/// The byte peer's native request and ordered cache positions (llm.md, section 4.7).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Request {
    pub prompt: Prompt,
    /// At most four positions in native request order.
    pub marks: Box<[Mark]>,
}

/// One native cache position, read by the peer and passed to its cache adapter.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Mark {
    /// A block of the system array, after the request's tools.
    System { block: u32 },
    /// A block of a conversation message.
    Message { message: u32, block: u32 },
}

/// Decodes a native Messages request for an independent peer, preserving every
/// tool schema and ordered conversation block. Unknown top-level deployment
/// options are not interpreted. Counts, structure and wire bytes remain bounded.
pub fn decode_request(value: &Json, limits: &Limits) -> Result<Request, DecodeError> {
    let value = Json::from_view(value.view(), &limits.request_document())?;
    let encoded = value.to_bytes(&limits.request_document())?;
    if encoded.len() > usize::try_from(limits.request).expect("u32 fits usize") {
        return Err(DecodeError::limit(crate::Cap::Request, limits.request));
    }
    let tokens = value.view();
    let marks = markers(tokens, limits)?;
    if !json::boolean(required(tokens, b"stream")?)? {
        return Err(DecodeError::WrongType);
    }
    let model = text(tokens, b"model")?;
    let max = output_cap(tokens)?;
    let instructions = system(tokens, limits)?;
    let mut tools = List::with_capacity(limits.tools);
    if let Some(items) = optional(tokens, b"tools")? {
        for tool in &elements(items, limits, limits.tools, crate::Cap::Tools)? {
            let tool = tool.view();
            tools
                .push(Tool {
                    name: text(tool, b"name")?,
                    description: match optional(tool, b"description")? {
                        Some(value) => bytes::copy_of(json::text_ref(value)?),
                        None => Box::new([]),
                    },
                    schema: Json::from_view(required(tool, b"input_schema")?, &limits.request_document())?,
                })
                .or(Err(DecodeError::limit(crate::Cap::Tools, limits.tools)))?;
        }
    }
    let mut messages = List::with_capacity(limits.history_items);
    for message in &elements(required(tokens, b"messages")?, limits, limits.history_items, crate::Cap::HistoryItems)? {
        let message = message.view();
        let role = match json::text_ref(required(message, b"role")?)? {
            b"user" => Role::User,
            b"assistant" => Role::Assistant,
            _ => return Err(DecodeError::WrongType),
        };
        let mut blocks = List::with_capacity(limits.history_items);
        let content = required(message, b"content")?;
        if json::kind(content, 0) == Some(Kind::String) {
            let text = json::text(content)?;
            blocks
                .push(Block::Text { text, replay: None })
                .or(Err(DecodeError::limit(crate::Cap::HistoryItems, limits.history_items)))?;
        } else {
            for block in &elements(content, limits, limits.history_items, crate::Cap::HistoryItems)? {
                let block = block.view();
                let kind = json::text_ref(required(block, b"type")?)?;
                let block = match kind {
                    b"text" => Block::Text { text: text(block, b"text")?, replay: None },
                    b"tool_use" => {
                        let input = Json::from_view(required(block, b"input")?, &limits.request_document())?;
                        if json::kind(input.view(), 0) != Some(Kind::ObjectStart) {
                            return Err(DecodeError::WrongType);
                        }
                        Block::ToolCall {
                            id: text(block, b"id")?,
                            name: text(block, b"name")?,
                            arguments: input.to_bytes(&limits.request_document())?,
                            replay: None,
                        }
                    }
                    b"tool_result" => Block::ToolResult {
                        id: text(block, b"tool_use_id")?,
                        text: content_text(required(block, b"content")?, limits)?,
                        is_error: match optional(block, b"is_error")? {
                            Some(value) => json::boolean(value)?,
                            None => false,
                        },
                    },
                    _ => Block::Reasoning {
                        replay: Replay {
                            provider: Provider::Anthropic,
                            value: Json::from_view(block, &limits.request_document())?,
                        },
                    },
                };
                blocks.push(block).or(Err(DecodeError::limit(crate::Cap::HistoryItems, limits.history_items)))?;
            }
        }
        messages
            .push(Message { role, content: blocks.into_boxed() })
            .or(Err(DecodeError::limit(crate::Cap::HistoryItems, limits.history_items)))?;
    }
    let choice = tool_choice(tokens)?;
    let prompt = Prompt {
        model,
        instructions,
        tools: tools.into_boxed(),
        messages: messages.into_boxed(),
        reasoning_effort: None,
        affinity: None,
        choice,
        max_output_tokens: Some(max),
    };
    match super::request::validate(&prompt, u32::MAX, limits) {
        Ok(()) => Ok(Request { prompt, marks }),
        Err(crate::Error::Limit { which, bound }) => Err(DecodeError::TooLarge { which, bound }),
        Err(crate::Error::Invalid | crate::Error::Unsupported) => Err(DecodeError::Malformed),
    }
}

fn output_cap(tokens: (&Document, json::Span)) -> Result<u32, DecodeError> {
    let max = json::unsigned(required(tokens, b"max_tokens")?)?;
    let max = u32::try_from(max).or(Err(DecodeError::WrongType))?;
    if max == 0 { Err(DecodeError::Malformed) } else { Ok(max) }
}

fn markers(tokens: (&Document, json::Span), limits: &Limits) -> Result<Box<[Mark]>, DecodeError> {
    let mut marks = List::with_capacity(4);
    if let Some(system) = optional(tokens, b"system")?
        && json::kind(system, 0) == Some(Kind::ArrayStart)
    {
        for (index, offset) in
            json::array_limit(system, limits.history_items, crate::Cap::HistoryItems)?.iter().enumerate()
        {
            if marker(json::value_at(system, *offset)?)? {
                marks
                    .push(Mark::System { block: u32::try_from(index).expect("bounded system blocks") })
                    .or(Err(DecodeError::Malformed))?;
            }
        }
    }
    let messages = required(tokens, b"messages")?;
    for (message_index, offset) in
        json::array_limit(messages, limits.history_items, crate::Cap::HistoryItems)?.iter().enumerate()
    {
        let content = required(json::value_at(messages, *offset)?, b"content")?;
        if json::kind(content, 0) == Some(Kind::ArrayStart) {
            for (block_index, offset) in
                json::array_limit(content, limits.history_items, crate::Cap::HistoryItems)?.iter().enumerate()
            {
                let block = json::value_at(content, *offset)?;
                if marker(block)? {
                    match json::text_ref(required(block, b"type")?)? {
                        b"text" | b"tool_use" | b"tool_result" => {}
                        _ => return Err(DecodeError::Malformed),
                    }
                    marks
                        .push(Mark::Message {
                            message: u32::try_from(message_index).expect("bounded messages"),
                            block: u32::try_from(block_index).expect("bounded blocks"),
                        })
                        .or(Err(DecodeError::Malformed))?;
                }
            }
        }
    }
    Ok(marks.into_boxed())
}

fn marker(block: (&Document, json::Span)) -> Result<bool, DecodeError> {
    match optional(block, b"cache_control")? {
        Some(value) => {
            if json::text_ref(required(value, b"type")?)? != b"ephemeral" {
                return Err(DecodeError::Malformed);
            }
            if optional(value, b"ttl")?.is_some() {
                return Err(DecodeError::Malformed);
            }
            Ok(true)
        }
        None => Ok(false),
    }
}

fn tool_choice(tokens: (&Document, json::Span)) -> Result<crate::ToolChoice, DecodeError> {
    match optional(tokens, b"tool_choice")? {
        Some(value) => match json::text_ref(required(value, b"type")?)? {
            b"none" => Ok(crate::ToolChoice::None),
            b"auto" => Ok(crate::ToolChoice::Auto),
            _ => Err(DecodeError::WrongType),
        },
        None => Ok(crate::ToolChoice::Auto),
    }
}

fn system(tokens: (&Document, json::Span), limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    match optional(tokens, b"system")? {
        Some(value) => content_text(value, limits),
        None => Ok(Box::new([])),
    }
}

fn content_text(tokens: (&Document, json::Span), limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    if json::kind(tokens, 0) == Some(Kind::String) { json::text(tokens) } else { text_blocks(tokens, limits) }
}

fn text_blocks(tokens: (&Document, json::Span), limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    let offsets = json::array_limit(tokens, limits.history_items, crate::Cap::HistoryItems)?;
    let mut length = 0_usize;
    let maximum = usize::try_from(limits.request).expect("u32 fits usize");
    for (index, offset) in offsets.iter().enumerate() {
        let block = json::value_at(tokens, *offset)?;
        if json::text_ref(required(block, b"type")?)? != b"text" {
            return Err(DecodeError::WrongType);
        }
        let text = json::text_ref(required(block, b"text")?)?;
        let separator = usize::from(index > 0);
        length = length.checked_add(separator).ok_or(DecodeError::limit(crate::Cap::Request, limits.request))?;
        length = length.checked_add(text.len()).ok_or(DecodeError::limit(crate::Cap::Request, limits.request))?;
        if length > maximum {
            return Err(DecodeError::limit(crate::Cap::Request, limits.request));
        }
    }
    let mut output = skein_lib::Writer::new(length);
    for (index, offset) in offsets.iter().enumerate() {
        if index > 0 {
            output.put(b"\n").expect("separator included in measured bytes");
        }
        let block = json::value_at(tokens, *offset)?;
        let text = json::text_ref(required(block, b"text")?)?;
        output.put(text).expect("text included in measured bytes");
    }
    Ok(output.finish())
}

fn elements(
    tokens: (&Document, json::Span),
    limits: &Limits,
    count: u32,
    cap: crate::Cap,
) -> Result<List<Json>, DecodeError> {
    let offsets = json::array_limit(tokens, count, cap)?;
    let mut elements = List::with_capacity(offsets.len());
    for offset in &offsets {
        let value = Json::from_view(json::value_at(tokens, *offset)?, &limits.request_document())?;
        elements.push(value).expect("one value per admitted array element");
    }
    Ok(elements)
}

fn required<'a>(tokens: (&'a Document, json::Span), name: &[u8]) -> Result<(&'a Document, json::Span), DecodeError> {
    json::value_at(tokens, json::required(tokens, name)?)
}

fn optional<'a>(
    tokens: (&'a Document, json::Span),
    name: &[u8],
) -> Result<Option<(&'a Document, json::Span)>, DecodeError> {
    json::optional_at(tokens, json::field(tokens, name)?)
}

fn text(tokens: (&Document, json::Span), name: &[u8]) -> Result<Box<[u8]>, DecodeError> {
    Ok(bytes::copy_of(json::text_ref(required(tokens, name)?)?))
}

/// Encodes one synthetic native stream event after measuring its bounded JSON.
/// Peer scripts choose the event; stream ordering belongs to the consumer.
/// Thinking heads must be objects with matching type/text/signature fields;
/// opaque extension fields survive unchanged. A missing initial signature means empty.
pub fn encode_event(event: &Event, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    match event {
        Event::Added { block: BlockStart::ToolCall { input, .. }, .. } => {
            let input = Json::from_bytes(input, &limits.document())?;
            if json::kind(input.view(), 0) != Some(Kind::ObjectStart) {
                return Err(DecodeError::WrongType);
            }
        }
        Event::Added { block: BlockStart::Thinking { text, signature, head }, .. } => {
            super::response::thinking_head(head, text, signature, limits)?;
        }
        Event::Added { block: BlockStart::Redacted { value }, .. } => {
            let value = Json::from_view(value.view(), &limits.document())?;
            super::response::validate_redacted(value.view())?;
            if value.to_bytes(&limits.document())?.len() > usize::try_from(limits.reasoning).expect("u32 fits usize") {
                return Err(DecodeError::limit(crate::Cap::Reasoning, limits.reasoning));
            }
        }
        Event::Added { block: BlockStart::Opaque { value }, .. } => {
            let value = Json::from_view(value.view(), &limits.document())?;
            super::response::validate_opaque(value.view())?;
            if value.to_bytes(&limits.document())?.len() > usize::try_from(limits.reasoning).expect("u32 fits usize") {
                return Err(DecodeError::limit(crate::Cap::Reasoning, limits.reasoning));
            }
        }
        Event::Started { .. }
        | Event::Added { .. }
        | Event::Delta { .. }
        | Event::Done { .. }
        | Event::MessageDelta { .. }
        | Event::Completed
        | Event::Failed { .. }
        | Event::Progress
        | Event::Unknown => {}
    }
    let bounded = limits.writer_limits();
    let mut measure = Encoder::measure(&bounded);
    write_event(&mut measure, event, limits);
    let len = openai::measured(measure, bounded, crate::Cap::Retained)?;
    let mut output = Encoder::write(len, &bounded);
    write_event(&mut output, event, limits);
    Ok(output.finish())
}

fn write_event(out: &mut Encoder, event: &Event, limits: &Limits) {
    out.object_start();
    out.key(b"type");
    match event {
        Event::Started { usage } => {
            out.string(b"message_start");
            out.key(b"message");
            out.object_start();
            out.key(b"id");
            out.string(b"synthetic-message");
            out.key(b"type");
            out.string(b"message");
            out.key(b"role");
            out.string(b"assistant");
            out.key(b"content");
            out.array_start();
            out.array_end();
            out.key(b"usage");
            usage_fields(
                out,
                UsagePatch {
                    input: usage.input,
                    output: usage.output,
                    cache_read: usage.cache_read,
                    cache_write: usage.cache_write,
                },
            );
            out.object_end();
        }
        Event::Added { index, block } => {
            out.string(b"content_block_start");
            out.key(b"index");
            out.unsigned(u64::from(*index));
            out.key(b"content_block");
            write_block(out, block, limits);
        }
        Event::Delta { index, delta } => {
            out.string(b"content_block_delta");
            out.key(b"index");
            out.unsigned(u64::from(*index));
            out.key(b"delta");
            out.object_start();
            out.key(b"type");
            let (kind, key, text) = match delta {
                Delta::Text { text } => (b"text_delta".as_slice(), b"text".as_slice(), text),
                Delta::Arguments { text } => (b"input_json_delta".as_slice(), b"partial_json".as_slice(), text),
                Delta::Thinking { text } => (b"thinking_delta".as_slice(), b"thinking".as_slice(), text),
                Delta::Signature { text } => (b"signature_delta".as_slice(), b"signature".as_slice(), text),
            };
            out.string(kind);
            out.key(key);
            out.string(text);
            out.object_end();
        }
        Event::Done { index } => {
            out.string(b"content_block_stop");
            out.key(b"index");
            out.unsigned(u64::from(*index));
        }
        Event::MessageDelta { stop, usage } => {
            out.string(b"message_delta");
            out.key(b"delta");
            out.object_start();
            out.key(b"stop_reason");
            match stop {
                Some(super::Stop::EndTurn) => out.string(b"end_turn"),
                Some(super::Stop::ToolUse) => out.string(b"tool_use"),
                Some(super::Stop::MaxTokens) => out.string(b"max_tokens"),
                Some(super::Stop::Refusal) => out.string(b"refusal"),
                None => out.null(),
            }
            out.object_end();
            out.key(b"usage");
            usage_fields(out, *usage);
        }
        Event::Completed => out.string(b"message_stop"),
        Event::Failed { error } => {
            out.string(b"error");
            out.key(b"error");
            out.object_start();
            out.key(b"type");
            out.string(&error.kind);
            out.key(b"message");
            out.string(&error.message);
            out.object_end();
        }
        Event::Progress => out.string(b"ping"),
        Event::Unknown => out.string(b"synthetic-extension"),
    }
    out.object_end();
}

fn write_block(out: &mut Encoder, block: &BlockStart, limits: &Limits) {
    match block {
        BlockStart::Redacted { value } | BlockStart::Opaque { value } => value.write(out),
        BlockStart::Text { text } => {
            out.object_start();
            out.key(b"type");
            out.string(b"text");
            out.key(b"text");
            out.string(text);
            out.object_end();
        }
        BlockStart::ToolCall { id, name, input } => {
            out.object_start();
            out.key(b"type");
            out.string(b"tool_use");
            out.key(b"id");
            out.string(id);
            out.key(b"name");
            out.string(name);
            out.key(b"input");
            Json::from_bytes(input, &limits.document()).expect("event input admitted before measurement").write(out);
            out.object_end();
        }
        BlockStart::Thinking { text, signature, head } => {
            super::response::write_thinking(out, head, text, signature);
        }
    }
}

fn usage_fields(out: &mut Encoder, usage: UsagePatch) {
    out.object_start();
    for (name, amount) in [
        (b"input_tokens".as_slice(), usage.input),
        (b"output_tokens".as_slice(), usage.output),
        (b"cache_read_input_tokens".as_slice(), usage.cache_read),
        (b"cache_creation_input_tokens".as_slice(), usage.cache_write),
    ] {
        if let Some(amount) = amount {
            out.key(name);
            out.unsigned(amount);
        }
    }
    out.object_end();
}
