//! Both provider document sides translated to the fake's neutral vocabulary.
//! This adapter has no agent dependency and decodes the actual request grammar.
//!
//! Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;
use skein_fake_llm_domain::api;
use skein_http::sse::writer::Outgoing;
use skein_json::{Document, Kind};
use skein_lib::{Decimal, List, Writer, bytes};
use skein_llm::anthropic;
use skein_llm::openai;

/// Selected provider dialect for the fake's request and answer document codecs.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Provider {
    /// Anthropic document and stream dialect.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Anthropic,
    /// `OpenAI` Responses document and stream dialect.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    OpenAi,
}

/// One usage field whose reporting the configured byte peer may enable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UsageField {
    /// Disjoint prompt input, sent by the peer when configured.
    Input,
    /// Cache reads, sent by the peer when configured.
    CacheRead,
    /// Cache writes, sent by the peer when configured.
    CacheWrite,
    /// Completion output, sent by the peer when configured.
    Output,
    /// Reasoning within output, sent only on Codex when configured.
    Reasoning,
}
/// The configured subset of usage fields the peer writes; omission preserves None.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct UsageFields {
    bits: u8,
}
impl UsageFields {
    pub const ALL: UsageFields = UsageFields { bits: 31 };
    pub const NONE: UsageFields = UsageFields { bits: 0 };
    #[must_use]
    pub const fn with(self, field: UsageField) -> UsageFields {
        UsageFields { bits: self.bits | usage_bit(field) }
    }
    #[must_use]
    pub const fn contains(self, field: UsageField) -> bool {
        self.bits & usage_bit(field) != 0
    }
}
const fn usage_bit(field: UsageField) -> u8 {
    match field {
        UsageField::Input => 1,
        UsageField::CacheRead => 2,
        UsageField::CacheWrite => 4,
        UsageField::Output => 8,
        UsageField::Reasoning => 16,
    }
}
/// Optional native echoes and the usage fields configured for this byte peer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Options {
    pub echo: openai::Echo,
    pub usage_fields: UsageFields,
}
impl Options {
    pub const DEFAULT: Options = Options { echo: openai::Echo::NONE, usage_fields: UsageFields::ALL };
}

/// Immutable ownership and document caps supplied by the caller to every entry point.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    /// Bounds for the Anthropic dialect's documents and stream.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub anthropic: anthropic::Limits,
    /// Bounds for the `OpenAI` dialect's documents and stream.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub openai: openai::Limits,
    /// Responses subscription requests have no wire token limit. The fake
    /// uses an explicit configured model ceiling, never invents a wire field.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub model_ceiling: u32,
}

/// Typed refusal or terminal failure of the fake peer, independent of the agent's policy.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    /// Input has invalid syntax or violates the codec's structural contract.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Malformed,
    /// A configured ownership, count or encoded-byte cap would be exceeded.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    TooLarge,
}

/// Decodes the selected provider's bounded request body into the neutral fake query, without interpreting agent policy.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn request(provider: Provider, data: &[u8], limits: &Limits) -> Result<api::Query, Error> {
    match provider {
        Provider::Anthropic => anthropic_request(data, &limits.anthropic),
        Provider::OpenAi => openai_request(data, limits),
    }
}

fn choice(choice: skein_llm::ToolChoice) -> api::ToolChoice {
    match choice {
        skein_llm::ToolChoice::Auto => api::ToolChoice::Auto,
        skein_llm::ToolChoice::None => api::ToolChoice::None,
        skein_llm::ToolChoice::Only(names) => api::ToolChoice::Only(names),
    }
}

fn anthropic_request(data: &[u8], limits: &anthropic::Limits) -> Result<api::Query, Error> {
    let json = anthropic::Json::from_bytes(data, limits).or(Err(Error::Malformed))?;
    let request = anthropic::decode_request(&json, limits).or(Err(Error::Malformed))?;
    let mut tools = List::with_capacity(limits.parts);
    for tool in request.tools {
        let parameters = tool.schema.to_bytes(limits).or(Err(Error::Malformed))?;
        tools
            .push(api::ToolSpec { name: tool.name, description: tool.description, parameters })
            .or(Err(Error::TooLarge))?;
    }
    let mut messages = List::with_capacity(limits.parts);
    for message in request.messages {
        let role = match message.role {
            skein_llm::Role::User => api::Role::User,
            skein_llm::Role::Assistant => api::Role::Assistant,
        };
        let mut parts = List::with_capacity(limits.parts);
        for block in message.content {
            let part = match block {
                skein_llm::Block::Text { text, .. } | skein_llm::Block::Refusal { text, .. } => {
                    api::Part::Text { text }
                }
                skein_llm::Block::ToolCall { id, name, arguments, .. } => api::Part::ToolCall { id, name, arguments },
                skein_llm::Block::ToolResult { id, text, is_error } => {
                    api::Part::ToolOutput { id, output: text, is_error }
                }
                skein_llm::Block::Oversize { .. } | skein_llm::Block::Cut { .. } | skein_llm::Block::Dropped { .. } => {
                    return Err(Error::Malformed);
                }
                skein_llm::Block::Reasoning { replay } => {
                    api::Part::Opaque { bytes: replay.value.to_bytes(limits).or(Err(Error::Malformed))? }
                }
            };
            parts.push(part).or(Err(Error::TooLarge))?;
        }
        messages.push(api::Message { role, parts: parts.into_boxed() }).or(Err(Error::TooLarge))?;
    }
    Ok(api::Query {
        cache_scope: None,
        model: request.model,
        system: request.instructions,
        tools: tools.into_boxed(),
        choice: choice(request.choice),
        messages: messages.into_boxed(),
        max_tokens: request.max_output_tokens.expect("Messages request decoded its required cap"),
    })
}

fn openai_request(data: &[u8], limits: &Limits) -> Result<api::Query, Error> {
    if limits.model_ceiling == 0 {
        return Err(Error::Malformed);
    }
    let Ok(json) = openai::Json::from_bytes(data, &limits.openai) else {
        return Err(Error::Malformed);
    };
    let Ok(request) = openai::decode_request(&json, &limits.openai) else {
        return Err(Error::Malformed);
    };
    let cache_scope = match &request.prompt_cache_key {
        Some(key) => Some(parse_uuid(key).ok_or(Error::Malformed)?),
        None => None,
    };
    let mut tools = List::with_capacity(u32::try_from(request.tools.len()).or(Err(Error::TooLarge))?);
    for tool in request.tools {
        let Ok(parameters) = tool.schema.to_bytes(&limits.openai) else {
            return Err(Error::Malformed);
        };
        tools.push(api::ToolSpec { name: tool.name, description: tool.description, parameters }).expect("one per tool");
    }
    let mut messages = List::with_capacity(limits.openai.parts);
    let mut parts = List::with_capacity(limits.openai.parts);
    let mut previous = None;
    for item in request.input {
        let (role, part) = match item {
            openai::Input::Message { role, text, id: _, phase: _, refusal: _ } => (
                match role {
                    openai::Role::User => api::Role::User,
                    openai::Role::Assistant => api::Role::Assistant,
                },
                api::Part::Text { text },
            ),
            openai::Input::FunctionCall { call_id, item_id: _, name, arguments } => {
                (api::Role::Assistant, api::Part::ToolCall { id: call_id, name, arguments })
            }
            openai::Input::FunctionOutput { call_id, output } => {
                (api::Role::User, api::Part::ToolOutput { id: call_id, output, is_error: false })
            }
            openai::Input::Opaque { value } => {
                let Ok(bytes) = value.to_bytes(&limits.openai) else {
                    return Err(Error::Malformed);
                };
                (api::Role::Assistant, api::Part::Opaque { bytes })
            }
        };
        if previous != Some(role) {
            if let Some(role) = previous {
                messages.push(api::Message { role, parts: parts.into_boxed() }).or(Err(Error::TooLarge))?;
                parts = List::with_capacity(limits.openai.parts);
            }
            previous = Some(role);
        }
        parts.push(part).or(Err(Error::TooLarge))?;
    }
    if let Some(role) = previous {
        messages.push(api::Message { role, parts: parts.into_boxed() }).or(Err(Error::TooLarge))?;
    }
    Ok(api::Query {
        cache_scope,
        model: request.model,
        system: request.instructions,
        tools: tools.into_boxed(),
        choice: choice(request.choice),
        messages: messages.into_boxed(),
        max_tokens: limits.model_ceiling,
    })
}

/// Measures one next event, rather than retaining an encoded response tape.
/// The server keeps its neutral Answer and one writer event at a time.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn event(
    provider: Provider,
    answer: &api::Answer,
    sequence: u32,
    call: u64,
    limits: &Limits,
) -> Result<Option<Outgoing>, Error> {
    event_with_options(provider, answer, sequence, call, &[], Options::DEFAULT, limits)
}

/// Encode one event with the optional Codex request echo from its bounded body.
pub fn event_with_options(
    provider: Provider,
    answer: &api::Answer,
    sequence: u32,
    call: u64,
    body: &[u8],
    options: Options,
    limits: &Limits,
) -> Result<Option<Outgoing>, Error> {
    let echo = options.echo;
    let (name, data) = match provider {
        Provider::Anthropic => {
            let Some((name, event)) = anthropic_event(answer, sequence, options.usage_fields, &limits.anthropic)?
            else {
                return Ok(None);
            };
            let Ok(data) = anthropic::encode_event(&event, &limits.anthropic) else {
                return Err(Error::TooLarge);
            };
            (name, data)
        }
        Provider::OpenAi => {
            let Some((name, event)) = openai_event(answer, sequence, call, options.usage_fields, &limits.openai)?
            else {
                return Ok(None);
            };
            let echo_event = match &event {
                openai::Event::Created { .. } | openai::Event::Completed { .. } => echo.enabled(),
                openai::Event::InProgress { .. }
                | openai::Event::Added { .. }
                | openai::Event::ToolAdded { .. }
                | openai::Event::Done { .. }
                | openai::Event::TextDelta { .. }
                | openai::Event::ArgumentsDelta { .. }
                | openai::Event::ReasoningDelta { .. }
                | openai::Event::Failed { .. }
                | openai::Event::Progress
                | openai::Event::Unknown => false,
            };
            let request = if echo_event {
                let value = openai::Json::from_bytes(body, &limits.openai).or(Err(Error::Malformed))?;
                Some(openai::decode_request(&value, &limits.openai).or(Err(Error::Malformed))?)
            } else {
                None
            };
            let data =
                openai::encode_peer_event(&event, request.as_ref(), echo, &limits.openai).or(Err(Error::TooLarge))?;
            (name, data)
        }
    };
    Ok(Some(Outgoing { name, data, id: None, retry: None }))
}

type AnthropicEvent = (Box<[u8]>, anthropic::Event);

type OpenAiEvent = (Box<[u8]>, openai::Event);

fn anthropic_event(
    answer: &api::Answer,
    sequence: u32,
    fields: UsageFields,
    limits: &anthropic::Limits,
) -> Result<Option<AnthropicEvent>, Error> {
    let mut usage = reported_usage(answer.usage, fields);
    usage.reasoning = None;
    if sequence == 0 {
        return Ok(Some((bytes::copy_of(b"message_start"), anthropic::Event::Started { usage })));
    }
    let at = sequence.saturating_sub(1).div_euclid(3);
    let phase = sequence.saturating_sub(1).rem_euclid(3);
    if let Some(part) = answer.parts.get(usize::try_from(at).expect("u32 fits usize")) {
        let (name, event) = match phase {
            0 => {
                let block = match part {
                    api::Part::Text { .. } => anthropic::BlockStart::Text { text: Box::new([]) },
                    api::Part::ToolCall { id, name, .. } => {
                        let Ok(input) = anthropic::Json::from_bytes(b"{}", limits) else {
                            return Err(Error::Malformed);
                        };
                        anthropic::BlockStart::ToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.to_bytes(limits).or(Err(Error::Malformed))?,
                        }
                    }
                    api::Part::Opaque { bytes } => {
                        let Ok(value) = anthropic::Json::from_bytes(bytes, limits) else {
                            return Err(Error::Malformed);
                        };
                        match string_field(value.document(), b"type")?.as_ref() {
                            b"thinking" => anthropic::BlockStart::Thinking {
                                text: string_field(value.document(), b"thinking")?,
                                signature: string_field(value.document(), b"signature")?,
                                head: value,
                            },
                            b"redacted_thinking" => anthropic::BlockStart::Redacted { value },
                            _ => anthropic::BlockStart::Opaque { value },
                        }
                    }
                    api::Part::ToolOutput { .. } => return Err(Error::Malformed),
                };
                (b"content_block_start".as_slice(), anthropic::Event::Added { index: at, block })
            }
            1 => match part {
                api::Part::Text { text } => (
                    b"content_block_delta".as_slice(),
                    anthropic::Event::Delta { index: at, delta: anthropic::Delta::Text { text: text.clone() } },
                ),
                api::Part::ToolCall { arguments, .. } => (
                    b"content_block_delta".as_slice(),
                    anthropic::Event::Delta {
                        index: at,
                        delta: anthropic::Delta::Arguments { text: arguments.clone() },
                    },
                ),
                api::Part::Opaque { .. } => (b"ping".as_slice(), anthropic::Event::Progress),
                api::Part::ToolOutput { .. } => return Err(Error::Malformed),
            },
            2 => match part {
                api::Part::ToolCall { .. } if answer.finish == api::Finish::Length => {
                    (b"ping".as_slice(), anthropic::Event::Progress)
                }
                api::Part::ToolCall { .. }
                | api::Part::Text { .. }
                | api::Part::Opaque { .. }
                | api::Part::ToolOutput { .. } => {
                    (b"content_block_stop".as_slice(), anthropic::Event::Done { index: at })
                }
            },
            _ => return Err(Error::Malformed),
        };
        return Ok(Some((bytes::copy_of(name), event)));
    }
    let count = u32::try_from(answer.parts.len()).or(Err(Error::TooLarge))?.checked_mul(3).ok_or(Error::TooLarge)?;
    if sequence == count.saturating_add(1) {
        return Ok(Some((
            bytes::copy_of(b"message_delta"),
            anthropic::Event::MessageDelta {
                stop: Some(anthropic_stop(answer.finish)),
                usage: anthropic::UsagePatch {
                    input: usage.input,
                    output: usage.output,
                    cache_read: usage.cache_read,
                    cache_write: usage.cache_write,
                },
            },
        )));
    }
    if sequence == count.saturating_add(2) {
        return Ok(Some((bytes::copy_of(b"message_stop"), anthropic::Event::Completed)));
    }
    Ok(None)
}

fn openai_event(
    answer: &api::Answer,
    sequence: u32,
    call: u64,
    fields: UsageFields,
    limits: &openai::Limits,
) -> Result<Option<OpenAiEvent>, Error> {
    if sequence == 0 {
        return Ok(Some((bytes::copy_of(b"response.created"), openai::Event::Created { echo: None })));
    }
    let index = sequence.saturating_sub(1).div_euclid(2);
    let done = sequence.saturating_sub(1).rem_euclid(2) == 1;
    if let Some(part) = answer.parts.get(usize::try_from(index).expect("u32 fits usize")) {
        let id = identifier(call, index);
        let (id, kind, item) = match part {
            api::Part::Text { text } => (
                id.clone(),
                bytes::copy_of(b"message"),
                openai::Item::Message {
                    id: id.clone(),
                    phase: Some(bytes::copy_of(b"final_answer")),
                    text: text.clone(),
                    refusal: false,
                },
            ),
            api::Part::ToolCall { id: call_id, name, arguments } => (
                id.clone(),
                bytes::copy_of(b"function_call"),
                openai::Item::FunctionCall {
                    id: id.clone(),
                    call_id: call_id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                },
            ),
            api::Part::Opaque { bytes } => {
                let Ok(value) = openai::Json::from_bytes(bytes, limits) else {
                    return Err(Error::Malformed);
                };
                let id = string_field(value.document(), b"id")?;
                let kind = string_field(value.document(), b"type")?;
                (id, kind, openai::Item::Opaque { value })
            }
            api::Part::ToolOutput { .. } => return Err(Error::Malformed),
        };
        match part {
            api::Part::ToolCall { id: call_id, name, arguments } => {
                if !done {
                    return Ok(Some((
                        bytes::copy_of(b"response.output_item.added"),
                        openai::Event::ToolAdded {
                            index,
                            id,
                            call_id: call_id.clone(),
                            name: name.clone(),
                            arguments: Box::new([]),
                        },
                    )));
                }
                if answer.finish == api::Finish::Length {
                    return Ok(Some((
                        bytes::copy_of(b"response.function_call_arguments.delta"),
                        openai::Event::ArgumentsDelta { index, delta: arguments.clone() },
                    )));
                }
            }
            api::Part::Text { .. } | api::Part::Opaque { .. } | api::Part::ToolOutput { .. } => {}
        }
        return Ok(Some(if done {
            (bytes::copy_of(b"response.output_item.done"), openai::Event::Done { index, item })
        } else {
            (bytes::copy_of(b"response.output_item.added"), openai::Event::Added { index, id, kind })
        }));
    }
    let count = u32::try_from(answer.parts.len()).or(Err(Error::TooLarge))?.checked_mul(2).ok_or(Error::TooLarge)?;
    if sequence == count.saturating_add(1) {
        return Ok(Some((
            bytes::copy_of(match answer.finish {
                api::Finish::Length | api::Finish::ContentFilter => b"response.incomplete",
                api::Finish::Stop | api::Finish::ToolCalls => b"response.completed",
            }),
            openai::Event::Completed { stop: openai_stop(answer.finish), usage: reported_usage(answer.usage, fields) },
        )));
    }
    Ok(None)
}

fn identifier(call: u64, index: u32) -> Box<[u8]> {
    let call = Decimal::of(call);
    let index = Decimal::of(u64::from(index));
    let mut out = Writer::new(call.as_bytes().len().saturating_add(index.as_bytes().len()).saturating_add(6));
    out.put(b"item_").expect("prefix");
    out.put(call.as_bytes()).expect("call");
    out.put(b"_").expect("separator");
    out.put(index.as_bytes()).expect("index");
    out.finish()
}

fn string_field(document: &Document, name: &[u8]) -> Result<Box<[u8]>, Error> {
    let mut depth = 0_u32;
    let mut found = None;
    for index in 0..document.len() {
        let record = document.token(index).ok_or(Error::Malformed)?;
        match record.kind {
            Kind::ObjectStart | Kind::ArrayStart => depth = depth.checked_add(1).ok_or(Error::TooLarge)?,
            Kind::ObjectEnd | Kind::ArrayEnd => depth = depth.checked_sub(1).ok_or(Error::Malformed)?,
            Kind::Key => {
                if depth == 1 && document.text(record) == Some(name) {
                    if found.is_some() {
                        return Err(Error::Malformed);
                    }
                    let next = document.token(index.checked_add(1).ok_or(Error::TooLarge)?).ok_or(Error::Malformed)?;
                    match next.kind {
                        Kind::String => found = Some(bytes::copy_of(document.text(next).ok_or(Error::Malformed)?)),
                        Kind::ObjectStart
                        | Kind::ObjectEnd
                        | Kind::ArrayStart
                        | Kind::ArrayEnd
                        | Kind::Key
                        | Kind::Number
                        | Kind::True
                        | Kind::False
                        | Kind::Null
                        | Kind::Long => return Err(Error::Malformed),
                    }
                }
            }
            Kind::String | Kind::Number | Kind::True | Kind::False | Kind::Null => {}
            Kind::Long => return Err(Error::Malformed),
        }
    }
    found.ok_or(Error::Malformed)
}

fn anthropic_stop(stop: api::Finish) -> anthropic::Stop {
    match stop {
        api::Finish::Stop => anthropic::Stop::EndTurn,
        api::Finish::ToolCalls => anthropic::Stop::ToolUse,
        api::Finish::Length => anthropic::Stop::MaxTokens,
        api::Finish::ContentFilter => anthropic::Stop::Refusal,
    }
}

fn openai_stop(stop: api::Finish) -> openai::Stop {
    match stop {
        api::Finish::Stop => openai::Stop::EndTurn,
        api::Finish::ToolCalls => openai::Stop::ToolUse,
        api::Finish::Length => openai::Stop::MaxTokens,
        api::Finish::ContentFilter => openai::Stop::Refusal,
    }
}

fn reported_usage(usage: api::Usage, fields: UsageFields) -> openai::Usage {
    openai::Usage {
        input: if fields.contains(UsageField::Input) { usage.input } else { None },
        cache_read: if fields.contains(UsageField::CacheRead) { usage.cache_read } else { None },
        cache_write: if fields.contains(UsageField::CacheWrite) { usage.cache_write } else { None },
        output: if fields.contains(UsageField::Output) { usage.output } else { None },
        reasoning: if fields.contains(UsageField::Reasoning) { usage.reasoning } else { None },
    }
}

// Deliberately independent of the client's affinity renderer: the byte peer
// validates the promised lowercase 8-4-4-4-12 form before assigning a scope.
pub(crate) fn parse_uuid(value: &[u8]) -> Option<[u8; 16]> {
    if value.len() != 36 {
        return None;
    }
    let mut result = [0_u8; 16];
    let mut at = 0_usize;
    for index in 0..16 {
        if index == 4 || index == 6 || index == 8 || index == 10 {
            if value.get(at) != Some(&b'-') {
                return None;
            }
            at = at.checked_add(1)?;
        }
        let high = nibble(*value.get(at)?)?;
        at = at.checked_add(1)?;
        let low = nibble(*value.get(at)?)?;
        at = at.checked_add(1)?;
        *result.get_mut(index).expect("fixed UUID width") = (high << 4_u32) | low;
    }
    Some(result)
}
fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => byte.checked_sub(b'0'),
        b'a'..=b'f' => byte.checked_sub(b'a')?.checked_add(10),
        _ => None,
    }
}
