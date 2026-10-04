use crate::anthropic::{DecodeError, Failure, Json, Limits, ProviderError, RateLimit, Stop, Usage, classify};
use crate::openai::{clip_detail, json};
use alloc::boxed::Box;
use skein_json::Token;
use skein_lib::{List, Queue, Wall, bytes};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum BlockStart {
    Text { text: Box<[u8]> },
    ToolCall { id: Box<[u8]>, name: Box<[u8]>, input: Box<[u8]> },
    Thinking { text: Box<[u8]>, signature: Box<[u8]> },
    Redacted { value: Json },
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Delta {
    Text { text: Box<[u8]> },
    Arguments { text: Box<[u8]> },
    Thinking { text: Box<[u8]> },
    Signature { text: Box<[u8]> },
}
/// Fields omitted from cumulative usage updates retain their previous values.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct UsagePatch {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    Started { usage: Usage },
    Added { index: u32, block: BlockStart },
    Delta { index: u32, delta: Delta },
    Done { index: u32 },
    MessageDelta { stop: Option<Stop>, usage: UsagePatch },
    Completed,
    Failed { error: ProviderError },
    Progress,
    Unknown,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Part {
    Text { text: Box<[u8]> },
    ToolCall { id: Box<[u8]>, name: Box<[u8]>, input: Box<[u8]>, too_large: bool },
    Opaque { bytes: Box<[u8]> },
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Output {
    Part(Part),
    TextDelta { index: u32, content_index: u32, text: Box<[u8]> },
    ArgumentsDelta { index: u32, delta: Box<[u8]> },
    ReasoningDelta { index: u32, summary_index: u32, text: Box<[u8]> },
    Completed { stop: Stop, usage: Usage },
    Failed { failure: Failure, detail: Box<[u8]> },
    Progress,
}
pub const MAX_OUT: u32 = 3;

#[derive(Debug)]
enum Active {
    Text { text: List<u8> },
    ToolCall { id: Box<[u8]>, name: Box<[u8]>, input: List<u8>, fragmented: bool, too_large: bool },
    Thinking { text: List<u8>, signature: List<u8> },
    Redacted { bytes: Box<[u8]> },
}
/// Messages emits one content block at a time, in increasing index order.
/// Completed blocks are emitted immediately; signed and redacted thinking
/// remain replayable opaque blocks. No unbounded fragment list is retained.
#[derive(Debug)]
pub struct StreamDecoder {
    active: Option<Active>,
    next: u32,
    started: bool,
    ending: bool,
    over: bool,
    stop: Option<Stop>,
    usage: Usage,
    received_bytes: u64,
    part_bytes: u64,
    detail_bytes: u32,
}
impl StreamDecoder {
    #[must_use]
    pub const fn new(limits: &Limits) -> StreamDecoder {
        StreamDecoder {
            active: None,
            next: 0,
            started: false,
            ending: false,
            over: false,
            stop: None,
            usage: Usage::ZERO,
            received_bytes: 0,
            part_bytes: 0,
            detail_bytes: limits.detail_bytes,
        }
    }
    /// The owner reserves `MAX_OUT` queue slots before delivering an event.
    /// Events following a terminal outcome are ignored.
    pub fn event(&mut self, event: Event, limits: &Limits, wall: Wall, out: &mut Queue<Output>) {
        if self.over {
            return;
        }
        let before = out.len();
        match self.accept(event, limits, wall, out) {
            Ok(()) => {
                if before == out.len() {
                    out.push(Output::Progress);
                }
            }
            Err(DecodeError::TooLarge) => {
                self.fail(Failure::Limit, bytes::copy_of(b"Anthropic stream exceeds configured limits"), out);
            }
            Err(DecodeError::Malformed | DecodeError::Missing | DecodeError::WrongType) => {
                self.fail(Failure::Protocol, bytes::copy_of(b"malformed Anthropic stream"), out);
            }
        }
    }
    /// All output is produced at the triggering event, with no ready backlog.
    #[must_use]
    pub const fn has_ready(&self) -> bool {
        false
    }
    /// Present for the common provider decoder interface; there is no backlog.
    pub const fn ready(&mut self, _out: &mut Queue<Output>) {}
    pub fn end(&mut self, out: &mut Queue<Output>) {
        if !self.over {
            self.fail(Failure::Protocol, bytes::copy_of(b"incomplete Anthropic stream"), out);
        }
    }
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.over
    }
    fn accept(
        &mut self,
        event: Event,
        limits: &Limits,
        wall: Wall,
        out: &mut Queue<Output>,
    ) -> Result<(), DecodeError> {
        match event {
            Event::Started { usage } => {
                if self.started {
                    return Err(DecodeError::Malformed);
                }
                self.started = true;
                self.usage = usage;
            }
            Event::Added { index, block } => {
                if !self.started || self.ending || self.active.is_some() || index != self.next {
                    return Err(DecodeError::Malformed);
                }
                if index >= limits.parts {
                    return Err(DecodeError::TooLarge);
                }
                self.active = Some(self.start(block, limits)?);
            }
            Event::Delta { index, delta } => {
                self.check_active(index)?;
                let len = match &delta {
                    Delta::Text { text }
                    | Delta::Arguments { text }
                    | Delta::Thinking { text }
                    | Delta::Signature { text } => text.len(),
                };
                self.reserve_received(len, limits)?;
                self.delta(index, delta, out)?;
            }
            Event::Done { index } => {
                self.check_active(index)?;
                let active = self.active.take().ok_or(DecodeError::Malformed)?;
                let part = finish(active, limits)?;
                let len = match &part {
                    Part::Text { text } => text.len(),
                    Part::ToolCall { id, name, input, .. } => {
                        id.len().saturating_add(name.len()).saturating_add(input.len())
                    }
                    Part::Opaque { bytes } => bytes.len(),
                };
                let total = self.part_bytes.checked_add(u64::try_from(len).expect("usize fits u64"));
                self.part_bytes = total.ok_or(DecodeError::TooLarge)?;
                if self.part_bytes > u64::from(limits.answer_bytes) {
                    return Err(DecodeError::TooLarge);
                }
                self.next = self.next.checked_add(1).ok_or(DecodeError::TooLarge)?;
                out.push(Output::Part(part));
            }
            Event::MessageDelta { stop, usage } => {
                if !self.started || self.active.is_some() {
                    return Err(DecodeError::Malformed);
                }
                self.ending = true;
                if let Some(stop) = stop {
                    if let Some(previous) = self.stop
                        && stop != previous
                    {
                        return Err(DecodeError::Malformed);
                    }
                    self.stop = Some(stop);
                }
                merge_usage(&mut self.usage, usage);
            }
            Event::Completed => {
                if !self.started || self.active.is_some() {
                    return Err(DecodeError::Malformed);
                }
                let stop = self.stop.ok_or(DecodeError::Malformed)?;
                self.over = true;
                out.push(Output::Completed { stop, usage: self.usage });
            }
            Event::Failed { error } => {
                let failure = classify(0, Some(&error), RateLimit::NONE, wall);
                self.fail(failure, clip_detail(&error.message, limits.detail_bytes), out);
            }
            Event::Progress | Event::Unknown => {}
        }
        Ok(())
    }
    fn start(&mut self, block: BlockStart, limits: &Limits) -> Result<Active, DecodeError> {
        match block {
            BlockStart::Text { text } => {
                self.reserve_received(text.len(), limits)?;
                let mut value = List::with_capacity(limits.answer_bytes);
                append(&mut value, &text)?;
                Ok(Active::Text { text: value })
            }
            BlockStart::ToolCall { id, name, input } => {
                let string_limit = usize::try_from(limits.string_bytes).expect("u32 fits usize");
                if id.is_empty() || name.is_empty() {
                    return Err(DecodeError::Malformed);
                }
                if id.len() > string_limit || name.len() > string_limit {
                    return Err(DecodeError::TooLarge);
                }
                self.reserve_received(id.len().saturating_add(name.len()).saturating_add(input.len()), limits)?;
                let mut value = List::with_capacity(limits.input_bytes);
                let too_large = input.len() > usize::try_from(value.room()).expect("u32 fits usize");
                if !too_large {
                    append(&mut value, &input)?;
                }
                Ok(Active::ToolCall { id, name, input: value, fragmented: false, too_large })
            }
            BlockStart::Thinking { text, signature } => {
                self.reserve_received(text.len().saturating_add(signature.len()), limits)?;
                let mut value = List::with_capacity(limits.opaque_bytes);
                let mut signed = List::with_capacity(limits.opaque_bytes);
                append(&mut value, &text)?;
                append(&mut signed, &signature)?;
                Ok(Active::Thinking { text: value, signature: signed })
            }
            BlockStart::Redacted { value } => {
                validate_redacted(value.as_tokens())?;
                let data = value.to_bytes(limits)?;
                if data.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
                    return Err(DecodeError::TooLarge);
                }
                self.reserve_received(data.len(), limits)?;
                Ok(Active::Redacted { bytes: data })
            }
        }
    }
    fn check_active(&self, index: u32) -> Result<(), DecodeError> {
        if !self.started || self.ending || index != self.next || self.active.is_none() {
            return Err(DecodeError::Malformed);
        }
        Ok(())
    }
    fn delta(&mut self, index: u32, delta: Delta, out: &mut Queue<Output>) -> Result<(), DecodeError> {
        let active = self.active.as_mut().ok_or(DecodeError::Malformed)?;
        match (active, delta) {
            (Active::Text { text }, Delta::Text { text: fragment }) => {
                append(text, &fragment)?;
                out.push(Output::TextDelta { index, content_index: 0, text: fragment });
            }
            (Active::ToolCall { input, fragmented, too_large, .. }, Delta::Arguments { text: fragment }) => {
                if !*fragmented {
                    // The start's `{}` is a placeholder, replaced by partial_json.
                    input.clear();
                    *fragmented = true;
                }
                if !*too_large {
                    if fragment.len() > usize::try_from(input.room()).expect("u32 fits usize") {
                        *too_large = true;
                        input.clear();
                    } else {
                        append(input, &fragment)?;
                    }
                }
                out.push(Output::ArgumentsDelta { index, delta: fragment });
            }
            (Active::Thinking { text, .. }, Delta::Thinking { text: fragment }) => {
                append(text, &fragment)?;
                out.push(Output::ReasoningDelta { index, summary_index: 0, text: fragment });
            }
            (Active::Thinking { signature, .. }, Delta::Signature { text }) => append(signature, &text)?,
            (
                Active::Text { .. } | Active::ToolCall { .. } | Active::Thinking { .. } | Active::Redacted { .. },
                Delta::Text { .. } | Delta::Arguments { .. } | Delta::Thinking { .. } | Delta::Signature { .. },
            ) => return Err(DecodeError::Malformed),
        }
        Ok(())
    }
    fn reserve_received(&mut self, len: usize, limits: &Limits) -> Result<(), DecodeError> {
        self.received_bytes = self
            .received_bytes
            .checked_add(u64::try_from(len).expect("usize fits u64"))
            .ok_or(DecodeError::TooLarge)?;
        if self.received_bytes > u64::from(limits.answer_bytes) {
            return Err(DecodeError::TooLarge);
        }
        Ok(())
    }
    fn fail(&mut self, failure: Failure, detail: Box<[u8]>, out: &mut Queue<Output>) {
        self.active = None;
        self.over = true;
        out.push(Output::Failed { failure, detail: clip_detail(&detail, self.detail_bytes) });
    }
}
fn append(out: &mut List<u8>, data: &[u8]) -> Result<(), DecodeError> {
    if data.len() > usize::try_from(out.room()).expect("u32 fits usize") {
        return Err(DecodeError::TooLarge);
    }
    for &byte in data {
        out.push(byte).or(Err(DecodeError::TooLarge))?;
    }
    Ok(())
}
fn finish(active: Active, limits: &Limits) -> Result<Part, DecodeError> {
    match active {
        Active::Text { text } => Ok(Part::Text { text: text.into_boxed() }),
        Active::ToolCall { id, name, input, too_large, .. } => {
            Ok(Part::ToolCall { id, name, input: input.into_boxed(), too_large })
        }
        Active::Thinking { text, signature } => {
            if signature.is_empty() {
                return Err(DecodeError::Malformed);
            }
            let tokens = [
                Token::ObjectStart,
                Token::Key(bytes::copy_of(b"type")),
                Token::String(bytes::copy_of(b"thinking")),
                Token::Key(bytes::copy_of(b"thinking")),
                Token::String(text.into_boxed()),
                Token::Key(bytes::copy_of(b"signature")),
                Token::String(signature.into_boxed()),
                Token::ObjectEnd,
            ];
            let value = Json::from_tokens(&tokens, limits)?;
            let data = value.to_bytes(limits)?;
            if data.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
                return Err(DecodeError::TooLarge);
            }
            Ok(Part::Opaque { bytes: data })
        }
        Active::Redacted { bytes } => Ok(Part::Opaque { bytes }),
    }
}
fn merge_usage(usage: &mut Usage, patch: UsagePatch) {
    if let Some(value) = patch.input_tokens {
        usage.input_tokens = value;
    }
    if let Some(value) = patch.output_tokens {
        usage.output_tokens = value;
    }
    if let Some(value) = patch.cache_read_tokens {
        usage.cache_read_tokens = value;
    }
    if let Some(value) = patch.cache_write_tokens {
        usage.cache_write_tokens = value;
    }
}

pub fn decode_event(value: &Json, limits: &Limits) -> Result<Event, DecodeError> {
    let tokens = value.as_tokens();
    match text_ref(tokens, b"type")? {
        b"message_start" => {
            let message = field(tokens, b"message")?;
            if text_ref(message, b"type")? != b"message" || text_ref(message, b"role")? != b"assistant" {
                return Err(DecodeError::Malformed);
            }
            if !json::array(field(message, b"content")?, 0)?.is_empty() {
                return Err(DecodeError::Malformed);
            }
            let patch = read_usage(field(message, b"usage")?)?;
            let mut usage = Usage::ZERO;
            merge_usage(&mut usage, patch);
            Ok(Event::Started { usage })
        }
        b"content_block_start" => {
            let block = field(tokens, b"content_block")?;
            let block = match text_ref(block, b"type")? {
                b"text" => BlockStart::Text { text: text(block, b"text")? },
                b"tool_use" => {
                    let input = field(block, b"input")?;
                    if input.first() != Some(&Token::ObjectStart) {
                        return Err(DecodeError::WrongType);
                    }
                    BlockStart::ToolCall {
                        id: text(block, b"id")?,
                        name: text(block, b"name")?,
                        input: Json::from_tokens(input, limits)?.to_bytes(limits)?,
                    }
                }
                b"thinking" => BlockStart::Thinking {
                    text: text(block, b"thinking")?,
                    signature: optional_text(block, b"signature")?,
                },
                b"redacted_thinking" => {
                    validate_redacted(block)?;
                    BlockStart::Redacted { value: Json::from_tokens(block, limits)? }
                }
                _ => return Err(DecodeError::WrongType),
            };
            Ok(Event::Added { index: index(tokens)?, block })
        }
        b"content_block_delta" => {
            let delta = field(tokens, b"delta")?;
            let delta = match text_ref(delta, b"type")? {
                b"text_delta" => Delta::Text { text: text(delta, b"text")? },
                b"input_json_delta" => Delta::Arguments { text: text(delta, b"partial_json")? },
                b"thinking_delta" => Delta::Thinking { text: text(delta, b"thinking")? },
                b"signature_delta" => Delta::Signature { text: text(delta, b"signature")? },
                _ => return Err(DecodeError::WrongType),
            };
            Ok(Event::Delta { index: index(tokens)?, delta })
        }
        b"content_block_stop" => Ok(Event::Done { index: index(tokens)? }),
        b"message_delta" => {
            let stop = match field(field(tokens, b"delta")?, b"stop_reason")? {
                [Token::Null] => None,
                [Token::String(value)] => match value.as_ref() {
                    b"end_turn" | b"stop_sequence" | b"pause_turn" => Some(Stop::EndTurn),
                    b"tool_use" => Some(Stop::ToolUse),
                    b"max_tokens" => Some(Stop::MaxTokens),
                    b"refusal" => Some(Stop::Refusal),
                    _ => return Err(DecodeError::WrongType),
                },
                _ => return Err(DecodeError::WrongType),
            };
            Ok(Event::MessageDelta { stop, usage: read_usage(field(tokens, b"usage")?)? })
        }
        b"message_stop" => Ok(Event::Completed),
        b"error" => Ok(Event::Failed { error: decode_error(value, limits)? }),
        b"ping" => Ok(Event::Progress),
        _ => Ok(Event::Unknown),
    }
}
fn validate_redacted(tokens: &[Token]) -> Result<(), DecodeError> {
    if text_ref(tokens, b"type")? != b"redacted_thinking" || text_ref(tokens, b"data")?.is_empty() {
        return Err(DecodeError::Malformed);
    }
    // Replay admission accepts precisely this schema, so no provider fields
    // may be silently dropped between receipt and replay.
    for token in tokens {
        if let Token::Key(name) = token
            && name.as_ref() != b"type"
            && name.as_ref() != b"data"
        {
            return Err(DecodeError::WrongType);
        }
    }
    Ok(())
}
pub fn decode_error(value: &Json, limits: &Limits) -> Result<ProviderError, DecodeError> {
    crate::openai::decode_error(value, limits)
}
fn read_usage(tokens: &[Token]) -> Result<UsagePatch, DecodeError> {
    Ok(UsagePatch {
        input_tokens: optional_unsigned(tokens, b"input_tokens")?,
        output_tokens: optional_unsigned(tokens, b"output_tokens")?,
        cache_read_tokens: optional_unsigned(tokens, b"cache_read_input_tokens")?,
        cache_write_tokens: optional_unsigned(tokens, b"cache_creation_input_tokens")?,
    })
}
fn optional_unsigned(tokens: &[Token], name: &[u8]) -> Result<Option<u64>, DecodeError> {
    match json::optional_at(tokens, json::field(tokens, name)?)? {
        Some(value) => Ok(Some(json::unsigned(value)?)),
        None => Ok(None),
    }
}
fn optional_text(tokens: &[Token], name: &[u8]) -> Result<Box<[u8]>, DecodeError> {
    match json::optional_at(tokens, json::field(tokens, name)?)? {
        Some(value) => json::text(value),
        None => Ok(bytes::copy_of(b"")),
    }
}
fn field<'a>(tokens: &'a [Token], name: &[u8]) -> Result<&'a [Token], DecodeError> {
    json::value_at(tokens, json::required(tokens, name)?)
}
fn text_ref<'a>(tokens: &'a [Token], name: &[u8]) -> Result<&'a [u8], DecodeError> {
    json::text_ref(field(tokens, name)?)
}
fn text(tokens: &[Token], name: &[u8]) -> Result<Box<[u8]>, DecodeError> {
    json::text(field(tokens, name)?)
}
fn index(tokens: &[Token]) -> Result<u32, DecodeError> {
    u32::try_from(json::unsigned(field(tokens, b"index")?)?).or(Err(DecodeError::TooLarge))
}
/// Includes the active block's buffers and replay JSON construction, excluding
/// the common JSON event/parser/writer and the owner's output queue storage.
#[must_use]
pub fn decoder_worst_case(limits: &Limits) -> Option<u64> {
    u64::from(limits.answer_bytes)
        .checked_add(u64::from(limits.input_bytes))?
        .checked_add(u64::from(limits.opaque_bytes).checked_mul(4)?)?
        .checked_add(u64::from(limits.string_bytes).checked_mul(2)?)?
        .checked_add(List::<Token>::worst_case(limits.tokens)?)
}
