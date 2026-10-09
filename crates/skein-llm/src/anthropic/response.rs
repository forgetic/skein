use crate::anthropic::{DecodeError, Failure, Json, Limits, ProviderError, RateLimit, Stop, Usage, classify};
use crate::openai::{clip_detail, json};
use alloc::boxed::Box;
use skein_json::{Document, Kind};
use skein_lib::{List, Queue, Wall, bytes};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum BlockStart {
    Text {
        text: Box<[u8]>,
    },
    ToolCall {
        id: Box<[u8]>,
        name: Box<[u8]>,
        input: Box<[u8]>,
    },
    /// Started signed thinking; the complete bounded provider head survives deltas.
    Thinking {
        /// Initial visible text, followed by subsequent thinking fragments.
        text: Box<[u8]>,
        /// Initial signature bytes, followed by signature fragments.
        signature: Box<[u8]>,
        /// Whole native object, including uninterpreted provider extension fields.
        /// Its type is `thinking`, its required thinking text matches `text`, and
        /// its optional signature matches `signature` (absence means empty).
        /// The decoder replaces only thinking/signature values when the block closes.
        head: Json,
    },
    Redacted {
        value: Json,
    },
    /// Whole provider-owned object of an unknown nonempty assistant block kind.
    /// Known text/tool/thinking kinds cannot masquerade as this opaque variant.
    /// No deltas or effects are inferred for an unknown kind.
    Opaque {
        /// Complete bounded native object, including nested proof/extension data.
        value: Json,
    },
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
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
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
    ToolCall { id: Box<[u8]>, name: Box<[u8]>, input: Box<[u8]>, too_large: bool, bytes: u64, cut: bool },
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
    ToolCall { id: Box<[u8]>, name: Box<[u8]>, input: List<u8>, fragmented: bool, too_large: bool, bytes: u64 },
    Thinking { text: List<u8>, signature: List<u8>, head: Json },
    Opaque { bytes: Box<[u8]> },
}
/// Messages emits one content block at a time, in increasing index order.
/// Completed blocks are emitted immediately; signed and redacted thinking
/// remain replayable opaque blocks. No unbounded fragment list is retained.
#[derive(Debug)]
pub struct StreamDecoder {
    active: Option<Active>,
    pending: Option<Part>,
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
            pending: None,
            next: 0,
            started: false,
            ending: false,
            over: false,
            stop: None,
            usage: Usage { input: None, output: None, cache_read: None, cache_write: None, reasoning: None },
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
            Err(DecodeError::TooLarge { which, bound }) => {
                self.fail(
                    Failure::Limit { which, bound },
                    bytes::copy_of(b"Anthropic stream exceeds configured limits"),
                    out,
                );
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
                self.usage.reasoning = None;
            }
            Event::Added { index, block } => {
                if let Some(part) = self.pending.take() {
                    self.emit(part, limits, out)?;
                }
                if !self.started || self.ending || self.active.is_some() || index != self.next {
                    return Err(DecodeError::Malformed);
                }
                if index >= limits.parts {
                    return Err(DecodeError::limit(crate::Cap::Parts, limits.parts));
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
                match &delta {
                    Delta::Arguments { .. } => {}
                    Delta::Text { .. } | Delta::Thinking { .. } | Delta::Signature { .. } => {
                        self.reserve_received(len, limits)?;
                    }
                }
                self.delta(index, delta, out)?;
            }
            Event::Done { index } => {
                self.check_active(index)?;
                let active = self.active.take().ok_or(DecodeError::Malformed)?;
                let part = finish(active, limits)?;
                self.next = self.next.checked_add(1).ok_or(DecodeError::limit(crate::Cap::Parts, limits.parts))?;
                match part {
                    part @ Part::ToolCall { .. } => self.pending = Some(part),
                    part @ (Part::Text { .. } | Part::Opaque { .. }) => self.emit(part, limits, out)?,
                }
            }
            Event::MessageDelta { stop, usage } => {
                if !self.started {
                    return Err(DecodeError::Malformed);
                }
                if let Some(active) = self.active.take() {
                    match active {
                        active @ Active::ToolCall { .. } if stop == Some(Stop::MaxTokens) => {
                            self.pending = Some(finish(active, limits)?);
                            self.next = self.next.checked_add(1).ok_or(DecodeError::Malformed)?;
                        }
                        Active::ToolCall { .. }
                        | Active::Text { .. }
                        | Active::Thinking { .. }
                        | Active::Opaque { .. } => return Err(DecodeError::Malformed),
                    }
                }
                if stop.is_some()
                    && let Some(mut part) = self.pending.take()
                {
                    match &mut part {
                        Part::ToolCall { cut, .. } => *cut = stop == Some(Stop::MaxTokens),
                        Part::Text { .. } | Part::Opaque { .. } => {}
                    }
                    self.emit(part, limits, out)?;
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
    fn emit(&mut self, part: Part, limits: &Limits, out: &mut Queue<Output>) -> Result<(), DecodeError> {
        let len = match &part {
            Part::Text { text } => text.len(),
            Part::ToolCall { id, name, input, too_large, .. } => {
                let arguments = if *too_large { 0 } else { input.len() };
                id.len().saturating_add(name.len()).saturating_add(arguments)
            }
            Part::Opaque { bytes } => bytes.len(),
        };
        self.part_bytes = self
            .part_bytes
            .checked_add(u64::try_from(len).expect("slice length fits u64"))
            .ok_or(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes))?;
        if self.part_bytes > u64::from(limits.answer_bytes) {
            return Err(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes));
        }
        out.push(Output::Part(part));
        Ok(())
    }
    fn start(&mut self, block: BlockStart, limits: &Limits) -> Result<Active, DecodeError> {
        match block {
            BlockStart::Text { text } => {
                self.reserve_received(text.len(), limits)?;
                let mut value = List::with_capacity(limits.answer_bytes);
                append(&mut value, &text, crate::Cap::Opaque)?;
                Ok(Active::Text { text: value })
            }
            BlockStart::ToolCall { id, name, input } => {
                let string_limit = usize::try_from(limits.string_bytes).expect("u32 fits usize");
                if id.is_empty() || name.is_empty() {
                    return Err(DecodeError::Malformed);
                }
                if id.len() > string_limit || name.len() > string_limit {
                    return Err(DecodeError::limit(crate::Cap::String, limits.string_bytes));
                }
                self.reserve_received(id.len().saturating_add(name.len()), limits)?;
                let mut value = List::with_capacity(limits.input_bytes);
                let too_large = input.len() > usize::try_from(value.room()).expect("u32 fits usize");
                if !too_large {
                    append(&mut value, &input, crate::Cap::String)?;
                }
                Ok(Active::ToolCall {
                    id,
                    name,
                    input: value,
                    fragmented: false,
                    too_large,
                    bytes: u64::try_from(input.len()).expect("slice length fits u64"),
                })
            }
            BlockStart::Thinking { text, signature, head } => {
                let head = thinking_head(&head, &text, &signature, limits)?;
                let serialized = head.to_bytes(limits)?;
                if serialized.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
                    return Err(DecodeError::limit(crate::Cap::Opaque, limits.opaque_bytes));
                }
                self.reserve_received(serialized.len(), limits)?;
                let mut value = List::with_capacity(limits.opaque_bytes);
                let mut signed = List::with_capacity(limits.opaque_bytes);
                append(&mut value, &text, crate::Cap::Opaque)?;
                append(&mut signed, &signature, crate::Cap::Opaque)?;
                Ok(Active::Thinking { text: value, signature: signed, head })
            }
            BlockStart::Redacted { value } => {
                validate_redacted(value.view())?;
                let data = value.to_bytes(limits)?;
                if data.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
                    return Err(DecodeError::limit(crate::Cap::Opaque, limits.opaque_bytes));
                }
                self.reserve_received(data.len(), limits)?;
                Ok(Active::Opaque { bytes: data })
            }
            BlockStart::Opaque { value } => {
                let value = Json::from_view(value.view(), limits)?;
                validate_opaque(value.view())?;
                let data = value.to_bytes(limits)?;
                if data.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
                    return Err(DecodeError::limit(crate::Cap::Opaque, limits.opaque_bytes));
                }
                self.reserve_received(data.len(), limits)?;
                Ok(Active::Opaque { bytes: data })
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
                append(text, &fragment, crate::Cap::Answer)?;
                out.push(Output::TextDelta { index, content_index: 0, text: fragment });
            }
            (Active::ToolCall { input, fragmented, too_large, bytes, .. }, Delta::Arguments { text: fragment }) => {
                if !*fragmented {
                    // The start's `{}` is a placeholder, replaced by partial_json.
                    input.clear();
                    *bytes = 0;
                    *too_large = false;
                    *fragmented = true;
                }
                *bytes = bytes
                    .checked_add(u64::try_from(fragment.len()).expect("slice length fits u64"))
                    .ok_or(DecodeError::Malformed)?;
                if !*too_large {
                    if fragment.len() > usize::try_from(input.room()).expect("u32 fits usize") {
                        *too_large = true;
                        input.clear();
                    } else {
                        append(input, &fragment, crate::Cap::String)?;
                    }
                }
                out.push(Output::ArgumentsDelta { index, delta: fragment });
            }
            (Active::Thinking { text, .. }, Delta::Thinking { text: fragment }) => {
                append(text, &fragment, crate::Cap::Opaque)?;
                out.push(Output::ReasoningDelta { index, summary_index: 0, text: fragment });
            }
            (Active::Thinking { signature, .. }, Delta::Signature { text }) => {
                append(signature, &text, crate::Cap::Opaque)?;
            }
            (
                Active::Text { .. } | Active::ToolCall { .. } | Active::Thinking { .. } | Active::Opaque { .. },
                Delta::Text { .. } | Delta::Arguments { .. } | Delta::Thinking { .. } | Delta::Signature { .. },
            ) => return Err(DecodeError::Malformed),
        }
        Ok(())
    }
    fn reserve_received(&mut self, len: usize, limits: &Limits) -> Result<(), DecodeError> {
        self.received_bytes = self
            .received_bytes
            .checked_add(u64::try_from(len).expect("usize fits u64"))
            .ok_or(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes))?;
        if self.received_bytes > u64::from(limits.answer_bytes) {
            return Err(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes));
        }
        Ok(())
    }
    fn fail(&mut self, failure: Failure, detail: Box<[u8]>, out: &mut Queue<Output>) {
        self.active = None;
        self.pending = None;
        self.over = true;
        let detail = match failure {
            Failure::Limit { which, bound } => crate::openai::limit_detail(which, bound),
            Failure::Protocol
            | Failure::Unauthorized
            | Failure::Exhausted { .. }
            | Failure::RateLimited { .. }
            | Failure::Overloaded
            | Failure::Unavailable
            | Failure::ContextTooLong
            | Failure::Invalid => detail,
        };
        out.push(Output::Failed { failure, detail: clip_detail(&detail, self.detail_bytes) });
    }
}

pub(super) fn thinking_head(head: &Json, text: &[u8], signature: &[u8], limits: &Limits) -> Result<Json, DecodeError> {
    let head = Json::from_view(head.view(), limits)?;
    let tokens = head.view();
    if text_ref(tokens, b"type")? != b"thinking" {
        return Err(DecodeError::WrongType);
    }
    if text_ref(tokens, b"thinking")? != text || optional_text(tokens, b"signature")?.as_ref() != signature {
        return Err(DecodeError::Malformed);
    }
    if head.to_bytes(limits)?.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
        return Err(DecodeError::limit(crate::Cap::Opaque, limits.opaque_bytes));
    }
    Ok(head)
}
fn append(out: &mut List<u8>, data: &[u8], cap: crate::Cap) -> Result<(), DecodeError> {
    if data.len() > usize::try_from(out.room()).expect("u32 fits usize") {
        return Err(DecodeError::limit(cap, out.capacity()));
    }
    for &byte in data {
        out.push(byte).or(Err(DecodeError::limit(cap, out.capacity())))?;
    }
    Ok(())
}
fn finish(active: Active, limits: &Limits) -> Result<Part, DecodeError> {
    match active {
        Active::Text { text } => Ok(Part::Text { text: text.into_boxed() }),
        Active::ToolCall { id, name, input, too_large, bytes, .. } => {
            Ok(Part::ToolCall { id, name, input: input.into_boxed(), too_large, bytes, cut: false })
        }
        Active::Thinking { text, signature, head } => {
            if signature.is_empty() {
                return Err(DecodeError::Malformed);
            }
            let bounded = skein_json::writer::Limits {
                depth: limits.depth,
                length: limits.opaque_bytes.min(limits.document_bytes),
            };
            let mut measure = skein_json::writer::Encoder::measure(&bounded);
            write_thinking(&mut measure, &head, text.as_slice(), signature.as_slice());
            let len = crate::openai::measured(measure, bounded, crate::Cap::Opaque)?;
            let mut out = skein_json::writer::Encoder::write(len, &bounded);
            write_thinking(&mut out, &head, text.as_slice(), signature.as_slice());
            Ok(Part::Opaque { bytes: out.finish() })
        }
        Active::Opaque { bytes } => Ok(Part::Opaque { bytes }),
    }
}
pub(super) fn write_thinking(out: &mut skein_json::writer::Encoder, head: &Json, text: &[u8], signature: &[u8]) {
    out.object_start();
    let tokens = head.view();
    let mut skip = 1_u32;
    for at in 1..json::len(tokens) {
        if at < skip {
            continue;
        }
        match json::kind(tokens, at).expect("admitted record") {
            Kind::Key => {
                let key = json::record_text(tokens, at).expect("admitted key");
                let start = at.checked_add(1).expect("bounded token offset");
                skip = json::span(tokens, start).expect("admitted provider envelope");
                if key != b"thinking" && key != b"signature" {
                    out.key(key);
                    json::write_view(out, json::value_at(tokens, start).expect("admitted value"));
                }
            }
            Kind::ObjectEnd => break,
            Kind::ObjectStart
            | Kind::ArrayStart
            | Kind::ArrayEnd
            | Kind::String
            | Kind::Number
            | Kind::True
            | Kind::False
            | Kind::Null
            | Kind::Long => unreachable!("object values already consumed"),
        }
    }
    out.key(b"thinking");
    out.string(text);
    out.key(b"signature");
    out.string(signature);
    out.object_end();
}

fn merge_usage(usage: &mut Usage, patch: UsagePatch) {
    if let Some(value) = patch.input {
        usage.input = Some(value);
    }
    if let Some(value) = patch.output {
        usage.output = Some(value);
    }
    if let Some(value) = patch.cache_read {
        usage.cache_read = Some(value);
    }
    if let Some(value) = patch.cache_write {
        usage.cache_write = Some(value);
    }
}

pub fn decode_event(value: &Json, limits: &Limits) -> Result<Event, DecodeError> {
    let tokens = value.view();
    match text_ref(tokens, b"type")? {
        b"message_start" => {
            let message = field(tokens, b"message")?;
            if text_ref(message, b"type")? != b"message" || text_ref(message, b"role")? != b"assistant" {
                return Err(DecodeError::Malformed);
            }
            if !json::array(field(message, b"content")?, 0)?.is_empty() {
                return Err(DecodeError::Malformed);
            }
            let patch = read_usage(json::reported_field(message, b"usage"));
            let mut usage = Usage::NONE;
            merge_usage(&mut usage, patch);
            Ok(Event::Started { usage })
        }
        b"content_block_start" => {
            let block = field(tokens, b"content_block")?;
            let block = match text_ref(block, b"type")? {
                b"text" => BlockStart::Text { text: text(block, b"text")? },
                b"tool_use" => {
                    let input = field(block, b"input")?;
                    if json::kind(input, 0) != Some(Kind::ObjectStart) {
                        return Err(DecodeError::WrongType);
                    }
                    BlockStart::ToolCall {
                        id: text(block, b"id")?,
                        name: text(block, b"name")?,
                        input: Json::from_view(input, limits)?.to_bytes(limits)?,
                    }
                }
                b"thinking" => BlockStart::Thinking {
                    text: text(block, b"thinking")?,
                    signature: optional_text(block, b"signature")?,
                    head: Json::from_view(block, limits)?,
                },
                b"redacted_thinking" => {
                    validate_redacted(block)?;
                    BlockStart::Redacted { value: Json::from_view(block, limits)? }
                }
                _ => {
                    validate_opaque(block)?;
                    BlockStart::Opaque { value: Json::from_view(block, limits)? }
                }
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
            let value = field(field(tokens, b"delta")?, b"stop_reason")?;
            let stop = if json::kind(value, 0) == Some(Kind::Null) && json::len(value) == 1 {
                None
            } else {
                match json::text_ref(value)? {
                    b"end_turn" | b"stop_sequence" | b"pause_turn" => Some(Stop::EndTurn),
                    b"tool_use" => Some(Stop::ToolUse),
                    b"max_tokens" => Some(Stop::MaxTokens),
                    b"refusal" => Some(Stop::Refusal),
                    _ => return Err(DecodeError::WrongType),
                }
            };
            Ok(Event::MessageDelta { stop, usage: read_usage(json::reported_field(tokens, b"usage")) })
        }
        b"message_stop" => Ok(Event::Completed),
        b"error" => Ok(Event::Failed { error: decode_error(value, limits)? }),
        b"ping" => Ok(Event::Progress),
        _ => Ok(Event::Unknown),
    }
}
pub(super) fn validate_redacted(tokens: (&Document, json::Span)) -> Result<(), DecodeError> {
    if text_ref(tokens, b"type")? != b"redacted_thinking" || text_ref(tokens, b"data")?.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(())
}

pub(super) fn validate_opaque(tokens: (&Document, json::Span)) -> Result<(), DecodeError> {
    match text_ref(tokens, b"type")? {
        b"" => Err(DecodeError::Malformed),
        b"text" | b"tool_use" | b"tool_result" | b"thinking" | b"redacted_thinking" => Err(DecodeError::WrongType),
        _ => Ok(()),
    }
}
pub fn decode_error(value: &Json, limits: &Limits) -> Result<ProviderError, DecodeError> {
    crate::openai::decode_error(value, limits)
}
fn read_usage(tokens: Option<(&Document, json::Span)>) -> UsagePatch {
    let Some(tokens) = tokens else {
        return UsagePatch { input: None, output: None, cache_read: None, cache_write: None };
    };
    UsagePatch {
        input: json::reported_unsigned(tokens, b"input_tokens"),
        output: json::reported_unsigned(tokens, b"output_tokens"),
        cache_read: json::reported_unsigned(tokens, b"cache_read_input_tokens"),
        cache_write: json::reported_unsigned(tokens, b"cache_creation_input_tokens"),
    }
}
fn optional_text(tokens: (&Document, json::Span), name: &[u8]) -> Result<Box<[u8]>, DecodeError> {
    match json::optional_at(tokens, json::field(tokens, name)?)? {
        Some(value) => json::text(value),
        None => Ok(bytes::copy_of(b"")),
    }
}
fn field<'a>(tokens: (&'a Document, json::Span), name: &[u8]) -> Result<(&'a Document, json::Span), DecodeError> {
    json::value_at(tokens, json::required(tokens, name)?)
}
fn text_ref<'a>(tokens: (&'a Document, json::Span), name: &[u8]) -> Result<&'a [u8], DecodeError> {
    json::text_ref(field(tokens, name)?)
}
fn text(tokens: (&Document, json::Span), name: &[u8]) -> Result<Box<[u8]>, DecodeError> {
    json::text(field(tokens, name)?)
}
fn index(tokens: (&Document, json::Span)) -> Result<u32, DecodeError> {
    u32::try_from(json::unsigned(field(tokens, b"index")?)?).or(Err(DecodeError::Malformed))
}
/// Includes the active block's buffers and replay JSON construction, excluding
/// the common JSON event/parser/writer and the owner's output queue storage.
#[must_use]
pub fn decoder_worst_case(limits: &Limits) -> Option<u64> {
    u64::from(limits.answer_bytes)
        .checked_add(u64::from(limits.input_bytes).checked_mul(2)?)?
        .checked_add(u64::from(limits.opaque_bytes).checked_mul(4)?)?
        .checked_add(u64::from(limits.string_bytes).checked_mul(2)?)?
        .checked_add(List::<skein_json::Compact>::worst_case(limits.tokens)?.checked_mul(2)?)?
        .checked_add(u64::from(limits.document_bytes).checked_mul(2)?)
}
