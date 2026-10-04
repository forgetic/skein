use crate::openai::{
    DecodeError, Failure, Json, Limits, ProviderError, RateLimit, Request, Stop, Usage, classify, common, json, request,
};
use alloc::boxed::Box;
use core::mem;
use skein_json::{Token, writer::Encoder};
use skein_lib::{List, Queue, Wall, Writer, bytes};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Item {
    Message { id: Box<[u8]>, phase: Option<Box<[u8]>>, text: Box<[u8]>, refusal: bool },
    FunctionCall { id: Box<[u8]>, call_id: Box<[u8]>, name: Box<[u8]>, arguments: Box<[u8]> },
    Opaque { value: Json },
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    Created { echo: Option<Request> },
    InProgress { echo: Option<Request> },
    Added { index: u32, id: Box<[u8]>, kind: Box<[u8]> },
    Done { index: u32, item: Item },
    TextDelta { index: u32, content_index: u32, text: Box<[u8]> },
    ArgumentsDelta { index: u32, delta: Box<[u8]> },
    ReasoningDelta { index: u32, summary_index: u32, text: Box<[u8]> },
    Completed { stop: Stop, usage: Usage },
    Failed { error: ProviderError },
    Progress,
    Unknown,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Part {
    Text { id: Box<[u8]>, phase: Option<Box<[u8]>>, text: Box<[u8]>, refusal: bool },
    Opaque { bytes: Box<[u8]> },
    ToolCall { id: Box<[u8]>, name: Box<[u8]>, input: Box<[u8]>, too_large: bool },
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
pub struct StreamDecoder {
    opened: List<Opened>,
    next: u32,
    parts: u32,
    bytes: u64,
    delta_bytes: u64,
    detail_bytes: u32,
    tools: bool,
    refusal: bool,
    over: bool,
    terminal: Option<Terminal>,
}
#[derive(Debug)]
enum Opened {
    Active { id: Box<[u8]>, kind: Box<[u8]> },
    Ready(Prepared),
    Emitted,
}
#[derive(Debug)]
struct Prepared {
    part: Part,
    tool: bool,
    refusal: bool,
}
#[derive(Clone, Copy, Debug)]
struct Terminal {
    stop: Stop,
    usage: Usage,
}
impl StreamDecoder {
    #[must_use]
    pub fn new(limits: &Limits) -> StreamDecoder {
        StreamDecoder {
            opened: List::with_capacity(limits.parts),
            next: 0,
            parts: 0,
            bytes: 0,
            delta_bytes: 0,
            detail_bytes: limits.detail_bytes,
            tools: false,
            refusal: false,
            over: false,
            terminal: None,
        }
    }
    /// One event, with the caller reserving `MAX_OUT` output slots. Completed
    /// items may arrive in any order; only the next ordered item is emitted.
    /// Deltas are forwarded immediately with their original item/content index.
    /// Drain `has_ready()` before delivering another event. Events following
    /// a terminal outcome are ignored.
    pub fn event(&mut self, event: Event, limits: &Limits, wall: Wall, out: &mut Queue<Output>) {
        if self.over {
            return;
        }
        if self.terminal.is_some() {
            self.ready(out);
            return;
        }
        let before = out.len();
        let result = self.accept(event, limits, wall, out);
        match result {
            Ok(()) => {
                if out.len() == before {
                    out.push(Output::Progress);
                }
            }
            Err(DecodeError::TooLarge) => {
                self.fail(Failure::Limit, bytes::copy_of(b"ChatGPT stream exceeds configured limits"), out);
            }
            Err(DecodeError::Malformed | DecodeError::Missing | DecodeError::WrongType) => {
                self.fail(Failure::Protocol, bytes::copy_of(b"malformed ChatGPT stream"), out);
            }
        }
    }
    /// True when another bounded ready call can emit. The owner puts this
    /// decoder on its ready list and drains it before reading another event.
    #[must_use]
    pub fn has_ready(&self) -> bool {
        if self.over {
            return false;
        }
        match self.opened.get(self.next) {
            Some(Opened::Ready(_)) => true,
            Some(Opened::Active { .. }) => false,
            Some(Opened::Emitted) | None => self.terminal.is_some(),
        }
    }
    /// Emits at most one ordered completed item and its final terminal.
    pub fn ready(&mut self, out: &mut Queue<Output>) {
        if self.over {
            return;
        }
        for _slot in 0..self.opened.len() {
            let Some(slot) = self.opened.get_mut(self.next) else {
                break;
            };
            match slot {
                Opened::Active { .. } => break,
                Opened::Emitted => self.next = self.next.saturating_add(1),
                Opened::Ready(_) => {
                    let state = mem::replace(slot, Opened::Emitted);
                    match state {
                        Opened::Ready(prepared) => {
                            out.push(Output::Part(prepared.part));
                        }
                        Opened::Active { .. } | Opened::Emitted => unreachable!("the slot was ready"),
                    }
                    self.next = self.next.saturating_add(1);
                    break;
                }
            }
        }
        if self.next == self.opened.len()
            && let Some(terminal) = self.terminal.take()
        {
            self.over = true;
            self.opened.clear();
            let stop = match terminal.stop {
                Stop::MaxTokens | Stop::Refusal => terminal.stop,
                Stop::EndTurn | Stop::ToolUse => {
                    if self.refusal {
                        Stop::Refusal
                    } else if self.tools {
                        Stop::ToolUse
                    } else {
                        terminal.stop
                    }
                }
            };
            out.push(Output::Completed { stop, usage: terminal.usage });
        }
    }
    pub fn end(&mut self, out: &mut Queue<Output>) {
        if !self.over {
            if self.terminal.is_some() {
                self.ready(out);
            } else {
                self.fail(Failure::Protocol, bytes::copy_of(b"incomplete ChatGPT stream"), out);
            }
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
            Event::Created { .. } | Event::InProgress { .. } | Event::Progress | Event::Unknown => {
                out.push(Output::Progress);
            }
            Event::Added { index, id, kind } => {
                let limit = usize::try_from(limits.string_bytes).expect("u32 fits usize");
                if id.len() > limit || kind.len() > limit {
                    return Err(DecodeError::TooLarge);
                }
                if index != self.opened.len() {
                    return Err(DecodeError::Malformed);
                }
                if self.opened.push(Opened::Active { id, kind }).is_err() {
                    return Err(DecodeError::TooLarge);
                }
                out.push(Output::Progress);
            }
            Event::TextDelta { index, content_index, text } => {
                self.delta(index, b"message", Some(content_index), text.len(), limits)?;
                out.push(Output::TextDelta { index, content_index, text });
            }
            Event::ArgumentsDelta { index, delta } => {
                self.delta(index, b"function_call", None, delta.len(), limits)?;
                out.push(Output::ArgumentsDelta { index, delta });
            }
            Event::ReasoningDelta { index, summary_index, text } => {
                self.delta(index, b"reasoning", Some(summary_index), text.len(), limits)?;
                out.push(Output::ReasoningDelta { index, summary_index, text });
            }
            Event::Done { index, item } => {
                let slot = self.opened.get_mut(index).ok_or(DecodeError::Malformed)?;
                let state = mem::replace(slot, Opened::Emitted);
                let prepared = match state {
                    Opened::Active { id, kind } => prepare(item, &id, &kind, limits)?,
                    Opened::Ready(_) | Opened::Emitted => return Err(DecodeError::Malformed),
                };
                self.reserve(1, part_size(&prepared.part), limits)?;
                self.tools = self.tools || prepared.tool;
                self.refusal = self.refusal || prepared.refusal;
                *self.opened.get_mut(index).expect("the slot exists") = Opened::Ready(prepared);
                self.ready(out);
            }
            Event::Completed { stop, usage } => {
                for index in 0..self.opened.len() {
                    match self.opened.get_mut(index).expect("within the slots") {
                        slot @ Opened::Active { .. } => match stop {
                            Stop::EndTurn | Stop::ToolUse => return Err(DecodeError::Malformed),
                            Stop::MaxTokens | Stop::Refusal => *slot = Opened::Emitted,
                        },
                        Opened::Ready(_) | Opened::Emitted => {}
                    }
                }
                self.terminal = Some(Terminal { stop, usage });
                self.ready(out);
            }
            Event::Failed { error } => {
                let failure = classify(0, Some(&error), RateLimit::NONE, wall);
                self.fail(failure, common::clipped(&error.message, limits.detail_bytes), out);
            }
        }
        Ok(())
    }
    fn delta(
        &mut self,
        index: u32,
        expected_kind: &[u8],
        sub_index: Option<u32>,
        size: usize,
        limits: &Limits,
    ) -> Result<(), DecodeError> {
        match self.opened.get(index) {
            Some(Opened::Active { kind, .. }) if kind.as_ref() == expected_kind => {}
            Some(Opened::Active { .. } | Opened::Ready(_) | Opened::Emitted) | None => {
                return Err(DecodeError::Malformed);
            }
        }
        if let Some(sub_index) = sub_index
            && sub_index >= limits.parts
        {
            return Err(DecodeError::TooLarge);
        }
        let bytes =
            self.delta_bytes.checked_add(u64::try_from(size).expect("usize fits u64")).ok_or(DecodeError::TooLarge)?;
        if bytes > u64::from(limits.answer_bytes) {
            return Err(DecodeError::TooLarge);
        }
        self.delta_bytes = bytes;
        Ok(())
    }
    fn reserve(&mut self, parts: u32, bytes: usize, limits: &Limits) -> Result<(), DecodeError> {
        let parts = self.parts.checked_add(parts).ok_or(DecodeError::TooLarge)?;
        let bytes =
            self.bytes.checked_add(u64::try_from(bytes).expect("usize fits u64")).ok_or(DecodeError::TooLarge)?;
        if parts > limits.parts || bytes > u64::from(limits.answer_bytes) {
            return Err(DecodeError::TooLarge);
        }
        self.parts = parts;
        self.bytes = bytes;
        Ok(())
    }
    fn fail(&mut self, failure: Failure, detail: Box<[u8]>, out: &mut Queue<Output>) {
        self.over = true;
        self.terminal = None;
        self.opened.clear();
        let detail = if detail.len() > usize::try_from(self.detail_bytes).expect("u32 fits usize") {
            common::clipped(&detail, self.detail_bytes)
        } else {
            detail
        };
        out.push(Output::Failed { failure, detail });
    }
}
fn part_size(part: &Part) -> usize {
    match part {
        Part::Text { id, phase, text, .. } => {
            let phase = match phase {
                Some(phase) => phase.len(),
                None => 0,
            };
            id.len().saturating_add(phase).saturating_add(text.len())
        }
        Part::Opaque { bytes } => bytes.len(),
        Part::ToolCall { id, name, input, .. } => id.len().saturating_add(name.len()).saturating_add(input.len()),
    }
}
fn prepare(item: Item, expected_id: &[u8], expected_kind: &[u8], limits: &Limits) -> Result<Prepared, DecodeError> {
    match item {
        Item::Message { id, phase, text, refusal } => {
            if expected_id != id.as_ref() || expected_kind != b"message" {
                return Err(DecodeError::Malformed);
            }
            Ok(Prepared { part: Part::Text { id, phase, text, refusal }, tool: false, refusal })
        }
        Item::FunctionCall { id, call_id, name, arguments } => {
            if expected_id != id.as_ref()
                || expected_kind != b"function_call"
                || bytes::find(&call_id, b"|").is_some()
                || bytes::find(&id, b"|").is_some()
            {
                return Err(DecodeError::Malformed);
            }
            let merged = joined_id(&call_id, &id)?;
            let too_large = arguments.len() > usize::try_from(limits.input_bytes).expect("u32 fits usize");
            let input = if too_large { bytes::copy_of(b"") } else { arguments };
            Ok(Prepared { part: Part::ToolCall { id: merged, name, input, too_large }, tool: true, refusal: false })
        }
        Item::Opaque { value } => {
            let tokens = value.as_tokens();
            if json::text_ref(json::value_at(tokens, json::required(tokens, b"id")?)?)? != expected_id
                || json::text_ref(json::value_at(tokens, json::required(tokens, b"type")?)?)? != expected_kind
            {
                return Err(DecodeError::Malformed);
            }
            let bytes = value.to_bytes(limits)?;
            if bytes.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
                return Err(DecodeError::TooLarge);
            }
            Ok(Prepared { part: Part::Opaque { bytes }, tool: false, refusal: false })
        }
    }
}

fn joined_id(call_id: &[u8], item_id: &[u8]) -> Result<Box<[u8]>, DecodeError> {
    let len = call_id
        .len()
        .checked_add(1)
        .ok_or(DecodeError::TooLarge)?
        .checked_add(item_id.len())
        .ok_or(DecodeError::TooLarge)?;
    let mut writer = Writer::new(len);
    writer.put(call_id).expect("measured");
    writer.put(b"|").expect("measured");
    writer.put(item_id).expect("measured");
    Ok(writer.finish())
}
pub fn decode_event(value: &Json, limits: &Limits) -> Result<Event, DecodeError> {
    let tokens = value.as_tokens();
    let kind = json::text_ref(json::value_at(tokens, json::required(tokens, b"type")?)?)?;
    match kind {
        b"response.created" => Ok(Event::Created { echo: None }),
        b"response.in_progress" => Ok(Event::InProgress { echo: None }),
        b"response.output_item.added" => {
            let item = json::value_at(tokens, json::required(tokens, b"item")?)?;
            Ok(Event::Added {
                index: index(tokens)?,
                id: json::text(json::value_at(item, json::required(item, b"id")?)?)?,
                kind: json::text(json::value_at(item, json::required(item, b"type")?)?)?,
            })
        }
        b"response.output_item.done" => Ok(Event::Done {
            index: index(tokens)?,
            item: read_item(json::value_at(tokens, json::required(tokens, b"item")?)?, limits)?,
        }),
        b"response.completed" | b"response.incomplete" | b"response.done" => {
            let response = json::value_at(tokens, json::required(tokens, b"response")?)?;
            let status = json::text_ref(json::value_at(response, json::required(response, b"status")?)?)?;
            if status == b"failed" {
                return Ok(Event::Failed {
                    error: read_error(json::value_at(response, json::required(response, b"error")?)?, limits)?,
                });
            }
            let stop = match status {
                b"completed" => {
                    if kind == b"response.incomplete" {
                        return Err(DecodeError::Malformed);
                    }
                    Stop::EndTurn
                }
                b"incomplete" => {
                    let details = json::value_at(response, json::required(response, b"incomplete_details")?)?;
                    match json::text_ref(json::value_at(details, json::required(details, b"reason")?)?)? {
                        b"max_output_tokens" => Stop::MaxTokens,
                        b"content_filter" => Stop::Refusal,
                        _ => return Err(DecodeError::WrongType),
                    }
                }
                _ => return Err(DecodeError::Malformed),
            };
            Ok(Event::Completed {
                stop,
                usage: read_usage(json::value_at(response, json::required(response, b"usage")?)?)?,
            })
        }
        b"response.failed" => {
            let response = json::value_at(tokens, json::required(tokens, b"response")?)?;
            Ok(Event::Failed {
                error: read_error(json::value_at(response, json::required(response, b"error")?)?, limits)?,
            })
        }
        b"error" => Ok(Event::Failed { error: decode_error(value, limits)? }),
        b"response.output_text.delta" => Ok(Event::TextDelta {
            index: index(tokens)?,
            content_index: named_index(tokens, b"content_index")?,
            text: json::text(json::value_at(tokens, json::required(tokens, b"delta")?)?)?,
        }),
        b"response.function_call_arguments.delta" => Ok(Event::ArgumentsDelta {
            index: index(tokens)?,
            delta: json::text(json::value_at(tokens, json::required(tokens, b"delta")?)?)?,
        }),
        b"response.reasoning_summary_text.delta" => Ok(Event::ReasoningDelta {
            index: index(tokens)?,
            summary_index: named_index(tokens, b"summary_index")?,
            text: json::text(json::value_at(tokens, json::required(tokens, b"delta")?)?)?,
        }),
        b"response.reasoning_summary_part.added"
        | b"response.function_call_arguments.done"
        | b"response.output_text.done"
        | b"response.content_part.added"
        | b"response.content_part.done"
        | b"response.reasoning_summary_text.done"
        | b"response.reasoning_summary_part.done" => Ok(Event::Progress),
        _ => Ok(Event::Unknown),
    }
}
fn index(tokens: &[Token]) -> Result<u32, DecodeError> {
    named_index(tokens, b"output_index")
}
fn named_index(tokens: &[Token], field: &[u8]) -> Result<u32, DecodeError> {
    match u32::try_from(json::unsigned(json::value_at(tokens, json::required(tokens, field)?)?)?) {
        Ok(n) => Ok(n),
        Err(_) => Err(DecodeError::TooLarge),
    }
}
fn read_item(tokens: &[Token], limits: &Limits) -> Result<Item, DecodeError> {
    match json::text_ref(json::value_at(tokens, json::required(tokens, b"type")?)?)? {
        b"function_call" => Ok(Item::FunctionCall {
            id: json::text(json::value_at(tokens, json::required(tokens, b"id")?)?)?,
            call_id: json::text(json::value_at(tokens, json::required(tokens, b"call_id")?)?)?,
            name: json::text(json::value_at(tokens, json::required(tokens, b"name")?)?)?,
            arguments: json::text(json::value_at(tokens, json::required(tokens, b"arguments")?)?)?,
        }),
        b"message" => {
            let values = json::value_at(tokens, json::required(tokens, b"content")?)?;
            let mut text = List::with_capacity(limits.answer_bytes);
            let mut refusal = false;
            let mut content_kind: Option<bool> = None;
            for &offset in &json::array(values, limits.parts)? {
                let part = json::value_at(values, offset)?;
                let kind = json::text_ref(json::value_at(part, json::required(part, b"type")?)?)?;
                let is_refusal = kind == b"refusal";
                if let Some(previous) = content_kind
                    && previous != is_refusal
                {
                    return Err(DecodeError::WrongType);
                }
                content_kind = Some(is_refusal);
                match kind {
                    b"output_text" => common::append(
                        &mut text,
                        json::text_ref(json::value_at(part, json::required(part, b"text")?)?)?,
                    )?,
                    b"refusal" => {
                        refusal = true;
                        common::append(
                            &mut text,
                            json::text_ref(json::value_at(part, json::required(part, b"refusal")?)?)?,
                        )?;
                    }
                    _ => return Err(DecodeError::WrongType),
                }
            }
            Ok(Item::Message {
                id: json::text(json::value_at(tokens, json::required(tokens, b"id")?)?)?,
                phase: request::optional_text(tokens, b"phase")?,
                text: text.into_boxed(),
                refusal,
            })
        }
        _ => Ok(Item::Opaque { value: Json::from_tokens(tokens, limits)? }),
    }
}
fn read_usage(tokens: &[Token]) -> Result<Usage, DecodeError> {
    let input = json::unsigned(json::value_at(tokens, json::required(tokens, b"input_tokens")?)?)?;
    let cached = match json::optional_at(tokens, json::field(tokens, b"input_tokens_details")?)? {
        Some(value) => match json::optional_at(value, json::field(value, b"cached_tokens")?)? {
            Some(value) => json::unsigned(value)?,
            None => 0,
        },
        None => 0,
    };
    Ok(Usage {
        input_tokens: input.checked_sub(cached).ok_or(DecodeError::Malformed)?,
        cache_read_tokens: cached,
        output_tokens: json::unsigned(json::value_at(tokens, json::required(tokens, b"output_tokens")?)?)?,
        cache_write_tokens: 0,
    })
}
pub fn decode_error(value: &Json, limits: &Limits) -> Result<ProviderError, DecodeError> {
    let tokens = value.as_tokens();
    let error = match json::optional_at(tokens, json::field(tokens, b"error")?)? {
        Some(value) => value,
        None => tokens,
    };
    read_error(error, limits)
}
fn read_error(tokens: &[Token], limits: &Limits) -> Result<ProviderError, DecodeError> {
    let kind = match json::optional_at(tokens, json::field(tokens, b"code")?)? {
        Some([Token::String(code)]) => code.clone(),
        Some([Token::Null]) | None => json::text(json::value_at(tokens, json::required(tokens, b"type")?)?)?,
        Some(_) => return Err(DecodeError::WrongType),
    };
    let message = common::clipped(
        json::text_ref(json::value_at(tokens, json::required(tokens, b"message")?)?)?,
        limits.detail_bytes,
    );
    let resets_in_seconds = match json::optional_at(tokens, json::field(tokens, b"resets_in_seconds")?)? {
        Some(value) => Some(json::unsigned(value)?),
        None => None,
    };
    let resets_at = match json::optional_at(tokens, json::field(tokens, b"resets_at")?)? {
        Some(value) => Some(json::unsigned(value)?),
        None => None,
    };
    Ok(ProviderError { kind, message, resets_in_seconds, resets_at })
}
pub fn encode_error(error: &ProviderError, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    encode_event(&Event::Failed { error: error.clone() }, limits)
}
pub fn encode_event(event: &Event, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    let bounded = limits.writer_limits();
    let mut measure = Encoder::measure(&bounded);
    write_event(&mut measure, event, None);
    let len = common::measured(measure)?;
    let mut write = Encoder::write(len, &bounded);
    write_event(&mut write, event, None);
    Ok(write.finish())
}
fn write_error(out: &mut Encoder, error: &ProviderError) {
    out.object_start();
    out.key(b"code");
    out.string(&error.kind);
    out.key(b"message");
    out.string(&error.message);
    if let Some(reset) = error.resets_in_seconds {
        out.key(b"resets_in_seconds");
        out.unsigned(reset);
    }
    if let Some(reset) = error.resets_at {
        out.key(b"resets_at");
        out.unsigned(reset);
    }
    out.object_end();
}
fn write_event(out: &mut Encoder, event: &Event, completion_echo: Option<&Request>) {
    out.object_start();
    out.key(b"type");
    match event {
        Event::Created { echo } | Event::InProgress { echo } => {
            out.string(match event {
                Event::Created { .. } => b"response.created",
                Event::InProgress { .. } => b"response.in_progress",
                Event::Added { .. }
                | Event::Done { .. }
                | Event::TextDelta { .. }
                | Event::ArgumentsDelta { .. }
                | Event::ReasoningDelta { .. }
                | Event::Completed { .. }
                | Event::Failed { .. }
                | Event::Progress
                | Event::Unknown => unreachable!("only echo events reach this arm"),
            });
            out.key(b"response");
            out.object_start();
            out.key(b"status");
            out.string(b"in_progress");
            if let Some(request) = echo {
                out.key(b"instructions");
                out.string(&request.instructions);
                out.key(b"tools");
                request::write_tools(out, &request.tools);
            }
            out.object_end();
        }
        Event::Added { index, id, kind } => {
            out.string(b"response.output_item.added");
            out.key(b"output_index");
            out.unsigned(u64::from(*index));
            out.key(b"item");
            out.object_start();
            out.key(b"id");
            out.string(id);
            out.key(b"type");
            out.string(kind);
            out.object_end();
        }
        Event::TextDelta { index, content_index, text } => {
            out.string(b"response.output_text.delta");
            out.key(b"output_index");
            out.unsigned(u64::from(*index));
            out.key(b"content_index");
            out.unsigned(u64::from(*content_index));
            out.key(b"delta");
            out.string(text);
        }
        Event::ArgumentsDelta { index, delta } => {
            out.string(b"response.function_call_arguments.delta");
            out.key(b"output_index");
            out.unsigned(u64::from(*index));
            out.key(b"delta");
            out.string(delta);
        }
        Event::ReasoningDelta { index, summary_index, text } => {
            out.string(b"response.reasoning_summary_text.delta");
            out.key(b"output_index");
            out.unsigned(u64::from(*index));
            out.key(b"summary_index");
            out.unsigned(u64::from(*summary_index));
            out.key(b"delta");
            out.string(text);
        }
        Event::Done { index, item } => {
            out.string(b"response.output_item.done");
            out.key(b"output_index");
            out.unsigned(u64::from(*index));
            out.key(b"item");
            write_item(out, item);
        }
        Event::Completed { stop, usage } => write_terminal(out, *stop, *usage, completion_echo),
        Event::Failed { error } => {
            out.string(b"error");
            out.key(b"error");
            write_error(out, error);
        }
        Event::Progress => out.string(b"response.content_part.added"),
        Event::Unknown => out.string(b"future_event"),
    }
    out.object_end();
}
fn write_terminal(out: &mut Encoder, stop: Stop, usage: Usage, completion_echo: Option<&Request>) {
    out.string(match stop {
        Stop::MaxTokens | Stop::Refusal => b"response.incomplete",
        Stop::EndTurn | Stop::ToolUse => b"response.completed",
    });
    out.key(b"response");
    out.object_start();
    out.key(b"status");
    out.string(match stop {
        Stop::MaxTokens | Stop::Refusal => b"incomplete",
        Stop::EndTurn | Stop::ToolUse => b"completed",
    });
    match stop {
        Stop::MaxTokens | Stop::Refusal => {
            out.key(b"incomplete_details");
            out.object_start();
            out.key(b"reason");
            out.string(match stop {
                Stop::MaxTokens => b"max_output_tokens",
                Stop::Refusal => b"content_filter",
                Stop::EndTurn | Stop::ToolUse => unreachable!("cut stop"),
            });
            out.object_end();
        }
        Stop::EndTurn | Stop::ToolUse => {}
    }
    if let Some(request) = completion_echo {
        out.key(b"instructions");
        out.string(&request.instructions);
        out.key(b"tools");
        request::write_tools(out, &request.tools);
    }
    out.key(b"usage");
    out.object_start();
    out.key(b"input_tokens");
    out.unsigned(usage.input_tokens.saturating_add(usage.cache_read_tokens));
    out.key(b"output_tokens");
    out.unsigned(usage.output_tokens);
    out.key(b"input_tokens_details");
    out.object_start();
    out.key(b"cached_tokens");
    out.unsigned(usage.cache_read_tokens);
    out.object_end();
    out.object_end();
    out.object_end();
}
fn write_item(out: &mut Encoder, item: &Item) {
    match item {
        Item::Opaque { value } => value.write(out),
        Item::FunctionCall { id, call_id, name, arguments } => {
            out.object_start();
            out.key(b"type");
            out.string(b"function_call");
            out.key(b"id");
            out.string(id);
            out.key(b"call_id");
            out.string(call_id);
            out.key(b"name");
            out.string(name);
            out.key(b"arguments");
            out.string(arguments);
            out.object_end();
        }
        Item::Message { id, phase, text, refusal } => {
            out.object_start();
            out.key(b"type");
            out.string(b"message");
            out.key(b"id");
            out.string(id);
            if let Some(phase) = phase {
                out.key(b"phase");
                out.string(phase);
            }
            out.key(b"content");
            out.array_start();
            out.object_start();
            out.key(b"type");
            out.string(if *refusal { b"refusal" } else { b"output_text" });
            out.key(if *refusal { b"refusal" } else { b"text" });
            out.string(text);
            out.object_end();
            out.array_end();
            out.object_end();
        }
    }
}

pub(crate) fn decoder_worst_case(limits: &Limits) -> Option<u64> {
    let slots = List::<Opened>::worst_case(limits.parts)?;
    let identifiers = u64::from(limits.parts)
        .checked_mul(u64::from(limits.string_bytes.min(limits.document_bytes)))?
        .checked_mul(2)?;
    slots.checked_add(identifiers)?.checked_add(u64::from(limits.answer_bytes))
}

/// Fake-server completion with the instructions/tools echo that the real Codex
/// route sends for the third time. The client decoder deliberately ignores it.
pub fn encode_completion(
    stop: Stop,
    usage: Usage,
    request: &Request,
    limits: &Limits,
) -> Result<Box<[u8]>, DecodeError> {
    let event = Event::Completed { stop, usage };
    let bounded = limits.writer_limits();
    let mut measure = Encoder::measure(&bounded);
    write_event(&mut measure, &event, Some(request));
    let len = common::measured(measure)?;
    let mut write = Encoder::write(len, &bounded);
    write_event(&mut write, &event, Some(request));
    Ok(write.finish())
}
