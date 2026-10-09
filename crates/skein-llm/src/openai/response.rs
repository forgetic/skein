use crate::openai::{
    DecodeError, Failure, Json, Limits, ProviderError, RateLimit, Request, Stop, Usage, classify, common, json, request,
};
use alloc::boxed::Box;
use core::mem;
use skein_json::{Document, Kind, writer::Encoder};
use skein_lib::{List, Queue, Wall, bytes};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Item {
    Message {
        id: Box<[u8]>,
        phase: Option<Box<[u8]>>,
        text: Box<[u8]>,
        refusal: bool,
    },
    FunctionCall {
        id: Box<[u8]>,
        call_id: Box<[u8]>,
        name: Box<[u8]>,
        arguments: Box<[u8]>,
    },
    /// The provider closed this unfinished function-call item at its output cap.
    CutCall {
        id: Box<[u8]>,
        call_id: Box<[u8]>,
        name: Box<[u8]>,
        arguments: Box<[u8]>,
    },
    Opaque {
        value: Json,
    },
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    Created {
        echo: Option<Request>,
    },
    InProgress {
        echo: Option<Request>,
    },
    Added {
        index: u32,
        id: Box<[u8]>,
        kind: Box<[u8]>,
    },
    /// A native function-call head retaining the identity for a possible output cut.
    ToolAdded {
        index: u32,
        id: Box<[u8]>,
        call_id: Box<[u8]>,
        name: Box<[u8]>,
        arguments: Box<[u8]>,
    },
    Done {
        index: u32,
        item: Item,
    },
    TextDelta {
        index: u32,
        content_index: u32,
        text: Box<[u8]>,
    },
    ArgumentsDelta {
        index: u32,
        delta: Box<[u8]>,
    },
    ReasoningDelta {
        index: u32,
        summary_index: u32,
        text: Box<[u8]>,
    },
    Completed {
        stop: Stop,
        usage: Usage,
    },
    Failed {
        error: ProviderError,
    },
    Progress,
    Unknown,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Part {
    /// Reasoning beyond the opaque cap, discarded only by owner opt-in.
    Dropped {
        bytes: u64,
    },
    Text {
        id: Box<[u8]>,
        phase: Option<Box<[u8]>>,
        text: Box<[u8]>,
        refusal: bool,
    },
    Opaque {
        bytes: Box<[u8]>,
    },
    /// The native decoder delivers this complete function call in `Output::Part`.
    /// The Client translates it before its eventual call terminal. IDs are
    /// admitted JSON strings, never packed into a delimiter-separated string.
    /// See `docs/design/llm.md`, Vocabulary and ownership.
    ToolCall {
        /// Exact identity paired with the application's result, under `Limits::string_bytes`.
        call_id: Box<[u8]>,
        /// Exact provider item identity retained for replay, under `Limits::string_bytes`.
        item_id: Box<[u8]>,
        /// Provider-written name under `Limits::string_bytes`; the caller checks its declaration.
        name: Box<[u8]>,
        /// Complete raw argument text under `Limits::input_bytes`, or empty when `too_large` is true.
        input: Box<[u8]>,
        /// Whether the raw argument text exceeded the receiving input cap.
        too_large: bool,
        /// Total unescaped argument bytes received, including refused bytes.
        bytes: u64,
        /// The provider ended this unfinished call at its output cap.
        cut: bool,
    },
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
    required_stop: Option<Stop>,
    reasoning: ReasoningPolicy,
    over: bool,
    terminal: Option<Terminal>,
}
#[derive(Clone, Copy, Debug)]
enum ReasoningPolicy {
    Keep,
    Drop,
}
#[derive(Debug)]
enum Opened {
    Active { id: Box<[u8]>, kind: Box<[u8]> },
    Tool { id: Box<[u8]>, call_id: Box<[u8]>, name: Box<[u8]>, input: List<u8>, bytes: u64, too_large: bool },
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
        Self::with_reasoning_drop(limits, false)
    }
    /// Select whether Codex reasoning past the opaque cap may be discarded.
    #[must_use]
    pub fn with_reasoning_drop(limits: &Limits, enabled: bool) -> StreamDecoder {
        StreamDecoder {
            reasoning: if enabled { ReasoningPolicy::Drop } else { ReasoningPolicy::Keep },
            opened: List::with_capacity(limits.parts),
            next: 0,
            parts: 0,
            bytes: 0,
            delta_bytes: 0,
            detail_bytes: limits.detail_bytes,
            tools: false,
            refusal: false,
            required_stop: None,
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
            Err(DecodeError::TooLarge { which, bound }) => {
                self.fail(
                    Failure::Limit { which, bound },
                    bytes::copy_of(b"ChatGPT stream exceeds configured limits"),
                    out,
                );
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
            Some(Opened::Active { .. } | Opened::Tool { .. }) => false,
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
                Opened::Active { .. } | Opened::Tool { .. } => break,
                Opened::Emitted => self.next = self.next.saturating_add(1),
                Opened::Ready(_) => {
                    let state = mem::replace(slot, Opened::Emitted);
                    match state {
                        Opened::Ready(prepared) => {
                            out.push(Output::Part(prepared.part));
                        }
                        Opened::Active { .. } | Opened::Tool { .. } | Opened::Emitted => {
                            unreachable!("the slot was ready")
                        }
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
                    return Err(DecodeError::limit(crate::Cap::String, limits.string_bytes));
                }
                if index != self.opened.len() {
                    return Err(DecodeError::Malformed);
                }
                if self.opened.push(Opened::Active { id, kind }).is_err() {
                    return Err(DecodeError::limit(crate::Cap::Parts, limits.parts));
                }
                out.push(Output::Progress);
            }
            Event::ToolAdded { index, id, call_id, name, arguments } => {
                if index != self.opened.len() {
                    return Err(DecodeError::Malformed);
                }
                for text in [&id, &call_id, &name] {
                    if text.len() > usize::try_from(limits.string_bytes).expect("u32 fits usize") {
                        return Err(DecodeError::limit(crate::Cap::String, limits.string_bytes));
                    }
                }
                let bytes = u64::try_from(arguments.len()).expect("slice length fits u64");
                let mut input = List::with_capacity(limits.input_bytes);
                let too_large = bytes > u64::from(limits.input_bytes);
                if !too_large {
                    for &byte in &arguments {
                        input.push(byte).expect("admitted initial arguments");
                    }
                }
                self.opened
                    .push(Opened::Tool { id, call_id, name, input, bytes, too_large })
                    .or(Err(DecodeError::limit(crate::Cap::Parts, limits.parts)))?;
                out.push(Output::Progress);
            }
            Event::TextDelta { index, content_index, text } => {
                self.delta(index, b"message", Some(content_index), text.len(), limits)?;
                out.push(Output::TextDelta { index, content_index, text });
            }
            Event::ArgumentsDelta { index, delta } => {
                self.delta(index, b"function_call", None, delta.len(), limits)?;
                match self.opened.get_mut(index) {
                    Some(Opened::Tool { input, bytes, too_large, .. }) => {
                        *bytes = bytes
                            .checked_add(u64::try_from(delta.len()).expect("slice length fits u64"))
                            .ok_or(DecodeError::Malformed)?;
                        if !*too_large {
                            if delta.len() > usize::try_from(input.room()).expect("u32 fits usize") {
                                *too_large = true;
                                input.clear();
                            } else {
                                for &byte in &delta {
                                    input.push(byte).expect("admitted argument fragment");
                                }
                            }
                        }
                    }
                    Some(Opened::Active { .. } | Opened::Ready(_) | Opened::Emitted) | None => {}
                }
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
                    Opened::Active { id, kind } => prepare(item, &id, &kind, limits, self.reasoning)?,
                    Opened::Tool { id, .. } => prepare(item, &id, b"function_call", limits, self.reasoning)?,
                    Opened::Ready(_) | Opened::Emitted => return Err(DecodeError::Malformed),
                };
                self.reserve(1, part_size(&prepared.part), limits)?;
                match &prepared.part {
                    Part::ToolCall { cut: true, .. } => self.required_stop = Some(Stop::MaxTokens),
                    Part::ToolCall { cut: false, .. }
                    | Part::Text { .. }
                    | Part::Opaque { .. }
                    | Part::Dropped { .. } => {}
                }
                self.tools = self.tools || prepared.tool;
                self.refusal = self.refusal || prepared.refusal;
                *self.opened.get_mut(index).expect("the slot exists") = Opened::Ready(prepared);
                self.ready(out);
            }
            Event::Completed { stop, usage } => self.complete(stop, usage, limits, out)?,
            Event::Failed { error } => {
                let failure = classify(0, Some(&error), RateLimit::NONE, wall);
                self.fail(failure, common::clipped(&error.message, limits.detail_bytes), out);
            }
        }
        Ok(())
    }
    fn complete(
        &mut self,
        stop: Stop,
        usage: Usage,
        limits: &Limits,
        out: &mut Queue<Output>,
    ) -> Result<(), DecodeError> {
        if let Some(expected) = self.required_stop
            && stop != expected
        {
            return Err(DecodeError::Malformed);
        }
        for index in 0..self.opened.len() {
            let slot = self.opened.get_mut(index).expect("within the slots");
            let state = mem::replace(slot, Opened::Emitted);
            let state = match state {
                Opened::Active { .. } => match stop {
                    Stop::EndTurn | Stop::ToolUse => return Err(DecodeError::Malformed),
                    Stop::MaxTokens | Stop::Refusal => Opened::Emitted,
                },
                Opened::Tool { id, call_id, name, input, bytes, too_large } => match stop {
                    Stop::EndTurn | Stop::ToolUse => return Err(DecodeError::Malformed),
                    Stop::Refusal => Opened::Emitted,
                    Stop::MaxTokens => {
                        let part = Part::ToolCall {
                            call_id,
                            item_id: id,
                            name,
                            input: input.into_boxed(),
                            too_large,
                            bytes,
                            cut: true,
                        };
                        self.reserve(1, part_size(&part), limits)?;
                        Opened::Ready(Prepared { part, tool: false, refusal: false })
                    }
                },
                state @ (Opened::Ready(_) | Opened::Emitted) => state,
            };
            *self.opened.get_mut(index).expect("within the slots") = state;
        }
        self.terminal = Some(Terminal { stop, usage });
        self.ready(out);
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
            Some(Opened::Tool { .. }) if expected_kind == b"function_call" => {}
            Some(Opened::Active { .. } | Opened::Tool { .. } | Opened::Ready(_) | Opened::Emitted) | None => {
                return Err(DecodeError::Malformed);
            }
        }
        if let Some(sub_index) = sub_index
            && sub_index >= limits.parts
        {
            return Err(DecodeError::limit(crate::Cap::Parts, limits.parts));
        }
        if expected_kind == b"function_call" {
            return Ok(());
        }
        let bytes = self
            .delta_bytes
            .checked_add(u64::try_from(size).expect("usize fits u64"))
            .ok_or(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes))?;
        if bytes > u64::from(limits.answer_bytes) {
            return Err(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes));
        }
        self.delta_bytes = bytes;
        Ok(())
    }
    fn reserve(&mut self, parts: u32, bytes: usize, limits: &Limits) -> Result<(), DecodeError> {
        let parts = self.parts.checked_add(parts).ok_or(DecodeError::limit(crate::Cap::Parts, limits.parts))?;
        let bytes = self
            .bytes
            .checked_add(u64::try_from(bytes).expect("usize fits u64"))
            .ok_or(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes))?;
        if parts > limits.parts {
            return Err(DecodeError::limit(crate::Cap::Parts, limits.parts));
        }
        if bytes > u64::from(limits.answer_bytes) {
            return Err(DecodeError::limit(crate::Cap::Answer, limits.answer_bytes));
        }
        self.parts = parts;
        self.bytes = bytes;
        Ok(())
    }
    fn fail(&mut self, failure: Failure, detail: Box<[u8]>, out: &mut Queue<Output>) {
        self.over = true;
        self.terminal = None;
        self.opened.clear();
        let detail = match failure {
            Failure::Limit { which, bound } => common::limit_detail(which, bound),
            Failure::Protocol
            | Failure::Unauthorized
            | Failure::Exhausted { .. }
            | Failure::RateLimited { .. }
            | Failure::Overloaded
            | Failure::Unavailable
            | Failure::ContextTooLong
            | Failure::Invalid => detail,
        };
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
        Part::Dropped { .. } => 0,
        Part::ToolCall { call_id, item_id, name, input, too_large, cut, .. } => {
            if *too_large {
                return call_id.len().saturating_add(name.len());
            }
            let item = if *cut { 0 } else { item_id.len() };
            call_id.len().saturating_add(item).saturating_add(name.len()).saturating_add(input.len())
        }
    }
}
fn prepare(
    item: Item,
    expected_id: &[u8],
    expected_kind: &[u8],
    limits: &Limits,
    reasoning: ReasoningPolicy,
) -> Result<Prepared, DecodeError> {
    match item {
        Item::Message { id, phase, text, refusal } => {
            if expected_id != id.as_ref() || expected_kind != b"message" {
                return Err(DecodeError::Malformed);
            }
            Ok(Prepared { part: Part::Text { id, phase, text, refusal }, tool: false, refusal })
        }
        item @ (Item::FunctionCall { .. } | Item::CutCall { .. }) => {
            let (id, call_id, name, arguments, cut) = match item {
                Item::FunctionCall { id, call_id, name, arguments } => (id, call_id, name, arguments, false),
                Item::CutCall { id, call_id, name, arguments } => (id, call_id, name, arguments, true),
                Item::Message { .. } | Item::Opaque { .. } => unreachable!("a function-call item"),
            };
            let cap = usize::try_from(limits.string_bytes).expect("u32 fits usize");
            if id.len() > cap || call_id.len() > cap || name.len() > cap {
                return Err(DecodeError::limit(crate::Cap::String, limits.string_bytes));
            }
            if expected_id != id.as_ref() || expected_kind != b"function_call" {
                return Err(DecodeError::Malformed);
            }
            let bytes = u64::try_from(arguments.len()).expect("slice length fits u64");
            let too_large = arguments.len() > usize::try_from(limits.input_bytes).expect("u32 fits usize");
            let input = if too_large { bytes::copy_of(b"") } else { arguments };
            Ok(Prepared {
                part: Part::ToolCall { call_id, item_id: id, name, input, too_large, bytes, cut },
                tool: !cut,
                refusal: false,
            })
        }
        Item::Opaque { value } => {
            let tokens = value.view();
            if json::text_ref(json::value_at(tokens, json::required(tokens, b"id")?)?)? != expected_id
                || json::text_ref(json::value_at(tokens, json::required(tokens, b"type")?)?)? != expected_kind
            {
                return Err(DecodeError::Malformed);
            }
            let bytes = value.to_bytes(limits)?;
            if bytes.len() > usize::try_from(limits.opaque_bytes).expect("u32 fits usize") {
                match reasoning {
                    ReasoningPolicy::Drop if expected_kind == b"reasoning" => {
                        return Ok(Prepared {
                            part: Part::Dropped { bytes: u64::try_from(bytes.len()).expect("slice length fits u64") },
                            tool: false,
                            refusal: false,
                        });
                    }
                    ReasoningPolicy::Keep | ReasoningPolicy::Drop => {
                        return Err(DecodeError::limit(crate::Cap::Opaque, limits.opaque_bytes));
                    }
                }
            }
            Ok(Prepared { part: Part::Opaque { bytes }, tool: false, refusal: false })
        }
    }
}

pub fn decode_event(value: &Json, limits: &Limits) -> Result<Event, DecodeError> {
    let tokens = value.view();
    let kind = json::text_ref(json::value_at(tokens, json::required(tokens, b"type")?)?)?;
    match kind {
        b"response.created" => Ok(Event::Created { echo: None }),
        b"response.in_progress" => Ok(Event::InProgress { echo: None }),
        b"response.output_item.added" => {
            let item = json::value_at(tokens, json::required(tokens, b"item")?)?;
            if json::text_ref(json::value_at(item, json::required(item, b"type")?)?)? == b"function_call"
                && json::field(item, b"call_id")?.is_some()
            {
                return Ok(Event::ToolAdded {
                    index: index(tokens)?,
                    id: json::text(json::value_at(item, json::required(item, b"id")?)?)?,
                    call_id: json::text(json::value_at(item, json::required(item, b"call_id")?)?)?,
                    name: json::text(json::value_at(item, json::required(item, b"name")?)?)?,
                    arguments: match json::optional_at(item, json::field(item, b"arguments")?)? {
                        Some(value) => json::text(value)?,
                        None => Box::new([]),
                    },
                });
            }
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
            Ok(Event::Completed { stop, usage: read_usage(json::reported_field(response, b"usage")) })
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
fn index(tokens: (&Document, json::Span)) -> Result<u32, DecodeError> {
    named_index(tokens, b"output_index")
}
fn named_index(tokens: (&Document, json::Span), field: &[u8]) -> Result<u32, DecodeError> {
    match u32::try_from(json::unsigned(json::value_at(tokens, json::required(tokens, field)?)?)?) {
        Ok(n) => Ok(n),
        Err(_) => Err(DecodeError::Malformed),
    }
}
fn read_item(tokens: (&Document, json::Span), limits: &Limits) -> Result<Item, DecodeError> {
    match json::text_ref(json::value_at(tokens, json::required(tokens, b"type")?)?)? {
        b"function_call" => {
            let id = json::text(json::value_at(tokens, json::required(tokens, b"id")?)?)?;
            let call_id = json::text(json::value_at(tokens, json::required(tokens, b"call_id")?)?)?;
            let name = json::text(json::value_at(tokens, json::required(tokens, b"name")?)?)?;
            let arguments = json::text(json::value_at(tokens, json::required(tokens, b"arguments")?)?)?;
            let status = request::optional_text(tokens, b"status")?;
            match status.as_deref() {
                Some(b"incomplete") => Ok(Item::CutCall { id, call_id, name, arguments }),
                Some(b"completed") | None => Ok(Item::FunctionCall { id, call_id, name, arguments }),
                Some(_) => Err(DecodeError::Malformed),
            }
        }
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
                        crate::Cap::Answer,
                    )?,
                    b"refusal" => {
                        refusal = true;
                        common::append(
                            &mut text,
                            json::text_ref(json::value_at(part, json::required(part, b"refusal")?)?)?,
                            crate::Cap::Answer,
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
        _ => Ok(Item::Opaque { value: Json::from_view(tokens, limits)? }),
    }
}
fn read_usage(tokens: Option<(&Document, json::Span)>) -> Usage {
    let Some(tokens) = tokens else {
        return Usage::NONE;
    };
    let details = json::reported_field(tokens, b"input_tokens_details");
    let cache_read = match details {
        Some(details) => json::reported_unsigned(details, b"cached_tokens"),
        None => None,
    };
    let cache_write = match details {
        Some(details) => json::reported_unsigned(details, b"cache_write_tokens"),
        None => None,
    };
    let reasoning = match json::reported_field(tokens, b"output_tokens_details") {
        Some(details) => json::reported_unsigned(details, b"reasoning_tokens"),
        None => None,
    };
    let mut input = json::reported_unsigned(tokens, b"input_tokens");
    for part in [cache_read, cache_write].into_iter().flatten() {
        input = match input {
            Some(input) => input.checked_sub(part),
            None => None,
        };
    }
    Usage { input, cache_read, cache_write, output: json::reported_unsigned(tokens, b"output_tokens"), reasoning }
}
pub fn decode_error(value: &Json, limits: &Limits) -> Result<ProviderError, DecodeError> {
    let tokens = value.view();
    let error = match json::optional_at(tokens, json::field(tokens, b"error")?)? {
        Some(value) => value,
        None => tokens,
    };
    read_error(error, limits)
}
fn read_error(tokens: (&Document, json::Span), limits: &Limits) -> Result<ProviderError, DecodeError> {
    let kind = match json::optional_at(tokens, json::field(tokens, b"code")?)? {
        Some(value) => {
            if json::kind(value, 0) == Some(Kind::Null) && json::len(value) == 1 {
                json::text(json::value_at(tokens, json::required(tokens, b"type")?)?)?
            } else {
                json::text(value)?
            }
        }
        None => json::text(json::value_at(tokens, json::required(tokens, b"type")?)?)?,
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
/// Fake-server metadata selection, bounded by the configured event document.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Echo {
    pub instructions: bool,
    pub tools: bool,
    /// Payload bytes in each input attribution entry; zero omits attribution.
    pub attribution_bytes: u32,
}
impl Echo {
    pub const NONE: Echo = Echo { instructions: false, tools: false, attribution_bytes: 0 };
    const REQUEST: Echo = Echo { instructions: true, tools: true, attribution_bytes: 0 };
    #[must_use]
    pub const fn enabled(self) -> bool {
        self.instructions || self.tools || self.attribution_bytes > 0
    }
}
pub fn encode_event(event: &Event, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    encode_peer_event(event, None, Echo::REQUEST, limits)
}
/// Encode a fake event with selected request echoes and sized usage attribution.
pub fn encode_peer_event(
    event: &Event,
    request: Option<&Request>,
    echo: Echo,
    limits: &Limits,
) -> Result<Box<[u8]>, DecodeError> {
    if echo.attribution_bytes > limits.document_bytes {
        return Err(DecodeError::limit(crate::Cap::Document, limits.document_bytes));
    }
    let bounded = limits.writer_limits();
    let mut measure = Encoder::measure(&bounded);
    write_event(&mut measure, event, request, echo);
    let len = common::measured(measure, bounded, crate::Cap::Document)?;
    let mut write = Encoder::write(len, &bounded);
    write_event(&mut write, event, request, echo);
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
fn write_event(out: &mut Encoder, event: &Event, completion_echo: Option<&Request>, selection: Echo) {
    out.object_start();
    out.key(b"type");
    match event {
        Event::Created { echo } | Event::InProgress { echo } => {
            out.string(match event {
                Event::Created { .. } => b"response.created",
                Event::InProgress { .. } => b"response.in_progress",
                Event::Added { .. }
                | Event::ToolAdded { .. }
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
            if let Some(request) = echo.as_ref().or(completion_echo) {
                write_echo(out, request, selection);
            }
            out.object_end();
        }
        Event::ToolAdded { index, id, call_id, name, arguments } => {
            out.string(b"response.output_item.added");
            out.key(b"output_index");
            out.unsigned(u64::from(*index));
            out.key(b"item");
            write_item(
                out,
                &Item::FunctionCall {
                    id: id.clone(),
                    call_id: call_id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                },
            );
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
        Event::Completed { stop, usage } => write_terminal(out, *stop, *usage, completion_echo, selection),
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
fn write_terminal(out: &mut Encoder, stop: Stop, usage: Usage, completion_echo: Option<&Request>, selection: Echo) {
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
        write_echo(out, request, selection);
    }
    out.key(b"usage");
    out.object_start();
    if let Some(request) = completion_echo {
        write_attribution(out, request.input.len(), selection.attribution_bytes);
    }
    write_usage(out, usage);
    out.object_end();
    out.object_end();
}
fn write_item(out: &mut Encoder, item: &Item) {
    match item {
        Item::Opaque { value } => value.write(out),
        Item::FunctionCall { id, call_id, name, arguments } | Item::CutCall { id, call_id, name, arguments } => {
            let cut = match item {
                Item::CutCall { .. } => true,
                Item::FunctionCall { .. } => false,
                Item::Message { .. } | Item::Opaque { .. } => unreachable!("a function-call item"),
            };
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
            if cut {
                out.key(b"status");
                out.string(b"incomplete");
            }
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
        .checked_mul(3)?;
    let inputs = u64::from(limits.parts).checked_mul(u64::from(limits.input_bytes))?;
    slots.checked_add(identifiers)?.checked_add(inputs)?.checked_add(u64::from(limits.answer_bytes))
}

/// Fake-server completion with the instructions/tools echo that the real Codex
/// route sends for the third time. The client decoder deliberately ignores it.
pub fn encode_completion(
    stop: Stop,
    usage: Usage,
    request: &Request,
    limits: &Limits,
) -> Result<Box<[u8]>, DecodeError> {
    encode_peer_event(&Event::Completed { stop, usage }, Some(request), Echo::REQUEST, limits)
}

fn write_echo(out: &mut Encoder, request: &Request, selection: Echo) {
    if selection.instructions {
        out.key(b"instructions");
        out.string(&request.instructions);
    }
    if selection.tools {
        out.key(b"tools");
        request::write_tools(out, &request.tools);
    }
}
fn write_attribution(out: &mut Encoder, items: usize, entry_bytes: u32) {
    if entry_bytes == 0 {
        return;
    }
    let mut payload = List::with_capacity(entry_bytes);
    for _byte in 0..entry_bytes {
        payload.push(b'x').expect("configured bounded entry");
    }
    out.key(b"attribution");
    out.object_start();
    out.key(b"items");
    out.array_start();
    for index in 0..items {
        out.object_start();
        out.key(b"input_index");
        out.unsigned(u64::try_from(index).expect("slice index fits u64"));
        out.key(b"payload");
        out.string(payload.as_slice());
        for field in [b"input_tokens".as_slice(), b"cached_tokens".as_slice(), b"cache_write_tokens".as_slice()] {
            out.key(field);
            out.unsigned(0);
        }
        out.key(b"model");
        out.string(b"fake-codex");
        out.key(b"kind");
        out.string(b"input");
        out.key(b"reasoning");
        out.boolean(false);
        out.object_end();
    }
    out.array_end();
    out.object_end();
}

fn write_usage(out: &mut Encoder, usage: Usage) {
    let mut input = usage.input;
    for part in [usage.cache_read, usage.cache_write].into_iter().flatten() {
        input = match input {
            Some(input) => input.checked_add(part),
            None => None,
        };
    }
    for (name, value) in [(b"input_tokens".as_slice(), input), (b"output_tokens".as_slice(), usage.output)] {
        if let Some(value) = value {
            out.key(name);
            out.unsigned(value);
        }
    }
    if usage.cache_read.is_some() || usage.cache_write.is_some() {
        out.key(b"input_tokens_details");
        out.object_start();
        for (name, value) in
            [(b"cached_tokens".as_slice(), usage.cache_read), (b"cache_write_tokens".as_slice(), usage.cache_write)]
        {
            if let Some(value) = value {
                out.key(name);
                out.unsigned(value);
            }
        }
        out.object_end();
    }
    if let Some(reasoning) = usage.reasoning {
        out.key(b"output_tokens_details");
        out.object_start();
        out.key(b"reasoning_tokens");
        out.unsigned(reasoning);
        out.object_end();
    }
}
