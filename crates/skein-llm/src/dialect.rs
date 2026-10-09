//! Internal dispatch from bounded provider codecs to the shared vocabulary.
use crate::{Block, Error, Failure, Provider, Replay, Stop, Usage, anthropic, client, filter, openai, translate};
use alloc::boxed::Box;
use skein_json::{Document, Kind, collector, tokenizer};
use skein_lib::{Queue, Wall};

pub(crate) const MAX_OUT: u32 = 3;
#[derive(Debug)]
pub(crate) enum Decoder {
    Codex(openai::StreamDecoder),
    Anthropic(anthropic::StreamDecoder),
}
#[derive(Debug)]
pub(crate) enum Output {
    Part(Result<Block, Error>),
    TextDelta { index: u32, content_index: u32, text: Box<[u8]> },
    ArgumentsDelta { index: u32, delta: Box<[u8]> },
    ReasoningDelta { index: u32, summary_index: u32, text: Box<[u8]> },
    Completed { stop: Stop, usage: Usage },
    Failed { failure: Failure, detail: Box<[u8]> },
    Progress,
}
impl Decoder {
    pub(crate) fn new(provider: Provider, limits: &openai::Limits, drop_reasoning: bool) -> Decoder {
        match provider {
            Provider::OpenAiCodex => Decoder::Codex(openai::StreamDecoder::with_reasoning_drop(limits, drop_reasoning)),
            Provider::Anthropic => Decoder::Anthropic(anthropic::StreamDecoder::new(limits)),
        }
    }
    pub(crate) fn has_ready(&self) -> bool {
        match self {
            Decoder::Codex(decoder) => decoder.has_ready(),
            Decoder::Anthropic(decoder) => decoder.has_ready(),
        }
    }
    pub(crate) fn ready(&mut self, limits: &openai::Limits, out: &mut Queue<Output>) {
        match self {
            Decoder::Codex(decoder) => {
                let mut raw = Queue::with_capacity(openai::MAX_OUT);
                decoder.ready(&mut raw);
                codex_outputs(&mut raw, limits, out);
            }
            Decoder::Anthropic(decoder) => {
                let mut raw = Queue::with_capacity(anthropic::MAX_OUT);
                decoder.ready(&mut raw);
                anthropic_outputs(&mut raw, limits, out);
            }
        }
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "event bytes and dispatch metadata accompany the existing decoding context"
    )]
    pub(crate) fn event(
        &mut self,
        name: &[u8],
        document: Document,
        limits: &openai::Limits,
        wall: Wall,
        status: u16,
        rate: openai::RateLimit,
        out: &mut Queue<Output>,
    ) -> Result<(), openai::DecodeError> {
        let json = openai::Json::collected(document);
        let long = receive_long(&json, self, limits)?;
        match self {
            Decoder::Codex(decoder) => {
                let event = openai::decode_event(&json, limits)?;
                let mut raw = Queue::with_capacity(openai::MAX_OUT);
                match event {
                    openai::Event::Failed { error } => provider_error(error, status, rate, wall, out),
                    event @ (openai::Event::Created { .. }
                    | openai::Event::InProgress { .. }
                    | openai::Event::Added { .. }
                    | openai::Event::ToolAdded { .. }
                    | openai::Event::Done { .. }
                    | openai::Event::TextDelta { .. }
                    | openai::Event::ArgumentsDelta { .. }
                    | openai::Event::ReasoningDelta { .. }
                    | openai::Event::Completed { .. }
                    | openai::Event::Progress
                    | openai::Event::Unknown) => {
                        decoder.received(event, long, limits, wall, &mut raw);
                        codex_outputs(&mut raw, limits, out);
                    }
                }
            }
            Decoder::Anthropic(decoder) => {
                let tokens = json.view();
                let kind = openai::json::required(tokens, b"type")?;
                let kind = openai::json::text_ref(openai::json::value_at(tokens, kind)?)?;
                if name != b"message" && name != kind {
                    return Err(openai::DecodeError::Malformed);
                }
                let event = anthropic::decode_event(&json, limits)?;
                let mut raw = Queue::with_capacity(anthropic::MAX_OUT);
                match event {
                    anthropic::Event::Failed { error } => provider_error(error, status, rate, wall, out),
                    event @ (anthropic::Event::Started { .. }
                    | anthropic::Event::Added { .. }
                    | anthropic::Event::Delta { .. }
                    | anthropic::Event::Done { .. }
                    | anthropic::Event::MessageDelta { .. }
                    | anthropic::Event::Completed
                    | anthropic::Event::Progress
                    | anthropic::Event::Unknown) => {
                        decoder.received(event, long, limits, wall, &mut raw);
                        anthropic_outputs(&mut raw, limits, out);
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn end(&mut self, limits: &openai::Limits, out: &mut Queue<Output>) {
        match self {
            Decoder::Codex(decoder) => {
                let mut raw = Queue::with_capacity(openai::MAX_OUT);
                decoder.end(&mut raw);
                codex_outputs(&mut raw, limits, out);
            }
            Decoder::Anthropic(decoder) => {
                let mut raw = Queue::with_capacity(anthropic::MAX_OUT);
                decoder.end(&mut raw);
                anthropic_outputs(&mut raw, limits, out);
            }
        }
    }
}
fn provider_error(
    error: openai::ProviderError,
    status: u16,
    rate: openai::RateLimit,
    wall: Wall,
    out: &mut Queue<Output>,
) {
    out.push(Output::Failed {
        failure: translate::failure(openai::classify(status, Some(&error), rate, wall)),
        detail: error.message,
    });
}
fn codex_outputs(raw: &mut Queue<openai::Output>, limits: &openai::Limits, out: &mut Queue<Output>) {
    for _slot in 0..openai::MAX_OUT {
        let Some(event) = raw.pop() else {
            break;
        };
        let event = match event {
            openai::Output::Part(part) => Output::Part(translate::part(part, limits)),
            openai::Output::TextDelta { index, content_index, text } => {
                Output::TextDelta { index, content_index, text }
            }
            openai::Output::ArgumentsDelta { index, delta } => Output::ArgumentsDelta { index, delta },
            openai::Output::ReasoningDelta { index, summary_index, text } => {
                Output::ReasoningDelta { index, summary_index, text }
            }
            openai::Output::Completed { stop, usage } => {
                Output::Completed { stop: translate::stop(stop), usage: translate::usage(usage) }
            }
            openai::Output::Failed { failure, detail } => {
                Output::Failed { failure: translate::failure(failure), detail }
            }
            openai::Output::Progress => Output::Progress,
        };
        out.push(event);
    }
}
fn anthropic_outputs(raw: &mut Queue<anthropic::Output>, limits: &openai::Limits, out: &mut Queue<Output>) {
    for _slot in 0..anthropic::MAX_OUT {
        let Some(event) = raw.pop() else {
            break;
        };
        let event = match event {
            anthropic::Output::Part(part) => Output::Part(anthropic_part(part, limits)),
            anthropic::Output::TextDelta { index, content_index, text } => {
                Output::TextDelta { index, content_index, text }
            }
            anthropic::Output::ArgumentsDelta { index, delta } => Output::ArgumentsDelta { index, delta },
            anthropic::Output::ReasoningDelta { index, summary_index, text } => {
                Output::ReasoningDelta { index, summary_index, text }
            }
            anthropic::Output::Completed { stop, usage } => {
                Output::Completed { stop: translate::stop(stop), usage: translate::usage(usage) }
            }
            anthropic::Output::Failed { failure, detail } => {
                Output::Failed { failure: translate::failure(failure), detail }
            }
            anthropic::Output::Progress => Output::Progress,
        };
        out.push(event);
    }
}
fn anthropic_part(part: anthropic::Part, limits: &openai::Limits) -> Result<Block, Error> {
    match part {
        anthropic::Part::Text { text } => Ok(Block::Text { text, replay: None }),
        anthropic::Part::ToolCall { id, name, input, too_large, bytes, cut } => {
            if too_large {
                return Ok(Block::Oversize { id, name, bytes });
            }
            if cut {
                return Ok(Block::Cut { id, name, arguments: input });
            }
            Ok(Block::ToolCall { id, name, arguments: input, replay: None })
        }
        anthropic::Part::Opaque { bytes: value } => {
            let value = match openai::Json::from_bytes(&value, limits) {
                Ok(value) => value,
                Err(error) => return Err(translate::decode(error)),
            };
            Ok(Block::Reasoning { replay: Replay { provider: Provider::Anthropic, value } })
        }
    }
}

/// The collector is built once for the call's configured dialect.
pub(crate) fn event_filter(provider: Provider) -> collector::Filter {
    match provider {
        Provider::OpenAiCodex => openai::filter::EVENT,
        Provider::Anthropic => anthropic::filter::EVENT,
    }
}

/// Total receive failure translation; named text caps retain their owner.
pub(crate) fn failure(error: collector::Error, limits: &client::Limits) -> Failure {
    let bounded = &limits.dialect;
    match error {
        collector::Error::TooManyTokens => {
            Failure::Limit { which: crate::Cap::Tokens, bound: u64::from(bounded.tokens) }
        }
        collector::Error::TooMuchText { cap: None } => {
            Failure::Limit { which: crate::Cap::Document, bound: u64::from(bounded.document_bytes) }
        }
        collector::Error::TooMuchText { cap: Some(cap) } => {
            if cap == filter::REASONING {
                Failure::Limit { which: crate::Cap::Opaque, bound: u64::from(bounded.opaque_bytes) }
            } else if cap == filter::STRINGS {
                Failure::Limit { which: crate::Cap::String, bound: u64::from(bounded.string_bytes) }
            } else {
                Failure::Protocol
            }
        }
        collector::Error::SkippedTooLong => {
            Failure::Limit { which: crate::Cap::Event, bound: u64::from(limits.sse.event) }
        }
        collector::Error::Duplicate | collector::Error::NotTagged => Failure::Protocol,
        collector::Error::Tokenizer(error) => match error {
            tokenizer::Error::TooDeep => Failure::Limit { which: crate::Cap::Depth, bound: u64::from(bounded.depth) },
            tokenizer::Error::StringTooLong => {
                Failure::Limit { which: crate::Cap::String, bound: u64::from(bounded.string_bytes) }
            }
            tokenizer::Error::TooLong => {
                Failure::Limit { which: crate::Cap::Event, bound: u64::from(limits.sse.event) }
            }
            tokenizer::Error::Stream(_) => Failure::Unavailable,
            tokenizer::Error::Unexpected
            | tokenizer::Error::Trailing
            | tokenizer::Error::NumberTooLong
            | tokenizer::Error::Number
            | tokenizer::Error::Escape
            | tokenizer::Error::Surrogate
            | tokenizer::Error::Utf8
            | tokenizer::Error::Control
            | tokenizer::Error::Truncated => Failure::Protocol,
        },
    }
}

/// Long values are meaningful only at argument or Codex reasoning paths.
/// Every other Long retains the configured string or reasoning refusal.
fn receive_long(
    value: &openai::Json,
    decoder: &Decoder,
    limits: &openai::Limits,
) -> Result<Option<u64>, openai::DecodeError> {
    use openai::json;
    let root = value.view();
    let mut allowed = None;
    let mut length = None;
    let kind = json::value_at(root, json::required(root, b"type")?)?;
    if openai::response::long_text(kind).is_some() {
        return Err(openai::DecodeError::limit(crate::Cap::String, limits.string_bytes));
    }
    let kind = json::text_ref(kind)?;
    match decoder {
        Decoder::Codex(_) => match kind {
            b"response.function_call_arguments.delta" => {
                let delta = json::value_at(root, json::required(root, b"delta")?)?;
                length = openai::response::long_text(delta);
                if length.is_some() {
                    allowed = Some(json::offset(delta));
                }
            }
            b"response.output_item.added" | b"response.output_item.done" => {
                let item = json::value_at(root, json::required(root, b"item")?)?;
                let item_kind = json::text_ref(json::value_at(item, json::required(item, b"type")?)?)?;
                let name = match item_kind {
                    b"function_call" => Some(b"arguments".as_slice()),
                    b"reasoning" => Some(b"encrypted_content".as_slice()),
                    _ => None,
                };
                if let Some(name) = name
                    && let Some(text) = json::optional_at(item, json::field(item, name)?)?
                    && let Some(bytes) = openai::response::long_text(text)
                {
                    allowed = Some(json::offset(text));
                    if item_kind == b"function_call" {
                        length = Some(bytes);
                    }
                }
            }
            _ => {}
        },
        Decoder::Anthropic(_) => {
            if kind == b"content_block_delta" {
                let delta = json::value_at(root, json::required(root, b"delta")?)?;
                let delta_kind = json::text_ref(json::value_at(delta, json::required(delta, b"type")?)?)?;
                match delta_kind {
                    b"input_json_delta" => {
                        let text = json::value_at(delta, json::required(delta, b"partial_json")?)?;
                        length = openai::response::long_text(text);
                        if length.is_some() {
                            allowed = Some(json::offset(text));
                        }
                    }
                    b"thinking_delta" | b"signature_delta" => {
                        let name = if delta_kind == b"thinking_delta" {
                            b"thinking".as_slice()
                        } else {
                            b"signature".as_slice()
                        };
                        let text = json::value_at(delta, json::required(delta, name)?)?;
                        if openai::response::long_text(text).is_some() {
                            return Err(openai::DecodeError::limit(crate::Cap::Opaque, limits.opaque_bytes));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    if let Some(length) = length
        && length <= u64::from(limits.input_bytes)
    {
        return Err(openai::DecodeError::limit(crate::Cap::String, limits.string_bytes));
    }
    for index in 0..value.document().len() {
        if value.document().token(index).expect("collected record").kind == Kind::Long && allowed != Some(index) {
            return Err(openai::DecodeError::limit(crate::Cap::String, limits.string_bytes));
        }
    }
    Ok(length)
}
