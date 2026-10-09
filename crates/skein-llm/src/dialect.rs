//! Internal dispatch from bounded provider codecs to the shared vocabulary.
use crate::{Block, Error, Failure, Provider, Replay, Stop, Usage, anthropic, openai, translate};
use alloc::boxed::Box;
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
    pub(crate) fn new(provider: Provider, limits: &openai::Limits) -> Decoder {
        match provider {
            Provider::OpenAiCodex => Decoder::Codex(openai::StreamDecoder::new(limits)),
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
    pub(crate) fn event(
        &mut self,
        message: &skein_http::sse::Message,
        limits: &openai::Limits,
        wall: Wall,
        status: u16,
        rate: openai::RateLimit,
        out: &mut Queue<Output>,
    ) -> Result<(), openai::DecodeError> {
        let json = openai::Json::from_bytes(&message.data, limits)?;
        match self {
            Decoder::Codex(decoder) => {
                let event = openai::decode_event(&json, limits)?;
                let mut raw = Queue::with_capacity(openai::MAX_OUT);
                match event {
                    openai::Event::Failed { error } => provider_error(error, status, rate, wall, out),
                    event @ (openai::Event::Created { .. }
                    | openai::Event::InProgress { .. }
                    | openai::Event::Added { .. }
                    | openai::Event::Done { .. }
                    | openai::Event::TextDelta { .. }
                    | openai::Event::ArgumentsDelta { .. }
                    | openai::Event::ReasoningDelta { .. }
                    | openai::Event::Completed { .. }
                    | openai::Event::Progress
                    | openai::Event::Unknown) => {
                        decoder.event(event, limits, wall, &mut raw);
                        codex_outputs(&mut raw, limits, out);
                    }
                }
            }
            Decoder::Anthropic(decoder) => {
                let tokens = json.as_tokens();
                let kind = openai::json::required(tokens, b"type")?;
                let kind = openai::json::text_ref(openai::json::value_at(tokens, kind)?)?;
                if message.name.as_ref() != b"message" && message.name.as_ref() != kind {
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
                        decoder.event(event, limits, wall, &mut raw);
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
        anthropic::Part::ToolCall { id, name, input, too_large } => {
            if too_large {
                return Err(Error::limit(crate::Cap::Input, limits.input_bytes));
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
