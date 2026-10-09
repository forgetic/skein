//! Neutral compact JSON admission (llm.md, section 2; json.md, sections 5.1–5.2).
//! The value owns one text buffer and fixed records. Admission checks grammar,
//! counts and measured writing; indexed ranges borrow only during the current
//! step and retain no references in machine state. Provider meaning stays in
//! the native codecs. `from_bytes` collects a complete value; `from_document`
//! admits a producer's owned document; the writer emits the document whole.
use crate::openai::{DecodeError, Limits};
use alloc::boxed::Box;
use skein_json::{Compact, Document, Kind, collector, document, tokenizer, writer};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, List, Queue, Stack, Time, Wall, bytes};

/// A neutral admitted value, owned by a prompt, replay or native codec.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Json {
    document: Document,
}

/// An indexed value range, used only while borrowing its document in a step.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Span {
    start: u32,
    end: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Frame {
    ObjectKey,
    ObjectValue,
    Array,
}

impl Json {
    /// Parses and admits a complete bounded value with the generic collector.
    pub fn from_bytes(input: &[u8], limits: &Limits) -> Result<Json, DecodeError> {
        if input.len() > usize::try_from(limits.document_bytes).expect("u32 fits usize") {
            return Err(DecodeError::limit(crate::Cap::Document, limits.document_bytes));
        }
        let bounded = collector::Limits {
            tokenizer: limits.tokenizer_limits(),
            tokens: limits.tokens,
            text: limits.document_bytes,
            skip: u64::from(limits.document_bytes),
        };
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: bounded };
        let mut machine = collector::Collector::new(collector::Filter { root: collector::Keep::Value }, &bounded, &[]);
        let mut above = Queue::with_capacity(1);
        let mut below = Queue::with_capacity(1);
        collector::down(&mut machine, &env, collector::Request::Collect, &mut above, &mut below);
        let mut at: usize = 0;
        let ticks = input.len().saturating_mul(4).saturating_add(16);
        for _tick in 0..ticks {
            match above.pop() {
                Some(collector::Event::Collected(document)) => return Json::from_document(document, limits),
                Some(collector::Event::Failed(error)) => return Err(collector_error(error, limits)),
                Some(collector::Event::Closed) => return Err(DecodeError::Malformed),
                None => {}
            }
            let demand = below.pop().ok_or(DecodeError::Malformed)?;
            let read = match demand {
                Down::Demand { read, .. } => read,
                Down::Send(_) | Down::Finish => return Err(DecodeError::Malformed),
            };
            let remaining = input.get(at..).ok_or(DecodeError::Malformed)?;
            let count = match read {
                Read::Fill(n) => usize::try_from(n).expect("u32 fits usize"),
                Read::Scan { until, max } => {
                    let max = usize::try_from(max).expect("u32 fits usize");
                    let scanned = remaining.get(..remaining.len().min(max)).ok_or(DecodeError::Malformed)?;
                    match bytes::find(scanned, until.as_bytes()) {
                        Some(pos) => pos.saturating_add(until.as_bytes().len()),
                        None => max,
                    }
                }
                Read::Nothing | Read::Line { .. } => return Err(DecodeError::Malformed),
            };
            let delivery = if count <= remaining.len() {
                let delivery = bytes::copy_of(remaining.get(..count).ok_or(DecodeError::Malformed)?);
                at = at.checked_add(count).ok_or(DecodeError::Malformed)?;
                Up::Bytes(delivery)
            } else {
                Up::End
            };
            collector::up(&mut machine, &env, delivery, &mut above, &mut below);
        }
        Err(DecodeError::Malformed)
    }

    /// Admits owned compact records, checking their grammar and measured bytes.
    pub fn from_document(document: Document, limits: &Limits) -> Result<Json, DecodeError> {
        if document.len() > limits.tokens {
            return Err(DecodeError::limit(crate::Cap::Tokens, limits.tokens));
        }
        if document.text_len() > limits.document_bytes {
            return Err(DecodeError::limit(crate::Cap::Document, limits.document_bytes));
        }
        for index in 0..document.len() {
            let record = document.token(index).expect("index within document");
            match record.kind {
                Kind::Key | Kind::String | Kind::Number => {
                    if record.len > limits.string_bytes {
                        return Err(DecodeError::limit(crate::Cap::String, limits.string_bytes));
                    }
                }
                Kind::Long => return Err(DecodeError::Malformed),
                Kind::ObjectStart
                | Kind::ObjectEnd
                | Kind::ArrayStart
                | Kind::ArrayEnd
                | Kind::True
                | Kind::False
                | Kind::Null => {}
            }
        }
        validate((&document, whole(&document)), limits.depth)?;
        let value = Json { document };
        let mut measure = writer::Encoder::measure(&limits.writer_limits());
        value.write(&mut measure);
        match measure.measured() {
            Ok(_) => Ok(value),
            Err(writer::Refusal::TooLong) => Err(DecodeError::limit(crate::Cap::Document, limits.document_bytes)),
            Err(writer::Refusal::TooDeep) => Err(DecodeError::limit(crate::Cap::Depth, limits.depth)),
            Err(writer::Refusal::Text | writer::Refusal::Number) => Err(DecodeError::Malformed),
        }
    }

    /// The admitted compact records and their shared decoded text.
    #[must_use]
    pub fn document(&self) -> &Document {
        &self.document
    }

    pub fn to_bytes(&self, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
        let bounded = limits.writer_limits();
        let mut measure = writer::Encoder::measure(&bounded);
        self.write(&mut measure);
        let len = crate::openai::common::measured(measure, bounded, crate::Cap::Document)?;
        let mut write = writer::Encoder::write(len, &bounded);
        self.write(&mut write);
        Ok(write.finish())
    }

    pub(crate) fn view(&self) -> (&Document, Span) {
        (&self.document, whole(&self.document))
    }

    pub(crate) fn from_view(view: (&Document, Span), limits: &Limits) -> Result<Json, DecodeError> {
        let count = len(view);
        if count > limits.tokens {
            return Err(DecodeError::limit(crate::Cap::Tokens, limits.tokens));
        }
        let mut size = 0_u32;
        for index in 0..count {
            size = size
                .checked_add(u32::try_from(record_text(view, index)?.len()).or(Err(DecodeError::Malformed))?)
                .ok_or(DecodeError::limit(crate::Cap::Document, limits.document_bytes))?;
        }
        if size > limits.document_bytes {
            return Err(DecodeError::limit(crate::Cap::Document, limits.document_bytes));
        }
        let mut text = List::with_capacity(size);
        let mut records = List::with_capacity(count);
        for index in 0..count {
            let source = record(view, index).ok_or(DecodeError::Malformed)?;
            let bytes = record_text(view, index)?;
            let copied = match source.kind {
                Kind::Key | Kind::String | Kind::Number => {
                    Compact { kind: source.kind, start: text.len(), len: source.len }
                }
                Kind::ObjectStart
                | Kind::ObjectEnd
                | Kind::ArrayStart
                | Kind::ArrayEnd
                | Kind::True
                | Kind::False
                | Kind::Null
                | Kind::Long => *source,
            };
            for &byte in bytes {
                text.push(byte).expect("measured selected text");
            }
            records.push(copied).expect("measured selected records");
        }
        let document = Document::from_parts(
            text.into_boxed(),
            records.into_boxed(),
            &document::Limits { tokens: count, text: size },
        )
        .or(Err(DecodeError::Malformed))?;
        Json::from_document(document, limits)
    }

    pub(crate) fn write(&self, out: &mut writer::Encoder) {
        out.document(&self.document);
    }
}

pub(crate) fn whole(document: &Document) -> Span {
    Span { start: 0, end: document.len() }
}

pub(crate) fn len(view: (&Document, Span)) -> u32 {
    view.1.end.checked_sub(view.1.start).expect("ordered admitted range")
}

pub(crate) fn record(view: (&Document, Span), index: u32) -> Option<&Compact> {
    if index >= len(view) {
        return None;
    }
    view.0.token(view.1.start.checked_add(index)?)
}

#[expect(clippy::manual_map, reason = "the strict subset excludes closure-taking maps")]
pub(crate) fn kind(view: (&Document, Span), index: u32) -> Option<Kind> {
    match record(view, index) {
        Some(record) => Some(record.kind),
        None => None,
    }
}

pub(crate) fn record_text(view: (&Document, Span), index: u32) -> Result<&[u8], DecodeError> {
    view.0.text(record(view, index).ok_or(DecodeError::Malformed)?).ok_or(DecodeError::Malformed)
}

/// Writes a complete selected value without allocating token payloads.
pub(crate) fn write_view(out: &mut writer::Encoder, view: (&Document, Span)) {
    for index in 0..len(view) {
        let record = record(view, index).expect("admitted record");
        let text = view.0.text(record).expect("admitted record text");
        match record.kind {
            Kind::ObjectStart => out.object_start(),
            Kind::ObjectEnd => out.object_end(),
            Kind::ArrayStart => out.array_start(),
            Kind::ArrayEnd => out.array_end(),
            Kind::Key => out.key(text),
            Kind::String => out.string(text),
            Kind::Number => out.number(text),
            Kind::True => out.boolean(true),
            Kind::False => out.boolean(false),
            Kind::Null => out.null(),
            Kind::Long => unreachable!("admitted Json contains no Long"),
        }
    }
}

fn collector_error(error: collector::Error, limits: &Limits) -> DecodeError {
    match error {
        collector::Error::Tokenizer(error) => tokenizer_error(error, limits),
        collector::Error::TooManyTokens => DecodeError::limit(crate::Cap::Tokens, limits.tokens),
        collector::Error::TooMuchText { cap: _ } => DecodeError::limit(crate::Cap::Document, limits.document_bytes),
        collector::Error::SkippedTooLong => unreachable!("Value skips no fields"),
        collector::Error::Duplicate => unreachable!("Value interprets no fields"),
    }
}

fn validate(view: (&Document, Span), depth: u32) -> Result<(), DecodeError> {
    let mut open = Stack::with_capacity(depth);
    let mut root = false;
    for index in 0..len(view) {
        let token = kind(view, index).ok_or(DecodeError::Malformed)?;
        match token {
            Kind::Key => match open.top_mut() {
                Some(Frame::ObjectKey) => *open.top_mut().expect("object present") = Frame::ObjectValue,
                Some(Frame::ObjectValue | Frame::Array) | None => return Err(DecodeError::Malformed),
            },
            Kind::ObjectEnd => {
                if open.pop() != Some(Frame::ObjectKey) {
                    return Err(DecodeError::Malformed);
                }
            }
            Kind::ArrayEnd => {
                if open.pop() != Some(Frame::Array) {
                    return Err(DecodeError::Malformed);
                }
            }
            Kind::Long => return Err(DecodeError::Malformed),
            Kind::ObjectStart
            | Kind::ArrayStart
            | Kind::String
            | Kind::Number
            | Kind::True
            | Kind::False
            | Kind::Null => {
                match open.top_mut() {
                    Some(Frame::ObjectValue) => *open.top_mut().expect("object present") = Frame::ObjectKey,
                    Some(Frame::ObjectKey) => return Err(DecodeError::Malformed),
                    Some(Frame::Array) => {}
                    None => {
                        if root {
                            return Err(DecodeError::Malformed);
                        }
                        root = true;
                    }
                }
                let frame = match token {
                    Kind::ObjectStart => Some(Frame::ObjectKey),
                    Kind::ArrayStart => Some(Frame::Array),
                    Kind::ObjectEnd
                    | Kind::ArrayEnd
                    | Kind::Key
                    | Kind::String
                    | Kind::Number
                    | Kind::True
                    | Kind::False
                    | Kind::Null
                    | Kind::Long => None,
                };
                if let Some(frame) = frame
                    && open.push(frame).is_err()
                {
                    return Err(DecodeError::limit(crate::Cap::Depth, depth));
                }
            }
        }
    }
    if !root || !open.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(())
}

pub(crate) fn span(view: (&Document, Span), at: u32) -> Result<u32, DecodeError> {
    let first = kind(view, at).ok_or(DecodeError::Malformed)?;
    match first {
        Kind::ObjectStart | Kind::ArrayStart => {
            let mut depth = 0_u32;
            for index in at..len(view) {
                match kind(view, index).ok_or(DecodeError::Malformed)? {
                    Kind::ObjectStart | Kind::ArrayStart => {
                        depth = depth.checked_add(1).ok_or(DecodeError::Malformed)?;
                    }
                    Kind::ObjectEnd | Kind::ArrayEnd => {
                        depth = depth.checked_sub(1).ok_or(DecodeError::Malformed)?;
                        if depth == 0 {
                            return index.checked_add(1).ok_or(DecodeError::Malformed);
                        }
                    }
                    Kind::Key | Kind::String | Kind::Number | Kind::True | Kind::False | Kind::Null | Kind::Long => {}
                }
            }
            Err(DecodeError::Malformed)
        }
        Kind::String | Kind::Number | Kind::True | Kind::False | Kind::Null | Kind::Long => {
            at.checked_add(1).ok_or(DecodeError::Malformed)
        }
        Kind::ObjectEnd | Kind::ArrayEnd | Kind::Key => Err(DecodeError::Malformed),
    }
}

pub(crate) fn field(view: (&Document, Span), name: &[u8]) -> Result<Option<u32>, DecodeError> {
    if kind(view, 0) != Some(Kind::ObjectStart) {
        return Err(DecodeError::WrongType);
    }
    let mut at = 1_u32;
    let mut found = None;
    for _step in 0..len(view) {
        match kind(view, at) {
            Some(Kind::ObjectEnd) => return Ok(found),
            Some(Kind::Key) => {
                let key = record_text(view, at)?;
                let start = at.checked_add(1).ok_or(DecodeError::Malformed)?;
                at = span(view, start)?;
                if key == name {
                    if found.is_some() {
                        return Err(DecodeError::Malformed);
                    }
                    found = Some(start);
                }
            }
            Some(
                Kind::ObjectStart
                | Kind::ArrayStart
                | Kind::ArrayEnd
                | Kind::String
                | Kind::Number
                | Kind::True
                | Kind::False
                | Kind::Null
                | Kind::Long,
            )
            | None => return Err(DecodeError::Malformed),
        }
    }
    Err(DecodeError::Malformed)
}

pub(crate) fn required(view: (&Document, Span), name: &[u8]) -> Result<u32, DecodeError> {
    field(view, name)?.ok_or(DecodeError::Missing)
}

pub(crate) fn optional_at(
    view: (&Document, Span),
    offset: Option<u32>,
) -> Result<Option<(&Document, Span)>, DecodeError> {
    match offset {
        Some(offset) => Ok(Some(value_at(view, offset)?)),
        None => Ok(None),
    }
}

pub(crate) fn text(view: (&Document, Span)) -> Result<Box<[u8]>, DecodeError> {
    Ok(bytes::copy_of(text_ref(view)?))
}

pub(crate) fn text_ref(view: (&Document, Span)) -> Result<&[u8], DecodeError> {
    if len(view) != 1 || kind(view, 0) != Some(Kind::String) {
        return Err(DecodeError::WrongType);
    }
    record_text(view, 0)
}

pub(crate) fn unsigned(view: (&Document, Span)) -> Result<u64, DecodeError> {
    if len(view) != 1 || kind(view, 0) != Some(Kind::Number) {
        return Err(DecodeError::WrongType);
    }
    let digits = record_text(view, 0)?;
    let mut n = 0_u64;
    if digits.is_empty() {
        return Err(DecodeError::Malformed);
    }
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(DecodeError::WrongType);
        }
        n = n
            .checked_mul(10)
            .ok_or(DecodeError::Malformed)?
            .checked_add(u64::from(byte.wrapping_sub(b'0')))
            .ok_or(DecodeError::Malformed)?;
    }
    Ok(n)
}

pub(crate) fn boolean(view: (&Document, Span)) -> Result<bool, DecodeError> {
    if len(view) != 1 {
        return Err(DecodeError::WrongType);
    }
    match kind(view, 0) {
        Some(Kind::True) => Ok(true),
        Some(Kind::False) => Ok(false),
        Some(
            Kind::ObjectStart
            | Kind::ObjectEnd
            | Kind::ArrayStart
            | Kind::ArrayEnd
            | Kind::Key
            | Kind::String
            | Kind::Number
            | Kind::Null
            | Kind::Long,
        )
        | None => Err(DecodeError::WrongType),
    }
}

pub(crate) fn array(view: (&Document, Span), count: u32) -> Result<List<u32>, DecodeError> {
    if kind(view, 0) != Some(Kind::ArrayStart) {
        return Err(DecodeError::WrongType);
    }
    let mut offsets = List::with_capacity(count);
    let mut at = 1_u32;
    for _step in 0..len(view) {
        if kind(view, at) == Some(Kind::ArrayEnd) {
            return Ok(offsets);
        }
        if offsets.push(at).is_err() {
            return Err(DecodeError::limit(crate::Cap::Parts, count));
        }
        at = span(view, at)?;
    }
    Err(DecodeError::Malformed)
}

pub(crate) fn value_at(view: (&Document, Span), offset: u32) -> Result<(&Document, Span), DecodeError> {
    let end = span(view, offset)?;
    Ok((
        view.0,
        Span {
            start: view.1.start.checked_add(offset).ok_or(DecodeError::Malformed)?,
            end: view.1.start.checked_add(end).ok_or(DecodeError::Malformed)?,
        },
    ))
}

fn tokenizer_error(error: tokenizer::Error, limits: &Limits) -> DecodeError {
    match error {
        tokenizer::Error::TooLong => DecodeError::limit(crate::Cap::Document, limits.document_bytes),
        tokenizer::Error::TooDeep => DecodeError::limit(crate::Cap::Depth, limits.depth),
        tokenizer::Error::StringTooLong => DecodeError::limit(crate::Cap::String, limits.string_bytes),
        tokenizer::Error::NumberTooLong => DecodeError::limit(crate::Cap::Number, limits.tokenizer_limits().number),
        tokenizer::Error::Unexpected
        | tokenizer::Error::Trailing
        | tokenizer::Error::Number
        | tokenizer::Error::Escape
        | tokenizer::Error::Surrogate
        | tokenizer::Error::Utf8
        | tokenizer::Error::Control
        | tokenizer::Error::Truncated
        | tokenizer::Error::Stream(_) => DecodeError::Malformed,
    }
}

/// Usage is accounting: an absent or unusable report never changes a terminal.
pub(crate) fn reported_field<'a>(view: (&'a Document, Span), name: &[u8]) -> Option<(&'a Document, Span)> {
    let at = field(view, name).ok().flatten()?;
    value_at(view, at).ok()
}

pub(crate) fn reported_unsigned(view: (&Document, Span), name: &[u8]) -> Option<u64> {
    unsigned(reported_field(view, name)?).ok()
}
