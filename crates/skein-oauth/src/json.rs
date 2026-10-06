//! A bounded owned JSON value, using skein's tokenizer and measured writer.
use crate::{DecodeError, Limits};
use alloc::boxed::Box;
use skein_json::{Token, tokenizer, writer};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, List, Queue, Stack, Time, Wall, bytes};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Json {
    tokens: Box<[Token]>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Frame {
    ObjectKey,
    ObjectValue,
    Array,
}

impl Json {
    /// Parses a whole bounded document. The connection may instead collect
    /// tokenizer tokens with `Collector` to keep tokenization incremental.
    pub fn from_bytes(input: &[u8], limits: &Limits) -> Result<Json, DecodeError> {
        if input.len() > usize::try_from(limits.document_bytes).expect("u32 fits usize") {
            return Err(DecodeError::TooLarge);
        }
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.tokenizer_limits() };
        let mut machine = tokenizer::Tokenizer::new(&env.limits);
        let mut above = Queue::with_capacity(1);
        let mut below = Queue::with_capacity(1);
        let mut collector = Collector::new(limits);
        let mut at: usize = 0;
        let ticks = input.len().saturating_mul(4).saturating_add(16);
        for _tick in 0..ticks {
            match machine.waiting() {
                tokenizer::Waiting::Next => {
                    tokenizer::down(&mut machine, &env, tokenizer::Request::Next, &mut above, &mut below);
                }
                tokenizer::Waiting::Bytes => {
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
                    if count <= remaining.len() {
                        let delivery = bytes::copy_of(remaining.get(..count).ok_or(DecodeError::Malformed)?);
                        at = at.checked_add(count).ok_or(DecodeError::TooLarge)?;
                        tokenizer::up(&mut machine, &env, Up::Bytes(delivery), &mut above, &mut below);
                    } else {
                        tokenizer::up(&mut machine, &env, Up::End, &mut above, &mut below);
                    }
                }
                tokenizer::Waiting::Close | tokenizer::Waiting::Nothing => return Err(DecodeError::Malformed),
            }
            if let Some(event) = above.pop() {
                match event {
                    tokenizer::Event::Token(token) => collector.push(token)?,
                    tokenizer::Event::Done => return collector.finish(limits),
                    tokenizer::Event::Failed(error) => return Err(tokenizer_error(error)),
                    tokenizer::Event::Closed => return Err(DecodeError::Malformed),
                }
            }
        }
        Err(DecodeError::Malformed)
    }

    pub fn from_tokens(tokens: &[Token], limits: &Limits) -> Result<Json, DecodeError> {
        let mut collector = Collector::new(limits);
        for token in tokens {
            collector.check(token)?;
            collector.push(token.clone())?;
        }
        collector.finish(limits)
    }

    #[must_use]
    pub fn as_tokens(&self) -> &[Token] {
        &self.tokens
    }

    pub fn to_bytes(&self, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
        let bounded = limits.writer_limits();
        let mut measure = writer::Encoder::measure(&bounded);
        self.write(&mut measure);
        let len = crate::common::measured(measure)?;
        let mut write = writer::Encoder::write(len, &bounded);
        self.write(&mut write);
        Ok(write.finish())
    }

    pub(crate) fn admit(&self, limits: &Limits) -> Result<(), DecodeError> {
        if self.tokens.len() > usize::try_from(limits.tokens).expect("u32 fits usize") {
            return Err(DecodeError::TooLarge);
        }
        let mut retained: u64 = 0;
        for token in &self.tokens {
            let size = match token {
                Token::Key(value) | Token::String(value) | Token::Number(value) => value.len(),
                Token::ObjectStart
                | Token::ObjectEnd
                | Token::ArrayStart
                | Token::ArrayEnd
                | Token::True
                | Token::False
                | Token::Null => 1,
            };
            if size > usize::try_from(limits.string_bytes).expect("u32 fits usize") {
                return Err(DecodeError::TooLarge);
            }
            retained =
                retained.checked_add(u64::try_from(size).expect("usize fits u64")).ok_or(DecodeError::TooLarge)?;
            if retained > u64::from(limits.document_bytes) {
                return Err(DecodeError::TooLarge);
            }
        }
        validate(&self.tokens, limits.depth)?;
        let mut measure = writer::Encoder::measure(&limits.writer_limits());
        self.write(&mut measure);
        let _length = crate::common::measured(measure)?;
        Ok(())
    }

    pub(crate) fn write(&self, out: &mut writer::Encoder) {
        for token in &self.tokens {
            out.token(token);
        }
    }
}

/// Incremental ownership of tokens from skein-json, under explicit count and
/// byte bounds. Finish checks grammar before the writer can see them.
#[derive(Debug)]
pub struct Collector {
    tokens: List<Token>,
    bytes: u64,
    byte_limit: u32,
    string_limit: u32,
}
impl Collector {
    #[must_use]
    pub fn new(limits: &Limits) -> Collector {
        Collector {
            tokens: List::with_capacity(limits.tokens),
            bytes: 0,
            byte_limit: limits.document_bytes,
            string_limit: limits.string_bytes,
        }
    }
    fn check(&self, token: &Token) -> Result<u64, DecodeError> {
        let size = match token {
            Token::Key(b) | Token::String(b) | Token::Number(b) => b.len(),
            Token::ObjectStart
            | Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::True
            | Token::False
            | Token::Null => 1,
        };
        if size > usize::try_from(self.string_limit).expect("u32 fits usize") {
            return Err(DecodeError::TooLarge);
        }
        let bytes =
            self.bytes.checked_add(u64::try_from(size).expect("usize fits u64")).ok_or(DecodeError::TooLarge)?;
        if bytes > u64::from(self.byte_limit) || self.tokens.room() == 0 {
            return Err(DecodeError::TooLarge);
        }
        Ok(bytes)
    }
    pub fn push(&mut self, token: Token) -> Result<(), DecodeError> {
        let bytes = self.check(&token)?;
        match self.tokens.push(token) {
            Ok(()) => {
                self.bytes = bytes;
                Ok(())
            }
            Err(_) => Err(DecodeError::TooLarge),
        }
    }
    pub fn finish(self, limits: &Limits) -> Result<Json, DecodeError> {
        validate(self.tokens.as_slice(), limits.depth)?;
        let value = Json { tokens: self.tokens.into_boxed() };
        let mut measure = writer::Encoder::measure(&limits.writer_limits());
        value.write(&mut measure);
        match measure.measured() {
            Ok(_) => Ok(value),
            Err(writer::Refusal::TooLong | writer::Refusal::TooDeep) => Err(DecodeError::TooLarge),
            Err(writer::Refusal::Text | writer::Refusal::Number) => Err(DecodeError::Malformed),
        }
    }
}

fn validate(tokens: &[Token], depth: u32) -> Result<(), DecodeError> {
    let mut open = Stack::with_capacity(depth);
    let mut root = false;
    for token in tokens {
        match token {
            Token::Key(_) => match open.top_mut() {
                Some(Frame::ObjectKey) => {
                    *open.top_mut().expect("object present") = Frame::ObjectValue;
                }
                Some(Frame::ObjectValue | Frame::Array) | None => return Err(DecodeError::Malformed),
            },
            Token::ObjectEnd => {
                if open.pop() != Some(Frame::ObjectKey) {
                    return Err(DecodeError::Malformed);
                }
            }
            Token::ArrayEnd => {
                if open.pop() != Some(Frame::Array) {
                    return Err(DecodeError::Malformed);
                }
            }
            Token::ObjectStart
            | Token::ArrayStart
            | Token::String(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => {
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
                    Token::ObjectStart => Some(Frame::ObjectKey),
                    Token::ArrayStart => Some(Frame::Array),
                    Token::ObjectEnd
                    | Token::ArrayEnd
                    | Token::Key(_)
                    | Token::String(_)
                    | Token::Number(_)
                    | Token::True
                    | Token::False
                    | Token::Null => None,
                };
                if let Some(frame) = frame
                    && open.push(frame).is_err()
                {
                    return Err(DecodeError::TooLarge);
                }
            }
        }
    }
    if !root || !open.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(())
}

pub(crate) fn span(tokens: &[Token], at: usize) -> Result<usize, DecodeError> {
    let first = tokens.get(at).ok_or(DecodeError::Malformed)?;
    match first {
        Token::ObjectStart | Token::ArrayStart => {
            let mut depth: u32 = 0;
            for (offset, token) in tokens.get(at..).ok_or(DecodeError::Malformed)?.iter().enumerate() {
                match token {
                    Token::ObjectStart | Token::ArrayStart => {
                        depth = depth.checked_add(1).ok_or(DecodeError::TooLarge)?;
                    }
                    Token::ObjectEnd | Token::ArrayEnd => {
                        depth = depth.checked_sub(1).ok_or(DecodeError::Malformed)?;
                        if depth == 0 {
                            return at
                                .checked_add(offset)
                                .ok_or(DecodeError::TooLarge)?
                                .checked_add(1)
                                .ok_or(DecodeError::TooLarge);
                        }
                    }
                    Token::Key(_) | Token::String(_) | Token::Number(_) | Token::True | Token::False | Token::Null => {}
                }
            }
            Err(DecodeError::Malformed)
        }
        Token::String(_) | Token::Number(_) | Token::True | Token::False | Token::Null => {
            at.checked_add(1).ok_or(DecodeError::TooLarge)
        }
        Token::ObjectEnd | Token::ArrayEnd | Token::Key(_) => Err(DecodeError::Malformed),
    }
}

pub(crate) fn field(tokens: &[Token], name: &[u8]) -> Result<Option<u32>, DecodeError> {
    if tokens.first() != Some(&Token::ObjectStart) {
        return Err(DecodeError::WrongType);
    }
    let mut at: usize = 1;
    let mut found = None;
    for _step in 0..tokens.len() {
        match tokens.get(at) {
            Some(Token::ObjectEnd) => return Ok(found),
            Some(Token::Key(key)) => {
                let start = at.checked_add(1).ok_or(DecodeError::TooLarge)?;
                at = span(tokens, start)?;
                if key.as_ref() == name {
                    if found.is_some() {
                        return Err(DecodeError::Malformed);
                    }
                    found = Some(u32::try_from(start).or(Err(DecodeError::TooLarge))?);
                }
            }
            Some(
                Token::ObjectStart
                | Token::ArrayStart
                | Token::ArrayEnd
                | Token::String(_)
                | Token::Number(_)
                | Token::True
                | Token::False
                | Token::Null,
            )
            | None => return Err(DecodeError::Malformed),
        }
    }
    Err(DecodeError::Malformed)
}
pub(crate) fn required(tokens: &[Token], name: &[u8]) -> Result<u32, DecodeError> {
    field(tokens, name)?.ok_or(DecodeError::Missing)
}
pub(crate) fn optional_at(tokens: &[Token], offset: Option<u32>) -> Result<Option<&[Token]>, DecodeError> {
    match offset {
        Some(offset) => Ok(Some(value_at(tokens, offset)?)),
        None => Ok(None),
    }
}
pub(crate) fn text_ref(tokens: &[Token]) -> Result<&[u8], DecodeError> {
    match tokens {
        [Token::String(b)] => Ok(b),
        _ => Err(DecodeError::WrongType),
    }
}
#[expect(clippy::manual_let_else, reason = "token matching stays explicit in the strict subset")]
pub(crate) fn unsigned(tokens: &[Token]) -> Result<u64, DecodeError> {
    let digits = match tokens {
        [Token::Number(b)] => b,
        _ => return Err(DecodeError::WrongType),
    };
    let mut n: u64 = 0;
    if digits.is_empty() {
        return Err(DecodeError::Malformed);
    }
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(DecodeError::WrongType);
        }
        n = n
            .checked_mul(10)
            .ok_or(DecodeError::TooLarge)?
            .checked_add(u64::from(byte.wrapping_sub(b'0')))
            .ok_or(DecodeError::TooLarge)?;
    }
    Ok(n)
}
pub(crate) fn value_at(tokens: &[Token], offset: u32) -> Result<&[Token], DecodeError> {
    let start = usize::try_from(offset).expect("u32 fits usize");
    let end = span(tokens, start)?;
    tokens.get(start..end).ok_or(DecodeError::Malformed)
}

fn tokenizer_error(error: tokenizer::Error) -> DecodeError {
    match error {
        tokenizer::Error::TooLong
        | tokenizer::Error::TooDeep
        | tokenizer::Error::StringTooLong
        | tokenizer::Error::NumberTooLong => DecodeError::TooLarge,
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

pub(crate) fn validation_worst_case(limits: &Limits) -> Option<u64> {
    Stack::<Frame>::worst_case(limits.depth)
}
