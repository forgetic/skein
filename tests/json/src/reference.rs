//! A simple reference parser (json.md, 6): recursive descent over a whole
//! document held in memory, which the tokenizer is checked against.
//!
//! It shares no code with the tokenizer. The standard library judges UTF-8
//! (`str::from_utf8`) and surrogate pairs (`char::decode_utf16`), and a
//! number is judged by a plain reading of the grammar. What it mirrors is
//! only what the tokenizer is promised to do (json.md, 3):
//!
//! - **what it sees of a string:** scans to the next quote of at most
//!   `chunk` bytes, so the bytes of a scan that the end of the document
//!   leaves unmet are never seen, and the string is cut short;
//! - **the order of errors at one byte:** a byte's validity before the
//!   limit it would pass, and a character's room at its first byte;
//! - **what each delivery is,** since a delivery that would pass the
//!   document's length fails it before it is read: a byte between tokens
//!   and within a number, the rest of a literal, a scan of a string.

use skein_json::Token;
use skein_json::tokenizer::{Error, Limits};

use crate::{Decoded, Outcome};

/// What `document`, the whole of a stream, decodes to under `limits`.
#[must_use]
pub fn parse(document: &[u8], limits: &Limits) -> Decoded {
    let mut parser = Parser {
        input: document,
        at: 0,
        limits: *limits,
        depth: 0,
        visible: 0,
        tokens: Vec::new(),
        values: Vec::new(),
    };
    let outcome = match parser.document() {
        Ok(()) => Outcome::Done,
        Err(error) => Outcome::Failed(error),
    };
    Decoded { tokens: parser.tokens, outcome }
}

struct Parser<'a> {
    input: &'a [u8],
    /// The next byte to read.
    at: usize,
    limits: Limits,
    /// Objects and arrays open.
    depth: u32,
    /// How far the scans of the string being read have reached.
    visible: usize,
    tokens: Vec<Token>,
    values: Vec<ValueSpan>,
}

impl Parser<'_> {
    fn document(&mut self) -> Result<(), Error> {
        let first = self.significant()?;
        self.value(first)?;
        while self.at < self.input.len() {
            self.delivered(self.at + 1)?;
            if !is_whitespace(self.input[self.at]) {
                return Err(Error::Trailing);
            }
            self.at += 1;
        }
        Ok(())
    }

    /// The next byte that is not whitespace, read, a delivery of its own.
    fn significant(&mut self) -> Result<u8, Error> {
        while let Some(&byte) = self.input.get(self.at) {
            self.delivered(self.at + 1)?;
            self.at += 1;
            if !is_whitespace(byte) {
                return Ok(byte);
            }
        }
        Err(Error::Truncated)
    }

    /// A delivery that ends at `end`, within the document's length.
    fn delivered(&self, end: usize) -> Result<(), Error> {
        if end > usize::try_from(self.limits.length).expect("fits a usize") {
            return Err(Error::TooLong);
        }
        Ok(())
    }

    /// A value, its first byte read.
    fn value(&mut self, first: u8) -> Result<(), Error> {
        let token = self.tokens.len();
        let start = self.at - 1;
        let result = match first {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => {
                let text = self.string()?;
                self.tokens.push(Token::String(text));
                Ok(())
            }
            b't' => self.literal(b"rue", Token::True),
            b'f' => self.literal(b"alse", Token::False),
            b'n' => self.literal(b"ull", Token::Null),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(Error::Unexpected),
        };
        if result.is_ok() {
            self.values.push(ValueSpan { token, end_token: self.tokens.len(), bytes: self.at - start });
        }
        result
    }

    fn object(&mut self) -> Result<(), Error> {
        self.open(Token::ObjectStart)?;
        let mut byte = self.significant()?;
        if byte != b'}' {
            loop {
                if byte != b'"' {
                    return Err(Error::Unexpected);
                }
                let key = self.string()?;
                self.tokens.push(Token::Key(key));
                if self.significant()? != b':' {
                    return Err(Error::Unexpected);
                }
                let first = self.significant()?;
                self.value(first)?;
                match self.significant()? {
                    b',' => byte = self.significant()?,
                    b'}' => break,
                    _ => return Err(Error::Unexpected),
                }
            }
        }
        self.close(Token::ObjectEnd);
        Ok(())
    }

    fn array(&mut self) -> Result<(), Error> {
        self.open(Token::ArrayStart)?;
        let mut byte = self.significant()?;
        if byte != b']' {
            loop {
                self.value(byte)?;
                match self.significant()? {
                    b',' => byte = self.significant()?,
                    b']' => break,
                    _ => return Err(Error::Unexpected),
                }
            }
        }
        self.close(Token::ArrayEnd);
        Ok(())
    }

    fn open(&mut self, token: Token) -> Result<(), Error> {
        if self.depth == self.limits.depth {
            return Err(Error::TooDeep);
        }
        self.depth += 1;
        self.tokens.push(token);
        Ok(())
    }

    fn close(&mut self, token: Token) {
        self.depth -= 1;
        self.tokens.push(token);
    }

    /// The rest of a literal: all of it is demanded at once, so a document
    /// that ends within it is cut short, whatever its bytes.
    fn literal(&mut self, rest: &[u8], token: Token) -> Result<(), Error> {
        let Some(read) = self.input.get(self.at..self.at + rest.len()) else {
            return Err(Error::Truncated);
        };
        self.delivered(self.at + rest.len())?;
        if read != rest {
            return Err(Error::Unexpected);
        }
        self.at += rest.len();
        self.tokens.push(token);
        Ok(())
    }

    /// A number, its first byte read: the run of a number's bytes, each
    /// judged as it comes, by whether some number begins with the text so
    /// far, then by the length.
    fn number(&mut self) -> Result<(), Error> {
        let start = self.at - 1;
        let mut end = self.at;
        while self.input.get(end).is_some_and(|byte| b"0123456789.eE+-".contains(byte)) {
            end += 1;
        }
        let run = &self.input[start..end];
        let limit = usize::try_from(self.limits.number).expect("fits a usize");
        for len in 1..=run.len() {
            // Each byte after the first is a delivery of its own.
            if len > 1 {
                self.delivered(start + len)?;
            }
            let prefix = &run[..len];
            let mut completed = prefix.to_vec();
            completed.push(b'0');
            if !is_number(prefix) && !is_number(&completed) {
                return Err(Error::Number);
            }
            if len > limit {
                return Err(Error::NumberTooLong);
            }
        }
        self.at = end;
        // Within an object or an array, only another byte ends a number;
        // that byte is read with it, a delivery of its own.
        if end == self.input.len() && self.depth > 0 {
            return Err(Error::Truncated);
        }
        if end < self.input.len() {
            self.delivered(end + 1)?;
        }
        if !is_number(run) {
            return Err(Error::Number);
        }
        self.tokens.push(Token::Number(run.into()));
        Ok(())
    }

    /// A string's text, its opening quote read, unescaped.
    fn string(&mut self) -> Result<Box<[u8]>, Error> {
        self.visible = self.at;
        let mut text = Vec::new();
        loop {
            let byte = self.next()?;
            match byte {
                b'"' => return Ok(text.into()),
                b'\\' => self.escape(&mut text)?,
                0x00..=0x1F => return Err(Error::Control),
                0x20..=0x7F => self.put(&mut text, &[byte])?,
                _ => self.character(byte, &mut text)?,
            }
        }
    }

    /// The next byte of a string, if a scan delivers it: scans run to the
    /// next quote, escaped or not, each of at most `chunk` bytes, and one
    /// that the end of the document leaves unmet delivers nothing.
    fn next(&mut self) -> Result<u8, Error> {
        let chunk = usize::try_from(self.limits.chunk).expect("fits a usize");
        while self.at >= self.visible {
            let rest = &self.input[self.visible..];
            let window = &rest[..rest.len().min(chunk)];
            match window.iter().position(|&byte| byte == b'"') {
                Some(quote) => self.visible += quote + 1,
                None if rest.len() >= chunk => self.visible += chunk,
                None => return Err(Error::Truncated),
            }
            self.delivered(self.visible)?;
        }
        let byte = self.input[self.at];
        self.at += 1;
        Ok(byte)
    }

    /// A character of more than one byte, its first byte read, judged by
    /// the standard library as each byte comes.
    fn character(&mut self, first: u8, text: &mut Vec<u8>) -> Result<(), Error> {
        let width = match first {
            0xC2..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF4 => 4,
            _ => return Err(Error::Utf8),
        };
        self.room(text, width)?;
        let mut bytes = vec![first];
        loop {
            match std::str::from_utf8(&bytes) {
                Ok(_) => {
                    text.extend_from_slice(&bytes);
                    return Ok(());
                }
                Err(error) if error.error_len().is_some() => return Err(Error::Utf8),
                Err(_) => bytes.push(self.next()?),
            }
        }
    }

    /// An escape, its backslash read.
    fn escape(&mut self, text: &mut Vec<u8>) -> Result<(), Error> {
        let unescaped = match self.next()? {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 0x08,
            b'f' => 0x0C,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'u' => return self.unicode(text),
            _ => return Err(Error::Escape),
        };
        self.put(text, &[unescaped])
    }

    /// A `\u` escape, its `u` read, and the low half's after a high one.
    fn unicode(&mut self, text: &mut Vec<u8>) -> Result<(), Error> {
        let unit = self.hex()?;
        let mut units = vec![unit];
        if (0xD800..=0xDBFF).contains(&unit) {
            if self.next()? != b'\\' || self.next()? != b'u' {
                return Err(Error::Surrogate);
            }
            units.push(self.hex()?);
        }
        let Some(Ok(character)) = char::decode_utf16(units.iter().copied()).next() else {
            return Err(Error::Surrogate);
        };
        if character.len_utf16() != units.len() {
            return Err(Error::Surrogate);
        }
        let mut buffer = [0; 4];
        self.put(text, character.encode_utf8(&mut buffer).as_bytes())
    }

    /// Four hex digits.
    fn hex(&mut self) -> Result<u16, Error> {
        let mut unit = 0;
        for _ in 0..4 {
            let digit = char::from(self.next()?).to_digit(16).ok_or(Error::Escape)?;
            unit = unit * 16 + u16::try_from(digit).expect("a hex digit");
        }
        Ok(unit)
    }

    fn room(&self, text: &[u8], more: usize) -> Result<(), Error> {
        if text.len() + more > usize::try_from(self.limits.string).expect("fits a usize") {
            return Err(Error::StringTooLong);
        }
        Ok(())
    }

    fn put(&self, text: &mut Vec<u8>, bytes: &[u8]) -> Result<(), Error> {
        self.room(text, bytes.len())?;
        text.extend_from_slice(bytes);
        Ok(())
    }
}

fn is_whitespace(byte: u8) -> bool {
    b" \t\n\r".contains(&byte)
}

/// Whether `text` is a number, by a plain reading of RFC 8259, section 6:
/// an optional minus, an integer without a leading zero, an optional
/// fraction, an optional exponent.
#[must_use]
pub fn is_number(text: &[u8]) -> bool {
    fn digits(text: &[u8]) -> usize {
        text.iter().take_while(|byte| byte.is_ascii_digit()).count()
    }
    let rest = text.strip_prefix(b"-").unwrap_or(text);
    let integer = digits(rest);
    if integer == 0 || (integer > 1 && rest[0] == b'0') {
        return false;
    }
    let mut rest = &rest[integer..];
    if let Some(fraction) = rest.strip_prefix(b".") {
        let count = digits(fraction);
        if count == 0 {
            return false;
        }
        rest = &fraction[count..];
    }
    if let Some(exponent) = rest.strip_prefix(b"e").or(rest.strip_prefix(b"E")) {
        let exponent = exponent.strip_prefix(b"+").or(exponent.strip_prefix(b"-")).unwrap_or(exponent);
        let count = digits(exponent);
        if count == 0 {
            return false;
        }
        rest = &exponent[count..];
    }
    rest.is_empty()
}

/// A complete value's token interval and wire length, recorded by the recursive reference.
struct ValueSpan {
    token: usize,
    end_token: usize,
    bytes: usize,
}

/// Draws legal demands from a complete document's own value positions and independently
/// computes their answers. The reference retains the original text; the tokenizer need not.
#[must_use]
pub fn demands(
    document: &[u8],
    limits: &Limits,
    seed: u64,
) -> (Vec<skein_json::tokenizer::Request>, Vec<skein_json::tokenizer::Event>) {
    use skein_json::tokenizer::{Event, Request};
    let mut parser = Parser {
        input: document,
        at: 0,
        limits: *limits,
        depth: 0,
        visible: 0,
        tokens: Vec::new(),
        values: Vec::new(),
    };
    parser.document().expect("a complete bounded generated document");
    let mut rng = skein_lib::Rng::new(seed);
    let mut requests = Vec::new();
    let mut events = Vec::new();
    let mut at = 0;
    while at < parser.tokens.len() {
        if rng.chance(300)
            && let Some(value) = parser.values.iter().find(|value| value.token == at)
        {
            requests.push(Request::Skip);
            events.push(Event::Skipped(value.bytes as u64));
            at = value.end_token;
        } else {
            let token = parser.tokens[at].clone();
            if rng.chance(500) {
                let cap = u32::try_from(rng.below(32)).expect("below 32");
                requests.push(Request::Text(cap));
                let long = match &token {
                    Token::String(text) | Token::Key(text) => text.len() > cap as usize,
                    Token::ObjectStart
                    | Token::ObjectEnd
                    | Token::ArrayStart
                    | Token::ArrayEnd
                    | Token::Number(_)
                    | Token::True
                    | Token::False
                    | Token::Null => false,
                };
                if long {
                    let length = match &token {
                        Token::String(text) | Token::Key(text) => text.len(),
                        Token::ObjectStart
                        | Token::ObjectEnd
                        | Token::ArrayStart
                        | Token::ArrayEnd
                        | Token::Number(_)
                        | Token::True
                        | Token::False
                        | Token::Null => unreachable!("only text is long"),
                    };
                    events.push(Event::Long(length as u64));
                } else {
                    events.push(Event::Token(token));
                }
            } else {
                requests.push(Request::Next);
                events.push(Event::Token(token));
            }
            at += 1;
        }
    }
    requests.push(Request::Next);
    events.push(Event::Done);
    (requests, events)
}

/// Prunes independently parsed value spans through a filter, including exact
/// source byte counts for every removed value and decoded lengths for Long.
#[must_use]
pub fn prune(
    document: &[u8],
    filter: skein_json::collector::Filter,
    limits: &skein_json::collector::Limits,
) -> Option<(skein_json::collector::Event, u64, Vec<Token>)> {
    use skein_json::collector::Event;
    let mut parser = Parser {
        input: document,
        at: 0,
        limits: Limits { string: u32::MAX, ..limits.tokenizer },
        depth: 0,
        visible: 0,
        tokens: Vec::new(),
        values: Vec::new(),
    };
    if parser.document().is_err() {
        return None;
    }
    let mut result = Pruned {
        parser: &parser,
        tokens: Vec::new(),
        text: Vec::new(),
        skipped: 0,
        skipped_values: Vec::new(),
        limits,
    };
    let outcome = match result.value(0, filter.root) {
        Ok(_) => Event::Collected(
            skein_json::Document::from_parts(
                result.text.into(),
                result.tokens.into(),
                &skein_json::document::Limits { tokens: limits.tokens, text: limits.text },
            )
            .expect("test value fits its admitted bounds"),
        ),
        Err(error) => Event::Failed(error),
    };
    Some((outcome, result.skipped, result.skipped_values))
}
struct Pruned<'a> {
    parser: &'a Parser<'a>,
    tokens: Vec<skein_json::Compact>,
    text: Vec<u8>,
    skipped: u64,
    skipped_values: Vec<Token>,
    limits: &'a skein_json::collector::Limits,
}
impl Pruned<'_> {
    fn push(&mut self, token: &Token, long: bool) -> Result<(), skein_json::collector::Error> {
        use skein_json::collector::Error;
        use skein_json::{Compact, Kind};
        if self.tokens.len() == self.limits.tokens as usize {
            return Err(Error::TooManyTokens);
        }
        let (kind, bytes) = match token {
            Token::ObjectStart => (Kind::ObjectStart, &[][..]),
            Token::ObjectEnd => (Kind::ObjectEnd, &[][..]),
            Token::ArrayStart => (Kind::ArrayStart, &[][..]),
            Token::ArrayEnd => (Kind::ArrayEnd, &[][..]),
            Token::Key(bytes) => (Kind::Key, bytes.as_ref()),
            Token::String(bytes) => (if long { Kind::Long } else { Kind::String }, bytes.as_ref()),
            Token::Number(bytes) => (Kind::Number, bytes.as_ref()),
            Token::True => (Kind::True, &[][..]),
            Token::False => (Kind::False, &[][..]),
            Token::Null => (Kind::Null, &[][..]),
        };
        let mut start = 0;
        if matches!(kind, Kind::Key | Kind::String | Kind::Number) {
            if self.text.len() + bytes.len() > self.limits.text as usize {
                return Err(Error::TooMuchText { cap: None });
            }
            start = u32::try_from(self.text.len()).expect("retained under u32 text");
            self.text.extend_from_slice(bytes);
        }
        self.tokens.push(Compact { kind, start, len: u32::try_from(bytes.len()).expect("a generated bounded string") });
        Ok(())
    }
    fn tag_variant(
        &self,
        at: usize,
        end: usize,
        tag: &'static skein_json::collector::Tagged,
    ) -> Result<Option<&'static skein_json::collector::Variant>, skein_json::collector::Error> {
        use skein_json::collector::Error;
        let mut tag_value = None;
        let mut child = at + 1;
        while child < end - 1 {
            let Token::Key(key) = &self.parser.tokens[child] else { unreachable!() };
            if key.as_ref() == tag.tag {
                if tag_value.is_some() {
                    return Err(Error::Duplicate);
                }
                let Token::String(value) = &self.parser.tokens[child + 1] else {
                    return Err(Error::NotTagged);
                };
                tag_value = Some(value.as_ref());
            }
            child = self
                .parser
                .values
                .iter()
                .find(|value| value.token == child + 1)
                .expect("the independent parser recorded every value")
                .end_token;
        }
        Ok(tag.known.iter().find(|variant| Some(variant.value) == tag_value))
    }

    fn unknown(
        &mut self,
        at: usize,
        end: usize,
        cap_name: skein_json::collector::Cap,
    ) -> Result<usize, skein_json::collector::Error> {
        use skein_json::collector::Error;
        let cap = crate::COLLECTOR_CAPS[usize::from(cap_name.index())] as usize;
        let mut text = 0;
        for token in &self.parser.tokens[at..end] {
            if self.tokens.len() == self.limits.tokens as usize {
                return Err(Error::TooManyTokens);
            }
            if let Token::Key(bytes) | Token::String(bytes) | Token::Number(bytes) = token {
                text += bytes.len();
            }
            if text > cap {
                return Err(Error::TooMuchText { cap: Some(cap_name) });
            }
            self.push(token, false)?;
        }
        Ok(end)
    }

    fn value(&mut self, at: usize, keep: skein_json::collector::Keep) -> Result<usize, skein_json::collector::Error> {
        use skein_json::collector::{Error, Keep, Key};
        let span =
            self.parser.values.iter().find(|span| span.token == at).expect("test value fits its admitted bounds");
        let token = &self.parser.tokens[at];
        if keep == Keep::Value {
            for token in &self.parser.tokens[at..span.end_token] {
                self.push(token, false)?;
            }
            return Ok(span.end_token);
        }
        match token {
            Token::String(bytes) => {
                self.push(
                    token,
                    matches!(keep,Keep::Text(cap) if bytes.len() > crate::COLLECTOR_CAPS[usize::from(cap.index())].min(self.limits.tokenizer.string) as usize),
                )?;
            }
            Token::ObjectStart | Token::ArrayStart => {
                if let Keep::Text(_) = keep {
                    return self.value(at, Keep::Value);
                }
                let (nodes, tag_field) = match keep {
                    Keep::Into(nodes) => (nodes, None),
                    Keep::Tagged(tag) => {
                        if !matches!(token, Token::ObjectStart) {
                            return Err(Error::NotTagged);
                        }
                        let variant = self.tag_variant(at, span.end_token, tag)?;
                        if let Some(variant) = variant {
                            (variant.children, Some(tag.tag))
                        } else {
                            return self.unknown(at, span.end_token, tag.unknown);
                        }
                    }
                    Keep::Value | Keep::Text(_) => unreachable!(),
                };
                self.push(token, false)?;
                let object = matches!(token, Token::ObjectStart);
                let mut child = at + 1;
                let mut seen = std::collections::BTreeSet::new();
                while child < span.end_token - 1 {
                    let key = if object {
                        let Token::Key(bytes) = &self.parser.tokens[child] else { unreachable!() };
                        Some(bytes.as_ref())
                    } else {
                        None
                    };
                    let selected = nodes.iter().find(|node| match node.key {
                        Key::Field(name) => key == Some(name),
                        Key::Each => !object,
                    });
                    let value = child + usize::from(object);
                    let selected_keep = if key == tag_field && tag_field.is_some() {
                        Some(Keep::Value)
                    } else {
                        selected.map(|node| node.keep)
                    };
                    if let Some(keep) = selected_keep {
                        if let Some(key) = key {
                            if !seen.insert(key) {
                                return Err(Error::Duplicate);
                            }
                            self.push(&self.parser.tokens[child], false)?;
                        }
                        child = self.value(value, keep)?;
                    } else {
                        let dropped = self
                            .parser
                            .values
                            .iter()
                            .find(|span| span.token == value)
                            .expect("test value fits its admitted bounds");
                        self.skipped_values.push(self.parser.tokens[value].clone());
                        self.skipped += dropped.bytes as u64;
                        if self.skipped > self.limits.skip {
                            return Err(Error::SkippedTooLong);
                        }
                        child = dropped.end_token;
                    }
                }
                self.push(&self.parser.tokens[span.end_token - 1], false)?;
            }
            Token::ObjectEnd
            | Token::ArrayEnd
            | Token::Key(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => self.push(token, false)?,
        }
        Ok(span.end_token)
    }
}
