//! Bounded JSON document access for CDP replies and events.
//!
//! The document owns one message. Values borrow spans from it, so walking
//! nested fields does not copy the subtree. Only requested strings unescape.

#![expect(clippy::disallowed_types, reason = "requested JSON strings are copied to owned byte vectors")]

use alloc::boxed::Box;
use alloc::vec::Vec;

/// A malformed or overlong CDP document.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DecodeError {
    TooLong,
    TooDeep,
    Syntax,
}

/// One complete JSON document, without the trailing NUL.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Document {
    bytes: Box<[u8]>,
}

impl Document {
    /// Validate and hold a CDP message under `limit` bytes.
    pub fn parse(bytes: &[u8], limit: u32) -> Result<Document, DecodeError> {
        let Ok(limit) = usize::try_from(limit) else {
            return Err(DecodeError::TooLong);
        };
        if bytes.len() > limit {
            return Err(DecodeError::TooLong);
        }
        let mut parser = Parser { bytes, at: 0 };
        parser.space();
        parser.value(0)?;
        parser.space();
        if parser.at != bytes.len() {
            return Err(DecodeError::Syntax);
        }
        Ok(Document { bytes: Box::from(bytes) })
    }

    /// The root JSON value.
    #[must_use]
    pub fn root(&self) -> Value<'_> {
        Value::new(&self.bytes)
    }
}

/// A valid JSON value borrowed from a validated document.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Value<'a> {
    bytes: &'a [u8],
}

impl<'a> Value<'a> {
    fn new(bytes: &'a [u8]) -> Value<'a> {
        Value { bytes: bytes.trim_ascii() }
    }

    /// The JSON representation of this value.
    #[must_use]
    pub fn raw(self) -> &'a [u8] {
        self.bytes
    }

    /// A member of an object, if it exists. Duplicate keys select the first.
    #[must_use]
    pub fn get(self, key: &[u8]) -> Option<Value<'a>> {
        if self.bytes.first() != Some(&b'{') {
            return None;
        }
        let mut parser = Parser { bytes: self.bytes, at: 1 };
        parser.space();
        while parser.peek() != Some(b'}') {
            let start = parser.at;
            parser.string().ok()?;
            let name = Value::new(self.bytes.get(start..parser.at)?).text()?;
            parser.space();
            parser.take(b':').ok()?;
            parser.space();
            let start = parser.at;
            parser.value(0).ok()?;
            if name.as_ref() == key {
                return Some(Value::new(self.bytes.get(start..parser.at)?));
            }
            parser.space();
            if parser.peek() == Some(b',') {
                parser.at = parser.at.checked_add(1)?;
                parser.space();
            } else {
                break;
            }
        }
        None
    }

    /// Iterate an array's elements.
    #[must_use]
    pub fn array(self) -> Option<Array<'a>> {
        if self.bytes.first() != Some(&b'[') {
            return None;
        }
        Some(Array { bytes: self.bytes, at: 1, done: false })
    }

    /// The unescaped UTF-8 bytes of a JSON string.
    #[must_use]
    pub fn text(self) -> Option<Box<[u8]>> {
        if self.bytes.first() != Some(&b'"') || self.bytes.last() != Some(&b'"') {
            return None;
        }
        let inner = self.bytes.get(1..self.bytes.len().checked_sub(1)?)?;
        let mut out = Vec::with_capacity(inner.len());
        let mut at = 0_usize;
        while at < inner.len() {
            let byte = *inner.get(at)?;
            if byte != b'\\' {
                out.push(byte);
                at = at.checked_add(1)?;
                continue;
            }
            at = at.checked_add(1)?;
            let escaped = *inner.get(at)?;
            match escaped {
                b'"' | b'\\' | b'/' => out.push(escaped),
                b'b' => out.push(8),
                b'f' => out.push(12),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'u' => {
                    let first = hex4(inner, at.checked_add(1)?)?;
                    at = at.checked_add(4)?;
                    let scalar = if (0xD800..=0xDBFF).contains(&first) {
                        if inner.get(at.checked_add(1)?..at.checked_add(3)?)? != b"\\u" {
                            return None;
                        }
                        let second = hex4(inner, at.checked_add(3)?)?;
                        if !(0xDC00..=0xDFFF).contains(&second) {
                            return None;
                        }
                        at = at.checked_add(6)?;
                        0x10000_u32
                            .checked_add((u32::from(first).checked_sub(0xD800)?).checked_mul(1024)?)?
                            .checked_add(u32::from(second).checked_sub(0xDC00)?)?
                    } else {
                        u32::from(first)
                    };
                    let ch = char::from_u32(scalar)?;
                    let mut encoded = [0_u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut encoded).as_bytes());
                }
                _ => return None,
            }
            at = at.checked_add(1)?;
        }
        Some(out.into_boxed_slice())
    }

    /// An unsigned JSON integer.
    #[must_use]
    pub fn u64(self) -> Option<u64> {
        unsigned(self.bytes)
    }

    /// A signed JSON integer.
    #[must_use]
    pub fn i64(self) -> Option<i64> {
        signed(self.bytes)
    }

    /// A JSON boolean.
    #[must_use]
    pub fn bool(self) -> Option<bool> {
        match self.bytes {
            b"true" => Some(true),
            b"false" => Some(false),
            _ => None,
        }
    }

    /// Whether this is JSON null.
    #[must_use]
    pub fn is_null(self) -> bool {
        self.bytes == b"null"
    }
}

/// A borrowed iterator through a validated JSON array.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Array<'a> {
    bytes: &'a [u8],
    at: usize,
    done: bool,
}

impl<'a> Iterator for Array<'a> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut parser = Parser { bytes: self.bytes, at: self.at };
        parser.space();
        if parser.peek() == Some(b']') {
            self.done = true;
            return None;
        }
        let start = parser.at;
        parser.value(0).ok()?;
        let value = Value::new(self.bytes.get(start..parser.at)?);
        parser.space();
        if parser.peek() == Some(b',') {
            self.at = parser.at.checked_add(1)?;
        } else {
            self.done = true;
        }
        Some(value)
    }
}

#[derive(Clone, Copy, Debug)]
struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn space(&mut self) {
        while whitespace(self.peek()) {
            self.at = self.at.saturating_add(1);
        }
    }

    fn take(&mut self, expected: u8) -> Result<(), DecodeError> {
        if self.peek() != Some(expected) {
            return Err(DecodeError::Syntax);
        }
        self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
        Ok(())
    }

    fn value(&mut self, depth: u32) -> Result<(), DecodeError> {
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => self.string(),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => Err(DecodeError::Syntax),
        }
    }

    fn object(&mut self, depth: u32) -> Result<(), DecodeError> {
        if depth >= 64 {
            return Err(DecodeError::TooDeep);
        }
        self.take(b'{')?;
        self.space();
        if self.peek() == Some(b'}') {
            return self.take(b'}');
        }
        loop {
            self.string()?;
            self.space();
            self.take(b':')?;
            self.space();
            self.value(depth.checked_add(1).ok_or(DecodeError::TooDeep)?)?;
            self.space();
            if self.peek() != Some(b',') {
                return self.take(b'}');
            }
            self.take(b',')?;
            self.space();
        }
    }

    fn array(&mut self, depth: u32) -> Result<(), DecodeError> {
        if depth >= 64 {
            return Err(DecodeError::TooDeep);
        }
        self.take(b'[')?;
        self.space();
        if self.peek() == Some(b']') {
            return self.take(b']');
        }
        loop {
            self.value(depth.checked_add(1).ok_or(DecodeError::TooDeep)?)?;
            self.space();
            if self.peek() != Some(b',') {
                return self.take(b']');
            }
            self.take(b',')?;
            self.space();
        }
    }

    fn string(&mut self) -> Result<(), DecodeError> {
        self.take(b'"')?;
        let mut segment = self.at;
        loop {
            let byte = self.peek().ok_or(DecodeError::Syntax)?;
            if byte == b'"' {
                valid_text(self.bytes.get(segment..self.at).ok_or(DecodeError::Syntax)?)?;
                return self.take(b'"');
            }
            if byte == b'\\' {
                valid_text(self.bytes.get(segment..self.at).ok_or(DecodeError::Syntax)?)?;
                self.take(b'\\')?;
                let escaped = self.peek().ok_or(DecodeError::Syntax)?;
                match escaped {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                        self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
                    }
                    b'u' => {
                        let first = hex4(self.bytes, self.at.checked_add(1).ok_or(DecodeError::Syntax)?)
                            .ok_or(DecodeError::Syntax)?;
                        self.at = self.at.checked_add(5).ok_or(DecodeError::Syntax)?;
                        if (0xD800..=0xDBFF).contains(&first) {
                            self.take(b'\\')?;
                            self.take(b'u')?;
                            let second = hex4(self.bytes, self.at).ok_or(DecodeError::Syntax)?;
                            if !(0xDC00..=0xDFFF).contains(&second) {
                                return Err(DecodeError::Syntax);
                            }
                            self.at = self.at.checked_add(4).ok_or(DecodeError::Syntax)?;
                        } else if (0xDC00..=0xDFFF).contains(&first) {
                            return Err(DecodeError::Syntax);
                        }
                    }
                    _ => return Err(DecodeError::Syntax),
                }
                segment = self.at;
            } else if byte < 0x20 {
                return Err(DecodeError::Syntax);
            } else {
                self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
            }
        }
    }

    fn literal(&mut self, literal: &[u8]) -> Result<(), DecodeError> {
        let end = self.at.checked_add(literal.len()).ok_or(DecodeError::Syntax)?;
        if self.bytes.get(self.at..end) != Some(literal) {
            return Err(DecodeError::Syntax);
        }
        self.at = end;
        Ok(())
    }

    fn number(&mut self) -> Result<(), DecodeError> {
        if self.peek() == Some(b'-') {
            self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
        }
        match self.peek() {
            Some(b'0') => self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?,
            Some(b'1'..=b'9') => {
                self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
                while digit(self.peek()) {
                    self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
                }
            }
            _ => return Err(DecodeError::Syntax),
        }
        if self.peek() == Some(b'.') {
            self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
            self.digits()?;
        }
        if exponent(self.peek()) {
            self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
            if sign(self.peek()) {
                self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
            }
            self.digits()?;
        }
        Ok(())
    }

    fn digits(&mut self) -> Result<(), DecodeError> {
        if !digit(self.peek()) {
            return Err(DecodeError::Syntax);
        }
        while digit(self.peek()) {
            self.at = self.at.checked_add(1).ok_or(DecodeError::Syntax)?;
        }
        Ok(())
    }
}

fn hex4(bytes: &[u8], start: usize) -> Option<u16> {
    let mut value = 0_u16;
    for offset in 0..4_usize {
        let digit = *bytes.get(start.checked_add(offset)?)?;
        let nibble = match digit {
            b'0'..=b'9' => u16::from(digit.checked_sub(b'0')?),
            b'a'..=b'f' => u16::from(digit.checked_sub(b'a')?).checked_add(10)?,
            b'A'..=b'F' => u16::from(digit.checked_sub(b'A')?).checked_add(10)?,
            _ => return None,
        };
        value = value.checked_mul(16)?.checked_add(nibble)?;
    }
    Some(value)
}

fn unsigned(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0_u64;
    for byte in bytes {
        if !digit(Some(*byte)) {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(byte.checked_sub(b'0')?))?;
    }
    Some(value)
}

fn signed(bytes: &[u8]) -> Option<i64> {
    if bytes.first() == Some(&b'-') {
        let magnitude = unsigned(bytes.get(1..)?)?;
        if magnitude == i64::MIN.unsigned_abs() { Some(i64::MIN) } else { i64::try_from(magnitude).ok()?.checked_neg() }
    } else {
        i64::try_from(unsigned(bytes)?).ok()
    }
}

fn whitespace(byte: Option<u8>) -> bool {
    match byte {
        Some(b' ' | b'\n' | b'\r' | b'\t') => true,
        Some(_) | None => false,
    }
}

fn digit(byte: Option<u8>) -> bool {
    match byte {
        Some(b'0'..=b'9') => true,
        Some(_) | None => false,
    }
}

fn exponent(byte: Option<u8>) -> bool {
    match byte {
        Some(b'e' | b'E') => true,
        Some(_) | None => false,
    }
}

fn sign(byte: Option<u8>) -> bool {
    match byte {
        Some(b'+' | b'-') => true,
        Some(_) | None => false,
    }
}

#[expect(clippy::disallowed_methods, reason = "a JSON string must contain valid UTF-8")]
fn valid_text(bytes: &[u8]) -> Result<(), DecodeError> {
    match core::str::from_utf8(bytes) {
        Ok(_) => Ok(()),
        Err(_) => Err(DecodeError::Syntax),
    }
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, Document};

    #[test]
    fn nested_reply_and_escaped_text() {
        let doc = Document::parse(br#"{"sessionId":"s","result":{"nodes":[{"role":{"value":"button"},"name":{"value":"a\u00e9"},"backendDOMNodeId":42}]},"id":7}"#, 512)
            .expect("valid CDP reply");
        let root = doc.root();
        assert_eq!(root.get(b"id").expect("id").u64(), Some(7));
        assert_eq!(root.get(b"sessionId").expect("session").text().expect("text").as_ref(), b"s");
        let mut nodes =
            root.get(b"result").expect("result").get(b"nodes").expect("nodes").array().expect("nodes array");
        let node = nodes.next().expect("one node");
        assert_eq!(node.get(b"backendDOMNodeId").expect("backend node").u64(), Some(42));
        assert_eq!(
            node.get(b"name").expect("name").get(b"value").expect("value").text().expect("text").as_ref(),
            "aé".as_bytes()
        );
        assert!(nodes.next().is_none());
    }

    #[test]
    fn malformed_and_over_limit() {
        assert_eq!(Document::parse(br#"{"x":01}"#, 100), Err(DecodeError::Syntax));
        assert_eq!(Document::parse(br#"{"x":1}"#, 2), Err(DecodeError::TooLong));
        assert_eq!(Document::parse(br#"{"x":"\uD800"}"#, 100), Err(DecodeError::Syntax));
    }
}
