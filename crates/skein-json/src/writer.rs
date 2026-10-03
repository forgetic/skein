//! The writer (json.md, 4): a document encoded from the caller's own calls,
//! made twice, first to measure it, then to write it, escaped, into
//! `Writer::new(len)` (programming-model.md, 8).
//!
//! There is no value to build: the caller writes one function that makes
//! the calls for its own data, and runs it once on each pass.
//!
//! ```text
//! fn encode(json: &mut Encoder, call: &Call) {
//!     json.object_start();
//!     json.key(b"name");
//!     json.string(&call.name);
//!     json.key(b"max_tokens");
//!     json.unsigned(call.max_tokens);
//!     json.object_end();
//! }
//!
//! let mut measure = Encoder::measure(&limits);
//! encode(&mut measure, &call);
//! let len = measure.measured()?;    // refused here, before anything is allocated
//! let mut write = Encoder::write(len, &limits);
//! encode(&mut write, &call);
//! let body = write.finish();        // exactly `len` bytes
//! ```
//!
//! What a caller's data can get wrong is refused by the measuring pass, as a
//! [`Refusal`]: text that is not UTF-8, a number's text that is not a
//! number, a document past the limits. What only its code can get wrong (a
//! key outside an object, an end that closes nothing, a second value, a
//! writing pass that differs from its measuring pass) is a bug, asserted.
//!
//! The document is compact, with no whitespace. A string escapes `"`, `\`
//! and the control characters, the common ones in their short forms and the
//! rest as `\u00XX`; every other byte, `/` and non-ASCII included, is
//! written as it is.

use alloc::boxed::Box;

use skein_lib::{Decimal, Stack, Writer};

use crate::Token;
use crate::number;
use crate::utf8::Utf8;

/// The writer's limits (programming-model.md, 7), the same for both passes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// How deep objects and arrays may nest. A deeper document is refused
    /// with [`Refusal::TooDeep`].
    pub depth: u32,
    /// The longest document, in bytes. A longer one is refused with
    /// [`Refusal::TooLong`] once measured, before anything is allocated for
    /// it.
    pub length: u32,
}

/// The most memory an encoder holds under `limits`, in bytes
/// (programming-model.md, 6.3), or `None` if it does not fit a `u64`: its
/// stack of open objects and arrays, and on the writing pass the document,
/// until [`Encoder::finish`] hands it over.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    Stack::<Container>::worst_case(limits.depth)?.checked_add(u64::from(limits.length))
}

/// Why the measuring pass refused a document.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// Longer than [`Limits::length`].
    TooLong,
    /// Nested deeper than [`Limits::depth`].
    TooDeep,
    /// A string or key whose bytes are not UTF-8.
    Text,
    /// A number whose text is not a JSON number.
    Number,
}

/// One pass over a document: measuring it, or writing it.
#[derive(Debug)]
pub struct Encoder {
    sink: Sink,
    /// The objects and arrays open, the innermost on top.
    open: Stack<Container>,
    /// What may come next.
    position: Position,
}

/// Where a pass puts its bytes.
#[derive(Debug)]
enum Sink {
    /// The measuring pass: `len` bytes so far, saturating, under `limit`.
    Measuring { len: u64, limit: u32 },
    /// The measuring pass, refused: what follows is neither measured nor
    /// checked.
    Refused(Refusal),
    /// The writing pass.
    Writing(Writer),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Container {
    Object,
    Array,
}

/// What may come next, between two calls.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Position {
    /// Nothing is written: the document's value comes next.
    Start,
    /// The innermost container was just started: its first member or
    /// element, or its end.
    First,
    /// A member or an element was written: a comma and the next, or the
    /// end.
    Later,
    /// A key was written: its value comes next.
    Value,
    /// The document's value is whole: nothing comes next.
    Whole,
}

impl Encoder {
    /// The measuring pass, under `limits`.
    #[must_use]
    pub fn measure(limits: &Limits) -> Encoder {
        Encoder {
            sink: Sink::Measuring { len: 0, limit: limits.length },
            open: Stack::with_capacity(limits.depth),
            position: Position::Start,
        }
    }

    /// The writing pass of a document the measuring pass measured at `len`
    /// bytes, under the same `limits`.
    #[must_use]
    pub fn write(len: u32, limits: &Limits) -> Encoder {
        assert!(len <= limits.length, "a document is written at the length it was measured");
        Encoder {
            sink: Sink::Writing(Writer::new(usize::try_from(len).expect("a u32 fits a usize"))),
            open: Stack::with_capacity(limits.depth),
            position: Position::Start,
        }
    }

    /// `{`
    pub fn object_start(&mut self) {
        self.start(Container::Object);
    }

    /// `}`
    pub fn object_end(&mut self) {
        self.end(Container::Object);
    }

    /// `[`
    pub fn array_start(&mut self) {
        self.start(Container::Array);
    }

    /// `]`
    pub fn array_end(&mut self) {
        self.end(Container::Array);
    }

    /// A member's key, its text unescaped. Its value is the next call.
    pub fn key(&mut self, key: &[u8]) {
        if self.is_refused() {
            return;
        }
        let comma = match self.position {
            Position::First => false,
            Position::Later => true,
            Position::Start | Position::Value | Position::Whole => {
                unreachable!("a key starts an object's member")
            }
        };
        assert!(self.open.top() == Some(&Container::Object), "a key is an object's");
        if comma {
            self.put(b",");
        }
        self.put_string(key);
        self.put(b":");
        self.position = Position::Value;
    }

    /// A string, its text unescaped.
    pub fn string(&mut self, text: &[u8]) {
        if !self.before_value() {
            return;
        }
        self.put_string(text);
        self.after_value();
    }

    /// A number, its text as JSON's grammar has it (RFC 8259, section 6),
    /// such as a `Token::Number`'s.
    pub fn number(&mut self, text: &[u8]) {
        if !self.before_value() {
            return;
        }
        if !number::is_number(text) {
            self.refuse(Refusal::Number);
            return;
        }
        self.put(text);
        self.after_value();
    }

    /// A count, in decimal digits.
    pub fn unsigned(&mut self, n: u64) {
        if !self.before_value() {
            return;
        }
        self.put(Decimal::of(n).as_bytes());
        self.after_value();
    }

    /// An integer, in decimal digits after a `-` if it is negative.
    pub fn signed(&mut self, n: i64) {
        if !self.before_value() {
            return;
        }
        if n < 0 {
            self.put(b"-");
        }
        self.put(Decimal::of(n.unsigned_abs()).as_bytes());
        self.after_value();
    }

    /// `true` or `false`.
    pub fn boolean(&mut self, value: bool) {
        if !self.before_value() {
            return;
        }
        self.put(if value { b"true" } else { b"false" });
        self.after_value();
    }

    /// `null`
    pub fn null(&mut self) {
        if !self.before_value() {
            return;
        }
        self.put(b"null");
        self.after_value();
    }

    /// A token, as the tokenizer reads it: what one reads, the other writes
    /// back.
    pub fn token(&mut self, token: &Token) {
        match token {
            Token::ObjectStart => self.object_start(),
            Token::ObjectEnd => self.object_end(),
            Token::ArrayStart => self.array_start(),
            Token::ArrayEnd => self.array_end(),
            Token::Key(key) => self.key(key),
            Token::String(text) => self.string(text),
            Token::Number(text) => self.number(text),
            Token::True => self.boolean(true),
            Token::False => self.boolean(false),
            Token::Null => self.null(),
        }
    }

    /// Ends the measuring pass: the document's length, or why it is
    /// refused, the first refusal met, and its length last.
    pub fn measured(self) -> Result<u32, Refusal> {
        match self.sink {
            Sink::Measuring { len, limit } => {
                assert!(self.position == Position::Whole, "a document is measured whole");
                match u32::try_from(len) {
                    Ok(len) if len <= limit => Ok(len),
                    Ok(_) | Err(_) => Err(Refusal::TooLong),
                }
            }
            Sink::Refused(refusal) => Err(refusal),
            Sink::Writing(_) => unreachable!("only the measuring pass is measured"),
        }
    }

    /// Ends the writing pass: the document, in a box of exactly the length
    /// measured.
    #[must_use]
    pub fn finish(self) -> Box<[u8]> {
        match self.sink {
            Sink::Writing(writer) => {
                assert!(self.position == Position::Whole, "a document is written whole");
                writer.finish()
            }
            Sink::Measuring { .. } | Sink::Refused(_) => unreachable!("only the writing pass is finished"),
        }
    }

    fn is_refused(&self) -> bool {
        match self.sink {
            Sink::Refused(_) => true,
            Sink::Measuring { .. } | Sink::Writing(_) => false,
        }
    }

    /// Readies a value, with the comma before it if one is due: `false`
    /// on a refused measuring pass, which goes no further.
    fn before_value(&mut self) -> bool {
        if self.is_refused() {
            return false;
        }
        let comma = match self.position {
            Position::Start | Position::Value => false,
            Position::First | Position::Later => {
                assert!(self.open.top() == Some(&Container::Array), "a value in an object follows its key");
                self.position == Position::Later
            }
            Position::Whole => unreachable!("a document holds one value"),
        };
        if comma {
            self.put(b",");
        }
        true
    }

    fn after_value(&mut self) {
        self.position = if self.open.is_empty() { Position::Whole } else { Position::Later };
    }

    fn start(&mut self, container: Container) {
        if !self.before_value() {
            return;
        }
        if self.open.push(container).is_err() {
            self.refuse(Refusal::TooDeep);
            return;
        }
        self.put(match container {
            Container::Object => b"{",
            Container::Array => b"[",
        });
        self.position = Position::First;
    }

    fn end(&mut self, container: Container) {
        if self.is_refused() {
            return;
        }
        match self.position {
            Position::First | Position::Later => {}
            Position::Start | Position::Value | Position::Whole => {
                unreachable!("an end follows its container's start or a member")
            }
        }
        assert!(self.open.top() == Some(&container), "an end is the innermost container's");
        let _: Option<Container> = self.open.pop();
        self.put(match container {
            Container::Object => b"}",
            Container::Array => b"]",
        });
        self.after_value();
    }

    /// Refuses the document being measured. The writing pass of a document
    /// measured first is never refused.
    fn refuse(&mut self, refusal: Refusal) {
        match self.sink {
            Sink::Measuring { .. } => self.sink = Sink::Refused(refusal),
            Sink::Refused(_) => {}
            Sink::Writing(_) => unreachable!("a document measured first is never refused"),
        }
    }

    fn put(&mut self, bytes: &[u8]) {
        match &mut self.sink {
            Sink::Measuring { len, .. } => *len = len.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
            Sink::Refused(_) => {}
            Sink::Writing(writer) => writer.put(bytes).expect("a document measured first fits"),
        }
    }

    /// A string or a key, quoted and escaped, or refused if not UTF-8.
    fn put_string(&mut self, text: &[u8]) {
        let Some(escaped) = escaped_len(text) else {
            self.refuse(Refusal::Text);
            return;
        };
        match &mut self.sink {
            Sink::Measuring { len, .. } => *len = len.saturating_add(escaped),
            Sink::Refused(_) => {}
            Sink::Writing(writer) => write_escaped(writer, text),
        }
    }
}

/// How a byte of a string's text is written.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Escaped {
    /// As it is.
    Plain,
    /// A backslash and this letter.
    Short(u8),
    /// `\u00` and two hex digits.
    Unicode,
}

fn escaped(byte: u8) -> Escaped {
    match byte {
        b'"' => Escaped::Short(b'"'),
        b'\\' => Escaped::Short(b'\\'),
        0x08 => Escaped::Short(b'b'),
        0x0C => Escaped::Short(b'f'),
        b'\n' => Escaped::Short(b'n'),
        b'\r' => Escaped::Short(b'r'),
        b'\t' => Escaped::Short(b't'),
        0x00..=0x1F => Escaped::Unicode,
        _ => Escaped::Plain,
    }
}

/// The length of `text` quoted and escaped, or `None` if it is not UTF-8.
fn escaped_len(text: &[u8]) -> Option<u64> {
    let mut utf8 = Utf8::Between;
    // The quotes.
    let mut len: u64 = 2;
    for &byte in text {
        utf8 = utf8.next(byte)?;
        let width: u64 = match escaped(byte) {
            Escaped::Plain => 1,
            Escaped::Short(_) => 2,
            Escaped::Unicode => 6,
        };
        len = len.saturating_add(width);
    }
    match utf8 {
        Utf8::Between => Some(len),
        Utf8::Within { .. } => None,
    }
}

/// Writes `text` quoted and escaped, the bytes between escapes in runs.
fn write_escaped(writer: &mut Writer, text: &[u8]) {
    put(writer, b"\"");
    // Where the run of bytes written as they are begins.
    let mut run = 0;
    for (at, &byte) in text.iter().enumerate() {
        let spelled: &[u8] = match escaped(byte) {
            Escaped::Plain => continue,
            Escaped::Short(letter) => &[b'\\', letter],
            Escaped::Unicode => &[b'\\', b'u', b'0', b'0', hex(byte.wrapping_shr(4)), hex(byte & 0x0F)],
        };
        put(writer, text.get(run..at).expect("the run ends at this byte"));
        put(writer, spelled);
        run = at.checked_add(1).expect("within the text");
    }
    put(writer, text.get(run..).expect("the run is within the text"));
    put(writer, b"\"");
}

/// The hex digit of a value below sixteen.
fn hex(value: u8) -> u8 {
    match value {
        0..=9 => b'0'.wrapping_add(value),
        _ => b'a'.wrapping_add(value).wrapping_sub(10),
    }
}

fn put(writer: &mut Writer, bytes: &[u8]) {
    writer.put(bytes).expect("a document measured first fits");
}
