//! Compact retained JSON (json.md, section 5.2): one text allocation and
//! fixed records pointing into it. This stores no stream or grammar state;
//! its producer supplies a bounded token sequence. `from_tokens` admits the
//! counts before allocation, `token` reads a borrowed record by index, and
//! `text` borrows that record's decoded bytes. The writer emits a document
//! whole; a `Long` has no text and cannot be written.

use alloc::boxed::Box;

use skein_lib::List;

use crate::Token;

/// The retained token and decoded text counts admitted by a document's owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub tokens: u32,
    pub text: u32,
}

/// A document refused at its retained count or record bounds.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// More retained records than the owner's token count.
    TooManyTokens,
    /// More decoded bytes than the owner's text count or offset range.
    TooMuchText,
    /// A record's text range is outside its document.
    Range,
}

/// A token's interpretation; emitted by a tokenizer or collector, read by its owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    /// An object's opening, for the decoder or writer.
    ObjectStart,
    /// An object's closing, for the decoder or writer.
    ObjectEnd,
    /// An array's opening, for the decoder or writer.
    ArrayStart,
    /// An array's closing, for the decoder or writer.
    ArrayEnd,
    /// A member's decoded key, for the decoder or writer.
    Key,
    /// A decoded string, for the decoder or writer.
    String,
    /// A validated number's source text, for the decoder or writer.
    Number,
    /// A true value, for the decoder or writer.
    True,
    /// A false value, for the decoder or writer.
    False,
    /// A null value, for the decoder or writer.
    Null,
    /// A string beyond its requested cap, for the decoder; its length alone is retained.
    Long,
}

/// One fixed record pointing into its document's text; the owner reads it by index.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Compact {
    pub kind: Kind,
    /// Byte offset in the shared text; zero for structural and `Long` records.
    pub start: u32,
    /// Decoded bytes; for `Long`, the discarded string's complete decoded length.
    pub len: u32,
}

impl Compact {
    /// The end of this record's text, checked against the offset range.
    #[must_use]
    pub fn end(&self) -> Option<u32> {
        self.start.checked_add(self.len)
    }
}

/// Retained JSON in document order, handed by a collector to its decoder.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Document {
    text: Box<[u8]>,
    tokens: Box<[Compact]>,
}

impl Document {
    /// Packs the producer's token sequence, checking both counts before allocating.
    pub fn from_tokens(tokens: &[Token], limits: &Limits) -> Result<Document, Error> {
        let Ok(count) = u32::try_from(tokens.len()) else {
            return Err(Error::TooManyTokens);
        };
        if count > limits.tokens {
            return Err(Error::TooManyTokens);
        }
        let mut size = 0_u32;
        for token in tokens {
            let bytes = token_text(token);
            let Ok(len) = u32::try_from(bytes.len()) else {
                return Err(Error::TooMuchText);
            };
            size = size.checked_add(len).ok_or(Error::TooMuchText)?;
            if size > limits.text {
                return Err(Error::TooMuchText);
            }
        }
        let exact = Limits { tokens: count, text: size };
        let mut builder = Builder::new(exact);
        for token in tokens {
            builder.push(token)?;
        }
        Ok(builder.into_document())
    }

    /// Admits owned records without copying; each ordinary record must point into `text`.
    pub fn from_parts(text: Box<[u8]>, tokens: Box<[Compact]>, limits: &Limits) -> Result<Document, Error> {
        if tokens.len() > usize::try_from(limits.tokens).expect("u32 fits usize") {
            return Err(Error::TooManyTokens);
        }
        if text.len() > usize::try_from(limits.text).expect("u32 fits usize") {
            return Err(Error::TooMuchText);
        }
        for record in &tokens {
            match record.kind {
                Kind::Long => {
                    if record.start != 0 {
                        return Err(Error::Range);
                    }
                }
                Kind::Key | Kind::String | Kind::Number => {
                    let end = record.end().ok_or(Error::Range)?;
                    if usize::try_from(end).expect("u32 fits usize") > text.len() {
                        return Err(Error::Range);
                    }
                }
                Kind::ObjectStart
                | Kind::ObjectEnd
                | Kind::ArrayStart
                | Kind::ArrayEnd
                | Kind::True
                | Kind::False
                | Kind::Null => {
                    if record.start != 0 || record.len != 0 {
                        return Err(Error::Range);
                    }
                }
            }
        }
        Ok(Document { text, tokens })
    }

    /// A borrowed record, or no record at that index.
    #[must_use]
    pub fn token(&self, index: u32) -> Option<&Compact> {
        self.tokens.get(usize::try_from(index).ok()?)
    }

    /// A record's decoded bytes; `Long` and structural records have no text.
    #[must_use]
    pub fn text(&self, record: &Compact) -> Option<&[u8]> {
        match record.kind {
            Kind::Key | Kind::String | Kind::Number => {
                let start = usize::try_from(record.start).ok()?;
                let end = usize::try_from(record.end()?).ok()?;
                self.text.get(start..end)
            }
            Kind::Long
            | Kind::ObjectStart
            | Kind::ObjectEnd
            | Kind::ArrayStart
            | Kind::ArrayEnd
            | Kind::True
            | Kind::False
            | Kind::Null => Some(&[]),
        }
    }

    /// The number of retained records.
    #[must_use]
    pub fn len(&self) -> u32 {
        u32::try_from(self.tokens.len()).expect("admitted under a u32 count")
    }

    /// Whether the retained record sequence is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// The retained decoded bytes.
    #[must_use]
    pub fn text_len(&self) -> u32 {
        u32::try_from(self.text.len()).expect("admitted under a u32 count")
    }
}

/// The document's two allocations at their admitted counts (programming-model.md, 6.3).
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    List::<Compact>::worst_case(limits.tokens)?.checked_add(u64::from(limits.text))
}

/// Reusable compact buffers owned by the selective collector.
#[derive(Debug)]
pub(crate) struct Builder {
    text: List<u8>,
    tokens: List<Compact>,
}

impl Builder {
    pub(crate) fn new(limits: Limits) -> Builder {
        Builder { text: List::with_capacity(limits.text), tokens: List::with_capacity(limits.tokens) }
    }

    pub(crate) fn push(&mut self, token: &Token) -> Result<(), Error> {
        if self.tokens.room() == 0 {
            return Err(Error::TooManyTokens);
        }
        let bytes = token_text(token);
        let Ok(len) = u32::try_from(bytes.len()) else {
            return Err(Error::TooMuchText);
        };
        if len > self.text.room() {
            return Err(Error::TooMuchText);
        }
        let kind = token_kind(token);
        let start = match kind {
            Kind::Key | Kind::String | Kind::Number => self.text.len(),
            Kind::ObjectStart
            | Kind::ObjectEnd
            | Kind::ArrayStart
            | Kind::ArrayEnd
            | Kind::True
            | Kind::False
            | Kind::Null
            | Kind::Long => 0,
        };
        for &byte in bytes {
            self.text.push(byte).expect("the text count was admitted");
        }
        self.tokens.push(Compact { kind, start, len }).expect("the record count was admitted");
        Ok(())
    }

    pub(crate) fn push_compact(&mut self, record: &Compact, bytes: &[u8]) -> Result<(), Error> {
        if self.tokens.room() == 0 {
            return Err(Error::TooManyTokens);
        }
        let len = match record.kind {
            Kind::Long => record.len,
            Kind::Key | Kind::String | Kind::Number => u32::try_from(bytes.len()).or(Err(Error::TooMuchText))?,
            Kind::ObjectStart
            | Kind::ObjectEnd
            | Kind::ArrayStart
            | Kind::ArrayEnd
            | Kind::True
            | Kind::False
            | Kind::Null => 0,
        };
        let start = match record.kind {
            Kind::Key | Kind::String | Kind::Number => {
                if len > self.text.room() {
                    return Err(Error::TooMuchText);
                }
                let start = self.text.len();
                for &byte in bytes {
                    self.text.push(byte).expect("admitted copied text");
                }
                start
            }
            Kind::ObjectStart
            | Kind::ObjectEnd
            | Kind::ArrayStart
            | Kind::ArrayEnd
            | Kind::True
            | Kind::False
            | Kind::Null
            | Kind::Long => 0,
        };
        self.tokens.push(Compact { kind: record.kind, start, len }).expect("admitted copied record");
        Ok(())
    }

    pub(crate) fn truncate(&mut self, records: u32, text: u32) {
        assert!(records <= self.tokens.len() && text <= self.text.len(), "truncate only removes stored data");
        self.tokens.truncate(records);
        self.text.truncate(text);
    }

    /// Compacts one selected range to an earlier position, without allocating.
    pub(crate) fn copy_within(&mut self, start: u32, end: u32, records: u32, text: u32) -> (u32, u32) {
        assert!(records <= start && start <= end && end <= self.tokens.len(), "selection moves only toward the prefix");
        let mut next_record = records;
        let mut next_text = text;
        for index in start..end {
            let mut record = *self.tokens.get(index).expect("selected source record");
            match record.kind {
                Kind::Key | Kind::String | Kind::Number => {
                    assert!(next_text <= record.start, "selected text moves only toward the prefix");
                    let source = record.start;
                    record.start = next_text;
                    for offset in 0..record.len {
                        let source_at = source.checked_add(offset).expect("admitted source text range");
                        let byte = *self.text.get(source_at).expect("selected source byte");
                        *self.text.get_mut(next_text).expect("destination remains within stored text") = byte;
                        next_text = next_text.checked_add(1).expect("selected text fits the existing buffer");
                    }
                }
                Kind::ObjectStart
                | Kind::ObjectEnd
                | Kind::ArrayStart
                | Kind::ArrayEnd
                | Kind::True
                | Kind::False
                | Kind::Null
                | Kind::Long => {}
            }
            *self.tokens.get_mut(next_record).expect("destination remains within stored records") = record;
            next_record = next_record.checked_add(1).expect("selected records fit the existing buffer");
        }
        (next_record, next_text)
    }

    pub(crate) fn len(&self) -> u32 {
        self.tokens.len()
    }

    pub(crate) fn text_len(&self) -> u32 {
        self.text.len()
    }

    pub(crate) fn clear(&mut self) {
        self.tokens.clear();
        self.text.clear();
    }

    pub(crate) fn document(&self) -> Document {
        Document { text: self.text.to_boxed(), tokens: self.tokens.to_boxed() }
    }

    pub(crate) fn token(&self, index: u32) -> Option<&Compact> {
        self.tokens.get(index)
    }

    pub(crate) fn text(&self, record: &Compact) -> &[u8] {
        let start = usize::try_from(record.start).expect("u32 fits usize");
        let end = usize::try_from(record.end().expect("the builder checked each text append")).expect("u32 fits usize");
        self.text.as_slice().get(start..end).expect("the builder's own text offsets")
    }

    pub(crate) fn push_long(&mut self, length: u64) -> Result<(), Error> {
        if self.tokens.room() == 0 {
            return Err(Error::TooManyTokens);
        }
        let Ok(len) = u32::try_from(length) else {
            return Err(Error::TooMuchText);
        };
        self.tokens.push(Compact { kind: Kind::Long, start: 0, len }).expect("the record count was admitted");
        Ok(())
    }

    fn into_document(self) -> Document {
        Document { text: self.text.into_boxed(), tokens: self.tokens.into_boxed() }
    }
}

fn token_text(token: &Token) -> &[u8] {
    match token {
        Token::Key(bytes) | Token::String(bytes) | Token::Number(bytes) => bytes,
        Token::ObjectStart
        | Token::ObjectEnd
        | Token::ArrayStart
        | Token::ArrayEnd
        | Token::True
        | Token::False
        | Token::Null => &[],
    }
}

fn token_kind(token: &Token) -> Kind {
    match token {
        Token::ObjectStart => Kind::ObjectStart,
        Token::ObjectEnd => Kind::ObjectEnd,
        Token::ArrayStart => Kind::ArrayStart,
        Token::ArrayEnd => Kind::ArrayEnd,
        Token::Key(_) => Kind::Key,
        Token::String(_) => Kind::String,
        Token::Number(_) => Kind::Number,
        Token::True => Kind::True,
        Token::False => Kind::False,
        Token::Null => Kind::Null,
    }
}
