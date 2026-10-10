//! An event stream's lines and fields (WHATWG HTML, 9.2.6): read a byte at
//! a time from what the line scans deliver, each line's field interpreted
//! as its bytes come, an event dispatched at each blank line.
//!
//! - **A line ends at LF, at CRLF, or at CR alone.** A CR ends its line at
//!   once; an LF right after it is the CR's pair, and ends nothing. A line
//!   scan stops at the CR, so the pair comes alone, in the next delivery.
//! - **A field's value goes where it belongs as it comes:** `data` into the
//!   data face, `event` into its type, `id` into a value held until its
//!   line ends (one with a NUL is ignored), `retry` into a number. A line
//!   that begins with a colon is a comment, and one whose name is no
//!   field's is ignored.
//! - **Bounds:** a line holds at most [`Limits::line`] bytes before its
//!   ending, an event at most [`Limits::event`] bytes read from the end of
//!   the last, and an event type or an id at most [`Limits::field`].
//! - **One leading byte order mark** is skipped, as UTF-8 decoding does. The
//!   reader does not otherwise check UTF-8: an event's data goes to a
//!   decoder that does, and a type or an id is compared as bytes.

use skein_lib::{List, bytes};

use super::{Dispatch, Error, Limits};

/// What a stream's lines so far have built: the event being read, and the
/// stream's own state across events.
#[derive(Debug)]
pub(super) struct Lines {
    /// How much of a byte order mark the stream began with, until it is
    /// skipped or turns out not to be one.
    bom: Bom,
    /// Where the line being read is.
    line: Line,
    /// The bytes of the line being read so far, its ending not counted.
    length: u32,
    /// Whether the last byte was a CR that ended a line: an LF now is its
    /// pair.
    after_cr: bool,
    /// The bytes read since the last blank line, endings included.
    size: u32,
    /// Whether this event has begun its first data line.
    data: bool,
    /// The event's type, set by its last `event` field.
    name: List<u8>,
    /// An `id` field's value, until its line ends.
    id_value: List<u8>,
    /// The last event ID buffer, set by each `id` field.
    id: List<u8>,
    /// The last event ID: the buffer, as it was at the last blank line.
    last_id: List<u8>,
    /// The reconnection time a `retry` field set, in milliseconds.
    retry: Option<u64>,
}

/// What a byte comes to.
#[derive(Debug)]
pub(super) enum Step {
    /// Nothing yet: the reader reads on.
    More,
    /// The first data line opened an event.
    Opened,
    /// One decoded data byte, with joined lines separated by LF.
    Data(u8),
    /// A blank line completed the event and its metadata.
    Dispatched(Dispatch),
    /// A wire or field bound was passed.
    Fail(Error),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Bom {
    /// This many bytes of it matched so far.
    Matching(u8),
    /// Skipped, or not there.
    Over,
}

const BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// Where a line is.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Line {
    /// Nothing of it read yet.
    Start,
    /// Its first bytes, a field's name, up to the longest a field has.
    Name { bytes: [u8; 5], len: u8 },
    /// A comment, or a field no reader knows: the rest of it is ignored.
    Ignored,
    /// The colon after a field's name: a space next is skipped.
    Colon(Field),
    /// A field's value.
    Value(Field),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Field {
    Data,
    Event,
    Id,
    Retry(Retry),
}

/// A `retry` field's value so far: only ASCII digits set the reconnection
/// time.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Retry {
    /// No byte yet: an empty value sets nothing.
    Empty,
    /// The digits so far, as a number.
    Digits(u64),
    /// A byte that is not a digit came, or the number passed a `u64`.
    Ignored,
}

impl Lines {
    pub(super) fn new(limits: &Limits) -> Lines {
        Lines {
            bom: Bom::Matching(0),
            line: Line::Start,
            length: 0,
            after_cr: false,
            size: 0,
            data: false,
            name: List::with_capacity(limits.field),
            id_value: List::with_capacity(limits.field),
            id: List::with_capacity(limits.field),
            last_id: List::with_capacity(limits.field),
            retry: None,
        }
    }

    pub(super) fn retry(&self) -> Option<u64> {
        self.retry
    }

    pub(super) fn last_id(&self) -> &[u8] {
        self.last_id.as_slice()
    }

    /// Lets go of the event being read: the stream ended or failed, or the
    /// reader was closed.
    pub(super) fn clear(&mut self) {
        self.data = false;
        self.name.clear();
        self.id_value.clear();
    }

    /// A byte from the stream: the byte order mark first, if it begins
    /// with one.
    pub(super) fn byte(&mut self, limits: &Limits, byte: u8) -> Step {
        let matched = match self.bom {
            Bom::Over => return self.stream_byte(limits, byte),
            Bom::Matching(matched) => matched,
        };
        let next = matched.saturating_add(1);
        if BOM.get(usize::from(matched)) == Some(&byte) {
            self.bom = if usize::from(next) == BOM.len() { Bom::Over } else { Bom::Matching(next) };
            return Step::More;
        }
        // Not a byte order mark: what matched of it is the stream's, none of
        // it a line's ending.
        self.bom = Bom::Over;
        for &earlier in BOM.get(..usize::from(matched)).expect("what matched") {
            match self.stream_byte(limits, earlier) {
                Step::More => {}
                Step::Fail(error) => return Step::Fail(error),
                Step::Opened | Step::Data(_) | Step::Dispatched(_) => {
                    unreachable!("a byte order mark begins no data line")
                }
            }
        }
        self.stream_byte(limits, byte)
    }

    /// A byte of the stream's lines.
    fn stream_byte(&mut self, limits: &Limits, byte: u8) -> Step {
        let paired = self.after_cr && byte == b'\n';
        self.after_cr = false;
        self.size = match self.size.checked_add(1) {
            Some(size) => size,
            None => return Step::Fail(Error::EventTooLong),
        };
        if self.size > limits.event {
            return Step::Fail(Error::EventTooLong);
        }
        if paired {
            return Step::More;
        }
        match byte {
            b'\r' => {
                self.after_cr = true;
                self.end()
            }
            b'\n' => self.end(),
            _ => {
                self.length = match self.length.checked_add(1) {
                    Some(length) => length,
                    None => return Step::Fail(Error::LineTooLong),
                };
                if self.length > limits.line {
                    return Step::Fail(Error::LineTooLong);
                }
                self.content(limits, byte)
            }
        }
    }

    /// A byte of a line's content; data goes to the reader's intake.
    fn content(&mut self, limits: &Limits, byte: u8) -> Step {
        self.line = match self.line {
            Line::Start if byte == b':' => Line::Ignored,
            Line::Start => Line::Name { bytes: [byte, 0, 0, 0, 0], len: 1 },
            Line::Name { bytes, len } if byte == b':' => match field(name(&bytes, len)) {
                Some(field) => {
                    self.line = Line::Colon(field);
                    return self.begin(field);
                }
                None => Line::Ignored,
            },
            Line::Name { mut bytes, len } => match bytes.get_mut(usize::from(len)) {
                Some(slot) => {
                    *slot = byte;
                    Line::Name { bytes, len: len.saturating_add(1) }
                }
                None => Line::Ignored,
            },
            Line::Ignored => Line::Ignored,
            Line::Colon(field) if byte == b' ' => Line::Value(field),
            Line::Colon(field) | Line::Value(field) => match field {
                Field::Data => {
                    self.line = Line::Value(field);
                    return Step::Data(byte);
                }
                Field::Event | Field::Id | Field::Retry(_) => match self.value(limits, field, byte) {
                    Ok(field) => Line::Value(field),
                    Err(error) => return Step::Fail(error),
                },
            },
        };
        Step::More
    }

    /// A field begins: data starts its face or joins the next line with LF.
    fn begin(&mut self, field: Field) -> Step {
        match field {
            Field::Event => self.name.clear(),
            Field::Id => self.id_value.clear(),
            Field::Data => {
                if self.data {
                    return Step::Data(b'\n');
                }
                self.data = true;
                return Step::Opened;
            }
            Field::Retry(_) => {}
        }
        Step::More
    }

    /// A byte of `field`'s value.
    fn value(&mut self, limits: &Limits, field: Field, byte: u8) -> Result<Field, Error> {
        match field {
            Field::Data => unreachable!("data bytes go to the reader's intake"),
            Field::Event => {
                if self.name.len() >= limits.field {
                    return Err(Error::FieldTooLong);
                }
                self.name.push(byte).expect("within the field limit");
            }
            Field::Id => {
                if self.id_value.len() >= limits.field {
                    return Err(Error::FieldTooLong);
                }
                self.id_value.push(byte).expect("within the field limit");
            }
            Field::Retry(retry) => return Ok(Field::Retry(digit(retry, byte))),
        }
        Ok(field)
    }

    /// The end of a line: its field takes effect, or, if it was blank, the
    /// event is dispatched.
    fn end(&mut self) -> Step {
        let line = self.line;
        self.line = Line::Start;
        self.length = 0;
        match line {
            Line::Start => return self.dispatch(),
            Line::Ignored => {}
            // A name with no colon: the field, with an empty value.
            Line::Name { bytes, len } => {
                if let Some(field) = field(name(&bytes, len)) {
                    let action = self.begin(field);
                    self.apply(field);
                    return action;
                }
            }
            Line::Colon(field) | Line::Value(field) => self.apply(field),
        }
        Step::More
    }

    /// A field whose line ended takes effect.
    fn apply(&mut self, field: Field) {
        match field {
            // Values were streamed as data or kept as the type.
            Field::Data | Field::Event => {}
            Field::Id => {
                if self.id_value.as_slice().contains(&0) {
                    return;
                }
                copy(&self.id_value, &mut self.id);
            }
            Field::Retry(retry) => match retry {
                Retry::Digits(value) => self.retry = Some(value),
                Retry::Empty | Retry::Ignored => {}
            },
        }
    }

    /// A blank line: the event as it stands is dispatched, if it has data
    /// (WHATWG HTML, 9.2.6, "dispatch the event").
    fn dispatch(&mut self) -> Step {
        self.size = 0;
        copy(&self.id, &mut self.last_id);
        if !self.data {
            self.name.clear();
            return Step::More;
        }
        let name = if self.name.is_empty() { bytes::copy_of(b"message") } else { self.name.to_boxed() };
        let id = self.last_id.to_boxed();
        self.data = false;
        self.name.clear();
        Step::Dispatched(Dispatch { name, id })
    }
}

/// The name a line began with.
fn name(bytes: &[u8; 5], len: u8) -> &[u8] {
    bytes.get(..usize::from(len)).expect("at most five bytes")
}

/// The field a name names, if any.
fn field(name: &[u8]) -> Option<Field> {
    match name {
        b"data" => Some(Field::Data),
        b"event" => Some(Field::Event),
        b"id" => Some(Field::Id),
        b"retry" => Some(Field::Retry(Retry::Empty)),
        _ => None,
    }
}

/// A `retry` value after one more byte.
fn digit(retry: Retry, byte: u8) -> Retry {
    let value = match retry {
        Retry::Empty => 0,
        Retry::Digits(value) => value,
        Retry::Ignored => return Retry::Ignored,
    };
    if !byte.is_ascii_digit() {
        return Retry::Ignored;
    }
    let digit = u64::from(byte.saturating_sub(b'0'));
    match value.checked_mul(10) {
        Some(value) => match value.checked_add(digit) {
            Some(value) => Retry::Digits(value),
            None => Retry::Ignored,
        },
        None => Retry::Ignored,
    }
}

/// `from`'s bytes into `to`, in place of its own: two lists of the same
/// capacity.
fn copy(from: &List<u8>, to: &mut List<u8>) {
    to.clear();
    for &byte in from {
        to.push(byte).expect("lists of the same capacity");
    }
}
