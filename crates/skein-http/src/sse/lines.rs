//! An event stream's lines and fields (WHATWG HTML, 9.2.6): read a byte at
//! a time from what the scans deliver, each line's field interpreted as
//! its bytes come, an event dispatched at each blank line.
//!
//! - **A line ends at LF, at CRLF, or at CR alone.** A CR ends its line at
//!   once; an LF right after it is the CR's pair, and ends nothing.
//! - **A field's value goes where it belongs as it comes:** `data` into the
//!   event's data, `event` into its type, `id` into a value held until its
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

use super::{Error, Limits, Message};

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
    /// How the last line that told ended: what the reader scans to next.
    ending: Ending,
    /// The bytes read since the last blank line, endings included.
    size: u32,
    /// The event's data: each `data` value and an LF after it.
    data: List<u8>,
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
    /// A blank line dispatched an event.
    Message(Message),
    Fail(Error),
}

/// How a line ended, as far as the next scan's delimiter goes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Ending {
    /// At an LF, alone or after a CR: scan to the next LF.
    Lf,
    /// At a CR with no LF after it: scan to the next CR.
    Cr,
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
            ending: Ending::Lf,
            size: 0,
            data: List::with_capacity(limits.event),
            name: List::with_capacity(limits.field),
            id_value: List::with_capacity(limits.field),
            id: List::with_capacity(limits.field),
            last_id: List::with_capacity(limits.field),
            retry: None,
        }
    }

    pub(super) fn ending(&self) -> Ending {
        self.ending
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
        self.data.clear();
        self.name.clear();
        self.id_value.clear();
    }

    /// Reads `piece` from `at` until an event is dispatched, a limit is
    /// passed, or it runs out: what it came to, and where it stopped.
    pub(super) fn read(&mut self, limits: &Limits, piece: &[u8], at: usize) -> (Step, usize) {
        for (offset, &byte) in piece.iter().enumerate().skip(at) {
            let step = self.byte(limits, byte);
            match step {
                Step::More => {}
                Step::Message(_) | Step::Fail(_) => return (step, offset.saturating_add(1)),
            }
        }
        (Step::More, piece.len())
    }

    /// A byte from the stream: the byte order mark first, if it begins
    /// with one.
    fn byte(&mut self, limits: &Limits, byte: u8) -> Step {
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
                Step::Message(_) => unreachable!("a byte order mark's bytes end no line"),
            }
        }
        self.stream_byte(limits, byte)
    }

    /// A byte of the stream's lines.
    fn stream_byte(&mut self, limits: &Limits, byte: u8) -> Step {
        let paired = self.after_cr && byte == b'\n';
        if self.after_cr && !paired {
            // The CR before it ended its line alone.
            self.ending = Ending::Cr;
        }
        self.after_cr = false;
        self.size = self.size.saturating_add(1);
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
            b'\n' => {
                self.ending = Ending::Lf;
                self.end()
            }
            _ => {
                self.length = self.length.saturating_add(1);
                if self.length > limits.line {
                    return Step::Fail(Error::LineTooLong);
                }
                self.content(limits, byte)
            }
        }
    }

    /// A byte of a line's content.
    fn content(&mut self, limits: &Limits, byte: u8) -> Step {
        self.line = match self.line {
            Line::Start if byte == b':' => Line::Ignored,
            Line::Start => Line::Name { bytes: [byte, 0, 0, 0, 0], len: 1 },
            Line::Name { bytes, len } if byte == b':' => match field(name(&bytes, len)) {
                Some(field) => self.colon(field),
                None => Line::Ignored,
            },
            Line::Name { mut bytes, len } => match bytes.get_mut(usize::from(len)) {
                Some(slot) => {
                    *slot = byte;
                    Line::Name { bytes, len: len.saturating_add(1) }
                }
                // Longer than any field's name.
                None => Line::Ignored,
            },
            Line::Ignored => Line::Ignored,
            Line::Colon(field) if byte == b' ' => Line::Value(field),
            Line::Colon(field) | Line::Value(field) => match self.value(limits, field, byte) {
                Ok(field) => Line::Value(field),
                Err(error) => return Step::Fail(error),
            },
        };
        Step::More
    }

    /// The colon after `field`'s name: its value begins.
    fn colon(&mut self, field: Field) -> Line {
        self.begin(field);
        Line::Colon(field)
    }

    /// `field`'s value begins: one that replaces what it sets is emptied.
    fn begin(&mut self, field: Field) {
        match field {
            Field::Event => self.name.clear(),
            Field::Id => self.id_value.clear(),
            Field::Data | Field::Retry(_) => {}
        }
    }

    /// A byte of `field`'s value.
    fn value(&mut self, limits: &Limits, field: Field, byte: u8) -> Result<Field, Error> {
        match field {
            Field::Data => self.data.push(byte).expect("an event's data within its size"),
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
                    self.begin(field);
                    self.apply(field);
                }
            }
            Line::Colon(field) | Line::Value(field) => self.apply(field),
        }
        Step::More
    }

    /// A field whose line ended takes effect.
    fn apply(&mut self, field: Field) {
        match field {
            Field::Data => self.data.push(b'\n').expect("an event's data within its size"),
            // Its value went into the type as it came.
            Field::Event => {}
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
        if self.data.is_empty() {
            self.name.clear();
            return Step::More;
        }
        let data = self.data.as_slice();
        let data = bytes::copy_of(data.get(..data.len().saturating_sub(1)).expect("an LF ends the data"));
        let name = if self.name.is_empty() { bytes::copy_of(b"message") } else { self.name.to_boxed() };
        let id = self.last_id.to_boxed();
        self.data.clear();
        self.name.clear();
        Step::Message(Message { name, data, id })
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
