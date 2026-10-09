//! The tokenizer (json.md, 3): a step machine that reads one JSON document
//! (RFC 8259) from the stream below, by demand, and hands it to the side
//! above one [`Token`] at a time, each when the side above asks for it.
//!
//! # Its two sides
//!
//! Below, a `lib::stream` (lib.md, 7) carrying one document, then its end.
//! The tokenizer demands only what the token it is reading needs, never
//! past the document: one byte at a time between tokens and within a
//! number, the rest of a literal after its first letter, and a string's
//! text in scans to the next quote of at most [`Limits::chunk`] bytes. It
//! sends nothing down, so it asks for no room. The side below meets the
//! contract of a stream (lib.md, 7); a scan that meets no quote within its
//! maximum is one piece of a long string, and the tokenizer reads on.
//!
//! Above, the service's decoder ([`Request`] down, [`Event`] up):
//!
//! - **`Next` demands one token.** Exactly one event answers it: the
//!   next `Token`, or the document's outcome, `Done` or `Failed`, after
//!   which no token follows. One `Next` at a time, and none after the
//!   outcome or the close: the side above's bug otherwise, asserted.
//! - **`Text(max)` keeps a string only when its decoded text fits `max`; a
//!   longer string is checked through its end and answered with `Long`.
//! - **`Skip` scans one value**, checking its grammar and text while keeping
//!   neither. `Skipped` reports the bytes delivered for that value.
//! - **`Close` ends the tokenizer in any state.** It withdraws what it
//!   demanded below, drops a `Next` not yet answered, and answers with
//!   `Closed`, its one terminal event. It does not close the stream below,
//!   which is its owner's to close.
//!
//! # Bounds
//!
//! Nesting is held in a `lib::Stack` of [`Limits::depth`], never in
//! recursion (programming-model.md, 8). The string or number being read is
//! held in one buffer of the longer of [`Limits::string`] and
//! [`Limits::number`], allocated with the tokenizer, and goes up in a box of
//! exactly its length. Every byte delivered counts against
//! [`Limits::length`], so no peer keeps the tokenizer reading without end,
//! whitespace included. Past any limit, the document fails. Each entry point
//! emits at most [`UP_MAX_OUT`] or [`DOWN_MAX_OUT`]; [`worst_case`] is what
//! a tokenizer holds; [`largest_demand`] is what whoever stacks it checks
//! against the cap of the side below at startup.

use core::mem;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Env, List, Queue, Stack};

use crate::Token;
use crate::number::{After, Number};
use crate::string::{self, Buffer, Piece, Retention, Text};

/// The tokenizer's limits (programming-model.md, 7): the same for every
/// step and for [`Tokenizer::new`], which allocates by them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// How deep objects and arrays may nest. A document nested deeper fails
    /// with [`Error::TooDeep`] at the first container past it.
    pub depth: u32,
    /// The longest retained string or key, in bytes of UTF-8 once unescaped. A longer
    /// one fails with [`Error::StringTooLong`].
    pub string: u32,
    /// The longest number, in bytes of its text. A longer one fails with
    /// [`Error::NumberTooLong`].
    pub number: u32,
    /// The most bytes of a string's text demanded at once: the maximum of
    /// each scan to the next quote. At least one.
    pub chunk: u32,
    /// The longest document, in bytes the stream delivers, whitespace
    /// around it included. A delivery that would pass it fails the document
    /// with [`Error::TooLong`], before it is read.
    pub length: u32,
}

/// The most bytes the tokenizer demands at once: a scan of
/// [`Limits::chunk`], or the four letters after the `f` of `false`.
///
/// Whoever stacks the tokenizer checks at startup that the side below's cap
/// holds it (lib.md, 7): a demand past that cap could never be met.
#[must_use]
pub fn largest_demand(limits: &Limits) -> u32 {
    limits.chunk.max(Literal::False.len())
}

/// The most memory a tokenizer holds under `limits`, in bytes
/// (programming-model.md, 6.3), or `None` if it does not fit a `u64` or the
/// limits cannot be honoured: a [`Limits::chunk`] of zero.
///
/// It is the stack of open objects and arrays and the text of the string or
/// number being read, both allocated when the tokenizer is made, and the
/// delivery it reads: a delivery is made to its demand, so it counts it
/// (testing.md, 5), held for the step that reads it, and at most
/// [`largest_demand`] bytes. A
/// token's box is the side above's to count from when it is emitted.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.chunk == 0 {
        return None;
    }
    let open = Stack::<Container>::worst_case(limits.depth)?;
    let text = List::<u8>::worst_case(text_capacity(limits))?;
    open.checked_add(text)?.checked_add(u64::from(largest_demand(limits)))
}

/// The most an entry point emits in one call, into each of its two queues.
/// Whoever calls it reserves this much room in each first
/// (programming-model.md, 2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    /// Events for the side above.
    pub above: u32,
    /// Requests for the side below.
    pub below: u32,
}

/// [`up`]'s: a token or the document's outcome, or the next demand.
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 1, below: 1 };

/// [`down`]'s: for a `Next`, a token or the outcome, or a demand; for a
/// `Close`, `Closed` and the demand withdrawn.
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 1, below: 1 };

/// From the side above.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// A demand for the next token. Exactly one [`Event`] answers it, a
    /// `Token`, `Done` or `Failed`, unless a `Close` comes first.
    Next,
    /// A demand keeping the next string up to this decoded byte count; answered by a token or `Long`.
    Text(u32),
    /// A demand scanning one value without retaining it; answered by `Skipped` or an array end.
    Skip,
    /// Closes the tokenizer, in any state. `Closed` answers it.
    Close,
}

/// To the side above.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// The next token, for a `Next`.
    Token(Token),
    /// For `Text`: the complete decoded length of a string past its cap.
    Long(u64),
    /// For `Skip`: the bytes delivered for one checked value.
    Skipped(u64),
    /// For a `Next`: the document is whole, and the stream ended after it
    /// with nothing but whitespace. Nothing follows but `Closed`.
    Done,
    /// For a `Next`: the document is malformed, past a limit or cut short,
    /// or the stream below failed. Nothing follows but `Closed`.
    Failed(Error),
    /// For a `Close`: the tokenizer is closed. Terminal.
    Closed,
}

/// Why a document failed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// A byte the grammar does not allow where it is: `{"a" 1}`, `[1,]`,
    /// `{a: 1}`, `nul!`, `'a'`.
    Unexpected,
    /// Something other than whitespace after the document: `{} {}`.
    Trailing,
    /// A document longer than [`Limits::length`].
    TooLong,
    /// Objects and arrays nested deeper than [`Limits::depth`].
    TooDeep,
    /// A string or key longer than [`Limits::string`] once unescaped.
    StringTooLong,
    /// A number longer than [`Limits::number`].
    NumberTooLong,
    /// A number that is not one: `01`, `1.`, `-`, `1e+`, `2-1`.
    Number,
    /// A backslash JSON gives no meaning to (`\x`), or a `\u` without four
    /// hex digits.
    Escape,
    /// A `\u` escape of half a surrogate pair, without the other half.
    Surrogate,
    /// Bytes in a string that are not UTF-8.
    Utf8,
    /// A control character, below U+0020, in a string without an escape.
    Control,
    /// The stream ended before the document did.
    Truncated,
    /// The stream below failed.
    Stream(Fault),
}

/// What a tokenizer is waiting for. Machines keep no timers
/// (programming-model.md, 4): each says what it waits for, and the
/// connection arms the deadlines.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// For the side above to ask for the next token. Nothing is demanded
    /// below.
    Next,
    /// For the side below to meet its demand: the peer's progress.
    Bytes,
    /// For the side above to close it: the document's outcome went up.
    Close,
    /// For nothing: it is closed.
    Nothing,
}

/// A tokenizer of one document: a connection's state for this machine.
#[derive(Debug)]
pub struct Tokenizer {
    state: State,
    document: Document,
}

impl Tokenizer {
    /// A tokenizer under `limits`, the limits its steps will be given. It
    /// demands nothing until the side above asks for a token.
    #[must_use]
    pub fn new(limits: &Limits) -> Tokenizer {
        assert!(limits.chunk > 0, "a scan holds the quote it ends at");
        Tokenizer {
            state: State::Idle(Held::Nothing),
            document: Document {
                expect: Expect::Value,
                open: Stack::with_capacity(limits.depth),
                text: Buffer {
                    bytes: List::with_capacity(text_capacity(limits)),
                    length: 0,
                    retention: Retention::Strict(limits.string),
                },
                mode: Mode::Next,
                read: 0,
                token_start: 0,
                token_end: 0,
            },
        }
    }

    /// Starts a new stream after Done or Failed, retaining every capacity.
    pub fn restart(&mut self) {
        match self.state {
            State::Over => self.reset(),
            State::Idle(_) | State::Reading(_) | State::Closed => unreachable!("restart follows a document outcome"),
        }
    }

    /// The last token's wire boundaries, excluding its leading separators and lookahead.
    pub(crate) fn token_wire(&self) -> (u32, u32) {
        (self.document.token_start, self.document.token_end)
    }

    /// Delivered bytes in the outstanding skip, excluding its leading separators.
    pub(crate) fn skip_progress(&self) -> u64 {
        match self.document.mode {
            Mode::Skip { start: Some(start), .. } => {
                u64::from(self.document.read.checked_sub(start).expect("the skip starts within delivered bytes"))
            }
            Mode::Skip { start: None, .. } | Mode::Next | Mode::Text(_) => 0,
        }
    }

    /// The collector may also reset a tokenizer it stopped after its own count failure.
    pub(crate) fn reset(&mut self) {
        match self.state {
            State::Idle(_) | State::Over | State::Closed => {}
            State::Reading(_) => unreachable!("reset has no outstanding stream demand"),
        }
        for _ in 0..self.document.open.len() {
            self.document.open.pop().expect("the current open container count");
        }
        self.document.text.reset(Retention::Discard);
        self.document.expect = Expect::Value;
        self.document.mode = Mode::Next;
        self.document.read = 0;
        self.document.token_start = 0;
        self.document.token_end = 0;
        self.state = State::Idle(Held::Nothing);
    }

    /// What it is waiting for: a function of its state alone.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match self.state {
            State::Idle(_) => Waiting::Next,
            State::Reading(_) => Waiting::Bytes,
            State::Over => Waiting::Close,
            State::Closed => Waiting::Nothing,
        }
    }
}

/// An event from the stream below. Emits at most [`UP_MAX_OUT`].
pub fn up(tokenizer: &mut Tokenizer, env: &Env<Limits>, ev: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let document = &mut tokenizer.document;
    let state = mem::replace(&mut tokenizer.state, State::Closed);
    tokenizer.state = match state {
        State::Reading(reading) => match ev {
            Up::Bytes(bytes) => {
                let step = delivered(document, limits, reading, &bytes);
                settle(document, step, above)
            }
            Up::End => ended(document, reading, above),
            Up::Failed(fault) => fail(document, Error::Stream(fault), above),
            Up::Room => unreachable!("the tokenizer asks for no room"),
        },
        State::Idle(held) => match ev {
            Up::Bytes(_) => unreachable!("bytes delivered without a read demand"),
            Up::End => State::Idle(held_end(held)),
            Up::Failed(fault) => State::Idle(held_failed(held, fault)),
            Up::Room => unreachable!("the tokenizer asks for no room"),
        },
        State::Over => match ev {
            Up::Bytes(_) => unreachable!("bytes delivered after the outcome, which leaves no read demand"),
            Up::End | Up::Failed(_) => State::Over,
            Up::Room => unreachable!("the tokenizer asks for no room"),
        },
        // What the close withdrew may have been met already, on its way.
        State::Closed => match ev {
            Up::Bytes(_) | Up::End | Up::Failed(_) => State::Closed,
            Up::Room => unreachable!("the tokenizer asks for no room"),
        },
    };
    demand(state, tokenizer.state, limits, below);
}

/// A request from the side above. Emits at most [`DOWN_MAX_OUT`].
pub fn down(
    tokenizer: &mut Tokenizer,
    env: &Env<Limits>,
    rq: Request,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    let limits = &env.limits;
    let document = &mut tokenizer.document;
    let state = mem::replace(&mut tokenizer.state, State::Closed);
    tokenizer.state = match rq {
        Request::Next | Request::Text(_) | Request::Skip => match state {
            State::Idle(held) => {
                document.mode = match rq {
                    Request::Next => Mode::Next,
                    Request::Text(cap) => Mode::Text(cap.min(limits.string)),
                    Request::Skip => {
                        assert_skip(document);
                        Mode::Skip { depth: document.open.len(), start: None }
                    }
                    Request::Close => unreachable!("a demand"),
                };
                next(document, limits, held, above)
            }
            State::Reading(_) => unreachable!("a Next before the last one was answered"),
            State::Over => unreachable!("a Next after the document's outcome"),
            State::Closed => unreachable!("a Next after Closed"),
        },
        Request::Close => close(document, state, above),
    };
    demand(state, tokenizer.state, limits, below);
}

/// What the tokenizer is doing about the side above's demand.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum State {
    /// Waiting for the side above to ask for the next token, with nothing
    /// demanded below, and holding what it learnt before it was asked.
    Idle(Held),
    /// Reading for the side above's `Next`, with a demand below for what
    /// the reading needs.
    Reading(Reading),
    /// The document's outcome, `Done` or `Failed`, went up.
    Over,
    /// Closed: terminal, and the placeholder of every transition.
    Closed,
}

/// What an idle tokenizer holds for the next `Next`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Held {
    /// Nothing: the next token begins with the next byte demanded.
    Nothing,
    /// The byte that ended a number, not whitespace, read but not yet taken
    /// as the grammar's.
    Byte(u8),
    /// The stream ended.
    End,
    /// The stream ended, after the byte that ended a number.
    ByteThenEnd(u8),
    /// The stream failed before it ended, whatever was held before.
    Failed(Fault),
}

/// What a tokenizer reads, and so what it demands below.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Reading {
    /// The next byte, whitespace skipped: a fill of one.
    Byte,
    /// The rest of a literal after its first letter: a fill of its length.
    Literal(Literal),
    /// A string's text, or a key's, through its closing quote: a scan to
    /// the next quote. The text so far is in the document's.
    String { key: bool, text: Text },
    /// A number's next byte: a fill of one. Its text so far is in the
    /// document's.
    Number(Number),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Literal {
    True,
    False,
    Null,
}

impl Literal {
    /// The letters after the first.
    fn rest(self) -> &'static [u8] {
        match self {
            Literal::True => b"rue",
            Literal::False => b"alse",
            Literal::Null => b"ull",
        }
    }

    fn len(self) -> u32 {
        u32::try_from(self.rest().len()).expect("a few letters")
    }

    fn token(self) -> Token {
        match self {
            Literal::True => Token::True,
            Literal::False => Token::False,
            Literal::Null => Token::Null,
        }
    }
}

/// What a tokenizer knows of the document, in every state.
#[derive(Debug)]
struct Document {
    /// What the grammar allows next.
    expect: Expect,
    /// The objects and arrays open, the innermost on top.
    open: Stack<Container>,
    /// The text of the string or number being read: empty between tokens.
    text: Buffer,
    mode: Mode,
    /// The bytes delivered so far, at most [`Limits::length`].
    read: u32,
    token_start: u32,
    token_end: u32,
}

/// Retention and framing of the demand outstanding above.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Mode {
    Next,
    Text(u32),
    Skip { depth: u32, start: Option<u32> },
}

fn assert_skip(document: &Document) {
    match document.expect {
        Expect::Value | Expect::FirstElement | Expect::Colon | Expect::Trailing => {}
        Expect::CommaOrEnd => match document.open.top() {
            Some(Container::Array) => {}
            Some(Container::Object) | None => unreachable!("Skip where a key comes"),
        },
        Expect::FirstKey | Expect::Key => unreachable!("Skip where a key comes"),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Container {
    Object,
    Array,
}

/// Where the grammar is, between two bytes of the document.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Expect {
    /// A value: the document's, an element's after a comma, or a member's
    /// after its colon.
    Value,
    /// An array's first element, or its end.
    FirstElement,
    /// An object's first key, or its end.
    FirstKey,
    /// An object's next key, after a comma.
    Key,
    /// The colon after a key.
    Colon,
    /// After an element or a member: a comma, or the end of the innermost
    /// container.
    CommaOrEnd,
    /// After the document's value: whitespace, then the end of the stream.
    Trailing,
}

/// What reading a byte, or a delivery, comes to.
#[derive(Debug)]
enum Step {
    /// A token for the side above, and what the tokenizer holds after it.
    Token(Token, Held),
    Long(u64),
    /// More is needed: the tokenizer reads on.
    Read(Reading),
    /// The document failed.
    Fail(Error),
}

/// The quote that ends a string: what every scan is to.
const QUOTE: Delimiter = Delimiter::new(b"\"").expect("one byte");

fn text_capacity(limits: &Limits) -> u32 {
    limits.string.max(limits.number)
}

/// States what the state `after` a transition demands below, in one place
/// after every transition (programming-model.md, 5.4).
///
/// A reading state states its demand: every transition into one follows a
/// delivery that met the last demand, or leaves `Idle`, which has none. A
/// close withdraws what the state `before` it demanded. No other state has
/// anything outstanding below.
fn demand(before: State, after: State, limits: &Limits, below: &mut Queue<Down>) {
    let read = match after {
        State::Reading(Reading::Byte | Reading::Number(_)) => Read::Fill(1),
        State::Reading(Reading::Literal(literal)) => Read::Fill(literal.len()),
        State::Reading(Reading::String { .. }) => Read::Scan { until: QUOTE, max: limits.chunk },
        State::Closed => match before {
            State::Reading(_) => Read::Nothing,
            State::Idle(_) | State::Over | State::Closed => return,
        },
        State::Idle(_) | State::Over => return,
    };
    below.push(Down::Demand { read, room: 0 });
}

/// `Next`, while idle.
fn next(document: &mut Document, limits: &Limits, held: Held, above: &mut Queue<Event>) -> State {
    match held {
        Held::Nothing => State::Reading(Reading::Byte),
        Held::Byte(byte) => {
            let step = significant(document, limits, byte);
            settle(document, step, above)
        }
        Held::ByteThenEnd(byte) => match significant(document, limits, byte) {
            Step::Token(token, held) => complete_token(document, token, held_end(held), above),
            Step::Long(length) => {
                document.token_end = document.read;
                above.push(Event::Long(length));
                State::Idle(Held::End)
            }
            Step::Read(reading) => ended(document, reading, above),
            Step::Fail(error) => fail(document, error, above),
        },
        Held::End => ended(document, Reading::Byte, above),
        Held::Failed(fault) => fail(document, Error::Stream(fault), above),
    }
}

/// `Close`, in any state: what was demanded below is withdrawn by
/// `demand`.
fn close(document: &mut Document, state: State, above: &mut Queue<Event>) -> State {
    match state {
        State::Idle(_) | State::Reading(_) | State::Over => {}
        State::Closed => unreachable!("a Close after Closed"),
    }
    document.text.bytes.clear();
    above.push(Event::Closed);
    State::Closed
}

/// What an idle tokenizer holds once the stream has ended.
fn held_end(held: Held) -> Held {
    match held {
        Held::Nothing => Held::End,
        Held::Byte(byte) => Held::ByteThenEnd(byte),
        // Nothing comes after an end or a failure, a second end included.
        Held::End | Held::ByteThenEnd(_) | Held::Failed(_) => held,
    }
}

/// What an idle tokenizer holds once the stream has failed. After the end,
/// a failure says only that the stream can no longer send: what was read
/// stands.
fn held_failed(held: Held, fault: Fault) -> Held {
    match held {
        Held::Nothing | Held::Byte(_) => Held::Failed(fault),
        Held::End | Held::ByteThenEnd(_) | Held::Failed(_) => held,
    }
}

/// The state a step leaves the tokenizer in, once what it made is emitted.
fn settle(document: &mut Document, step: Step, above: &mut Queue<Event>) -> State {
    match step {
        Step::Token(token, held) => complete_token(document, token, held, above),
        Step::Long(length) => {
            document.token_end = document.read;
            above.push(Event::Long(length));
            State::Idle(Held::Nothing)
        }
        Step::Read(reading) => State::Reading(reading),
        Step::Fail(error) => fail(document, error, above),
    }
}

fn complete_token(document: &mut Document, token: Token, held: Held, above: &mut Queue<Event>) -> State {
    let lookahead = match held {
        Held::Byte(_) | Held::ByteThenEnd(_) => 1,
        Held::Nothing => match token {
            Token::Number(_) => 1,
            Token::ObjectStart
            | Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::Key(_)
            | Token::String(_)
            | Token::True
            | Token::False
            | Token::Null => 0,
        },
        Held::End | Held::Failed(_) => 0,
    };
    document.token_end = document.read.checked_sub(lookahead).expect("the token preceded its lookahead");
    match document.mode {
        Mode::Next | Mode::Text(_) | Mode::Skip { depth: _, start: None } => above.push(Event::Token(token)),
        Mode::Skip { depth, start: Some(start) } => {
            if document.open.len() > depth {
                assert!(held == Held::Nothing, "a skipped inner number consumes its delimiter");
                return State::Reading(Reading::Byte);
            }
            let lookahead = match held {
                Held::Byte(_) | Held::ByteThenEnd(_) => 1,
                Held::Nothing | Held::End | Held::Failed(_) => 0,
            };
            let length = document
                .read
                .checked_sub(start)
                .expect("the value starts within the bytes delivered")
                .checked_sub(lookahead)
                .expect("the value was delivered");
            above.push(Event::Skipped(u64::from(length)));
        }
    }
    State::Idle(held)
}

fn fail(document: &mut Document, error: Error, above: &mut Queue<Event>) -> State {
    document.text.bytes.clear();
    above.push(Event::Failed(error));
    State::Over
}

/// The stream ended while the tokenizer read.
fn ended(document: &mut Document, reading: Reading, above: &mut Queue<Event>) -> State {
    match reading {
        Reading::Byte => match document.expect {
            Expect::Trailing => {
                above.push(Event::Done);
                State::Over
            }
            Expect::Value
            | Expect::FirstElement
            | Expect::FirstKey
            | Expect::Key
            | Expect::Colon
            | Expect::CommaOrEnd => fail(document, Error::Truncated, above),
        },
        // Only the end of the stream ends a number that is the whole
        // document; within an object or an array, more was due.
        Reading::Number(number) => {
            if !document.open.is_empty() {
                return fail(document, Error::Truncated, above);
            }
            if !number.is_complete() {
                return fail(document, Error::Number, above);
            }
            let token = number_token(document);
            complete_token(document, token, Held::End, above)
        }
        Reading::Literal(_) | Reading::String { .. } => fail(document, Error::Truncated, above),
    }
}

/// The bytes delivered for `reading`'s demand.
fn delivered(document: &mut Document, limits: &Limits, reading: Reading, bytes: &[u8]) -> Step {
    let read = match u32::try_from(bytes.len()) {
        Ok(len) => document.read.checked_add(len),
        Err(_) => None,
    };
    match read {
        Some(read) if read <= limits.length => document.read = read,
        Some(_) | None => return Step::Fail(Error::TooLong),
    }
    match reading {
        Reading::Byte => significant(document, limits, one(bytes)),
        Reading::Number(number) => number_byte(document, limits, number, one(bytes)),
        Reading::Literal(literal) => {
            assert!(bytes.len() == literal.rest().len(), "a fill delivers what it demanded");
            if bytes != literal.rest() {
                return Step::Fail(Error::Unexpected);
            }
            after_value(document);
            Step::Token(literal.token(), Held::Nothing)
        }
        Reading::String { key, text } => {
            let chunk = usize::try_from(limits.chunk).expect("a u32 fits a usize");
            assert!(
                bytes.len() <= chunk && (bytes.last() == Some(&b'"') || bytes.len() == chunk),
                "a scan delivers through its quote or its maximum"
            );
            match string::decode(text, bytes, &mut document.text) {
                Ok(Piece::More(text)) => Step::Read(Reading::String { key, text }),
                Ok(Piece::Closed) => string_token(document, key),
                Err(error) => Step::Fail(error),
            }
        }
    }
}

/// The one byte a fill of one delivers.
fn one(bytes: &[u8]) -> u8 {
    match *bytes {
        [byte] => byte,
        _ => unreachable!("a fill of one delivers one byte"),
    }
}

/// A byte of the grammar, between tokens.
fn significant(document: &mut Document, limits: &Limits, byte: u8) -> Step {
    if is_whitespace(byte) {
        return Step::Read(Reading::Byte);
    }
    match document.expect {
        Expect::Value => value(document, limits, byte),
        Expect::FirstElement => match byte {
            b']' => end(document, Container::Array),
            _ => value(document, limits, byte),
        },
        Expect::FirstKey => match byte {
            b'"' => begin_string(document, limits, true),
            b'}' => end(document, Container::Object),
            _ => Step::Fail(Error::Unexpected),
        },
        Expect::Key => match byte {
            b'"' => begin_string(document, limits, true),
            _ => Step::Fail(Error::Unexpected),
        },
        Expect::Colon => match byte {
            b':' => {
                document.expect = Expect::Value;
                Step::Read(Reading::Byte)
            }
            _ => Step::Fail(Error::Unexpected),
        },
        Expect::CommaOrEnd => match byte {
            b',' => comma(document),
            b']' => end(document, Container::Array),
            b'}' => end(document, Container::Object),
            _ => Step::Fail(Error::Unexpected),
        },
        Expect::Trailing => Step::Fail(Error::Trailing),
    }
}

/// The first byte of a value.
fn value(document: &mut Document, limits: &Limits, byte: u8) -> Step {
    document.token_start = document.read.checked_sub(1).expect("the value opener was delivered");
    match &mut document.mode {
        Mode::Skip { start, .. } => {
            if start.is_none() {
                *start = Some(document.read.checked_sub(1).expect("the first byte was delivered"));
            }
        }
        Mode::Next | Mode::Text(_) => {}
    }
    match byte {
        b'{' => start(document, Container::Object),
        b'[' => start(document, Container::Array),
        b'"' => begin_string(document, limits, false),
        b't' => Step::Read(Reading::Literal(Literal::True)),
        b'f' => Step::Read(Reading::Literal(Literal::False)),
        b'n' => Step::Read(Reading::Literal(Literal::Null)),
        _ => match Number::start(byte) {
            Some(number) => {
                document.text.reset(Retention::Discard);
                number_more(document, limits, number, byte)
            }
            None => Step::Fail(Error::Unexpected),
        },
    }
}

fn begin_string(document: &mut Document, limits: &Limits, key: bool) -> Step {
    if key {
        document.token_start = document.read.checked_sub(1).expect("the key quote was delivered");
    }
    let retention = match document.mode {
        Mode::Next => Retention::Strict(limits.string),
        Mode::Text(cap) => Retention::Capped(cap),
        Mode::Skip { .. } => Retention::Discard,
    };
    document.text.reset(retention);
    Step::Read(Reading::String { key, text: Text::Plain })
}

/// The start of an object or an array, refused past the depth.
fn start(document: &mut Document, container: Container) -> Step {
    if document.open.push(container).is_err() {
        return Step::Fail(Error::TooDeep);
    }
    let (expect, token) = match container {
        Container::Object => (Expect::FirstKey, Token::ObjectStart),
        Container::Array => (Expect::FirstElement, Token::ArrayStart),
    };
    document.expect = expect;
    Step::Token(token, Held::Nothing)
}

/// The end of an object or an array, which must be the innermost open.
fn end(document: &mut Document, container: Container) -> Step {
    document.token_start = document.read.checked_sub(1).expect("the closer was delivered");
    // A document that fails is over, so a pop that does not match is not
    // undone.
    match document.open.pop() {
        Some(top) if top == container => {}
        Some(_) | None => return Step::Fail(Error::Unexpected),
    }
    after_value(document);
    let token = match container {
        Container::Object => Token::ObjectEnd,
        Container::Array => Token::ArrayEnd,
    };
    Step::Token(token, Held::Nothing)
}

fn comma(document: &mut Document) -> Step {
    document.expect = match document.open.top() {
        Some(Container::Object) => Expect::Key,
        Some(Container::Array) => Expect::Value,
        None => unreachable!("a comma is expected only within an object or an array"),
    };
    Step::Read(Reading::Byte)
}

/// A byte after a number's first.
fn number_byte(document: &mut Document, limits: &Limits, number: Number, byte: u8) -> Step {
    match number.next(byte) {
        After::More(next) => number_more(document, limits, next, byte),
        After::Invalid => Step::Fail(Error::Number),
        After::Ended => {
            if !number.is_complete() {
                return Step::Fail(Error::Number);
            }
            let token = number_token(document);
            // The byte that ended the number is the grammar's: held for the
            // next `Next`, unless it is whitespace, which it would skip.
            let held = match document.mode {
                Mode::Skip { depth, .. } => {
                    if document.open.len() > depth {
                        return significant(document, limits, byte);
                    }
                    Held::Byte(byte)
                }
                Mode::Next | Mode::Text(_) => {
                    if is_whitespace(byte) {
                        Held::Nothing
                    } else {
                        Held::Byte(byte)
                    }
                }
            };
            Step::Token(token, held)
        }
    }
}

/// A byte that continues a number, refused past the number limit.
fn number_more(document: &mut Document, limits: &Limits, number: Number, byte: u8) -> Step {
    if document.text.length >= u64::from(limits.number) {
        return Step::Fail(Error::NumberTooLong);
    }
    document.text.length = document.text.length.checked_add(1).expect("under the number limit");
    match document.mode {
        Mode::Skip { .. } => {}
        Mode::Next | Mode::Text(_) => {
            document.text.bytes.push(byte).expect("the text holds the longest number");
        }
    }
    Step::Read(Reading::Number(number))
}

/// The number read, as a token, the text emptied for the next.
fn number_token(document: &mut Document) -> Token {
    let text = document.text.bytes.to_boxed();
    document.text.reset(Retention::Discard);
    after_value(document);
    Token::Number(text)
}

/// The string or key read, as a token, the text emptied for the next.
fn string_token(document: &mut Document, key: bool) -> Step {
    let text = document.text.bytes.to_boxed();
    document.text.bytes.clear();
    if key {
        document.expect = Expect::Colon;
        return match document.mode {
            Mode::Text(cap) if document.text.length > u64::from(cap) => Step::Long(document.text.length),
            Mode::Next | Mode::Text(_) | Mode::Skip { .. } => Step::Token(Token::Key(text), Held::Nothing),
        };
    }
    after_value(document);
    match document.mode {
        Mode::Text(cap) if document.text.length > u64::from(cap) => Step::Long(document.text.length),
        Mode::Next | Mode::Text(_) | Mode::Skip { .. } => Step::Token(Token::String(text), Held::Nothing),
    }
}

/// Where the grammar is after a value: within its container, or at the
/// document's end.
fn after_value(document: &mut Document) {
    document.expect = if document.open.is_empty() { Expect::Trailing } else { Expect::CommaOrEnd };
}

fn is_whitespace(byte: u8) -> bool {
    b" \t\n\r".contains(&byte)
}
