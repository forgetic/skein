//! Selective JSON collection (json.md, section 5.1): a tokenizer and one
//! bounded walk keep only a filter's named paths. The machine owns reusable
//! compact buffers and counts skipped wire bytes; it knows no transport or
//! application meaning. `Collect` reads one stream through its end; `Close`
//! withdraws its read and ends it. `restart` reuses the buffers on a new stream.
//!
//! | State | Collect | stream input | Close |
//! |---|---|---|---|
//! | Idle | Reading | retain end/fault below | Closed |
//! | Reading | owner bug | read, or Collected/Failed -> Over | Closed |
//! | Over | owner bug; restart first | absorb end/fault | Closed |
//! | Closed | owner bug | absorb withdrawn delivery | owner bug |

use alloc::boxed::Box;

use crate::document::{self, Builder};
use crate::tokenizer::{self as json, Tokenizer};
use crate::{Document, Kind, Token};
use skein_lib::stream::{Down, Up};
use skein_lib::{Env, Queue, Stack};

/// A static path selection supplied by the collector's owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Filter {
    pub root: Keep,
}

/// A named runtime cap's index, supplied by the filter's owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Cap(u8);

impl Cap {
    /// Names an entry in the cap table used to construct the collector.
    #[must_use]
    pub const fn new(index: u8) -> Cap {
        Cap(index)
    }

    /// The owner's cap-table index, for translating a named failure.
    #[must_use]
    pub const fn index(self) -> u8 {
        self.0
    }
}

/// How the owner asks to retain a value; unknown children are scanned.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Keep {
    /// Keep the complete bounded value for the decoder.
    Value,
    /// Keep a string only through this decoded byte count, or its length alone.
    Text(Cap),
    /// Keep a container and only the named children for the decoder.
    Into(&'static [Node]),
}

/// One statically named child and its retention, supplied by the owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Node {
    pub key: Key,
    pub keep: Keep,
}

/// A child path supplied by the owner, matched against the peer's container.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Key {
    /// This decoded object field, for its named value.
    Field(&'static [u8]),
    /// Every array element, for the same child selection.
    Each,
}

/// The tokenizer and retained counts admitted by the collector's owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub tokenizer: json::Limits,
    pub tokens: u32,
    pub text: u32,
    pub skip: u64,
}

/// From the owner; each receives exactly one answer unless Close comes first.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// Read the stream's complete document; Collected or Failed answers it.
    Collect,
    /// End the machine in every state; Closed is its terminal.
    Close,
}

/// To the owner: its document's outcome, or the collector's terminal.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// The selected document, answering Collect.
    Collected(Document),
    /// The document failed or passed a count, answering Collect.
    Failed(Error),
    /// Close has ended the collector; nothing follows.
    Closed,
}

/// Why a Collect did not yield a document, reported to its owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// The underlying JSON or stream failed.
    Tokenizer(json::Error),
    /// The document exceeded its retained record count.
    TooManyTokens,
    /// The document exceeded its decoded retained byte count.
    TooMuchText {
        /// A named cap when one applies, or the global retained text count.
        cap: Option<Cap>,
    },
    /// The skipped values exceeded their delivered byte count.
    SkippedTooLong,
    /// An object repeated a field named by the filter.
    Duplicate,
}

/// What the owner observes while deciding whether its progress deadline runs.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// No Collect is outstanding; the machine demands nothing below.
    Collect,
    /// The tokenizer is reading the outstanding Collect's stream.
    Bytes,
    /// The document's outcome was emitted; the owner closes or restarts it.
    Close,
    /// Close ended the machine.
    Nothing,
}

/// The counts reached by a document's selected records and skipped values.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Counts {
    pub tokens: u32,
    pub text: u32,
    pub skipped: u64,
}

/// One event above and one stream request below per entry point.
pub const UP_MAX_OUT: json::MaxOut = json::MaxOut { above: 1, below: 1 };

/// One event above and one stream request below per entry point.
pub const DOWN_MAX_OUT: json::MaxOut = json::MaxOut { above: 1, below: 1 };

/// The reusable state of one collector, owned by its connection.
#[derive(Debug)]
pub struct Collector {
    tokenizer: Tokenizer,
    events: Queue<json::Event>,
    walk: Stack<Frame>,
    document: Builder,
    state: State,
    position: Position,
    skipped: u64,
    caps: Box<[u32]>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum State {
    Idle,
    Reading,
    Over,
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Position {
    Value(Keep),
    Key,
    Skip,
    End,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Frame {
    kind: Kind,
    keep: Keep,
    start: u32,
}

impl Collector {
    /// Makes one collector under stable limits, initially waiting for Collect.
    /// Named fields must fit the tokenizer's retained string cap.
    #[must_use]
    pub fn new(filter: Filter, limits: &Limits, caps: &[u32]) -> Collector {
        assert!(caps.len() <= 256, "cap indexes fit u8");
        Collector {
            tokenizer: Tokenizer::new(&limits.tokenizer),
            events: Queue::with_capacity(1),
            walk: Stack::with_capacity(limits.tokenizer.depth),
            document: Builder::new(document::Limits { tokens: limits.tokens, text: limits.text }),
            state: State::Idle,
            position: Position::Value(filter.root),
            skipped: 0,
            caps: Box::from(caps),
        }
    }

    /// Its wait is a function of the machine's state, for the owner's deadline.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match self.state {
            State::Idle => Waiting::Collect,
            State::Reading => Waiting::Bytes,
            State::Over => Waiting::Close,
            State::Closed => Waiting::Nothing,
        }
    }

    /// Running counts, including those reached before a failure.
    #[must_use]
    pub fn counts(&self) -> Counts {
        let progress = match self.state {
            State::Reading => match self.position {
                Position::Skip => self.tokenizer.skip_progress(),
                Position::Value(_) | Position::Key | Position::End => 0,
            },
            State::Idle | State::Over | State::Closed => 0,
        };
        let skipped = self.skipped.checked_add(progress).expect("the delivered stream is bounded by u32 length");
        Counts { tokens: self.document.len(), text: self.document.text_len(), skipped }
    }

    /// Starts a new stream after the previous outcome, retaining all capacities.
    pub fn restart(&mut self, filter: Filter) {
        match self.state {
            State::Over => {}
            State::Idle | State::Reading | State::Closed => unreachable!("restart follows a document outcome"),
        }
        self.tokenizer.reset();
        self.document.clear();
        clear_walk(&mut self.walk);
        self.position = Position::Value(filter.root);
        self.skipped = 0;
        self.state = State::Idle;
    }
}

/// The collector's retained buffers, bounded walk and transient ownership.
#[must_use]
pub fn worst_case(limits: &Limits, caps: &[u32], _filter: &Filter) -> Option<u64> {
    if caps.len() > 256 {
        return None;
    }
    let caps = u64::try_from(caps.len()).ok()?.checked_mul(u64::try_from(size_of::<u32>()).ok()?)?;
    let delivery = u64::from(json::largest_demand(&limits.tokenizer));
    let tokenizer = json::worst_case(&limits.tokenizer)?.checked_sub(delivery)?;
    let walk = Stack::<Frame>::worst_case(limits.tokenizer.depth)?;
    let queue = Queue::<json::Event>::worst_case(1)?;
    let retained = document::worst_case(&document::Limits { tokens: limits.tokens, text: limits.text })?;
    let token = u64::from(limits.tokenizer.string.max(limits.tokenizer.number));
    // A token's delivery and owned text coexist while the tokenizer emits it.
    // The output document is copied only after that delivery and token are gone.
    let transient = delivery.checked_add(token)?.max(retained);
    tokenizer.checked_add(walk)?.checked_add(queue)?.checked_add(retained)?.checked_add(transient)?.checked_add(caps)
}

/// Receives a stream event, emitting at most `UP_MAX_OUT`.
pub fn up(collector: &mut Collector, env: &Env<Limits>, event: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let tokenizer_env = tokenizer_env(env);
    json::up(&mut collector.tokenizer, &tokenizer_env, event, &mut collector.events, below);
    if collector.state == State::Reading && collector.events.is_empty() && collector.position == Position::Skip {
        let skipped = collector.skipped.checked_add(collector.tokenizer.skip_progress());
        match skipped {
            Some(count) if count <= env.limits.skip => {}
            Some(_) | None => {
                // The tokenizer's next demand has not left this entry point.
                // Consume it and its close withdrawal before reporting the count.
                let next = below.pop().expect("an unfinished skip demands more");
                match next {
                    Down::Demand { .. } => {}
                    Down::Send(_) | Down::Finish => unreachable!("the tokenizer demands only bytes"),
                }
                collector.skipped = skipped.unwrap_or(u64::MAX);
                collector.state = State::Over;
                stop_tokenizer(collector, env, below);
                assert!(
                    below.pop() == Some(Down::Demand { read: skein_lib::stream::Read::Nothing, room: 0 }),
                    "the unforwarded demand was withdrawn"
                );
                above.push(Event::Failed(Error::SkippedTooLong));
                return;
            }
        }
    }
    drive(collector, env, above, below);
}

/// Receives the owner's demand, emitting at most `DOWN_MAX_OUT`.
pub fn down(
    collector: &mut Collector,
    env: &Env<Limits>,
    request: Request,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    match request {
        Request::Collect => {
            match collector.state {
                State::Idle => {}
                State::Reading | State::Over | State::Closed => {
                    unreachable!("one Collect per stream, before its outcome or close")
                }
            }
            collector.state = State::Reading;
            demand(collector, env, below);
            drive(collector, env, above, below);
        }
        Request::Close => {
            match collector.state {
                State::Idle | State::Reading | State::Over => {}
                State::Closed => unreachable!("a Close after Closed"),
            }
            stop_tokenizer(collector, env, below);
            collector.document.clear();
            clear_walk(&mut collector.walk);
            collector.state = State::Closed;
            above.push(Event::Closed);
        }
    }
}

fn tokenizer_env(env: &Env<Limits>) -> Env<json::Limits> {
    Env { now: env.now, wall: env.wall, limits: env.limits.tokenizer }
}

fn clear_walk(walk: &mut Stack<Frame>) {
    for _ in 0..walk.len() {
        walk.pop().expect("the walk's current frame count");
    }
}

fn stop_tokenizer(collector: &mut Collector, env: &Env<Limits>, below: &mut Queue<Down>) {
    match collector.tokenizer.waiting() {
        json::Waiting::Nothing => {}
        json::Waiting::Next | json::Waiting::Bytes | json::Waiting::Close => {
            json::down(
                &mut collector.tokenizer,
                &tokenizer_env(env),
                json::Request::Close,
                &mut collector.events,
                below,
            );
            assert!(collector.events.pop() == Some(json::Event::Closed), "the tokenizer answers Close once");
        }
    }
}

fn drive(collector: &mut Collector, env: &Env<Limits>, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    // One delivery can finish a number, its held delimiter and a held end.
    // Every other continuation states a demand below rather than emitting a token.
    for _ in 0_u32..3 {
        let Some(event) = collector.events.pop() else {
            return;
        };
        match event {
            json::Event::Token(value) => {
                if let Err(error) = token(collector, &value) {
                    failed(collector, env, error, above, below);
                    return;
                }
            }
            json::Event::Long(length) => {
                if let Err(error) = long(collector, length) {
                    failed(collector, env, error, above, below);
                    return;
                }
            }
            json::Event::Skipped(length) => {
                assert!(collector.position == Position::Skip, "a Skip was outstanding");
                let Some(skipped) = collector.skipped.checked_add(length) else {
                    failed(collector, env, Error::SkippedTooLong, above, below);
                    return;
                };
                collector.skipped = skipped;
                if skipped > env.limits.skip {
                    failed(collector, env, Error::SkippedTooLong, above, below);
                    return;
                }
                collector.position = after_value(collector);
            }
            json::Event::Done => {
                collector.state = State::Over;
                above.push(Event::Collected(collector.document.document()));
                return;
            }
            json::Event::Failed(error) => {
                collector.state = State::Over;
                above.push(Event::Failed(Error::Tokenizer(error)));
                return;
            }
            json::Event::Closed => unreachable!("Close is consumed by stop_tokenizer"),
        }
        demand(collector, env, below);
    }
    assert!(collector.events.is_empty(), "at most a number, its held delimiter and its end");
}

fn failed(
    collector: &mut Collector,
    env: &Env<Limits>,
    error: Error,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    collector.state = State::Over;
    stop_tokenizer(collector, env, below);
    above.push(Event::Failed(error));
}

fn demand(collector: &mut Collector, env: &Env<Limits>, below: &mut Queue<Down>) {
    let request = match collector.position {
        Position::Value(Keep::Text(cap)) => {
            json::Request::Text(*collector.caps.get(usize::from(cap.index())).expect("the filter names a supplied cap"))
        }
        Position::Value(Keep::Value | Keep::Into(_)) | Position::End => json::Request::Next,
        Position::Skip => json::Request::Skip,
        Position::Key => {
            let frame = collector.walk.top().expect("a key is within an object");
            match frame.keep {
                Keep::Value | Keep::Text(_) => json::Request::Next,
                Keep::Into(nodes) => {
                    let mut cap = 0;
                    for node in nodes {
                        match node.key {
                            Key::Field(name) => {
                                cap = cap.max(u32::try_from(name.len()).expect("a static field length fits u32"));
                            }
                            Key::Each => {}
                        }
                    }
                    assert!(cap <= env.limits.tokenizer.string, "the tokenizer retains every named field");
                    json::Request::Text(cap)
                }
            }
        }
    };
    json::down(&mut collector.tokenizer, &tokenizer_env(env), request, &mut collector.events, below);
}

fn current_keep(collector: &Collector) -> Keep {
    match collector.position {
        Position::Value(keep) => keep,
        Position::Key | Position::Skip | Position::End => unreachable!("a value is selected before its first token"),
    }
}

fn token(collector: &mut Collector, token: &Token) -> Result<(), Error> {
    match token {
        Token::ObjectStart | Token::ArrayStart => {
            let kind = match token {
                Token::ObjectStart => Kind::ObjectStart,
                Token::ArrayStart => Kind::ArrayStart,
                Token::ObjectEnd
                | Token::ArrayEnd
                | Token::Key(_)
                | Token::String(_)
                | Token::Number(_)
                | Token::True
                | Token::False
                | Token::Null => unreachable!("a container start"),
            };
            let keep = match current_keep(collector) {
                Keep::Text(_) | Keep::Value => Keep::Value,
                Keep::Into(nodes) => Keep::Into(nodes),
            };
            let start = collector.document.len();
            push(collector, token)?;
            collector.walk.push(Frame { kind, keep, start }).expect("the tokenizer already admitted this depth");
            collector.position = match kind {
                Kind::ObjectStart => Position::Key,
                Kind::ArrayStart => array_position(keep),
                Kind::ObjectEnd
                | Kind::ArrayEnd
                | Kind::Key
                | Kind::String
                | Kind::Number
                | Kind::True
                | Kind::False
                | Kind::Null
                | Kind::Long => unreachable!("a kept container start"),
            };
        }
        Token::ObjectEnd | Token::ArrayEnd => {
            collector.walk.pop().expect("the tokenizer closes a kept container");
            push(collector, token)?;
            collector.position = after_value(collector);
        }
        Token::Key(key) => {
            let frame = *collector.walk.top().expect("a key's object");
            let selected = match frame.keep {
                Keep::Value | Keep::Text(_) => Some(Keep::Value),
                Keep::Into(nodes) => field(nodes, key),
            };
            collector.position = match selected {
                Some(keep) => {
                    match frame.keep {
                        Keep::Into(_) => {
                            if duplicate(&collector.document, frame.start, key) {
                                return Err(Error::Duplicate);
                            }
                        }
                        Keep::Value | Keep::Text(_) => {}
                    }
                    push(collector, token)?;
                    Position::Value(keep)
                }
                None => Position::Skip,
            };
        }
        Token::String(_) | Token::Number(_) | Token::True | Token::False | Token::Null => {
            push(collector, token)?;
            collector.position = after_value(collector);
        }
    }
    Ok(())
}

fn push(collector: &mut Collector, token: &Token) -> Result<(), Error> {
    match collector.document.push(token) {
        Ok(()) => Ok(()),
        Err(error) => Err(document_error(error)),
    }
}

fn document_error(error: document::Error) -> Error {
    match error {
        document::Error::TooManyTokens => Error::TooManyTokens,
        document::Error::TooMuchText => Error::TooMuchText { cap: None },
        document::Error::Range => unreachable!("the builder computes every record offset"),
    }
}

fn long(collector: &mut Collector, length: u64) -> Result<(), Error> {
    match collector.position {
        Position::Key => collector.position = Position::Skip,
        Position::Value(Keep::Text(_)) => {
            match collector.document.push_long(length) {
                Ok(()) => {}
                Err(error) => return Err(document_error(error)),
            }
            collector.position = after_value(collector);
        }
        Position::Value(Keep::Value | Keep::Into(_)) | Position::Skip | Position::End => {
            unreachable!("only a capped key or value answers Long")
        }
    }
    Ok(())
}

fn after_value(collector: &Collector) -> Position {
    match collector.walk.top() {
        None => Position::End,
        Some(frame) => match frame.kind {
            Kind::ObjectStart => Position::Key,
            Kind::ArrayStart => array_position(frame.keep),
            Kind::ObjectEnd
            | Kind::ArrayEnd
            | Kind::Key
            | Kind::String
            | Kind::Number
            | Kind::True
            | Kind::False
            | Kind::Null
            | Kind::Long => unreachable!("only open containers are walked"),
        },
    }
}

fn array_position(keep: Keep) -> Position {
    match keep {
        Keep::Value | Keep::Text(_) => Position::Value(Keep::Value),
        Keep::Into(nodes) => {
            for node in nodes {
                match node.key {
                    Key::Each => return Position::Value(node.keep),
                    Key::Field(_) => {}
                }
            }
            Position::Skip
        }
    }
}

fn field(nodes: &'static [Node], key: &[u8]) -> Option<Keep> {
    for node in nodes {
        match node.key {
            Key::Field(name) => {
                if name == key {
                    return Some(node.keep);
                }
            }
            Key::Each => {}
        }
    }
    None
}

fn duplicate(builder: &Builder, start: u32, key: &[u8]) -> bool {
    let mut depth = 0_u32;
    for index in start..builder.len() {
        let record = builder.token(index).expect("a retained record");
        match record.kind {
            Kind::ObjectStart | Kind::ArrayStart => {
                depth = depth.checked_add(1).expect("bounded by the tokenizer depth");
            }
            Kind::ObjectEnd | Kind::ArrayEnd => depth = depth.checked_sub(1).expect("inside the open object"),
            Kind::Key => {
                if depth == 1 && builder.text(record) == key {
                    return true;
                }
            }
            Kind::String | Kind::Number | Kind::True | Kind::False | Kind::Null | Kind::Long => {}
        }
    }
    false
}
