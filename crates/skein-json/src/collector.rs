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
use skein_lib::{Env, List, Queue, Stack};

mod tagged;
use tagged::Pending;

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
    /// Keep an object through the variant selected by its string tag.
    Tagged(&'static Tagged),
}

/// A tagged object selection supplied by the filter's owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Tagged {
    /// The field whose string value selects a variant.
    pub tag: &'static [u8],
    pub known: &'static [Variant],
    /// Decoded text bytes in an unknown or untagged object kept whole.
    pub unknown: Cap,
}

/// One known tag value and its retained children, supplied by the owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Variant {
    pub value: &'static [u8],
    pub children: &'static [Node],
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
    /// The peer supplied a non-string discriminant for a tagged object.
    NotTagged,
}

/// A static filter keeps a shared field two ways; returned to its owner at construction.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct AmbiguousFilter {
    pub field: &'static [u8],
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
    filter: Filter,
    tags: List<Pending>,
    limits: Limits,
    settled_tokens: u32,
    settled_text: u32,
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
    Ignored,
    Tag,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Frame {
    kind: Kind,
    keep: Keep,
    start: u32,
    tag: Option<u32>,
    stored: bool,
}

impl Collector {
    /// Makes one collector under stable limits, initially waiting for Collect.
    /// Named fields must fit the tokenizer's retained string cap.
    pub fn new(filter: Filter, limits: &Limits, caps: &[u32]) -> Result<Collector, AmbiguousFilter> {
        assert!(caps.len() <= 256, "cap indexes fit u8");
        let tags = tagged::build(filter, limits, caps)?;
        let plan = tagged::describe(filter, limits)?;
        let capacity = tagged::projection_limits(limits, &plan).expect("the filter's projection capacities fit u32");
        Ok(Collector {
            tokenizer: Tokenizer::new(&limits.tokenizer),
            events: Queue::with_capacity(1),
            walk: Stack::with_capacity(limits.tokenizer.depth),
            document: Builder::new(capacity),
            state: State::Idle,
            position: Position::Value(filter.root),
            skipped: 0,
            caps: Box::from(caps),
            filter,
            tags,
            limits: *limits,
            settled_tokens: 0,
            settled_text: 0,
        })
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
                Position::Value(_) | Position::Key | Position::End | Position::Ignored | Position::Tag => 0,
            },
            State::Idle | State::Over | State::Closed => 0,
        };
        let skipped = self.skipped.checked_add(progress).expect("the delivered stream is bounded by u32 length");
        Counts { tokens: self.document.len(), text: self.document.text_len(), skipped }
    }

    /// Starts a new stream after the previous outcome, retaining all capacities.
    pub fn restart(&mut self) {
        match self.state {
            State::Over => {}
            State::Idle | State::Reading | State::Closed => unreachable!("restart follows a document outcome"),
        }
        self.tokenizer.reset();
        self.document.clear();
        clear_walk(&mut self.walk);
        self.position = Position::Value(self.filter.root);
        for index in 0..self.tags.len() {
            self.tags.get_mut(index).expect("tag").reset();
        }
        self.settled_tokens = 0;
        self.settled_text = 0;
        self.skipped = 0;
        self.state = State::Idle;
    }
}

/// The collector's retained buffers, bounded walk and transient ownership.
#[must_use]
pub fn worst_case(limits: &Limits, caps: &[u32], filter: &Filter) -> Option<u64> {
    if caps.len() > 256 {
        return None;
    }
    let cap_bytes = u64::try_from(caps.len()).ok()?.checked_mul(u64::try_from(size_of::<u32>()).ok()?)?;
    let delivery = u64::from(json::largest_demand(&limits.tokenizer));
    let tokenizer = json::worst_case(&limits.tokenizer)?.checked_sub(delivery)?;
    let walk = Stack::<Frame>::worst_case(limits.tokenizer.depth)?;
    let queue = Queue::<json::Event>::worst_case(1)?;
    let tags = tagged::describe(*filter, limits).ok()?;
    let retained = document::worst_case(&tagged::projection_limits(limits, &tags)?)?;
    let candidates = tagged::candidate_bytes(limits, caps, &tags)?;
    let token = u64::from(limits.tokenizer.string.max(limits.tokenizer.number));
    // A token's delivery and owned text coexist while the tokenizer emits it.
    // The output document is copied only after that delivery and token are gone.
    let planning = List::<&Tagged>::worst_case(tags.capacity())?;
    let output = document::worst_case(&document::Limits { tokens: limits.tokens, text: limits.text })?;
    let transient = delivery.checked_add(token)?.max(output).max(planning);
    tokenizer
        .checked_add(walk)?
        .checked_add(queue)?
        .checked_add(retained)?
        .checked_add(transient)?
        .checked_add(cap_bytes)?
        .checked_add(candidates)
}

/// Receives a stream event, emitting at most `UP_MAX_OUT`.
pub fn up(collector: &mut Collector, env: &Env<Limits>, event: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let tokenizer_env = tokenizer_env(env);
    json::up(&mut collector.tokenizer, &tokenizer_env, event, &mut collector.events, below);
    let skip_base = match active_tag(collector) {
        Some(index) if !has_parent(collector, index) => {
            match collector.tags.get(index).expect("tag").selected_skipped() {
                Some(skipped) => collector.skipped.checked_add(skipped),
                None => None,
            }
        }
        Some(_) => None,
        None => Some(collector.skipped),
    };
    if collector.state == State::Reading
        && collector.events.is_empty()
        && collector.position == Position::Skip
        && let Some(skip_base) = skip_base
    {
        let skipped = skip_base.checked_add(collector.tokenizer.skip_progress());
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
                match active_tag(collector) {
                    Some(index) => collector.tags.get_mut(index).expect("tag").skip_value(length, env.limits.skip),
                    None => {
                        let Some(skipped) = collector.skipped.checked_add(length) else {
                            failed(collector, env, Error::SkippedTooLong, above, below);
                            return;
                        };
                        collector.skipped = skipped;
                        if skipped > env.limits.skip {
                            failed(collector, env, Error::SkippedTooLong, above, below);
                            return;
                        }
                    }
                }
                collector.position = after_value(collector);
                if let Some(frame) = collector.walk.top()
                    && let Some(index) = frame.tag
                {
                    collector.tags.get_mut(index).expect("tag").skip_finished();
                }
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
        Position::Skip => json::Request::Skip,
        Position::End => json::Request::Next,
        Position::Tag => json::Request::Text(env.limits.tokenizer.string),
        Position::Ignored => json::Request::Text(copy_cap(collector)),
        Position::Value(keep) => {
            let cap = match keep {
                Keep::Text(cap) => cap_value(collector, cap),
                Keep::Value | Keep::Into(_) | Keep::Tagged(_) => {
                    let room = match active_tag(collector) {
                        Some(index) => collector.tags.get(index).expect("tag").retention_room(),
                        None => {
                            collector.limits.text.checked_sub(collector.settled_text).expect("admitted settled text")
                        }
                    };
                    env.limits.tokenizer.string.min(room)
                }
            };
            json::Request::Text(cap.max(copy_cap(collector)))
        }
        Position::Key => {
            let frame = collector.walk.top().expect("a key is within an object");
            let cap = match frame.keep {
                Keep::Value | Keep::Text(_) => env.limits.tokenizer.string,
                Keep::Into(nodes) => field_cap(nodes),
                Keep::Tagged(tag) => {
                    let mut cap = u32::try_from(tag.tag.len()).expect("static name fits u32");
                    for variant in tag.known {
                        cap = cap.max(field_cap(variant.children));
                    }
                    cap
                }
            };
            assert!(cap <= env.limits.tokenizer.string, "the tokenizer retains every named field");
            json::Request::Text(cap.max(copy_cap(collector)))
        }
    };
    json::down(&mut collector.tokenizer, &tokenizer_env(env), request, &mut collector.events, below);
}

fn cap_value(collector: &Collector, cap: Cap) -> u32 {
    *collector.caps.get(usize::from(cap.index())).expect("the filter names a supplied cap")
}

fn copy_cap(collector: &Collector) -> u32 {
    let mut cap = 0;
    for tag in &collector.tags {
        if tag.active && tag.copy_error.is_none() {
            cap = cap.max(tag.copy_room());
        }
    }
    cap.min(collector.limits.tokenizer.string)
}

fn field_cap(nodes: &[Node]) -> u32 {
    let mut cap = 0;
    for node in nodes {
        match node.key {
            Key::Field(name) => cap = cap.max(u32::try_from(name.len()).expect("static name fits u32")),
            Key::Each => {}
        }
    }
    cap
}

fn current_keep(collector: &Collector) -> Keep {
    match collector.position {
        Position::Value(keep) => keep,
        Position::Ignored => Keep::Into(&[]),
        Position::Key | Position::Skip | Position::End | Position::Tag => unreachable!("a value is selected first"),
    }
}

fn active_tag(collector: &Collector) -> Option<u32> {
    let mut active = None;
    let mut depth = 0;
    for (index, tag) in collector.tags.iter().enumerate() {
        if tag.active && tag.depth >= depth {
            active = Some(u32::try_from(index).expect("bounded tags"));
            depth = tag.depth;
        }
    }
    active
}

fn capture(collector: &mut Collector, token: &Token) {
    for index in 0..collector.tags.len() {
        let tag = collector.tags.get_mut(index).expect("tag");
        if tag.active {
            tag.capture(token);
        }
    }
}

fn capture_long(collector: &mut Collector) {
    for index in 0..collector.tags.len() {
        let tag = collector.tags.get_mut(index).expect("tag");
        if tag.active && tag.copy_error.is_none() {
            tag.copy_error = Some(if tag.copy_room() <= collector.limits.tokenizer.string {
                Error::TooMuchText { cap: Some(tag.filter.unknown) }
            } else {
                Error::Tokenizer(json::Error::StringTooLong)
            });
        }
    }
}

fn token(collector: &mut Collector, token: &Token) -> Result<(), Error> {
    begin_value(collector);
    capture(collector, token);
    if prepare_tag(collector, token)? {
        return Ok(());
    }
    match token {
        Token::ObjectStart | Token::ArrayStart => open(collector, token)?,
        Token::ObjectEnd | Token::ArrayEnd => close(collector, token)?,
        Token::Key(field_name) => key(collector, token, field_name)?,
        Token::String(value) => {
            if collector.position != Position::Ignored {
                match current_keep(collector) {
                    Keep::Text(cap)
                        if u32::try_from(value.len()).expect("bounded string") > cap_value(collector, cap) =>
                    {
                        push_long(collector, u64::try_from(value.len()).expect("bounded string"))?;
                    }
                    Keep::Text(_) | Keep::Value | Keep::Into(_) | Keep::Tagged(_) => {
                        push(collector, token)?;
                    }
                }
            }
            collector.position = after_value(collector);
            finish_field(collector);
        }
        Token::Number(_) | Token::True | Token::False | Token::Null => {
            if collector.position != Position::Ignored {
                push(collector, token)?;
            }
            collector.position = after_value(collector);
            finish_field(collector);
        }
    }
    Ok(())
}

fn prepare_tag(collector: &mut Collector, token: &Token) -> Result<bool, Error> {
    if collector.position == Position::Tag {
        match token {
            Token::String(value) => {
                select(collector, token, value)?;
                return Ok(true);
            }
            Token::ObjectStart
            | Token::ArrayStart
            | Token::ObjectEnd
            | Token::ArrayEnd
            | Token::Key(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => {
                let index = collector.walk.top().expect("tag frame").tag.expect("tag");
                if !has_parent(collector, index) {
                    return Err(Error::NotTagged);
                }
                collector.tags.get_mut(index).expect("tag").deferred_error = Some(Error::NotTagged);
                collector.position = Position::Ignored;
            }
        }
    }
    match collector.position {
        Position::Value(Keep::Tagged(_)) => match token {
            Token::ObjectStart | Token::ArrayEnd => {}
            Token::ObjectEnd
            | Token::ArrayStart
            | Token::Key(_)
            | Token::String(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => {
                match active_tag(collector) {
                    Some(index) => collector.tags.get_mut(index).expect("parent").fail_field(Error::NotTagged),
                    None => return Err(Error::NotTagged),
                }
                collector.position = Position::Ignored;
            }
        },
        Position::Value(Keep::Value | Keep::Text(_) | Keep::Into(_))
        | Position::Key
        | Position::Skip
        | Position::End
        | Position::Ignored
        | Position::Tag => {}
    }
    Ok(false)
}

fn open(collector: &mut Collector, token: &Token) -> Result<(), Error> {
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
        | Token::Null => unreachable!("container start"),
    };
    let mut keep = current_keep(collector);
    let mut stored = collector.position != Position::Ignored;
    match keep {
        Keep::Tagged(_) if kind == Kind::ArrayStart => return Err(Error::NotTagged),
        Keep::Value | Keep::Text(_) => keep = Keep::Value,
        Keep::Into(_) | Keep::Tagged(_) => {}
    }
    let start = collector.document.len();
    let text_start = collector.document.text_len();
    let tag = match keep {
        Keep::Tagged(filter) => {
            let (tokens, text) = match active_tag(collector) {
                Some(parent) => collector.tags.get(parent).expect("parent").remaining(),
                None => (
                    collector.limits.tokens.checked_sub(collector.settled_tokens).expect("admitted settled tokens"),
                    collector.limits.text.checked_sub(collector.settled_text).expect("admitted settled text"),
                ),
            };
            let mut found = None;
            for index in 0..collector.tags.len() {
                let pending = collector.tags.get_mut(index).expect("tag");
                if pending.filter == filter && !pending.active {
                    pending.begin(collector.walk.len(), start, text_start, tokens, text);
                    pending.capture(token);
                    found = Some(index);
                    break;
                }
            }
            Some(found.expect("the static tagged node was allocated"))
        }
        Keep::Value | Keep::Text(_) | Keep::Into(_) => None,
    };
    if stored {
        stored = push(collector, token)?;
    }
    collector.walk.push(Frame { kind, keep, start, tag, stored }).expect("tokenizer admitted depth");
    collector.position = match kind {
        Kind::ObjectStart => Position::Key,
        Kind::ArrayStart => array_position(keep, stored, has_copy(collector)),
        Kind::ObjectEnd
        | Kind::ArrayEnd
        | Kind::Key
        | Kind::String
        | Kind::Number
        | Kind::True
        | Kind::False
        | Kind::Null
        | Kind::Long => unreachable!("container start"),
    };
    Ok(())
}

fn close(collector: &mut Collector, token: &Token) -> Result<(), Error> {
    let frame = *collector.walk.top().expect("kept container");
    if frame.stored {
        push(collector, token)?;
    }
    collector.walk.pop().expect("kept container");
    if let Some(index) = frame.tag {
        finish_tag(collector, index)?;
    }
    collector.position = after_value(collector);
    finish_field(collector);
    Ok(())
}

fn key(collector: &mut Collector, token: &Token, key: &[u8]) -> Result<(), Error> {
    let frame = *collector.walk.top().expect("key's object");
    let selected = match frame.tag {
        Some(index) => {
            let pending = collector.tags.get_mut(index).expect("allocated tag");
            match pending.key(key, collector.tokenizer.token_wire().1, collector.document.len()) {
                Ok(()) => {}
                Err(error) => {
                    pending.deferred_error = Some(error);
                    collector.position = Position::Ignored;
                    return Ok(());
                }
            }
            if key == pending.filter.tag {
                push(collector, token)?;
                collector.position = Position::Tag;
                return Ok(());
            }
            pending.keep(key)
        }
        None => match frame.keep {
            Keep::Value | Keep::Text(_) => Some(Keep::Value),
            Keep::Into(nodes) => field(nodes, key),
            Keep::Tagged(_) => unreachable!("tag has runtime state"),
        },
    };
    collector.position = match selected {
        Some(keep) if frame.stored => {
            match frame.keep {
                Keep::Into(_) => {
                    if duplicate(&collector.document, frame.start, key) {
                        match active_tag(collector) {
                            Some(index) => {
                                collector.tags.get_mut(index).expect("tag").fail_field(Error::Duplicate);
                                collector.position = Position::Ignored;
                                return Ok(());
                            }
                            None => return Err(Error::Duplicate),
                        }
                    }
                }
                Keep::Value | Keep::Text(_) | Keep::Tagged(_) => {}
            }
            if push(collector, token)? { Position::Value(keep) } else { ignored_position(collector) }
        }
        Some(_) | None => ignored_position(collector),
    };
    Ok(())
}

fn has_parent(collector: &Collector, index: u32) -> bool {
    for other in 0..collector.tags.len() {
        if other != index && collector.tags.get(other).expect("tag").active {
            return true;
        }
    }
    false
}

fn has_copy(collector: &Collector) -> bool {
    for tag in &collector.tags {
        if tag.active && tag.copy_error.is_none() {
            return true;
        }
    }
    false
}

fn ignored_position(collector: &Collector) -> Position {
    if has_copy(collector) { Position::Ignored } else { Position::Skip }
}

fn charge(collector: &mut Collector, tokens: u32, text: u32) -> Result<bool, Error> {
    match active_tag(collector) {
        Some(index) => {
            let tag = collector.tags.get_mut(index).expect("active tag");
            let stored = tag.charge(tokens, text);
            let error = tag.selected_error();
            if !has_parent(collector, index)
                && let Some(error) = error
            {
                return Err(error);
            }
            Ok(stored)
        }
        None => {
            collector.settled_tokens = collector.settled_tokens.checked_add(tokens).ok_or(Error::TooManyTokens)?;
            if collector.settled_tokens > collector.limits.tokens {
                return Err(Error::TooManyTokens);
            }
            collector.settled_text =
                collector.settled_text.checked_add(text).ok_or(Error::TooMuchText { cap: None })?;
            if collector.settled_text > collector.limits.text {
                return Err(Error::TooMuchText { cap: None });
            }
            Ok(true)
        }
    }
}

fn push(collector: &mut Collector, token: &Token) -> Result<bool, Error> {
    let text = match token {
        Token::Key(bytes) | Token::String(bytes) | Token::Number(bytes) => {
            u32::try_from(bytes.len()).expect("bounded token")
        }
        Token::ObjectStart
        | Token::ObjectEnd
        | Token::ArrayStart
        | Token::ArrayEnd
        | Token::True
        | Token::False
        | Token::Null => 0,
    };
    if !charge(collector, 1, text)? {
        return Ok(false);
    }
    if let Err(error) = collector.document.push(token) {
        return Err(document_error(error));
    }
    Ok(true)
}

fn push_long(collector: &mut Collector, length: u64) -> Result<(), Error> {
    if charge(collector, 1, 0)?
        && let Err(error) = collector.document.push_long(length)
    {
        return Err(document_error(error));
    }
    Ok(())
}

fn document_error(error: document::Error) -> Error {
    match error {
        document::Error::TooManyTokens => Error::TooManyTokens,
        document::Error::TooMuchText => Error::TooMuchText { cap: None },
        document::Error::Range => unreachable!("builder checks offsets"),
    }
}

fn long(collector: &mut Collector, length: u64) -> Result<(), Error> {
    begin_value(collector);
    capture_long(collector);
    match collector.position {
        Position::Tag => return Err(Error::Tokenizer(json::Error::StringTooLong)),
        Position::Key => match collector.walk.top().expect("key frame").keep {
            Keep::Value | Keep::Text(_) => return Err(Error::Tokenizer(json::Error::StringTooLong)),
            Keep::Into(_) | Keep::Tagged(_) => collector.position = ignored_position(collector),
        },
        Position::Value(Keep::Text(_)) => {
            push_long(collector, length)?;
            collector.position = after_value(collector);
            finish_field(collector);
        }
        Position::Value(Keep::Value | Keep::Into(_) | Keep::Tagged(_)) => {
            match active_tag(collector) {
                Some(index) => {
                    let tag = collector.tags.get_mut(index).expect("tag");
                    tag.strict_long(length, collector.limits.tokenizer.string);
                    let error = tag.selected_error();
                    if !has_parent(collector, index)
                        && let Some(error) = error
                    {
                        return Err(error);
                    }
                }
                None => {
                    let room = collector.limits.text.checked_sub(collector.settled_text).expect("admitted text");
                    return Err(if collector.limits.tokenizer.string <= room {
                        Error::Tokenizer(json::Error::StringTooLong)
                    } else {
                        Error::TooMuchText { cap: None }
                    });
                }
            }
            collector.position = after_value(collector);
            finish_field(collector);
        }
        Position::Ignored => {
            collector.position = after_value(collector);
            finish_field(collector);
        }
        Position::Skip | Position::End => unreachable!("capped demand"),
    }
    Ok(())
}

fn select(collector: &mut Collector, token: &Token, value: &[u8]) -> Result<(), Error> {
    let index = collector.walk.top().expect("tag's object").tag.expect("tagged frame");
    let pending = collector.tags.get_mut(index).expect("allocated tag");
    let selected = pending.select(value);
    match selected {
        Ok(()) => {}
        Err(error) => {
            if !has_parent(collector, index) {
                return Err(error);
            }
            collector.tags.get_mut(index).expect("tag").deferred_error = Some(error);
        }
    }
    push(collector, token)?;
    collector.position = Position::Key;
    finish_field(collector);
    Ok(())
}

fn begin_value(collector: &mut Collector) {
    match collector.position {
        Position::Value(_) | Position::Ignored | Position::Tag => {
            if let Some(frame) = collector.walk.top()
                && let Some(index) = frame.tag
            {
                collector.tags.get_mut(index).expect("tag").begin_value(collector.tokenizer.token_wire().0);
            }
        }
        Position::Key | Position::Skip | Position::End => {}
    }
}

fn finish_field(collector: &mut Collector) {
    if let Some(frame) = collector.walk.top()
        && let Some(index) = frame.tag
    {
        collector
            .tags
            .get_mut(index)
            .expect("tag")
            .finish_field(collector.tokenizer.token_wire().1, collector.document.len());
    }
}

fn finish_tag(collector: &mut Collector, index: u32) -> Result<(), Error> {
    let pending = collector.tags.get_mut(index).expect("allocated tag");
    let outcome = pending.finish(&mut collector.document);
    pending.active = false;
    let start = pending.start;
    let text_start = pending.text_start;
    match outcome {
        Err(error) => match active_tag(collector) {
            Some(parent) => {
                collector.tags.get_mut(parent).expect("parent tag").fail_field(error);
                collector.document.truncate(start, text_start);
                Ok(())
            }
            None => Err(error),
        },
        Ok((tokens, text, skipped)) => {
            let stored = match active_tag(collector) {
                Some(parent) => collector.tags.get_mut(parent).expect("parent tag").charge_result(
                    tokens,
                    text,
                    skipped,
                    collector.limits.skip,
                ),
                None => {
                    collector.skipped = collector.skipped.checked_add(skipped).ok_or(Error::SkippedTooLong)?;
                    if collector.skipped > collector.limits.skip {
                        return Err(Error::SkippedTooLong);
                    }
                    charge(collector, tokens, text)?
                }
            };
            if !stored {
                collector.document.truncate(start, text_start);
            }
            Ok(())
        }
    }
}

fn after_value(collector: &Collector) -> Position {
    match collector.walk.top() {
        None => Position::End,
        Some(frame) => match frame.kind {
            Kind::ObjectStart => Position::Key,
            Kind::ArrayStart => array_position(frame.keep, frame.stored, has_copy(collector)),
            Kind::ObjectEnd
            | Kind::ArrayEnd
            | Kind::Key
            | Kind::String
            | Kind::Number
            | Kind::True
            | Kind::False
            | Kind::Null
            | Kind::Long => unreachable!("open container"),
        },
    }
}

fn array_position(keep: Keep, stored: bool, copy: bool) -> Position {
    if !stored {
        return if copy { Position::Ignored } else { Position::Skip };
    }
    match keep {
        Keep::Value | Keep::Text(_) => Position::Value(Keep::Value),
        Keep::Into(nodes) => {
            for node in nodes {
                match node.key {
                    Key::Each => return Position::Value(node.keep),
                    Key::Field(_) => {}
                }
            }
            if copy { Position::Ignored } else { Position::Skip }
        }
        Keep::Tagged(_) => unreachable!("tagged arrays are refused"),
    }
}

fn field(nodes: &'static [Node], key: &[u8]) -> Option<Keep> {
    for node in nodes {
        match node.key {
            Key::Field(name) if name == key => return Some(node.keep),
            Key::Field(_) | Key::Each => {}
        }
    }
    None
}

fn duplicate(builder: &Builder, start: u32, key: &[u8]) -> bool {
    let mut depth = 0_u32;
    for index in start..builder.len() {
        let record = builder.token(index).expect("retained record");
        match record.kind {
            Kind::ObjectStart | Kind::ArrayStart => depth = depth.checked_add(1).expect("tokenizer depth"),
            Kind::ObjectEnd | Kind::ArrayEnd => depth = depth.checked_sub(1).expect("open object"),
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
