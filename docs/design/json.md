# JSON

Provisional, 2026-10-03; revised 2026-10-09. The design of `skein-json`:
a bounded JSON tokenizer, pulled by demand, a selective collector over it,
and a sized writer. The tokenizer and the collector are step machines of
a connection's stack (programming-model.md, 4), and the crate depends on
lib only.

## 1. In one page

- **Tokens, not documents.** The tokenizer turns bytes into a stream of
  tokens, by demand. An application decodes its own documents from the
  tokens, with small state machines, into its domain's types.
- **Keep what is read, scan the rest.** The selective collector keeps the
  values an application names by path and scans every other one: checked
  as JSON and counted, never stored. Its nesting is a bounded stack and
  its counts run as it reads, so what a peer adds that the application
  does not read costs a scan, not memory.
- **Compact documents.** What is kept is one buffer of text and a list
  of fixed-size tokens that point into it, not a box per token.
- **Bounded everywhere.** Nesting is held in a `lib::Stack` of configured
  depth, never in recursion; strings, numbers and the whole document are
  under maximum lengths.
- **No floats.** Numbers go up as validated text; the consumer parses the
  integers it expects, with checks.
- **The writer is sized:** measure the document first, then write it,
  escaped, into a box of exactly its length.

## 2. In skein

`skein-json` depends on lib only. A service's protocol layer stacks it
over whatever carries the documents (an HTTP body, server-sent events'
data) and under its own decoders (http.md, 2). It has its own `Limits`
and `worst_case`, and each entry point declares its `MAX_OUT`.

```rust
// skein_json::tokenizer, the machine
pub fn up(tokenizer: &mut Tokenizer, env: &Env<Limits>, ev: stream::Up,
          above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
pub fn down(tokenizer: &mut Tokenizer, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<stream::Down>)

// skein_json::collector, the machine: the tokenizer's shape, over the same stream
pub fn up(collector: &mut Collector, env: &Env<collector::Limits>, ev: stream::Up,
          above: &mut Queue<collector::Event>, below: &mut Queue<stream::Down>)
pub fn down(collector: &mut Collector, env: &Env<collector::Limits>, rq: collector::Request,
            above: &mut Queue<collector::Event>, below: &mut Queue<stream::Down>)

// skein_json::writer, not a machine: a pass over the caller's own calls
Encoder::measure(&limits) -> Encoder      Encoder::write(len, &limits) -> Encoder
```

Both machines and the writer speak `skein_json::Token`: what the tokenizer
reads, the writer writes back. The collector hands up a `Document`
(section 5.2), which the writer writes back too.

## 3. The tokenizer

- **Pulled by demand:** it demands what it needs from the stream below,
  and emits a token when the side above has room for it.
- **Nesting** is held in a `lib::Stack` of configured depth, since the
  depth of nested input is the peer's choice (programming-model.md,
  section 8). Past the depth, the document is refused.
- **Strings** are unescaped and checked as UTF-8, into exact-size
  `Box<[u8]>`s under a maximum length.
- **Numbers** go up as validated text, never as floats.

### 3.1 The side above

The vocabulary is the tokenizer's own; the service's decoder translates
it into its domain's.

```rust
pub enum Request {                               // from the side above
    Next,                                        // one token, a string's text kept under Limits::string
    Text(u32),                                   // one token, a string's text kept only up to this many bytes
    Skip,                                        // the next value whole, read and checked, kept nowhere
    Close,
}

pub enum Event {                                 // to the side above
    Token(Token),                                // for a Next or a Text
    Long(u64),                                   // for a Text: a longer string, read to its end; its length unescaped
    Skipped(u64),                                // for a Skip: the value's bytes as delivered
    Done,                                        // for a Next, a Text or a Skip: the document is whole
    Failed(Error),                               // for the same: it is not, or the stream failed
    Closed,                                      // for a Close: terminal
}

pub enum Token {
    ObjectStart, ObjectEnd, ArrayStart, ArrayEnd,
    Key(Box<[u8]>),                              // unescaped UTF-8, exactly its length
    String(Box<[u8]>),                           // the same
    Number(Box<[u8]>),                           // the document's text, validated
    True, False, Null,
}
```

- **`Next` is the side above's room:** a demand for one token. Exactly
  one event answers it: the next `Token`, or the document's outcome,
  `Done` or `Failed`, after which no token follows. The tokenizer reads
  nothing while no `Next` is pending, so a decoder that stops asking stops
  the reading, and the backpressure chain runs on below it
  (programming-model.md, 7). One `Next` at a time, and none after the
  outcome or the close: the side above's bug otherwise, asserted. `Text`
  and `Skip` are demands too, and the same rules hold for them.
- **`Text(max)` caps one string.** It is a `Next` whose string, if the
  next token is one, is kept only when its text fits `max` bytes, at most
  `Limits::string`. A longer string is read to its closing quote, still
  checked, and answered `Long` with its length once unescaped; nothing of
  it is kept. Any other token answers as for a `Next`.
- **`Skip` passes over one value.** Asked where a value may come (the
  document's start, after a key, or at an array's next element), it reads
  the value through, nested containers and all, checking its grammar, its
  escapes, its UTF-8 and its depth, and answers `Skipped` with the bytes
  delivered for it. No text of it is kept, so its strings are bounded by
  `Limits::length` alone, never by `Limits::string`. At an array's end it
  answers the `ArrayEnd` token instead, as there is no value to skip.
  Asked where a key comes, it is the side above's bug, asserted.
- **`Done` waits for the end of the stream.** A stream carries one
  document (RFC 8259's `ws value ws`), so after the document's last token
  the tokenizer reads to the end: whitespace, then `End`, is `Done`;
  anything else fails as `Trailing`. A number that is the whole document
  ends at the end of the stream.
- **`Close` ends the tokenizer in any state.** It withdraws what it
  demanded below, drops a `Next` not yet answered, and answers `Closed`,
  its one terminal event; nothing follows. It does not close the stream
  below, which its owner closes: the stream vocabulary has no close.
- **Errors** are values: `Unexpected` (a byte the grammar does not allow
  there), `Trailing`, `TooLong`, `TooDeep`, `StringTooLong`, `NumberTooLong`,
  `Number` (a number that is not one: `01`, `1.`, `-`), `Escape`,
  `Surrogate` (half a pair), `Utf8`, `Control` (an unescaped control
  character), `Truncated` (the stream ended first), and `Stream(Fault)`.
- **What it waits for** is a function of its state, `waiting()`: for a
  `Next` (or a `Text` or a `Skip`), for bytes from below, for a `Close`
  after its outcome, or for nothing once closed. Machines keep no timers
  (programming-model.md, 4): the connection arms its progress deadline
  while it waits for bytes.
- **Progress is counted in tokens,** not in demands met. Between tokens
  and within a number the tokenizer demands one byte at a time, and
  single bytes are not progress (programming-model.md, 7): the connection
  counts a token sent up, or the chunks received below it, and
  `Limits::length` bounds the bytes a peer can send for any one token. A
  `Skip` over a long value sends nothing up until its end, so there only
  the chunks received count.

### 3.2 The side below

The tokenizer demands only what the token it is reading needs, and never
past the document:

| Reading | Demand |
|---|---|
| between tokens, skipping whitespace | `Fill(1)` |
| the rest of `true`, `false`, `null`, after the first letter | `Fill(3)`, `Fill(4)`, `Fill(3)` |
| a string's or a key's text | `Scan { until: '"', max: chunk }`, again after a quote that was escaped |
| a number | `Fill(1)` per byte; the byte that ends it is held for the next token |

It sends nothing down, so it asks for no room. The side below meets the
contract of a stream (lib.md, 7), and the tokenizer keeps its side of
it: it states a demand only after an answer, withdraws one only when it
closes, and reads by JSON's framing, so that no read of a whole document
is larger than what is left of it. What that contract leaves to the side
above, a scan that meets no quote within its maximum, is for the
tokenizer one piece of a string longer than a scan, and it reads on.
What a scan does not deliver before the end of the stream is never seen:
a string cut short is `Truncated`, whatever its last bytes hold.

### 3.3 Limits

```rust
pub struct Limits {
    pub depth: u32,    // objects and arrays nested: past it, TooDeep
    pub string: u32,   // a string or key kept, in bytes once unescaped: past it, StringTooLong
    pub number: u32,   // a number's text: past it, NumberTooLong
    pub chunk: u32,    // the most of a string's text demanded at once: a scan's max, at least 1
    pub length: u32,   // the document, in bytes delivered, whitespace included: past it, TooLong
}
```

- **`length`** is counted a delivery at a time, before the delivery is
  read: a delivery that would pass it fails the document. Without it, a
  peer could hold one `Next` unanswered for ever with whitespace, or one
  `Skip` with a value that never ends.
- **`string`** bounds only what is kept. A string skipped, or read past
  its `Text` cap, holds none of the text buffer.
- **`worst_case(&limits)`** is the stack of open containers and one
  buffer for the text being read, the longer of `string` and `number`,
  both allocated with the tokenizer, and the delivery it reads, at most
  `largest_demand`: a delivery is made to the tokenizer's demand, so it
  counts it, held for the step that reads it (testing.md, 5). A token's box is the side above's to
  count once emitted. `None` for a `chunk` of zero.
- **`largest_demand(&limits)`** is the larger of `chunk` and 4 (the rest
  of `false`). Whoever stacks the tokenizer checks at startup that it fits
  the cap of the side below: a demand past that cap could never be met,
  and the side below asserts it (lib.md, 7).
- **`UP_MAX_OUT` and `DOWN_MAX_OUT`** are one event above and one request
  below each: a call emits at most one of each. Only a `Close` of a
  reading tokenizer emits both, `Closed` and the demand withdrawn; an end
  or a failure while idle, and anything after the outcome or the close,
  emit nothing.

### 3.4 Text

- **Escapes:** `\" \\ \/ \b \f \n \r \t` and `\uXXXX`; a high surrogate's
  escape must be followed at once by a low one's, and the pair is one
  character; a lone half of a pair is `Surrogate`. `\u0000` is a NUL in
  the text, which is valid UTF-8.
- **UTF-8 is checked a byte at a time** (RFC 3629, 4): no overlong form,
  no surrogate, nothing past U+10FFFF. A string's text arrives in scans
  cut anywhere, so a character may span two of them, and the check keeps
  its place between them.
- **The limit is decided at a character's first byte,** for the whole
  character, so whether a raw character fits never waits on whether its
  next bytes are valid; and at an escape's last byte, once it is known to
  spell a character, for what it spells. A surrogate pair cut by a bad
  digit is then an `Escape`, whatever the room left.

### 3.5 Numbers

A number's text is checked against RFC 8259, 6, a byte at a time. A
digit, `.`, `e`, `E`, `+` or `-` where the grammar allows none of them
makes it `Number` at once (`01`, `1.e5`, `2-1`); any other byte ends it.
Nothing converts it: the consumer parses the integers it expects.

## 4. The writer

Measure first, then write with escaping into `Writer::new(len)`. A
document's length is the writer's own computation, so every write fits,
and finishing short is an assertion.

There is no value to build. The caller writes one function that makes
its own calls for its own data, and runs it on an `Encoder` twice: once
measuring, once writing.

```rust
fn encode(json: &mut Encoder, call: &Call) {
    json.object_start();
    json.key(b"name");
    json.string(&call.name);
    json.key(b"max_tokens");
    json.unsigned(call.max_tokens);
    json.object_end();
}

let mut measure = Encoder::measure(&limits);
encode(&mut measure, &call);
let len = measure.measured()?;           // Refusal, before the document is allocated
let mut write = Encoder::write(len, &limits);
encode(&mut write, &call);
let body = write.finish();               // exactly len bytes
```

- **Calls:** `object_start`, `object_end`, `array_start`, `array_end`,
  `key`, `string`, `number` (validated text, a `Token::Number`'s),
  `unsigned`, `signed` (through `lib::Decimal`), `boolean`, `null`,
  `token`, which writes any `Token`, and `document`, which writes a
  compact document's tokens in order (section 5.2). A `Long` has no text
  to write: writing one is a bug of the caller's code, asserted.
- **What the caller's data gets wrong is a `Refusal`** of the measuring
  pass: `Text` (not UTF-8), `Number`, `TooDeep`, `TooLong`; the first one
  met, and the length last. What only its code gets wrong (a key outside
  an object, an end that closes nothing, a second value, a writing pass
  of another length than its measuring pass) is a bug, asserted.
- **`Limits { depth, length }`**, and `worst_case`: the stack and the
  document, which the writing pass holds until `finish` hands it over.
- **Compact:** no whitespace. A string escapes `"`, `\` and the control
  characters, the common ones in their short forms and the rest as
  `\u00XX`; every other byte, `/` and non-ASCII included, is written as it
  is.

## 5. Decoding is the application's

An application decodes its own documents with small state machines over
these tokens, in its own protocol layer, into its domain's types.
Structure the domain acts on is decoded on the way in, all of it: a tool
call inside an LLM's answer reaches the domain as a typed call, not as
JSON to be sent back down for decoding later (programming-model.md, 4).

The collector does not change that: it decides what reaches the
application's decoder, which still decodes it. It is generic: an LLM
dialect, a forge client or a webhook uses it the same way.

### 5.1 The selective collector

```rust
pub enum Request { Collect, Close }              // from the side above

pub enum Event {                                 // to the side above
    Collected(Document),                         // for a Collect: what was kept, the document whole
    Failed(Error),                               // for a Collect: not whole, or past a count
    Closed,                                      // for a Close: terminal
}

pub struct Filter { root: Keep }                 // what is kept; anything not named is scanned
pub struct Cap(u8);                              // a named cap: an index into the collector's caps

pub enum Keep {
    Value,                                       // the value whole
    Text(Cap),                                   // a string up to the cap's bytes; a longer one, its length only
    Into(&'static [Node]),                       // a container, with only the children named
    Tagged(&'static Tagged),                     // an object kept by its tag's value
}

pub struct Node { key: Key, keep: Keep }
pub enum Key { Field(&'static [u8]), Each }      // an object's field, or every element of an array

pub struct Tagged {
    tag: &'static [u8],                          // the field whose string value selects a variant
    known: &'static [Variant],                   // the tag values it knows, each with its children
    unknown: Cap,                                // an unknown tag's object is kept whole, up to this cap
}
pub struct Variant { value: &'static [u8], children: &'static [Node] }

pub struct Limits {
    pub tokenizer: tokenizer::Limits,
    pub tokens: u32,   // tokens kept: past it, TooManyTokens
    pub text: u32,     // bytes of text kept, unescaped: past it, TooMuchText
    pub skip: u64,     // bytes scanned and not kept: past it, SkippedTooLong
}
```

- **One machine over one stream.** The collector owns a tokenizer and is
  its side above, so it meets the stream below through it: the
  tokenizer's demands, waits and progress are its own. `Collect` asks for
  the stream's one document; exactly one event answers it: `Collected`
  once the document is whole, or `Failed`. `Close` ends it in any state,
  closing the tokenizer, and answers `Closed`.
- **A filter names what is kept,** as a tree of paths written by hand as
  static data. Its caps are named, not numbered: a `Cap` is an index
  into the caps the collector is built with, which the application
  derives from its limits at startup (llm.md, 4.4). One filter thus
  serves every set of derived limits, and a test lowers a cap without
  writing another filter. At each value it walks to, the collector looks the value's
  key up among the children of the node it is in (`Each` for an array's
  elements):
  - `Value` keeps the value whole, token by token;
  - `Text(n)` keeps a string up to `n` bytes, through the tokenizer's
    `Text`; a longer one is kept as a `Long` token holding its length
    alone; a value of another kind is kept whole;
  - `Into` keeps a container's start and end and walks into it, keeping
    only the children it names; a value of another kind is kept whole;
  - `Tagged` keeps an object by the string value of its tag field:
    - **the tag first:** a known value selects its variant, whose children
      are kept as `Into` keeps them; an unknown one keeps the object whole,
      up to the unknown cap;
    - **the tag later, or never:** until the tag is read, the collector
      keeps two candidates side by side:
      - the object projected through every known variant's children
        together, counted per variant: each variant's own tokens and text
        run against `tokens` and `text`, as if it alone were kept;
      - the object whole, copied up to the unknown cap and, past it, only
        marked as over.

      A variant whose own count passes a limit is marked over. The
      collector stops storing what only over-variants keep, and fails
      nothing yet. At the tag, a known value keeps its variant's
      projection and drops the rest, or fails if that variant is over,
      naming the count it passed. An unknown value, or an object that
      ends with no tag, keeps the copy, or fails past the unknown cap
      (`TooMuchText`, naming the cap). So a known object fails only on
      what its own variant keeps, whatever the order of its fields;
    - the tag given twice, or not a string, fails (`Duplicate`,
      `NotTagged`);
  - a key the node does not name is read, compared and dropped, and its
    value skipped through the tokenizer's `Skip`.
- **The result is itself a document.** What is kept goes up in document
  order with the keys and containers on its path, so the application's
  decoder walks it as it would walk the whole: the same small state
  machines, over less.
- **Bounded by a stack and running counts.** Where the collector is in the
  filter is a `lib::Stack` no deeper than the filter, and a skipped
  value's nesting is the tokenizer's own bounded stack. Three counts run
  as it reads, each checked as it grows: tokens kept, text kept and bytes
  skipped. The first past its limit fails the document, naming itself.
- **Skipped values are still checked.** A value the collector does not
  keep is read as JSON, so a document that is not one fails wherever the
  fault lies; skipping changes what is kept, never what is accepted.
- **Duplicates.** A field the filter names, given twice in one object,
  fails the document (`Duplicate`): a decoder must never have to choose.
  Fields it does not name may repeat, uninterpreted.
- **Errors** are the tokenizer's, and `TooManyTokens`, `TooMuchText`
  (with the cap it passed, if a named one), `SkippedTooLong`, `Duplicate`
  and `NotTagged`.
- **`worst_case(&limits, &caps, &filter)`** is the tokenizer's, the walk
  stack, and the buffers the document is kept in, at `tokens` and `text`.
  Each `Tagged` node on the filter's deepest path adds its provisional
  candidates: one projection at `tokens` and `text` for each known
  variant past the first, and one whole copy at its unknown cap. The
  filter is static, so the collector computes this once, at
  construction. All of it is allocated with the collector; and the document being handed up, of the same size at
  most, which is the side above's once emitted. A skipped value adds
  nothing to it, whatever its size.
- **Restarting.** Once its outcome is out, a collector, and its tokenizer,
  may be restarted on the next stream, keeping their buffers, so a stream
  of small documents (one per server-sent event) costs one allocation of
  each, not one per document.
- **`UP_MAX_OUT` and `DOWN_MAX_OUT`** are the tokenizer's: one event
  above and one request below each.

### 5.2 Compact documents

```rust
pub struct Document { text: Box<[u8]>, tokens: Box<[Compact]> }

pub struct Compact { kind: Kind, start: u32, len: u32 }  // its text: text[start..start + len]

pub enum Kind {
    ObjectStart, ObjectEnd, ArrayStart, ArrayEnd,
    Key, String, Number,                         // their text in the buffer, unescaped; numbers as validated text
    True, False, Null,
    Long,                                        // a string past its Text cap: len is its length, no text
}
```

- **One buffer, fixed-size tokens.** Every kept key's, string's and
  number's text is written once into one buffer, and each token is a
  small fixed-size record pointing into it. A document costs its text and
  a few bytes a token, in two allocations, instead of a box and a tagged
  value for every token.
- **Read by index.** `Document::token(i)` gives a borrowed view, its kind
  and its text, so a decoder walks a document as it walks the tokenizer's
  tokens, and copies out only what it keeps.
- **Written back whole.** The writer's `document` call writes a document's
  tokens in order (section 4), so a value kept for replay goes back as it
  came, escaped afresh.
- **Any bounded value.** A document need not come from the collector: a
  filter of `Value` at the root keeps a whole document compact, which is
  how an application keeps an opaque value, such as an LLM provider's
  replay metadata or a tool's schema (llm.md).
- **Offsets are `u32`**, as a document's text is under the collector's
  `text` limit.

## 6. Testing

- **Step tests** (`crates/skein-json/src/tests.rs`): each demand, each
  answer, the end and a failure of the stream while idle and while
  reading, a close in each state, every limit at and past its edge, every
  escape and bad escape, UTF-8 against the standard library's check over
  every two-byte sequence and the edges of longer ones, a number's
  grammar against a plain reading of it over every short text of its
  bytes, the writer's escapes, refusals and assertions, and its output
  read back. The tokenizer's `Skip` over every kind of value at every
  place one may come, its count, and where it may not be asked; `Text`
  at, below and one past its cap, and the `Long` it answers. The
  collector: each disposition, a key named and one not, a duplicate named
  and unnamed, each count at its limit and one past it, a `Long` kept, a
  close in each state, and a restart that keeps its buffers. A compact
  document read by index and written back.
- **Machine worlds** (testing-strategy.md, 2.4) in `tests/json`
  (`skein-json-world`): one tokenizer from a seed, in one loop with both
  its neighbours.
  - The side below receives the peer's bytes in pieces cut at random,
    late, into an intake under its cap, and meets each demand exactly; it
    ends when the bytes run out, early at a cut, sometimes with nothing
    demanded and a demand crossing that end on its way; it fails, with
    each fault, before its end or after it; after a close, it may still
    deliver what was on its way, end, or fail. The tokenizer asks for no
    room, so there is none to grant late.
  - The side above asks for a token when it feels like it, stops asking
    for a while, so that the stream below fills to its cap with nothing
    demanded, and closes after the outcome, or at any moment: waiting for
    a `Next`, for bytes, or for the close.
  - The world checks the contracts as it goes: `MAX_OUT` on each call;
    one answer per `Next`, at most one outcome, `Closed` once and last;
    one demand at a time, none past the largest declared or the cap
    below, none of a stream that ended, one withdrawn only by a close;
    nothing sent down; and `waiting()` matching what the neighbours see.
    A seed replays to the same run.
  - **The collector's world** runs the same neighbours around one
    collector, with a filter drawn from each seed out of the document's
    own paths and some it lacks, `Text` caps among them, either side of
    their strings' lengths. What it collects is held to the reference's
    reading pruned by the filter, by a plain recursive function of the
    test's own, and its skipped count to the bytes of what was pruned.
- **A reference parser** in the world crate, recursive descent over a
  whole document, sharing no code with the tokenizer: the standard
  library judges UTF-8 and surrogate pairs, and a plain reading of the
  grammar judges numbers. It mirrors only what the tokenizer promises:
  which bytes its scans see, and the order of errors at one byte. Every
  run is checked against it: the same tokens in order, and the same
  outcome, unless the stream failed or the side above closed first.
- **Transcripts** in `tests/json/transcripts/`, each `<name>.json` beside
  `<name>.expect`, what it must decode to:
  - realistic: an Anthropic Messages API response ending in tool calls,
    three of its stream events and an error; an OpenAI chat completion
    with tool calls, their arguments JSON in a string, with `\u` escapes
    and a surrogate pair; a Forgejo pull request, a page of issues and a
    404, compact, with `<`, `>` and `&` escaped as Go's encoder does. No
    real answer can be captured offline, so each is written by hand after
    the API's published format, and says so; its expectation was drafted
    by the reference parser and checked by hand.
  - hostile: nested too deep, strings too long (and escapes that fit
    once undone), invalid and overlong UTF-8, an encoded surrogate, bad
    and short escapes, lone and swapped surrogates, control characters,
    numbers that are not numbers or are too long, a truncated response,
    a second document, a trailing comma, quotes and keys JavaScript
    allows, an empty stream, a byte order mark, and whitespace without
    end.

  Each decodes to its expectation under several seeds and scan maximums,
  and cut anywhere, as the reference reads the prefix. A transcript
  without an expectation fails, printing the reference's reading as a
  draft to check.
- **The writer against the tokenizer:** every transcript read whole, and
  generated documents, written and read back the same through the world;
  every byte a string may hold; and what the tokenizer refuses, the writer
  refuses to write.
- **Memory** (`tests/json/tests/memory.rs`, with the counting allocator,
  testing.md, 5): every call of an entry point is a step of the meter, and
  what the tokenizer or the encoder held of its own is never more than its
  `worst_case`. The connection's queues and stream are made before the
  meter; each delivery is made between steps and counts from when it is
  handed over; what a step emits is handed out. First the case that showed
  the delivery counts: a string at its limit and the delivery that closes
  it, held at once. Then a string, a key and a number at their limits,
  nesting at its depth and past it, the longest literal, escapes, failures,
  each closed and failed after every step; generated documents under tiny
  limits; and the writer's two passes for documents at their own depth and
  length. The collector under tiny limits (a handful of tokens, a few
  hundred bytes of text) over documents that skip megabytes: its high
  water is the same whatever it skipped, and a document kept at exactly
  its `tokens` and `text` reaches its worst case.
- **The fuzzy suite** (`tests/json/tests/fuzzy_*.rs`): 20,000 generated
  documents, most of them mutated, and 5,000 transcripts cut and mutated,
  under limits and neighbours drawn from each seed, each against the
  reference. Each sweep asserts that what it injects fell
  (testing-strategy.md, 3): every error; every way of ending, an end that
  crossed a demand and one after a number's last byte; each fault, a
  failure while the tokenizer waited for each thing and after the end;
  a delivery after the close; a stall that filled the stream; and a close
  in every state. Then 5,000 generated documents written and read back;
  and texts and numbers drawn at random, 20,000 of each, which the writer
  writes exactly when the standard library and the grammar accept them.
  The collector's: 10,000 generated and mutated documents under drawn
  filters, each against the pruned reference, asserting that every
  disposition, every count's failure, a duplicate, a `Long` and a skip of
  each kind of value fell; and documents with unnamed fields added at
  random, which never change what is collected, only what is skipped.
  It stands in for the fuzz target until a nightly toolchain is
  installed.

## 7. Decisions

- **The side above pulls one token per `Next`,** the token stream's
  equivalent of a fill: the tokenizer reads nothing it was not asked
  for, and each entry point emits at most one event up.
- **A stream carries one document,** read to its end. Several documents
  in one stream (concatenated, or JSON lines) wait for a user.
- **One text buffer, allocated at the limit,** with the tokenizer, and
  held for its life, idle too: a string's length is not known until its
  closing quote, and its pieces must be joined. A tokenizer then costs the
  whole limit from the start; the capped buffer that grows by doubling,
  proposed in temper's performance.md, would make a string cost only its
  length, and only while it is read. A string skipped, or past its `Text`
  cap, uses none of it, so the limit is the longest string an application
  keeps, not the longest a peer sends.
- **Selection by path, declared before reading.** What an application
  reads of a peer's document is known when it is written, so the filter
  is static data, and the collector decides each value as it reaches it,
  holding nothing to decide later. Collecting everything and discarding
  afterwards would size memory and token counts by what the peer chose to
  echo; skipping by a callback would need closures or traits, which step
  code does without (programming-model.md, 10.3).
- **A skip is read, not trusted.** A skipped value is checked as JSON, so
  a filter never widens what is accepted. Counting brackets alone would be
  cheaper, but would pass documents the reference refuses.
- **Compact tokens.** A box per string and a tagged value per token cost
  several times the text they hold. One text buffer with fixed-size
  records pointing into it costs the text once and a few bytes a token,
  and is written back without a copy of each token.
- **A string past its cap is a length, not a failure.** An application
  may want to know that a value was too large, and by how much, without
  holding it: the LLM client turns such a tool call into a per-call
  outcome (llm.md, section 2.4). A failure would lose the rest of the
  document.
- **UTF-8 is checked by the tokenizer's own code,** not by
  `core::str::from_utf8` under a scoped `#[expect]`. A string arrives in
  scans cut anywhere, so a character may span two of them; the check
  runs a byte at a time in the same pass that undoes escapes and counts
  the limit, and needs no exemption from the subset. The writer checks
  its text with the same code.
- **Errors at the same byte are decided in a fixed order,** so that an
  outcome does not depend on how the bytes were cut: a delivery's length
  before its bytes, a byte's validity before the limit it would pass, and
  a character's room at its first byte.

## 8. Open questions

- **Decoding by hand** into an application's types is verbose without
  serde or traits. If that hurts, the candidate is codec.md's generator,
  writing JSON codecs from schemas the way it writes binary ones, its
  output checked in and reviewed. Procedural macros stay out. The
  collector shortens what a decoder walks, not how it is written; the same
  generator could write a decoder's filter from the same schema.
- **Text that is not UTF-8,** such as a command's output going to an LLM,
  is refused by the writer. A caller that must send it replaces what is
  not UTF-8 first; whether the writer should offer that is open.

## 9. Not built yet

- **The selective collector, the tokenizer's `Text` and `Skip`, and
  compact documents** (sections 3.1, 5.1 and 5.2). Their first user is
  skein's LLM client, whose events they decode as they stream (llm.md,
  section 4.4).
- **The fuzz target** (`fuzz/`, fed `Bytes` under every demand), which
  waits for a nightly toolchain; the fuzzy suite stands in for it.
- **Writing in pieces:** temper's performance.md asks for a body measured
  whole and encoded a piece at a time, as io grants room. The writer
  writes a document whole.
- **Several documents in one stream,** concatenated or as JSON lines.

The protocol worlds stack the tokenizer over HTTP and server-sent events,
and the writer under them, at both ends of an LLM exchange (http.md, 6);
once built, the collector takes the tokenizer's place on each event's
data stream.
