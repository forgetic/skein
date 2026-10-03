# JSON

Provisional, 2026-10-03. The design of `skein-json`: a bounded JSON
tokenizer, pulled by demand, and a sized writer. It is a step machine of
a connection's stack (programming-model.md, 4), and depends on lib only.

## 1. In one page

- **Tokens, not documents.** The tokenizer turns bytes into a stream of
  tokens, by demand. An application decodes its own documents from the
  tokens, with small state machines, into its domain's types.
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

// skein_json::writer, not a machine: a pass over the caller's own calls
Encoder::measure(&limits) -> Encoder      Encoder::write(len, &limits) -> Encoder
```

Both speak `skein_json::Token`: what the tokenizer reads, the writer
writes back.

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
pub enum Request { Next, Close }                 // from the side above

pub enum Event {                                 // to the side above
    Token(Token),                                // for a Next
    Done,                                        // for a Next: the document is whole
    Failed(Error),                               // for a Next: it is not, or the stream failed
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
  outcome or the close: the side above's bug otherwise, asserted.
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
  `Next`, for bytes from below, for a `Close` after its outcome, or for
  nothing once closed. Machines keep no timers (programming-model.md, 4):
  the connection arms its progress deadline while it waits for bytes.
- **Progress is counted in tokens,** not in demands met. Between tokens
  and within a number the tokenizer demands one byte at a time, and
  single bytes are not progress (programming-model.md, 7): the connection
  counts a token sent up, or the chunks received below it, and
  `Limits::length` bounds the bytes a peer can send for any one token.

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
    pub string: u32,   // a string or key, in bytes once unescaped: past it, StringTooLong
    pub number: u32,   // a number's text: past it, NumberTooLong
    pub chunk: u32,    // the most of a string's text demanded at once: a scan's max, at least 1
    pub length: u32,   // the document, in bytes delivered, whitespace included: past it, TooLong
}
```

- **`length`** is counted a delivery at a time, before the delivery is
  read: a delivery that would pass it fails the document. Without it, a
  peer could hold one `Next` unanswered for ever with whitespace.
- **`worst_case(&limits)`** is the stack of open containers and one
  buffer for the text being read, the longer of `string` and `number`,
  both allocated with the tokenizer, and the delivery it reads, at most
  `largest_demand`: a delivery is its receiver's to count (lib.md, 7),
  held for the step that reads it. A token's box is the side above's to
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
let len = measure.measured()?;           // Refusal, before anything is allocated
let mut write = Encoder::write(len, &limits);
encode(&mut write, &call);
let body = write.finish();               // exactly len bytes
```

- **Calls:** `object_start`, `object_end`, `array_start`, `array_end`,
  `key`, `string`, `number` (validated text, a `Token::Number`'s),
  `unsigned`, `signed` (through `lib::Decimal`), `boolean`, `null`, and
  `token`, which writes any `Token`.
- **What the caller's data gets wrong is a `Refusal`** of the measuring
  pass: `Text` (not UTF-8), `Number`, `TooDeep`, `TooLong`; the first one
  met, and the length last. What only its code gets wrong (a key outside
  an object, an end that closes nothing, a second value, a writing pass
  unlike its measuring pass) is a bug, asserted.
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

## 6. Testing

- **Step tests** (`crates/skein-json/src/tests.rs`): each demand, each
  answer, the end and a failure of the stream while idle and while
  reading, a close in each state, every limit at and past its edge, every
  escape and bad escape, UTF-8 against the standard library's check over
  every two-byte sequence and the edges of longer ones, a number's
  grammar against a plain reading of it over every short text of its
  bytes, the writer's escapes, refusals and assertions, and its output
  read back.
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
  length.
- **The fuzzy suite** (`tests/json/tests/fuzzy_*.rs`): 20,000 generated
  documents, most of them mutated, and 5,000 transcripts cut and mutated,
  under limits and neighbours drawn from each seed, each against the
  reference. Each sweep asserts that what it injects fell
  (testing-strategy.md, 3): every error; every way of ending, an end that
  crossed a demand and one after a number's last byte; each fault, a
  failure while the tokenizer waited for each thing and after the end;
  a delivery after the close; a stall that filled the stream; and a close
  in every state. Then 5,000 generated documents written and read back;
  and 20,000 of each of texts and numbers drawn at random, which the
  writer writes exactly when the standard library and the grammar accept
  them. It
  stands in for the fuzz target until a nightly toolchain is installed.

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
  length, and only while it is read.
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
  serde or traits. If that hurts, the candidate is a generator that turns
  a schema into plain step code at build time, with its output checked in
  and reviewed. Procedural macros stay out.
- **Text that is not UTF-8,** such as a command's output going to an LLM,
  is refused by the writer. A caller that must send it replaces what is
  not UTF-8 first; whether the writer should offer that is open.

## 9. Not built yet

- **The fuzz target** (`fuzz/`, fed `Bytes` under every demand), which
  waits for a nightly toolchain; the fuzzy suite stands in for it.
- **Writing in pieces:** temper's performance.md asks for a body measured
  whole and encoded a piece at a time, as io grants room. The writer
  writes a document whole.
- **Several documents in one stream,** concatenated or as JSON lines.
- **Protocol worlds,** once the tokenizer is stacked under a service's
  decoder and over HTTP or server-sent events (http.md, 5).
