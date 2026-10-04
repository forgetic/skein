# HTTP

Provisional, 2026-10-04. The design of `skein-http`: the protocol
machines for HTTP/1.1, client and server, and for server-sent events,
reader and writer. They are step machines of a connection's stack
(programming-model.md, 4): each depends on lib only, and meets the
stream below it and the user above it through entry points of the
model's shape. The client and the reader are built; the server and the
writer are not (section 9).

## 1. In one page

- **HTTP/1.1 only.** Heads are read a line at a time under a maximum
  size; bodies are framed by length, by chunks, or by the end of the
  stream; the body goes up as a stream that the side above demands.
- **One exchange at a time.** A client connection carries one exchange
  at a time and is used again when both sides allow it; a server
  connection will parse the next request only once the response is
  queued and there is room for the one after it.
- **Two streams per exchange.** The side above writes the request body
  as a stream and reads the response body as one, each a `lib::stream`
  face (lib.md, 7) of which the client is the side below. A machine
  stacked on the response body (server-sent events, JSON) cannot tell it
  from a socket.
- **Server-sent events** are a machine over a body stream: lines, fields,
  and an event at each blank line, each under a maximum. An event's data
  goes up whole, and `sse::Data` reads it to a JSON tokenizer.
- **Both sides of each machine** are skein's (client and server, reader
  and writer), and each is tested against transcripts of real peers'
  formats, generated messages, and, once both are built, the other side.

## 2. In skein

`skein-http` depends on lib only. A service's protocol layer stacks it:
over TLS's plaintext, a socket or a pipe, whichever the connection has,
and under the service's own decoders. An LLM client's stack, from the
socket up:

```
domain
  ▲  typed calls
the service's decoder
  ▲  tokens
json                  a tokenizer for each event's data, through sse::Data
  ▲  events
sse                   skein_http::sse
  ▲  body stream
http client           skein_http::client
  ▲  stream: plaintext
tls
  ▲  stream: ciphertext
io socket
```

Each machine has its own `Limits` and `worst_case`, each entry point
declares its `MAX_OUT`, and each says what it waits for:

```rust
// skein_http::client, the machine
pub fn up(client: &mut Client, env: &Env<Limits>, ev: stream::Up,
          above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
pub fn down(client: &mut Client, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<stream::Down>)

// skein_http::sse, the machine: the same shape, over a body stream
pub fn up(reader: &mut Reader, env: &Env<Limits>, ev: stream::Up,
          above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
pub fn down(reader: &mut Reader, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
```

Whoever stacks a machine checks at startup that its largest demand fits
the side below: `client::largest_read` and `client::largest_room`
against the stream's intake and output caps, `sse::largest_demand` and
the JSON tokenizer's against the client's `Limits::read`.

## 3. The HTTP/1.1 client

The client's vocabulary is its own; the service's protocol layer
translates between it and the domain's.

```rust
pub enum Request {                         // from the side above
    Call(Call),                            // start an exchange: Done or Failed answers it
    Upload(stream::Down),                  // the request body's stream, written
    Body(stream::Down),                    // the response body's stream, read
    Discard,                               // the rest of the body is read and dropped
    Close,                                 // Closed answers it
}

pub enum Event {                           // to the side above
    Response(Response),                    // the final response's head
    Upload(stream::Up),                    // Room, or Failed once the upload can go no further
    Body(stream::Up),                      // Bytes, End, or Failed
    Done(Reuse),                           // for a Call: over, and whether the connection is kept
    Failed(Error),                         // for a Call: failed, or never began
    Closed,                                // for a Close: terminal
}

pub struct Call { method: Method, target: Box<[u8]>, headers: Box<[Header]>, body: Body, close: bool }
pub enum Body { None, Length(u64) }
pub struct Response { version: Version, status: u16, headers: Box<[Header]>, framing: Framing }
pub enum Framing { Empty, Length(u64), Chunked, UntilEnd }
pub struct Header { name: Box<[u8]>, value: Box<[u8]> }   // names compared without regard to case
```

- **A call starts an exchange,** while none is in progress, and gets
  exactly one terminal event: `Done` once the response body has been read
  to its end or discarded, or `Failed`. A call refused writes nothing and
  leaves the connection as it was; any other failure leaves it for the
  close. A call during an exchange, or after the connection was told it
  is not to be used again, is the side above's bug, asserted.
- **`Response`** goes up once the final head is whole. Its body follows
  on the body stream, even when it is empty: the first demand of an empty
  body gets `End`.
- **`Close` ends the client in any state.** It withdraws what the client
  demanded below, ends the exchange in progress without a word, and
  answers `Closed`, its one terminal event. It does not close the stream
  below, which its owner closes.

### 3.1 The request

- **The head is written sized** (programming-model.md, 8): the request
  line, the caller's fields in order, then the client's own:
  `Content-Length` for a body (and `Content-Length: 0` for a POST, PUT or
  PATCH without one, RFC 9110, 8.6), and `Connection: close` for a call
  that closes. It is measured, refused past `Limits::request`, written
  into a box of exactly its length, and sent whole within room it asks
  for, before anything is read.
- **What the caller gets wrong is a refusal,** checked in a fixed order
  before anything is written: a target that is empty or not visible ASCII
  (`Target`), a field name that is not a token (`Name`), a value with a
  control character other than a tab, CR, LF and NUL among them
  (`Value`), a field the client writes itself, `Content-Length`,
  `Transfer-Encoding` or `Connection` (`Reserved`), and the length
  (`TooLong`). No header can be injected through a call.
- **The methods** are those an API's client sends: GET, HEAD, POST, PUT,
  PATCH, DELETE and OPTIONS. `Host` is a field the caller gives.
- **The request body is a stream** (`Upload`), written, by length: the
  side above demands room only, of at most `Limits::send`, sends within
  the room granted, and finishes once the call's length is sent. Each
  demand goes below as room, with the next line of a response that may
  come first; the client demands nothing below while it waits for the
  side above's next demand, as a read stated alone would hold a demand
  that the upload's room could not join. A withdrawal of the upload's
  demand is not supported: the side above closes the client instead.

### 3.2 The response head

- **Read a line at a time:** each a scan to LF of at most what is left of
  `Limits::head`, the line's CR before its LF dropped, so a head of LF
  alone is read (RFC 9112, 2.2). Each line is parsed as it comes into a
  bounded head: the status line, then fields into a list of at most
  `Limits::headers`. The budget covers every head of the exchange,
  interim ones included, so no server keeps a client reading interim
  heads for ever.
- **The status line** is `HTTP/x.y`, a space, three digits from 100 to
  599, then nothing or a space and a reason, which is checked and not
  kept (RFC 9110, 15). Another major version is `Version`; a minor past 1
  is read as 1.1 (RFC 9112, 2.3).
- **A field** is a token, a colon with no whitespace before it, and a
  value, trimmed, with no control character but a tab. A line that
  begins with whitespace is an obsolete fold, joined to the last value
  with one space (RFC 9112, 5.2).
- **Errors at one line are decided in a fixed order:** its length (a line
  that met no LF filled what was left of the head), then its bytes, then
  the room for one more field.
- **Interim responses** (1xx) are read and skipped, a 100 mid-upload
  among them; a 101 is refused (`Upgrade`): the client never asks to
  switch protocols.
- **The framing** (RFC 9112, 6.3): no body for a response to HEAD, a 204
  or a 304, whatever the head says; chunked for `Transfer-Encoding:
  chunked`, alone, in HTTP/1.1; by length for `Content-Length`, however
  many times it is given, as long as it is one length; otherwise, to the
  end of the stream. `Transfer-Encoding` beside `Content-Length` is
  refused rather than read by the one that wins (RFC 9112, 6.3, 3: "ought
  to be handled as an error"), and so is any coding but `chunked`, which
  the client does not undo, and chunked in HTTP/1.0 (`Framing`).

### 3.3 The response body

The client is the side below of the body's stream, so it keeps the
stream's contract (lib.md, 7) and holds its carry-over, as io does for a
socket (programming-model.md, 4.3): the bytes read below that no demand
above has taken yet, in an intake of `Limits::read`, allocated with the
client.

- **It reads below only what a demand above still needs,** in the
  demand's own shape, a fill or a scan to the same delimiter, and never
  past the body's framing. A scan for a line passes through as a scan,
  so a slow event stream is read as each event comes; nothing is read
  ahead of a demand. A delivery that meets the demand whole, with nothing
  held, goes up as it came; others are met from the intake, across the
  chunks of a chunked body too, a delimiter split between two chunks
  included.
- **The framing is read only when a demand needs what lies past it:** a
  chunk's size line (a scan to LF of at most `Limits::head`, then
  hexadecimal digits, then nothing or an extension, which is ignored),
  the line ending after a chunk's data (a scan to LF of at most two), and
  the trailer section (lines within `Limits::head` in all, ended by a
  blank line, not kept).
- **The end:** `End` answers a demand that what the body holds can no
  longer meet, once the framing says the body is over, and `Done`
  follows it in the same step. A read larger than what is left is never
  met (lib.md, 7): a side above that must see every byte reads by its
  framing, as JSON and the reader do.
- **A withdrawal** of a body demand means the side above reads no more
  (lib.md, 7), as when a machine stacked on the body closes: what was
  read for it is dropped, a demand after it is asserted, and the side
  above discards the rest or closes the client. A withdrawal may cross
  its demand's answer: one that comes after the answer withdraws all the
  same, and one that comes after the body's end and `Done` is dropped, as
  is a `Discard` that crosses them.
- **`Discard`** gives up the rest of the body, a demand outstanding or
  withdrawn among it: the client reads and drops it, by fills of at most
  `Limits::read` within the framing, so that the connection can be used
  again, then says `Done`. A body that runs to the end of the stream has
  no end to discard to: the exchange is done at once, on a connection not
  used again, and the read below for it is withdrawn.

### 3.4 The exchange

- **A response that comes first:** a final response read before the
  request body is all sent stops the upload, whose stream is told
  `Failed(Fault::Other)`, and the exchange ends with that response, on a
  connection not used again: the server's view of the request's framing
  is not known.
- **Reuse** (RFC 9112, 9.3): the connection carries another exchange
  when the response persists (HTTP/1.1 without `Connection: close`;
  HTTP/1.0 with `Connection: keep-alive` and not `close`), its body was
  framed by length or by chunks and read to its end or discarded, the
  call did not ask to close, and the upload was not stopped. `Done(Keep)`
  then leaves the client waiting for the next call; `Done(Close)`, for
  its close.
- **The stream ending or failing:**

  | When | `End` | `Failed(fault)` |
  |---|---|---|
  | idle | the next call fails at once, `Closed` | the next call fails, `Stream(fault)` |
  | before the request head went down | `Closed`: nothing was sent | `Stream(fault)` |
  | after it, before the response is whole | `Truncated` | `Stream(fault)` |
  | in a body by length or chunks | `Truncated` | `Stream(fault)` |
  | in a body to the end of the stream | the body's end | `Stream(fault)` |
  | once the body is all read below | the body stands, not reused | the body stands, not reused |

  `Closed` is the one a pool can retry on another connection whatever
  the method: the server never saw the request. A stream of the side
  above's that is still open is told first, `Failed` with the stream's
  fault, or `Invalid` for an error of the peer's data; a demand that
  still asks for room, which may come after the end, is withdrawn.
- **What it waits for**, `waiting()`, a function of its state, so the
  connection can arm deadlines (programming-model.md, 4): `Call` (idle),
  `Room` (the peer is not reading the request), `Response` (the peer has
  the request, or as much as the side above wrote, and has not
  answered), `Body` (the body or its framing from the peer), `Above` (the
  side above must demand, send or finish), `Close` and `Nothing`.
  Progress below is a line of a head or of the framing, a piece of the
  body, or room granted, each a demand met.

### 3.5 Limits and the worst case

```rust
pub struct Limits {
    pub request: u32,   // the longest request head written: past it, Refused(TooLong)
    pub head: u32,      // the bytes of a response's heads, interim ones included: past it, HeadTooLong;
                        // also the longest chunk size line, and the longest trailer section
    pub headers: u32,   // fields in a head: past it, TooManyHeaders
    pub read: u32,      // the most the side above demands of the body at once: the intake's cap
    pub send: u32,      // the most room the side above demands at once for the request body
}
```

- **`worst_case(&limits)`** is the intake (`read`); the request head,
  held until room comes for it, with the list of the response's fields
  (`headers`); the head being read, its fields' bytes and the line that
  holds the next within `head`, and as much again while a fold joins a
  value; and, once the body is read, a delivery or the carry-over an
  exchange leaves unread (`read`, a line of the framing being within the
  `2 × head` already counted). A delivery is the client's to count
  (lib.md, 7); a call is the side above's, read and dropped by the step
  that writes it; what goes up is the side above's from when it is
  emitted. `None` for a head shorter than a blank line or a read of
  nothing.
- **`largest_read`** is the larger of `head` and `read`, and at least 2;
  **`largest_room`** the larger of `request` and `send`.
- **`UP_MAX_OUT`** is two events and two requests: a response and the
  upload stopped, a body's end and `Done`, or a stream told it failed
  and `Failed`; below, the request head sent and the next demand.
  **`DOWN_MAX_OUT`** is two events and one request: a body's end and
  `Done`, or `Closed`; below, a demand, a send or a withdrawal.

## 4. The server-sent events reader

A machine over a body stream (WHATWG HTML, 9.2), with the same shape on
both sides as the client's.

```rust
pub enum Request { Next, Close }           // from the side above

pub enum Event {                           // to the side above
    Message(Message),                      // for a Next
    Ended,                                 // for a Next: the stream ended; an event it cut short is dropped
    Failed(Error),                         // for a Next: a limit passed, or the stream failed
    Closed,                                // for a Close: terminal
}

pub struct Message { name: Box<[u8]>, data: Box<[u8]>, id: Box<[u8]> }
pub enum Error { LineTooLong, EventTooLong, FieldTooLong, Stream(Fault) }
```

- **`Next` demands one event.** Exactly one event answers it: the next
  `Message`, or the stream's outcome, `Ended` or `Failed`, after which
  nothing follows but `Closed`. One `Next` at a time, and none after the
  outcome or the close: the side above's bug otherwise, asserted.
- **`Close` ends the reader in any state,** as the JSON tokenizer's does
  (json.md, 3.1): it withdraws what it demanded below, drops a `Next`
  not yet answered, and answers `Closed`.
- **An event's data goes up whole,** in one box, once its blank line is
  read. A machine stacked above reads it through **`sse::Data`**, a side
  below over one box: each demand answered at once by exactly what it
  reads, or by `End` when what is left cannot meet it, as lib's intake
  would. A tokenizer is made for each event's data, as temper's LLM client
  does (its llm.md, 3).
- **What it waits for**, `waiting()`: `Next`, `Bytes`, `Close`,
  `Nothing`. Every line is progress, a comment sent to keep the stream
  alive among them. The reconnection time and the last event ID are
  `retry()` and `last_event_id()`, for a client that reconnects.

### 4.1 Lines, fields and events

- **A line ends at LF, at CRLF, or at CR alone,** wherever it falls in
  what a scan delivers. A CR ends its line at once; an LF right after it
  is its pair, and ends nothing.
- **The reader scans to the byte that ended the last line:** to LF, the
  ending of every line in practice and the pair of a CRLF; to CR after a
  line that ended with a CR alone. Each scan is of at most
  `Limits::chunk`. A stream of LF, of CRLF or of CR alone is so read as
  it comes, a line in each scan once its convention is known. One whose
  lines change convention is read a scan's worth later after each
  change; and at the end of the stream, what follows a change that the
  last scan did not deliver is never seen (lib.md, 7), as a line cut by
  the end would not be.
- **A delivery may hold more than the event asked for:** lines ended by
  a CR alone within a scan to LF. The reader then holds the rest of the
  delivery, at most a scan, and reads it before it demands more. Before
  the stream's end, a failure overrides what is held, as the tokenizer's
  does; after it, what was read stands.
- **Fields** (WHATWG HTML, 9.2.6), each value going where it belongs as
  its bytes come: `data` into the event's data, an LF after each line;
  `event` into its type; `id` into a value held until its line ends, then
  into the last event ID buffer unless it holds a NUL; `retry` into the
  reconnection time if its value is ASCII digits, and no more than a
  `u64`. A line that begins with a colon is a comment; one whose name is
  no field's is ignored; a name with no colon is the field with an empty
  value; one space after the colon is dropped.
- **Dispatch at each blank line:** the last event ID is set from its
  buffer; with no data, nothing is dispatched; otherwise the data, its
  last LF dropped, goes up with its type (`message` when none was set)
  and the last event ID.
- **One leading byte order mark** is skipped, as UTF-8 decoding does.
  Nothing else is decoded: the data goes to a decoder that checks UTF-8
  (JSON does), and a type or an id is compared as bytes.

### 4.2 Limits and the worst case

```rust
pub struct Limits {
    pub line: u32,    // the longest line before its ending: past it, LineTooLong
    pub event: u32,   // the bytes read from the end of the last event to the blank line of this one,
                      // endings included: past it, EventTooLong
    pub field: u32,   // the longest event type or id: past it, FieldTooLong
    pub chunk: u32,   // the most demanded at once: each scan's maximum, and the most held; at least one
}
```

- **Every byte read counts against the event being read,** a comment's
  and an unknown field's too, so a stream that never ends an event fails
  whatever it sends; each blank line starts over, so a stream kept alive
  by comment blocks runs on.
- **`worst_case(&limits)`** is the event's data (`event`), its type, an
  `id` value, the last event ID buffer and the last event ID (`field`
  each), all allocated with the reader, and the delivery it reads or holds
  the rest of (`chunk`). An event's boxes are the side above's from when
  it goes up. `None` for a chunk of zero.
- **`largest_demand`** is `chunk`.
- **`UP_MAX_OUT` and `DOWN_MAX_OUT`** are one event and one request each:
  only a `Close` while reading emits both, `Closed` and the demand
  withdrawn.

## 5. The server and the event writer

Not built yet (section 9). As planned:

- **A server connection** takes one request at a time. It parses the
  next request only once the response is queued and there is room for
  the response after it (programming-model.md, section 7). Its request
  head is read as the client reads a response head (3.2), its body
  framed by length or chunks.
- **The event writer** frames events for a server, sized.
- **Not planned:** HTTP/2 and upgrades. Revisit them when a peer
  requires them.

## 6. Testing

- **Step tests** (`crates/skein-http/src/tests/`): the request head
  written and every refusal in order; the status line, fields, folds and
  every head limit at and past its edge; interim responses; the framing
  each head decides and every conflict refused; reuse by each rule; each
  body framing under fills and scans of every shape, a delimiter split
  across chunks, the reads below shaped by the demand above, every
  framing error, withdrawal, discard, and what an exchange leaves unread;
  the upload, a response that comes first, the stream ending and failing
  in each state, a close in each state, `waiting()`, and each bug of the
  side above's asserted. The reader: every field, every line ending and
  a scan that follows them, the rest of a delivery held, the end and a
  failure while idle and while reading, every limit at its edge, the
  byte order mark, every cut of a stream reading the same, and a close in
  each state; and `sse::Data`.
- **Machine worlds** (testing-strategy.md, 2.4) in `tests/http`
  (`skein-http-world`), one machine from a seed in one loop with both its
  neighbours:
  - **The client's** runs one connection for one exchange after another.
    Below, the server's stream: its bytes arrive in pieces cut at random,
    late, into an intake under its cap, and meet each read exactly; room
    is granted late, one `Send` a grant, while a response may wait; now
    and then the server is patient, and answers each exchange only once
    it has the whole request, so that a client that waited for the
    response before it took the body would wait for ever; the stream ends
    when the bytes run out, early at a cut, idle or with a read on its
    way, and fails, before its end or after it; after a close it may
    still answer what was on its way. Above, a user makes
    the calls, uploads each body in pieces within the room granted, reads
    with fills and scans to LF, CRLF and a quote of every size, slowly,
    withdraws a demand and discards, discards the rest now and then,
    stops for a while, and closes after the last exchange or at any
    moment. The world checks `MAX_OUT` on each call; below, one demand at
    a time, none past the caps or once the stream ended or failed, a
    withdrawal only as the client stops reading, each `Send` within the
    room granted; above, each answer for a demand and exactly what it
    reads, `End` and `Failed` once and nothing after, one response and
    one terminal event per call, `Closed` once and last; and `waiting()`
    against what the neighbours see. Each exchange is held to a reference
    reader: the request written, against a writer of the test's own; the
    head; the body, a prefix of the reference's, and when `End` came,
    nothing left that meets the demand it answered; and the outcome,
    unless the stream failed or the side above closed first, with a
    connection whose upload stopped, or whose stream ended or failed
    during the exchange, not used again.
  - **The reader's** does the same for an event stream, against a
    reference that follows the standard's parser a byte at a time and
    mirrors only what the reader promises: which bytes its scans see, and
    the order of errors at one byte.
  - A seed replays to the same run.
- **Transcripts** in `tests/http/transcripts/`, each `<name>.http` beside
  `<name>.expect`, what it must decode to: the head, the body or the
  events its body holds, and the outcome. No real response can be
  captured offline, so each is written by hand after the public format
  of its peer, and says so; its expectation was drafted by the reference
  readers and checked by hand. Forty-five:
  - an LLM provider's streams: Anthropic's Messages, a text answer
    chunked and a tool call by length, and an overloaded error; OpenAI's
    Chat Completions, a text answer chunked and tool calls by length,
    each ending with `[DONE]`;
  - a forge's API: a Forgejo pull request by length, a page of issues
    chunked with its `Link` and `X-Total-Count`, and a 404;
  - responses curl accepts: a head of LF alone, a folded field, HTTP/1.0
    kept alive and to the end of the stream, a 100 and a 103 before the
    response, a 204, a 304, chunk extensions and trailers, a length
    repeated, no reason phrase, fields in odd case and whitespace, a
    response to HEAD, and `Connection: close`;
  - hostile ones: an oversized head, a head that never ends, chunk sizes
    that overflow or are not hexadecimal, a chunk without its line
    ending, a chunked body and a body by length cut short, bad status
    lines (a bad one, HTTP/2's, not HTTP at all), conflicting framing
    (both headers, two lengths, a coding not undone, chunked in
    HTTP/1.0), a CR in a field, whitespace before a colon, a fold before
    any field, too many fields, an upgrade, a trailer section that never
    ends, and event streams whose event never ends, whose line is too
    long, and whose comments never reach a blank line.

  Each decodes to its expectation under several seeds, read a byte at a
  time so that every byte of the body is seen, and cut anywhere, as the
  reference reads the prefix; each event stream reads to its events under
  scans of several sizes. A transcript without an expectation fails,
  printing the reference readers' reading as a draft to check.
- **The machines stacked** (`tests/http/tests/stack.rs`), a small step
  towards the protocol worlds: the reader and a tokenizer per event over
  the client, as a connection routes between them, the request body
  written by the JSON writer and uploaded, over the LLM transcripts cut
  at random; every event is the transcript's and every document the JSON
  reference parser's reading of its data.
- **Memory** (`tests/http/tests/memory.rs`, with the counting allocator,
  testing.md, 5): every call of an entry point a step of the meter. The
  client at its limits: a head at the head limit with folds among its
  fields, bodies by length, chunked with a trailer section at its limit,
  and to the end of the stream, an interim head, an upload, a request
  head near its limit, a response cut short, each read with three
  demands, and closed, failed and discarded after every step; and the
  carry-over held with a delivery. The reader at its limits: an event at
  every limit with lines ended every way, a line, an event and a type
  past theirs, each closed after every step; its peak is its worst case
  exactly.
- **The fuzzy suite** (`tests/http/tests/fuzzy_*.rs`): 20,000 connections
  of one to four generated exchanges, valid, mutated, and corrupted where
  a random edit seldom lands (another major version, a code past 599, an
  upgrade, conflicting framing, a chunk size past a `u64`, a trailer
  section without end), and 2,000 transcripts cut and mutated, each
  under limits and neighbours drawn from its seed; 12,000 event streams
  generated and mutated, and 2,000 of the transcripts' event bodies. Each
  sweep asserts that what it injects fell (testing-strategy.md, 3): every
  outcome of an exchange and every error of a stream, each way a stream
  can end or fail below and what the machine waited for when it failed,
  a close while it waited for each thing, room granted while a response
  waited, a response read mid-upload, a withdrawal and an answer after
  it, a discard, a connection reused, the rest of a delivery held, and a
  scan to CR. It stands in for the fuzz targets, which wait for a
  nightly toolchain.

## 7. Decisions

- **Two stream faces per exchange,** the upload and the body, each a
  `lib::stream` of which the client is the side below, and the exchange's
  events beside them. A machine stacked on the body sees a stream like a
  socket's; one face for both directions would make a reader stacked on
  the body share its demands with the upload's writer, and a response
  that came mid-upload could leave the writer's demand for room never
  answered.
- **The client holds the body's carry-over,** in an intake of
  `Limits::read`, as the side below of a stream does (programming-model.md,
  4.3), and reads below in the demand's own shape. Reading ahead, with
  fills of what the intake has room for, would hold a slow event stream
  until a fill's worth arrived; passing demands through without an intake
  could not meet a fill or a scan across two chunks.
- **The request body goes by length only.** Every body temper sends is
  measured first (its llm.md, 3); chunked uploads wait for a user.
- **A response that comes first ends the exchange** with the connection,
  and the upload's stream hears `Failed(Fault::Other)`, the fault the
  stream vocabulary has for a side below that stops for a reason of its
  own.
- **A withdrawal on the body means the side above reads no more,** as
  the stream contract says; the side above follows it with `Discard` or
  a close. `Discard` exists because a withdrawal alone cannot say it:
  a machine stacked on the body withdraws only what it has outstanding.
- **Framing headers that conflict are refused,** not resolved: a
  `Transfer-Encoding` beside a `Content-Length` is how requests are
  smuggled and responses split, and the client has nothing to gain by
  guessing.
- **The head's budget covers interim heads,** rather than a separate
  limit on how many there may be.
- **An event's data goes up whole.** The standard dispatches an event
  only at its blank line, and its type and id may come after its data
  lines, so a decoder learns what the data is (or that it is not JSON at
  all: `[DONE]`) only then; an event the stream cuts short is dropped,
  which data already streamed up could not be. The reader then holds an
  event under `Limits::event`, which temper sizes for ChatGPT's large
  events (its llm.md, 3 and 13). Streaming an event's data to the
  tokenizer would hold a line instead, at the price of these.
- **The reader holds the rest of a delivery** when it holds more than the
  event asked for, at most a scan's worth, rather than reading a byte at
  a time: a demand met is a loop iteration, and a byte at a time would
  cost one per byte of every event.
- **The reader's scan follows the last line's ending.** A scan has one
  delimiter, and a line may end at CR or LF. Scanning to LF always would
  read a stream of CR alone only when a scan filled, and lose its last
  events at the end; following the last ending reads every convention as
  it comes, and costs a stream that mixes them a scan's worth of delay at
  each change.
- **Every byte read counts against the event,** comments included, so an
  event that never ends fails whatever the stream sends it as.
- **The reader does not decode UTF-8,** and so does not replace what is
  not UTF-8 with U+FFFD as a browser would: its data goes to a decoder
  that refuses it.
- **A `retry` past a `u64` is ignored,** as a value of other bytes is.

## 8. Open questions

- **Connection reuse in the client.** Sequential reuse of one connection
  is in. Still open: a pool of connections per peer, who sets its size,
  and how a pool learns that an idle connection ended (the client says
  so in `waiting()` only).
- **`Expect: 100-continue`.** A caller may send the field, but the client
  does not wait for the 100 before it takes the body; the side above
  could, by demanding room only once it has seen it. Whether the client
  should wait waits for a server that needs it.

## 9. Not built yet

The client and the server-sent events reader are built, with their
machine worlds and transcripts. temper pulls next:

1. **the server and the event writer** (section 5), for the fake LLM
   provider; with them, each machine tested against the other side, and
   the **protocol worlds** of testing-strategy.md, 2.5: an LLM client's
   stack against a server's, joined by bytes cut at random, where a slow
   reader at the top of one end stops the writer at the top of the other;
2. both, for the engine's forge client and its webhooks.

Also not built: chunked uploads; content codings (`gzip`), which the
client refuses; trailer fields, which are read and dropped; reconnecting
an event stream, for which the reader keeps the reconnection time and
the last event ID; and the **fuzz targets** (`fuzz/`, fed `Bytes` under
every demand), which wait for a nightly toolchain, the fuzzy suite
standing in for them. Transition coverage of the handlers
(testing-strategy.md, 6) waits for `cargo llvm-cov`, which is not
installed.
