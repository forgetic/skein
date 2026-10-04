# HTTP

Provisional, 2026-10-04. The design of `skein-http`: the protocol
machines for HTTP/1.1, client and server, and for server-sent events,
reader and writer. They are step machines of a connection's stack
(programming-model.md, 4): each depends on lib only, and meets the
stream below it and the user above it through entry points of the
model's shape. All four are built (section 9).

## 1. In one page

- **HTTP/1.1 only.** Heads are read a line at a time under a maximum
  size; bodies are framed by length, by chunks, or by the end of the
  stream; the body goes up as a stream that the side above demands.
- **One exchange at a time.** A client connection carries one exchange
  at a time and is used again when both sides allow it; a server
  connection parses the next request only once the response is queued
  and there is room for the one after it: it sets that room aside before
  it reads anything of a request, so whatever the request comes to, its
  answer goes down at once.
- **Two streams per exchange.** The side above writes the request body
  as a stream and reads the response body as one, each a `lib::stream`
  face (lib.md, 7) of which the client is the side below; the server's
  side above reads the request body and writes the response body the
  same way. A machine stacked on a body (server-sent events, JSON)
  cannot tell it from a socket.
- **Refuse at the entrance.** What is wrong with a request's head the
  server answers itself, small and fixed: 400, 413, 414, 431, 501 or 505,
  and the connection ends. Bad framing in a body closes, unanswered; a
  chunked body past the limit is a 413 if the room set aside is still
  held.
- **Server-sent events** are a machine over a body stream each way. The
  reader reads lines, fields, and an event at each blank line, each
  under a maximum; an event's data goes up whole, and `sse::Data` reads
  it to a JSON tokenizer. The writer frames each event sized and sends
  it within the room it is granted.
- **Both sides of each machine** are skein's (client and server, reader
  and writer), and each is tested against transcripts of real peers'
  formats, generated messages, and the other side: a protocol world
  stacks both ends of an LLM's stream (section 6).

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

A fake LLM provider's, the other end:

```
domain
  ▲  typed calls              ▼  what it answers
the service's protocol layer
  ▲  tokens                   ▼  events, their data written by the JSON writer
json (the request body)      sse writer     skein_http::sse::writer
  ▲  request body             ▼  response body
http server                                 skein_http::server
  ▲  stream: plaintext        ▼
tls, or an io socket
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

// skein_http::server and skein_http::sse::writer: the same shape
pub fn up(server: &mut Server, env: &Env<Limits>, ev: stream::Up,
          above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
pub fn down(server: &mut Server, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
```

Whoever stacks a machine checks at startup that its largest demand fits
the side below: `client::largest_read` and `client::largest_room`, or
`server::largest_read` and `server::largest_room`, against the stream's
intake and output caps; `sse::largest_demand` and the JSON tokenizer's
against the client's `Limits::read`; the tokenizer's against the
server's `Limits::read`, and `writer::largest_room` against its
`Limits::send`. The method and the version a start line names
(`Method`, `Version`) and a field (`Header`) are the crate's, both sides
the same.

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
  `Transfer-Encoding` or `Connection` (`Reserved`), each field in turn,
  its name before its value; then no `Host`, or more than one (`Host`,
  RFC 9112, 3.2); and the length (`TooLong`). No header can be injected
  through a call.
- **The methods** are those an API's client sends: GET, HEAD, POST, PUT,
  PATCH, DELETE and OPTIONS. `Host` is a field the caller gives, exactly
  once, anywhere among its fields.
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
  chunks of a chunked body too. A delimiter split between two chunks is
  completed a byte at a time, while what is held ends partway through
  it, so that nothing past it is read either.
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
  | idle | the next call fails at once, `Closed(None)` | the next call fails at once, `Closed(Some(fault))` |
  | before the request head went down | `Closed(None)` | `Closed(Some(fault))` |
  | after it, before any line of a response | `Truncated { answered: false }` | `Stream(fault)` |
  | in the response's heads | `Truncated { answered: true }` | `Stream(fault)` |
  | in a body by length or chunks | `Truncated { answered: true }` | `Stream(fault)` |
  | in a body to the end of the stream | the body's end | `Stream(fault)` |
  | once the body is all read below | the body stands, not reused | the body stands, not reused |

  `Closed` says the server never saw the request, so a pool can send it
  on another connection whatever its method. `Truncated` with no line of
  a response is the race of RFC 9112, 9.3.1, a connection kept idle that
  the server closed as the request went out: an idempotent request may
  be sent again. A stream of the side above's that is still open, and not
  withdrawn or discarded, hears its end first, `Failed` with: the stream's
  own fault, for a failure; `Other`, for an end that cut the exchange
  short (`Closed(None)`, `Truncated`), which is no error of the peer's
  data; and `Invalid`, for an error of the peer's data. A demand that
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
    pub send: u32,      // the most room the side above demands at once for the request body; at least 1
}
```

- **`worst_case(&limits)`** is the intake (`read`); the request head,
  held until room comes for it, with the list of the response's fields
  (`headers`); the head being read, its fields' bytes and the line that
  holds the next within `head`, and as much again while a fold joins a
  value; and, once the body is read, a delivery or the carry-over an
  exchange leaves unread (`read`, a line of the framing being within the
  `2 × head` already counted). A delivery is made to the client's
  demand, so it counts it; a call and a piece of the request body are
  counted by the side above, which made them, and the step that takes one
  reads it and drops it or passes it on (testing.md, 5); what goes up is
  handed out when it is emitted. `None` for a head shorter than a blank line, a read of
  nothing, or room for nothing of a request body (`send` of 0), which
  no upload could get past.
- **`largest_read`** is the larger of `head` and `read`, and at least 2;
  **`largest_room`** the larger of `request` and `send`.
- **`UP_MAX_OUT`** is two events and two requests: a response and the
  upload stopped, a body's end and `Done`, or a stream told it failed
  and `Failed`; below, the request head sent and the next demand.
  **`DOWN_MAX_OUT`** is two events and one request: a body's end and
  `Done`, or `Closed`; below, a demand, a send or a withdrawal.

## 4. Server-sent events

Two machines over a body stream (WHATWG HTML, 9.2), each with the same
shape on both sides as the client's: the reader, which a client stacks
on a response body, and the writer (4.3), which a server stacks on its
reply. The reader:

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

- **A line ends at LF, at CRLF, or at CR alone.** A CR ends its line at
  once; an LF right after it is its pair, and ends nothing.
- **The reader reads a line at a time,** by line scans of at most
  `Limits::chunk` (`Read::Line`, lib.md, 7): each delivery ends at the
  first CR or LF, or is a chunk of a line longer than one. Every line is
  read as soon as its end arrives, whatever the stream's convention, and
  a stream shorter than a scan as it comes. An LF-only or a CR-only
  stream is one delivery a line; a CRLF is two, the line to its CR and
  then the LF alone, which the reader skips as the CR's pair.
- **An event ends its delivery,** as the line end that dispatches it is
  a delivery's last byte, so the reader keeps nothing of a delivery past
  the step that reads it. An end or a failure that comes while the reader
  demands nothing answers the next `Next`; after the end, a failure says
  only that the stream can no longer send, and what was read stands.
- **At the end of the stream, only an incomplete last line goes unread,**
  as the standard drops it: a line scan larger than what is left is never
  met (lib.md, 7). Of a last line longer than a chunk, the whole chunks
  are read, so one that passes a limit fails if a whole chunk past the
  limit came, and otherwise ends the stream.
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
    pub chunk: u32,   // the most demanded at once: each line scan's maximum; at least one
}
```

- **Every byte read counts against the event being read,** a comment's
  and an unknown field's too, so a stream that never ends an event fails
  whatever it sends; each blank line starts over, so a stream kept alive
  by comment blocks runs on.
- **`worst_case(&limits)`** is the event's data (`event`), its type, an
  `id` value, the last event ID buffer and the last event ID (`field`
  each), all allocated with the reader, and the delivery it reads
  (`chunk`), dropped by the step that reads it. An event's boxes are
  handed out when it goes up. `None` for a chunk of zero.
- **`largest_demand`** is `chunk`.
- **`UP_MAX_OUT` and `DOWN_MAX_OUT`** are one event and one request each:
  only a `Close` while reading emits both, `Closed` and the demand
  withdrawn.

### 4.3 The writer

```rust
pub enum Request {                         // from the side above
    Event(Outgoing),                       // write an event: Sent, Refused or Failed answers it
    Comment(Box<[u8]>),                    // write a comment, to keep the stream alive: the same
    Finish,                                // end the body, once nothing is being written
    Close,                                 // Closed answers it
}

pub enum Event { Sent, Refused(Refusal), Failed(Fault), Closed }

pub struct Outgoing { name: Box<[u8]>, data: Box<[u8]>, id: Option<Box<[u8]>>, retry: Option<u64> }
pub enum Refusal { Name, Id, Comment, TooLong }
```

- **One at a time.** Exactly one event answers each event or comment:
  `Sent`, once all of it went down; `Refused`, for what the side above
  got wrong, writing nothing and leaving the writer as it was; or
  `Failed`, the stream's failure, now or before, after which nothing
  follows but `Closed`. Another while one is written, or one after
  `Finish`, is the side above's bug, asserted.
- **Framed sized, a block each:** `event: ` and the type, if it has one;
  `id: ` and the id, if it has one, an empty one resetting a reader's
  last event ID; `retry: ` and its digits; a `data: ` line for each line
  of the data, split at each LF, CRLF or CR, so one line more than its
  endings; and a blank line. One space follows each colon, which a reader
  drops, so a value's own leading space survives. A comment is `: ` and
  its text, or the colon alone, then a blank line, so that a reader's
  count for an event starts over after it (4.2).
- **Refused in a fixed order:** a type with a CR or an LF (`Name`), an id
  with a CR, an LF or a NUL, which a reader ignores (`Id`), a comment
  with a CR or an LF (`Comment`), then a frame past `Limits::event`
  (`TooLong`).
- **Every event written is one a reader dispatches,** and reads back as
  it was written: a type of none as `message`, the data's line endings
  as LFs, the last event ID as each `id` left it.
- **Sent in pieces** of at most `Limits::chunk`, each within the room
  granted for it, one `Send` a grant; the frame itself goes, uncopied,
  when it fits one. The server's chunked reply makes each a chunk.
- **`Finish`** ends the body below, once nothing is being written; after
  the stream failed there is nothing to end. The stream's `End` changes
  nothing for a writer, which reads nothing: room may still come after
  it.
- **`Close` ends the writer in any state:** it withdraws what it
  demanded below, drops an event not yet sent, and answers `Closed`.
- **What it waits for**, `waiting()`: `Above`, `Room` (the reader is not
  reading), `Close`, `Nothing`.

### 4.4 The writer's limits and worst case

```rust
pub struct Limits {
    pub event: u32,   // the longest event or comment framed, its blank line included: past it, TooLong
    pub chunk: u32,   // the most room demanded at once; at least one
}
```

- **`worst_case(&limits)`** is the event being written, framed, at most
  `event`, held until all of it went down. What the side above gives it
  is the side above's to count, and a piece copied out is handed out as
  it is sent. `None` for a chunk of zero.
- **`largest_room`** is `chunk`, which whoever stacks the writer checks
  against the server's `Limits::send`. A reader whose `Limits::event` is
  at least the writer's reads every event it writes.
- **`UP_MAX_OUT`** is one event and two requests: room granted sends a
  piece and asks room for the next, or sends the last and says `Sent`.
  **`DOWN_MAX_OUT`** is one and one.

## 5. The HTTP/1.1 server

The server's vocabulary is its own, as the client's is: the service's
protocol layer translates between it and the domain's.

```rust
pub enum Request {                         // from the side above
    Next,                                  // the next request: Call, Ended or Failed answers it
    Respond(Response),                     // the call's response head, once; Refused answers one refused
    Body(stream::Down),                    // the request body's stream, read
    Discard,                               // the rest of the request body is read and dropped, or given up
    Reply(stream::Down),                   // the response body's stream, written
    Close,                                 // Closed answers it
}

pub enum Event {                           // to the side above
    Call(Call),                            // for a Next: a request's head, whole and sound
    Ended,                                 // for a Next: the client ended the connection between requests
    Body(stream::Up),                      // Bytes, End, or Failed
    Reply(stream::Up),                     // Room, or Failed once the reply can go no further
    Refused(Refusal),                      // for a Respond: refused, writing nothing
    Done(Reuse),                           // for a Call: over, and whether the connection is kept
    Failed(Error),                         // for a Call, or for a Next no call answered
    Closed,                                // for a Close: terminal
}

pub struct Call { method: Method, target: Box<[u8]>, version: Version, headers: Box<[Header]>, body: Body }
pub struct Response { status: u16, headers: Box<[Header]>, body: Body, close: bool }
pub enum Body { None, Length(u64), Chunked }
pub enum Error { Rejected(Rejection), Truncated, Stream(Fault), ChunkSize, Chunk, Trailer, Extensions, BodyTooLong }
```

- **`Next` asks for the next request,** while no exchange is in
  progress, and exactly one event answers it: `Call`; `Ended`, the
  client's end with no line of a request; or `Failed`, a request
  rejected with the server's own answer (`Rejected`), one cut short
  (`Truncated`), or the stream's failure. The side above asks only when
  it can take a request: at its entrance it refuses by not asking, or by
  answering a busy status.
- **A call is an exchange,** and exactly one terminal event ends it:
  `Done`, once the response is all queued below and the request body was
  read to its end, discarded or given up; or `Failed`. After `Ended`, a
  `Failed` or `Done(Close)` the server waits for its close; a `Next` then
  is the side above's bug, asserted, as is a request of any kind with no
  call in progress.
- **`Close` ends the server in any state.** It withdraws what it
  demanded below, ends the exchange in progress without a word, and
  answers `Closed`, its one terminal event. It does not close the stream
  below, which its owner closes.

### 5.1 One request at a time

- **Room first.** On `Next`, before it reads anything of a request, the
  server asks below for room for the longest response head it writes,
  `Limits::response`, and holds it once granted, as io holds a grant
  across demands that ask for none (io.md, 3.3). Only then does it read.
  Whatever the request comes to, a response head or a rejection's
  answer, goes down at once within that room, so the server never parses
  a request it could not answer (programming-model.md, 7).
- **The next request is read** only once the side above asks for it,
  which it does after `Done(Keep)`, once the response is queued.
  Pipelined requests wait below meanwhile, in the stream's intake, under
  its cap, and then in the client's socket.

### 5.2 The request head

- **Read a line at a time,** as the client reads a response head (3.2):
  each line a scan to LF of at most what is left of `Limits::head`, its
  CR dropped. Empty lines before the request line are skipped, within the
  budget (RFC 9112, 2.2).
- **The request line** is a method, a space, a target, a space and
  `HTTP/x.y`, exactly so: one that is not is rejected rather than
  repaired (RFC 9112, 3). The method is a token, and one of `Method`'s,
  the methods an API's client sends; the target is visible ASCII, kept as
  it came, in whatever form the client wrote it. In absolute form its
  authority names the host, and `Host` is to be ignored (RFC 9112,
  3.2.2): the server still checks `Host`, and the side above, which
  reads the target, takes the host from it. A minor version past 1 is
  read as 1.1.
- **A field** is read as the client reads one (3.2), but an obsolete
  fold, or whitespace before the first field, is rejected (RFC 9112, 5.2
  lets a server).
- **At the blank line:** one `Host` in HTTP/1.1, at most one in HTTP/1.0
  (RFC 9112, 3.2), and that one a host and a port, `uri-host [":"
  port]` (RFC 9110, 7.2): an IP literal in brackets, or a name or an IPv4
  address of unreserved bytes, sub-delimiters and percent-escapes, then
  digits, or empty, as a client sends it for a target with no authority;
  `a b/c@` is no host. Then the framing (RFC 9112, 6.1 and 6.3):
  `Transfer-Encoding` is read only in HTTP/1.1 and only alone, so in
  HTTP/1.0 or beside a `Content-Length` it is faulty framing, as it is
  how requests are smuggled; its codings end with `chunked`, given once,
  and hold no other, which the server does not undo. `Content-Length` is
  one length, however many times it is given. Neither: no body. Then a
  length past `Limits::body`.
- **Errors are decided in a fixed order:** at a line, its length, then its
  bytes, then the room for one more field; at the request line, its
  form, then its version, then its method; at the blank line, the
  `Host`, the framing, then the length.
- **Each rejection** has a small answer of a fixed length
  (programming-model.md, 8): the status line, the `Date` (5.4),
  `Content-Length: 0` and `Connection: close`. It goes down within the
  room set aside, `Failed(Rejected(_))` answers the `Next`, and the
  connection ends.

  | Rejection | What | Answer |
  |---|---|---|
  | `RequestLine` | a request line that is not one | 400 |
  | `TargetTooLong` | a request line that does not fit in what is left of the head | 414 |
  | `Version` | another major version than HTTP/1 | 505 |
  | `Method` | a method the server does not know | 501 |
  | `Header` | a field line that is not a field, or a fold | 400 |
  | `HeadTooLong` | a head past `Limits::head` | 431 |
  | `TooManyHeaders` | more than `Limits::headers` fields | 431 |
  | `Host` | no `Host` in HTTP/1.1, more than one, or one that is not a host and a port | 400 |
  | `Framing` | framing that cannot be read | 400 |
  | `Coding` | a transfer coding other than `chunked` | 501 |
  | `BodyTooLong` | a body by length past `Limits::body` | 413 |

### 5.3 The request body

- **Read as the client reads a response body** (3.3), by the same code:
  the server is the side below of the body's stream, keeps its contract,
  and holds its carry-over in an intake of `Limits::read`. A request is
  framed by length or by chunks, never to the end of the stream: one with
  neither has no body. The first demand of an empty body gets `End`; the
  exchange is done only once the side above read its `End`, or discarded
  it.
- **`Discard`** reads the rest and drops it, on a connection that may be
  used again, so that the next request can be read; on one that may not,
  it reads no more: the body is given up, and a read outstanding for it
  withdrawn.
- **Bad framing** (`ChunkSize`, `Chunk`, `Trailer`) fails the exchange,
  and the body's stream hears `Failed(Fault::Invalid)`: nothing is
  answered, and the connection ends.
- **A chunked body is bounded as one by length is** (RFC 9112, 7.1.1):
  its chunks' data, in all, by `Limits::body`, and its size lines'
  extensions, in all, by `Limits::head`, the budget of a head, as each
  size line and the trailer section are. Each size line takes its share
  of both as it is read, before its chunk's data, a discard's reading
  too. Past the data's, the exchange fails with `BodyTooLong`: a 413
  goes down in the room set aside if it is still held, no response given
  and no 100 (Continue) sent in it, and the connection otherwise closes
  unanswered, as for bad framing. Past the extensions', it fails with
  `Extensions`, unanswered, as bad framing. The body's stream hears
  `Failed(Fault::Invalid)` either way.
- **A 100 (Continue)** (RFC 9110, 10.1.1): for an HTTP/1.1 request with a
  body whose `Expect` lists `100-continue`, the server writes `HTTP/1.1
  100 Continue` before the body's first read below, for a demand of the
  side above's or a discard, within the room set aside; the head then
  asks room of its own. A final response given first answers instead, and
  no 100 goes. An HTTP/1.0 client's expectation, and any other, is
  ignored.

### 5.4 The response

- **`Respond` gives the head,** once a call. What the side above gets
  wrong is a refusal, checked in a fixed order, which writes nothing and
  leaves the exchange as it was: a status outside 200 to 599 (`Status`:
  the server writes no interim response but its own 100, and switches no
  protocols); each field, a name that is not a token (`Name`), a value
  with a control character but a tab (`Value`), a field the server
  writes itself, `Content-Length`, `Transfer-Encoding`, `Connection` or
  `Date` (`Reserved`); a body for a 204 or a 304 (`Body`); then a head
  past `Limits::response` (`TooLong`).
- **Written sized:** `HTTP/1.1` whatever the request's version (RFC 9110,
  2.5), the status and the standard's reason phrase for it, or none; the
  `Date`, `env.wall` as the step that writes the head sees it, as an
  IMF-fixdate (RFC 9110, 5.6.7 and 6.6.1): 37 bytes, `Date: ` and 29 of
  the date, a `Wall` ending in 2554, and a line ending; the side above's
  fields in order; the framing as the response says it,
  even to `HEAD` (RFC 9110, 9.3.2), `Content-Length` for a length, `0`
  for no body but in a 204 or a 304, `Transfer-Encoding: chunked` for
  chunks; and `Connection: close` for a connection that does not persist,
  `Connection: keep-alive` for an HTTP/1.0 one that does.
- **The head goes down** once the request body is all read below, or
  given up (5.5), within the room set aside, or room asked for it after a
  100.
- **The body is a stream** (`Reply`), written as the client's upload is
  (3.1): the side above demands room of at most `Limits::send`, sends
  within it, and finishes. By length, each `Send` goes down as it is, and
  `Finish` once the length is sent. In chunks, each `Send` is one chunk,
  framed into a box of its own, its size line, the bytes and a line
  ending, below room asked for the chunk; an empty `Send` is no chunk;
  `Finish` writes the last chunk, `0` and a blank line, with no trailer
  section, within room asked for it. To an HTTP/1.0 client, which knows
  no chunks, a chunked response goes to the end of the stream instead
  (RFC 9112, 6.1), on a connection that ends with it. A response to
  `HEAD`, a 204, a 304 or one without a body has no reply stream: a
  request on one is the side above's bug, asserted.
- **A withdrawal on the reply,** as a machine stacked on it sends when it
  closes, means the side above writes no more: the response can never
  end. The server withdraws what it demanded below, demands nothing more,
  drops an answer on its way, and waits for its close.

### 5.5 The exchange

- **Reuse** (RFC 9112, 9.3): `Done(Keep)` when the request persists
  (HTTP/1.1 without `Connection: close`; HTTP/1.0 with `Connection:
  keep-alive` and not `close`), the response did not ask to close and is
  not sent to the end of the stream, the request body was all read below
  when the head was written, and the stream did not end. Otherwise
  `Done(Close)`, and the head says so.
- **A response given first,** before the request body is all read below,
  gives up the rest of it: the body's stream, if the side above still
  reads it, hears `Failed(Fault::Other)`, the fault the stream
  vocabulary has for a side below that stops for a reason of its own; a
  read outstanding for it is withdrawn; and the head says `Connection:
  close`, as the client may stop its upload when a response comes first,
  as skein's does (3.4), so the rest may never come. Unless the side
  above discards the body on a connection that may be kept: the head then
  waits for the body's end below, and keeps the connection.
- **The stream ending or failing:**

  | When | `End` | `Failed(fault)` |
  |---|---|---|
  | idle | the next `Next`: `Ended` | the next `Next`: `Failed(Stream(fault))` |
  | setting room aside, or before a request line | `Ended` | `Failed(Stream(fault))` |
  | in a head, after its request line | `Failed(Truncated)` | `Failed(Stream(fault))` |
  | in a body by length or chunks | `Failed(Truncated)` | `Failed(Stream(fault))` |
  | once the request is all read below | the response still goes, room may still come; `Done(Close)` | `Failed(Stream(fault))` |

  A stream of the side above's that is still open hears its end first,
  `Failed` with the stream's own fault, `Other` for an end that cut the
  request short, and `Invalid` for bad framing or a body past its
  limits.
- **What it waits for**, `waiting()`, a function of its state, so the
  connection can arm deadlines: `Next` (idle), `Room` (the client is not
  reading: the room set aside, a head after a 100, the reply's room or
  the last chunk's), `Request` (a request's head or its next line: an
  idle keep-alive, or a head that comes slowly), `Body` (the request body
  or its framing), `Above` (the side above must respond, demand, discard,
  send or finish), `Close` and `Nothing`.

### 5.6 Limits and the worst case

```rust
pub struct Limits {
    pub head: u32,      // a request's head: past it 431, or 414 for a request line that does not fit;
                        // also the longest chunk size line, the most chunk extensions of a body
                        // in all, and the longest trailer section
    pub headers: u32,   // fields in a head: past it 431
    pub body: u64,      // the longest body: by length, past it 413 at the head; in chunks,
                        // Failed(BodyTooLong) at the size line that passes it
    pub read: u32,      // the most the side above demands of the request body at once: the intake's cap
    pub response: u32,  // the longest response head: past it Refused(TooLong); also the room set aside
                        // before each request, at least the longest of the server's own answers
    pub send: u32,      // the most room the side above demands at once for the reply; at least 1
}
```

- **`worst_case(&limits)`** is the intake (`read`), and the larger of
  what a request's two phases hold, which never meet: `intake +
  max(fields + 2 × head − 14, response + max(read, head))`. Reading its
  head, the list of its fields (`headers`), and the bytes of its target
  and fields with the line being read, each line held with its copy: the
  most is a request line filling the head, `GET`, its target and
  `HTTP/1.1` ended by an LF, held with its target, the head twice less
  the 14 bytes that are not the target. Its exchange, the response head,
  held until the body is all read below and room comes for it
  (`response`), with a delivery read for the body, a piece (`read`) or a
  line of its framing (`head`), or the carry-over an exchange leaves
  unread. The memory tests reach each exactly, under limits where it is
  the larger. A response and a piece of the reply are counted by the side
  above, which made them, and the step that takes one is checked against
  the worst case and that input (testing.md, 5); what goes down (a head, a
  chunk, an answer of the server's own) and what goes up is handed out
  when it is emitted. `None`
  for a head shorter than a blank line, a read of nothing, room for
  nothing of a reply, room set aside short of the server's own answers,
  or a chunk's room past a `u32`.
- **`largest_read`** is the larger of `head` and `read`, and at least 2;
  **`largest_room`** the larger of `response` and the room of a chunk of
  `send`: `send`, its size in hexadecimal, and four.
- **`UP_MAX_OUT`** is three events and two requests: the stream's
  failure told to both bodies' streams and the exchange; below, the head
  and the reply's room, or a 100 and the read it goes before.
  **`DOWN_MAX_OUT`** is two and two: the body given up and `Done`; below,
  a read withdrawn and the head.

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
  the line scans that read them, a CR's LF skipped, streams of LF, CR,
  CRLF and mixed endings shorter than one scan and the deliveries they
  take, the end and a failure while idle and while reading, every limit
  at its edge, the byte order mark, every cut of a stream reading the
  same, and a close in each state; and `sse::Data`. The server: the room
  set aside before anything is read; the request line, fields, and every
  head limit at and past its edge; every rejection and its answer; the
  framing each head decides; the body under demands of every shape, its
  empty end, every framing error, a withdrawal and a discard on a
  connection kept and on one not; a 100 (Continue), and where none goes;
  the response head written, its reasons and framing, and every refusal
  in order; each reply's framing, the room of a chunk, HTTP/1.0's
  chunked reply to the end of the stream, and a reply withdrawn; reuse
  by each rule, a response given before the body is read and one given
  while it is discarded; the stream ending and failing in each state, a
  close in each state, `waiting()`, and each bug of the side above's
  asserted. The writer: each field and data split at every line ending,
  comments, every refusal, an event in pieces within the room granted,
  the end and a failure, a close in each state, and events read back by
  the reader as they were written.
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
    still answer what was on its way. Above, a user makes the calls,
    uploads each body in pieces within the room granted, reads with
    fills, line scans and scans to LF, CRLF and a quote of every size,
    slowly, withdraws a demand and discards, discards the rest now and then,
    sends a withdrawal or a discard that crosses its answer, the body's
    end among them, stops for a while, and closes after the last exchange
    or at any moment. The world checks `MAX_OUT` on each call; below, one
    demand at a time, none past the caps or once the stream ended or
    failed, a withdrawal only as the client stops reading, each `Send`
    within the room granted; above, each answer for a demand and exactly
    what it reads, `End` and `Failed` once and nothing after, every stream
    still open told it failed before the exchange's `Failed`, one response
    and one terminal event per call, nothing sent for a call that failed
    `Closed`, `Closed` once and last; and `waiting()` against what the
    neighbours see. Each exchange is held to a reference
    reader: the call refused, or not, as a check of the test's own
    refuses it, one call in fifty getting one thing wrong; the request
    written, against a writer of the test's own; the head; the body, a prefix of the reference's, and when `End` came,
    nothing left that meets the demand it answered; and the outcome,
    unless the stream failed or the side above closed first, with a
    connection whose upload stopped, or whose stream ended or failed
    during the exchange, not used again.
  - **The reader's** does the same for an event stream, each demand a
    line scan of the chunk, against a reference that reads by the
    standard's parser a byte at a time, with the reader's limits, and
    knows nothing of scans. The events, the outcome, the reconnection
    time and the last event ID are the reference's, unless the stream
    failed or the side above closed first; the one exception is the
    incomplete line a stream may end with (4.1), of which only whole
    chunks are read, so a failure the reference finds past them reads
    as the end.
  - **The server's** runs one connection for one request after another.
    Below, the client's stream: its bytes arrive in pieces cut at random,
    late, and meet each read exactly; room is granted late and each
    `Send` held to it as io holds it, one a grant; the client pipelines
    its requests, or is patient and sends each once the last exchange is
    over; one that asks for a 100 (Continue) holds its body back until
    the 100 comes, sends none once a final response comes first, and now
    and then tires of waiting; the stream ends when the bytes run out,
    early at a cut, idle or with a read on its way, after a request or
    mid-way, and fails, before its end or after it; an answer to a
    demand the server withdrew may still come, before its close or after.
    Above, a service asks for each request when it feels like it, reads
    the body with demands of every shape, slowly, withdraws a demand and
    discards, responds at the moment its plan draws (at once, while a
    demand of its on the body is outstanding, partway through the body,
    once it read it, or once it discarded it) with a response now and
    then one the server must refuse, writes the reply in pieces within the
    room granted, withdraws the reply's demand now and then, as a machine
    stacked on it does when it closes, and closes then or a while after,
    stops for a while, and closes after the last request or at any
    moment. The world checks both sides' contracts as the client's does,
    a 100 before any read of a body its client holds back among them, and
    `waiting()` against what the neighbours see. Each call is held to a
    reference reader of requests, which shares nothing with the server:
    the call, or the rejection; the body; and the outcome, the reuse as
    the world reckons it from the request, the response and when the body
    was read. What the server wrote is held, byte for byte, to a writer of
    the test's own: the 100 if it went, the head, and the reply framed as
    the side above sent it; and a rejection's answer.
  - **The writer's** runs one writer for a stream of events and comments
    of every shape, one in fifty flawed. Below, room is granted late, the
    stream says it ended now and then, and fails; above, a user writes
    one at a time when it feels like it, stops for a while, finishes, or
    closes at any moment. The world checks `MAX_OUT`, room only, one
    demand at a time, one `Send` a grant within it, one answer each, and
    `waiting()`. Each item is refused exactly when a check of the test's
    own refuses it; what was written is the frames of what was sent, byte
    for byte against a writer of the test's own, and a prefix of the one
    being written; and it reads back, by the reference reader and by the
    reader's own world, as each event was written.
  - A seed replays to the same run.
- **Transcripts** in `tests/http/transcripts/`, each `<name>.http` beside
  `<name>.expect`, what it must decode to: the head, the body or the
  events its body holds, and the outcome. Each is written by hand after
  the public format of its peer, and says so, but one captured from the
  user's own forge with curl, unauthenticated; its expectation was
  drafted by the reference readers and checked by hand. Forty-seven:
  - an LLM provider's streams: Anthropic's Messages, a text answer
    chunked, a tool call by length (a synthetic variation: Anthropic
    sends its streams chunked), an overloaded error as a 529, and one as
    an `error` event mid-stream; OpenAI's Chat Completions, a text answer
    chunked and tool calls by length, each ending with `[DONE]`;
  - a forge's API: a Forgejo pull request by length, a page of issues
    chunked with its `Link` and `X-Total-Count`, a 404, and a repository
    as a real Forgejo behind nginx sent it, chunked;
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

  Requests are kept the same way, in `tests/http/transcripts/requests/`,
  each with what the server must make of it, served by a side above that
  reads each body to its end and answers `200`: the call or the
  rejection, the body, and the outcome. Each is written by hand after the
  public format of its client, and says so. Thirty-two:
  - curl's: a GET, a HEAD, a JSON POST, a large body that waits for a 100
    (Continue), a chunked upload from standard input that waits for one
    too, HTTP/1.0, two requests on one connection, and `Connection:
    close` before a request never read;
  - an LLM client's POST with a JSON body, as Anthropic's and OpenAI's
    Python SDKs send them, their keys redacted;
  - hostile ones: an oversized head and an oversized request line, a
    chunk size past a `u64`, smuggling with both framing headers in
    either order and with whitespace before a colon, a bad request line,
    HTTP/2's preface, a method not implemented, no `Host` and two, a
    fold, a NUL and a bare CR in a field, a coding not undone, a body
    past the limit, a head and a body cut short, a chunk without its line
    ending, a trailer section that never ends, two lengths, and too many
    fields.

  Each comes to its expectation through the server's world under several
  seeds, read a byte at a time, and cut anywhere, as the reference reads
  the prefix.
- **The machines stacked** (`tests/http/tests/stack.rs`), a small step
  towards the protocol worlds: the reader and a tokenizer per event over
  the client, as a connection routes between them, the request body
  written by the JSON writer and uploaded, over the LLM transcripts cut
  at random; every event is the transcript's and every document the JSON
  reference parser's reading of its data.
- **Protocol worlds** (testing-strategy.md, 2.5) in `tests/protocol`
  (`skein-protocol-world`): both ends of an LLM streaming exchange, built
  as two services would build them. The client's end stacks the client,
  the reader on its body and a tokenizer for each event's data, its
  request written by the JSON writer; the server's stacks the server, a
  tokenizer on the request body and the writer on the reply, each event's
  data written by the JSON writer; each routes as a connection routes,
  every call held to its `MAX_OUT`, and a scripted user sits at each top.
  The two bottoms are joined by a stream each way, carried in pieces cut
  at random and joined in the receiving intake, each end's side below
  keeping the stream's contract as io keeps it; a closed end drains what
  comes for its linger, and what comes after resets the other's stream,
  as on a socket. A referee watches what the users saw: what the top of
  one end sent is what the top of the other received, the request and
  every event, token for token; and what the writer sent and the reader's
  user has not read is never more than the caps between them. The
  scenarios: an answer streamed whole; a slow reader at the client's top,
  far more events than the caps hold, which must stop the writer at the
  server's; a response that comes mid-upload, an error before the body
  is read, which stops the client's upload and is read; one end closing
  while the other sends; and the wire resetting at any moment. Once both
  ends are closed, every machine is, and each stack withdrew what it
  demanded below before its owner closed the stream. A seed replays to
  the same run. The worlds run in plaintext. `skein-world` drives
  processes' `iterate` over the simulator, through the kernel's records,
  which a world joined by bytes has none of, so these keep a small
  harness of their own of the same shape: one loop, the contracts as it
  goes, a referee for the scenario's expectations.
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
  exactly. The server and the writer, in a binary of their own
  (`tests/http/tests/memory_server.rs`): the server at its limits, a head
  at the head limit with its fields at theirs, bodies by length and
  chunked with a trailer section at its limit, a 100 (Continue),
  responses at the response limit by length, chunked and without a
  body, a rejection and a body cut short, each read with three demands,
  responded to before and after its body, and closed, failed and
  discarded after every step; and the carry-over held with a delivery.
  The writer's peak at its limits is its worst case exactly.
- **The fuzzy suite** (`tests/http/tests/fuzzy_*.rs`): 20,000 connections
  of one to four generated exchanges, valid, mutated, and corrupted where
  a random edit seldom lands (another major version, a code past 599, an
  upgrade, conflicting framing, a chunk size past a `u64`, a trailer
  section without end), and 2,000 transcripts cut and mutated, each
  under limits and neighbours drawn from its seed; 12,000 event streams
  generated and mutated, and 2,000 of the transcripts' event bodies. Each
  sweep asserts that what it injects fell (testing-strategy.md, 3): every
  outcome of an exchange, every refusal of a call and every error of a
  stream, each way a stream can end or fail below and what the machine
  waited for when it failed, each thing it waits for, a close while it
  waited for each thing, room granted while a response
  waited, a response read mid-upload, a withdrawal and an answer after
  it, a discard, a connection reused, a CR's LF delivered alone, and a
  line longer than a chunk read in pieces. The server's: 10,000
  connections of one to four generated requests, valid, mutated, and
  corrupted where a random edit seldom lands (another major version, a
  method not implemented, a fold, whitespace before a colon, both framing
  headers, a coding not undone, two lengths, a chunk size past a `u64`,
  no `Host`, a field and a request line past the head, too many fields, a
  body past the limit, a trailer section without end, HTTP/2's preface),
  and 1,000 request transcripts cut and mutated; it asserts that every
  outcome, every rejection and refusal, each way a stream ends or fails
  and what the server waited for then, a close while it waited for each
  thing, a 100 (Continue), a client tired of waiting for one, a body
  given up, a head that waited for a discard, room after the end, a
  reply withdrawn, pipelining and reuse fell. The writer's: 5,000 runs,
  every answer and refusal, an event in pieces, the end and room after
  it, a failure idle and while writing, and a close in each state. The
  protocol worlds' (`tests/protocol/tests/fuzzy_worlds.rs`): 300 runs of
  the scenarios under caps drawn down to the least the stacks allow,
  asserting that a writer was held back, an upload stopped, a writer
  heard its stream fail, a stream reset, and each outcome at each end
  fell. It stands in for the fuzz targets, which wait for a nightly
  toolchain.

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
- **lib's line scan, for the reader's lines.** A line ends at LF, CRLF
  or CR alone, and a scan has one delimiter. Scanning to LF always would
  read a stream of CR alone only when a scan filled, and lose its last
  events at the end. Following the last line's ending, as the reader
  first did, cost a stream that mixed endings a scan's delay at each
  change, a rest of a delivery held, and at the end what followed a
  change. A read that stops at either byte is lib's own (`Read::Line`),
  met by the intake's search as any scan is: every line is read as its
  end arrives, a line a demand rather than a byte, at the price of a
  one-byte delivery for a CRLF's LF; and nothing is held.
- **Every byte read counts against the event,** comments included, so an
  event that never ends fails whatever the stream sends it as.
- **The reader does not decode UTF-8,** and so does not replace what is
  not UTF-8 with U+FFFD as a browser would: its data goes to a decoder
  that refuses it.
- **A `retry` past a `u64` is ignored,** as a value of other bytes is.
- **One body reader for both sides.** The client reads a response's body
  and the server a request's by the same code (`body.rs`), which returns
  what goes up for each machine to emit as its own event: the demands'
  shapes and a delimiter split between chunks are subtle enough to have
  once.
- **The server sets room aside before it reads a request,** room for the
  longest response head, held while it reads, as io keeps a grant across
  demands for none. The answer to whatever the request comes to then
  goes down at once: a rejection never waits on a client that does not
  read, and the server never parses a request it could not answer
  (programming-model.md, 7). It is the side above that asks for each
  request (`Next`), so a service's entrance is where it chooses to ask.
- **Rejections are the server's own,** small and fixed, and end the
  connection; bad framing in a body closes unanswered, as
  programming-model.md, 8 has bad lengths do. The request line that does not fit in
  the head is a 414, as RFC 9112, 3 requires, rather than a 431, which
  is for fields; a fold is rejected, not joined, as RFC 9112, 5.2 lets a
  server; an unknown method is a 501 (RFC 9110, 9.1) and a coding not
  undone too (RFC 9112, 6.1), while codings that do not end with
  `chunked`, or give it twice, are faulty framing, a 400 (RFC 9112,
  6.3).
- **A body by length past `Limits::body` is refused at the entrance**
  (413), and a chunked body at the size line that takes it past, before
  that chunk's data is read: the server refuses whatever the service
  limits, so a side above that discards, or reads slowly, is not made
  to read an upload without end. Its answer is the same fixed 413 while
  the room set aside is still held, which it is unless a response was
  given or a 100 (Continue) took it; then the connection closes
  unanswered, as for bad framing, since a response given waits for the
  body's end and cannot go. The extensions of a body's size lines are
  bounded in all by the head's limit, the budget the server already
  gives each line of framing, rather than by a limit of their own: they
  are dropped unread, and a client that sends more than a head's worth
  of them is not one an API serves.
- **A response given before the body is all read gives up the rest,** on
  a connection not used again, rather than read the rest to keep it: a
  client may stop its upload when a response comes first, as skein's
  does, and the rest would never come. A side above that wants the
  connection kept discards the body: the head then waits for the body's
  end. This rules out answering while reading the same request's body (a
  streaming echo), which nothing pulls.
- **The server reads the body only before the head goes,** and writes the
  reply only after it, so a demand below is a read or room, never both,
  and the server never holds a read that a client which stopped its
  upload would leave unanswered while the reply waits for room.
- **A 100 (Continue) goes before the body's first read,** the moment the
  server means to read it, which a discard counts as; a final response
  given first answers instead. The client still does not wait for one
  (section 8).
- **A withdrawal on the reply is taken,** as the event writer stacked on
  it sends one when it closes, unlike the client's upload, on which
  nothing is stacked. It means the side above writes no more: the server
  withdraws what it demanded below and waits for its close.
- **A chunked response to HTTP/1.0 goes to the end of the stream,** as
  RFC 9112, 6.1 forbids `Transfer-Encoding` to it, rather than being
  refused: the side above writes the same reply to either.
- **The server writes the `Date`,** from `env.wall`, on every head it
  writes, its own answers included, as RFC 9110, 6.6.1 asks of a server
  with a clock, and refuses one from the side above as a field it writes
  itself: no service forgets it, and none writes it twice. Its length is
  fixed, so the answers stay of a fixed length and the longest of them,
  and the least `Limits::response`, grows by its 37 bytes. The date is
  reckoned with integers alone, by whole 400-year cycles of the
  Gregorian calendar (Howard Hinnant's `civil_from_days`), as no `std`
  is at hand. A 100 (Continue) carries none, as 6.6.1 allows of a 1xx.
  Reason phrases are the standard's, or none.
- **Each event or comment the writer writes is a block of its own,**
  ended by a blank line, so a reader's count starts over at each, and
  every event written is one a reader dispatches: a block that sets only
  an id or a reconnection time is not written. Data is split at every
  line ending, so a reader reads its CRs and CRLFs back as LFs.

## 8. Open questions

- **Connection reuse in the client.** Sequential reuse of one connection
  is in. Still open: a pool of connections per peer, who sets its size,
  and how a pool learns that an idle connection ended (the client says
  so in `waiting()` only).
- **`Expect: 100-continue` in the client.** A caller may send the field,
  but the client does not wait for the 100 before it takes the body.
  Waiting would need an interim event plus a read while the upload is
  idle, which section 3.1 rules out. skein's server sends a 100 when it
  reads, and a client that sends at once is read all the same; whether
  the client should wait waits for a server that needs it.
- **A server's deadlines.** `waiting()` says `Request` both for an idle
  keep-alive and for a head that comes slowly; a connection that wants a
  shorter deadline for the second cannot tell them apart. Whether the
  server says so waits for the engine's webhooks.

## 9. Not built yet

The client, the server, and both sides of server-sent events are built,
with their machine worlds and transcripts, and the protocol worlds of
testing-strategy.md, 2.5, an LLM client's stack against a server's. temper
pulls next: both, for the engine's forge client and its webhooks.

Also not built: chunked uploads; content codings (`gzip`), which the
client refuses and the server answers with a 501; trailer fields, which
are read and dropped; reconnecting an event stream, for which the reader
keeps the reconnection time and the last event ID; HTTP/2 and upgrades,
until a peer requires them; reading a request's body while answering
it; the heap metered in the protocol
worlds, which join two stacks in one thread and so meet testing.md, 9's
open question on heap handed between them, while each machine's worst
case is checked in its memory tests; and the **fuzz targets** (`fuzz/`,
fed `Bytes` under every demand), which wait for a nightly toolchain, the
fuzzy suite standing in for them. Transition coverage of the handlers
(testing-strategy.md, 6) waits for `cargo llvm-cov`, which is not
installed.
