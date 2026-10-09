# LLM calls

Provisional, 2026-10-09. `skein-llm` is a reusable composition of HTTP,
SSE and a provider's JSON dialect. It depends on `skein-lib`, `skein-http`
and `skein-json`, and knows nothing of a service's domain, sockets,
credentials store or tools. This is a composition crate, rather than a
primitive protocol machine: the lib-only rule for the individual machines
remains in force.

Providers are OpenAI's ChatGPT/Codex subscription Responses route and Anthropic's
OAuth subscription Messages route.
For Codex, the caller obtains and renews an OAuth access token and supplies its
account ID. The default endpoint is `chatgpt.com/backend-api/codex/responses`.
Transport connects to that authority and provides a secured plaintext
stream. Endpoint overrides support fakes and explicitly chosen deployment
routes; the caller must bind the stream to the endpoint it supplied.
Historical client identity strings are optional data, not silently installed
as defaults. Sign-in and refresh are outside this crate.
Anthropic uses `api.anthropic.com/v1/messages`, a bearer token with no account
header, an API version and the OAuth beta flag. Version/beta overrides are
explicit endpoint headers; reserved credential/framing headers are rejected.
Its archived Claude Code identity profile is opt-in, including its system
instruction. The caller selects the model and any required compatibility headers.

## 1. In one page

- **One call, one terminal.** A client prepares one call from a neutral
  prompt, measured before its body is allocated, drives it over a
  caller-owned plaintext stream, and ends it with exactly one terminal:
  completed, failed or cancelled. It keeps no timer and retries nothing.
- **A neutral vocabulary.** Prompts, messages, blocks, tools, tool choice,
  affinity and usage name no provider. Provider metadata travels as
  tagged opaque replay. Two dialects render the vocabulary: Codex's
  Responses route and Anthropic's Messages.
- **Per-call outcomes are not failures.** A tool call too large to keep
  arrives as an oversize block with its byte count; a tool call the
  provider cut at its output cap arrives marked as cut. The completion
  completes, and the caller tells the model.
- **Failures are typed.** A local limit names itself and its bound, an
  HTTP failure carries its status, and a timeout names the deadline that
  passed.
- **Limits are derived.** `Limits::derive` turns a few declared
  quantities (window, output, reasoning item, tool payload, calls per
  response, conversations) into every limit of the client, per dialect,
  with checked arithmetic. Each relationship between them is checked at
  startup by name. skein ships the derivation, not a model catalogue.
- **Retained, not buffered.** Provider events are decoded as they
  stream, keeping only what the dialect reads; everything else is
  scanned and counted, never stored. Echoed instructions, tools and
  metadata cost a scan, not memory.
- **A stable prefix.** Encoding is byte-deterministic, so a conversation
  that only appends sends requests that only grow. Codex routes requests
  by the prompt's affinity; Anthropic's cache breakpoints sit at positions
  fixed by the prompt's shape.

## 2. Vocabulary and ownership

A `Prompt` holds instructions, a model, conversation messages, tool
definitions, a tool choice, reasoning effort, an affinity and an optional
output-token cap. A message holds ordered text, refusals, tool calls,
tool results and reasoning blocks; a completion may also hold the
per-call outcomes of section 2.4. Provider-owned
`Replay` metadata travels with the block it belongs to and is tagged with
its provider. `Replay` can be retained as a bounded durable envelope without
exposing provider fields to a domain. The exported seven-byte header is
additional to the raw opaque cap; `replay_bytes` and `replay_worst_case`
state complete receiver/transit bounds. A replay value for a different provider
is refused. Native tool calls keep the application call ID and provider item
ID in separate owned fields. Both are bounded opaque strings: pipes, quotes and backslashes
are admitted without imposing an additional identity grammar. The call ID
pairs the exact result; the item ID moves into complete replay metadata and
returns unchanged on a continued request. Neither field is joined or split.
The native answer budget charges both ID payloads, name and input. Its slot
bound uses the enlarged `Part`/`Opened` wrapper size, with active ID/kind bytes
and admitted ready payloads priced simultaneously. Actual Client controls
complete pipe/escaped IDs, restore durable metadata and observe exact continuation
pairing. An independent observer rejects altered identity evidence. The controls
reach both maximum ID payloads under the installed counting allocator.

Generic tool calls retain raw JSON argument text; the application validates and
translates it into its own tool types before execution. Codex history carries
the exact bounded UTF-8 argument string, including malformed JSON, so the
application can send an error result and let the model correct its call. The
Client does not interpret that feedback. Anthropic's native `tool_use.input`
is an embedded JSON object: replay refuses malformed or non-object input
rather than normalizing or discarding the original call. A call the
provider cut short never reaches history as a call (section 2.4), so this
refusal only meets a caller's own malformed history. Schemas are bounded JSON
values, independent of the application's tool vocabulary.

Every emitted replay also fits the complete serialized raw opaque cap, including
synthesized text/refusal item IDs, phase fields and tool item IDs. An oversized
native metadata value produces actual Client `Failure::Limit` before a completed
block or Completed terminal, with lower settlement still owed. An admitted value
at that exact cap fits its exported durable envelope without dropping metadata.
JSON admission measures the complete escaped metadata under the smaller document
and opaque cap, without allocating a serialized buffer just to count it. Actual
Client controls cover exact/one-over escaped text, refusal and tool metadata.
Replay metadata and reasoning have separate caps (section 4.2): a large
reasoning item never widens what a text block's metadata may hold.

`Json`, `DocumentLimits` and `DocumentError` expose bounded document admission
without naming a wire dialect. `Json` is a compact document (json.md,
section 5.2). `document_error` retains receiving overflow as
`Error::Limit` and malformed grammar, missing fields or wrong types as
`Error::Invalid`. It reuses the Client's classification; it starts no call,
emits no terminal and supplies no retry policy.

`Client::prepare` validates and measures the whole request before allocating
its exact body. Admission is fallible and does not touch the stream. An
accepted client has one call, named by the caller's opaque token. Every
accepted call has one terminal: `Completed`, `Failed` or `Cancelled`.
Completed blocks and deltas are separate events; completion includes all
completed blocks in provider order, its stop reason and token usage.
Delta indexes are the provider's output/content indexes, not the order in
which completed blocks are delivered. Tool arguments are incomplete JSON
until the whole call block arrives. A local size limit is a failure, not a
provider token-limit stop. A tool call past the input limit is the one
exception: a per-call outcome (section 2.4). A Codex reasoning item past
its limit is dropped instead only where the owner opted in (section 4.6).

### 2.1 Affinity

```rust
pub struct Affinity {
    pub key: [u8; 16],   // opaque: the tree of conversations that share a prefix
    pub thread: u32,     // one conversation within the key; the first is 0
}
```

- **`Prompt.affinity: Option<Affinity>`** says which conversation a
  request continues, so a provider that routes by it can serve the
  conversation's prefix from one cache. It is the only cache key: no
  endpoint carries one of its own.
- **The caller owns it.** The caller supplies the key, minted at random or
  from a seeded generator, never derived from credentials, paths or
  content, and puts the same affinity, unchanged, into every prompt of a
  conversation, resumed ones included. Conversations that share a prefix, such as a run and its
  children, share the key and differ in the thread.
- **The dialect renders it** (sections 4.6 and 4.7): Codex routes by it;
  Anthropic accepts it with no wire effect. A prompt without affinity
  renders none of it.
- **Fixed size:** it allocates nothing, and its rendering has a fixed
  length, which the request head's limit counts.

### 2.2 Tool choice

```rust
pub enum ToolChoice {
    Auto,                    // the model may call any offered tool, or none
    None,                    // the model may call no tool
    Only(Box<[Box<[u8]>]>),  // the model may call only these offered tools, or none
}
```

- **`Prompt.choice`** says which of the offered tools the model may call.
  The offered definitions (`Prompt.tools`) stay the same in every prompt
  of a conversation, whatever the choice: only the choice changes, so the
  definitions stay in the cached prefix.
- **`Only` names offered tools,** each once, at least one; admission
  refuses anything else as `Invalid`.
- **A dialect that cannot express `Only` renders `Auto`.** Neither does
  today (sections 4.6 and 4.7).
- **The client does not filter.** A call outside the choice arrives as any
  call does, and the caller answers it, as not run.

### 2.3 Usage

```rust
pub struct Usage {
    pub input: Option<u64>,        // prompt tokens neither read from nor written to a cache
    pub cache_read: Option<u64>,   // prompt tokens read from a cache
    pub cache_write: Option<u64>,  // prompt tokens written to a cache
    pub output: Option<u64>,       // completion tokens, reasoning included
    pub reasoning: Option<u64>,    // the part of output spent reasoning
}
```

- **`None` means not reported;** zero is a report of zero. A completion
  whose provider reported no usage has every field `None`.
- **The three prompt fields are disjoint.** When all three are reported,
  their sum is the prompt's size as the provider counted it.
- **Usage never fails a completion.** A dialect's arithmetic is checked.
  A report that contradicts it keeps what was reported, and the derived
  field is `None`; the completion completes. An example is a total below
  the sum of its parts. Usage is accounting: a mismatch never discards
  what the model said, and a consumer charges for a missing count as for
  an unreported one.
- **`reasoning` is part of `output`,** never added to it.
- Each dialect maps its own fields (sections 4.6 and 4.7). A cumulative
  patch replaces a field only when it reports that field.

### 2.4 Per-call outcomes

- **Oversize.** A tool call whose argument text passes the dialect's
  input limit (section 4.2) is a completed block of its own,
  `Block::Oversize { id, name, bytes }`: the call's ID, its name, and the
  byte count of its argument text as received. The arguments were scanned
  and counted, never stored (section 4.4), so the call's size changes no
  memory bound. The completion completes, and the answer budget charges
  only the block's ID and name. The caller tells the model that the call
  was too large, by how much, and how to make it smaller.
- **Cut.** A tool call the provider cut off at its output cap is a
  completed block of its own, `Block::Cut { id, name, arguments }`: the
  argument text received so far, which need not be JSON. The completion's
  stop says why (`Stop::MaxTokens`).
- **Dropped.** A Codex reasoning item past the reasoning limit, where the
  owner opted in, is a completed block of its own, `Block::Dropped {
  bytes }`: the item's byte count as received, scanned and never stored
  (section 4.6). The dialect sends nothing for it, so a history may keep
  it as it came.
- **Neither oversize nor cut is a call.** A caller that matches only
  `Block::ToolCall` never runs one. Neither can be replayed as a call: a
  history holding either is refused at admission as `Invalid`. The caller
  replaces each in its history, as its own policy decides, with a note
  and an error result for instance, so the history it sends stays
  replayable.
- **Deltas** of an oversized or cut call's arguments still go up as they
  come: they are for display, never a call.

### 2.5 Failures

```rust
pub enum Failure {
    Unauthorized,
    Exhausted { retry_after: Duration },
    RateLimited { retry_after: Duration },
    Overloaded,
    Unavailable,
    ContextTooLong,
    Invalid,
    Limit { which: Cap, bound: u64 },   // a local limit fired: which one, and its value
    Protocol,
    Cancelled,
    TimedOut { phase: Phase },          // the owner's deadline passed: Connect, Handshake, Head, Idle or Whole
}
```

- **`Limit { which, bound }`** names the local limit that fired and its
  value: the request, the response head or its field count, an event's
  skipped bytes, retained bytes or tokens, nesting depth, a string, the
  answer, a reasoning item, replay metadata, the completion's items, or
  an error body. Admission's `Error::Limit` names its limit the same way.
  The failure's text detail is derived from it; a limit is never reported
  as a malformed event.
- **HTTP status.** `Evidence::Response { status }`: a response head
  arrived, with this status. Every failure after it carries it, a
  non-JSON error body's included. The other evidence is unsent, or
  possibly sent.
- **`TimedOut { phase }`** names the deadline that passed
  (llm-connection.md, section 6). The client arms none: the owner aborts
  with it. Head and idle say the peer stalled; whole says the owner's own
  time ran out. That is the distinction an owner's retry policy needs.
- **Provider text** stays in the failure's detail, within the detail
  limit. It is data: what to show, and where, is the owner's policy.

## 3. Driving the stack

`client::down` accepts `Start`, `Next`, `Cancel` and `Close`.
`client::up` accepts the plaintext stream's events. `resume` drains bounded
internal routing work while `has_work` says there is runnable work. Every
entry point declares maximum output counts; the owner reserves them first.

`Next` asks for one delta, completed block or completion. It is idempotent
while one demand is outstanding. JSON/SSE progress and unknown extension
events do not consume that demand. The stack reads the next SSE event only
while demanded and after buffered output has been delivered. Transport
faults and cancellations may end the call without a `Next`. A terminal
answers an outstanding demand and ends all future reads for that call.

The connection's states and transitions are:

| State | Holds / waits for | Input and result |
|---|---|---|
| Prepared | sized request head/body | Start -> Head; Cancel/Close -> Closing |
| Head | HTTP upload and final head | room -> next upload piece; response -> Streaming or ErrorBody |
| Streaming | SSE reader, selective decoder, completion, at most one demand | Next -> consume one output or demand a read; provider terminal -> Draining; fault -> Closing |
| ErrorBody | bounded HTTP error document | bytes/end -> classified Failed; limit -> Failed and Closing |
| Draining | terminal already emitted, HTTP body discarded | HTTP Done(Keep) -> Idle; Done(Close) -> Closing |
| Idle | retained HTTP client | replace with admitted call -> Prepared; Close -> Closing |
| Closing | machines stopped, actual stream still owned below | caller's settled `closed` -> Closed |
| Closed | no running machines | late inputs ignored |

`Close` on an unfinished call is cancellation. `Close` is a lifecycle
notification asking the owner to close the actual lower stream; stopping
the protocol machines does not claim that socket/TLS ownership has settled.
Cancellation's terminal comes only after the owner calls `closed`, then
`Closed` follows. A previously completed/failed call emits no second
terminal while closing. Completion may precede HTTP drainage; `Reusable`
is emitted only once drainage finishes. Reuse preserves HTTP carry-over
and checks the next call's provider and authority against the binding.

There are no hidden retries or timers. `waiting()` describes the stack's
current demand, and the owning protocol layer arms the connect,
handshake, head, idle and whole deadlines (llm-connection.md, section 6).
It uses `abort` with `TimedOut { phase }` to report its timeout, or with
the transport's failure. Failures include evidence: unsent, possibly
sent, or a provider response received with its status. This does not
instruct a caller to retry.

## 4. Bounds and wire format

Every limit of the client is derived from declared quantities (sections
4.1 to 4.3). Limits are per dialect: Codex and Anthropic each have their
own, as their events, replay and output caps differ. Startup checks
ensure SSE demands fit HTTP's read cap and that each piece size fits the
transport's (llm-connection.md, section 7). JSON is decoded as each
event's data streams, selectively (section 4.4). HTTP read/room maxima are
exported for the stream owner to check.
`worst_case` adds the child machines, bounded routing queues, the
selective decoder's retained values and depth stack, request storage and
held completion; no size computation may wrap.

### 4.1 Declared quantities

```rust
pub struct Declared {
    pub window: u32,              // prompt tokens the largest model accepts
    pub output: u32,              // completion tokens, reasoning included
    pub reasoning_item: u32,      // bytes of one opaque reasoning item as the provider sends it
    pub tool_payload: u32,        // bytes of one tool call's arguments once unescaped
    pub calls_per_response: u32,  // tool calls in one completion
    pub conversations: u32,       // calls outstanding at once, across the owner
}
```

- **Per model:** window, output and reasoning item. An endpoint serves the
  models its owner sends to it, and its declaration takes the largest of
  each. Head and idle, also per model, are deadlines the owner gives per
  call (llm-connection.md, section 6), not inputs to byte limits.
- **Per deployment:** tool payload, calls per response and conversations.
- **skein ships no catalogue.** A model's window and output are the
  provider's facts and change too often for a kit. The application
  declares them; smith's come from its shell's presets and settings.
- **Configuration declares quantities, never byte limits.** `Limits`
  stays a plain value, so a test may build one by hand or lower any limit
  after deriving it (testing-strategy.md, section 3).

### 4.2 Derived limits

`Limits::derive(&Declared) -> Result<Limits, Violation>` computes every
limit, for every dialect, with checked arithmetic: `Limits` holds one set
per dialect, and a connection takes its endpoint's. An overflow, or a relationship of section
4.3 that does not hold, is a `Violation` naming it. Two factors are the
derivation's own:

- **`ESCAPE` = 6:** the most bytes JSON escaping makes of one byte
  (`\u00XX`).
- **`TOKEN_BYTES` = 8:** the bytes of text one token is allowed to stand
  for. Text averages about four bytes a token; the factor is generous
  because memory is bounded by admission (llm-connection.md, section 7),
  not by these caps.

| Limit | Rule | What it bounds |
|---|---|---|
| request | window × `TOKEN_BYTES` × `ESCAPE` | one encoded request; a prompt within its window meets it only if its text passes `TOKEN_BYTES` a token with every byte escaped |
| answer | output × `TOKEN_BYTES` | what the model writes in one completion: text, refusals, call IDs, names and arguments |
| input | tool payload × `ESCAPE` | one call's argument text as received; past it, an oversize block (2.4) |
| reasoning | reasoning item | one reasoning item: Codex's encrypted content, Anthropic's thinking and signature |
| metadata | the dialect's constant | one block's other replay metadata: item IDs, phase |
| output items | 2 × calls per response + 2 | blocks in one completion: each call with a reasoning item before it, a text and a refusal; also the content indexes |
| strings | the larger of answer and reasoning | the longest single string the decoder keeps |
| retained tokens | the dialect's fixed fields + reasoning | tokens kept from one event: an opaque value takes at most a token per byte |
| retained text | the largest of answer, input and reasoning, + the dialect's constant for its short kept fields | bytes of text kept from one event, unescaped: the collector's `text` |
| receiving | answer + output items × reasoning + one event's retained values | what one call holds of its answer at once: its memory reservation, beside its measured request |
| skip | request + `ESCAPE` × receiving | bytes of one event the dialect scans without keeping: an echo of the request and of the whole answer, escaped once more |
| tools | request ÷ the dialect's smallest encoded tool | tools in one request |
| history items | request ÷ the dialect's smallest encoded item | messages and blocks in one request |

- **Counts follow bytes.** Tools and history items bound tables, not
  memory: a request within its byte limit can hold no more of either, so
  neither is a cliff of its own.
- **Sent text has no cap of its own.** A string the caller sends is
  bounded by the request as a whole; what the caller renders into it is
  the caller's to bound.
- **The SSE reader's** line and event limits are the skip cap, counted
  and never held (http.md, section 4.2). Its chunk, and HTTP's read and
  send, follow the transport (llm-connection.md, section 7).
- **Heads.** The request head's limit is measured per endpoint
  (llm-connection.md, section 3). The response head and its field count
  are the dialect's constants, sized for its provider's header set.
- **The error body** and the failure detail are the dialect's constants.

### 4.3 Relationships checked at startup

Each is checked when the limits are derived or composed, and a violation
refuses the configuration and names the relationship:

- **input ≤ answer:** a call within the tool payload is never cut by the
  answer limit;
- **output items ≥ calls per response,** and strings ≥ input and
  reasoning, which the rules above give by construction and the check
  keeps true when a test lowers one;
- **receiving + request ≤ memory pool:** one largest call fits the pool
  (llm-connection.md, section 7);
- **connections ≥ conversations** (llm-connection.md, section 7);
- **demands fit their caps:** the SSE reader's chunk within HTTP's read,
  the tokenizer's largest demand within the reader's data face, HTTP's
  largest read and room within the transport's. These depend on the
  transport's piece sizes, so they are checked where the connection
  component is built (llm-connection.md, section 7), not in `derive`;
- **a model's output** is at most what its route accepts as `max_tokens`,
  per model, checked where an endpoint of that dialect is composed: a
  declaration for one dialect is never refused for another's route.

### 4.4 Selective, streamed decoding

- **Each event's data streams** from the SSE reader (http.md, section 4)
  into a JSON tokenizer and the selective collector (json.md, section
  5.1). The dialect's filter names the paths it reads. Every other value
  is scanned, checked as JSON, counted against the skip cap, and never
  stored.
- **Codex keeps** an event's `type`, its indexes, its item ID and delta;
  of an item, its type, IDs, name, status, phase, content parts' text,
  arguments (under the input limit) and encrypted reasoning (under the
  reasoning limit), and an unknown item whole, as opaque replay under the
  reasoning limit; of `response`, its status, the known usage fields,
  error and incomplete details. It never keeps `response.output`,
  `instructions`, `tools` or per-item usage attribution, which echo what
  it already has or does not read.
- **Anthropic keeps** each event type's known fields; a thinking block
  whole, its extension fields included, under the reasoning limit, so
  that its replay is exact; tool input under the input limit; and an
  unknown native block whole. Its events are told apart by their SSE
  event name, so its filter's root is chosen per event, and need not be
  `Tagged`.
- **Items and blocks are tagged** (json.md, 5.1): a Codex output item by
  its `type`, an Anthropic content block by its `type`. A known type
  keeps its fields; an unknown one is kept whole under the reasoning cap,
  whatever the order of its fields. The input, reasoning and string caps
  are the filter's named caps, set from the derived limits.
- **Retained values are bounded** by the retained-token and string limits.
  A value past its limit is `Limit { which }`, except a tool call's
  arguments, which become an oversize block, and a Codex reasoning item
  under the opt-in of section 4.6, which becomes a dropped block.
- **Memory is one event's retained values,** the tokenizer's text buffer
  and depth stack, and the reader's chunk: never an event's wire bytes.
  The size of what a provider echoes changes no bound.
- **Arguments stay text.** Codex's arguments are a string holding JSON;
  the decoder keeps the string's text and never parses it. The caller
  does.

### 4.5 Byte-deterministic encoding

- **The same input encodes to the same bytes.** The same prompt,
  endpoint, credential and limits always give the same request head and
  body: fixed key order, compact JSON, no clock, no randomness, no
  iteration over unordered state. Per-call headers are a function of the
  prompt alone.
- **Replay goes back unchanged.** The dialect writes provider metadata,
  reasoning and arguments from what it kept, the same way every time, and
  never rewrites their content.
- **So a prefix that is kept is sent again unchanged.** When a caller
  keeps the model, endpoint, affinity, instructions and tools of a
  conversation fixed and only appends to its history, each request's
  instructions, tools and history items are the previous request's,
  followed by what was appended. Anthropic's breakpoints are a pure
  function of the prompt's shape (section 4.7): only the tail marker
  moves, and a marker is not part of the content the provider caches.

### 4.6 The Codex dialect

Codex requests are measured JSON, `store:false`, `stream:true`, full context in
`input`, client-side function tools and encrypted reasoning included for
replay. The subscription route does not receive `max_output_tokens` or
sampling fields, and admission rejects an explicit output-token cap on Codex.
`Prompt::output_ceiling` configures the dialect-supported option while
preserving local output bounds for Codex.

- **Affinity.** The key renders as 36 bytes: lowercase hexadecimal in the
  8-4-4-4-12 form of a UUID. It goes in the body as `prompt_cache_key` and
  in a `session-id` header. The thread renders the same way, from the key
  with the thread number XORed, big-endian, into its last four bytes, in a
  `thread-id` header: thread 0 is the key itself, as a native Codex root
  thread's ID is its session's. A prompt without affinity sends none of
  the three.
- **Per-call headers are skein's.** `session-id` and `thread-id` are
  reserved names: an endpoint's configured headers may not use them, as
  they may not use the credential and framing fields.
- **Tool choice.** `Auto` sends `"auto"`, `None` sends `"none"`, and
  `Only` sends `"auto"` (section 2.2). `parallel_tool_calls` stays true.
- **Usage.** `input_tokens_details.cached_tokens` is cache read;
  `input_tokens_details.cache_write_tokens`, when present, is cache write;
  `input_tokens` less the parts that are reported is input, and `None`
  when `input_tokens` is below their sum (section 2: usage never fails a
  completion). A part the wire does not carry stays `None` and is not
  separated: its tokens, if any, stay within input. Nothing is invented
  for it; `output_tokens` is output;
  `output_tokens_details.reasoning_tokens` is reasoning. A field absent
  from the wire is `None`.
- **Cut calls.** A response that ends incomplete for its output limit
  with a function call unfinished delivers that call as `Block::Cut`.
- **Reasoning past its limit** is a `Limit` failure naming the reasoning
  limit. An owner may opt in to dropping it instead: the item is scanned,
  never stored, and `Block::Dropped { bytes }` takes its place in the
  completion. The dialect sends nothing for a dropped block, so later
  requests go without the item. The route keeps no state (`store:false`),
  so a conversation continues with less of its reasoning, not wrongly.

### 4.7 The Anthropic dialect

Anthropic sends `stream:true`, native `messages`, tool `input_schema`, and
`max_tokens`. Adaptive thinking maps explicit effort values.
Signed/redacted thinking is preserved
as native opaque replay, tool names/IDs remain unchanged, and tool results
retain their native `is_error`. The bounded decoder verifies message/block
ordering, delta types, cumulative usage patches and completion at `message_stop`.

- **`max_tokens` comes from the declared output.** It is the prompt's
  output cap, which the caller sets from its model's declared output, or
  lower; a prompt without one sends the endpoint's declared output. There
  is no fixed default. A cap above the declared output is refused at
  admission.
- **Affinity** is accepted and has no wire effect: a prompt encodes to the
  same bytes with and without it.
- **Breakpoints.** `system` is a block array, the identity profile's block
  first when one is configured, and its last block carries
  `cache_control: {"type":"ephemeral"}`. Anthropic orders its cache as
  tools, then system, then messages, so this marker caches the tools with
  the system text. A second marker, the tail, goes on the last block of
  the last message that may carry one: text, tool use or tool result,
  never thinking or redacted thinking. No `ttl` is written, so each entry
  lives the provider's default of five minutes. There are never more
  than four markers; these two leave room. Empty instructions have no
  system marker, and a last message without an eligible block has no
  tail.
- **The markers move as the conversation grows.** The system marker stays
  where it is; the tail moves to the new end, so each request reads up to
  the point the previous one wrote.
- **Tool choice.** `Auto` sends no `tool_choice`, which is the provider's
  default; `None` sends `{"type":"none"}`; `Only` renders `Auto`
  (section 2.2): Anthropic's `tool` and `any` force a call, which `Only`
  does not mean, and do not combine with thinking. A change of choice
  keeps the cached tools and system text but not the cached messages.
- **Usage.** `input_tokens` is input, `cache_read_input_tokens` cache
  read, `cache_creation_input_tokens` cache write and `output_tokens`
  output. Reasoning is `None`: the provider counts thinking within output
  and does not report it apart.
- **Cut calls.** A `tool_use` whose input is unfinished when the message
  stops at `max_tokens` arrives as `Block::Cut`, with the partial input
  text. It is never parsed into an object, so it cannot make a history
  unreplayable.
- **Thinking past the reasoning limit** is always `Limit`: Anthropic's
  thinking is replayed whole or not at all, so the Codex opt-in does not
  apply.

Bodies are uploaded in bounded pieces using HTTP room.
Responses must be identity-encoded SSE on success; non-success responses
are parsed as bounded JSON errors. Provider-defined retry hints are data
in structured failures, never automatic retries.

## 5. Verification

Focused tests cover encoding, replay, concurrent output ordering, deltas,
limits, error classification and every lifecycle boundary. Protocol worlds
drive the real HTTP/SSE/JSON stack over a demand-checking in-memory stream,
with fragmentation, slow reads, early response, truncation, transport faults,
cancellation and reuse. Memory tests compare the counting allocator's high
water with `worst_case`. Fuzzy cases live in the separate nextest profile.
Archived Temper/Tongs transcripts retain their provenance; synthetic cases
are labelled as such. Live subscription admission is not implied by offline
tests and requires a caller-provided current credential.

The [shared scripted peer](fake-llm.md) composes an independent lib-only
script domain with native bounded byte codecs. Applications supply schemas,
body menus, invalid inputs and scripts. Signed/redacted replay preserves all
bounded provider extension fields; known fields are validated without dropping
unknown opaque data. Historical Anthropic captures retain provenance and
flow through the actual Client independently of synthetic scripts.

The contracts of sections 2 and 4 add these, all in the focused suite
unless marked:

- **Affinity:** Codex carries the key in the body and in both headers,
  each conversation's thread distinct and thread 0 equal to the key; no
  affinity sends none of them; an endpoint header named `session-id` or
  `thread-id` is refused; Anthropic encodes byte-identical requests with
  and without affinity. Through the fake peer, every request of one
  conversation carries the same affinity (fake-llm.md, section 3.1).
- **Determinism:** each request of a growing conversation, encoded twice,
  gives the same bytes, and its instructions, tools and history are the
  previous request's, the moved tail marker aside, followed by what was
  appended.
- **Breakpoints:** markers at exactly the system's last block and the
  tail, never on thinking, at most four, the system marker unmoved as the
  conversation grows; the fake peer reports cache reads up to the
  previous request's tail.
- **Tool choice:** each choice's rendering per dialect; `Only` with an
  unknown, repeated or empty name refused.
- **Usage:** each field present, absent and zero, per dialect.
- **Per-call outcomes:** an oversized call next to a normal one completes
  with one oversize block and one call, the high water independent of the
  oversized call's size; a cut `tool_use` arrives as `Cut` and a history
  holding it is refused; the Codex reasoning opt-in drops an item at one
  byte over and fails without the opt-in.
- **Typed failures:** each limit, at its edge and one over, names itself
  and its bound; a non-JSON error body carries its status; each deadline's
  phase reaches the failure.
- **Derivation:** properties over drawn declarations: `derive` either
  refuses, naming a relationship, or returns limits for which every
  relationship of section 4.3 holds; no product wraps.
- **Selective decoding:** a protocol world under tiny limits, a handful of
  retained tokens and a few hundred bytes of strings, decodes events that
  echo instructions, tools and attribution far larger than both, with a
  high water independent of the echo's size. A real-structure Codex
  `response.completed` event is kept as a golden event. In the fuzzy
  suite, random extension fields never change what is decoded.

## 6. Later

- **Codex routing state.** Codex's private turn-state token, which a
  response returns and the client replays within a turn, and its
  `reasoning.context`, which keeps reasoning across user messages, are
  adopted only if a consumer's benchmarks show a material gain (smith's
  are one example). Adopted, routing state is
  an opaque value the dialect returns with a completion and the caller
  hands back within the same turn, and the reasoning context is a dialect
  option chosen per model.
- **Incremental transport.** Responses over WebSocket send only the new
  items after a previous response on the same connection, which would
  decouple a request's size from its history's. It is on skein's roadmap
  as a transport of its own, with a full resend when a connection is lost.

## 7. Open questions

- **Codex's thread rendering.** The backend's use of `thread-id` is
  private. The UUID-shaped rendering of section 4.6 mirrors native Codex;
  whether the header matters at all is for a consumer's benchmarks to
  measure, such as smith's `affinity` experiment.
- **Codex's cache writes.** Whether `cache_write_tokens` is counted
  within Codex's `input_tokens`, as cached tokens are, is not yet seen on
  the wire. Section 4.6 assumes it is; a capture that shows otherwise
  changes only the dialect's arithmetic.
- **`TOKEN_BYTES`** and the dialects' smallest tool and item, response
  head and error body constants are set from captures, and checked
  against fresh captures as models change, which a consumer's benchmarks
  can take.
- **`Only` on Codex.** The public Responses API expresses `Only` as an
  allowed-tools choice; whether the subscription route accepts it waits
  for a capture. Until then Codex renders `Auto`.
- **Anthropic's lookback.** The provider looks for a cached prefix only a
  bounded number of blocks before a marker. A turn that appends more
  blocks than that would miss the previous tail; a third marker on the
  previous tail would cure it within the limit of four. Whether a turn's
  calls per response can reach it decides whether the dialect adds it.
- **Anthropic's lifetime.** Five minutes until a consumer's benchmarks
  measure what a conversation loses when it pauses for longer, as one
  does while its caller waits on a long piece of work (smith's
  `cache-reuse/idle-gap` probe is one such measurement); a longer
  lifetime on the system marker alone is the candidate.
- **Calls past the declared count.** A completion with more calls than
  output items allow fails as `Limit`. Whether the calls past the count
  should instead be per-call outcomes, told as not run, waits for a
  completion that meets it.
