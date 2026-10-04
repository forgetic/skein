# LLM calls

`skein-llm` is a reusable composition of HTTP, SSE and a provider's JSON
dialect. It depends on `skein-lib`, `skein-http` and `skein-json`, and knows
nothing of a service's domain, sockets, credentials store or tools. This is
a composition crate, rather than a primitive protocol machine: the lib-only
rule for the individual machines remains in force.

The first provider is OpenAI's ChatGPT/Codex subscription Responses route.
The caller obtains and renews an OAuth access token and supplies its account
ID. The default endpoint is `chatgpt.com/backend-api/codex/responses`.
Transport connects to that authority and provides a secured plaintext
stream. Endpoint overrides support fakes and explicitly chosen deployment
routes; the caller must bind the stream to the endpoint it supplied.
Historical client identity strings are optional data, not silently installed
as defaults. Sign-in and refresh are outside this crate.

## Vocabulary and ownership

A `Prompt` holds instructions, a model, conversation messages, tool schemas,
reasoning effort and an optional cache key. A message holds ordered text,
refusals, tool calls, tool results and reasoning blocks. Provider-owned
`Replay` metadata travels with the block it belongs to and is tagged with
its provider. A replay value for a different provider is refused. Generic
tool calls retain raw JSON argument text; the application validates and
translates it into its own tool types before execution. Replay admission
requires a valid JSON object for arguments. Schemas are bounded JSON
values, independent of the application's tool vocabulary.

`Client::prepare` validates and measures the whole request before allocating
its exact body. Admission is fallible and does not touch the stream. An
accepted client has one call, named by the caller's opaque token. Every
accepted call has one terminal: `Completed`, `Failed` or `Cancelled`.
Completed blocks and deltas are separate events; completion includes all
completed blocks in provider order, its stop reason and token usage.
Delta indexes are the provider's output/content indexes, not the order in
which completed blocks are delivered. Tool arguments are incomplete JSON
until the whole call block arrives. A local size limit is a failure, not a
provider token-limit stop.

## Driving the stack

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
| Streaming | SSE reader, dialect decoder, completion, at most one demand | Next -> consume one output or demand a read; provider terminal -> Draining; fault -> Closing |
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
current demand, and the owning protocol layer arms connect, upload, head,
idle and absolute deadlines. It uses `abort` to report its timeout or
transport failure. Failures include evidence: unsent, possibly sent, or a
provider response received. This does not instruct a caller to retry.

## Bounds and wire format

Limits cap request/head/event/error bytes, headers, JSON depth, token count,
strings, output parts, raw tool arguments, opaque replay and answer bytes.
Startup checks ensure SSE demands fit HTTP's read cap. JSON is decoded from
bounded SSE event buffers. HTTP read/room maxima are exported for the stream
owner to check.
`worst_case` adds the child machines, bounded routing queues, temporary
JSON/request storage and held completion; no size computation may wrap.

Requests are measured JSON, `store:false`, `stream:true`, full context in
`input`, client-side function tools and encrypted reasoning included for
replay. The subscription route does not receive `max_output_tokens` or
sampling fields. Bodies are uploaded in bounded pieces using HTTP room.
Responses must be identity-encoded SSE on success; non-success responses
are parsed as bounded JSON errors. Provider-defined retry hints are data
in structured failures, never automatic retries.

## Verification

Focused tests cover encoding, replay, concurrent output ordering, deltas,
limits, error classification and every lifecycle boundary. Protocol worlds
drive the real HTTP/SSE/JSON stack over a demand-checking in-memory stream,
with fragmentation, slow reads, early response, truncation, transport faults,
cancellation and reuse. Memory tests compare the counting allocator's high
water with `worst_case`. Fuzzy cases live in the separate nextest profile.
Archived Temper/Tongs transcripts retain their provenance; synthetic cases
are labelled as such. Live subscription admission is not implied by offline
tests and requires a caller-provided current credential.
