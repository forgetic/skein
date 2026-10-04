# skein-llm

A provider-neutral LLM client following Skein's step model: no async, runtime,
sockets or hidden retries. Its backends are ChatGPT's Codex subscription
Responses route and Anthropic's OAuth Messages route. It composes
`skein-http` (HTTP/SSE) and `skein-json` over a caller-owned plaintext stream,
normally supplied by TLS.

`Prompt` contains instructions, model, conversation messages and function
tools. Ordered `Block` values describe text, refusals, tool calls/results and
opaque reasoning. Responses expose text, reasoning and tool-argument deltas,
completed blocks, and one final `Completion` with stop reason and usage.
Completed blocks preserve provider replay metadata for the next turn.

For Codex, the caller obtains and refreshes an OAuth access token, supplies the
`chatgpt-account-id`, and connects to `Endpoint::codex()`'s authority. The
crate never reads an auth file or initiates sign-in. Additional endpoint
headers can identify the actual client or provide session/request IDs.
Archived Pi identity constants in `openai::identity` are explicitly opt-in.
Codex requests use `store:false`, `stream:true` and full history; they omit
unsupported token caps and sampling settings.

To drive one call:

1. Configure `client::Limits`; check `worst_case` against available memory
   and `largest_read`/`largest_room` against the lower stream's caps.
2. Build a `Call` with its owner token, endpoint, credential and prompt.
   `Client::prepare(call, &limits)` validates it before touching the stream.
3. Reserve `client::MAX_OUT` room in the upper and lower queues before each
   entry point. Call `client::down(..., Request::Start, ...)`, and forward its
   `stream::Down` records to the lower stream.
4. Feed lower `stream::Up` records into `client::up`. Drain `resume` while
   `has_work()` is true before delivering another input.
5. Request one data event with `Request::Next`. A delta, completed block or
   completion consumes that demand; progress/extension events do not. Stop
   asking to stop further response reads. Failures need no demand.
6. Execute application tools using the completed `ToolCall` block. Argument
   deltas are incomplete JSON. Keep the final completion's replay metadata
   when appending its assistant message to history.
7. After `Completed`, wait for `Reusable` before `next_call` on the same
   authority/provider. It preserves the HTTP machine and buffered carry-over.
8. On `Event::Close`, close the actual lower stream and call `client::closed`
   only after it settles. Cancellation ends with `Cancelled`, then `Closed`.

The caller schedules deadlines using `waiting()`, and reports them through
`abort(..., Failure::TimedOut, ...)`. There are no automatic retries; failures
include structured classification and evidence of whether the request may
have reached the provider. Limits remain fixed across all entry points and
connection reuse.

For Anthropic, use `Endpoint::anthropic()` and
`Credential::anthropic(access_token)`. Sign-in, refresh and secure token storage
remain with the caller. The default route is `api.anthropic.com/v1/messages`;
requests send `Authorization: Bearer`, `anthropic-version: 2023-06-01` and
`anthropic-beta: oauth-2025-04-20`. Version/beta headers can be overridden
through `Endpoint::headers`. Credential and framing headers cannot be overridden.

Anthropic uses native ordered message blocks, tool schemas and tool-result
error flags. `Prompt::max_output_tokens` sets its required `max_tokens`, defaulting
to 4096; zero is invalid. Codex requires this setting to be `None`.
`reasoning_effort` accepts `off` or adaptive-thinking effort `low`, `medium`,
`high`, `max`; model compatibility is the caller's choice. Anthropic rejects
`cache_key`, which has no equivalent in this dialect. Completed thinking and
redacted-thinking blocks retain signatures/data as Anthropic-tagged replay.
Received tool arguments remain raw; resending requires valid JSON objects and
native Anthropic identifiers. Provider replay cannot cross dialects.

The optional `anthropic::identity` module archives Tongs' Claude Code identity
constants and static headers. A caller choosing that compatibility profile
explicitly uses `claude_code_headers()` and `identity::instructions()` to
place the identity in a separate first system block, and supplies its own
session/request IDs. Skein sends the
caller's tool names unchanged. These historical values do not establish current
live subscription admission.

The low-level `openai` and `anthropic` modules expose bounded document codecs
for fakes and specialized protocol stacks. Both share `Json` and dialect limits.
The Anthropic event vocabulary follows the
[Messages streaming protocol](https://platform.claude.com/docs/en/build-with-claude/streaming).

The [design](../../docs/design/llm.md) defines lifecycle and memory contracts.
`cargo nextest run -p skein-llm -p skein-llm-world` checks codecs and the full
stack; the world crate's fuzzy profile varies fragmentation, flow control and
close races. Archived response fixtures document their capture provenance.
Offline verification does not claim current live subscription admission.
