# Shared scripted LLM peer

## 1. Role

Skein owns reusable provider-neutral LLM calls and their independent wire
peers. `skein-fake-llm-domain` depends only on `skein-lib`; it knows no HTTP,
JSON, provider client, application tools, checkout paths or credential policy.
`skein-fake-llm-protocol` depends on that domain and the shared HTTP/SSE/JSON
and LLM codecs; provider codecs are `skein-llm`'s (llm.md). Application
worlds supply their own tool schemas, inputs, invalid calls, scripts and
effect expectations, and interpret the answers themselves.

## 2. Script domain

`Config` supplies fixed pending-call and owned-byte limits, seeded failure
chances, injected latency and output limits, and the cache, usage and
tool-choice settings of section 2.1. `Domain::configured` consumes
scripts and `Menu` together under `script_bytes`. The menu supplies complete
argument bodies and deliberately invalid names/bodies. An empty menu produces
no random tool calls. No application body or unknown tool name is invented.
`Line::Opaque` carries complete caller-supplied provider replay bytes; the
domain counts and preserves them without parsing.

`Event::Call` transfers a query and one reply right. A request is refused or
produces one delayed `Request::Reply`; `fire` uses only injected time.
Conversation validation requires every result ID to answer exactly one prior
call ID; tool calls belong to assistant messages and results to user messages.
Generated provider-call names use checked counters; exhaustion
refuses rather than reusing an identity. Query, script/menu and generated
answer ownership is checked before effects or payload copies.

### 2.1 The cache, usage and tool choice

- **A query says how it may be cached,** in the domain's own terms, which
  the byte peer fills from the wire (section 3.1): automatically, by
  prefix, within a scope, the opaque key a provider routes by, or none;
  or only up to marks, the positions the request asked to cache.
- **The domain keeps what calls wrote:** a bounded table of prefixes, each
  with its scope, its length in tokens and a digest of its content,
  written at injected time and expired after the configured lifetime. The
  oldest entry gives way when the table is full.
- **Reads.** An automatic query reads the longest prefix written in its
  scope that it begins with. A query without a scope reads only with the
  configured chance, as a provider's random routing does, so a world can
  tell an affinity that reaches the wire from one that does not. A marked
  query reads the longest prefix, ending at one of its marks, that an
  earlier call wrote, and never past its last mark.
- **Writes.** An automatic query writes its whole prompt. A marked query
  writes the prefix at each of its marks; what it writes past what it
  read is its cache write.
- **Usage is optional, field by field.** The domain's usage has the same
  five fields as skein-llm's `Usage` (llm.md, section 2.3), each present
  or not. The
  configuration says which the wire reports, per dialect; the peer omits
  the rest, so worlds meet usage not reported as well as usage of zero.
- **Tool choice.** A query carries its choice: with `None` the domain's
  random answer calls no tool, and with `Only` it calls only the tools
  named. With the configured chance it calls outside the choice anyway,
  as a provider may, so a world sees its caller answer such a call as not
  run. Scripted turns are the application's, and call what they name.
- **Cut and oversized calls** need nothing new: an answer longer than the
  query's output cap is cut short inside its first tool call, which the
  peer sends as its dialect's own truncation, and a menu body longer than
  the client's input limit is an oversized call.

## 3. Byte peer

`Service` owns bounded routing across connections; `Server` owns one HTTP
exchange and one SSE writer. Start/up/down/resume entrances reserve their
published output maxima. Lower room and reads are explicit. A complete
provider request becomes the neutral domain query, and an actual domain
terminal becomes a bounded native stream.
`Service::target` consumes a whole domain terminal and returns it alongside
the actual connection token; an unmatched/closed route returns the same
owned terminal. Looking up a route never discards its reply right.

Each entry receives a borrowed explicit credential. The fake compares the
bearer and required account bytes; it owns no sign-in, refresh, JWT parser,
credential generation or durable credential record. The caller remains the
credential owner. The peer's provider selection is wire configuration, never
application policy.

`Close` requests lower settlement; `closed` acknowledges actual settlement.
A lost connection cannot authorize effects or turn a withdrawal into a
provider answer. The independent script domain still produces its actual
terminal; old connection routing may no longer deliver it.

**On io,** the byte peer also runs as a process of its own. It listens on
loopback, in plaintext, or in TLS with skein's test certificates, so that
a simulated world and the real loop host it as they host a service
(testing-strategy.md, 4 and 4.1). The fake issuer runs the same way
(oauth.md, 5).

### 3.1 Request heads, affinity and echoes

- **The peer records each request's head:** its fields, names and values,
  within the observation caps, beside the decoded query, so a world can
  assert what reached the wire: that every request of a conversation
  carries the same affinity, and that two conversations' threads differ.
- **It reads the dialect's cache instructions into the query.** Codex:
  `prompt_cache_key` and the `session-id` header must be the same key in
  its rendered form, and `thread-id` must be well formed; the key becomes
  the query's scope. Anthropic: each `cache_control` marker becomes a
  mark; there may be at most four, never on thinking or redacted
  thinking. A request that breaks a rule the dialect promises is refused
  as invalid: the fake checks its client (testing-strategy.md, section 6).
- **It echoes as a provider does.** Its Codex events can carry, as the
  backend's do, the request's instructions and tools and a usage
  attribution entry per input item, sized by configuration, so worlds
  exercise a client that keeps only what it reads (llm.md, section 4.4).

## 4. Shared codec and replay

`skein-llm::DocumentLimits` is the neutral document-bound vocabulary. Native
Anthropic peer decode/encode entrances live in the shared codec, not an
application's adapter. Tool schemas remain whole bounded JSON values.
Signed/redacted thinking preserves unknown extension fields. Required known
fields and complete JSON shape/bounds are checked; thinking/signature deltas
replace only their own values in the retained provider head.
The native request decoders read the dialects' cache instructions and
tool choice into the neutral query (section 3.1), and the usage encoders
write only the fields the configuration reports (section 2.1).
Unknown nonempty assistant-native block kinds keep their complete object,
including nested proof fields, without inferred effects or deltas. Known
text/tool/thinking kinds cannot bypass their admission rules by masquerading
as the opaque variant. Duplicate queried known fields are refused; unknown
extension keys remain uninterpreted, including duplicates.

`Replay::to_bytes` and `Replay::from_bytes` provide a versioned opaque durable
envelope. Its seven-byte version/tag/length header is additional to the raw
`opaque_bytes` allowance. `replay_bytes` exports the complete receiver cap;
`replay_worst_case` includes temporary JSON and simultaneous raw/envelope
storage. A complete raw replay admitted at its cap still fits its envelope.
Provider compatibility is validated by the real Client when replay is used.
`Prompt::output_ceiling` applies configured dialect support; Codex omits its
unsupported wire field and the caller still bounds local output.

## 5. Outside stories and bounds

`skein-llm-world::World::prepared` adopts one freshly prepared Client and
caller-owned literal HTTP/SSE response bytes. The caller supplies the same
limits used at preparation. Adoption has no stream entrance, send, read or
terminal effect and constructs no second Client. `World::new` prepares once
and delegates to this identical initialization. Demand and intake,
fragmentation, queue limits, terminal checks and lower settlement are shared;
the constructor neither parses fixtures nor controls application policy.
The original Client keeps its callback owner and prepared request bytes.
Its price is counted once; the world's source tape, request tape, terminal
observations, queues and intakes remain separate owned storage. Its stories
expect literal bytes, never the peer's response encoder's output, and
establish no live provider admission.

`skein-llm-world::fake::Exchange` connects the actual shared Client to the
independent byte peer and real script domain. It records requests and
their heads, queries, response bytes and actual terminals. Its transport uses bounded intakes and
checks every read/room/send; it owns no provider parser. Its stories
carry whole schemas, literal argument bytes, exact paired feedback and
continuation through both wire configurations, then corrupt provider IDs.
Cancellation has no terminal before actual lower settlement; repeated close
settlement produces no second terminal.

`Exchange::prepared` adopts an application's already prepared actual Client.
The caller moves the exact endpoint, credential and unchanged receiving limits
used at admission as independent peer metadata, released after configuring
the peer. It creates no second Client and performs no second preparation;
`Exchange::new` prepares once and delegates to the same constructor. The
Client is priced once. Peer credential bytes and the retained target are
separate ownership, as are the caller's retained application declarations,
decoded results and observation buffers.

`Exchange::at(now, wall)` installs the caller's one iteration snapshot in
the actual Client, independent script domain and byte peer before their
entries run. The caller supplies nondecreasing monotonic `Time`; `Wall` may
jump independently and never arms deadlines. Installing time neither starts
the exchange nor fires timers, delivers bytes or settles effects; the caller
still drives the existing entrances. It adds no retained state or memory
allowance.

`Exchange::observe(ObservationLimits)` opts a fresh, unstarted exchange into
fixed observation counts and whole-record byte caps. Events, decoded queries,
request heads, manual pending calls and both wire tapes reserve their exact
wrapper capacity.
Payload ownership is checked before cloning or appending, including public
native replay token wrappers and their owning bytes. Drained records become
caller ownership; a moved vector's replacement capacity is reserved at the next
physical entrance. Without `observe`, observation is unconstrained.

`extra_worst_case` prices the peer, script domain, routing service, queues,
intakes, delivery scratch, credential/target and configured observations. It
excludes the one actual Client: callers add the Client or adapter price once,
plus their own retained inputs, metadata clones and drained records. Checked
arithmetic refuses impossible products without allocating the proposed cap.
The driver reclaims retired service routes after draining their actual outputs;
`Service::calls()` reports allocated live and retired slots without content.

Memory stories compare the counting allocator's high water with production
`worst_case` values plus the separately priced world intakes, wire tapes and
observation copies: replay transit, extended opaque heads, a maximum-size
scripted response, the exact joint script/menu cap with every delayed-call
slot at its full answer cap (one over each cap is refused), and two
connections sharing one delayed script domain. They establish shared
Client/peer ownership only: application worlds add their schema, result and
replay copies and their outside effect ledger to this envelope.

The redacted Anthropic captures beside the shared codec keep their
provenance. They flow through the actual Client under fragmentation and
prove wire preservation, not current live subscription admission.
