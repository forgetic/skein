# Shared scripted LLM peer

Provisional, 2026-10-09. skein's fake LLM provider: a provider-neutral
script domain, and a byte peer that speaks both dialects' wire formats
over it, for the worlds of every service that calls LLMs.

## 1. Role and source

Skein owns reusable provider-neutral LLM calls and their independent wire
peers. `skein-fake-llm-domain` depends only on `skein-lib`; it knows no HTTP,
JSON, provider client, application tools, checkout paths or credential policy.
`skein-fake-llm-protocol` depends on that domain and the shared HTTP/SSE/JSON
and LLM codecs. Application worlds supply their own tool schemas, inputs,
invalid calls, scripts and effect expectations.

The finite-script domain and byte peer originate in Temper `25ac2ad`, copied
into Smith in migration 05s2. This extraction removes the checkout argument
menu and OAuth issuer from the generic machinery. The original provider
codec ownership moves to `skein-llm`; application-specific interpretation
stays with Smith or Temper. No branch merge from the parked runtime occurs.

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
and delegates to this identical initialization. Existing demand/intake,
fragmentation, queue limits, terminal checks and lower settlement are shared;
the constructor neither parses fixtures nor controls application policy.
The original Client keeps its callback owner and prepared request bytes.
Its price is counted once; the world's source tape, request tape, terminal
observations, queues and intakes remain separate owned storage. Focused
synthetic literal controls cover both native endpoints: original request and
opaque callback (including zero), exact text and usage, unique completion and
reuse, explicit Close followed by actual Closed, and repeated settlement
without another terminal. These controls establish no live provider admission
and do not use the peer's response encoder as their expected-value oracle.

`skein-llm-world::fake::Exchange` connects the actual shared Client to the
independent byte peer and real script domain. It records requests and
their heads, queries, response bytes and actual terminals. Its transport uses bounded intakes and
checks every read/room/send; it owns no provider parser. Positive controls
carry whole schemas, literal argument bytes, exact paired feedback and
continuation through both wire configurations, then corrupt provider IDs.
Cancellation has no terminal before actual lower settlement; repeated close
settlement produces no second terminal.

`Exchange::prepared` adopts an application's already prepared actual Client.
The caller moves the exact endpoint, credential and unchanged receiving limits
used at admission as independent peer metadata. It creates no second Client
and performs no second preparation; `Exchange::new` prepares once and delegates
to the same constructor. The direct adoption control drives both configured
dialects through the real script domain and byte peer, checking the original
callback owner, exact configured input and actual completion/reuse terminals.
The Client is priced once. Peer credential bytes and the retained target are
separate ownership, as are the caller's retained application declarations,
decoded results and observation buffers. The temporary endpoint metadata is
caller input and is released after configuring the peer.

`Exchange::at(now, wall)` installs the caller's one iteration snapshot in
the actual Client, independent script domain and byte peer before their
entries run. The caller supplies nondecreasing monotonic `Time`; `Wall` may
jump independently and never arms deadlines. Installing time neither starts
the exchange nor fires timers, delivers bytes or settles effects; the caller
still drives the existing entrances. Focused controls adopt one actual Client
for each configured dialect, inject a nonzero origin and fixed fake latency,
and observe no response before due despite a wall-clock jump. At due the
actual fake, byte peer and Client produce the exact answer and unique
completion/reuse, followed by real Close/Closed settlement. A second control
cancels after installing due time but before ticking, then settles lower
effects and fires/reclaims the late fake terminal without another Client
terminal or wire response. This entrance adds no retained state or memory
allowance.

The four redacted Anthropic Tongs captures retain their provenance beside the
shared codec. Actual Client tests check known completion text, calls, stop
reason and all usage fields under fragmentation. Synthetic cases are separate
and these archives establish no current live subscription admission.

Counted tests exercise replay transit, extended opaque heads and an actual
maximum-size scripted response through Client and byte peer. Another driver
fills the exact joint script/menu cap with 128 tiny entries and empty part
wrappers, then holds every delayed-call/alarm slot at its full answer cap.
One-over slot, script/menu and answer caps are refused. The two-connection
story uses one actual delayed script domain: both issued HTTP calls enter
`step`, actual `fire` terminals route through the shared service, and both
Clients observe the exact scripted answer. A third actual terminal survives
observed connection cancellation as an unmatched owned reply. This story
counts each native Client, Server, Service, intake, queue and observation
buffer separately from the inline routing slots. These drivers compare
production `worst_case` values plus separately priced world intakes, wire
tapes and observation copies. Application worlds must add their application
schema/result/replay copies and outside effect ledger to this envelope.
The integrating parent's runtime checks and serial suite measurements are
recorded in this increment's commit. The final rebased workspace gate precedes
its merge; Smith's real consumer has a separate review and gate.

`Exchange::observe(ObservationLimits)` opts a fresh, unstarted exchange into
fixed observation counts and whole-record byte caps. Events, decoded queries,
request heads, manual pending calls and both wire tapes reserve their exact
wrapper capacity.
Payload ownership is checked before cloning or appending, including public
native replay token wrappers and their owning bytes. Drained records become
caller ownership; a moved vector's replacement capacity is reserved at the next
physical entrance. Existing worlds keep unconstrained observation behavior
until `observe` is called.

`extra_worst_case` prices the peer, script domain, routing service, queues,
intakes, delivery scratch, credential/target and configured observations. It
excludes the one actual Client: callers add the Client or adapter price once,
plus their own retained inputs, metadata clones and drained records. Checked
arithmetic refuses impossible products without allocating the proposed cap.
The driver reclaims retired service routes after draining their actual outputs;
`Service::calls()` reports allocated live and retired slots without content.

Focused controls meter both native Clients and peers through construction,
Start, every progress step, Close, actual Closed and drop with an 8,192-byte
input. Caller-held terminal replay ownership is counted independently and
returns to zero after its final drop. The existing independent HTTP reference
reader checks complete chunked response consumption and the whole literal
answer; its passive validation scratch is outside runtime entrance peaks and
drops before progress resumes. Another control opens five actual connections
through one four-slot service, observing reclamation after each settled call.
Exact query capacity copies one whole actual query; one byte less refuses
before cloning and still settles cancellation and physical close. These
controls establish shared Client/peer ownership, not an application's combined
root, schemas, copies or simultaneously retained physical bindings.

## 6. Copied-codec disposition

This increment supplies shared client/peer machinery before Smith removes
its provider-specific copies. It does not claim that the old leaf APIs or
automatic request policies are equivalent to the shared API. Historical
source remains named by Smith's migration-05s2 ledger: the copied provider
codecs originated in Temper `25ac2ad`. Smith's current application domains
did not consume those leaf APIs; its former fake byte peer did.

The audit covers `crates/smith-llm-anthropic/src/tests.rs`, the corresponding
OpenAI test module, and the old fake-domain/protocol packages. The original
fixture provenance READMEs remain beside the moved captures. All 28 captured
resource files (16 Anthropic and 12 OpenAI, excluding README prose) have been
checked byte for byte by the integrating parent. Captures prove historical
wire preservation, separately from synthetic semantic controls and current
deployment admission.

| Old assertion or surface | Shared ownership and disposition |
| --- | --- |
| `opaque_thinking_and_unknown_blocks_are_replayed_with_all_fields` and `thinking_start_extensions_survive_and_server_rejects_oversize_request` | Native codec controls preserve thinking extensions and complete unknown `future_block` nested proofs. `tests/llm/tests/fake.rs` carries actual Client → byte peer → resumed request controls for signed thinking, encrypted reasoning and unknown native blocks. Tagged replay admission still rejects another configured dialect. |
| `archived_real_provider_requests_and_answers_match_known_completions` | `tests/llm/tests/archives.rs` uses the actual Client and the four moved Anthropic captures, retaining known text/call/stop/all-counter expectations. Existing OpenAI codec fixture controls remain. This establishes no current live subscription admission. |
| `mutation_of_order_kind_index_or_terminal_fails_exactly_once` | Native decoder ordering/delta/terminal controls and actual stacked-client worlds retain one terminal. Native peer entrances now also refuse malformed known request/event fields before any write traversal. |
| `malformed_json_duplicates_and_every_document_limit_are_refused` | Shared JSON and native codec controls retain grammar, queried-field duplicate, depth/string/token/document/part bounds. Unknown opaque extension fields are preserved rather than assigned a new global duplicate-key policy. |
| Malformed received tool arguments and correction history | Codex's native argument field is a string: actual Client controls preserve malformed argument bytes and durable item metadata, send the exact paired error feedback, and receive a corrected call. UTF-8, raw string and complete escaped request bounds remain enforced. Anthropic's native input is an embedded object, so unrepresentable malformed history is explicitly refused; no rewritten call is substituted. Application effect admission still validates the original arguments. |
| `server_events_roundtrip_and_errors_classify_status_and_resets` | Shared codec/common failure classification and real Client worlds own native event encoding, status/reset handling and error-body limits. Caller retry policy is not moved into the peer or Client. |
| `input_cap_discards_only_input_and_answer_cap_cuts_the_open_tool` | Both are per-call outcomes of the shared client (llm.md, section 2.4). A tool call past the input limit completes as `Block::Oversize`, its ID, name and byte count kept and its input discarded; a call the provider cut at its output cap completes as `Block::Cut`, with provider-reported `MaxTokens`. Neither is executed or replayed as a call. A local limit on anything else is still the typed `Failure::Limit`, and the client fabricates no `Stop::MaxTokens`. |
| `measured_request_roundtrips_and_marks_only_four_tail_positions` | Measured bounded encoding, known sender/tool/schema admission and current native core decoding remain. The Anthropic dialect places `cache_control` markers itself, on the last system block and on the tail, with the default five-minute lifetime and never more than four (llm.md, section 4.7); the fake peer models cache reads up to the last marker (section 2.1). The old placement on each of the last two user messages is not restored. |
| Legacy request `system` block array, `thinking_budget`, `metadata`, `context_management` | The shared Prompt has neutral instructions, configured identity and reasoning effort; the Anthropic dialect writes `system` as a block array to carry its marker. It does not expose the other unconsumed native request fields. The peer parses admitted historical core requests and leaves unknown deployment options uninterpreted; it does not promise to reconstruct those options from the decoded neutral Prompt. Smith must not recreate provider policy to recover the old leaf surface. |
| Fake random checkout argument menu and `delete_repository` | Caller-owned bounded `Menu` supplies complete bodies and invalid names. Smith/Temper worlds retain application scripts and effect expectations. No checkout vocabulary or tool authority remains in either shared fake. |
| Fake OAuth issuer and Smith OAuth copy | The peer receives an explicit borrowed caller credential. Sign-in, claims exchange, refresh and durable credential storage remain with the external credential owner; this fake does not prove or replace Temper's credential-owner stories. |
| Fake owned-memory assertions | `fake_memory.rs` attains script/menu/wrapper and all delayed-slot caps; composed memory controls price real protocol/client ownership and raw/envelope transit. Production bounds are checked without counting transferred outputs as retained domain state. |
| `scripts_queries_and_answers_past_their_caps_are_refused` | `exact_joint_menu_script_cap_and_all_delayed_slots_include_empty_part_wrappers` retains script/menu and answer refusal. `exact_query_cap_is_accepted_and_one_byte_less_returns_one_actual_refusal` sends the same query at its exact cap and one byte below, then observes actual delayed success or `ContextTooLong`, one terminal for the original reply right and complete reclamation. |
| `an_unrepresentable_memory_bound_is_refused` | `extreme_call_and_answer_configuration_has_no_representable_memory_bound` checks an ordinary bound and checked refusal for maximum call and answer capacities, without allocating the extreme configuration. |
| `random_and_truncated_tool_answers_stay_within_the_scratch_bound` | `random_and_scripted_full_and_truncated_tool_scratch_stays_within_the_bound` meters actual startup, full generation, simultaneous truncation scratch, delayed `fire` and reclamation for random caller-menu tools and scripted tools. Both slots hold real answers; outputs preserve whole caller names/bodies or their literal cut prefix, actual ToolCalls/Length and usage. Owned outputs are observed and released before the meter checks the production bound; all caller data and retired slots are released afterward. |

Smith's actual adapter, root/provider wire histories, application decoding,
canonical concrete result rendering and consumer pin are a subsequent gated
part of this same migration. They are pending here. No copied Smith package
has been deleted and no consumer test count is inferred from this shared-kit
checkpoint.

## 7. Open questions

- **Cache granularity.** Providers cache only past a minimum length and in
  steps of tokens; the fake caches any prefix at a message's or a mark's
  end. Whether a world needs the providers' granularity to catch a
  regression waits for one that would.
- **Digests.** The cache table compares prefixes by a digest of their
  content, not the content itself, to keep its bound small. A collision
  would show as a false hit; with a 64-bit digest and a world's few
  prefixes, it is accepted rather than paid for in memory.
