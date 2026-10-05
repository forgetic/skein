# Shared scripted LLM peer

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
chances, injected latency and output limits. `Domain::configured` consumes
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

## 4. Shared codec and replay

`skein-llm::DocumentLimits` is the neutral document-bound vocabulary. Native
Anthropic peer decode/encode entrances live in the shared codec, not an
application's adapter. Tool schemas remain whole bounded JSON values.
Signed/redacted thinking preserves unknown extension fields. Required known
fields and complete JSON shape/bounds are checked; thinking/signature deltas
replace only their own values in the retained provider head.
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

`skein-llm-world::fake::Exchange` connects the actual shared Client to the
independent byte peer and real script domain. It records requests, queries,
response bytes and actual terminals. Its transport uses bounded intakes and
checks every read/room/send; it owns no provider parser. Positive controls
carry whole schemas, literal argument bytes, exact paired feedback and
continuation through both wire configurations, then corrupt provider IDs.
Cancellation has no terminal before actual lower settlement; repeated close
settlement produces no second terminal.

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
| `server_events_roundtrip_and_errors_classify_status_and_resets` | Shared codec/common failure classification and real Client worlds own native event encoding, status/reset handling and error-body limits. Caller retry policy is not moved into the peer or Client. |
| `input_cap_discards_only_input_and_answer_cap_cuts_the_open_tool` | A local receiving/input limit yields actual shared `Failure::Limit`; the adapter must convey that structured failure, execute no partial call, and retain lower settlement. It does not fabricate provider `Stop::MaxTokens` or a truncated successful completion. Provider-reported `MaxTokens` remains a distinct terminal. This deliberately changes the old truncation assertion. |
| `measured_request_roundtrips_and_marks_only_four_tail_positions` | Measured bounded encoding, known sender/tool/schema admission and current native core decoding remain. Automatic four-tail `cache_control`/ephemeral TTL placement is deliberately unsupported by the present shared Prompt; no equivalent cache optimization is claimed. |
| Legacy request `system` block array, `thinking_budget`, `metadata`, `context_management` | The present shared Prompt has neutral instructions, configured identity and reasoning effort. It does not expose those unconsumed native request fields. The peer parses admitted historical core requests and leaves unknown deployment options uninterpreted; it does not promise to reconstruct those options from the decoded neutral Prompt. Smith must not recreate provider policy to recover the old leaf surface. |
| Fake random checkout argument menu and `delete_repository` | Caller-owned bounded `Menu` supplies complete bodies and invalid names. Smith/Temper worlds retain application scripts and effect expectations. No checkout vocabulary or tool authority remains in either shared fake. |
| Fake OAuth issuer and Smith OAuth copy | The peer receives an explicit borrowed caller credential. Sign-in, claims exchange, refresh and durable credential storage remain with the external credential owner; this fake does not prove or replace Temper's credential-owner stories. |
| Fake owned-memory assertions | `fake_memory.rs` attains script/menu/wrapper and all delayed-slot caps; composed memory controls price real protocol/client ownership and raw/envelope transit. Production bounds are checked without counting transferred outputs as retained domain state. |

Smith's actual adapter, root/provider wire histories, application decoding,
canonical concrete result rendering and consumer pin are a subsequent gated
part of this same migration. They are pending here. No copied Smith package
has been deleted and no consumer test count is inferred from this shared-kit
checkpoint.
