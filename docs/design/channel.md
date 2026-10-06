# Framed channels

The bounded channel mechanics shared by protocol vocabularies. `skein-channel`
keeps immutable schema, opening negotiation, exact raw-body receipts, bounded
frame queues and real named output rights. It never knows service credentials,
domain policy, timers, LLM providers or the meaning of service bodies. Concrete
wrappers validate their typed bodies and phases before resolving a receipt.
The step crate depends only on skein-lib; its worlds compose real native IO and,
before consumer adoption, the native TLS face.

This contract follows programming-model.md and testing-strategy.md. The Temper
source facts below are the frozen extraction constraints, not a dependency on
Temper or evidence of completed adoption. Implementation, independent source
review, measured allocation bounds and repository gates remain separate work.

## 1. Exact frozen source facts

| Mechanism | Actual source proof and required behavior |
| --- | --- |
| Header | codec::framing checks exactly8 bytes; kind/u16 BE, reserved/u16 zero, body length/u32 BE. Complete frame decoding requires total length equality and body reader exhaustion. |
| Open | wire::put_message writes4-byte tmpr then put_open: channel1 + lowest2 + highest2 + name length4 + name<=64 + secret length4 + secret<=64. Maximum actual body is145, not historical141; empty-name/secret body17. Bound remains256 and output reserve remains264. Actual max frame153. Decoder bounds use Sizes.worker_name_bytes/secret_bytes, not unused Limits.name_bytes substitutions. |
| Refuse/Accept | Refuse body reason2 + text length4 + text<=506 =512; full reserve520. Empty fatal refusal body6/frame14, not10. Accept body2/frame10. Refuse reason remains rawu16 through shared wire; do not normalize unknown codes or improve diagnostics while extracting. |
| Terms/status | Terms body4+32*6=196/frame204; exact receive-kind order from kinds tables and terms_version. Exclude all common kinds<=17. Unsupported body2/frame10 is common in V1 even though the old V1 kinds array omits17. |
| Incoming first record | deliver(Terms) sets Engine Hello and Agent Starting, reading=true. Thus Hello257/Start641 is automatically read without a parent Read. deliver(Hello/AgentStart) then sets Open and reading=false and emits one Message. Automatic first read does NOT automatically read the next frame. |
| Other initial reading | WorkerLink is Open and reading=true after Terms; its first received ordinary message then pauses. WorkerAgent is Open and reading=false after Terms, needing its parent's explicit Read. Outgoing WorkerLink Hello257 and WorkerAgent Start641 must be first service payload, once; link Ping may precede Hello, agent Ping is never allowed. |
| Unsupported received | wire::get_message(17) returns Message::Unsupported; deliver classifies it as ordinary Message, sets reading=false and emits it. It is not QuietAndRead and cannot be silently discarded or automatically rearm input. |
| Unknown skipped | receive permits unknown only Phase::Open and length<=sizes::largest_body(selected version); known selected-version foreign channel/direction is rejected, and known V2 wrong-direction is rejected before V1 skip. Completed skip holds one kind until exactly one10-byte Unsupported is admitted, then next header. received_frames advances only at status admission. |
| Service phase after body | Input::Held is delivered by poll. All ordinary service messages, incoming Finish included, pause reading; WorkerAgent subsequent Read still reads late stdout through actual EOF. Hello/Start duplicates can decode before deliver rejects their phase. Do not claim all source semantic phase errors are header-time errors. |
| Output admission | machine::queue encodes exact frame, checks pending_bytes+length+sent_bytes<=cap and queue room; admitted Request::Send emits Sent. Payload-version mismatch emits Unsent without close. Capacity/encoding failure emits Unsent then Closed(OutputFull). Wrong direction/phase closes Framing. Unsent has no returned owning box in original Event. Sent is queue admission, never actual write/peer/durable ACK. |
| Final output | Successful outgoing kind519 Finish marks Finished/end_read before Sent and final poll. Incoming Finish does not mark Finished. Explicit Request::Refuse queues refusal then follows the same draining-final path. |
| EOF | Agent in Phase::Open emits ReadEnded once and keeps output, even if input is a partial header/body/unknown skip. Other endpoint/phase EOF closes Fault::End. Preserve this source behavior; tightening partial EOF is a separate protocol change. |

Local configured versions must be the actual supported contiguous range (default new remains1..1). Do not require every peer-advertised offered version to be locally supported: source accepts an incoming Open offer containing unknown/outside versions if intersection exists, including a lowest0 offer intersecting1; malformed lowest>highest is Framing, disjoint range is Version. Local outgoing Open must remain a subset of locally configured range. Accept must be in both offered and local supported sets. Auto-accept on Agent selects highest intersection; Engine authorization is parent policy. RecordVERSION2 in Smith is a separate binary-record identity, not an instruction to change Temper default wire version.

## 2. Concrete plain schema and phase seam

Use concrete owned/frozen configuration, bounded before allocation, without trait/generic machine/callback/function pointer/retained service reference:

```
Role = Initiator | Responder
Direction = InitiatorToResponder | ResponderToInitiator | Both
KnownKind { kind:u16, channel:u8, direction:Direction }
KindRule { version:u16, kind:u16, body_bytes:u32 }
VersionRule { version:u16, unknown_body_bytes:u32, first:First }
First {
  receive:Option<u16>, send:Option<u16>,
  initial_read:bool, ping_before_receive:bool
}
OpeningMode = AskParent | AcceptHighest
OpeningProfile { magic:[u8;4], channel:u8,
  name_bytes:u32, secret_bytes:u32, mode:OpeningMode, ping_allowed:bool }
Limits { chunk_bytes:u32, queued_bytes:u32, queued_frames:u32,
  schema_rows:u32, known_kinds:u32, versions:u32, terms:u32, refuse_bytes:u32 }
```

These are proposed shapes, not Rust changes. Common codecs/kind bounds are fixed and cannot be overridden by service rows. Validate unique rows, supported-version completeness/order, first kinds' legal flow and actual common-kind reservation. The bounded KnownKind union must cover foreign channels and newer known kinds even when the local configured range is V1 only. A locally unknown row must not turn a known foreign-channel/direction kind into a skip. The separate unknown_body_bytes is exactly Temper largest_body over ALL kinds in the selected version, including its minimum512/common bound and foreign-channel bodies, not merely local receive rows. Preserve local advertised receive-Terms source order; peer Terms contain every local selected-version send kind at sufficient body bound, with no foreign/duplicate/common kinds. Check minimum6 bytes per Term before allocating actual Term array.

Preinstall version-specific First rows in the immutable configuration, so selecting a version installs the mechanical first gate before Ready or a new application header. No post-Ready policy race or hidden inferred mode. Map Engine(Link,Responder) to receive257/auto-read/ping-before; Agent(Agent,Responder) receive641/auto-read/no-ping; WorkerLink(Link,Initiator) send257/initial-read/no required incoming first; WorkerAgent(Agent,Initiator) send641/initial-read=false/no required incoming first. Source service phase enum remains in each concrete wrapper. Shared framing knows mechanical opening/Terms/first gates; only wrapper's exhaustive typed-message match determines Hello/Start/Finish/ACK/domain policy. Never let a metadata hook mean a service callback.

Opening emits bounded opaque name/secret values; AskParent pauses for Accept/Refuse, while AcceptHighest is frozen mechanical intersection selection. Auth/security/name lookup, timers and domain refusal decisions remain outside Skein. Ready means selected version, local Terms queued and peer Terms validated, not transmitted/security/domain Hello complete. No auto-promotion of secured service: actual TLS/client/server owner must provide the correct native boundary.

## 3. Receipt, exact schema decode and upper Read

Public requests/events remain plain enums, with explicitly documented identity/ownership and sections:

```
Request: Open{owner,opening}, Accept{version}, Refuse{refusal},
  Read, Resolve{receipt,disposition}, Send{owner,encoded}, Ping{owner},
  Finish, Close
Event: Opening{opening}, Ready{version}, Body{receipt,version,kind,bytes},
  Unsupported{kind}, Refused{reason,text}, Sent{owner}, Unsent{owner},
  ReadEnded, Closed{fault}
Disposition: InvalidBody | DecodedPause | DecodedContinue | DecodedReject
LowerRequest: Stream(Down) | Output(OutputDown)
LowerEvent: Stream(Up) | Output(OutputUp) | Closing | Closed
```

Concrete names follow section 8; these semantics remain required. Owner tokens identify admission disposition, not write settlement. Caller generates bounded unique local receipt/output identities with checked exhaustion; lower stores only current right, no history. Native library's true output right is distinct from frame owner and Body receipt.

A validated known body is exactly one raw Box moved to Body. Input AwaitDecode(receipt) emits no next read. The concrete service wrapper synchronously decodes with bounded Reader, validates tags/bools/field lengths/counts/full consumption and service phase BEFORE domain effects, drops raw body, then resolves the exact receipt. At most one decoded service record is held. DecodedPause means publish this record using the Read already permitting its delivery (or retain it until the caller's reserved upper slot is available), then stop input; it does NOT require a second Read to deliver the just-completed message. Only after that held record leaves may a new parent Read request the next record. Early/repeated Read must not erase held/AwaitDecode state or accumulate credits.

For Temper every service message, automatic first included, uses DecodedPause. Ping/common opening/Terms have their source mechanical continuation, not a service-body decoder. Received Unsupported gets a small typed event that wrapper maps to Message::Unsupported and pauses like a service record. Other future service wrappers may explicitly choose DecodedContinue, but this is a plain resolution value, not a Skein domain decision. DecodedReject counts a fully decoded frame then closes for semantic phase failure; InvalidBody does not count a malformed/trailing/tag-failed body. This preserves received_frames timing: original deliver increments before semantic checks, while body decode failure never reaches deliver. Completed unknown skip counts only when its status is admitted; Ping counts and delivers no domain message. Checked receipt exhaustion is a named new local failure control, not permission to wrap into an old receipt or silently change timer progress.

AwaitDecode/HeldByWrapper can coexist with independent output movement. A stale/mismatched receipt cannot rearm read or settle another body. Closing retires the receipt and wrapper-held value; late Resolve is inert. No second Body or hidden decoded payload inside Skein. Raw plus decoded actual arrays/boxes coexist during synchronous decode and must be charged in aggregate.

Encoded owns one exact final frame allocation, with private validated kind/version/body length/header metadata; service codec uses checked measure then write directly into it, not body-then-frame copies. Distinguish Frozen-common from selected service payload version so defaultV1/common pre-negotiation behavior remains real. Nonmutating receiving preflight checks queue slot, exact frame length and C-P-S before encoder allocation where possible; the entrance revalidates metadata against immutable schema. Invalid typed sender encoding must retain Temper's Unsent+OutputFull close mapping, not become a new panic merely because design prose says sender bug. Only internal matching native grant oversize is the already approved trusted assertion. Unsent consumes/drops the failed input frame and identifies its owner; returning its box is not a preservation requirement and would add scratch/ownership obligations. Internal Accept/Terms/Unsupported controls do not invent public Sent events; explicit Open/Send/Ping map to original Request::Send admission dispositions.

## 4. Truthful native output and separate legacy admission credit

Public room() is logical queue admission credit; a physical native grant is a distinct affine right. Define C=frozen source output_cap, P=owned queued-frame bytes, S=conservative bytes moved below since last observed whole-cap grant. After initialization keep checked P+S<=C. Before the first grant, legacy upper room is0, with only the source opening/control reserve admissible before it. Once initialized, publish room=C-P-S, pending_bytes=P, queued_bytes=P+S. This is upper queue admission credit, not permission to spend a consumed native grant.

Native output cell is Idle | Waiting{right,bytes:C} | Granted{right,bytes:C} | Cancelling/Retiring{right}. A real matching whole-cap Granted proves C bytes AND one actual lower slot available; only then reset S=0. Never reset on Bytes, stale right, Cancelled/Failed, Release, guessed write completion or peer ACK. Send one queued frame f only with matching Granted, checked f<=C; P-=f, S+=f, and consume the ENTIRE native grant, including unused bytes. Legacy admission room can remain C-P-S even when that physical grant is gone. Queuing a later frame is allowed by that logical budget, but lower movement waits for its next genuine whole-cap right.

Keep eager initial/replenishment requests where needed to initialize or clear S, even if no frame currently queued; do not forget outstanding sent debt or reserve a second grant while one is live. A retained unused grant is not runnable work by itself. Only a matching grant+front frame, absent needed read/output request, resolvable held item or status whose exact10 bytes+slot fit is ready; Waiting alone is not readiness. Poll sends at most one frame and issues at most one new read and one new output Room. Separate Q queued frames, C bytes and lower N queued BESIDE one flight; retain original output_cap/output_slots/stream_slots formulas and all field maxima until equivalence proof permits a narrower receiving dimension.

This baseline changes lower scheduling: full-cap requests must wait for previous sent bytes to drain before another full-cap grant, whereas old socket bytewise residual credit allowed crossing sends. Therefore preserve V1 upper/wire/domain oracles and map old lower-credit stories to true named rights explicitly. Required scenarios include send while read live, queued-grant debit/no double credit, wrong old right across new grant, idle replenishment debt, unknown-status pressure, output after Agent EOF, no-spin readiness and payload/frame-slot maxima. Do not assume timer equivalence: engine connection::input updates progress on Room; worker link updates stalled on Room with queued_bytes>0 and heard on received_frames. Give named actual-progress reset controls under genuine native Granted, stalled writes and crossed grant delivery, retaining all original timeout positive/negative outcomes. Front-frame-sized lower requests are deferred; they need a different proved S-refresh/admission scheme.

Fatal best-effort Refuse may move only within a currently observed matching native grant. Normal Request::Refuse must queue/drain its real frame. An old residual-byte refusal assumption cannot survive the consumed affine grant: require real-grant/no-grant/queued-grant failure-order wire controls, with frozen refusal codes/text and immediate parent Fault unchanged. Do not silently wait for new credit or remove old refusal positives under the name of native conversion. This is a named evidence obligation, not a claim the historical physical trace is preserved.

## 5. Final poll and bounded entry-point staging

Use plain step outcome NeedDecode{receipt} | NeedPoll | Halt, or equally explicit bounded event/outcome pair. Raw lower consume and down admission do not secretly poll. Resolve is state-only (except bounded fatal retirement); it cannot run another normal output poll. Wrapper synchronously completes one body/phase resolution, sets outgoing-final state only after Sent admission, then performs the one allowed final poll. No callback or recursive/unbounded pump is needed.

| Original path | Wrapper final-poll rule |
| --- | --- |
| Valid Bytes/header/partial body/skip; valid Room; valid zero body | Exactly one after synchronous decode/resolve if completed. |
| Successful Request Read/Accept/Refuse/Send | Exactly one; outgoing final519 or Refuse marks finishing/end_read before it. |
| Invalid body/header; stream Failed; illegal request/phase; queue failure | Stop/retire, no normal final poll. Preserve Unsent before OutputFull Closed. |
| Unsupported payload-version Send | Unsent only, immediate return, no poll/close. |
| Agent Open-phase End | ReadEnded/end_read, immediate return, no same-call output poll. Output may advance on later poll. |
| Other End | Fault::End stop, no normal poll. |
| Finished/read-ended late Bytes | Drop crossed answer then exactly one normal final poll; cannot consume a new output right. |
| Finished/read-ended End | Retire only actual read, then exactly one normal final poll. |
| Closing Bytes/Room/End; Closed input | Inert, no legacy normal poll; new native retirement events still drain mechanical right. |
| Request Close | Stop/retire, no normal poll. |

Candidate shared/composed ceilings are above2/below3 per outward step, subject to exact frozen implementation proof. A normal poll is at most one Send+one read-only Demand+one Room=3 lower records. Final admission path is read withdrawal+last framed Send+lower Finish=3; it must not issue replacement Room. Closing with an observed unused grant can withdraw read+Send bounded best-effort refusal (or Release)+Finish=3. Closing Waiting emits withdrawal+Cancel<=2 and defers Finish until actual terminal; late Granted retires via Release/Send then Finish<=2. No branch can stack a fatal stop and an extra normal poll.

Public Temper MAX_UP2 remains: peer Refused message then Closed, or Unsent then Closed, or a published valid body then a newly encountered local fatal condition. Existing MAX_DOWN4 may remain the legacy conservative reservation, while the new shared lower ceiling3 still requires source proof; do not equate scalar MAX_UP with one generic queue count or omit internal Body scratch. Wrapper has fixed typed scratch for at most2 raw events and3 lower requests per shared entry plus one Body/one decoded record; publishing Body internally then a concrete service Message does not make two domain messages. Reserve actual event/record layouts and all real helper calls in the aggregate. If frozen code genuinely co-emits another action, derive/report its maximum before changing declared bounds; never suppress true events or truncate queues.

## 6. Closing and lifetime retirement

Preserve Temper parent-visible Closed{fault} as immediate logical framing notification on stop, distinct from physical IO/TLS resource Closed. The new native right still has to drain: public stopped state coexists with one bounded mechanical retirement cell. Late genuine terminal is accepted after logical Closed; Granted is Released, Cancelled/Failed retires the exact pending cell; no new body/frame/public Closed is generated. Expose is_retired()/equivalent owner bookkeeping so a connection can notify its domain immediately but remain in its bounded retiring slot until that right is observed/retired and actual lower resource lifecycle settles. A stop must not drop a necessary lower identity or let anonymous Closed impersonate its terminal.

Normal final queue drain stops input/withdraws read under closing rule, consumes/retires final output right, emits lower Finish once, then awaits actual enclosing lifetime settlement. Native TLS Finish requires Ready and read-no-more; do not route a pre-Ready framing fatal close into forbidden TLS Finish. Start framing only on the actual usable plaintext native face, or use native Client::Close on that pre-Ready owner path. No secured-server/portability claim is implied. Actual TLS TransportClosing/TransportClosed or IO lifecycle is explicitly bridged, not converted to invented Up::Failed.

For worker process pipes, stdout read and stdin write are distinct owned IO entities. Read End/actual read-pipe Closed must not accidentally become whole duplex Closed while Agent output is still usable; parent bridge associates the correct read/write/resource lifetimes. An unsolicited matching lower output Cancelled means output lifecycle loss, not a new reusable grant; notify logical closure, prevent new sends/reads and let owner perform actual close. Keep TLS's approved real lifecycle/winner rules unchanged. Actual input EOF and process reap remain separate; WorkerAgent incoming Finish still drains late stdout until EOF and domain last-word policy.

## 7. Allocation, schema, scratch and aggregate bound

Check representability/configuration before constructor allocations: supported versions/schema/known-kind tables, every send frame header+body and common opening reserve, positive legal chunk, exact term count/cap and Q count; each version's maximum frame must fit C. Include every configured supported version, not only range endpoints, when deriving generic maxima. Unknown skip allocates counters only, one status, no peer-length body or status chain. Receive header/body/skip is one exact Fill (8 or min(remaining,chunk)); bound its delivery by max(8,chunk). Header reserved/phase/direction/selected-version length checks happen before raw body allocation, except true source wrapper semantic checks after decoding as noted above.

A shared own allocation bound comprises actual owning schema boxes/container records; actual Queue<frame record>::worst_case(Q); queued frame bytes C; one largest receive/common raw body B; one delivery max(8,chunk); exact bounded common decoded Opening/Refuse/Term heap and raw/internal event scratch. Price owning Schema/Opening boxes and their payloads. Inline Machine/wrapper cells belong to the actual host/slab/stack ownership convention; no duplicate size_of charge. Control frame staging fits C only with proved byte+slot preflight before allocation; otherwise charge its simultaneous temporary term explicitly. Encoded rejected input/typed producer boxes and raw Body moved to wrapper remain caller receiving ownership, not vanished allocation.

Service aggregate additionally prices maximum concrete decoded array layouts+their field boxes, raw B during decode, one retained decoded cell, exact encoder staging/header+body output, simultaneous retained/caller typed input and IDs/results, public queues and fixed dispatch scratch. Incoming raw plus decoded plus outgoing exact frame can coexist: use checked sums for simultaneous allocations, not a max just because only one was retained between steps. Preserve original bounds' stronger global all-kind body/decoded maxima and output formulas where still externally exposed until a reviewed equivalence change. Actual lower IO/TLS Intake, ciphertext records/full owed, complete temporary encryption output and output flight/slots use their reviewed own/composed helpers once each. Approved TLS max-work formula requires drop sequencing and meter proof; it is not a passed TLS gate. No arbitrary allocator headroom, hidden per-step queue, service reference/callback, trait/generic machine, lint waiver or Result shadow.

## 8. Concrete choices and preservation obligations

### Ownership and package

Use `skein-channel` for generic framing, opening negotiation, terms, native
output rights, raw-body receipts and the checked encoding primitives needed by
both consumers. Its step crate uses Skein lib, with no dependency on Temper,
Smith or IO. The worlds use actual native IO and, once verified, native TLS.
`temper-channel` retains its closed vocabulary and frozen V1 APIs/bytes;
`smith-channel` owns its new agent vocabulary. Credentials' meaning, schemas'
service semantics, domain transitions, timers, transcripts and system rendering
remain in their concrete consumers. No callback, trait, generic machine,
function pointer, retained service reference or duplicated protocol engine.

The plain owned configuration is the reviewed Role/Direction/KnownKind/
KindRule/VersionRule/First/OpeningMode/OpeningProfile/Limits vocabulary. Freeze
it at construction. `KnownKind` includes foreign-channel and newer-version
known kinds, not only selected local send/receive rows. Per-version unknown-body
cap retains the source maximum over all kinds, including common minimum 512.
Rows are unique and complete for every locally supported contiguous version;
common kinds are reserved and cannot be overridden. Validate source order of
advertised receive terms, exact kind/direction/channel, first gates, counts,
checked storage and every frame's fit in the output cap before allocation.

### Opening, read and body delivery

Header is exactly eight big-endian bytes: kind/u16, reserved/u16 zero,
body-length/u32. A complete body must be exactly consumed. Open's actual maximum
body is 145, empty body 17; retain its configured 256 body bound and 264 reserve.
Accept frame is 10, maximum Refuse frame 520, empty fatal Refuse frame 14,
maximum Terms frame 204, Unsupported frame 10. Refuse reason is opaque u16;
preserve code/text bytes. Name/secret bounds come from the actual service sizes.

Local outgoing Open is a subset of supported versions. A peer offer can contain
unsupported versions, including lowest zero, if its ordered range intersects
the local supported range. Disjoint is Version; reversed range is Framing.
Accept is within both offers. Responder policy is immutable AskParent or
AcceptHighest; service authentication and deadlines are outside the machine.
Ready follows local Terms queue admission plus peer Terms validation, before
any inference of transmission, authenticated domain Hello or peer receipt.

Preinstalled First records preserve automatic first Hello/Start reads followed
by pause after delivery. WorkerLink starts reading after Terms; WorkerAgent
requires explicit Read. Received Unsupported is a parent-delivered message that
pauses input. It is not silently discarded. Ping/opening/terms retain their
specific mechanical continuation. Unknown skip allocates no peer-sized body,
holds one kind until one status is admitted, and then starts the next header.

One valid known raw body moves to `Body { receipt, version, kind, bytes }`.
AwaitDecode cannot read another body. The wrapper decodes, validates service
phase and exact bounded shape, then resolves the exact receipt. It owns at most
one decoded value. DecodedPause publishes using the Read that already permitted
this body, then stops input; it does not require another Read for that delivery.
A new Read cannot erase a held record or accumulate extra credit. InvalidBody
does not count malformed input; DecodedReject counts a fully decoded frame
before semantic closure. Closing retires receipt/held ownership; late Resolve
is inert. Checked body-receipt or native-output-token exhaustion produces
shared Fault::Limits before the attempted right's allocation/effects, without
wrapping/reuse, panic or an invented lower stream fault. Logical stop retains
and retires any previously admitted real lower right under the lifetime rules.

### Concrete entrances and one final poll

Select the reviewed public vocabulary, with these exact additions/choices:

- `Step::{NeedDecode { receipt }, NeedPoll, Halt}` is a plain bounded result of
  `up`/`down`; those functions do not secretly execute the ordinary final poll.
- `resolve` consumes the matching receipt and updates state, without an ordinary
  poll. Fatal resolution may perform its bounded stop/retirement only.
- `poll` runs once where the reviewed final-poll table permits it. The wrapper
  completes synchronous decode/phase resolution first; no recursive pump.
- `Request::Send { owner, encoded: Option<Encoded> }`: preserve the original
  ordered sender checks. Finished/Closing/Closed returns Unsent only, without
  encoding or polling, including shared Send(None). Next the service wrapper
  checks payload-version mismatch (Unsent only), then legal direction and
  service phase (Framing closure) before attempting encoding. Only after those
  checks may None represent failed typed encoding: Unsent then OutputFull Closed,
  with no ordinary poll. None has no kind/version metadata and cannot bypass
  those required wrapper checks. The shared entrance revalidates valid Encoded
  metadata against the immutable schema before queue admission.
- `Encoded` is one sealed exact final frame allocation with private validated
  kind/version/body-length metadata. The codec uses a shared checked measure
  pass then writes header and body directly into this allocation. No extra
  body-then-frame copy. Known sender encoding failure is data, not a new panic.
- Frame owner identifies Sent/Unsent queue-admission disposition. Unsent drops
  the rejected owning frame; it returns no box. Internal controls create no
  public Sent. Explicit Open/Send/Ping retain their original disposition meaning.

Other reviewed requests/events/lower routes and Disposition values retain the
reviewed shapes. Header/body/version/direction and service-phase checks precede
domain effects at the appropriate original stage. Shared code never guesses a
service final record; the wrapper marks finishing only after its admission.
Received WorkerAgent Finish pauses that delivery but still permits later stdout
reads through actual EOF. Agent Open-phase End emits ReadEnded and returns
without same-call poll, including the original partial-input cases.

Proposed shared ceilings: above two and below three for each outward entry,
including the composed decode/resolve/single-poll path. Poll is at most one Send,
one read Demand and one Room. Fatal retirement cannot stack another ordinary
poll. Final last-Send/withdrawal/Finish fits three and requests no replacement
Room. Keep Temper's conservative existing MAX_DOWN four and MAX_UP two until
consumer equivalence proof. These are source targets: derive/report any genuine
larger co-emission; never suppress an event or truncate a queue to fit them.
The actual raw-event/decoded-value/dispatch scratch is separately priced.

### Admission credit and physical output

Separate C (frozen output cap), P (owned queued frame bytes), and S (bytes moved
below since the last real whole-cap grant). Before initialization upper room is
zero, with the original opening/control reserve still usable. Afterwards
P+S<=C, room=C-P-S, pending_bytes=P, queued_bytes=P+S, using checked arithmetic.
These are upper queue admission figures, not the remaining physical grant.

A matching native Room asks for C bytes and one real Send slot. Only a genuine
whole-cap Granted resets S to zero. Send one queued frame, debit P, add S and
consume the entire physical grant. Never spend its unused remainder twice.
Bytes, Release, fake write completion and peer/durable ACK cannot reset S.
Initial/idle replenishment remains eager where needed; a retained unused grant
or a pending request is not runnable work by itself. Queue-slot pressure and
byte pressure are independent. Preserve actual lower N queued beside one
in-flight/stalled Send. Front-frame-sized grants need a different reviewed
refresh proof and are outside this proposed implementation.

Fatal best-effort Refuse can use only an actually retained matching physical
grant. It does not wait for new credit. Normal Refuse queues/drains in sequence.
Preserve immediate parent fault/code/text and give explicit real-grant/no-grant/
queued-grant refusal controls. Native conversion changes lower scheduling;
V1 byte/domain preservation is not proof of identical timer traces. Test real
progress resets, stalled writing, crossed grant delivery and idle debt, retaining
every old timeout positive/negative oracle.

### Lifetime and memory

Parent-visible framing Closed remains immediate logical notification, distinct
from physical IO/TLS Closed. Keep one bounded retiring output identity after it;
accept actual late winners and retire them without another public Closed/body.
Expose `is_retired` or equivalent owner bookkeeping. Anonymous resource closure
cannot impersonate a missing named terminal. Normal final drain retires output,
withdraws read under closing rules, emits Finish once, then follows actual
enclosing lifetime. Pre-Ready TLS fatal closure uses native Close, never an
illegal native Finish. Bridge real native transport Closing/Closed explicitly.
Distinct process stdin/stdout entities cannot collapse read EOF into duplex
resource closure; process reap and transcript commitment are independent rights.

Include actual schema/known-kind/version owning records, frame Queue nodes and
C frame bytes, largest raw body, max(8,chunk) delivery, common decoded values,
one receipt/retirement cell, typed scratch and exact service decoder arrays.
Raw+decoded+outgoing encoded allocations can coexist: checked sum, not max.
Constructor/preflight eliminates temporary ownership only where source proves
it. Keep original global all-kind body/decoded maxima and output formulas at
the service boundary until reviewed equivalence permits changes. Actual
IO/TLS buffers, queued/flight boxes and native wrapper fixed layouts are charged
once under their reviewed helpers. No guessed headroom or early deallocation.

### Required acceptance evidence

Retain all fixtures byte-identically, original drift/regeneration commands,
focused/version/maximum-heap worlds, machine seeds0..128 plus4096 raw inputs,
version seeds0..64 plus4096 bounded inputs, and all protocol/system oracles.
Add independent literals, exact truncation/tag/count/trailing/one-over cases,
true read/output independence with actual native IO, and verified native TLS
composition. Preserve automatic first-read/pause, Unsupported delivery,
foreign/newer direction, unknown-flood/status credit, max name/secret/Start
arrays, no-spin, Agent EOF and WorkerAgent late stdout after Finish positives.

Full gate discipline is unchanged: shared source first, then consumer adoption
through each repository's exact gates and independent review. Kernel-dependent
failures remain failures. Draft preparation on isolated branches is not main
acceptance. No source copying into Smith, provider cleanup or pin advance is
authorized by this contract alone.
