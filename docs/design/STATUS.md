# Status

Updated 2026-10-09. How far skein's code is from its design, and which
plans take it there. The designer keeps this file (development.md,
section 3.3).

## 1. Plans

| Plan | State | Takes the design to | Where |
|---|---|---|---|
| reliability-plan | active on debian-16gb-hel1-1 since 2026-10-09 | io.md 5–7; kernel.md 6; simulator.md 3; shell.md 6, 12–13; tls.md 3.4; json.md 3, 5; http.md 4; llm.md 2, 4; llm-connection.md 4–7; fake-llm.md 2–3; oauth.md 1, 6; examples.md 3.3, 6; testing-strategy.md 6 | `~/src/rust/plans/reliability-plan/` (not versioned) |
| smith-testing | done 2026-10-08 | simulator.md 3 (hosted spawns); llm-connection.md and oauth.md, plaintext to loopback; fake-llm.md 3 (peers on loopback); examples.md 6, 8; testing-strategy.md 2.8–2.9 | `~/src/rust/plans/smith-testing/` (not versioned) |
| smith-protocol-layer | done 2026-10-08 | codec.md; channel.md; llm-connection.md 3–6, 8 | `~/src/rust/plans/smith-protocol-layer/` (not versioned) |
| domain-completion-plan | done 2026-10-07 | fake-checkout.md 4 (commit messages); oauth.md 1–5 | `~/src/rust/plans/domain-completion-plan/` (not versioned) |
| browser-implementation | done 2026-10-06 | browser.md; io.md 6 (pipes at chosen descriptors); lib.md 7 (a held buffer read as a stream) | `docs/plans/browser-implementation.md` |
| kv-implementation | done 2026-10-05 | kv.md 1–5, 7–9; io.md 5 (files held open); simulator.md 3.1 (a disk that crashes) | `docs/plans/kv-implementation.md` |

Section 2 names an increment of the reliability plan as
`reliability <session>.<n>`, from that plan's `skein/` session files.

## 2. The design

| Document | Built | Left |
|---|---|---|
| `foundation/programming-model.md` | yes | 10.3: rules held in review only, with no checker: not planned |
| `foundation/testing-strategy.md` | partly | 6: the teardown invariant (reliability 02.4); 6: state digests, transition coverage, `cargo fuzz`: not planned |
| `lib.md` | partly | 12: `Slab::get2_mut`, the state digest: not planned |
| `kernel.md` | partly | 6.3: `Window` (reliability 02.6); 9: fixed files, 10: the ring's descriptor limit: not planned |
| `io.md` | partly | 5.3: private files, making a directory (reliability 01.7); 6.1: `Usage` (01.8, running); 7: `Resized`, `Window` (02.6); 4: DNS and datagram sockets, 9: streams that read files, 6: contained trees: not planned |
| `simulator.md` | partly | 3.3: cuts at an operation (reliability 01.11); 5: the state digest: not planned |
| `shell.md` | partly | 6.1–6.2: startup reads, trust roots (reliability 01.10); 6.3: terminal modes (02.6, after smith's session 10); 12: `next_policy_deadline` (02.4); 6: names at startup, 7: the readiness backend, 8: optimisations: not planned |
| `examples.md` | partly | 6: the echo's worlds under the teardown check (reliability 02.4); 8: the HTTP example, the echo's domain worlds: not planned |
| `testing.md` | partly | 5: the teardown check (reliability 02.4); 8: TLS in the real loop, the HTTP example's worlds, digests, coverage, fuzz targets, the referee's purity: not planned. Section 7 is as of 2026-10-04 |
| `json.md` | partly | 5.1: `Tagged` in the collector (reliability 05.tagged, running); 8–9: writing in pieces, several documents in a stream, codecs from schemas, fuzz target: not planned |
| `http.md` | partly | 8–9: chunked uploads, codings, trailers, reconnecting event streams, HTTP/2, a pool per peer, fuzz targets: not planned |
| `tls.md` | partly | 3.4: `Config::from_der` (reliability 01.9); 8: roots read at startup (01.10); 8: the server side, client certificates, the real loop, kernel TLS, fuzz target: not planned |
| `codec.md` | yes | 7: maps and sets, smaller integers, other languages: not planned |
| `channel.md` | yes | 13: compression, bodies across frames, named calls, reconnecting: not planned |
| `kv.md` | partly | 6: payload logs, 12: a shared sync, values on disk: not planned |
| `llm.md` | partly | 2.5: `TimedOut { phase }` (reliability 04.12); 4.1–4.3: `Declared`, limits by meaning, `derive` and its checks (05.7, 05.8); 4.4: selective decoding (05.6); 4.7: breakpoints (04.10, running); 6: Codex routing state, WebSocket: not planned |
| `llm-connection.md` | partly | 7: the memory pool and derived `calls` (reliability 03.5, after 05.8); 9: names while running, proxies, HTTP/2, pool fairness: not planned |
| `fake-llm.md` | partly | 2.1: the cache table (reliability 04.11); 7: providers' cache granularity: not planned |
| `oauth.md` | partly | 6: the driver, `skein-oauth-accounts` (reliability 06.3–06.6, after 01.7); 7: IPv6 loopback, device authorization, a credential helper: not planned |
| `browser.md` | yes | 11: open questions only: not planned |
| `fake-checkout.md` | yes | Nothing. A process face for real-loop worlds was proposed, not designed |

Several "Not built yet" sections still list parts that `main` has:
`io.md`, 9 (group signals, reaping at close, append streams);
`kernel.md`, 10 (all but `Window`); `shell.md`, 11 (`Host` and `drive`,
append files, io's records); `simulator.md`, 7 (groups, usage, appends,
`Stat`); `json.md`, 9 (the collector, `Text` and `Skip`, compact
documents); `http.md`, 9 (the reader's data face); `examples.md`, 8 (the
echo over `drive`, draining); `testing.md`, 8 (the harness on `Host`,
process trees in the end-to-end kit).

## 3. Not planned

- **Contained process trees** (`io.md`, section 6): a cgroup and a view
  of the file system per tree, stopped whole and proved empty, in place
  of process groups. Waits for `draft/process.md` to become a design, then
  a plan of its own; smith's containment checks wait on it.
- **Names while running** (`io.md`, section 4; `llm-connection.md`,
  section 9): a DNS client, and the datagram sockets it needs. Waits for a
  service that must resolve while it runs.
- **Names at startup in the kit** (`shell.md`, sections 6 and 11):
  services resolve their peers with the standard library. Waits for a
  decision whether the kit does it.
- **Streams that read files** (`io.md`, section 9): waits for a user.
- **The readiness backend and the deferred optimisations** (`shell.md`,
  sections 7 and 8; `kernel.md`, section 9): wait for a deployment
  without io_uring, and for measurements.
- **The ring's descriptor limit** (`kernel.md`, section 10): waits for a
  way to lower it without `unsafe` outside the ring adapter.
- **State digests for replay** (`lib.md`, section 12; `simulator.md`,
  section 5; `testing-strategy.md`, section 6): waits for a plan. smith's
  worlds take them after skein's harness.
- **Transition coverage and fuzz targets** (`testing-strategy.md`,
  section 6; the "Not built yet" sections of `json.md`, `http.md` and
  `tls.md`): wait for `cargo llvm-cov` and a nightly toolchain.
- **`Slab::get2_mut`** (`lib.md`, section 12): waits for its first user.
- **HTTP's remaining features** (`http.md`, sections 8 and 9): chunked
  uploads, content codings, trailers, reconnecting event streams and their
  `retry` block, HTTP/2 and upgrades, a request body read while
  answering, a pool per peer, `Expect: 100-continue`, the server's
  deadlines. Each waits for a peer that needs it; temper's forge and
  webhooks pull next.
- **The HTTP example** (`examples.md`, section 8; `testing.md`, section
  8): its service, worlds and real loop. Waits for a plan.
- **JSON's remaining parts** (`json.md`, sections 8 and 9): writing in
  pieces, several documents in one stream, codecs from schemas, text that
  is not UTF-8. Each waits for a user; temper asks for the first.
- **TLS's server side and client certificates** (`tls.md`, section 8;
  `channel.md`, section 13): wait for temper's engine, or smith's
  connected agent's domain contract.
- **TLS in the real loop, and kernel TLS** (`tls.md`, sections 7 and 8;
  `testing.md`, section 8): a loopback exchange through the shell. Waits
  for a plan; a TLS that replays and session resumption are open
  (`notes.md`). The drafted rustls issue waits for the user to file it.
- **Channel features** (`channel.md`, section 13): compression, bodies
  across frames, named calls, reconnecting. Named calls wait for smith's
  and temper's channels both to have them.
- **Codec extensions** (`codec.md`, section 7): maps and sets,
  variable-width integers, generators for other languages. Each waits for
  a record or a peer that needs it.
- **LLM routing and transport** (`llm.md`, sections 6 and 7): Codex's
  turn state and `reasoning.context`, Responses over WebSocket, and the
  open questions on captures, Anthropic's lookback and lifetime. Wait for
  smith's `affinity` and `cache-reuse` measurements.
- **LLM connections' later work** (`llm-connection.md`, section 9):
  proxies, HTTP/2, fairness in the memory pool. Wait for a provider that
  needs them, or a measurement under load.
- **OAuth's extras** (`oauth.md`, section 7): an IPv6 loopback listener,
  device authorization or a pasted redirect, a credential helper. Wait for
  a decision.
- **kv's payload logs and later work** (`kv.md`, sections 6 and 12):
  sketched in `kv-implementation.md`, sections 6 and 9, not scheduled.
  Payload logs wait for temper's transcripts; values on disk and a shared
  sync, for a dataset or a second store.
- **The fake LLM's cache granularity** (`fake-llm.md`, section 7): waits
  for a world that needs providers' minimum lengths and steps.
- **The echo's worlds made pure** (`examples.md`, section 8; `testing.md`,
  section 8): the referee reads `listening()` rather than the fact `main`
  prints, and the echo has no domain worlds. Waits for a plan.
- **Heap handed between services in one thread** (`testing.md`, section
  9; `http.md`, section 9): the protocol worlds unmetered. Waits for a way
  to count the hand-off.
- **Rules held by a checker** (`programming-model.md`, section 10.3;
  `notes.md`): a `syn` checker and two more lints. Waits for review to
  prove not enough.
- **io's open questions** (`io.md`, section 10; `notes.md`): a fault that
  names a stall, and operations the kernel cannot interrupt. Wait for a
  decision.
- **The browser's open questions** (`browser.md`, section 11): Chromium's
  sandbox under containment, other browsers, text by pattern. The first
  waits for contained trees.

## 4. Drafts

| Draft | About | Next |
|---|---|---|
| `draft/process.md` | Contained process trees: a cgroup and a view of the file system per tree, stopped whole, proved empty, nested | Promotion into `io.md` by a design pass, then a plan of its own |
| `draft/redb.md` | redb as an adapter over skein's file records, at the boundary of a service and its shell | A decision to adopt it, once a store outgrows memory (`kv.md`, section 12); then its section 7 checks |
