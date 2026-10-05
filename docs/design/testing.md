# Testing skein

Provisional, 2026-10-03. How testing-strategy.md applies to skein itself:
which tiers skein has, what they run, the example services and the
minimal fake machine they need, where the tests live, what skein supplies
for a service's own tiers, and where things stand. How each part is
tested in its own tier is in that part's document.

## 1. In one page

- **skein tests the kit.** Its tiers stop at io under small example
  services: an echo server, an HTTP server, and a client of it. A
  service's domain, protocol layer and fakes are tested in the service's
  own tiers, which stand on skein's and do not retest them.
- **Each part has its tier:** lib in step tests, each machine in machine
  worlds, the machines stacked in protocol worlds, io in io worlds, the
  loop in simulated worlds of the examples, and the ring in the real loop
  and the conformance suite.
- **The examples are the template.** skein's simulated worlds and real
  loop run them, and a service's worlds copy them.
- **skein ships no fake machine.** Its own io and simulated worlds use a
  minimal one, which stays that small.

## 2. skein's tiers

| Tier | In skein | Its design |
|---|---|---|
| step tests | lib's containers and value types; each crate's step functions | lib.md, 10 |
| machine worlds | HTTP, server-sent events, JSON, each side alone; the TLS client, against rustls's server in memory | http.md, 6; json.md, 6; tls.md, 5 |
| protocol worlds | an LLM client's HTTP, server-sent events and JSON against a server's; skein-llm's subscription client against independent wire scenarios | http.md, 6; llm.md, Verification |
| io worlds | io over the simulator, with a scripted owner | io.md, 8 |
| simulated worlds | the examples' `iterate`, each a process of the simulator | section 3 |
| real loop | the examples under the shell, on the real kernel | section 3 |
| beside them | the conformance suite, against the simulator and the ring | kernel.md, 8 |
| beside them | the simulator's own tests; the ring's own tests | simulator.md, 6; shell.md, 9 |
| durable store | `skein-kv` step tests; `tests/kv` over the crashing fake disk and the real ring | kv.md, 9 |

TLS is tested on its own (tls.md, 5), and the replaying tiers run in
plaintext.

## 3. The examples

Small services in `examples/`: an echo server, an HTTP server, and a
client of it. They exist to be tested, and to be copied.

- **In simulated worlds,** each runs as a process of the simulator, with
  every layer real. It is the first tier where the loop runs, so it tests
  what the loop adds: the two passes, `MAX_OUT` reserved at each stage,
  deadlines across layers, and requests made in the down pass reaped in a
  later iteration.
- **In the real loop,** they run under the shell on the real kernel: the
  ring, loopback sockets, a scratch directory as the root, real child
  processes, the real clock, and TLS. It shows what only the real kernel
  can: the ring adapter's `unsafe`, the probe at startup, signals read
  from a signalfd, and a process tree that ends.
- **As a template,** a service's simulated worlds start from theirs.

Their design, the steps before code of each, their fakes and the harness
their worlds run on are examples.md. The echo is built: a domain, a
protocol layer, a service and a `main`, its fake client, and its worlds
in `tests/echo`, over the world harness, `skein-world` (examples.md, 6).

## 4. The minimal fake machine

The simulator plays the kernel, not what a program does: that is the
embedder's fake machine (simulator.md, 3). skein's own io and simulated
worlds need a minimal one:

- a few files beneath a root;
- a program that echoes its input, one that exits with a given status,
  and one that never exits.

It lives with skein's tests, and stays that small. A service's fake
machine is the service's.

Its files are built: `skein-fake-machine`, in `testing/`. A world lays a
root in it from a scenario's items (files, directories, symbolic links,
each with its mode) and gives its handle to a process as the shell would
a root opened at startup; after each submit, the world takes the
simulator's calls and has the machine answer each (simulator.md, 3.1).
The machine resolves paths beneath a root as `openat2` with
`RESOLVE_BENEATH` does, follows the links that stay beneath it, keeps its
owner's permissions, and refuses what a real filesystem refuses, in the
order Linux checks; the conformance suite holds it, through the
simulator, to the real kernel in a scratch directory. It keeps a step
machine's shape, a call in and an answer out, in a vocabulary of its own
that a face translates to and from the simulator's. Its programs come
with processes.

A separate dependency-free kit, `skein-fake-checkout`, supplies generic
byte-path files, scripted commands and local git mechanics for domain
worlds (fake-checkout.md). It has no simulated-kernel adapter, inode or
permission model. Services retain their policy, remote implementations,
clock, cancellation and delivery ledgers; the kit answers synchronous
mechanical operations only. Its focused leaf tests and small replay sweep
stay beside the crate in `testing/skein-fake-checkout/tests`.

## 5. What skein supplies for a service's tiers

| Service tier | Real | What skein supplies |
|---|---|---|
| step tests | one of its step functions | lib |
| domain worlds | one domain or child domain | lib, the counting allocator |
| system worlds | several of its domains, or several services' domains | lib, the counting allocator |
| protocol worlds | its protocol layer, over skein's machines | the machines, tested in skein's tiers; the counting allocator |
| simulated worlds | every layer, `iterate` per process | the simulator, the world harness, the counting allocator, the examples as a template |
| real loop | the service as it ships | the shell kit, the world harness's real loop; for a service with a web, the browser kit (browser.md) |

The service supplies its fakes, the fake machine that plugs into the
simulator among them, and the scenarios its worlds run. A failure in a
service's world that comes from io, a machine or the simulator is
reproduced in skein's own tier, and fixed there.

**The counting allocator** is `skein-heap`, in `testing/` (section 6): a
global allocator that counts each thread's heap, and a meter that checks a
step at the most it held of its own, less what it handed out in its
requests, against its worst case (programming-model.md, 6.3). Every world
that checks memory uses it, whatever its tier (testing-strategy.md, 6), in
a test binary of its own that declares it. It measures; it runs no world.
In a simulated world, the check at every iteration is the world harness's:
each hosted service's heap, grown within its own calls, against its own
worst case (simulator.md, 5), measured with the allocator's span, the
heap's growth around one call, signed.

**Who counts a payload handed to a step.** A step's bound covers what
it keeps and what it allocates; what it hands out in its requests is the
next step's input, and the meter takes it off the step's own. An input
moved into a step is counted by whoever made it, with one exception: a
delivery from the side below is made to the reading machine's own
demand, within its largest demand, so the reader's bound covers the one
it reads, for the step that reads it. A request whose size the receiving
step's limits do not bound, such as an HTTP call's target and fields or
a piece of a request body, is counted in the sender's worst case (the
protocol layer's), and the memory test checks the step that takes it
against its bound plus that input's size.

**The world harness** is `skein-world`, in `testing/` (examples.md, 6):
one loop over the processes' `iterate`, generic over a scenario's
processes, with the referee's expectations, the trace, the heap at every
iteration, and the settled world's invariants; and the same loop over the
shell's rings, on the real clock. The examples are its first user and its
second, which is why it is skein's (testing-strategy.md, 7).

## 6. Layout

```
crates/*/src/tests.rs           step tests; lib's, io's, JSON's, HTTP's and TLS's in a module per area, under src/tests/
testing/skein-conformance       the conformance suite: the backend interface, the scenarios, the driver, the checks
testing/skein-heap              the counting allocator, the meter that checks a step against its worst case, and the span a world meters its processes with
testing/skein-scratch           a scratch directory beneath the system's temporary one, removed when dropped, for tests of files on the real kernel
testing/skein-world             the world harness: processes' iterate over the simulator or the real ring, the referee, the trace, the heap
testing/skein-echo-client       the fake echo client, a step machine
testing/skein-fake-machine      the minimal fake machine: files beneath a root, and its face behind the simulator
testing/skein-browser           the browser kit, a step machine: headless Chromium over the DevTools protocol on a pipe (browser.md)
tests/heap                      the counting allocator's own tests, skein-heap-tests
tests/echo                      the echo's simulated worlds and its real loop, skein-echo-world
tests/io                        io worlds: io over the simulator, a scripted owner, a referee, skein-io-world
tests/lib                       lib's comparisons with naive functions, run long, and its worst cases against the counting allocator, skein-lib-tests
tests/sim                       the simulator's own tests, skein-sim-tests
tests/world                     the world harness's own tests, over scripted processes of raw records, skein-world-tests
tests/ring                      the ring adapter's own tests, skein-ring-tests
tests/conformance/sim           the suite against the simulator, skein-conformance-sim
tests/conformance/ring          the suite against the ring, skein-conformance-ring
tests/json                      the JSON tokenizer's machine worlds, the writer against it, and their worst cases against the counting allocator, skein-json-world
tests/json/transcripts          its transcripts, each with what it must decode to
tests/http                      the HTTP client's and server's and the event stream reader's and writer's machine worlds, a reference reader of each, the client and the reader stacked with JSON, and their worst cases against the counting allocator, skein-http-world
tests/http/transcripts          its transcripts, responses and, in requests/, requests, each with what it must decode to
tests/protocol                  the protocol worlds: an LLM client's stack against a server's, joined by bytes cut at random, skein-protocol-world
tests/llm                       skein-llm's subscription protocol worlds, fragmentation and fault sweeps, and bounds against the counting allocator, skein-llm-world
tests/browser                   the browser kit's machine world against a fake browser, its tests against real Chromium, and its worst case against the counting allocator, skein-browser-world
tests/tls                       the TLS client's machine worlds against a rustls server in memory, the HTTP client stacked on it, and its worst case against the counting allocator, skein-tls-world
tests/tls/fixtures              its certificates and key, and the script that makes them
tests/**/tests/*.rs             a crate's focused tests
tests/**/tests/fuzzy_*.rs       its fuzzy tests: sweeps over many seeds
tests/clippy.toml               what the crates under tests/ may not use
examples/echo/*                 the echo: its domain, protocol layer, service and shell, a crate each (examples.md, 2)
fuzz/                           one target per machine
```

No crate under `crates/` has a `tests/` directory: its step tests are
in `src/`, and whatever drives it from outside is a crate under `tests/`,
named by its path. Such a crate is ordinary Rust (programming-model.md,
10.2): what its test binaries share is its library, in `src/` (the
simulator's harness and its scripted exchange, a backend of the suite,
the naive functions lib is compared with, a machine's world and the
reference parser it is checked against),
and each file in its `tests/` is a test binary of its own, a fuzzy one
when its name starts with `fuzzy_`. `tests/clippy.toml` bans what would
make a run unrepeatable: hash maps with a random seed, the system clocks,
threads. Each backend of the conformance suite is implemented beside the
tests that run the suite against it. A crate's memory tests are binaries
of their own, as the global allocator a binary declares is the whole
binary's: `tests/memory.rs`, and `tests/fuzzy_memory.rs` for a sweep
against the worst case (testing-strategy.md, 8). The crate's other tests
run on the system allocator. A simulated world checks memory at every
iteration, so each binary of `tests/echo` that runs one declares the
counting allocator; its real loop runs on the system allocator.

What a crate under `testing/` holds is shared by the tests of more than
one crate, and is ordinary Rust held to the step crates' lints, each with
a `clippy.toml` of its own. `skein-heap` holds the one `unsafe` in skein
beside the ring adapter: a global allocator is an `unsafe impl`.
programming-model.md (1, 2.1, 10.2) confines `unsafe` to the ring adapter
in code a service runs, and in tests to this `unsafe impl GlobalAlloc`: a
test-only crate, never linked into a service. It allows its `unsafe` in
place, with a scoped
`#[expect(unsafe_code, reason = "…")]` and a `SAFETY` comment on each
block. A fake that runs as a step machine, as `skein-echo-client` does,
is held to the subset instead (testing-strategy.md, 4): it has no
`clippy.toml` of its own, so the step crates' applies.

A machine's worlds are a crate of their own, `tests/<machine>`, with its
transcripts in `transcripts/` beside its tests. The minimal machine joins
the io worlds with files and processes; the protocol worlds are a crate
of their own, `tests/protocol`. `scripts/check.sh` runs what CI runs:
formatting, the lints as errors, then the focused suite and the fuzzy
suite, with nextest (section 7).

## 7. Where things stand

As of 2026-10-04.

| Tier | Built |
|---|---|
| step tests | lib: every container and value type; io: the kernel records' rules, and every cell of its listener and stream; JSON: the tokenizer and the writer; HTTP: the client and the server, the event stream reader and the writer; TLS: its limits and configuration, and the client fed by hand; the echo: its domain, every cell of its connection and listener, its `iterate`, its startup checks, and its fake client |
| machine worlds | JSON: the tokenizer, with its transcripts, and the writer against it; HTTP: the client and the server, with their transcripts of responses and of requests, and the event stream reader and the writer, the writer read back by the reader; TLS: the client against a rustls server in memory, not replayed |
| protocol worlds | an LLM client's stack (the HTTP client, the event stream reader, JSON) against a server's (the HTTP server, the event stream writer, JSON), seven scenarios |
| io worlds | sockets, over the simulator; one exchange over the ring |
| simulated worlds | the echo and its fake clients, seven scenarios |
| real loop | the echo and its fake clients, on loopback |
| conformance | sockets and files, against the simulator and the ring |
| the simulator's and the ring's own tests | sockets and files |
| the counting allocator | built, with its own tests; lib's worst cases checked against it |

The simulator plays the kernel for sockets and files, with every fault
of simulator.md, 4, files through its machine seam to the minimal fake
machine. Its own tests submit records by hand, and a client and a server
exchange bytes, calm and replayed in the focused suite, and under chaos
over 200 seeds in the fuzzy one; for files, each broken invariant of the
records and of the seam, each fault, and a replay in the focused suite,
and a workload of every operation on files under chaos over 200 seeds in
the fuzzy one, every fault of files falling. The conformance suite covers
sockets: a connection's lifecycle over IPv4 and IPv6, graceful close, a
send after the peer closed, refused connects, `AddressInUse`, IPv6-only
sockets, the wrong-state records, a full accept queue, closes with bytes
unread, a reset after the end of stream, a client closed before accept,
a listener closing on waiting connections, backpressure, and cancels of
every waiting operation and every race (kernel.md, 8). It covers files
beneath a root each scenario lays out for itself, a scratch directory on
the ring and the minimal fake machine in the simulator: a file's life at
its offsets, renames, removals, new directories, listings, a root beneath
a root, paths that escape their root and paths that stay, and
permissions, each error its operations can be made to answer on a
healthy scratch directory, and an `Open` past the descriptor limit on
the simulator. Against the simulator, the focused suite runs each
scenario over 16 calm seeds and 4 of chaos, and the fuzzy suite over 200
of chaos, counting the pairings each race shows over them, and the short
and whole counts of reads and writes; against the ring, each runs once,
in the focused suite.

The JSON tokenizer runs in a machine world between a stream below that
cuts the peer's bytes at random, ends early, idle or not, and fails, and
a user above that demands slowly, stops, and closes in every state,
checking the machine's contracts as it goes; every run is checked
against a reference parser (json.md, 6). Its transcripts, nine in the
shape of an LLM provider's and a forge's answers and thirty hostile ones,
decode to their expectations in the focused suite, with the writer
reading back what it writes. The fuzzy suite runs 20,000 generated and mutated
documents and 5,000 cut and mutated transcripts under neighbours drawn
from each seed, and writes 5,000 documents and reads them back.

The HTTP client runs in a machine world for one exchange after another
on a connection, between a server's stream that cuts its bytes at
random, grants room late, ends early and fails, and a user that uploads
within the room granted, reads with demands of every shape, withdraws,
discards, stops, and closes in every state, checking both of the
client's streams as it goes; the event stream reader runs in one of its
own (http.md, 6). Each run is held to a reference reader. Forty-seven
transcripts, in the shape of two LLM providers' streams, a forge's
answers (one captured from a real forge) and responses curl accepts,
and hostile ones, decode to their
expectations in the focused suite, and the LLM ones go up the client,
the reader and a JSON tokenizer per event, stacked. The fuzzy suite runs
20,000 connections of generated, mutated and corrupted exchanges, 12,000
event streams, and the transcripts cut and mutated, asserting that every
fault fell.

The HTTP server runs in a machine world of its own, for one request
after another, between a client's stream that cuts its bytes at random,
pipelines or waits, holds a body back for a 100 (Continue), grants room
late, ends and fails, and a service that reads, discards, withdraws, and
responds at every moment, now and then with a response the server must
refuse; each call is held to a reference reader of requests, and what
the server wrote, byte for byte, to a writer of the test's own. The event
stream writer runs in one too, and what it writes reads back, by the
reference reader and by the reader's own world, as it was written.
Thirty-six request transcripts, curl's, two LLM SDKs' JSON POSTs, and
hostile ones, come to their expectations in the focused suite. The fuzzy
suite runs 10,000 connections of generated, mutated and corrupted
requests, the request transcripts cut and mutated, and 5,000 runs of the
writer, asserting that every outcome, rejection, refusal and fault fell.

The protocol worlds (http.md, 6) build both ends of an LLM streaming
exchange as two services would, the client's stack and the server's, a
scripted user at each top, joined by a stream each way cut and joined at
random, in one loop with `skein-world`'s referee, which holds what each
top sent to what the other received, token for token, the reader's last
event ID and reconnection time to the events', and the bytes the writer
runs ahead of the reader to the caps between them, and each scenario's
goals to their deadlines. Seven scenarios run in the focused suite: an
answer streamed whole, two or three calls on one connection, a slow
reader that stops the writer, a slow consumer that stops the upload, a
response that comes mid-upload, an end closing while the other sends,
and the wire resetting at any moment; 300 runs of them under caps drawn
down to the least the stacks allow run in the fuzzy suite. They keep a
loop of their own, as `skein-world`'s drives processes over the
simulator's kernel records, which a world joined by bytes has none of.

The TLS client runs in a machine world against rustls's own server, in
memory, with a test root, an intermediate and the server's certificates
as fixtures (tls.md, 5): the server's ciphertext cut at random, room
granted late, ended or failed below; a user above that reads with demands
of every shape, slowly, writes within the room granted, finishes, and
closes in every state. Each run is held to its scenario: the plaintext
each side received, the server's ending (`close_notify`, a truncation, a
corrupted record), certificates refused at the wall time handed in, and
`close_notify` sent on a finish or a close. Focused tests handshake each
version, retry, agree ALPN, refuse a certificate for each reason the
client names and a chain longer than the records held, cut the
ciphertext a byte at a time, refuse a renegotiation sealed by hand in
front of the side above's data, and stack the HTTP client on the TLS
client; the fuzzy suite runs 400 drawn scenarios, asserting that the
outcomes it draws and each oddity of the neighbours fell. No test reaches
rustls failing for a reason of its own (tls.md, 5).

io's worlds run io over the simulator with a scripted owner above it and
a referee beside it, every process in one loop (io.md, 8). Their harness
checks `MAX_OUT` and the accept batch at every call, both halves of the
stream contract as it goes, and, once settled, every slab empty, nothing
in flight and every descriptor closed. Thirteen scenarios, with slabs of
two to four sockets and caps of a few dozen bytes, reach every admission
point, each checking its trace for the evidence: accept, bind and reject;
connects made, refused for a slot and by the peer; connects waiting on a
full backlog, cancelled; descriptors run out; two listeners under one
accept batch; a socket discarded for want of a slot; a burst of connects
past the slab and the refusals; an exchange both ways under demands of
every kind; backpressure; a refusal mid-upload that still reaches the
peer; abort; the close deadline; closes and aborts at random moments, in
every state.
The focused suite runs each over 4 calm seeds and 3 of chaos, and the
fuzzy suite over 150 of each, asserting that every fault of the
simulator fell and that a cancel of each operation io cancels was seen
to stop it, to come too late and to go unsubmitted. One exchange runs
through io over the real ring, in the focused suite.

The echo's worlds (examples.md, 7) run the echo and its fake clients as
processes of the simulator, each through its own `iterate`, over the
world harness, with a referee holding each scenario's expectations on
what the clients saw. Under tiny limits (two sessions, three connections,
five sockets, lines of sixteen bytes), nine scenarios: many clients
refused at both entrances and retried until served; a line too long; a
peer told busy at the domain's entrance and one rejected at the protocol
layer's; idle connections closed at their deadline and not before; a
client that stops reading, held by backpressure to what the buffers
between them hold, then idled out; closes, aborts and resets in every
state, with a shutdown among them; a shutdown while connections live; a
half-close with lines unanswered, every whole line answered before the
end; and the echo at its worst case. The focused suite runs each over 3
calm seeds and 3 of chaos, with a chaos seed pinned for a rare cell (a
stream failing while its line is out with the domain); the fuzzy suite
over 300 of each, asserting that every fault of the simulator fell and
that every outcome a client can see came of some connection. io holds
the echo, as every owner, to the room it was granted (io.md, 3.3). The
real loop runs the echo and two fake clients on loopback, a ring each, in
half a second. The world harness has tests of its own (`tests/world`),
over scripted processes of raw records.

The two suites of testing-strategy.md, section 8, are
`.config/nextest.toml`'s profiles, each with its budget as a global
timeout: the focused suite by default, within 15 seconds, and the fuzzy
suite, the `fuzzy_*` binaries, with `--profile fuzzy`, within a minute.
lib's comparisons of the byte search with a naive one and of the intake
with a plain reference run 300 random cases as step tests, and 20,000
from the same seeds in the fuzzy suite.

Replay: a seed replays to the same trace of submissions and completions,
and a JSON world, an HTTP world, an io world and an echo world to the
same run; a TLS world does not, as rustls draws from the kernel. No state
digest yet.

Memory: the counting allocator is temper's heap meter, ported with its
own tests. Each of lib's containers is checked against its worst case
(lib.md, 10), in the focused suite, and so are the JSON tokenizer's and
writer's, the HTTP client's and server's, and the event stream reader's
and writer's, a call of an entry point at a time (json.md, 6; http.md,
6), and the TLS client's, rustls's heap included, with the server it
talks to measured apart by a span (tls.md, 5), and io (io.md,
8): driven by hand to its limits and back, every call a step of the
meter, as an io world's simulator would allocate on the same thread. The
echo's simulated worlds check memory at every iteration: each process's
heap, what grew within its own calls, measured with the allocator's span,
at its peak within each call, against its own worst case (simulator.md,
5); and once settled, each process, dropped, must free exactly what was
metered as its own, which finds a leak, and heap made or freed outside
its calls. The scenarios leave slack: the echo peaks about a third below
its worst case, the fake clients a quarter below theirs, and a part that
held more than it should within that slack would pass the check unseen.
Driven to its limits (every connection's intake, receive and output full
at once), the echo still holds only about three fifths of its worst case:
the rest is the bookkeeping of io's and the protocol layer's B-tree
tables (deadlines, ready lists), whose worst cases count a full table's
nodes while a world arms a few timers. A focused test holds that world
to at least half its worst case, so that it keeps reaching the limits.

## 8. Not built yet

By tier, in the order temper pulls the parts (README.md):

- **io worlds for files and processes,** with the minimal machine's files
  and programs and the simulator's processes, when the worker pulls them.
  Sockets are built, and so are the simulator's files, its machine seam
  and the minimal machine's files.
- **Conformance** for processes, when io pulls them; against the
  readiness backend when it exists. Sockets and files are built.
- **Fuzz targets** for every machine, JSON's, HTTP's and TLS's included,
  when a nightly toolchain is installed; and the heap metered in the
  protocol worlds, which meets the open question of heap handed between
  stacks in one thread (section 9).
- **TLS in the real loop,** a loopback exchange through the shell against
  a local rustls server (tls.md, 8). A TLS that replays is an open
  question (notes.md).
- **The browser kit** (browser.md): its machine world first, its tests
  against real Chromium once io's processes and the HTTP example exist.
- **The HTTP examples' simulated worlds and real loop,** with skein-http.
  The echo's are built; the real loop's signals, child processes, scratch
  directory and TLS wait for the parts that pull them.

By check: state digests for replay, transition coverage, fuzzing.
Transition coverage of io's handlers needs `cargo llvm-cov`, which is not
installed.

In the echo's worlds, the referee's purity: it reads the echo's address
from `svc.listening()`, standing in for the fact `main` prints, which the
service will emit; and it calls `svc.shutdown()`, standing in for the
termination signal, which will come as io's `Shutdown` event (io.md, 7).
Both move out of the referee once those are built (examples.md, 6).

## 9. Open questions

- **The world harness.** It is skein's now, `skein-world` (section 5),
  ordinary Rust, as small as the echo needs. How much of temper's own
  harness (its schedule, its ledger of requests) joins it is settled when
  temper moves its worlds onto it.
- **Heap handed between services in one thread.** A world checks each
  service against its own worst case by metering around its own calls
  (simulator.md, 5), which holds while nothing one service allocates is
  freed by another: true over the kernel boundary, whose backend hands
  every buffer back. A world that joins two services' layers in-process,
  passing a box from one to the other, would need the hand-off counted.
- The real loop in CI and sanitizers on the ring adapter are the shell's
  (shell.md, 10).
