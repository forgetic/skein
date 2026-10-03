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
| machine worlds | HTTP, server-sent events, JSON, each side alone | http.md, 5; json.md, 6 |
| protocol worlds | an LLM client's HTTP, server-sent events and JSON against a server's | http.md, 5 |
| io worlds | io over the simulator, with a scripted owner | io.md, 8 |
| simulated worlds | the examples' `iterate`, each a process of the simulator | section 3 |
| real loop | the examples under the shell, on the real kernel | section 3 |
| beside them | the conformance suite, against the simulator and the ring | kernel.md, 8 |
| beside them | the simulator's own tests; the ring's own tests | simulator.md, 6; shell.md, 9 |

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

## 4. The minimal fake machine

The simulator plays the kernel, not what a program does: that is the
embedder's fake machine (simulator.md, 3). skein ships none, but its own
io and simulated worlds need one:

- a few files beneath a root;
- a program that echoes its input, one that exits with a given status,
  and one that never exits.

It lives with skein's tests, and stays that small. A service's fake
machine is the service's.

## 5. What skein supplies for a service's tiers

| Service tier | Real | What skein supplies |
|---|---|---|
| step tests | one of its step functions | lib |
| domain worlds | one domain or child domain | lib, the counting allocator |
| system worlds | several of its domains, or several services' domains | lib, the counting allocator |
| protocol worlds | its protocol layer, over skein's machines | the machines, tested in skein's tiers; the counting allocator |
| simulated worlds | every layer, `iterate` per process | the simulator, the counting allocator, the examples as a template |
| real loop | the service as it ships | the shell kit |

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
the live heap against the sum of the worst cases of the services it hosts
(simulator.md, 5).

## 6. Layout

```
crates/*/src/tests.rs           step tests; lib's in a module per area, under src/tests/
testing/skein-conformance       the conformance suite: the backend interface, the scenarios, the driver, the checks
testing/skein-heap              the counting allocator, and the meter that checks a step against its worst case
tests/heap                      the counting allocator's own tests, skein-heap-tests
tests/lib                       lib's comparisons with naive functions, run long, and its worst cases against the counting allocator, skein-lib-tests
tests/sim                       the simulator's own tests, skein-sim-tests
tests/ring                      the ring adapter's own tests, skein-ring-tests
tests/conformance/sim           the suite against the simulator, skein-conformance-sim
tests/conformance/ring          the suite against the ring, skein-conformance-ring
tests/**/tests/*.rs             a crate's focused tests
tests/**/tests/fuzzy_*.rs       its fuzzy tests: sweeps over many seeds
tests/clippy.toml               what the crates under tests/ may not use
examples/                       echo, an HTTP server and client: simulated worlds and the real loop
fuzz/                           one target per machine
```

No crate under `crates/` has a `tests/` directory: its step tests are
in `src/`, and whatever drives it from outside is a crate under `tests/`,
named by its path. Such a crate is ordinary Rust (programming-model.md,
10.2): what its test binaries share is its library, in `src/` (the
simulator's harness and its scripted exchange, a backend of the suite,
the naive functions lib is compared with),
and each file in its `tests/` is a test binary of its own, a fuzzy one
when its name starts with `fuzzy_`. `tests/clippy.toml` bans what would
make a run unrepeatable: hash maps with a random seed, the system clocks,
threads. Each backend of the conformance suite is implemented beside the
tests that run the suite against it. A crate's memory tests are binaries
of their own, as the global allocator a binary declares is the whole
binary's: `tests/memory.rs`, and `tests/fuzzy_memory.rs` for a sweep
against the worst case (testing-strategy.md, 8). The crate's other tests
run on the system allocator.

What a crate under `testing/` holds is shared by the tests of more than
one crate, and is ordinary Rust held to the step crates' lints, each with
a `clippy.toml` of its own. `skein-heap` holds the one `unsafe` in skein
beside the ring adapter: a global allocator is an `unsafe impl`.
programming-model.md (2.1, 10.2) confines `unsafe` to the ring adapter;
the counting allocator is the exception, as a test-only crate never
linked into a service. It allows its `unsafe` in place, with a scoped
`#[expect(unsafe_code, reason = "…")]` and a `SAFETY` comment on each
block.

The io worlds with the minimal machine, the machine worlds with their
transcripts, and the protocol worlds find their homes under `tests/` when
the first of each is built. `scripts/check.sh` runs what CI runs:
formatting, the lints as errors, then the focused suite and the fuzzy
suite, with nextest (section 7).

## 7. Where things stand

As of 2026-10-03.

| Tier | Built |
|---|---|
| step tests | lib: every container and value type; io: the kernel records' rules |
| machine worlds | none: no machine exists |
| protocol worlds | none |
| io worlds | none: no io yet |
| simulated worlds | none: no examples |
| real loop | not yet: no examples |
| conformance | sockets, against the simulator and the ring |
| the simulator's and the ring's own tests | sockets |
| the counting allocator | built, with its own tests; lib's worst cases checked against it |

The simulator plays the kernel for sockets, with every fault of
simulator.md, 4. Its own tests submit records by hand, and a client and a
server exchange bytes, calm and replayed in the focused suite, and under
chaos over 200 seeds in the fuzzy one. The conformance suite covers
sockets: a connection's lifecycle over IPv4 and IPv6, graceful close, a
send after the peer closed, refused connects, `AddressInUse`, IPv6-only
sockets, the wrong-state records, a full accept queue, closes with bytes
unread, a reset after the end of stream, a client closed before accept,
a listener closing on waiting connections, backpressure, and cancels of
every waiting operation and every race (kernel.md, 8). Against the
simulator, the focused suite runs each scenario over 16 calm seeds and 4
of chaos, and the fuzzy suite over 200 of chaos, counting the pairings
each race shows over them; against the ring, each runs once, in the
focused suite.

The two suites of testing-strategy.md, section 8, are
`.config/nextest.toml`'s profiles, each with its budget as a global
timeout: the focused suite by default, within 15 seconds, and the fuzzy
suite, the `fuzzy_*` binaries, with `--profile fuzzy`, within a minute.
lib's comparisons of the byte search with a naive one and of the intake
with a plain reference run 300 random cases as step tests, and 20,000
from the same seeds in the fuzzy suite.

Replay: a seed replays to the same trace of submissions and completions.
No state digest yet.

Memory: the counting allocator is temper's heap meter, ported with its
own tests. Each of lib's containers is checked against its worst case
(lib.md, 10), in the focused suite. No world of skein's checks memory
yet: its io worlds and simulated worlds are not built (section 8).

## 8. Not built yet

By tier, in the order temper pulls the parts (README.md):

- **io worlds,** with the minimal machine, when the agent's LLM client
  pulls io sockets. The simulator's files, processes and machine seam
  come with io's.
- **Conformance** for files and processes, with a scratch directory as
  the root, when io pulls them; against the readiness backend when it
  exists.
- **Machine worlds** for HTTP, server-sent events and JSON, with their
  transcripts and fuzz targets, and **protocol worlds** once two of them
  stack.
- **TLS's own tests,** when the TLS client is built.
- **Simulated worlds and the real loop,** with the examples.

By check: memory at every iteration of a simulated world, with the world
harness that runs it (simulator.md, 5); state digests for replay,
transition coverage, fuzzing.

## 9. Open questions

- **The world harness.** When a second service needs temper's schedule,
  ledger, trace and referee, whether they move into skein as a crate of
  their own, and how much of it stays ordinary Rust. Its heap meter has
  moved already: `skein-heap` (section 5).
- **Attributing heap to each service in a shared thread.** The services
  of a simulated world and the simulator run on one thread, and a
  service's submit and reap run simulator code, so the counting allocator
  sees one heap: a world checks the total against the sum of the services'
  worst cases (simulator.md, 5). Checking each service against its own
  would take attributing every allocation to the code that made it, and
  every hand-off between them.
- The real loop in CI and sanitizers on the ring adapter are the shell's
  (shell.md, 10).
