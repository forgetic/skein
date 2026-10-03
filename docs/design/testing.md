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
| domain worlds | one domain or child domain | lib |
| system worlds | several of its domains, or several services' domains | lib |
| protocol worlds | its protocol layer, over skein's machines | the machines, tested in skein's tiers |
| simulated worlds | every layer, `iterate` per process | the simulator, the counting allocator, the examples as a template |
| real loop | the service as it ships | the shell kit |

The service supplies its fakes, the fake machine that plugs into the
simulator among them, and the scenarios its worlds run. A failure in a
service's world that comes from io, a machine or the simulator is
reproduced in skein's own tier, and fixed there.

## 6. Layout

```
crates/*/src/**                 step tests, in each module's tests
crates/skein-sim/               the simulator and the conformance suite; later the counting allocator
crates/skein-sim/tests/         the simulator's own tests and the suite against it; later io worlds and the minimal machine
crates/skein-shell/tests/       the ring's own tests, and the conformance suite against the ring
crates/skein-<machine>/tests/   machine worlds and transcripts
tests/protocol/                 protocol worlds
examples/                       echo, an HTTP server and client: simulated worlds and the real loop
fuzz/                           one target per machine
```

Each finds its home when the first of its kind is built.
`scripts/check.sh` runs what CI runs: formatting, the lints as errors,
and the tests with nextest.

## 7. Where things stand

As of 2026-10-03.

| Tier | Built |
|---|---|
| step tests | lib: every container and value type |
| machine worlds | none: no machine exists |
| protocol worlds | none |
| io worlds | none: no io yet |
| simulated worlds | none: no examples |
| real loop | not yet: no examples |
| conformance | sockets, against the simulator and the ring |
| the simulator's and the ring's own tests | sockets |

The simulator plays the kernel for sockets, with every fault of
simulator.md, 4. Its own tests submit records by hand, and a client and a
server exchange bytes, calm and under chaos over 200 seeds. The
conformance suite covers sockets: a connection's lifecycle over IPv4 and
IPv6, graceful close, a send after the peer closed, refused connects,
`AddressInUse`, IPv6-only sockets, the wrong-state records, a full accept
queue, closes with bytes unread, a reset after the end of stream, a
client closed before accept, a listener closing on waiting connections,
backpressure, and cancels of every waiting operation and every race
(kernel.md, 8).

Replay: a seed replays to the same trace of submissions and completions.
No state digest yet.

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

By check: the counting allocator, state digests for replay, transition
coverage, fuzzing.

By suite: the two suites of testing-strategy.md, section 8. skein has one
today, which runs everything, the conformance and chaos sweeps over
hundreds of seeds included. It needs `.config/nextest.toml` with a
default and a `fuzzy` profile, each with its global timeout (15 seconds
and 1 minute), and its long sweeps moved to the fuzzy suite.

## 9. Open questions

- **The world harness.** When a second service needs temper's schedule,
  ledger, trace, heap and referee, whether they move into skein as a
  crate of their own, and how much of it stays ordinary Rust.
- The real loop in CI and sanitizers on the ring adapter are the shell's
  (shell.md, 10).
