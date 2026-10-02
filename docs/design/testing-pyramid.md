# skein's testing pyramid

Provisional, 2026-10-02. How skein is tested, from one step function to
a service on the real kernel: the tiers, what is real and what is fake
in each, and the fakes that stand in for skein's neighbours. Section 7
covers the tiers a service built on skein adds on top, and what skein
supplies for them. The checks themselves are the rules of
`programming-style.md`, section 10. Section 9 says where things stand,
section 10 what is not built yet, and section 11 what is still open.

## 1. In one page

- **Each tier makes one more layer real.** The bottom tier tests one step
  function. Each tier above runs more of skein for real and fakes the
  rest, up to a service on the real kernel. A tier is added when the
  part it makes real is built, and that part's first user pulls it
  (overview.md, section 1).
- **skein has two kinds of neighbour.** Peers sit across a wire and
  speak HTTP, server-sent events, JSON and TLS to a machine. The kernel
  sits under io and answers its records (overview.md, section 6). A
  peer's fake is a stream of bytes. The kernel's fake is the simulator.
- **A machine's other side is a peer, but not the only one.** skein
  ships both sides of most machines: the HTTP client and the server, the
  server-sent events reader and writer, the JSON tokenizer and writer.
  Each side is tested against the other, and also against bytes that
  real implementations wrote, because the two sides could share a
  misreading of the protocol (section 4.1).
- **The kernel's fake is checked against the kernel.** One conformance
  suite runs against the simulator and against the ring on the real
  kernel (section 5). A simulator that drifts from the kernel tests the
  wrong thing.
- **Fakes check their client.** The simulator refuses what the kernel
  refuses and asserts what the kernel boundary promises. A peer's fake
  refuses what a real peer refuses.
- **Deterministic up to the simulator.** Every tier but the top one owns
  the clock, the seeds and everything around skein, so a seed replays to
  the same run. The top tier trades replay for the real kernel. TLS is
  the exception below the top: it is not deterministic, so the replaying
  tiers run in plaintext (overview.md, 10.4).
- **skein tests the kit; services test themselves.** skein's tiers stop
  at io under a small example service. A service's model, protocol layer
  and fake machine are tested in the service's own tiers, on skein's
  simulator and shell (section 7).

## 2. The tiers

```
            ┌───────────────┐
            │   real loop   │  examples on the real kernel, with TLS
          ┌─┴───────────────┴─┐
          │   service worlds  │  examples' iterate over the simulated kernel
        ┌─┴───────────────────┴─┐
        │       io worlds       │  io real, over the simulated kernel
      ┌─┴───────────────────────┴─┐
      │      stack worlds         │  machines stacked, two ends joined by bytes
    ┌─┴───────────────────────────┴─┐
    │        machine worlds         │  one machine, its stream and its user faked
  ┌─┴───────────────────────────────┴─┐
  │            step tests             │  one step function or one container
  └───────────────────────────────────┘

  beside the pyramid: conformance, the same records against every backend
```

| Tier | Real | Faked | Replays |
|---|---|---|---|
| step tests | one step function, or one lib container | its events, by hand | yes |
| machine worlds | one protocol machine | the stream below and the user above; the peer's bytes | yes |
| stack worlds | a connection's machines on both ends | the stream between the two ends | yes, in plaintext |
| io worlds | io | the step above io, scripted; the kernel, by the simulator; files and processes, by a minimal machine | yes |
| service worlds | an example service's `iterate`, every layer | the kernel, the network and the machine, by the simulator | yes |
| real loop | an example service under `skein-shell` | its peers, as other examples on loopback; nothing below | no |

### 2.1 Step tests

A crate's own tests feed its step functions events and inspect the
requests they emit, or drive one container through its operations: one
transition, a full queue, a stale handle, a refusal at the entrance, a
deadline that fires as it is cancelled. lib's containers are tested hard
here, since every layer of every service stands on them. Fuzzing joins
this tier with the machines: each step function fed recorded event
sequences (programming-style.md, section 10).

### 2.2 Machine worlds

A machine world runs one protocol machine from a seed, and plays both
of its neighbours:

- **The side below:** a stream that delivers exactly what was demanded,
  from peer bytes that arrive cut at random; that grants room late; that
  ends early, or fails.
- **The side above:** a user that demands slowly, stops demanding,
  requests at awkward moments, and closes in every state.

The peer's bytes come from three places: transcripts written by real
implementations (section 4.1), messages generated valid from the seed,
and those messages mutated. Most of a machine's tests live here, and so
does its fuzz target, fed `Bytes` under every demand (overview.md,
section 10). The world checks the machine's contracts as it goes: one
terminal event per request, `MAX_OUT` honoured, a reader that stops
demanding stops the reads below, and no buffer past its cap.

### 2.3 Stack worlds

A stack world builds a connection's stack on both ends, as two services
would (programming-style.md, 3.4), and joins the two bottoms with an
in-memory stream in each direction, cut and joined at random: an LLM
client's HTTP, server-sent events and JSON against a server's. It tests
what stacking adds: flow control through every machine, so a slow
reader at the top of one end stops the writer at the top of the other;
a response that arrives mid-upload; one end closing while the other is
still sending. What the top of one end sent is what the top of the other
received. It runs in plaintext; TLS is tested alone (section 4.3).

### 2.4 io worlds

An io world runs io over `skein-sim`, with a scripted owner as the step
above it: listens, connects, streams, files and spawns, in the order a
test chooses, with closes and aborts in every state. The simulator plays
the ring, the network, the clock and the seeds (overview.md, section 9),
under tiny limits, and injects cancels, short receives and sends,
completions after a cancel, resets and refused connections. Files and
processes go to a minimal machine of skein's own tests (section 4.2).
This tier tests what io adds: buffers in flight, cancellation, settling,
graceful close, the receive and send queues, and the accept budget.

### 2.5 Service worlds

A service world drives the `iterate` of the small services in
`examples/` (an echo server, an HTTP server and a client of it), each as
a process of the simulator, with every layer real. It is the first tier
where the loop of consumers.md, section 4 runs, its queues bounded and
its ready lists drained, so it tests what the loop adds: the two passes,
`MAX_OUT` reserved at each stage, deadlines taken across layers, and
requests made in the down pass reaped in a later iteration. It is also
the template a service's simulated worlds copy (section 7).

### 2.6 The real loop

The top tier runs the examples under `skein-shell` on the real kernel:
the ring, loopback sockets, a scratch directory as the root, real child
processes, the real clock, and TLS. It shows what only the real kernel
can: the ring adapter's `unsafe`, the probe at startup, signals read from
a signalfd, and a process tree that ends. It does not replay. A failure
found there is rerun as a scenario lower down, where it does.

## 3. Neighbours and their faces

### 3.1 Peers

A peer meets a machine as bytes on a stream. Each tier joins the two at
the lowest layer skein has built:

| Tier | The peer is | It meets the machine through |
|---|---|---|
| machine worlds | transcripts and generated messages | a scripted stream below the machine |
| stack worlds | the other side's stack | an in-memory stream |
| io and service worlds | another socket in the world | the simulated network |
| real loop | another example process | loopback sockets |

### 3.2 The kernel

The kernel is not a peer. It answers io's records, and its fake stays
one model, the simulator, behind the same `Submit` and `Complete` the
ring adapter speaks (overview.md, section 6):

| skein is real down to | The kernel's face | It answers |
|---|---|---|
| a machine | none | no io: streams are scripted |
| io | the simulator | records, with completions drawn from the seed |
| everything | the kernel | the ring, through `skein-shell` |

## 4. The fakes

What every fake shares:

- **No shared types.** A fake's vocabulary is its own. A transcript is
  bytes; the simulator has its own descriptors, network and clock.
- **It checks its client.** It refuses what the real neighbour refuses,
  and asserts what the real neighbour's contract guarantees.
- **Faults are configured:** latency, short transfers, failures, resets
  and refusals, drawn from the seed.

### 4.1 Peers

A machine's peer in its world is not only the other side of the same
machine. Two sides written together can share one misreading of the
protocol and agree on it, so each machine is also fed transcripts:
requests and responses captured from real clients and servers (curl, a
forge's API, an LLM provider's stream), kept as files beside the
machine's tests, with what each one must decode to. A peer that misbehaves
the way real peers do (an oversized head, a chunk size that overflows, an
event that never ends, a document nested too deep) is a transcript too.

### 4.2 The minimal machine

The simulator plays the kernel, not what a program does; that is the
embedder's fake machine (overview.md, section 9). skein ships none, but
its own io and service worlds need one: a few files beneath a root, and
a few programs (one that echoes its input, one that exits with a given
status, one that never exits). It lives with skein's tests, and stays
that small. A service's fake machine is the service's (section 7).

### 4.3 TLS's peer

TLS is not deterministic: its cryptography draws entropy from the kernel
(overview.md, 10.4). It is tested on its own, with in-memory handshakes
against itself under every split of the ciphertext, against
transcripts where they can be replayed, and in the real loop. The
replaying tiers run in plaintext.

## 5. Keeping the simulator honest

Every tier between io worlds and the real loop trusts the simulator. Two
checks keep that trust earned:

- **Conformance.** One suite of scripted operation sequences runs
  against each backend: the ring on the real kernel, with loopback
  sockets and a scratch directory; the simulator; later the readiness
  backend (overview.md, section 9). Each run checks that the backend
  answers as section 6 of the overview allows. Where the simulator draws
  among outcomes the kernel allows (a short send, a cancel that loses
  the race), the suite checks the kernel's answer is one of them. Where
  the kernel answers one way, the simulator must too.
- **The simulator checks io.** It fails the world on the kernel
  boundary's broken invariants (an operation on a closed descriptor, a
  token already in flight; overview.md, section 6), and asserts its
  promises: one completion per operation, every record handed back,
  nothing in flight at quiescence.

A behaviour found in the kernel that the simulator lacks is added to
the suite first, then to the simulator.

## 6. What the tiers check

Every world, and the simulator, checks (programming-style.md, section 10):

- **Contracts as it goes:** one terminal event per request, one reply
  per `ReplyTo`, `MAX_OUT` honoured, no buffer past its cap.
- **Invariants once it settles:** no live entities, nothing in flight,
  every record handed back, every child gone and its pipes read to the
  end.
- **Memory:** a counting allocator measures the most a part held in one
  step, against its worst case (programming-style.md, 5.4).
- **Replay:** a seed replays to the same trace, and to the same digest
  of the state.
- **Transition coverage:** function coverage of the handlers over a run.

The machine tiers add fuzzing of each machine. Conformance (section 5)
checks the simulator. The real loop adds the startup probe, the
signal path and the `unsafe` of the ring adapter under a sanitizer.

## 7. A service's tiers

A service built on skein tests its own model, protocol layer and wiring
in tiers of its own, in the shape of consumers.md, section 8:

| Service tier | Real | What skein supplies |
|---|---|---|
| step tests | one of its step functions | lib |
| model worlds | one model or sub-model | lib |
| system worlds | several of its models, or several services' models | lib |
| protocol worlds | its protocol layer, over skein's machines | the machines, tested as above |
| simulated worlds | every layer, `iterate` per process | `skein-sim`, the counting allocator, the examples as a template |
| real loop | the service as it ships | `skein-shell` |

The service supplies its fakes, the fake machine that plugs into the
simulator among them, and the scenarios its worlds run. skein's tiers
mean a service's do not retest the kit: a failure in a service's
simulated world that comes from io or a machine is reproduced in
skein's own tier, and fixed there.

The world harness a service writes (a schedule, a trace, a ledger of
requests, the heap's peak, a referee for the scenario's expectations)
belongs to the service until a second service needs the same, then
moves into skein (section 11).

## 8. Layout

```
crates/*/src/**                 step tests, in each module's tests
crates/skein-sim/               the simulator, the counting allocator, the conformance suite
crates/skein-sim/tests/         io worlds, and the simulator's own tests, with the minimal machine
crates/skein-shell/tests/       the conformance suite against the ring
crates/skein-<machine>/tests/   machine worlds and transcripts
tests/stacks/                   stack worlds
examples/                       echo, an HTTP server and client: service worlds and the real loop
fuzz/                           one target per machine
```

Each finds its home when the first of its kind is built.

## 9. Where things stand

As of 2026-10-02.

| Tier | Built |
|---|---|
| step tests | lib: every container and value type, by hand |
| machine worlds | none: no machine exists |
| stack worlds | none |
| io worlds | none: no io, no simulator |
| service worlds | none: no examples |
| real loop | none: no shell |

The checks:

- **Contracts and invariants** are asserted in lib's step tests where a
  container has them.
- **Memory, replay, coverage and fuzzing:** none yet. State types
  already derive `Hash` for the replay digest.
- **Conformance:** none, as there is no backend.
- `scripts/check.sh` runs formatting, the lints as errors, and the tests
  with nextest, as CI will.

## 10. Not built yet

By tier, in the order temper pulls the parts (overview.md, section 11):

- **io worlds and the simulator,** with the minimal machine, when the
  agent's LLM client pulls io sockets.
- **Conformance,** with the ring, at the same time.
- **Machine worlds** for HTTP, server-sent events and JSON, with their
  transcripts and fuzz targets, and **stack worlds** once two of them
  stack.
- **TLS's own tests,** when the TLS client is built.
- **Service worlds and the real loop,** with the examples and the shell.

By check: the counting allocator, replay digests over a run, transition
coverage, fuzzing.

## 11. Open questions

- **A deterministic TLS.** A test-only crypto provider that draws from
  the seed would let TLS join the replaying tiers. Whether rustls's
  unbuffered connection allows it, and whether it tests enough of the
  real provider to be worth it.
- **Where transcripts come from,** and how they are kept current when a
  peer's protocol changes.
- **The world harness:** when a second service needs temper's schedule,
  ledger, trace, heap and referee, whether they move into skein as a
  crate of their own, and how much of it stays ordinary Rust.
- **The real loop in CI:** a kernel at the floor (overview.md, 7.1), with
  io_uring allowed by the CI's container profile.
- **Sanitizers** on the ring adapter: Miri cannot run the ring, so which
  of the address and leak sanitizers the real loop runs under.
