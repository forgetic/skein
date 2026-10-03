# Testing strategy

Provisional, 2026-10-03. How code in skein and in every service built on
it is tested. Read it with programming-model.md, whose terms it uses,
before writing or reviewing tests.

**How to read this.** Section 1 is the whole strategy in one page. The
sections after it take each part in turn: the tiers, faults, the fakes
that stand in for neighbours, keeping the simulator honest, what every
world checks, scenarios and the referee, the two suites, and where a
failure is fixed.

## 1. In one page

- **Each tier makes one more layer real.** The bottom tier tests one step
  function; each tier above runs more of the code for real and fakes the
  rest, up to everything on the real kernel. A tier is added when the
  layer it makes real is built.
- **Behaviour above, mechanics below.** Domain worlds run the domain with
  no protocol and no io, and test what the service does; most of a
  service's tests live there. The tiers from machine worlds up test how
  bytes, buffers, cancellations and the loop behave under faults.
- **One thread, one loop, in every world.** A world follows the mechanics
  of programming-model.md: its one loop drives every step machine in it
  (the code under test, its fakes, the referee), so between two
  iterations the whole world is a frozen snapshot that checks can inspect
  and reason about. The tiers differ in what sits under the loop.
- **Deterministic up to the real loop.** Every tier but the top one owns
  the clock, the seeds and the kernel, so a seed replays to the same run.
  The real loop trades replay for the real kernel. TLS is not
  deterministic, so the replaying tiers run in plaintext.
- **Faults everywhere, limits tiny.** Worlds run with capacities small
  enough to reach every admission point, and inject faults in every state,
  drawn from the seed.
- **Fakes are step machines too.** A fake shares no types with what it
  stands in for, checks its client as it goes, and is the same at every
  tier; only the face it shows changes.
- **Two kinds of neighbour.** Peers sit across a wire and meet the code as
  bytes. The kernel sits under io; its fake is skein's simulator, which a
  conformance suite holds to the real kernel.
- **Every world checks the same things:** contracts as it goes,
  invariants once it settles, memory against the worst case, replay, and
  transition coverage. A scenario's own expectations go in a referee.
- **Focused tests for quick feedback, both suites at the gate.** Focused
  tests are fast, and serve as quick feedback during development as the
  agent sees fit. The fuzzy suite is slow, so it is usually left alone
  during development, or run in part when that helps. The merge gate runs
  both suites in full.
- **A failure is fixed in the lowest tier that shows it.** skein tests the
  kit and a service tests itself: a failure in a service's world that
  comes from skein is reproduced and fixed in skein's own tier.

## 2. The tiers

Every tier above step tests is a world: one thread, whose one loop drives
the code under test, its fakes and the referee, iteration by iteration,
and runs its checks between iterations. What sits under the loop is what
changes from tier to tier: nothing, byte streams between two ends, the
simulator, or the real ring.

| Tier | Real | Faked | Whose | Replays |
|---|---|---|---|---|
| step tests | one step function, or one lib container | its events, by hand | both | yes |
| domain worlds | one domain with its child domains, or one child domain | its parent and neighbours: scripted, or fakes | a service's | yes |
| system worlds | several domains together | the peers and the machine, at their domain faces | a service's | yes |
| machine worlds | one protocol machine | the stream below, the user above, the peer's bytes | both | yes |
| protocol worlds | both ends of a connection: machine stacks, and the protocol layers and domains above them where they exist | the bytes between the two ends | both | yes, in plaintext |
| io worlds | io | the step above it, scripted; the kernel, by the simulator | skein's | yes |
| simulated worlds | every layer: each process's `iterate` | the kernel, the network and the machine, by the simulator | both | yes, in plaintext |
| real loop | the service as it ships and its fakes, in one loop over the real ring | the peers, as fakes on loopback sockets; nothing below | both | no |

### 2.1 Step tests

A crate's own tests feed a step function events and inspect the requests
it emits, or drive one container through its operations: one transition,
a full queue, a stale handle, a refusal at the entrance, a deadline that
fires as it is cancelled. lib's containers are tested hardest, since
every layer of every service stands on them.

### 2.2 Domain worlds

A domain world runs one domain in a loop, from a seed: a child domain
with the world as its parent, or a root domain with its children beneath
it. Each child domain has a world of its own, and so does the root. The
world plays everything else. It scripts the neighbours that are the
service's own, taking liberties the real ones do not, and uses fakes for
the peers and the machine, translating between their vocabularies and
the domain's as a protocol layer would.

Behaviour is tested here. Domain worlds come first: they can run as soon
as the domain exists, long before its protocol layer and io.

### 2.3 System worlds

A system world is a domain world where several domains meet, each real:
the components of one service, or several services' domains. It aims at
what crosses them, which no single domain's world can see. It is still
the domain layer only: where a protocol layer will sit, the world
translates.

### 2.4 Machine worlds

A machine world runs one protocol machine from a seed, and plays both of
its neighbours:

- **The side below:** a stream that delivers exactly what was demanded,
  from peer bytes cut at random; that grants room late; that ends early,
  or fails.
- **The side above:** a user that demands slowly, stops demanding,
  requests at awkward moments, and closes in every state.

The peer's bytes are transcripts written by real implementations (4.1),
messages generated valid from the seed, and those messages mutated. Most
of a machine's tests live here, and so does its fuzz target.

### 2.5 Protocol worlds

A protocol world builds both ends of a connection, as two services would:
their machine stacks and, where they exist, the protocol layers and
domains above them. It joins the two bottoms with an in-memory stream in
each direction, cut and joined at random. It tests what stacking and
translation add:

- framing and codecs, such as the JSON a peer writes inside a message;
- flow control through every machine, so a slow reader at the top of one
  end stops the writer at the top of the other;
- a response that arrives mid-upload, and one end closing while the
  other is still sending.

What the top of one end sent is what the top of the other received.

### 2.6 io worlds

An io world runs io over the simulator, with a scripted owner as the step
above it: listens, connects, streams, files and spawns, in an order the
test chooses, with closes and aborts in every state. It tests what io
adds: buffers in flight, cancellation, settling, graceful close, the
receive and send queues, and the accept budget. These worlds are skein's.
A service does not retest io.

### 2.7 Simulated worlds

A simulated world runs each process's `iterate`, the function its shell
runs, and the simulator plays the kernel under them: the ring, the
network between the processes' sockets, the clock, the seeds, and files
and programs through a fake machine (4.3). Processes come and go: a spawn
may start another service in the same world.

It is the first tier where the loop runs, so it tests what the loop adds:
the two passes, `MAX_OUT` reserved at every stage, deadlines across
layers, and requests made in the down pass reaped in a later iteration.
skein's simulated worlds run small example services. A service's run the
service, with skein's as their template.

### 2.8 The real loop

A real-loop test is still one thread and one loop, but its loop makes real
io_uring calls, through the shell's kernel, and drives everything through
them: the service as it ships, any service it would spawn, and its fakes,
which follow the same programming model and meet it on loopback sockets.
Only programs that are not step machines (git, a shell) run outside the
loop, as real child processes in a scratch directory. Between iterations
the test sees a frozen snapshot, as in every other world, so its checks
and its referee work as they do lower down, with deadlines on the real
clock.

It shows what only the real kernel can: the ring adapter's `unsafe`,
under a sanitizer; the probe at startup; signals; TLS; a process tree
that ends; real programs' output. It does not replay. A failure found
there is rerun lower down as a scenario, where it does.

## 3. Faults

Every world below the real loop draws its faults from its seed:

- **Tiny limits.** Every capacity is small enough that a run reaches every
  admission point: a slab of capacity 2.
- **Cancellation and timeout in every state,** and completions that
  arrive after their cancel.
- **Refusal at every admission point.**
- **Short reads and writes,** bytes cut and joined at random, latency.
- **Resets, refused connections, failures** of what a neighbour does.

A fault that is configured but never falls tests nothing. A sweep over
seeds asserts that each fault it injects fell at least once, and that
each outcome a race allows (a cancel that wins, one that loses) appeared.

## 4. Neighbours and their fakes

What every fake shares:

- **A step machine,** following programming-model.md, when more than one
  tier needs it, so that the simulator and the real loop can host it as
  they host the service. What only one world needs stays in that world,
  as a script specialised to what the world aims at.
- **No shared types.** A fake's vocabulary is its own. A world, or a
  protocol layer, translates.
- **It checks its client.** It refuses what the real neighbour refuses,
  and asserts what the real neighbour's contract guarantees.
- **Faults are configured,** and drawn from the seed (section 3).
- **One fake, every tier.** A fake's state machines are the same at every
  tier, and only the face it shows changes. A scenario written for the
  fakes then runs unchanged wherever they appear (section 7).
- **Some fakes are temporary.** A fake of the service's own component
  stands in until the real one exists, then retires.

Tests are ordinary Rust: test code may `unwrap`, `expect`, `panic!` and
index, which the lints allow in tests. A fake that runs as a step machine
may not.

### 4.1 Peers

A peer meets the code as bytes on a stream. Its fake grows the layers
the code facing it grows, and each tier joins the two at the lowest layer
both have: in domain worlds, the world translates between the two
domains; in protocol worlds, bytes pass between the two stacks; in
simulated worlds, the simulated network joins them; in the real loop,
sockets do.

A machine's peer is not only the other side of the same machine. Two
sides written together can share one misreading of the protocol and
agree on it, so each machine is also fed **transcripts**: requests and
responses captured from real clients and servers, kept as files beside
the machine's tests, each with what it must decode to. A peer that
misbehaves the way real peers do (an oversized head, a chunk size that
overflows, an event that never ends, a document nested too deep) is a
transcript too.

### 4.2 The kernel

The kernel is not a peer. It answers io's records, and its fake is one:
skein's simulator, behind the same submit and reap as the shell's
kernel. It decides when and how each operation completes, races every
cancel, plays the network between the world's sockets, and owns the
clock, jumping it to the next completion or deadline when every process
is idle. Every choice is drawn from the seed.

It checks its client. It fails the world on each broken invariant of the
kernel boundary (an operation on a closed descriptor, a token already in
flight), and asserts its promises: one completion per operation, every
record handed back, nothing in flight at quiescence.

### 4.3 The machine

The machine is the files and programs under io. There is no wire to meet
it at, so its fake grows no protocol or io layers: it stays one set of
state machines, with a face for whichever layer sits just above it:

| Real down to | The machine's face | It answers |
|---|---|---|
| the domain | domain face | typed operations: files, commands, searches |
| the protocol layer | io face | io's records: open, read, write, spawn, bytes on a pipe, an exit |
| io | behind the simulator | the file and process operations the simulator passes on |
| everything | none | the real kernel, in a sandbox |

skein ships no fake machine. Its own tests use a minimal one: a few files
beneath a root, a program that echoes its input, one that exits with a
given status, and one that never exits. A service's fake machine is the
service's.

### 4.4 TLS

TLS is not deterministic: its cryptography draws entropy from the kernel.
It is tested on its own, with in-memory handshakes against itself under
every split of the ciphertext, against transcripts where they can be
replayed, and in the real loop. The replaying tiers run in plaintext.

## 5. Keeping the simulator honest

Every tier between io worlds and the real loop trusts the simulator. A
simulator that drifts from the kernel tests the wrong thing, so:

- **One conformance suite runs against every backend:** the ring on the
  real kernel (loopback sockets, a scratch directory), the simulator, and
  any backend added later. Each run checks that the backend answers as
  the kernel boundary allows, and its checks name the rule behind each
  assertion.
- **Where the simulator draws among outcomes the kernel allows** (a short
  send, a cancel that loses its race), the suite checks that the kernel's
  answer is one of them, and that the simulator shows every one of them
  over its seeds. **Where the kernel answers one way,** the simulator
  must too.
- **A behaviour found in the kernel that the simulator lacks** goes into
  the suite first, then into the simulator.

## 6. What every world checks

The harness checks, in every world and in the simulator:

- **Contracts as it goes:** one terminal event per request, one reply per
  `ReplyTo`, `MAX_OUT` honoured, no buffer past its cap, and each
  component's own contracts.
- **Invariants once it settles:** no live entities (every slab empty),
  nothing in flight, every record handed back, every child gone and its
  pipes read to the end, every answer taken once; and ownership is a
  tree, with no orphans.
- **Memory:** a counting allocator measures the most each part held,
  against its worst case (programming-model.md, 6.3). The simulator
  checks it at every iteration.
- **Replay:** a seed replays to the same trace, and to the same digest of
  the state, which state types derive `Hash` for.
- **Transition coverage:** each cell of a state machine is a handler
  function (programming-model.md, 5.4), so function coverage of the
  handlers over a run (`cargo llvm-cov`) lists the transitions exercised
  and those never reached.

On top of these, the fakes check their clients (section 4) and the
referee checks the scenario's expectations (section 7). Each machine, and
each of a service's decoders, is fuzzed alone with `cargo fuzz`, fed
`Bytes` under every demand; step functions are fuzzed with recorded event
sequences.

## 7. Scenarios and the referee

A **scenario** is what a test sets up, in the fakes' own terms, with a
seed: what each peer will say, the data it holds, and the faults to
inject. Since a fake is the same at every tier, a scenario runs at every
tier its fakes reach, and a failure found high up reruns lower down,
where it replays.

A scenario's expectations are a step machine of their own, the
**referee**, which runs in the loop beside everything else, at every
tier:

- **It has the shape of every step:** its own state, events in, requests
  out, and a deadline table of its own. In the replaying tiers its
  deadlines are simulated time, so "within an hour" takes milliseconds.
- **It watches from outside:** what the fakes saw and the facts the
  service emits, never a service's state. What it sees is then the same at
  every tier, and its expectations are written once.
- **Two kinds of expectation.** Safety, what must always or never happen,
  is checked on every observation and fails at once. Liveness, what must
  happen by a deadline, is a deadline armed; one that fires fails the
  test, listing what is still pending.
- **It ends the test:** passed, once every expectation is met and nothing
  is in flight; or failed, with when, why, what was pending, and the
  seed.
- **It injects what belongs to no fake:** the network dropping, a
  component restarting, a shutdown.

Boundary contracts and the invariants once things settle are not the
referee's: each tier sees them differently, so each tier's harness keeps
them (section 6). What a scenario expects of the system as a whole goes
in the referee, and travels with the scenario.

A world harness (schedule, trace, ledger of requests, heap, referee)
belongs to the service that built it until a second service needs the
same. Then it moves into skein.

## 8. Two suites

- **Focused tests** check what the code is expected to do: the step
  tests, and each world's scenarios, referee tests, replay, and memory at
  the worst case. A scenario that needs randomness runs the few seeds
  that show its behaviour, and a cheap random world may stand as a smoke
  test. They run by default (`cargo nextest run --workspace`), and the
  suite takes at most **15 seconds**, so it can serve as quick feedback
  during development, as often as is useful.
- **Fuzzy tests** look for what no scenario names: sweeps of many random
  worlds, each settled under every invariant; domains driven at random
  against their worst case; conformance over many seeds of faults; a
  function against a naive one. They run with `--profile fuzzy`, and the
  suite takes at most **1 minute**: too slow to run in full at every turn
  of development. It is usually left alone until the merge gate, or run in
  part (one world's sweep, a few seeds) when that is worth it.

**The merge gate runs both suites in full,** whatever ran before it.

The budgets are wall time on the development machine, run idle, and they
are enforced: each nextest profile has a global timeout, so a suite that
runs past its budget fails. A change that breaks a budget is fixed by
making tests cheaper, or by moving randomized ones to the fuzzy suite.
Raising a budget is an explicit decision, not a fix.

A fuzzy test lives beside its world and uses that world's settings,
random ones included. When a fuzzy seed fails, the bug is fixed and the
seed kept: as a scenario if it shows behaviour worth naming, or among the
sweep's pinned seeds. A finding that cannot be fixed yet is replayed by
an ignored test until it is.

## 9. Where a failure is fixed

- **In the lowest tier that shows it.** A failure found in the real loop
  or a simulated world is reproduced lower down, where it replays and
  where the cause is closest, and fixed there. The lower scenario stays,
  as its regression test.
- **In skein, when it comes from skein.** A service's tiers stand on
  skein's and do not retest the kit. A failure in a service's world that
  comes from io, a machine or the simulator is reproduced in skein's own
  tier and fixed in skein.
- **A disagreement between the simulator and the kernel** is a
  conformance bug: the suite first, then the simulator (section 5).
- **What a service finds missing in skein** is added to skein, with the
  service as its first user and first test.
