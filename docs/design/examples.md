# The examples

Provisional, 2026-10-04, revised 2026-10-09. The design of skein's
example services (testing.md, 3): what each one is, the steps README.md
asks for before code (the wire protocol, the limits, the entities, the
state machines), its fakes, the harness its worlds run on, and how it is
tested. The examples exist to be tested and to be copied: a service
starts from them. The echo server is built; the HTTP server and its
client come with `skein-http`.

## 1. In one page

- **Laid out as a service is** (README.md, "Building a service on
  skein"): a domain, a protocol layer, a service with its `iterate` and
  the sum of its worst cases, and a shell with its `main`, each a crate.
  The step crates are in the model's subset, under the workspace's lints.
- **Their fakes are step machines** (testing-strategy.md, 4), so that the
  simulator and the real loop host them as they host the service.
- **One harness hosts them all:** `skein-world`, the loop of a simulated
  world over the simulator, with its trace, its memory check and its
  referee; and the same loop over the shell's kernels, on the real clock.
- **Tested in simulated worlds and on the real loop,** under tiny limits
  and every fault the simulator injects.

## 2. Layout

```
examples/echo/domain        skein-echo-domain     sessions, and the answer to each line
examples/echo/protocol      skein-echo-protocol   the listener and the connections: lines to calls and back
examples/echo/service       skein-echo-service    iterate, the limits, the sum of the worst cases
examples/echo/shell         skein-echo-shell      main: configuration, startup, the loop; the binary skein-echo
testing/skein-echo-client   skein-echo-client     the fake echo client, a step machine
testing/skein-world         skein-world           the world harness: simulated worlds and the real loop
tests/echo                  skein-echo-world      the echo's simulated worlds, and its real loop
```

The crate graph is README.md's: the domain depends on lib; the protocol
layer on lib, io and the domain; the service on all three, and re-exports
the domain's and the protocol layer's crates; the shell on lib, io, the
service and `skein-shell`; the worlds on lib, io, the service, the fake
client and skein's testing crates. Every crate may name lib, and every
one but the domain's may name io, as the role graph has it
(programming-model.md, 4): the shell names io's limits and records, and
reaches the domain and the protocol layer only through the service. The
fake client shares no type with the echo (testing-strategy.md, 4): it
depends on lib and io only.

## 3. The echo, before code

### 3.1 The wire protocol

A foreign-style line protocol, so scanned framing (programming-model.md,
8):

- **A request is a line:** any bytes but `\n`, then `\n`, at most
  `Limits::line` bytes with it.
- **The answer to a line is the same line,** its `\n` included. One
  request is in flight per connection: the server asks for room for the
  next answer before it reads the next line.
- **Refusals are small and fixed.** `busy\n`, as the first and only
  answer, when the domain has no session for the peer; then the server
  closes. `too long\n`, when a line passes `line` bytes without its `\n`;
  then the server closes, and io's graceful close discards the rest.
- **Idle:** a connection that sends no line for `idle` while the server
  waits for one, or reads nothing for as long while the server waits for
  room for its answer, is closed, gracefully and without a word. Each
  deadline is spread by up to `spread`, drawn from the seed, so that
  connections opened together do not all expire in one iteration.
- **End:** a peer that half-closes has every complete line answered, then
  the server closes gracefully. A last piece of a line, with no `\n`, is
  dropped.
- **Past the protocol's slab** of connections, an accepted socket is
  rejected: io closes it without a word.

### 3.2 Limits and the worst case

| Layer | Limit | What |
|---|---|---|
| domain | `sessions` | sessions at once: an `Open` past it is answered `Busy` |
| protocol | `conns` | connections at once: an accepted socket past it is rejected |
| protocol | `line` | the longest line, `\n` included, and the room each answer asks for: at least the longest refusal |
| protocol | `idle`, `spread` | the idle deadline, and how far it is spread |
| protocol | `retry` | how long the listener waits to listen again after io refused it for want of resources |
| io | io.md, 2 | `intake` and `output` at least `line`: the protocol's largest read and room |
| service | `queue` | each queue between the stages, at least the largest `MAX_OUT` |

```
worst case = io::worst_case(io)
           + domain:   the slab of sessions
           + protocol: the slab of connections, its deadlines and ready list,
                       conns × line (one line in flight per connection:
                       the bytes read, the domain's text, or the answer
                       before io takes it), and one line more (a step's
                       copy while it decodes or encodes)
           + service:  its queues' containers
```

Startup refuses a worst case past the configured memory, and io limits
that cannot meet the protocol's largest demand (io.md, 2): `line` past
`intake` or `output`.

### 3.3 Entities

| Layer | Entity | Made by | Owned by | Named by | Ends |
|---|---|---|---|---|---|
| protocol | the listener | startup | the protocol layer | a fixed owner token below; io's listener token | io's `Closed` |
| protocol | a connection | an accepted socket it binds | the protocol layer, which owns io's socket | `Id<Conn>`: io's owner token, and the `ReplyTo` of its calls | io's `Closed`, with no call out and its session told gone |
| domain | a session | an `Open` admitted | the domain | `Id<Session>`, as a token the connection holds | `Gone` |

The domain's vocabulary (programming-model.md, 4.4): calls up, `Open {
reply_to }` and `Line { session, reply_to, text }`, answered by `Reply {
to, reply }` with `Admitted { session }`, `Busy` or `Echo(text)`; and
events up, `Gone { session }` and `Shutdown`, the latter answered by the
request `Stop`: admit no one more. The text of a line is its bytes without
the `\n`: framing stays in the protocol layer, which copies the text out
when it decodes and writes the answer at its length when it encodes.

The protocol layer tells the domain `Shutdown`, from its ready list,
when io's `Shutdown` event arrives (io.md, 7), or when the service's
`shutdown` asks it to, which worlds below the binary call. The domain
decides what it means: it admits no one more, and asks for the listener
to stop. The connections it has drain (programming-model.md, 5.2): one
waiting for the peer's next line, or for its greeting's room, is idle and
closes at once; one with a line out answers it, then closes. None waits
for its idle deadline, so the echo ends without one firing
(testing-strategy.md, 6). Today the connections run to their end, each
until its peer ends or its idle deadline passes (section 8).

### 3.4 State machines

**The connection.**

| State | Holds | Demand | Deadline |
|---|---|---|---|
| Greeting | io's socket | room for an answer | idle |
| Admitting | socket; whether the peer ended | | |
| Reading | socket, session | a scan to `\n`, at most `line` | idle |
| Answering | socket, session; whether the peer ended | | |
| Draining | socket, session | room for an answer | idle |
| Closing | whether io told `Closed`; whether a call is out; the session to tell `Gone` | | |
| Closed | | | |

The demand and the deadline are one function of the state, applied after
every transition: entering Greeting, Reading or Draining states its demand
and arms the idle deadline afresh, so that each line read and each room
granted is progress (programming-model.md, 7).

| State | Event | Next, and what it does |
|---|---|---|
| Greeting | `Room` | Admitting: `Open` |
| Admitting | `Admitted` | Reading; or, if the peer ended, Closing: `Close`, `Gone` owed |
| Admitting | `Busy` | Closing: `busy\n`, `Close` |
| Reading | `Bytes`, a line | Answering: `Line` |
| Reading | `Bytes`, no `\n` | Closing: `too long\n`, `Close`, `Gone` |
| Answering | `Echo` | Draining: the answer sent; or, if the peer ended, Closing: the answer, `Close`, `Gone` owed |
| Draining | `Room` | Reading |
| Admitting, Answering | `End` | the same, the end noted |
| Greeting, Reading, Draining | `End` | Closing: `Close`, `Gone` if admitted |
| Greeting, Reading, Draining | the idle deadline | Closing: `Close`, `Gone` if admitted |
| any open state | `Failed` | Closing: `Close`, and `Gone` if the session is known; in Admitting, once the domain answers |
| Closing | the reply to its call | `Admitted`: its session owed `Gone`; `Busy`, `Echo`: dropped |
| Closing | `Bytes`, `Room`, `End`, `Failed` | ignored: told before io took the close |
| Closing | `Closed` | retired once no call is out and nothing is owed |
| Closing | resumed | `Gone` told |
| any but Closing | `Closed` | impossible: io tells it only after the close |
| any without the demand | `Bytes`, `Room` | impossible: io answers only a demand |
| any without a call out | a reply | impossible: one reply per call |
| any without the deadline | the idle deadline | impossible: cancelled on leaving |

A `Gone` owed by a down-pass cell (a reply that finds the connection
closing) is up from the down pass (programming-model.md, 2): the
connection goes on the protocol's ready list, and `resume` tells it.
`Gone` always comes after the session's last call in the domain's queue,
which is first in, first out: a stream that fails while its line is out
tells `Gone` at once, behind the line, which the domain answers first. So
the domain never answers for a session it has retired. A connection
retires once io told `Closed`, no call is out, and its session was told
gone: the bindings above and below have both ended (programming-model.md,
5.2).

**The listener.** The address it listens at is the layer's, from its
making.

| State | Holds | Deadline |
|---|---|---|
| Unopened | (on the ready list) | |
| Opening | whether the domain asked it to stop | |
| Listening | io's listener token, the address bound | |
| Closing | its failure, if one came | |
| Failed | its failure; whether the domain asked it to stop | |
| Backoff | when it listens again; the shortage that refused it, for `main` to report | `retry` |
| Closed | its failure, if one came | |

| State | Event | Next, and what it does |
|---|---|---|
| Unopened | resumed | Opening: `Listen` |
| Opening | `Listening` | Listening, its address kept; or, if stopped, Closing: `Close` |
| Opening | `Failed` | Failed: io closes it |
| Opening | `Stop` | Opening, stopped |
| Listening | `Accepted` | a connection: `Bind`, Greeting; or, the slab full, `Reject` |
| Listening | `Failed` (its accept stopped) | Closing: `Close` |
| Listening | `Stop` | Closing: `Close` |
| Closing | `Accepted` | `Reject`: announced before io took the close |
| Closing | `Failed` | its failure kept: told before io took the close |
| Failed | `Closed` | `Busy`, and not stopped: Backoff, its deadline `retry` on; `Busy` and stopped: Closed; otherwise Closed, the failure kept |
| Backoff | its deadline | Opening: `Listen` |
| Closing | `Closed` | Closed |
| Unopened, Backoff | `Stop` | Closed |
| Failed | `Stop` | Failed, stopped |
| Closing, Closed | `Stop` | ignored |

io's `Busy` says that nothing was made and the request may be made again
later (io.md, 2): a listen refused for want of descriptors or buffers is a
shortage, not a failure, so the listener listens again after `retry`.
Any other failure stops it for good, and the service with it: `main` then
says why and exits. A failure is kept whether a stop came meanwhile or
not; a shortage never counts as one, but Backoff keeps it, and `main`
says once that the listen is refused and retried, so that a shortage that
lasts is seen. Retrying is the protocol layer's policy, kept where the
listener is.

**The session** is active from `Open` admitted to `Gone`, and counts the
lines it answered. **The domain** admits until `Shutdown`, then answers
every `Open` with `Busy` and asks once for `Stop`.

## 4. The echo's main

`main` reads its configuration from its arguments (the address to listen
on, and the memory it may take), runs startup (shell.md, 6): the worst
case against the memory, io's caps against the protocol's largest demand,
the termination signals blocked and their signalfd opened
(`open_termination_signals`) for io to adopt, then the seed and the
kernel; and then the loop of programming-model.md, section 2, over the
shell's `Kernel` and `Clock`.

- **It ends by itself.** `SIGINT` or `SIGTERM` arrives as io's `Shutdown`:
  the echo admits no one more, its connections end (section 3.3), and the
  loop exits once the service holds nothing, with a success (shell.md,
  13). A listener that fails, as when the address is in use, ends it too,
  with the failure on standard error.
- **Its loop is `drive`** (shell.md, 12), over the echo's `Host`, once
  `drive` is in the kit. Its hook says once that the echo listens, and at
  what address, and once that a listen refused for want of resources is
  being retried: what `main`'s loop says today between `iterate` and the
  submit.

## 5. The fake echo client

`skein-echo-client` is a step machine (testing-strategy.md, 4): io and
one layer above it, which plays the line protocol over io's stream
vocabulary, with an `iterate` of its own and its own limits and worst
case. It shares no type with the echo. Each connection follows a plan:

- when it connects, how many lines it sends and their lengths, drawn
  from its seed, one of them past the server's limit if the plan says;
  how many it sends ahead of their answers, in pieces of what size;
  from when it reads, and after how many bytes it stops sending;
- what it does after its last answer: half-close and wait for the
  server's end, close, abort, or linger until the server closes; or an
  abort at a given time, whatever it is doing;
- how often it tries again, after a refusal or a broken stream, and how
  long it waits first.

It holds no copy of what it sent: a line's bytes are drawn from the
seed, and drawn again, from the same seed, to check its answer. It checks
its peer as it goes, failing the world on the first breach: each answer
is the oldest line unanswered, byte for byte; `busy\n` comes only first;
`too long\n` only to the line past the limit; nothing comes after either
but the end. What it saw (answers, refusals, the server's end, failures,
the bytes it handed io, when it closed) is kept for the referee.

## 6. The world harness

`skein-world` (testing-strategy.md, 7) is skein's: its examples are the
second user of a world. It is ordinary Rust, generic over a scenario's
processes, each a host of an `iterate` (a service, or a fake client):

- **One loop** drives every process over the simulator: each reaps,
  iterates and submits, in turn, and the referee observes each.
- **Time** moves only when the world is idle, to the earlier of what the
  simulator has due and the processes' and the referee's earliest
  deadline (simulator.md, 3).
- **Trace and replay:** a run returns the simulator's trace, which the
  same seed replays.
- **Memory** (simulator.md, 5): under the counting allocator, each
  process's heap, what grew within its own calls (building it, and each
  `iterate`), at its peak within each call, against its own worst case.
  What grew within the simulator's calls and the harness's is left out,
  by metering around the processes' calls rather than theirs; and as
  nothing one process allocates is freed by another or by the simulator,
  each process's growth is its own. A process frees in one call what it
  allocated in another, so the heap falls below where a call began: the
  counting allocator's span measures a call's growth signed, where its
  meter, which checks a step from a base it never falls below, cannot. A
  process's worst case counts the box the harness keeps its state in.
  Once settled, dropping the run's outcome drops each process under a
  span: it must free exactly what was metered as its own, which finds a
  leak, and heap made or freed outside its calls.
- **Contracts:** each process keeps its own as it goes, as its loop is
  its own: the echo's and the fake client's `iterate` assert each call
  within its `MAX_OUT`, the protocol layer one reply per call, the fake
  client the echo's contract; the simulator, the kernel boundary's.
- **Settled:** once the referee passed and nothing is busy, every process
  holds nothing, the simulator has nothing in flight, and every
  descriptor is closed.
- **The shell's own `Host`** (shell.md, 12), which `skein-world`
  re-exports: the harness calls each process's hook where `drive` does,
  after its `iterate`, and checks the teardown invariant
  (testing-strategy.md, 6): after the scenario's last word, no deadline
  but io's own may fire before the world settles. The echo's idle
  deadline runs at its shipped value or beyond the world's horizon,
  unless the scenario is about it.
- **The referee** holds a scenario's expectations, each with a deadline:
  safety on every observation, liveness as the deadline; it may also
  inject what belongs to no fake. The echo's tells the fake clients the
  echo's address once it listens, as a directory would, and shuts the
  echo down once the clients are done, or at a given time, so that the
  world settles. Both reach into the service where the referee should
  only watch from outside (testing-strategy.md, 7): its reading of
  `svc.listening()` stands in for the fact `main` prints, "listening at",
  which the service will emit as a fact; and its call of `svc.shutdown()`
  stands in for the termination signal, which io's `Shutdown` event
  already carries to the shipped echo (io.md, 7), and which the simulator
  can deliver to a process's signal source. Each moves out of the
  referee.
- **The real loop** runs the same processes and referee over one ring, in
  one thread on loopback, with deadlines on the real clock. An idle loop
  blocks on the ring until the next completion or deadline.
  - **Each process's operations stay its own.** The harness renames each
    token to one unique in the world, and back as its completion is
    reaped. It holds each process to the operations in flight its own
    ring would allow, and the ring has room for all of them.
  - **A spawned service is hosted, not run.** Its spawn never reaches the
    kernel: the harness makes the child's pipes, hands the parent its
    ends, and starts the child in the same loop with the other ends, as
    the simulator does (simulator.md, 3). The parent's wait ends once the
    child has finished and the harness has closed what it still held, as
    the kernel would at its exit. A kill drops the child and closes
    everything it held.
  - **Signals.** One service reads the process's signalfd, as it would
    alone, and the referee sends it real signals. A process's signals
    cannot be aimed at one of the services it hosts, so every other
    service, a hosted child among them, reads its signals from a pipe the
    harness gives it. A parent's signal to a hosted child, or the
    referee's, arrives there as a signal record.
- **End to end** (testing-strategy.md, 2.9), the harness starts a
  service's binary as a child, as it ships, on pipes or under a
  pseudo-terminal. The test's ends of the pipes, or the terminal's other
  side, join the test's loop as streams of its scripted processes. The
  child's exit arrives in the loop as an event, and the referee may send
  the child a signal. Nothing of the child is hosted: it runs its own
  loop.
  - **Its tree is the kit's to settle and to count.** A binary may start
    processes of its own, which may leave its group or its session. None
    may outlive the test, and a test that measures what the binary used,
    a benchmark's harness among them, wants all of it: CPU time and peak
    memory. The kit settles and counts the binary's whole tree, one of two
    ways, and the test's result says which ran.
  - **A cgroup, where the machine delegates one.** When the test's user
    holds a delegated cgroup v2 subtree (a systemd user scope, a
    container's), the kit makes a cgroup per binary and starts the binary
    in it (`CLONE_INTO_CGROUP`), so every descendant is in it, whatever
    group or session it makes. The tree has settled once the cgroup is
    empty: `cgroup.events` says it is not populated, and the kit is told
    when that file changes. Its counts are the cgroup's: user and
    system CPU from `cpu.stat`, and the tree's peak memory, all its
    processes at once, from `memory.peak`.

    This is the first use of the kernel's cgroup facilities that
    contained trees build on (draft/process.md). It is written as a tree
    in that draft's terms (spawned into a cgroup, empty, killed, counted)
    so that it can move into io whole. When io gains trees, the kit
    starts its binary as an io tree and drops its own cgroup code; the
    walk below stays only for machines with no delegated subtree.
  - **A walk of pidfds, otherwise.** The test process is made a
    subreaper (`PR_SET_CHILD_SUBREAPER`), so a descendant whose parent
    exits is reparented to it rather than to init. The kit holds a pidfd
    for the binary from its spawn, and opens one (`pidfd_open`) for each
    process it finds among its own children (`/proc/self/task/*/children`):
    when the binary exits, whenever one it holds exits, and at a short
    period while any runs. It waits on each pidfd, never on any child, so
    it takes no other part's children. The tree has settled once it has
    reaped every one. Its counts are the kernel's count of reaped
    children (`Usage`, kernel.md, 6.3), taken before the binary starts and
    after the tree settles: CPU summed over the tree, and a peak that is
    the largest one process reached, not the tree's at once, which only a
    cgroup gives. The kernel's peak is the largest over every child the
    test process ever reaped, so it is this tree's only where nothing
    larger was reaped before: a test that measures by the walk starts one
    binary per process. A process orphaned below a parent the kit does not
    hold is reparented to the test process all the same, and the next
    listing finds it.
  - **The end.** Once the binary has exited and the test is done with
    it, or at the test's deadline, the kit kills what is left of the tree
    (`cgroup.kill`, or each pidfd it holds and the binary's group), waits
    for it to settle, then reads the counts and removes the cgroup. A
    test that expects the tree to end with its binary fails on a process
    still running at its exit, naming it; one that measures a binary it
    does not own only kills and counts.

## 7. Testing

- **Step tests** in each example crate: every cell of the connection and
  the listener, the domain's admission and refusal, `iterate` with its
  `MAX_OUT` reserved at each stage, the worst case and the startup
  checks; and the fake client's own cells.
- **Simulated worlds** (`tests/echo`): the echo and fake clients as
  processes of the simulator, under tiny limits, calm and chaotic: many
  clients; a line too long; a busy refusal at the entrance, and a
  rejection; an idle timeout; a client that stops reading, stopped at the
  server by backpressure and then idled out, its close discarding the
  rest; closes and resets in every state; a shutdown; a half-close with
  lines unanswered, every whole line answered before the end and a
  trailing piece of a line dropped (section 3.1's end, tested); and the
  echo driven to its worst case. Calm, each scenario holds whole; under
  faults, a stream may break, so each expects only that every connection
  finishes, while the fake clients still check every answer they get, and
  the idle deadline's "not before" is calm's alone: a server whose side of
  a stream broke ends it at once, which the client cannot tell from an
  early idle. So that the faulted sweep still shows the echo at work,
  every outcome a client can see must appear over it: served, busy,
  rejected, broken, too long, idled out. Each admission point's scenario
  checks what the clients saw for evidence that it was reached; the
  protocol layer's rejection shows as the end without a word, or as a
  reset when the client's first line was already there unread. A rare
  cell, a stream failing while its line is out with the domain, is pinned
  by its seed. Focused seeds in the default suite; sweeps in the fuzzy
  one, asserting that every fault fell. Every binary checks memory at
  every iteration, and that each process frees what it held once
  settled; io holds the echo to the room it was granted in every world.
- **The real loop** (`tests/echo/tests/real.rs`): the echo and its fake
  clients in one loop over the shell's ring, on loopback, quick, failing
  clearly where `io_uring` is unusable.

## 8. Not built yet

- **The HTTP server and its client,** with `skein-http`.
- **The echo's `main` over `drive`,** with its hook (section 4), and its
  worlds under the teardown check (section 6), once `skein-shell` has
  both (shell.md, 11); with them, its connections draining at a shutdown
  rather than running to their end (section 3.3).
- **Domain worlds** for the echo's domain: it is one slab and a counter,
  and its step tests cover it.
