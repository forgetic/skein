# Consumers

Provisional, 2026-10-02. How a service built on skein is put together:
what it writes, what it takes from skein, and how the pieces meet. The
rules for writing the code are `programming-style.md`; the kit itself is
`overview.md`. temper is the first consumer (overview.md, section 11).

## 1. In one page

- **A service is three layers over a shell:** io, protocol and model. io
  is skein's. The protocol layer and the model are the service's. The
  shell is the service's `main`, built from skein's shell kit.
- **The crate graph is the layer diagram.** The model does not depend on
  io, so it cannot name a file descriptor. The protocol layer is the one
  crate that sees both vocabularies.
- **The model is complete.** It runs the service's whole behaviour in a
  world of models and fakes, with no protocol and no io. The protocol layer
  translates between bytes and model entities, and decides nothing.
- **The loop is the service's,** about ten lines over skein's `Kernel`,
  `Clock` and seed. skein has no `run` and calls nothing in the service.
- **One process, one loop.** A service that needs more cores runs more
  processes.
- **Worlds first.** The model is built and tested in model worlds before
  any byte is parsed; the protocol layer and the loop follow, with skein's
  simulator standing in for the kernel.

## 2. Who writes what

| Part | Written by | Crate |
|---|---|---|
| handles, slabs, queues, cursors, deadlines, streams | skein | `skein-lib` |
| io: sockets, pipes, files, processes | skein | `skein-io` |
| machines for foreign protocols: HTTP/1.1, server-sent events, JSON, TLS | skein | `skein-http`, `skein-json`, `skein-tls` |
| the protocol layer: its own protocols, its decoders, its connections | the service | `protocol` |
| the model, and its sub-models | the service | `model` |
| `iterate`, and the sum of the worst cases | the service | `service` |
| `main`: configuration, startup, the loop | the service | `shell`, on `skein-shell` |
| the simulated kernel | skein | `skein-sim` |
| the fake machine a world plugs into the simulator | the service | its tests |
| lints and `clippy.toml` | copied from skein | the workspace |

Anything a service needs that belongs to no one application (a lib
container, a protocol machine, an io operation) goes into skein, pulled by
that service (overview.md, section 1).

## 3. Layout

```
Cargo.toml     workspace: profiles and lints, copied from skein (programming-style.md, 9.4)
clippy.toml    disallowed types and macros, copied from skein
protocol/      protocol::up, protocol::down: connections, the service's own machines and decoders
model/         model::step: domain machines, with any sub-models below it (section 7)
service/       the three layers wired together; service::iterate, worst_case
shell/         main: configuration, startup, the loop
fuzz/          one target per machine of the service's own
```

```
crate      depends on
model      skein-lib                                  and its sub-models (section 7)
protocol   skein-lib, skein-io, the skein machines it stacks, model
service    skein-lib, skein-io, protocol, model
shell      service, skein-shell
tests      service, skein-sim
```

## 4. The loop

```rust
// shell: the service's main, after startup; a sketch
loop {
    kernel.reap(&mut svc.completions);         // completions, one per operation
    let now = clock.now();                     // monotonic and wall time, read once
    service::iterate(&mut svc, now);           // pure: both passes, the reclaim point
    let wait = svc.wait();                     // nothing while work is pending, else the earliest deadline
    kernel.submit(&mut svc.submissions, wait);
}
```

```rust
// service::iterate: pure, and the same function a simulated world drives

// up pass
for c  in take(completions) { io::up(&mut io, &io_env, c, &mut proto_in, &mut subs) }
for ev in take(proto_in)    { protocol::up(&mut proto, &proto_env, ev, &mut model_in, &mut proto_out) }
for ev in take(model_in)    { model::step(&mut model, &model_env, ev, &mut model_out) }

// down pass
for rq in take(model_out)   { protocol::down(&mut proto, &proto_env, rq, &mut proto_out) }
for rq in take(proto_out)   { io::down(&mut io, &io_env, rq, &mut subs) }

// reclaim point
io.reclaim(); proto.reclaim(); model.reclaim();
```

```
completions -> io::up   -> protocol::up   -> model::step     (up pass)
submissions <- io::down <- protocol::down <- model::step     (down pass)
```

The sketch shows the main flow only. `take` hands a stage its inputs one
at a time while the step's output queues have room for the most one input
can produce, a bound each entry point declares as `MAX_OUT`; whatever does
not fit waits for the next iteration. A step in the up pass may also queue
requests downward (a protocol machine closing on a framing error), and
those join the down pass of the same iteration. The reverse happens too: a
step in the down pass may produce an event that must go up, such as a read
demand already met by buffered bytes, or a request refused without
reaching the kernel. That event is held as state of the entity that will
emit it, not as a queued record, so it outlives the iteration's queues:
the entity goes on its layer's ready list, which the layer drains at the
start of its stage in the next up pass. A stage then takes its input
events, then fires its expired timers (programming-style.md, section 8).

Properties the rest of the design relies on:

- **Work per iteration is bounded.** Every queue is a `lib::Queue` whose
  capacity is fixed at startup.
- **Requests made in the down pass are submitted at the end of the
  iteration;** the events they cause arrive in a later one. A step never
  waits for its own effect.
- **The loop never blocks while a queue or a ready list is non-empty,** so
  it does all the work it can before waiting on the kernel. Reaping every
  round keeps one peer's backlog from holding up other connections'
  completions and timers.
- **Reclaiming happens at the end** (programming-style.md, 4.2).

What `main` does before the loop (checking the worst case, blocking the
termination signals, reading the seed, opening the roots and resolving
peer names) is done with skein's shell kit (overview.md, 5.2, section 8
and 10.5).

## 5. The three layers

| Layer | Written by | State is bound to | State dies when | Main risk | Best check |
|---|---|---|---|---|---|
| io | skein | the kernel | the operation completes | buffers in flight, cancellation | the simulator, conformance |
| protocol | the service, on skein's machines | the peer | the connection closes | untrusted input | fuzzing |
| model | the service | the domain | the session ends | logic, policy | model worlds, replay |

- **Every completion goes through io first.** The model never sees a raw
  completion, a file descriptor, a kernel error code or a half-filled
  buffer; it cannot, since it does not depend on io.
- **The model never parses.** Bytes are untrusted until the protocol layer
  has turned them into typed, size-bounded messages. Structure inside a
  payload (JSON in a body) is one more protocol machine, not model code.
- **The model is complete.** Everything a peer can cause arrives as a
  model entity, and everything the model wants done leaves as one, so a
  world of models and fakes runs the service's whole behaviour with no
  protocol and no io (section 8). The protocol layer only translates
  between bytes and model entities: it decides nothing, and it never sits
  between two pieces of model logic. Structure the domain acts on is
  decoded on the way in, all of it: a tool call inside an LLM's answer
  reaches the model as a typed call, not as JSON to be sent back down for
  decoding later.
- **The boundaries follow the style's rules** (programming-style.md,
  section 3): tokens, owned records, one terminal event per request,
  policy above and mechanism below.

## 6. The protocol layer

- **A connection is a stack of machines,** skein's and the service's own,
  stacked by the rules of programming-style.md, 3.4. The connection routes
  between them, arms their deadlines and holds the one deadline table of
  the layer. An LLM client's stack, from the socket up: io, TLS, the HTTP
  client, server-sent events, JSON, the service's decoder, the model
  (overview.md, section 4).
- **Protocols the service designs are sized** (programming-style.md,
  section 7). Foreign protocols are built on skein's machines.
- **Documents are decoded by the service,** with small state machines over
  skein's JSON tokens, into the model's types (overview.md, 10.3).
- **The protocol crate is the one place where io and the machines meet:**
  it depends on io and on the machines it stacks; the machines depend on
  neither.

### 6.1 Protocol to model

The types at this boundary belong to the model: the model crate defines
what it accepts and emits, and the protocol crate depends on it.

```rust
pub enum Event {                                  // protocol -> model
    Call { reply_to: ReplyTo, op: Op, key: Box<[u8]>, value: Box<[u8]> },
    // terminal events for requests the model made
}

pub enum Request {                                // model -> protocol
    Reply { to: ReplyTo, status: Status, value: Option<Box<[u8]>> },
    // requests the model makes: outbound calls, each with its own token
}
```

Two shapes cross this boundary:

- **Requests down, one terminal event up,** as at every boundary.
- **Calls up, replies down.** A peer's request arrives at the model as a
  `Call` carrying a `ReplyTo`; the model answers with exactly one `Reply`,
  which consumes it, now or later (a model that answers later keeps the
  `ReplyTo` in the state that waits for the answer). `ReplyTo` is neither
  `Copy` nor `Clone`, so the compiler rejects a second reply; the
  simulator catches a missing one. Wire correlation ids stay in the
  protocol layer; opcodes and statuses cross as domain enumerations. The
  model knows nothing about connections.

## 7. Sub-models

A model too large for one crate is a tree of sub-models under one
top-level model crate.

- **Each sub-model is a step machine of its own:** its own vocabulary,
  limits, worst case, state machines, entry points and `MAX_OUT`, and its
  own tests. It depends on lib and on its children, never on a sibling or
  a parent.
- **A parent owns its children's states and routes between them** within
  its step. Hand-offs inside the model are short and acyclic, so a step
  completes them before it returns; its `MAX_OUT` follows from its
  children's along the longest chain.
- **Siblings share no domain types.** The parent translates between their
  vocabularies with small total functions; an exhaustive match makes a
  change on either side break the build in one place.
- **Only the top-level model faces the protocol layer,** and its worst case
  is the sum of its sub-models'.
- **Each sub-model has a world of its own** (section 8), and so does the
  top-level model.

## 8. Testing a service

The rules every world follows (tiny limits, faults in every state, the
universal invariants, coverage, memory, replay) are programming-style.md,
section 10. How the service's tiers sit on skein's own, and what skein
supplies for each, is testing-pyramid.md, section 7. The worlds a service
builds:

- **Model worlds.** Each model, and each sub-model, runs in a world of its
  own: the model, the clock, the seeds, and fakes standing in for its
  neighbours (another party's model, a filesystem), with no protocol and
  no io. A fake shares no domain types with what it stands in for; the
  world translates between them, as a protocol layer would. Model worlds
  come first and test behaviour.
- **Machine fuzzing.** Each of the service's own machines and decoders is
  fuzzed alone. skein fuzzes its own.
- **Simulated worlds.** skein-sim plays the kernel for one service or
  several (overview.md, section 9). The world runs the services: its loop
  calls each one's `iterate` in turn. The service supplies the fake
  machine that answers file operations and runs programs; a spawned
  program may be another service, hosted in the same world. These worlds
  run in plaintext, since TLS is not deterministic. They test mechanics:
  the protocol layer and io under faults.
- **The real loop.** The shell against the real kernel, with TLS, for the
  paths no world can reach.

## 9. Starting a new service

Before code:

1. The wire protocol, sized, with every length and limit; or, for a
   foreign protocol, the skein machines it stacks.
2. The limits of each layer, and the worst case they imply
   (programming-style.md, 5.4).
3. The entities of each layer, who owns each, and how they bind.
4. The state machines: states, what each holds, the total transition
   table, demands and deadlines per state.

Then, in order:

1. the model, unit-tested by feeding events and inspecting requests, and
   run in model worlds;
2. the protocol layer, on skein's machines, its own machines fuzzed alone;
3. the service and the shell last, with the simulator standing in for the
   kernel until then.

What the service finds missing in skein along the way is added to skein,
with the service as its first user and first test.
