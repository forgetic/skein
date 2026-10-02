# Programming style (Rust)

Provisional, 2026-10-02. How step code is written, in skein and in every
service built on it.

**How to read this.** This is the language-agnostic model (saf's
`PROGRAMMING-MODEL-AGNOSTIC-RELAXED.md`) with its open choices made for
Rust: the memory strategy (section 5), the subset of the language (section
9), and what checks each rule. Section 12 lists where it departs from the
agnostic model. The subset is deliberately small: Rust is used for
ownership and borrow checking, enums with data and exhaustive matching,
and crate boundaries, and for little else. Where the compiler or clippy can
check a rule, the check is named; where neither can, the rule is a
convention held in review. Section 9.1 says which is which.

This document holds the rules. Its neighbours hold the rest:

- `overview.md` is skein itself: the parts, the kernel boundary, the
  backends, the simulator and the protocol machines.
- `consumers.md` is the shape of a service built on skein: its loop, its
  three layers (io, protocol, model), its sub-models and its worlds.

## 1. In one page

- **One thread, one loop, no async.** The loop reaps completions from the
  kernel, runs the step functions, and submits what they asked for. It is
  the only code that talks to the kernel, and the only thing that schedules
  work: no `async`, no futures, no runtime, no callbacks. The loop belongs
  to the service (consumers.md, section 4).
- **Layers of step machines, and the crate graph is the layer diagram.**
  Each layer has its own state, entities and vocabulary, and depends only
  on the layers below it. A layer that does not depend on io cannot name a
  file descriptor.
- **Step functions are sans-io, and the compiler knows it.** Step crates
  are `#![no_std]` with `alloc`: no syscalls, no clock, no threads, no
  printing, no hash maps with random seeds. Time and randomness are inputs;
  effects are outputs. The same state and input give the same output. TLS
  is the one exception (section 2).
- **Entities are named, not referenced.** Each entity lives in a slab owned
  by its layer and is named everywhere else by a typed handle, `Id<T>`,
  that is never reused for another entity. No application type has a
  lifetime parameter, so no reference can be stored and no borrow outlives
  a step.
- **Flow control is explicit.** Reading is a demand, a connection has one
  request in flight, and a peer that does not read stops being read from.
  Whatever the service limits, it refuses at the entrance, where saying no
  is cheap.
- **Counted entities, owned bytes.** Slabs and queues are sized at startup,
  and a full slab is a refusal. Bytes are a `Box<[u8]>` allocated at its
  final length after validation, owned by one place at a time and moved,
  never shared. The worst-case footprint is computed at startup and must
  fit.
- **One lifecycle everywhere.** Every entity is active, then closing, then
  closed. Close goes down, closed comes up, reclaiming is bottom-up, and
  every request gets exactly one terminal event. Nothing happens in a
  destructor.
- **A small Rust.** Structs, enums, `match`, functions, references, moves
  and a short list of library types. No `async`, closures, trait objects,
  user-defined traits or generics, `Rc` or `RefCell`, `Drop` impls, or
  `unsafe` outside one module. Panics abort.

## 2. Step functions

```rust
pub fn step(model: &mut Model, env: &Env<Limits>, ev: Event, out: &mut Queue<Request>)
```

```
model   this layer's state, updated in place: the only &mut the step receives
env     the times and this layer's limits, behind a shared borrow, so read-only
ev      one self-contained input, moved in
out     a bounded queue of owned requests, with MAX_OUT slots reserved by the loop
```

A step returns nothing. Every outcome of an event is a change to the
layer's state or a request in `out`; the loop reserved the room, so
emitting cannot fail, and running out of heap aborts (section 5). Inside a
step, every fallible operation returns a `Result` or an `Option`, which the
caller cannot silently drop (section 9).

A layer in the middle has two entry points of this shape: `up` for events
from below and `down` for requests from above. A protocol machine has the
same pair, one per side (section 3.4). The top layer has one.

What "pure" means, and what holds it:

- **No syscalls,** directly or through a library: no printing or logging,
  no clock reads, no entropy, no file, socket or process operations, no
  threads. Step crates are `#![no_std]` with `extern crate alloc`, so
  `std::{io, fs, net, time, thread, process, env}`, `println!` and
  `thread_local!` do not exist there, and they depend on nothing outside
  the workspace.
- **Time is data.** The step reads `env.now`, a `lib::Time`; a deadline is
  a value it computes and arms in its own layer's table (section 8).
- **Randomness is injected state:** a `lib::Rng` in the layer's state,
  seeded by the shell or the simulator.
- **Nothing observable depends on memory.** No output may depend on
  addresses or on allocation order. `alloc` has no `HashMap`; maps are
  `BTreeMap`, whose order is the keys'. There is no formatting in step
  code, so no address can be printed.
- **No hidden state.** No `static` (a mutable one needs `unsafe`, and
  atomics and cells are disallowed types), no closures carrying state
  between calls; all state is in the arguments.
- **No hidden control flow or effects.** Outcomes are returned; panics are
  fail-stop. There are no `Drop` impls in step code: closing is a request,
  and reclaiming follows *closed* (4.2). Dropping a `Box<[u8]>` frees
  memory and does nothing else.
- **Effects are data.** To send bytes or close a socket, a step pushes a
  request into its output queue. The outcome arrives later as an event.
- **Bounded.** No `loop` or `while` in step code: `for` over a range or a
  slice, whose bound is a configured limit or the size of an input already
  validated. No scan over an unbounded structure, no recursion.
- **Diagnostics are data too.** Trace records are enum values pushed into a
  bounded queue the shell writes out. Assertions are fail-stop.

**The exception is TLS.** `skein-tls` wraps rustls: it is the only step
crate that depends on code from outside the workspace, and the only one
that is not deterministic, since its cryptography draws entropy from the
kernel. The simulator and the worlds run without it (overview.md, 10.4).

What it buys: a simulator can own the clock, the random state and the
kernel, and run a service deterministically under fault schedules; a
recorded event log replays to the same state; each layer can be fuzzed or
unit-tested by feeding events and inspecting the requests that come out;
and a step that cannot block cannot stall the loop.

## 3. Layers and boundaries

A layer is a step machine, or a stack of them, with state bound to one
thing: the kernel (io), the peer (a protocol connection), the domain (a
model). A service's layers are in consumers.md, section 5. Whatever the
layers are, these hold at every boundary:

- **Policy above, mechanism below.** The model decides the deadline and
  whether to retry; the protocol layer runs the timer and the attempt.
- **A lower layer absorbs mechanics, not information.** The layer above
  still learns *that* a call timed out; it does not see the cancel race
  behind it. And nothing hides cost: an entity that lingers still holds
  what it holds.
- **Each layer has its own entities.** There is no shared "connection"
  across layers. A model client may outlive many connections; one socket
  may carry many protocol streams; one protocol connection may serve many
  model calls over time; a child process is three pipes and an exit
  status. Name them differently per layer so they are not conflated:
  *socket* (io), *connection* (protocol), *client* or *session* (model).

### 3.1 Bindings are echoed tokens

Adjacent layers link the way io_uring's `user_data` works, applied at each
boundary:

- Going down, the upper layer passes a **token**, its own handle for the
  entity concerned. The lower layer stores it without interpreting it.
- Going up, the lower layer **echoes the token** on every event.
- The upper layer stores the lower layer's handle, also as a token, to
  address requests to it.

Every name that crosses a boundary is a `lib::Token`, an opaque `u64`. A
layer makes one from its own handle (`id.token()`) and turns it back into
one (`Id::<Conn>::from_token(t)`), and only for tokens it issued, in the
record variant it issued them for. Within a layer, names are `Id<T>`. Each
side holds the other's name as an opaque field; there are no mapping
tables. For entities created from below (an accepted socket), the lower
layer announces the new entity and the upper layer either binds to it or
asks for it to be closed.

One token type keeps io free of generics and the model free of protocol
types. The price is that the kind of entity a token names is carried by the
record variant, not by the token's type: nothing but that variant stops a
layer from decoding a token as the wrong kind.

### 3.2 Records

Events up and requests down are owned values with no lifetime parameters,
so they hold no references into either layer's state: a token, a variant,
and a payload the record owns, size-bounded by the limits of the layer
that made it. Such a queue is loggable and replayable as it stands. The
types belong to the lower of the two layers' vocabularies, except at the
top, where the model defines what it accepts and emits and the protocol
layer depends on it (consumers.md, 6.1).

**Requests go down, and each gets one terminal event up,** at every
boundary (4.2).

### 3.3 Streams

io's vocabulary has two parts: **entities** (listen, accept, connect,
spawn, close) and **bytes** (demand, deliver, room, send, end). The byte
part is the same wherever bytes flow, so it is defined once, as
`lib::stream`, and every byte boundary uses it: a socket, a pipe, the
plaintext above TLS, the body above HTTP. The types are in overview.md,
section 4; io's records in section 5.

- **Reading is a demand, not a request.** Each state says what it needs,
  "fill N bytes" or "scan to a delimiter, at most M bytes", and how much
  output room. The side below delivers `Bytes` only when that demand can
  be met, and `Room` likewise; a state with no demand receives neither.
  `Bytes` carries exactly the demanded bytes as an owned `Box<[u8]>`, read
  through a `lib::Reader`.
- **Unparsed input belongs to the side below,** under that side's cap, in
  its `lib::Intake`: io for a socket, TLS for its plaintext. The side above
  never handles another layer's receive buffers.
- **Writing is a move.** The side above encodes a message with a
  `lib::Writer` into a `Box<[u8]>` of exactly its length and moves it down
  in `Send`. The side below queues it against the output cap, then moves
  it on (5.3).
- **Machines express demand, not buffer handling.** The byte source can
  then change (copying today, exact-size kernel reads later) without
  touching the code above.

### 3.4 Machine stacks

A protocol connection is a stack of machines (TLS, HTTP, event framing,
JSON, the service's own), and its stage is a stack, not one function.

- **Each machine is a step machine** with entry points of the same shape
  on each side, its own limits and worst case, and its own fuzz target.
- **Machines depend on lib only,** not on io and not on each other. The
  stream below a machine may be a socket, a pipe or TLS, and the machine
  cannot tell which.
- **The stack is static.** The connection holds one state per machine and,
  within its step, routes each event from a machine to the one above it
  and each request the other way. The order is the code: no pipeline type,
  no trait, no `dyn`.
- **Each machine pulls from the one below only when it has room to push
  up,** so a reader that stops demanding stops the peer, through every
  machine (section 6).
- **Machines keep no timers.** Each says what it is waiting for (a head, a
  body, room); the connection that stacks them arms their deadlines from
  that, in one place (4.4, section 8).

## 4. Entities, handles and lifecycle

### 4.1 Handles

A handle is typed, opaque, and never reused for another entity: a slot
index plus a generation, typed by the entity it names.

```rust
conns.insert(conn)  -> Result<Id<Conn>, Conn>   // Err hands the value back: full, so refuse
conns.get_mut(id)   -> Option<&mut Conn>        // None if the slot holds another entity now
conns.retire(id)                                // freed at the reclaim point (4.2)
```

- **One handle type per entity kind.** `Id<Conn>` and `Id<Session>` are
  different types, so passing one where the other is expected does not
  compile. Never a bare integer.
- **Lookup checks the generation** and returns an `Option`: a completion
  for slot 27 generation 12 cannot act on the unrelated entity now in slot
  27 generation 13. A slot whose generation would wrap is retired for good.
- **The reference a lookup returns is borrowed for the current step only.**
  The compiler holds this: no application type has a lifetime parameter,
  so the reference cannot be stored in state, in a record or in a queue.
- **One entity per slab at a time.** `get_mut` borrows the whole slab, so a
  step works on one entity, copies out the handles and values it needs,
  and then looks up the next. The rare step that needs two at once (the
  two halves of a proxy) uses `Slab::get2_mut`, which fails on equal
  handles.
- **Handlers are free functions over the fields they touch,** not `&mut
  self` methods on the whole layer: the borrow checker splits borrows
  across fields at a call site, but not through a method that takes all of
  them.
- **Long-lived state holds handles, not references to other entities.** It
  is then snapshottable and comparable, and it can neither dangle nor keep
  a closed entity alive.

### 4.2 Lifecycle

Every entity is *active*, then *closing*, then *closed*, at every level:
operations in io, connections and requests in protocol, sessions and
exchanges in the model.

- **Close is a request going down; closed is an event coming up.** An
  entity is never retired on *end of stream*: the kernel may still hold
  its buffers.
- **Retire only when no bindings remain;** each binding ends with a closed
  (or detached) event from below. `slab.retire(id)` marks the slot, and the
  slab frees it at the reclaim point. Freeing is therefore bottom-up.
- **Nothing is reclaimed mid-iteration.** An entity that reaches *closed*
  is retired, not removed; slabs free retired slots at the reclaim point
  at the end of the iteration, so every entity present when an iteration
  begins can still be looked up until it ends.
- **Every request gets exactly one terminal event:** success, failure,
  cancelled or timed out, whatever happened below.
- **Stale-handle asymmetry.** A token travelling up is never stale, so a
  stale one is a bug and an assertion (`expect`). A handle travelling down
  may be stale (a reply to a client that just left), so a failed lookup is
  a silent drop.
- **Ownership forms a tree** (server owns connections, a connection owns
  its requests, a request owns its exchanges); every other link is a
  handle. The owner closes what it owns. Ownership is a property of
  states: it is written down per state and moves only in transitions. In
  Rust terms a layer owns its slabs, a slab owns its entities and an entity
  owns its bytes; the model's tree sits on top, and its links are handles.
- **Lifetime is the lifecycle.** An entity ends when it is reclaimed after
  *closed*, never earlier and never later. Rust's ownership decides when
  memory is freed, when a `Box` is dropped or a slot reclaimed, never when
  an entity ends; and with no `Drop` impls, freeing memory has no other
  effect.

### 4.3 Races

A request with a deadline has two competing outcomes. If the timer wins,
the operation is still in flight, cancelling it is asynchronous, and a
late event always arrives (sometimes a completion, when the cancel lost in
the kernel). The lowest layer that knows both competitors runs the race:
it reports the winner upward at once, keeps its entity in a *settling*
state until the loser's terminal event has arrived, then retires it.
Layers above see one event.

### 4.4 State machines

- **States are explicit:** an enum per machine, each variant holding
  exactly what that state holds (bytes, handles, a `ReplyTo`). A state and
  what it holds are one value; no field is meaningful in some states only.
- **The table is total.** Every state × event cell is a transition, an
  ignore, or impossible. "Impossible" is only for cells the loop's own
  rules make unreachable (bytes delivered to a state with no read demand),
  written `unreachable!("why")`; anything a peer can cause is handled.
- **Matches over states and events are exhaustive,** with no `_` arm, no
  `matches!` and no `if let`, and no `#[non_exhaustive]` on the enums, so a
  new state or event is a build error at every site. Match on one enum,
  then on the other inside each arm, never on a tuple of both: a `_`
  inside a tuple pattern escapes clippy's wildcard lint.
- **One small handler per cell:** a free function that takes the source
  state's data by value and returns the target state. Small handlers are
  easier to test, fuzz and review, and function coverage of the handlers
  is transition coverage (section 10).

```rust
enum ConnState {
    Header,                        // demand: fill HEADER_LEN; progress deadline runs
    Body { header: Header },       // demand: fill header.body_len; progress deadline runs
    Waiting { correlation: u32 },  // one request in flight: no demand, no deadline
    Closing,                       // close requested, waiting for closed
    Closed,                        // terminal: holds nothing
}
```

Every transition uses the same idiom: move the state out, leaving the
terminal state in its place, and assign the result of the match back.

```rust
let state = mem::replace(&mut conn.state, ConnState::Closed);
conn.state = match state {
    ConnState::Header => header_read(&bytes, env, conn.socket, down),
    ConnState::Body { header } => body_read(header, bytes, id, up),
    ConnState::Waiting { .. } | ConnState::Closing | ConnState::Closed => {
        unreachable!("bytes delivered without a read demand")
    }
};
follow_state(conn, id, env, timers, down);  // demand and progress deadline, in one place
```

- The placeholder is the terminal state, which holds nothing, so putting it
  in costs nothing. The match is the right-hand side of the assignment, so
  no cell can forget to produce a target state. A panic in between is
  fail-stop, so the placeholder is never observed.
- The handler owns the source state's data, so everything the source held
  is visibly moved into the target, moved out in a request, or dropped
  there. That is "a transition releases whatever the source state held
  and the target does not", with the compiler pointing at every case.
  Dropping is right for bytes; for a handle to something that must be
  closed, the handler requests the close, and the simulator checks that
  nothing is left open.
- What a state implies (its read demand, whether the progress deadline
  runs) is an exhaustive function of the state, applied in one place after
  every transition, not repeated in every handler.

## 5. Memory and data

### 5.1 The strategy: counted entities, owned bytes

- **Entities live in slabs sized at startup.** Every entity kind has a
  `lib::Slab<T>` whose capacity comes from the limits and never changes. A
  full slab is a refusal at that layer's entrance, which is where refusals
  belong anyway (section 6). Handles are slot index plus generation.
- **Queues and tables are bounded at startup:** every `Queue`, ready list and
  deadline table has a capacity from the limits and refuses past it. Slabs
  and queues allocate that capacity up front; a table may allocate as it
  fills (`lib::Deadlines` is a pair of B-trees), and its worst case counts
  its container overhead, not just its entries.
- **Bytes live on the heap as `Box<[u8]>`,** allocated at their final
  length after the length has been checked against the limits. A
  `Box<[u8]>` cannot grow, has one owner, and keeps its address when it is
  moved. Bytes are moved from owner to owner, never shared.
- **Domain storage is ordinary owned data under domain limits:** a
  `BTreeMap<Box<[u8]>, Box<[u8]>>`, say, with the model counting entries
  and bytes and answering "full" as a domain result.
- **Running out of heap aborts.** Allocation failure is never a status.
  Short of a bug it cannot happen, because the worst case is computed and
  checked at startup (5.4).

### 5.2 Why this strategy

Every entity's bytes are already capped by protocol limits: the largest
message, the unparsed input, the queued output. Capping how many entities
exist therefore caps the bytes, without pooling them. Counts are where
fixed budgets are cheap and useful; bytes are where they are expensive.

Against budgets fixed at startup for bytes as well (byte pools):

- **No mutable state shared between layers.** A payload moves from io to
  protocol to model as a `Box`: a move the compiler checks, no copy, and
  every layer's state stays private. With byte pools, either every step
  gets `&mut` to one shared pool, or each layer has its own and copies at
  every boundary.
- **Exact sizes.** A 100-byte message takes 100 bytes, not a slot of a size
  class, and there are no size classes to tune.
- **Fewer cells.** Making a payload cannot fail, so there is no "pool
  empty" transition at every point that makes one; refusals happen at the
  entrances only.

Against the language's heap for everything:

- **The admission check is the container.** A full slab is the refusal;
  there is no separate counter to keep in step with the entities.
- **Handles come with the slab:** a generation check and constant-time
  lookup, instead of a map from id to entity.
- **Entity tables never grow,** so nothing reallocates under load.
- **Exhaustion is testable:** a simulation with a capacity of 2 reaches
  every admission point.

Against an accounted heap: it is the same for counts; for bytes it relies
on per-entity caps instead of quotas, and keeps accounting as the fallback
(5.4).

What it gives up: running out of heap aborts rather than refuses, so the
worst case must fit (5.4); and the general allocator sits in the hot path,
so allocation time is not constant, and fragmentation can push the
resident size above the live bytes.

### 5.3 What holds

- **Memory the kernel touches moves with the operation.** A buffer the
  kernel will read or write is a `Box` inside the operation record. io
  moves it down, the backend holds the record until the operation
  completes, and the completion moves it back up. Moving a `Box` moves the
  pointer, not the bytes, so the address the kernel holds stays valid.
  Kernel structures (socket addresses, `statx`, `open_how`) belong to the
  backend and never reach io. The mechanism is overview.md, section 6.

  ```
  owned by the stream --moved into the op--> held by the backend, kernel holds its address --completion--> moved back: freed, reused or moved up
  ```

  The compiler checks io's side: once the `Box` is in the record, no other
  code can name it. The backend's `unsafe` covers the kernel's side: it
  takes addresses only from records it holds, and hands a record back only
  after its completion.
- **The kernel never holds model memory.** Only transit memory moved into
  an operation is referenced by a submission. Model state is always safe to
  mutate, evict or snapshot.
- **Layers share nothing by reference; a move is the copy.** Records are
  owned values. A payload that crosses a boundary is moved into the
  record, and the emitter cannot reach it afterwards, so the move gives
  what the agnostic model gets from copying, at no cost. A body io reads
  is moved into the protocol's message, then into the model's `Call`, then
  into the model's store, and is copied nowhere on the way.
- **Copy at emission.** Data the model keeps and also sends goes out as a
  copy made when the reply is emitted (`lib::bytes::copy_of(&stored)`),
  because a later event in the same batch may change the stored value
  before the down pass runs. Do not reach for `Rc` or `Arc` to save the
  copy.
- **No borrow outlives a step.** No application type has a lifetime
  parameter (4.1).
- **Validate before allocating.** A length from a peer is checked against
  the limits before a buffer is allocated for the data it announces. Every
  `Box<[u8]>` is made by the side below for a demand the side above
  validated, by `Writer::new(len)` for a length the code computed itself,
  or by `copy_of`.

### 5.4 The worst case

```
worst case =   Σ over entity kinds:  slab + capacity × the bytes each entity may hold
             + queues and tables:    their containers
             + operations in flight: the backend's table and the buffers they carry
             + the model's stored-data limit
```

Every layer and every machine exports `fn worst_case(limits: &Limits) ->
Option<u64>` (checked arithmetic, `None` on overflow), and the shell
refuses to start when the sum exceeds the configured memory. An
allocation failure is then a bug or fragmentation, never load. A layer
adds up what its containers report, not `size_of` times a capacity: each
lib container has its own `worst_case(capacity)` (`Slab`, `Queue`, `List`,
`Map`, `Set`, `Stack`, `Deadlines`, `Intake`), which counts its bookkeeping too: a
slab's slot tags and free lists, the tree nodes of a map, a set or a
deadline table. The formula counts containers and payload bytes, not
allocator overhead: leave headroom, and measure the resident size under
load before trusting it. The simulator checks the formula at every
iteration (section 10).

If the worst case forces the limits too low for a service, the fallback is
a byte budget for the large consumers, each kept in the layer that owns
them. io grants output *room* only within a global queued-output budget,
so pressure turns into the ordinary backpressure chain (section 6) rather
than a new kind of refusal; the model already answers "full" against its
stored-data limit. Do not add the budget before the worst case demands
it.

### 5.5 Rejected

- **`Rc<RefCell<T>>`, `Arc<Mutex<T>>`.** Reachability decides lifetime,
  which 4.2 forbids, and borrow errors move from compile time to runtime
  panics.
- **References in long-lived state, arenas with lifetimes** (`bumpalo`,
  `typed-arena`). Lifetime parameters spread to every type that touches
  them, and state stops being a plain value that can be snapshotted and
  compared. Slabs and boxes need none.
- **Byte pools fixed at startup.** See 5.2. Reconsider only if the
  allocator is measured to be the problem.
- **Custom allocators per layer.** The allocator API is unstable, and
  everything builds on stable Rust.
- **Shared immutable buffers (`Arc<[u8]>`) as the ownership system.** At
  most an optimisation, confined to io.

### 5.6 Deferred optimisations

Optimisations go below the records, where code above io cannot see them:
exact-size reads, provided-buffer rings, registered buffers, kernel TLS
and the rest are listed in overview.md, 7.1. Each must keep the contract
of overview.md, section 6, and is measured before it goes in.

## 6. Flow control, admission and backpressure

**Limits are configured.** Each layer and machine defines a `Limits`
struct; the shell's configuration record holds one per layer and hands
each to its layer as `&Env<Limits>`, so a simulation with tiny limits
means something. The limits are also the inputs of the worst case (5.4),
and slab capacities are the admission limits. The flow-control limits are:
maximum message size, cap on queued output, cap on unparsed input,
concurrent streams, accept batch, progress chunk and timeout.

**Refuse at the entrance, never in the middle.** Whatever the service
limits, it checks at the last point where saying no has no consequences:
accept for connections, request start for requests. A request refused
there leaves nothing half-done; a request abandoned in the middle leaves
partial state behind and a peer with half an answer.

**Each layer refuses at its own entrance.** An accepted socket the
protocol layer has no slot for is rejected; a request the service cannot
take on gets a busy response; a store that is full answers "full" as a
domain result. A flood of connections waits in the kernel's backlog, since
io accepts only while the listener's owner has room.

**One request in flight per connection** beyond the protocol layer: the
next is parsed only after the previous response is queued. For multiplexed
protocols the unit is the stream, with a cap on concurrent streams.

**The backpressure chain,** for a client that sends faster than it reads:

1. The connection's queued output has a cap; sends do not complete, so it
   stays full.
2. The protocol layer asks for output room (queued output plus one
   worst-case response within the cap) **before parsing the next
   request**, and otherwise leaves input unparsed.
3. Unparsed input grows to its cap in the intake below, through every
   machine of the stack; then io stops receiving.
4. The socket buffer fills, the TCP window closes, the peer blocks.

Release runs the other way. Without the cap in step 1, one pipelining peer
that never reads turns the service's memory into unbounded queued output,
and the worst case of 5.4 is no longer a bound.

**Full duplex.** The output check gates the *next* request only. In a
relay the two directions have independent credit, or two peers that both
write before reading deadlock through it. Credit is a grant-and-consume
message; where two connections are coupled (a proxy), it flows through the
model, since only the model knows they are related.

**Progress deadlines.** A peer that holds resources without making
progress is closed. Progress is counted in chunks, by the peer: a read
demand met, a chunk of queued output drained. The service's own writes do
not count, or a peer that never reads is kept alive by the responses it
provokes; single bytes do not count, or a trickling peer lives forever.
The chunk size is therefore the minimum rate below which a peer is cut
off; many peers holding resources at exactly that rate are a matter for
per-peer limits. Whether the deadline runs is an exhaustive function of
the state, and one place arms and re-arms it (4.4), not every handler.

## 7. Protocols

- **Any protocol we design is sized:** a fixed header carrying every
  length. Parsing a header is a fixed-size decode with a `lib::Reader` into
  a plain struct; lengths are validated against the limits *before*
  anything is set aside for the body; the machine is header, body,
  dispatch.
- **Encoding is sized too:** compute the message's length,
  `Writer::new(len)`, write the fields, `finish()`. The length is the
  code's own, so the caller expects every write to fit (a write past
  the end is refused whole, writing nothing), and finishing short is an
  assertion.
- **Lengths are never trusted by construction.** No `[]` indexing and no
  `as` on anything a peer sent: the `Reader` returns `Option`, a narrowing
  is `u32::try_from`, and a failure is a framing error, not a panic.
- **Scanned framing** (delimiters, HTTP/1 heads, line protocols) is only
  for foreign protocols: a "scan to a delimiter, at most M bytes" demand,
  with the carry-over held by the side below (3.3).
- **One request per delivery.** Not a loop that parses while enough bytes
  are buffered: one in flight, and flow control assumes it.
- **Nested formats use an explicit bounded stack** (`lib::Stack`), never
  recursion: the nesting depth is the peer's choice. JSON and friends will
  tempt recursive descent.
- **Unknown opcode with valid lengths:** refuse it with a status and skip
  the body, so an older server survives a newer client. Bad lengths are a
  framing error and close the connection.
- **Refusals are small and fixed-size,** so a service under pressure can
  still say no.

## 8. Time, timers and randomness

- **Time is read once per iteration** by the shell and passed to every
  step through `Env`, the same for every step in the iteration:
  - `env.now`, monotonic (`CLOCK_MONOTONIC`), a `lib::Time` (nanoseconds
    in a `u64`), for every deadline;
  - wall time, for things about the world: a certificate's validity, a
    timestamp a peer will read. It never arms a deadline.

  Spans are `lib::Duration`. `std::time::Instant` is not used: it is
  opaque, and a simulator cannot make one.
- **Each layer owns its deadline table** (`lib::Deadlines`, in the
  layer's state) for its own concerns: close deadlines in io; idle,
  handshake and call deadlines in protocol, armed by the connection for
  the machines it stacks (3.4); backoff and session expiry in the model.
  The expiry goes to the layer that armed it because no other layer holds
  the table.
- **Timers are not kernel operations.** The shell waits for completions
  with one timeout: the earliest deadline over all the layers. One kernel
  operation per timer would make resetting an idle timeout on every read
  cost a cancel, a re-arm, two completions and a settling state.
- **Arm and cancel are synchronous calls on the layer's own table.** A
  stage fires its expired timers at one point, after its input events, so
  progress that arrived in the same iteration wins over a deadline that
  passed while the loop waited. Firing removes the timer before its
  handler runs, so a timer ends with exactly one of *fired* or
  *cancelled*, and timers never need settling. Cancelling a timer that has
  already fired is a stale handle, dropped silently.
- **Randomness comes from injected state:** a `lib::Rng` in each layer's
  state, seeded by the shell from `getrandom`, or by the simulator, which
  then owns both the clock and the random state.

## 9. The Rust subset

### 9.1 What enforces each rule

| Rule | How Rust holds it | Checked by |
|---|---|---|
| Steps make no syscalls, read no clock, spawn nothing, print nothing | step crates are `no_std`; those APIs do not exist there | compiler |
| No global or thread-local state | `static mut` needs `unsafe`; `thread_local!` is std; atomics and cells are disallowed | compiler, clippy |
| Output does not depend on hash seeds or addresses | no `HashMap` in `alloc`; no formatting | compiler, clippy |
| The model never sees an fd or a kernel error | the model crate does not depend on io | compiler |
| A machine cannot tell what stream is below it | machines depend on lib only | compiler |
| A step touches only its own layer | it receives `&mut` to its own state and nothing else | compiler |
| Configuration is read-only | `&Env<Limits>` | compiler |
| No borrow outlives a step; records hold no references | no lifetime parameters on application types | review (visible syntax) |
| Layers share nothing by reference | payloads are owned and moved | compiler |
| A buffer the kernel holds is touched by nothing else | the `Box` is moved into the operation record | compiler; the backend's `unsafe` contract |
| A reply is sent at most once | `ReplyTo` is not `Copy` or `Clone`; replying consumes it | compiler |
| A reply is sent at least once; one terminal event per request | | simulator |
| Handles are typed | `Id<T>` | compiler |
| A stale handle is detected | the generation | runtime |
| A state and what it holds are one value | enums with data | compiler |
| Matches are exhaustive, with no catch-all | `match`; `wildcard_enum_match_arm`; `matches!` disallowed | compiler, clippy; review for `if let` and tuples |
| Statuses are not ignored | `#[must_use]`, `unused_must_use`, `let_underscore_must_use` | compiler, clippy |
| Arithmetic and narrowing are checked | `arithmetic_side_effects`, `as_conversions`; `overflow-checks` traps the rest | clippy, runtime |
| Bytes go through bounds-checked cursors | `indexing_slicing`; `Reader`, `Writer` | clippy |
| Fail-stop | `panic = "abort"` | build profile |
| Nothing closes or frees in a destructor | no `Drop` impls; no `OwnedFd`, `File`, `TcpStream` | review |
| `unsafe` is confined | `forbid(unsafe_code)` everywhere but the ring adapter | compiler |
| Execution is bounded | no `loop` or `while` in step code; no recursion | review |
| State is plain | no closures, function pointers or `dyn` | review |
| No leaks; ownership is a tree | | simulator |

### 9.2 What is in

Step code (skein's lib, io and protocol machines; a service's protocol
layer, model and `iterate`) uses:

- `struct` with named fields; tuple structs only as one-field newtypes
  (`Fd(i32)`).
- `enum` with data, for states, events, requests, statuses and errors.
- `match`, exhaustive; `if` and `else`; `if let` and `let … else` on
  `Option` and `Result` only.
- `for` over a range, a slice or a lib container.
- Free functions, inherent `impl` blocks, `const` items, modules.
- `&` and `&mut` in parameters, locals and return values, with elided
  lifetimes, written `'_` where a type borrows (`Reader<'_>`).
- Moves; `Copy` for handles, tokens and small plain values.
- `?` on `Option` and `Result`, with the same error type on both sides (no
  `From` conversions).
- Integers, `bool`, arrays, slices, `Option`, `Result`, `Box`, `BTreeMap`,
  `BTreeSet`, `core::mem::replace`.
- `#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]`
  and no other derive. Entity state does not derive `Clone`: an entity
  exists once.
- `assert!`, `unreachable!("why")`, `expect("the invariant relied on")`.
- From lib: `Id<T>`, `Slab<T>`, `Token`, `ReplyTo`, `Queue<T>`,
  `List<T>`, `Map<K, V>` and `Set<K>` (bounded and ordered, over
  B-trees), `Stack<T>`, `stream`, `Intake`, `Reader`, `Writer` (sized),
  `Decimal` (the digits of a count, for text protocols), `bytes::copy_of`, the byte search `bytes::find`, `find_from` and
  `count` (linear time, no allocation), `Deadlines`, `Time`, `Duration`,
  `Wall` (wall-clock time, never a deadline), `Rng`, `Env<L>`.

lib uses the same, plus generic types, lifetime parameters on its cursors,
hand-written impls of std traits for its own types, `Vec` inside its
containers, and `while` loops bounded by a container's capacity. It is
written once and tested hard. Application code does not hand-roll data
structures: what is missing goes into skein-lib.

The shell, the simulator and tests are ordinary Rust, within the habits of
section 11, and with two rules of their own:

- **`unsafe` lives in the ring adapter only** (overview.md, 7.1). Its
  dependencies, `io-uring` and `libc`, are with rustls the only ones from
  outside the workspace.
- **No std handle types that close on drop.** `OwnedFd`, `File`,
  `TcpStream`, `TcpListener` and `std::process::Child` release kernel
  resources in their destructors, at a moment the lifecycle did not
  choose. A file descriptor is a plain `Fd(i32)`, closed by a close
  operation. `std::process` is not used (overview.md, 5.3).

### 9.3 What is out

| Out of step code | Why | Checked by |
|---|---|---|
| `std` | syscalls, clock, threads, printing, `HashMap` | compiler (`no_std`) |
| `unsafe` | one audited place: the ring adapter | compiler (`forbid(unsafe_code)`) |
| `async`, `.await` | the loop schedules; state hides in generated futures, neither total nor snapshottable | review |
| closures, function pointers | captured state, and calls the reader cannot follow | review |
| `dyn Trait`, `impl Trait` | dynamic dispatch, hidden types | review; clippy `impl_trait_in_params` |
| defining traits, generic items, hand-written trait impls | one way to call code, on concrete types | review |
| lifetime parameters on types, references in fields | a stored borrow; state stops being a plain value | review |
| `Rc`, `Arc`, `Cell`, `RefCell`, atomics | shared ownership, interior mutability, global state | clippy `disallowed_types` |
| `static`, `thread_local!` | global state | review; compiler (`no_std`) |
| `Vec`, `VecDeque`, `String`, `vec!`, `format!` | growth without a check; formatting | clippy `disallowed_types`, `disallowed_macros` |
| `impl Drop`, `Deref`, operator overloading, `Index` | hidden calls and hidden effects | review |
| `macro_rules!`, procedural macros beyond the derives | code the reader cannot see | review |
| `loop`, `while`, recursion | unbounded execution | review |
| a `_` arm on an own enum, `matches!` | a new case builds silently | clippy `wildcard_enum_match_arm`, `disallowed_macros` |
| `if let` on an own enum, a tuple as scrutinee | the same, out of the lint's sight | review |
| `#[non_exhaustive]` on own types | forces a `_` arm | review |
| `as` casts | silent truncation | clippy `as_conversions` |
| unchecked `+ - * /` on integers | a silent wrap, or a panic a peer can trigger | clippy `arithmetic_side_effects` |
| `[]` indexing and slicing | a panic a peer can trigger | clippy `indexing_slicing` |
| floats | no total order, nothing in this domain needs them | clippy `float_arithmetic` |
| `unwrap()` | an assertion without its reason | clippy `unwrap_used` |
| `let _ =` on a `#[must_use]` value | a silently ignored status | clippy `let_underscore_must_use` |
| `mem::forget`, `todo!`, `unimplemented!` | a leak; unfinished cells | clippy |

Integers go through `checked_*`, or through `wrapping_*` and
`saturating_*` where that is the intent (generations wrap on purpose).
`overflow-checks = true` in every profile traps whatever the lint does not
see, in lib and the shell.

### 9.4 Configuration

skein's workspace holds the reference copy of this configuration; a
service's workspace copies it.

```toml
# Cargo.toml (workspace)
[profile.dev]
panic = "abort"
overflow-checks = true

[profile.release]
panic = "abort"
overflow-checks = true

[workspace.lints.rust]
unsafe_code = "deny"                  # forbid in every crate root but the shell's
elided_lifetimes_in_paths = "deny"    # a type that borrows says so: Reader<'_>
unused_must_use = "deny"

[workspace.lints.clippy]
wildcard_enum_match_arm = "deny"
indexing_slicing = "deny"
arithmetic_side_effects = "deny"
as_conversions = "deny"
float_arithmetic = "deny"
unwrap_used = "deny"
let_underscore_must_use = "deny"
mem_forget = "deny"
todo = "deny"
unimplemented = "deny"
impl_trait_in_params = "deny"
allow_attributes_without_reason = "deny"
undocumented_unsafe_blocks = "deny"
multiple_unsafe_ops_per_block = "deny"
disallowed_types = "deny"
disallowed_macros = "deny"
```

```toml
# clippy.toml (workspace root: applies to the step crates)
disallowed-types = [
  { path = "alloc::vec::Vec",              reason = "Box<[u8]> for bytes, lib::Queue for sequences" },
  { path = "alloc::collections::VecDeque", reason = "lib::Queue" },
  { path = "alloc::string::String",        reason = "bytes are Box<[u8]>" },
  { path = "alloc::rc::Rc",                reason = "one owner" },
  { path = "alloc::sync::Arc",             reason = "one owner" },
  { path = "core::cell::Cell",             reason = "no interior mutability" },
  { path = "core::cell::RefCell",          reason = "no interior mutability" },
  { path = "core::sync::atomic::AtomicU64", reason = "no global state" },
  # ... and every other atomic type
]
disallowed-macros = [
  { path = "alloc::format", reason = "no formatting in step code" },
  { path = "alloc::vec",    reason = "no Vec in step code" },
  { path = "core::matches", reason = "a catch-all match in disguise" },
]
```

- Each step crate's root starts with `#![cfg_attr(not(test), no_std)]`
  (unit tests get std) and `#![forbid(unsafe_code)]`, and lib's does the
  same. The shell allows `unsafe` in its ring adapter module only, with
  the reason stated.
- lib is held to the root `clippy.toml` too. Where it builds a container
  on a type that is out (`Vec`, `VecDeque`, `PhantomData`), the module
  says so with a scoped `#[expect(clippy::disallowed_types, reason =
  "...")]`, which fails the build once it is stale. The shell and the
  simulator are ordinary Rust, with a `clippy.toml` of their own.
- Warnings are errors: `cargo clippy --all-targets -- -D warnings` in CI.
- Stable Rust, edition 2024. Nightly is used only for the fuzz targets.

### 9.5 Discipline

- **Errors are values.** Expected outcomes (busy, full, not found,
  refused) are enum variants, never panics. Every fallible operation
  returns a `Result`, an `Option` or a `#[must_use]` status.
- **One side effect per statement.**
- **Assertions are fail-stop** and are for the code's own bugs, never
  for inputs a peer controls. `expect` names the invariant it relies on.
- **Runtime checks stay on in production:** bounds checks, overflow
  checks, assertions. Safe Rust has no undefined behaviour left to trap;
  the ring adapter, the one place that could have some, is tested against
  the real kernel (overview.md, section 9).

## 10. Testing

What each kind of world is, and how a service arranges its own, is in
consumers.md, section 8; the simulator itself is overview.md, section 9.
These rules hold for all of them:

- **Deterministic simulation.** The simulator owns the clock, the seeds
  and the kernel, and drives the same `iterate` the shell runs. It runs
  with tiny limits (slabs of capacity 2) and injects cancellation and
  timeout in every state, completions after cancel, refusal at every
  admission point, and short reads and short writes.
- **Universal invariants,** checked by every world and the simulator: no
  live entities at quiescence (every slab empty); ownership is a tree with
  no orphans; one terminal event per request; every `ReplyTo` answered.
- **Transition coverage.** Each cell is a handler function, so function
  coverage of the handlers (`cargo llvm-cov` over a simulation run) is the
  list of transitions exercised and of those never reached.
- **Memory.** A counting `#[global_allocator]` records the peak of live
  heap bytes, and every iteration asserts that it stays within the worst
  case of 5.4.
- **Fuzzing** each protocol machine alone with `cargo fuzz`, feeding
  `Bytes` under every demand, and each step function with recorded event
  sequences.
- **Replay:** a recorded run replays to the same state. State types derive
  `Hash`, and lib's fixed-key hasher gives a field-wise digest of the
  logical state, independent of its layout in memory.

## 11. Habits to avoid

| Habit | Why it is wrong | Instead |
|---|---|---|
| An `async fn` handler, a runtime | the runtime schedules instead of the loop; state hides in generated futures | an enum state machine driven by the loop |
| Registering a `Box<dyn FnMut>` with the loop | hidden control flow, captured state | events in, requests out |
| `Rc<RefCell<T>>` to share an entity | reachability decides lifetime; borrow errors become panics | one owner; everyone else holds an `Id<T>` |
| A struct with a lifetime parameter to hold `&Conn` | a stored borrow drags lifetimes through every type | store the `Id<T>`; look it up when needed |
| `&mut self` methods on the whole layer | the borrow checker cannot split the borrow; `clone` or `RefCell` follow | free functions over the fields they touch |
| `.clone()` on entity state to calm the borrow checker | now the entity exists twice | copy the handle out, end the borrow, look it up again |
| `Instant::now()` or `rand` in a step | not replayable | `env.now`, the layer's `Rng` |
| `HashMap` | iteration order depends on a random seed | `BTreeMap` |
| `unwrap()` or `[]` on something a peer sent | a remote crash | `Reader`, `try_from`, a framing error |
| `as u32` on a length | silent truncation | `u32::try_from`, and refuse |
| `impl Drop` that closes or frees | a hidden effect outside the lifecycle | request *close*; reclaim on *closed* |
| `OwnedFd`, `File`, `TcpStream` in io | closes on drop, outside the lifecycle | an `Fd(i32)`, closed by a close operation |
| Retiring a connection on end of stream | *closed* has not arrived; the kernel may hold its buffers | go to *closing*; retire on *closed* |
| A `_` arm, `matches!` or `if let` over states or events | a new case builds silently | an exhaustive `match` |
| `match (state, event)` | a `_` in a tuple escapes the lint | match one, then the other |
| A placeholder that is a live state in `mem::replace` | a forgotten assignment leaves a plausible state | the terminal state, with the match as the right-hand side |
| Queuing output without a cap | a peer that never reads grows it without bound | cap queued output; check room before parsing |
| Parsing every request available in one loop | one in flight; flow control overshoots | one request per delivery |
| Recursive descent on peer input | the peer chooses the depth; a remote crash | `lib::Stack` |
| `Vec::push` while reading a body | the peer chooses the size | validate the length, then demand exactly it |
| Sharing stored data with a reply through `Arc` | the kernel would hold model memory | copy at emission |
| Asserting on input a peer can send | a remote crash | handle the cell |
| Re-arming the idle timeout by hand in every handler | one forgotten re-arm closes a connection mid-body | derive the deadline from the state, in one place |
| A timer inside a protocol machine | deadlines scattered across the stack | the machine says what it waits for; the connection arms it |
| One kernel timeout per timer | cancel races, settling, two completions per reset | the layer's `Deadlines` |

## 12. Departures and open questions

Where this document departs from the agnostic model:

1. **Timers live in each layer,** not in io (section 8). Arm and cancel
   never cross a boundary, and the expiry reaches the layer that armed it
   without routing; in Rust, io could not name the model's queue anyway.
   io's vocabulary loses *arm timer* and *cancel timer*.
2. **The side below delivers the demanded bytes in the event,** as an
   owned `Box<[u8]>`, instead of the side above reading its buffers
   through a cursor in place (3.3). Every boundary record is then
   self-contained, and exact-size reads later become a move.
3. **A move replaces the copy at every boundary** (5.3). Copy at emission
   still applies to data the model keeps.
4. **A step returns nothing** (section 2). Each entry point declares
   `MAX_OUT` and the loop reserves the room, so there is no status left to
   return.
5. **The random generator lives in each layer's state,** and the limits
   reach the step read-only through `&Env<Limits>`.
6. **One opaque `Token` crosses every boundary** (3.1), and reply tokens
   are affine (`ReplyTo`).
7. **The memory strategy is chosen:** counted entities and owned bytes,
   with the worst case checked at startup (section 5).

Open questions:

- **Affine owner handles.** An `Owned<T>`, neither `Copy` nor `Clone`,
  returned by `insert` and consumed by `retire`, would let only an
  entity's owner end it, checked by the compiler. It is awkward for
  top-level entities (who holds the `Owned<Conn>` of an accepted
  connection?). Decide after the first service.
- **A checker for the review-only rules.** No `async`, closures, `loop` or
  `while`, recursion, tuple scrutinees, or trait and generic definitions
  in step code. Without closures, `dyn` or traits in step code every call
  is static, so a small `syn`-based check could find recursion too. Write
  it if review proves not to be enough.
- **Kinds in tokens.** If decoding a token as the wrong kind turns out to
  be a real mistake, give `Token` a kind tag that `from_token` checks.
- **The byte budget** of 5.4, for a service whose worst case is too large
  to provision.
