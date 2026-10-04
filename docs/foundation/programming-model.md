# Programming model (Rust)

Provisional, 2026-10-03. How code is written in skein and in every
service built on it. Read this first, before writing or reviewing any of
it.

**How to read this.** Section 1 is the whole model in one page. The
sections after it take each part in turn: the loop and step functions, the
layers, entities and their lifecycle, memory, flow control, protocols, and
time; then the subset of Rust the model is written in (section 10). The
subset is deliberately small: Rust is used for ownership and borrow
checking, enums with data and exhaustive matching, and crate boundaries,
and for little else. Where the compiler or clippy can check a rule, the
check is named; where neither can, the rule is a convention held in
review. Sections 10.1 and 10.3 say which is which. How code is tested
is testing-strategy.md.

## 1. In one page

- **One thread, one loop, no async.** The loop reaps completions from
  io_uring, runs the step functions, and submits what they asked for. It is
  the only code that talks to the kernel, and the only thing that schedules
  work: no `async`, no futures, no runtime, no callbacks.
- **Three layers, and the crate graph enforces them.** `io` (kernel
  operations and their buffers), `protocol` (bytes to typed messages and
  back), `domain` (the service's own logic). A layer may be several
  crates: a domain with child domains (4.5), a crate per protocol machine.
  The domain's crates do not depend on io, so they cannot name a file
  descriptor.
- **The domain is complete.** It runs the service's whole behaviour in a
  world of fakes, with no protocol and no io. The protocol layer
  translates between bytes and domain entities, and decides nothing about
  the domain.
- **Step functions are sans-io, and the compiler knows it.** Step crates
  are `#![no_std]` with `alloc`: no syscalls, no clock, no threads, no
  printing, no hash maps with random seeds. Time and randomness are inputs;
  effects are outputs. The same state and input give the same output.
  TLS is the one exception (section 3).
- **Entities are named, not referenced.** Each entity lives in a slab owned
  by its layer and is named by a handle that is never reused for another
  entity: a typed `Id<T>` within its layer, an opaque token across layers
  (4.2). No application type has a lifetime parameter, so no reference can
  be stored and no borrow outlives a step.
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
  `unsafe` outside one module (in tests, also the counting allocator's:
  skein's testing.md, section 6). Panics abort.

## 2. The loop

```rust
// shell: the only impure code
loop {
    ring.reap(&mut svc.completions);         // completions and effect results
    let (now, wall) = clock.now();           // read once: the times for the iteration
    service::iterate(&mut svc, now, wall);   // pure: both passes, the reclaim point
    let wait = !svc.work_pending();          // block only when there is nothing to do
    ring.submit(&mut svc, wait);
}
```

```rust
// service::iterate: pure, and the same function the simulator drives

// up pass
for c  in take(completions) { io::up(&mut io, &io_env, c, &mut proto_in, &mut subs) }
for ev in take(proto_in)    { protocol::up(&mut proto, &proto_env, ev, &mut domain_in, &mut proto_out) }
for ev in take(domain_in)   { domain::step(&mut domain, &domain_env, ev, &mut domain_out) }

// down pass
for rq in take(domain_out)  { protocol::down(&mut proto, &proto_env, rq, &mut proto_out) }
for rq in take(proto_out)   { io::down(&mut io, &io_env, rq, &mut subs) }

// reclaim point
io.reclaim(); proto.reclaim(); domain.reclaim();
```

The sketch shows the main flow only. What it leaves out:

- **Room first.** `take` hands a stage its inputs one at a time while the
  step's output queues have room for the most one input can produce, a
  bound each entry point declares as `MAX_OUT`, and while the layer can
  take one more (io's `takes`, while it can hold a refusal: io.md, 2);
  whatever does not fit waits for the next iteration.
- **The order within a stage.** A stage first drains its layer's ready
  list, then takes its input events, then fires its expired timers
  (section 9).
- **Down from the up pass.** A step in the up pass may also queue requests
  downward (a protocol machine closing on a framing error); they join the
  down pass of the same iteration.
- **Up from the down pass.** A step in the down pass may produce an event
  that must go up, such as a read demand already met by buffered bytes, or
  a request refused without reaching the kernel. That event is held as
  state of the entity that will emit it, not as a queued record, so it
  outlives the iteration's queues: the entity goes on its layer's ready
  list, drained at the start of its stage in the next up pass.

```
completions -> io::up   -> protocol::up   -> domain::step    (up pass)
submissions <- io::down <- protocol::down <- domain::step    (down pass)
```

Properties the rest of this document relies on:

- **Work per iteration is bounded.** Every queue is a `lib::Queue` whose
  capacity is fixed at startup; whatever does not fit waits for the next
  iteration.
- **Requests made in the down pass are submitted at the end of the
  iteration;** the events they cause arrive in a later one. A step never
  waits for its own effect.
- **The loop never blocks while a queue or a ready list is non-empty, or
  an expired timer waits to fire,** so it does all the work it can before
  waiting on the kernel.
- **Nothing is reclaimed mid-iteration.** An entity that reaches *closed*
  is retired, not removed; slabs free retired slots at the reclaim point,
  so every entity present when an iteration begins can still be looked up
  until the iteration ends.

### 2.1 The shell

The shell is the only impure code: the service's loop above, and skein's
kernel, clock and seed. The kernel is the ring adapter: the ring, and a
small closed list of syscalls that are not ring operations (spawning a
process, sending a signal, reading the clock, drawing the seed). Shell
effects use the same request and event shapes as ring operations, so step
code cannot tell them apart. Anything that can be a ring operation is one:
accept, connect, read, write, open, close, waiting for a child.

- **The ring adapter is the only `unsafe` code a service runs,** since it
  makes every kernel call: ring operations through the `io-uring` crate,
  the other syscalls through `libc`. With rustls in the TLS machine, and
  its dependencies, the `ring` crate among them for its cryptography
  (section 3), these are the only crates from outside skein and the
  service; `ring`'s `unsafe` code and the entropy it draws from the kernel
  are that exception's. In
  tests, the only `unsafe` is the counting allocator's `unsafe impl
  GlobalAlloc` (skein's testing.md, section 6).
- **No std handle types that close on drop.** `OwnedFd`, `File`,
  `TcpStream`, `TcpListener` and `std::process::Child` release kernel
  resources in their destructors, at a moment the lifecycle did not
  choose. A file descriptor is a plain `Fd(i32)` inside io, closed by a
  ring `close`. `std::process` is not used. The shell's `clippy.toml`
  bans them all.
- **Panics abort.** `panic = "abort"` in every profile: a panic anywhere is
  fail-stop, and a supervisor restarts the process.

## 3. Step functions

```rust
pub fn step(domain: &mut Domain, env: &Env<Limits>, ev: Event, out: &mut Queue<Request>)
```

```
domain  this layer's state, updated in place: the only state the step changes
env     now, wall and this layer's limits, behind a shared borrow, so read-only
ev      one self-contained input, moved in
out     a bounded queue of owned requests, with MAX_OUT slots reserved by the loop
```

A step returns nothing. Every outcome of an event is a change to the
layer's state or a request in `out`; the loop reserved the room, so
emitting cannot fail, and running out of heap aborts (section 6). Inside a
step, every fallible operation returns a `Result` or an `Option`, and the
caller handles it: the compiler rejects an ignored `Result`, and review
catches an ignored `Option` (section 10).

The protocol layer has two entry points of this shape: `protocol::up` for
events from io and `protocol::down` for requests from the domain. An up
entry point has two output queues, events up and requests down (section
2). io has the same pair, and the domain has one. Anything with state and
entry points of this shape is a step machine: a layer, a protocol
machine, a child domain.

What "pure" means, and what holds it:

- **No syscalls,** directly or through a library: no printing or logging,
  no clock reads, no entropy, no file, socket or process operations, no
  threads. Step crates are `#![no_std]` with `extern crate alloc`, so
  `std::{io, fs, net, time, thread, process, env}`, `println!` and
  `thread_local!` do not exist there, and they depend on nothing but what
  the role graph of section 4 allows.
- **Time is data.** The step reads `env.now` and `env.wall` (section 9); a
  deadline is a value it computes from `env.now` and arms in its own
  layer's table.
- **Randomness is injected state** (section 9).
- **Nothing observable depends on memory.** No output may depend on
  addresses or on allocation order. `alloc` has no `HashMap`; maps and
  sets are lib's `Map` and `Set`, ordered by key. There is no formatting
  in step code, so no address can be printed.
- **No hidden state.** No `static` (a mutable one needs `unsafe`, and
  atomics and cells are disallowed types), no closures carrying state
  between calls; all state is in the arguments.
- **No hidden control flow or effects.** Outcomes are returned; panics are
  fail-stop. There are no `Drop` impls in step code: closing is a
  request, and reclaiming follows *closed* (5.2). Dropping a `Box<[u8]>`
  frees memory and does nothing else.
- **Effects are data.** To send bytes or close a socket, a step pushes a
  request into its output queue. The outcome arrives later as an event.
- **Bounded.** No `loop` or `while` in step code: `for` over a range, a
  slice or a lib container, whose bound is a configured limit or the size
  of an input already validated. No scan over an unbounded structure, no
  recursion.
- **Diagnostics are data too.** Trace records are enum values pushed into a
  bounded queue the shell writes out.

**What rests on it.** The simulator can own the clock, the seeds and the
kernel only because no step reads them itself; a recorded run replays,
and a layer can be fuzzed alone, only because events in and requests out
are all a step has. One impurity, however small, breaks all three.

**The one exception is TLS.** The TLS machine wraps rustls: it is the
only step code that depends on a crate from outside skein and the
service, and the only step code that is not deterministic, since its
cryptography draws entropy from the kernel. Worlds and the simulator
therefore run in plaintext. No other step code gets an exception.

## 4. The three layers

```
role              depends on
lib               nothing                          skein's `skein-lib`
io                lib
protocol machine  lib                              one per format; TLS also on rustls
domain            lib, and its child domains (4.5)
protocol layer    lib, io, its machines, domain    sees both vocabularies
service           lib, io, protocol layer, domain  its `iterate`
shell             io, service, io-uring, libc      skein's kernel and clock; the service's loop
sim               io                               skein's; a service's worlds drive it
```

Each role is one crate or more (a domain with its child domains, a crate
per machine), and the crate graph is the role graph: what a role does not
depend on, it cannot name. skein provides lib, io, the shared machines,
the shell's kernel, clock and seed, and the simulator. A service provides
its protocol layer, domain, `iterate` and the shell's loop, may add
machines of its own, and has worlds: test harnesses that run its layers
against fakes, or its `iterate` on the simulator.

- **Every completion goes through io first.** The domain never sees a raw
  completion, a file descriptor, a kernel error code or a half-filled
  buffer; it cannot, since it does not depend on io.
- **The domain never parses.** Bytes are untrusted until the protocol layer
  has turned them into typed, size-bounded messages. Structure inside a
  payload (JSON in a body) is one more protocol machine, not domain code.
- **The domain is complete.** Everything a peer can cause arrives as a
  domain entity, and everything the domain wants done leaves as one, so
  the domain, in a world of fakes, runs the service's whole behaviour with
  no protocol and no io. The protocol layer only translates between bytes
  and domain entities: it decides nothing about the domain, and it never
  sits between two pieces of domain logic. Structure the domain acts on is
  decoded on the way in, all of it: a JSON document carried inside a
  message's field reaches the domain as typed values, not as text to be
  sent back down for decoding later.
- **Policy above, mechanism below.** The domain decides the deadline and
  whether to retry; the protocol layer runs the timer and the attempt.
- **A lower layer absorbs mechanics, not information.** The domain still
  learns *that* a call timed out; it does not see the cancel race behind
  it. And nothing hides cost: an entity that lingers still holds what it
  holds.
- **Three stages, not three functions.** The protocol stage may be a stack
  of machines per connection (TLS, HTTP, event framing, JSON).
  - Each machine is a step machine, with entry points of the same shape on
    each side and its own limits and worst case, and it is fuzzable alone.
  - **Machines depend on lib only,** not on io and not on each other: the
    stream below a machine may be a socket, a pipe or TLS, and it cannot
    tell which.
  - **The stack is static.** The connection holds one state per machine
    and, within its step, routes each event from a machine to the one
    above it and each request the other way, the way a parent routes
    between child domains (4.5). No pipeline type, no trait, no `dyn`.
  - Each machine pulls from the one below only when it has room to push
    up.
  - **Machines keep no timers.** Each says what it is waiting for, and the
    connection arms the deadlines, in one place (5.4, section 9).

### 4.1 Each layer has its own entities

There is no shared "connection" across layers. A domain client may outlive
many connections; one socket may carry many protocol streams; one protocol
connection may serve many domain calls over time; a child process is three
pipes and an exit status. Name them differently per layer so they are not
conflated: *socket* (io), *connection* (protocol), *client* or *session*
(domain).

### 4.2 Bindings are echoed tokens

Adjacent layers link the way io_uring's `user_data` works, applied at each
boundary:

- Going down, the upper layer passes a **token**, its own handle for the
  entity concerned. The lower layer stores it without interpreting it.
- Going up, the lower layer **echoes the token** on every event.
- The upper layer stores the lower layer's handle, also as a token, to
  address requests to it.

Every name that crosses a boundary is a `lib::Token`, an opaque `u64` (a
`ReplyTo`, 4.4, wraps one). A layer makes one from its own handle
(`id.token()`) and turns it back into one (`Id::<Conn>::from_token(t)`),
and only for tokens it issued, in the record variant it issued them for.
Within a layer, names are `Id<T>`. Each side holds the other's name as an
opaque field; there are no mapping tables. For entities created from below
(an accepted socket), the lower layer announces the new entity and the
upper layer either binds to it or asks for it to be closed.

One token type keeps io free of generics and the domain free of protocol
types. The price is that the kind of entity a token names is carried by the
record variant, not by the token's type: nothing but that variant stops a
layer from decoding a token as the wrong kind.

### 4.3 Byte streams

Every byte boundary has one shape, `lib::stream`, whether the stream below
is a socket, a pipe, TLS's plaintext or an HTTP body. The side below is io
for a socket or a pipe, and the machine beneath in a stack.

- **Reading is a demand, not a request.** Each state of the side above
  says what it needs, "fill N bytes" or "scan to a delimiter, at most M
  bytes", and how much output room. The side below delivers `Bytes` only
  when that demand can be met, and `Room` likewise; a state with no demand
  receives neither. `Bytes` carries exactly the demanded bytes as an owned
  `Box<[u8]>`, which the side above reads through a `lib::Reader`.
- **Unparsed input belongs to the side below,** in its `lib::Intake`,
  under its cap: io for a socket, TLS for its plaintext. The side above
  never handles another side's receive buffers.
- **Writing is a move.** The side above encodes a message with a
  `lib::Writer` into a `Box<[u8]>` of exactly its length and moves it down
  in `Send`. The side below queues it against its output cap, then passes
  it on: io moves it into the send operation (6.2).

### 4.4 Protocol to domain

Events up and requests down are owned values with no lifetime parameters,
so they hold no references into either layer's state: a token, a variant,
and a payload the record owns, size-bounded by the protocol's limits. Such
a queue is loggable and replayable as it stands. The types belong to the
domain: the domain defines what it accepts and emits, and the protocol
layer depends on it.

```rust
pub enum Event {                                  // protocol -> domain
    Call { reply_to: ReplyTo, op: Op, key: Box<[u8]>, value: Box<[u8]> },
    // terminal events for requests the domain made
}

pub enum Request {                                // domain -> protocol
    Reply { to: ReplyTo, status: Status, value: Option<Box<[u8]>> },
    // requests the domain makes: outbound calls, each with its own token
}
```

Two shapes cross this boundary:

- **Requests down, one terminal event up,** as at every boundary.
- **Calls up, replies down.** A peer's request arrives at the domain as a
  `Call` carrying a `ReplyTo`; the domain answers with exactly one `Reply`,
  which consumes it, now or later (a domain that answers later keeps the
  `ReplyTo` in the state that waits for the answer). `ReplyTo` is neither
  `Copy` nor `Clone`, so the compiler rejects a second reply; the
  simulator catches a missing one. Wire correlation ids stay in the
  protocol layer; opcodes and statuses cross as domain enumerations. The
  domain knows nothing about connections.

### 4.5 Child domains

A domain too large for one crate is a tree of child domains under one
root domain.

- **Each child domain is a step machine of its own:** its own vocabulary,
  limits, worst case, state machines, entry points and `MAX_OUT`, and its
  own tests. It depends on lib and on its children, never on a sibling or
  a parent.
- **A parent owns its children's states and routes between them** within
  its step. Hand-offs inside the domain are short and acyclic, so a step
  completes them before it returns; its `MAX_OUT` follows from its
  children's along the longest chain.
- **Siblings share no domain types.** The parent translates between their
  vocabularies with small total functions; an exhaustive match makes a
  change on either side break the build in one place.
- **Only the root domain faces the protocol layer,** and its worst case is
  the sum of its children's.

## 5. Entities, handles and lifecycle

### 5.1 Handles

A handle is typed, opaque, and never reused for another entity: a slot
index plus a generation, typed by the entity it names.

```rust
conns.insert(conn)  -> Result<Id<Conn>, Conn>   // Err hands the value back: full, so refuse
conns.get_mut(id)   -> Option<&mut Conn>        // None if the slot holds another entity now
conns.retire(id)                                // freed at the reclaim point (5.2)
```

- **One handle type per entity kind.** `Id<Conn>` and `Id<Session>` are
  different types, so passing one where the other is expected does not
  compile. Never a bare integer.
- **Lookup checks the generation** and returns an `Option`: a completion
  for slot 27 generation 12 cannot act on the unrelated entity now in slot
  27 generation 13. A slot whose generation would wrap is retired for good.
- **The reference a lookup returns is borrowed for the current step only.**
  Given that no application type has a lifetime parameter (a rule held in
  review, 10.3), the compiler holds this: the reference cannot be stored
  in state, in a record or in a queue.
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

### 5.2 Lifecycle

Every entity is *active*, then *closing*, then *closed*, at every level:
operations in io, connections and requests in protocol, sessions and
exchanges in the domain. These are phases: a machine's own states sit
inside them, and *settling* (5.3) is part of *closing*.

- **Close is a request going down; closed is an event coming up.** An
  entity is never retired on *end of stream*: the kernel may still hold
  its buffers.
- **Retire only when no bindings remain;** each binding ends with a closed
  event from below. `slab.retire(id)` marks the slot, and the slab frees
  it at the reclaim point. Freeing is therefore bottom-up.
- **Every request gets exactly one terminal event:** success, failure,
  cancelled or timed out, whatever happened below.
- **Stale-handle asymmetry.** A token travelling up is never stale, since
  an entity is retired only after everything below it has closed, so a
  stale one is a bug and an assertion (`expect`). A handle travelling down
  may be stale (a reply to a client that just left), so a failed lookup is
  a silent drop.
- **Ownership forms a tree** (server owns connections, a connection owns
  its requests, a request owns its exchanges); every other link is a
  handle. The owner closes what it owns. Ownership is a property of
  states: it is written down per state and moves only in transitions. In
  Rust terms a layer owns its slabs, a slab owns its entities and an entity
  owns its bytes; the domain's tree sits on top, and its links are handles.
- **Lifetime is the lifecycle.** An entity ends when it is reclaimed after
  *closed*, never earlier and never later. Rust's ownership decides when
  memory is freed, when a `Box` is dropped or a slot reclaimed, never when
  an entity ends; and with no `Drop` impls, freeing memory has no other
  effect.
- **Shutdown is an event.** A termination signal arrives from io as a
  `Shutdown` event, and the domain decides what shutting down means; from
  there, closing follows this lifecycle.

### 5.3 Races

A request with a deadline has two competing outcomes. If the timer wins,
the operation is still in flight, cancelling it is asynchronous, and a
late event always arrives (sometimes a completion, when the cancel lost in
the kernel). The lowest layer that knows both competitors runs the race:
it reports the winner upward at once, keeps its entity in a *settling*
state until the loser's terminal event has arrived, then retires it.
Layers above see one event.

### 5.4 State machines

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
  is transition coverage.

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
  and the target does not", with the compiler pointing at most cases; a
  handle or a `ReplyTo` dropped in silence is the simulator's to catch.
  Dropping is right for bytes; for a handle to something that must be
  closed, the handler requests the close, and the simulator checks that
  nothing is left open.
- What a state implies (its read demand, whether the progress deadline
  runs) is an exhaustive function of the state, applied in one place after
  every transition, not repeated in every handler.

## 6. Memory and data

### 6.1 The strategy: counted entities, owned bytes

- **Entities live in slabs sized at startup.** Every entity kind has a
  `lib::Slab<T>` whose capacity comes from the limits and never changes. A
  full slab is a refusal at that layer's entrance, which is where refusals
  belong anyway (section 7). Handles are slot index plus generation.
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
  `lib::Map<Box<[u8]>, Box<[u8]>>`, say, bounded in entries, with the
  domain counting bytes and answering "full" as a domain result.
- **Running out of heap aborts.** Allocation failure is never a status.
  Short of a bug it cannot happen, because the worst case is computed and
  checked at startup (6.3).

### 6.2 What holds

- **Memory the kernel touches stays put.** A buffer a submission refers
  to is a heap allocation, a `Box` moved into the operation and held with
  it until its completion. Moving a `Box` moves the pointer, not the
  bytes, so the address the kernel holds stays valid; nothing the kernel
  touches is stored inline in a struct that a slab could move, and no
  other code can name it while the operation is in flight.
- **The kernel never holds domain memory.** Only io's transit memory is
  referenced by a submission. Domain state is always safe to mutate, evict
  or snapshot.
- **Layers share nothing by reference; a move is the copy.** Records are
  owned values. A payload that crosses a boundary is moved into the
  record, and the emitter cannot reach it afterwards, so the move keeps
  the layers apart as a copy would, at no cost. A body io reads
  is moved into the protocol's message, then into the domain's `Call`, then
  into the domain's store, and is copied nowhere on the way.
- **Copy at emission.** Data the domain keeps and also sends goes out as a
  copy made when the reply is emitted (`lib::bytes::copy_of(&stored)`),
  because a later event in the same iteration may change the stored value
  before the down pass runs. Do not reach for `Rc` or `Arc` to save the
  copy.
- **Validate before allocating.** A length from a peer is checked against
  the limits before a buffer is allocated for the data it announces. Every
  `Box<[u8]>` is made by the side below for a demand the side above
  validated (4.3), by `Writer::new(len)` for a length the code computed
  itself, or as a copy of one that exists (`copy_of`, or `clone`).

### 6.3 The worst case

```
worst case =   Σ over entity kinds:  slab + capacity × the bytes each entity may hold
             + queues and tables:    their containers
             + operations in flight: their table and the buffers they carry
             + the domain's stored-data limit
```

Each layer and each machine exports `fn worst_case(limits: &Limits) ->
Option<u64>` (checked arithmetic, `None` on overflow), and the shell
refuses to start when the sum exceeds the configured memory. An allocation
failure is then a bug or fragmentation, never load. A layer adds up what
its containers report, not `size_of` times a capacity: every lib container
has its own `worst_case(capacity)`, which counts its bookkeeping too: a
slab's slot tags and free lists, the tree nodes of a map, a set or a
deadline table. The formula counts containers and payload bytes, not
allocator overhead: leave headroom, and measure the resident size under
load before trusting it. In the simulator, a counting allocator checks at
every iteration that the live heap stays within the formula.

## 7. Flow control, admission and backpressure

**Limits are configured.** Each layer and each machine defines a `Limits`
struct; the shell's configuration record holds them and hands each to its
step as `&Env<Limits>`, so a simulation with tiny limits means something.
The limits are also the inputs of the worst case (6.3), and slab
capacities are the admission limits. The flow-control limits are: maximum
message size, cap on queued output, cap on unparsed input, concurrent
streams, accept batch, progress chunk and timeout.

**Refuse at the entrance, never in the middle.** Whatever the service
limits, it checks at the last point where saying no has no consequences:
accept for connections, request start for requests. A request refused
there leaves nothing half-done; a request abandoned in the middle leaves
partial state behind and a peer with half an answer.

**Each layer refuses at its own entrance.** An accepted socket the
protocol layer has no slot for is rejected; a request the service cannot
take on gets a busy response; a store that is full answers "full" as a
domain result.

**One request in flight per connection:** the protocol layer parses the
next only after the previous response is queued. For multiplexed
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
and the worst case of 6.3 is no longer a bound.

**Full duplex.** The room check of step 2 ties a connection's input to its
own output, which is right when its input causes its output. In a relay
(a proxy, a tunnel), what is read from one peer is written to the other,
so each direction has its own credit: read from A only while the queue
toward B has room, and the other way round. Gating A's input on A's own
output instead holds A's sending hostage to A's reading, and a peer that
writes before it reads deadlocks through the relay where it would not on
a direct connection. Only the domain knows that two connections are
coupled, so credit flows through it: the connection with room grants it,
and the other consumes it as it reads.

**Progress deadlines.** A peer that holds resources without making
progress is closed. Progress is counted in chunks, by the peer: a read
demand met, a chunk of queued output drained. The service's own writes do
not count, or a peer that never reads is kept alive by the responses it
provokes; single bytes do not count, or a trickling peer lives forever.
The chunk size is therefore the minimum rate below which a peer is cut
off. Whether the deadline runs is an exhaustive function of the state, and
one place arms and re-arms it (5.4), not every handler.

## 8. Protocols

- **Any protocol we design is sized:** a fixed header carrying every
  length. Parsing a header is a fixed-size decode with a `lib::Reader` into
  a plain struct; lengths are validated against the limits *before*
  anything is set aside for the body; the machine is header, body,
  dispatch.
- **Encoding is sized too:** compute the message's length,
  `Writer::new(len)`, write the fields, `finish()`. The length is the
  service's own, so the caller expects every write to fit (a write past
  the end is refused whole, writing nothing), and finishing short is an
  assertion.
- **Lengths are never trusted by construction.** No `[]` indexing and no
  `as` (10.3): on what a peer sent, the `Reader` returns `Option`, a
  narrowing is `u32::try_from`, and a failure is a framing error, not a
  panic.
- **Scanned framing** (delimiters, HTTP/1 heads, line protocols) is only
  for foreign protocols: a "scan to a delimiter, at most M bytes" demand,
  with the carry-over held by the side below (4.3).
- **Machines express demand, not buffer handling** (4.3). The byte source
  can then change (copying today, exact-size kernel reads later) without
  touching protocol or domain code.
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

## 9. Time, timers and randomness

- **Time is read once per iteration** by the shell, or the simulator, and
  passed to every step in `Env`, the same for every step in the iteration:
  - `env.now`, monotonic (`CLOCK_MONOTONIC`), a `lib::Time` (nanoseconds
    in a `u64`), for every deadline;
  - `env.wall`, a `lib::Wall`, for things about the world: a
    certificate's validity, a timestamp a peer will read. It never arms a
    deadline: the wall clock can jump.

  Spans are `lib::Duration`. `std::time::Instant` is not used: it is
  opaque, and a simulator cannot make one.
- **Each layer owns its deadline table** (`lib::Deadlines`, in the
  layer's state) for its own concerns: close deadlines in io, idle and
  handshake timeouts and call deadlines in protocol, backoff and session
  expiry in the domain. The expiry goes to the layer that armed it because
  no other layer holds the table.
- **Timers are not ring operations.** The shell sets one ring timeout for
  the earliest deadline over all the layers (`svc.next_deadline()`). One
  ring operation per timer would make resetting an idle timeout on every
  read cost a cancel, a re-arm, two completions and a settling state.
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

## 10. The Rust subset

### 10.1 What holds each guarantee

| Guarantee | How Rust holds it | Checked by |
|---|---|---|
| The domain never sees an fd or a kernel error | the domain's crates do not depend on io | compiler |
| A step touches only its own layer | it receives `&mut` to its own state and nothing else | compiler |
| Configuration is read-only | `&Env<Limits>` | compiler |
| Layers share nothing by reference | payloads are owned and moved | compiler |
| A buffer the kernel holds is touched by nothing else | the `Box` is moved into the operation | compiler; the ring adapter's `unsafe` contract |
| A reply is sent at most once | `ReplyTo` is not `Copy` or `Clone`; replying consumes it | compiler |
| A reply is sent at least once; one terminal event per request | | simulator |
| Handles are typed | `Id<T>` | compiler |
| A stale handle is detected | the generation | runtime |
| A state and what it holds are one value | enums with data | compiler |
| Statuses are not ignored | `#[must_use]`, `unused_must_use`, `let_underscore_must_use` | compiler, clippy; review for an ignored `Option` |
| Fail-stop | `panic = "abort"` | build profile |
| No leaks; ownership is a tree | | simulator |

What step code may not use, and what checks that, is 10.3. Each
workspace's `Cargo.toml` and `clippy.toml` hold the lints and the
disallowed types, macros and methods, and are the full list: 10.3 names
the families. Warnings are errors. The root of each step crate, and of
lib, starts with `#![cfg_attr(not(test), no_std)]` (unit tests get std)
and `#![forbid(unsafe_code)]`. Stable Rust, edition 2024; nightly only
for fuzz targets.

### 10.2 What is in

Step code (io, the protocol machines, and a service's protocol layer,
domain and `iterate`) uses:

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
- Integers, `bool`, arrays, slices, `Option`, `Result`, `Box`,
  `core::mem::replace`.
- `#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]`
  and no other derive. Entity state does not derive `Clone`: an entity
  exists once.
- `assert!`, `unreachable!("why")`, `expect("the invariant relied on")`.
- Everything lib (skein's `skein-lib`) exports:
  - entities and names: `Id<T>`, `Slab<T>`, `Token`, `ReplyTo`;
  - bounded containers: `Queue<T>`, `List<T>`, `Map<K, V>` and `Set<K>`
    (ordered, over B-trees), `Stack<T>`;
  - bytes: the `stream` vocabulary, `Intake`, `Reader`, `Writer` (sized)
    and its `Overflow`, `Decimal` (a count's digits, for text),
    `bytes::copy_of`, `bytes::zeroed` (a buffer for the side below to
    fill), and the byte search `bytes::find`, `find_from` and
    `count` (linear time, no allocation);
  - time and randomness: `Time`, `Duration`, `Wall` (wall-clock time,
    never a deadline), `Deadlines`, `Rng`;
  - the environment: `Env<L>`.

lib uses the same, plus generic types, lifetime parameters on its cursors,
hand-written impls of std traits for its own types, `Vec`, `BTreeMap` and
`BTreeSet` inside its containers, and `while` loops bounded by a
container's capacity. It is written once and tested hard. Application code
does not hand-roll data structures: what is missing goes into lib.

The shell, the simulator and tests are ordinary Rust, with `unsafe`
confined to the ring adapter in code a service runs, and in tests to the
counting allocator's `unsafe impl GlobalAlloc` (skein's testing.md,
section 6).

### 10.3 What is out

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
| `BTreeMap`, `BTreeSet` | growth without a check: lib's `Map` and `Set` are bounded | review |
| `impl Drop`, `Deref`, operator overloading, `Index` | hidden calls and hidden effects | review |
| `macro_rules!`, procedural macros beyond the derives | code the reader cannot see | review |
| `loop`, `while`, recursion | unbounded execution | review |
| a `_` arm on an own enum, `matches!` | a new case builds silently | clippy `wildcard_enum_match_arm`, `disallowed_macros` |
| `if let` on an own enum, a tuple as scrutinee | the same, out of the lint's sight | review |
| `#[non_exhaustive]` on own types | forces a `_` arm | review |
| `as` casts | silent truncation | clippy `as_conversions` |
| unchecked `+ - * /` on integers | a silent wrap, or a panic a peer can trigger | clippy `arithmetic_side_effects` |
| `[]` indexing and slicing | a panic a peer can trigger | clippy `indexing_slicing` |
| floats | no total order, nothing here needs them | clippy `float_arithmetic` |
| `unwrap()`, `panic!` | an assertion without its reason | clippy `unwrap_used`, `panic` |
| `debug_assert!` | an assertion that is off in release builds | clippy `disallowed_macros` |
| `let _ =` on a `#[must_use]` value | a silently ignored status | clippy `let_underscore_must_use` |
| `mem::forget`, `todo!`, `unimplemented!` | a leak; unfinished cells | clippy |

Integers go through `checked_*`, or through `wrapping_*` and
`saturating_*` where that is the intent. `overflow-checks = true` in every
profile traps whatever the lint does not see, in lib and the shell.

### 10.4 Discipline

- **Errors are values.** Expected outcomes (busy, full, not found,
  refused) are enum variants, never panics. Every fallible operation
  returns a `Result`, an `Option` or a `#[must_use]` status.
- **One side effect per statement,** so the order of effects is the order
  of the lines: no `out.push(queue.pop())`. The transition idiom (5.4) is
  the one exception: the handler's effects come before the assignment of
  its result.
- **Assertions are fail-stop** and are for the service's own bugs, never
  for inputs a peer controls. `expect` names the invariant it relies on.
- **Runtime checks stay on in production:** bounds checks, overflow
  checks, assertions. Safe Rust has no undefined behaviour left to trap;
  the ring adapter, the one place that could have some, is tested against
  the real kernel.
