# Notes

Provisional, 2026-10-03, revised 2026-10-09. What belongs with
programming-model.md and testing-strategy.md but is not a rule: the
questions still open, why the memory strategy is what it is, and why facts
hold the step.

## Open questions

- **Affine owner handles.** An `Owned<T>`, neither `Copy` nor `Clone`,
  returned by `insert` and consumed by `retire`, would let only an
  entity's owner end it, checked by the compiler. It is awkward for
  top-level entities (who holds the `Owned<Conn>` of an accepted
  connection?). Decide after the first service.
- **Kinds in tokens.** Nothing but the record variant stops a layer from
  decoding a token as the wrong kind (programming-model.md, 4.2). If that
  turns out to be a real mistake, give `Token` a kind tag that
  `from_token` checks.
- **Moving review-only rules into checks.** Some rules are held in review
  only (programming-model.md, 10.3):
  - a small `syn`-based checker could hold no `async`, closures, `loop`
    or `while`, tuple scrutinees, or trait and generic definitions in
    step code. Without closures, `dyn` or traits there, every call is
    static, so it could find recursion too;
  - lints could hold two more: `unused_results`, or `#[must_use]` on
    lib's functions, for an ignored `Option`; and `BTreeMap` and
    `BTreeSet` among the step crates' disallowed types.

  Write the checker if review proves not to be enough.
- **A byte budget,** for a service whose worst case is too large to
  provision (programming-model.md, 6.3). The fallback is a budget for the
  large consumers of bytes, each kept in the layer that owns them. io
  grants output room only within a global queued-output budget, so
  pressure turns into the ordinary backpressure chain rather than a new
  kind of refusal; the domain already answers "full" against its
  stored-data limit. Do not add it before a worst case demands it.
- **Operations the kernel cannot interrupt.** A process's end is bounded
  by io's deadlines (programming-model.md, 5.2), but a cancelled
  operation keeps its entity settling until the kernel completes it, and
  a write to a filesystem in uninterruptible sleep (a hard-mounted NFS
  whose server went away) may never complete. The process then cannot
  end, and its supervisor's kill waits on the same kernel. Whether to
  refuse roots on such filesystems at startup, or only to say that they
  hold a process's end.
- **Who names the last word.** The teardown invariant starts at the last
  word (testing-strategy.md, 6), which a scenario's referee names today.
  A service could mark its own, as a fact the harness reads, so that
  every scenario checks the same moment without saying so. Decide when a
  second service's worlds use the invariant.
- **A deterministic TLS.** A test-only crypto provider that draws from the
  seed would let TLS join the replaying tiers (testing-strategy.md, 4.4).
  Whether rustls's unbuffered connection allows it, and whether it tests
  enough of the real provider to be worth it.
- **Where transcripts come from** (testing-strategy.md, 4.1), and how they
  are kept current when a peer's protocol changes.

## Why facts hold the step

The trace queue was an open question: where the queue lives, and what
happens when it is full. It is one of the step's output queues, with room
reserved by `MAX_OUT`, and a full one holds the step
(programming-model.md, section 3). Each sink downstream then drops and
counts, or backpressures through its stream, by its declared policy.

- **Loss belongs at the edge.** A queue that dropped inside the process
  would lose facts before any sink could say so, and a reader could not
  tell a quiet run from a lossy one. Holding the step keeps every fact
  until a sink has decided, and the sink that decided counts what it
  lost. Each loss is then counted once, where it happened.
- **Each sink has its own reason.** A projection to a peer that falls
  behind must not slow the service, so it drops and counts. A trace read
  later as the record of what happened must be complete or say it is
  not, so it backpressures by default: a local disk is fast next to most
  peers a service waits on, so holding the step for it costs little. One
  policy for both would be wrong for one of them.
- **Holding the step is the ordinary backpressure.** It is room first
  (programming-model.md, section 2) applied to one more output, not a new
  mechanism: a step that cannot emit waits, as it waits for a full queue
  of requests.

What it gives up: a sink that backpressures can slow the service down to
its own pace, and a stalled one would hold it for good, so a stream that
can stall has a deadline that abandons it (io.md, 5.1), and the sink
records that it was abandoned.

## Why counted entities and owned bytes

The memory strategy of programming-model.md, section 6, and what it was
chosen against.

Every entity's bytes are already capped by protocol limits: the largest
message, the unparsed input, the queued output. Capping how many entities
exist therefore caps the bytes, without pooling them. Counts are where
fixed budgets are cheap and useful; bytes are where they are expensive.

Against budgets fixed at startup for bytes as well (byte pools):

- **No mutable state shared between layers.** A payload moves from io to
  protocol to domain as a `Box`: a move the compiler checks, no copy, and
  every layer's state stays private. With byte pools, either every step
  gets `&mut` to one shared pool, or each layer has its own and copies at
  every boundary.
- **Exact sizes.** A 100-byte message takes 100 bytes, not a slot of a
  size class, and there are no size classes to tune.
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
(the byte budget, above).

What it gives up: running out of heap aborts rather than refuses, so the
worst case must fit; and the general allocator sits in the hot path, so
allocation time is not constant, and fragmentation can push the resident
size above the live bytes.

Also rejected:

- **`Rc<RefCell<T>>`, `Arc<Mutex<T>>`.** Reachability decides lifetime,
  which the lifecycle forbids, and borrow errors move from compile time to
  runtime panics.
- **References in long-lived state, arenas with lifetimes.** Lifetime
  parameters spread to every type that touches them, and state stops
  being a plain value that can be snapshotted and compared.
- **Custom allocators per layer.** The allocator API is unstable, and
  everything builds on stable Rust.
- **Shared immutable buffers (`Arc<[u8]>`) as the ownership system.** At
  most an optimisation, confined to io.
