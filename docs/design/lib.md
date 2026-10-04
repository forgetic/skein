# lib

Provisional, 2026-10-03. The design of `skein-lib`: the building blocks
every step crate shares, in skein and in every service built on it. The
mechanics are those of programming-model.md; this document says what lib
holds and what each part promises.

## 1. In one page

- **Everything step code needs and the subset does not give it.** Typed
  handles and the slabs that issue them, bounded containers, cursors over
  owned bytes, the stream vocabulary and its intake, deadlines, time,
  randomness, and the environment a step reads.
- **Bounded, and priced.** Every container has a capacity fixed when it
  is made, refuses past it, and reports its own `worst_case(capacity)`,
  bookkeeping included, so a layer adds up what its containers report.
- **Written once, tested hardest.** Every layer of every service stands on
  lib. Application code does not hand-roll a data structure: what is
  missing goes into lib.
- **The subset, with a few allowances.** lib is step code, `no_std` with
  `alloc`, with the allowances of programming-model.md, 10.2: generic
  types, lifetime parameters on its cursors, hand-written impls of std
  traits for its own types, `Vec`, `VecDeque` and B-trees inside its
  containers, and `while` loops bounded by a container's capacity. Where
  a module builds on a type that is out, it says so with a scoped
  `#[expect(clippy::disallowed_types, reason = "…")]`.

## 2. In skein

lib depends on nothing. Every other crate, in skein and in a service,
depends on it. It is the only part of skein a service's domain uses.

## 3. Handles and slabs

- **`Id<T>`** is a slot index plus a generation, typed by the entity it
  names, and `Copy`. It is never reused for another entity: a slot whose
  generation would wrap is never used again. `id.token()` and
  `Id::<T>::from_token(t)` convert to and from the name that crosses a
  boundary.
- **`Slab<T>`** holds one entity kind, at a capacity fixed at startup that
  is the admission limit for that kind.
  - `insert` returns the new `Id`, or hands the value back when the slab
    is full: the refusal.
  - `get` and `get_mut` check the generation and return `None` for a
    handle whose slot holds another entity now.
  - `retire` marks a slot; `reclaim`, at the iteration's reclaim point,
    frees every retired one, so an entity can be looked up until the
    iteration ends.

## 4. Names across boundaries

- **`Token`** is an opaque `u64`, the only name that crosses a boundary.
  A layer makes one from its own handle and turns it back only for tokens
  it issued, in the record variant it issued them for.
- **`ReplyTo`** wraps a token, and is neither `Copy` nor `Clone`, and
  `#[must_use]`: replying consumes it, so a second reply does not
  compile. A missing one is the simulator's to catch.

## 5. Bounded containers

| Container | What | Over |
|---|---|---|
| `Queue<T>` | the records between stages, first in first out | a ring buffer allocated at capacity |
| `List<T>` | a sequence a step appends to | a buffer allocated at capacity |
| `Map<K, V>` | a table keyed by value, ordered by key | a B-tree, priced in tree nodes |
| `Set<K>` | a `Map` without values | a B-tree, priced in tree nodes |
| `Stack<T>` | what a parser keeps instead of recursing | a buffer allocated at capacity |

Each refuses past its capacity, handing the value back rather than
panicking, with one exception: `Queue::push` is how a step emits into
the room the loop reserved for it (`MAX_OUT`), so a full queue there is a
bug, asserted; `Queue::try_push` is the refusing form. A B-tree allocates
as it fills, so its worst case counts its nodes at capacity, not its
entries.

## 6. Bytes

- **Owned bytes** are a `Box<[u8]>` allocated at its final length.
  `bytes::copy_of` copies one, for data a domain keeps and also sends;
  `bytes::zeroed` makes one for the side below to fill, a receive buffer.
  The byte search (`bytes::find`, `find_from`, `count`) runs in linear
  time and allocates nothing.
- **`Reader`** reads a delivery without trusting any length in it: every
  read (`u8`, `u16`, `u32`, `u64`, `bytes`, `skip`) returns an `Option`,
  and a short one is a framing error for the caller, never a panic.
- **`Writer`** builds a message into a box whose length was computed
  first: `Writer::new(len)`, `put` the fields, `finish()`. A write past
  the end is refused whole (`Overflow`), writing nothing; finishing short
  is an assertion.
- **`Decimal`** holds the decimal digits of a count, for a step that
  writes a number as text without formatting: measure it, then put it.

## 7. Streams

`lib::stream` is the one shape of every byte boundary
(programming-model.md, 4.3):

```rust
pub enum Read { Nothing, Fill(u32), Scan { until: Delimiter, max: u32 }, Line { max: u32 } }

pub enum Down {                         // to the side below
    Demand { read: Read, room: u32 },   // what this state needs, and the output room it wants
    Send(Box<[u8]>),                    // moved down, within the room granted
    Finish,                             // nothing more to send: flush, then end the stream
}

pub enum Up {                           // from the side below
    Bytes(Box<[u8]>),                   // exactly the demand
    Room,                               // the room asked for is free
    End,                                // the other side will send nothing more
    Failed(Fault),                      // the stream is broken: no more Bytes or Room
}
```

A `Fault` says why a stream broke as far as the side above can act on it:
the peer reset it, the side below could not make sense of the peer's data
(a TLS record that fails to decrypt), or anything else.

**The contract of a stream.** Every side below meets it, io's and every
machine's:

- **A demand is answered at most once:** by `Bytes`, exactly what its
  read asks for, or by `Room`, whichever the side below can give first;
  either answer ends it, and nothing is outstanding until the side above
  states its next. The side above states its next demand only after an
  answer, never in place of one outstanding. A state that wants nothing
  more after an answer states nothing (programming-model.md, 5.4).
- **`Read::Nothing` with no room withdraws** the outstanding demand, and
  only when the side above will read no more: it is closing, or its read
  crossed `End`. An answer
  already on its way may still arrive, and the side above drops it:
  dropping `Bytes` loses data, so only a reader giving up the stream may.
  No other `Bytes` or `Room` come without a demand.
- **`End` comes once nothing the side below holds can meet a demand:**
  with one outstanding, when it can never be met; with none, only when
  nothing is held. It comes once, and ends reading only. A read that
  crosses it is never met, and `End` does not end the demand: it stays
  outstanding until `Room` answers it, if it asked for room, or until the
  side above withdraws it, and the side above states no other demand
  meanwhile. Room may still be granted after `End`, as the stream can
  still send to a peer that only half-closed. A read larger than what is
  left before the end is never met; a side above that must see every byte
  reads by its framing (a scan, or fills no larger than its framing says
  remain).
- **`Room` grants one `Send`** of at most the room asked for; the side
  above sends within it (an empty `Send` gives it up), or finishes, before
  it demands room again. **`Finish` comes with no read outstanding,**
  other than one that crossed `End`: a side below that reads another
  stream to meet a read, as TLS does, could not send the end behind it.
  io asserts neither; a side above keeps both, so that it works over any
  side below.
- **`Failed` may come at any time,** a demand outstanding or not, and
  nothing follows it. After `End` it says only that the stream can no
  longer send: what was read stands.
- **A scan that meets no delimiter within its maximum delivers exactly
  the maximum.** What that means is the side above's: a framing error for
  a line or a head, one piece of a longer text for a string scanned to its
  quote (json.md, 3.2).
- **A line scan (`Line`) ends at the first line end, a CR or an LF,
  whichever comes first,** for text whose lines end with LF, CRLF or CR
  alone, as an event stream's do (http.md, 4.1): a CRLF is two ends, the
  CR ending one delivery and the LF the next, and the side above pairs
  them. Otherwise it is a scan: within its maximum, or exactly the
  maximum. A `Delimiter` stays one exact sequence of bytes; "either of
  two bytes" is a read of its own.

**`Intake`** is the carry-over of a stream: the bytes the side below has
received and the side above has not yet demanded, up to a cap fixed when
it is made. It is allocated once, at the cap, and never grows.

- It appends what arrives while it has room, and reports the room left,
  so receiving stops at the cap.
- It meets a fill, a scan or a line scan as soon as it can, each delivery
  a box of exactly the demanded length. A scan that reaches its maximum
  with no delimiter delivers exactly the maximum: the side above sees a
  scan that does not end with the delimiter, and decides what it means.
- A fill larger than the cap, or a scan whose maximum is shorter than the
  delimiter (one byte, for a line scan) or longer than the cap, could
  never be met: the caller's bug, asserted.
- A scan remembers how far it has searched, so a scan for the same
  delimiter, or a line scan after a line scan, does not search the same
  bytes again.
- It says whether what it holds ends partway through a delimiter
  (`ends_partway`): a side below that fills its own intake from another
  stream, as the HTTP client does a body's, then reads a byte at a time,
  so that it never reads past a delimiter its next bytes complete.

## 8. Time, deadlines and randomness

- **`Time`** is nanoseconds on a monotonic clock, in a `u64`, read by the
  shell or the simulator. **`Duration`** is a span, with checked and
  saturating arithmetic. **`Wall`** is wall-clock time, nanoseconds since
  the Unix epoch, a type of its own with no arithmetic on spans, so it is
  never mixed with `Time` and never arms a deadline.
- **`Deadlines`** is a layer's own timer table, bounded: `arm` a deadline
  for a key, `cancel` it, ask for the `next` one, and `expire` those that
  have passed, each removed before its handler runs. It is a pair of
  B-trees, priced at capacity.
- **`Rng`** is a seeded generator held in a layer's state: `next_u64`,
  `below`, `between`, `chance`.

## 9. The environment

**`Env<L>`** is what a step reads besides its own state: `now`, `wall`,
and its layer's `limits: L`. A step gets it behind a shared borrow.

## 10. Testing

Step tests drive each container through its operations, at and past its
capacity: a full queue, a stale handle, a slot whose generation would
wrap, a refusal at the entrance, a deadline that fires as it is
cancelled, a scan cut at every byte. The step tests are `src/tests.rs`, a
module for each area of this document under `src/tests/`: handles,
containers, bytes, streams, and time.

The byte search is also compared with a naive one, and the intake with a
plain reference, over random cases drawn from a seed: a few hundred in
the step tests, and 20,000 from the same seeds in the fuzzy suite, in
`tests/lib` (testing-strategy.md, 8).

Each `worst_case` is checked against the counting allocator (testing.md,
5), in `tests/lib/tests/memory.rs`, in the focused suite. Every container
is built at capacities from 0 to 300, filled to them, emptied and filled
again, every operation a step of the meter, and what it held of its own is
never more than `worst_case(capacity)`. What an item owns is its owner's
to count, so the items own no heap; they come in several sizes and
alignments, up to 64 bytes, as a container's price depends on both. An
input moved into a step was counted by whoever made it; the step's bound
covers what it keeps and what it allocates (testing.md, 5). What an
operation hands out (an item taken, a delivery, a list moved into a box)
is no longer the container's, and the meter takes it off.

- **A slab, a queue, a list, a stack and an intake** are allocated once,
  at their capacity, and hold exactly their worst case from the start.
- **A map, a set and a deadline table** are filled in order, each leaf
  left behind holding six entries, then thinned to five a leaf, the fewest
  a leaf holds: a tree within a few nodes of the most its capacity allows,
  and driven at random. Their
  worst case prices every node as an internal one, the larger kind, so at
  a capacity of 12 or more they hold from about 30% of it, for small
  entries, whose nodes are mostly edges, to 96%, for 64-byte keys and
  values. Below 12, a tree is at most one leaf, and the worst case counts
  up to three nodes.

## 11. Not built yet

- **`Slab::get2_mut`,** which looks up two entities at once and fails on
  equal handles (programming-model.md, 5.1).
- **A state digest** for replay: a fixed-key hasher over the state types'
  derived `Hash`, independent of their layout in memory
  (testing-strategy.md, 6).
