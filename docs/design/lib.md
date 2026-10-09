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

`lib::stream` is the classic shape of a byte boundary
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

### 7.1 Independent output reservations

A framed connection can need to send while its next read waits for a peer
that is itself waiting for that output. Its lower face supports an explicit
independent output vocabulary alongside the classic read vocabulary:

```rust
pub enum OutputDown {
    Room { right: Token, bytes: u32 },
    Cancel { right: Token },
    Send { right: Token, bytes: Box<[u8]> },
    Release { right: Token },
}

pub enum OutputUp {
    Settled { right: Token, outcome: OutputOutcome },
}

pub enum OutputOutcome { Granted, Cancelled, Failed(Fault) }
```

One admitted Room request owns one obligation. Its token is generated by
the caller, never reused for another request on that stream, and never a
wire identity. Token exhaustion prevents a new request before effects.
Its positive byte count fits the lower output cap. The lower grants only
when both those bytes and one Send record slot are reserved, including
output held in flight or stalled. Exactly one Settled terminal consumes
the demand. Granted creates an affine reservation with the same token;
one matching Send, Release, Finish or actual failure/close consumes it.
A Send moves at most the granted bytes and spends the whole reservation,
including unused capacity. An empty Send also consumes it.

There is one independent output demand or grant at a time. It can coexist
with a classic read-only Demand, which it neither answers nor withdraws.
Bytes, End and classic Room cannot settle it. Classic room demands and
grants cannot coexist with independent output ownership, and a classic
Send cannot spend an independent grant. Classic consumers keep the joined
Demand.

Cancel names a pending output demand. Cancellation wins only before the
lower has emitted its terminal. A Granted terminal already queued above
remains the winner; the caller consumes it and releases its grant while
closing. Old Cancel, Send and Release tokens cannot consume a newer right.
They are inert, including a Send with no live matching grant. A matching
Send larger than its reservation is the caller's bug, asserted before
retaining or submitting its bytes. There is no
second terminal when an already granted reservation is retired.

Actual failure settles a pending output demand with Failed before telling
classic stream Failed. Close or Abort settles it with Cancelled before the
entity's real Closed. The caller stops admitting output when closing;
requests naming an already closing or absent entity are not admitted.
Only a positive, within-cap Room on an open stream with idle output and
no competing classic room ownership creates an obligation; the lower
retains only its active cell, without a history of the caller's tokens.
Neither a grant nor Closed proves peer receipt or durable commitment.

Finish keeps its classic restriction: withdraw a read only when closing or
after End, settle any pending output demand, and finish once. There is no
general live-read cancellation and no Finish terminal. End concerns
reading alone; independent output can progress after it. Native lower
support is required: an adapter over an arbitrary classic combined Demand
cannot manufacture independent room.

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

The request table's step tests are `src/tests/requests.rs`; its independent
obligation model, replay and seeded sweep are in `tests/lib/src/request_table.rs`
and `tests/lib/tests/{request_table,fuzzy_request_table}.rs`.

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

## 11. The journal

`Journal<W, O>` is a bounded commit barrier for writes and outputs. It is
generic over both and inspects neither. It belongs in lib because it is a
data structure, like a queue; a service does not hand-roll one
(programming-model.md, 10.2).

- **Admission by counts:** the owner states a decision's worst-case room
  for writes and held outputs before changing its state. `takes(room)`
  leaves the journal unchanged, and refuses unless the room fits the
  commits in flight, writes per commit, and outputs held. A decision
  reserves that room; accepting it returns unused room.
- **One decision:** a `#[must_use]` value, not `Clone`, collects its
  writes and held outputs. Each addition past its reserved room is
  refused and returns ownership to the caller. Such an overrun is the
  owner's bug: accepting the decision stops the journal.
- **Commit and tags:** accepting a decision makes at most one numbered
  commit, holding all its writes. Numbers begin at one and increase in
  order. A decision without a write makes no commit. Each held output
  follows that decision's commit, or the last commit made if there was
  no write; it leaves only once that commit is durable. A commit answer
  makes that commit and every earlier one durable. Answers outside the
  order the store sends commits are the owner's bug and stop the
  journal.
- **The door and release:** `now(output)` admits an untagged output that
  decides nothing and is ready to leave at once. These outputs leave in
  their own order, within the door's capacity. Held outputs leave in
  the order they were collected, after their tags are durable. Each
  release call moves at most its configured count. The journal does not
  arm a deadline or advance commits by itself.
- **Failure:** when a commit fails, no output tagged with it or a later
  commit leaves, and the journal reports that it has stopped. It admits
  no more decisions or outputs.
- **Memory:** capacities are fixed at construction. `worst_case` prices
  the journal's containers and bookkeeping from its limits with checked
  arithmetic, as other lib containers do. Values it holds own their
  own payload bytes; their owner prices those separately.

The journal uses generic types and lib's containers. It defines no
traits, takes no closures, and uses no `dyn` (programming-model.md,
10.2).

## 12. The request table

`RequestTable<R>` keeps bounded keyed requests until their answers are
final. It is generic over what it holds and inspects neither requests nor
answers. It belongs in lib because it is a data structure with a
lifecycle, like a queue or the journal; a service does not hand-roll one
(programming-model.md, 10.2).

- **Keys:** each admitted request gets a key drawn from the seed given
  at construction, never reused for another request. A restored request
  keeps its key. Exhausting the key space refuses admission rather than
  reusing a key.
- **Scope:** each request keeps an opaque number its asker gives. Only
  requests whose scope is confirmed may be sent.
- **Kept before sent:** each admitted request emits its record for the
  asker's store: its key, scope, request and first sending's wall time,
  if it has been sent. Its first sending updates that record. Records
  leave before the sends they cover, in order; each take emits at most
  its configured count. Retiring a request emits its erase.
- **One attempt:** a request has at most one attempt in flight. Each
  attempt has its own fresh token, distinct from the owner's token.
  An answer for a superseded or retired attempt changes nothing.
- **Envelopes:** the protocol layer says which kind of answer came.
  *Final* retires the request and hands back its owner's token, so the
  asker routes the answer unopened. *Again* schedules a retry under the
  same key. *Lost* does likewise while the link is up, and parks while
  it is down. *Signed out* parks until the request's scope is confirmed
  again. Retry spans start at a configured first span, double to a
  configured most, and are jittered from the seed.
- **Parking and resuming:** a down link parks requests and sends
  nothing. An up link resumes the confirmed scope's parked requests;
  confirming a scope permits only that scope's requests. Changing the
  scope parks every other scope's requests.
- **Retention:** the asker tells the table how long answers are kept.
  A request first sent longer ago than that span less the configured
  margin is never sent again: it is retired, erased, and its owner told
  that its outcome is unknown. The saved first sending is a wall time,
  so the cutoff survives a restart; retries and the cutoff use monotonic
  deadlines computed from the supplied clocks (programming-model.md,
  section 9). A restored request is not sent until retention is known.
- **Restoration:** records are admitted at startup within the same
  limits, keeping their keys and first sending times. They begin parked
  until the link is up and their scope is confirmed. The asker supplies
  their owner tokens and declared sizes again.
- **Progress:** changes to in flight, retrying, parked or unknown leave
  with the owner's token, for display. The table never reads the answer
  or decides how progress is shown.
- **Admission and memory:** capacities are fixed at construction. A
  request past the held-request count or the sum of owners' declared
  bytes is refused at admission, returning ownership and changing
  nothing. The table does not measure a request. `worst_case` prices its
  containers and bookkeeping from the limits with checked arithmetic;
  the owner prices the payload bytes it declares separately.

The asker decides each request's scope and contents, supplies its owner
token and declared size, and tells the table the link, confirmed scope,
retention and clocks. The protocol layer classifies envelopes; final
answers go directly to their owner. The table decides keys, attempts,
backoff, parking, retention cutoffs, store records and progress.

The table uses generic types and lib's own containers. It defines no
traits, takes no closures, and uses no `dyn` (programming-model.md,
10.2).

## 13. Not built yet

- **`Slab::get2_mut`,** which looks up two entities at once and fails on
  equal handles (programming-model.md, 5.1).
- **A state digest** for replay: a fixed-key hasher over the state types'
  derived `Hash`, independent of their layout in memory
  (testing-strategy.md, 6).
