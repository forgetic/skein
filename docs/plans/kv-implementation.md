# Implementing skein-kv

Implemented core, 2026-10-05. A plan for building `skein-kv`, the key-value
store of [the kv design](../design/kv.md): every key and value held
in memory, an append-only commit log, fuzzy snapshots, and, last,
payload logs beside the store. Values on disk, with only keys in memory
and compaction in place of snapshots, are a later version (section 9).
Read it with [the programming model](../foundation/programming-model.md),
[the testing strategy](../foundation/testing-strategy.md),
[io](../design/io.md), [the kernel](../design/kernel.md) and
[testing](../design/testing.md).

## 1. In one page

- **A protocol-layer machine.** `skein-kv` is a `no_std` step machine with
  the shape of `skein-http`'s client: `down` takes the owner's requests,
  `up` takes io's file events, and each emits events above and file
  requests below (programming-model.md, 3 and 4). Reads are answered in
  the step that asks; commits are answered once their sync completes.
- **Two prerequisites outside the crate,** each its own increment through
  skein's gate: io's file entity (positioned writes and reads, syncs, the
  replacement sequence), and a simulator disk that crashes as a disk
  does. The store's tests are only as good as the second.
- **Then the store in four steps:** memory and codecs; the log and
  recovery; snapshots; bounds. Payload logs come after, when temper's
  transcripts pull them.
- **Verified by crashing.** A world runs the store against a model map on
  the simulated disk, crashes it at every file operation and sync
  boundary, recovers, and holds the referee: the last acknowledged
  commit, or that and one whole uncertain one, never part of a commit.
- **Keys are bytes, ordered.** The crate ships the order-preserving key
  encoding its owners need for composite keys and indexes (section 3.3).

## 2. Prerequisites

### 2.1 io's file entity (skein-io)

io.md, section 9 already orders io's whole-file operations next, over the
kernel's file records, which are built (kernel.md, 6.1). The store needs
one more kind of file entity beside them, a file held open:

| Request | Terminal event | Over the kernel |
|---|---|---|
| `Create { root, name, mode }` | `Opened { file }` | `Open` with `OpenHow::Create`, the mode given (`0o600` for a store of secrets) |
| `OpenRead { root, name }` | `Opened { file, len }` | `Open` with `OpenHow::Read`, `Stat` |
| `WriteAt { file, offset, bytes }` | `Written { file }` | `Write`, short writes continued |
| `ReadAt { file, offset, max }` | `Read { file, bytes }` | `Read`, short reads continued to `max` or the end |
| `Sync { file }` | `Synced { file }` | `Sync` |
| `Close { file }` | `Closed { file }` | `Close` |
| `SyncDirectory { root }` | `Synced` | `Open` with `OpenHow::Directory`, `Sync`, `Close` |
| `Rename`, `Remove`, `List` | as io.md, 5 | as kernel.md, 6.1 |

- Every request carries a deadline, and a file whose operation stalls
  settles as io.md, 5 says for whole-file operations.
- A failed `Sync` is reported as such, once; io never retries it.
- The segments and the snapshot are always new files, written at offsets
  the store tracks, so no open of an existing file for writing is needed
  until payload logs (2.3).

### 2.2 A disk that crashes (skein-sim, skein-fake-machine)

The simulator's files, and the minimal fake machine's, gain what a crash
keeps (draft, section 8):

- **Durable and pending state per file:** a write lands in pending, a
  `Sync` of the file makes its pending writes durable; a directory's
  entries (creations, renames, removals) are pending until the directory
  is synced.
- **A crash** keeps the durable state and, of what is pending, a choice
  the seed draws: none, all, any subset of writes in any order, the last
  one torn at any byte; a rename whole or not at all.
- **Faults on demand:** a failed or short `Write`, a failed `Sync` (its
  pending writes then kept or lost, as the seed draws), a stalled
  operation.
- **Restart:** the world stops the process at a crash point and starts a
  new one over the surviving disk.

The conformance suite gains the cases both backends can hold (ordering
of a write and a read after it, a rename's atomicity to readers); what a
power loss keeps cannot be checked on the real ring, so the simulator's
model is stated in simulator.md and reviewed against the Linux
documentation, not tested against a kernel.

### 2.3 Opening a file to write, later

Payload logs append to a file across restarts, so they need `OpenHow::Write`
(an existing file, to write), in the kernel's records, the ring, the
simulator and the conformance suite. It comes with section 6, not before.

## 3. The crate

### 3.1 Layout

```
crates/skein-kv/
├── Cargo.toml          skein-lib, skein-io (its file vocabulary only)
└── src/
    ├── lib.rs          the crate's doc: what it keeps, what it never knows, its entry points; re-exports; MaxOut
    ├── boundary.rs     Request, Event, Op, Range, Row, Refusal, Failure
    ├── key.rs          the order-preserving key encoding (3.3)
    ├── limits.rs       Limits, worst_case
    ├── store.rs        Store, its phases, up and down
    ├── map.rs          the map: lib's Map of keys to values, with its bytes counted
    ├── frame.rs        a commit's frame: encode, decode, checksum
    ├── log.rs          segments, the open one, group commit
    ├── snapshot.rs     writing a snapshot a chunk at a time; reading one back
    ├── recovery.rs     listing the directory, the snapshot, the segments' longest valid prefix
    ├── crc.rs          CRC-32C, table-driven
    ├── trace.rs        trace records (programming-model.md, 3: diagnostics are data)
    ├── tests.rs
    └── tests/          step tests per area: key, frame, map, log, snapshot, recovery
```

### 3.2 The owner's vocabulary

```rust
//! The key-value store of skein (kv.md): ordered byte keys to byte values,
//! all in memory, made durable by an append-only log of commits and
//! snapshots in a directory it owns. ...
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

/// A request from the store's owner. Each is answered by exactly one
/// terminal event naming the same `owner` token.
pub enum Request {
    /// Recover from the directory beneath `root`, or start an empty store
    /// there. Terminal: `Opened` or `Failed`. Nothing else is admitted
    /// before `Opened`.
    Open { owner: Token, root: Root },
    /// Apply `ops` in order, as one. Terminal: `Committed` once durable,
    /// `Refused` at the entrance, or `Failed` (outcome uncertain).
    Commit { owner: Token, ops: Box<[Op]> },
    /// Terminal: `Got`, in the same step.
    Get { owner: Token, key: Box<[u8]> },
    /// A page of `range`, in key order. Terminal: `Loaded`, in the same step.
    Load { owner: Token, range: Range, max: Page },
    /// Finish what is in flight and close every file. Terminal: `Closed`.
    Close { owner: Token },
}

pub enum Op {
    Put { key: Box<[u8]>, value: Box<[u8]> },
    Erase { key: Box<[u8]> },
}

/// From `start` (inclusive) to `end` (exclusive; `None` is the end of the
/// keys). `Range::prefix(p)` is every key starting with `p`.
pub struct Range { pub start: Box<[u8]>, pub end: Option<Box<[u8]>> }

/// The most a page may hold, both checked.
pub struct Page { pub rows: u32, pub bytes: u32 }

pub enum Event {
    /// Recovered (or new): commits up to `last` are durable; the next is
    /// `last + 1`.
    Opened { owner: Token, last: u64 },
    Committed { owner: Token, number: u64 },
    Refused { owner: Token, refusal: Refusal },
    /// The commit was in flight when the store stopped (a failed write or
    /// sync, or a stall past its deadline). Its outcome is learned from the
    /// `Opened` that follows: durable if `number <= last`.
    Failed { owner: Token, number: u64 },
    Got { owner: Token, value: Option<Box<[u8]>> },
    /// `rows` in key order; `next` is where the following page starts, if
    /// the range holds more.
    Loaded { owner: Token, rows: Box<[Row]>, next: Option<Box<[u8]>> },
    Closed { owner: Token },
}

pub enum Refusal {
    /// A key, a value, the ops or the commit's bytes past their limits.
    TooLarge,
    /// The map would pass its budget.
    Full,
    /// The queue of commits waiting for a sync is full: ask again later.
    Busy,
    /// Not open, recovering, or closing.
    Unavailable,
}
```

- **After a stop,** the store answers `Failed` for each commit in flight,
  settles its files, recovers from the disk on its own, and emits
  `Opened` with the recovered `last`; the owner's `Open` is not repeated.
- **Reads see durable commits only:** a commit is applied to the map
  when its sync completes, in commit order.
- **Entry points,** as `skein-http`'s client:

```rust
pub fn down(store: &mut Store, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<io::Down>);
pub fn up(store: &mut Store, env: &Env<Limits>, ev: io::Up,
          above: &mut Queue<Event>, below: &mut Queue<io::Down>);
pub fn fire(store: &mut Store, env: &Env<Limits>,
            above: &mut Queue<Event>, below: &mut Queue<io::Down>);
pub const MAX_OUT: MaxOut = MaxOut { above: .., below: .. };
pub fn worst_case(limits: &Limits) -> Option<u64>;
```

`fire` drives the snapshot a chunk at a time when no file event is due,
so a snapshot never waits on commits to make progress.

### 3.3 Keys

Owners build composite keys whose byte order is their logical order
(temper's `task/<n>/msg/<seq>`, `proj/<p>/ended/<rtime>/<n>`):

```rust
pub struct KeyWriter { .. }   // bounded by Limits::key

impl KeyWriter {
    pub fn tag(&mut self, tag: u8) -> &mut Self;        // a key space
    pub fn u64(&mut self, n: u64) -> &mut Self;         // big-endian, fixed width
    pub fn rev_u64(&mut self, n: u64) -> &mut Self;     // newest first
    pub fn bytes(&mut self, b: &[u8]) -> &mut Self;     // 0x00 escaped as 0x00 0xff, ended by 0x00 0x00
    pub fn finish(self) -> Option<Box<[u8]>>;           // None past the limit
}

pub struct KeyReader<'a> { .. } // the inverse, for owners decoding index keys
```

A step test checks the order against a naive comparison of the decoded
tuples, over generated tuples.

### 3.4 Limits

```rust
pub struct Limits {
    pub key: u32,             // bytes in a key
    pub value: u32,           // bytes in a value
    pub ops: u32,             // ops in a commit
    pub commit: u32,          // bytes in a commit's frame
    pub queued: u32,          // commits waiting for a sync
    pub queued_bytes: u64,    // their frames' bytes
    pub budget: u64,          // the map's bytes: keys, values, per-entry overhead
    pub segment: u64,         // bytes before the log moves to a new segment
    pub snapshot_after: u64,  // log bytes since the last snapshot's start before the next
    pub chunk: u32,           // bytes a snapshot or recovery reads or writes at once
    pub page: Page,           // the largest page a Load may ask for
    pub deadline: Duration,   // each file operation
}
```

`worst_case` sums the budget, the queue's frames, a chunk's buffer, the
largest page and the map's per-entry bookkeeping, as lib's containers
report it (programming-model.md, 6.3).

## 4. On disk

- **The directory** beneath the owner's root holds `snapshot`, the log's
  segments `log-<first number, 20 digits>`, and, only during a snapshot,
  io's temporary for it.
- **A frame:**

| Field | Bytes |
|---|---|
| magic, format version | 4, 2 |
| commit number | 8 |
| op count, body length | 4, 4 |
| ops: kind (1), key length (4), key, and for a put value length (4), value | body |
| CRC-32C of everything before it | 4 |

- **A snapshot:** a header (magic, version, the replay's start number S),
  chunks (row count, length, rows, CRC each), and a trailer (row count,
  bytes, CRC, end marker). Without its trailer, a snapshot is invalid.
- **A new segment is announced to the disk** before any commit in it is
  answered: create it, sync the directory, then write.

## 5. Increments

Each a branch through skein's gate (`scripts/check.sh`), merged
`--ff-only`.

### 5.0 Prerequisites

**io's file entity** (2.1), with its io world stories; **the crashing
disk** (2.2), with the simulator's and the fake machine's tests and the
conformance cases. Independent of each other.

### 5.1 Memory and codecs

`boundary.rs`, `limits.rs`, `key.rs`, `map.rs`, `frame.rs`, `crc.rs`;
`Store` with an in-memory mode only (no root: `Open` answers at once,
commits apply at once), so `Get` and `Load` paging, refusals and the
budget are tested before any file. Step tests: keys' order, frames both
ways and every corruption refused, the map's accounting against its
entries.

### 5.2 The log and recovery

Segments, group commit (one write and one sync in flight; commits queue
behind), segment rollover, the directory sync for a new segment;
recovery from segments alone, the longest valid prefix, a new segment
after it; a failed write or sync stopping the store, `Failed` for what
was in flight, recovery, `Opened`. The world (section 7) starts here.

### 5.3 Snapshots

The trigger, a rollover to a segment starting at S, writing the map in
key order a chunk per `fire` or completion while commits go on, the
replacement sequence, deleting segments before S; recovery from a
snapshot and the segments from S; io's leftover temporary removed.
Written down in kv.md first: the argument that replaying from S
corrects a fuzzy snapshot (frames carry whole values; a commit is applied
to the map only once durable, so nothing in a snapshot is missing from
the log).

### 5.4 Bounds

`worst_case` against the counting allocator, at the limits and under
random load; recovery within the budget, reading in chunks; work per step
bounded (a commit's ops, a page, a chunk), measured in the world.

### 5.5 kv.md out of draft

`docs/design/kv.md`, with what the increments settled; the draft
removed; testing.md's layout and state updated.

## 6. Payload logs, after

When temper's transcripts pull them: `OpenHow::Write` (2.3); ops in a
commit that name a payload log's extent, so the store keeps extents in a
key space of its own and clamps reads to them:

```rust
pub enum Op {
    Put { .. }, Erase { .. },
    /// The payload log `log` is committed up to `length` bytes.
    Extend { log: Box<[u8]>, length: u64 },
    /// The payload log `log` goes, once this commit is durable.
    Drop { log: Box<[u8]> },
}

pub enum Request {
    ..
    /// Write `bytes` at the log's committed extent and sync them. Terminal:
    /// `Appended { owner, length }`, the length to commit with `Extend`.
    Append { owner: Token, log: Box<[u8]>, bytes: Box<[u8]> },
    /// Up to `max` bytes from `offset`, never past the committed extent.
    ReadLog { owner: Token, log: Box<[u8]>, offset: u64, max: u32 },
}
```

Recovery removes logs with no extent; an append writes over whatever
lies past the extent. Its world crashes between an append's sync and the
commit of its extent.

## 7. Tests

```
crates/skein-kv/src/tests/       step tests: key, frame, map, log, snapshot, recovery
tests/kv/                        skein-kv-world
├── Cargo.toml                   skein-lib, skein-io, skein-kv, skein-sim, skein-world; skein-heap for memory
├── src/lib.rs                   what the world plays and checks, in its module doc
├── src/world.rs                 the store over the simulator's crashing disk; Settings::calm, ::random; run
├── src/owner.rs                 a scripted owner: commits, gets, loads, closes, from the seed
├── src/model.rs                 the model: a BTreeMap, the acknowledged commits, the one uncertain
├── src/referee.rs               the safety rules, and liveness as deadlines
└── tests/
    ├── store.rs                 stories: commit then read; paging; group commit; refusals; close
    ├── recovery.rs              a crash at every operation of a scripted run, each recovered
    ├── faults.rs                failed and short writes, failed syncs, stalls: stop, Failed, recover
    ├── snapshot.rs              commits racing a snapshot at every chunk; crashes at each phase
    ├── referee.rs               the referee fails what it must, a test per rule
    ├── memory.rs                at the limits, the heap under worst_case
    ├── real.rs                  on the ring, in a skein-scratch directory: open, commit, close, reopen
    ├── fuzzy_crash.rs           random workloads, random crash points, many seeds
    ├── fuzzy_snapshot.rs        random snapshot timing under load
    └── fuzzy_memory.rs          random load against worst_case
fuzz/kv_frame                    the frame decoder on arbitrary bytes
fuzz/kv_recovery                 recovery over an arbitrary directory's contents
```

**The referee:** after every recovery, the store's contents equal the
model's after its last acknowledged commit, or after that and the whole
of one uncertain commit; numbers contiguous; no acknowledged commit
lost; every `Get` and `Load` equal to the model's, pages without gaps or
repeats when nothing commits between them.

**What the real ring cannot show:** a killed process leaves the page
cache, so `real.rs` checks integration, not power loss. The crash model's
fidelity (2.2) is reviewed, not tested.

**Budgets:** the world's focused tests within a few tenths of a second,
its fuzzy ones within a few seconds, measured and recorded in
testing.md, section 7, with each increment.

## 8. For temper

Its engine's store protocol layer uses `skein-kv` and owns what is
temper's: the record codecs, the key layout and indexes (temper's
`domain/engine.md`, section 5), a second store for secrets in a `0o600`
root, retention, and, after section 6, transcripts as payload logs.
Temper's plan for the store's protocol and io (its
`docs/plans/next-domain/08-after.md`, section 3) cites this one.

## 9. Later

- **Values on disk:** an in-memory index of keys to where their values
  lie, values read by offset, compaction in place of snapshots, the index
  rebuilt from hint files at start. It changes `Get` and `Load` to answer
  after reads, so their events stop being same-step; the owner's
  vocabulary otherwise stays.
- **Several stores sharing a sync,** if a service runs more than one.
- **Reads at a commit** across pages, if an owner needs a consistent
  multi-page view.

## 10. Open

- CRC-32C or a 64-bit hash for frames and chunks.
- The trigger's ratio and the segment's size, measured.
- Whether `Load` gets a reverse order, or owners keep using reversed
  keys.
- How much the crate's step tests and world can share with
  `skein-world`'s harness, as temper's worlds do.
