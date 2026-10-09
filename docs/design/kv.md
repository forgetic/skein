# A key-value store on Skein's loop

Implemented core, 2026-10-05. A built-in store for a service whose durable
dataset fits in memory: ordered keys and values held in memory, an
append-only commit log, and snapshots, all as a Skein machine on Skein's
file I/O. Large append-only payloads go beside it, in logs of their own
whose committed lengths the store records (section 6, not built yet).
Read it with [the programming model](../foundation/programming-model.md),
[the testing strategy](../foundation/testing-strategy.md), and
[the file design](io.md).

## 1. In one page

- **What it offers.** Ordered byte keys to byte values. A commit is a
  batch of puts and erases, numbered, applied in order, and answered
  once it is durable. Reads (a key, or a page of a key range) are served
  from memory and see only durable commits.
- **What it assumes.** The dataset fits a stated memory budget; one
  service owns the store's directory; values are opaque, and the service
  versions its own record shapes.
- **Why not redb here.** Keeping redb on the loop means an in-memory
  virtual file and a persistence machine that replays redb's storage
  calls (redb.md, sections 1 to 4): all the data in memory and a
  persistence protocol of our own anyway, with crash-safety resting on
  redb's internal call patterns. With the data in memory, a log and
  snapshots are the whole of what is needed, and their recovery rule is
  simple enough to test by crashing at every operation.
- **A Skein machine.** Unlike redb (redb.md, section 6), the store is an
  ordinary `no_std` step machine: its state is the map and its
  persistence state, and its disk work is io's file requests, with one
  terminal event each.
- **Not offered:** secondary indexes, queries, several processes, reads
  of what is not durable, a dataset larger than the budget.

## 2. Requests and events

The owner's vocabulary (`skein-kv::Request` and `Event`):

| Request | Terminal event |
|---|---|
| `Commit { ops }`, each op a `Put { key, value }` or an `Erase { key }` | `Committed { number }`; `Refused` (too large, past the budget, too many queued); `Failed` (the store stopped; outcome uncertain) |
| `Get { key }` | `Got { value }` |
| `Load { range, max }` | `Loaded { rows, next }` |
| `Open { root }` | `Opened { last }` or `OpenFailed` |
| `Close` | `Closed` |

After a fatal file failure, repair reports `Recovered { last }` or
`RecoveryFailed` as a store state event. It does not answer the original
`Open` request a second time.

- **Numbers are contiguous,** from one, and survive restarts.
- **A commit is applied to the map when its sync completes,** so a read
  never sees what a crash could lose, and the owner's outputs that
  depend on a commit wait for `Committed`.
- **Refusals happen at the entrance,** before anything is written: a
  commit whose bytes would take the map past its budget, an op past the
  key or value limit, a queue full.
- **An in-memory store** (`Store::memory`) has no directory: it takes
  commits without an `Open` and applies them at once, for an owner's
  tests that need no files.

### 2.1 Keys

Owners build composite keys whose byte order is their logical order
(temper's `task/<n>/msg/<seq>`, `proj/<p>/ended/<rtime>/<n>`), with the
crate's `KeyWriter`, bounded by the key limit, and read them back with
`KeyReader`:

- `tag(u8)`: a key space;
- `u64(n)`: big-endian, fixed width; `rev_u64(n)`, its complement, newest
  first;
- `bytes(b)`: `0x00` escaped as `0x00 0xff`, ended by `0x00 0x00`, so a
  shorter string sorts before its extensions;
- `finish()`: the key, or none past the limit.

## 3. On disk

The store owns a directory, a root (io.md, section 5):

- **`snapshot`:** the map in key order, with the commit number its replay
  starts from (section 5), in checksummed chunks and a trailer;
  replaced atomically.
- **Log segments,** `log-<first number, 20 decimal digits>`: frames
  appended in commit order. New segments are announced with a directory
  sync before any commit in them is acknowledged.

| A frame's field | Bytes |
|---|---|
| magic `SKVC`, format version | 4, 2 |
| commit number | 8 |
| op count, body length | 4, 4 |
| ops: kind (1), key length (4), key, and for a put value length (4), value | body |
| CRC-32C of everything before it | 4 |

**Recovery** reads the snapshot, then applies the frames of the segments
from the snapshot's start number, accepting the longest valid prefix: it
stops at the first frame that is short, fails its checksum, or is not the
next number. Nothing after that point was ever answered as committed. A
new segment starts after recovery; a segment with a torn tail is never
appended to, and is deleted with the others the next snapshot covers.

## 4. Commits

- **Group commit.** While a sync is in flight, commits queue; the next
  write carries every queued frame, followed by one sync, and each is
  answered once that sync completes. Queued bytes are bounded.
- **A failed sync is fatal to the open store.** After a failed `fsync`
  the kernel may have dropped the dirty pages and will not report the
  error again, so a retry proves nothing. The store stops admitting,
  answers what is in flight `Failed`, settles its I/O, and recovers from
  the disk as at a start. The owner learns an uncertain commit's outcome
  from the recovered number.
- **A failed or short write** is continued or treated as a failed sync;
  either way no commit is answered unless every byte before its sync
  barrier is written and synced.

## 5. Snapshots

- **When:** the log since the last snapshot passes a bound (a multiple of
  the snapshot's size, or a fixed size), which bounds both disk use and
  the replay a restart does.
- **Fuzzy, so nothing is frozen.** Frames carry whole values, puts and
  erases, so applying a frame twice is harmless. A snapshot records the
  next commit number as it starts, S, then writes the live map in key
  order, a chunk per step, resuming after the last key written, while
  commits go on. A key that changes behind the cursor is put right by
  replaying the log from S; one ahead of it is written as it then is.
  There is no frozen copy, and memory does not double.
- **Finishing** follows io.md's replacement sequence, with the writes a
  chunk at a time: create the temporary, write, sync, close, rename over
  `snapshot`, sync the directory. Only then are the segments wholly
  before S deleted.
- **A crash during a snapshot** leaves the previous snapshot and every
  segment since it, so recovery is unchanged.

## 6. Payload logs beside the store (planned)

For values that are large, appended to, and read rarely (temper's
transcripts), and so are not held in memory:

- **One log file per owner key,** frames each with a sequence number and
  a checksum, in a directory beside the store's.
- **Data first, then the commit.** The service appends the frames and
  syncs them, then commits a store record holding the log's committed
  extent (its length and last sequence), with whatever else that decision
  changes. The store's commit is the point of truth.
- **Past the extent is garbage:** bytes of a write that never committed.
  Readers stop at the extent, and the next append writes at the extent,
  over them, so nothing is ever truncated.
- **Removal:** a log whose extent record is erased is removed once that
  commit is durable; at recovery, a log with no extent record is removed.
- **Bounds:** a size per log, the open logs, the bytes in flight.
- **In the vocabulary:** two ops, `Extend { log, length }` and
  `Drop { log }`, which the store keeps in a key space of its own; and
  two requests, `Append { log, bytes }`, written at the committed extent
  and synced, answered with the length to commit, and
  `ReadLog { log, offset, max }`, never past the extent.
- **What it needs of io:** opening an existing file to write, in the
  kernel's records, the ring, the simulator and the conformance suite.

## 7. Memory and bounds

- **The budget** covers the map's keys, values and per-entry overhead,
  queued commits, the frames being written, a snapshot's chunk, and
  recovery's buffers. Recovery that would pass it fails the start, saying
  so.
- **The worst case is declared** (programming-model.md) from the limits:
  key and value sizes, ops and bytes per commit, queued bytes, segment
  size, chunk size, and the budget itself. `budget` limits the map; the
  `worst_case` declaration includes queue, recovery, snapshot and page
  buffers in addition to the map.
- **Loop latency** stays bounded: a commit applies its ops to the map, a
  load returns a page, a snapshot writes a chunk per step.

## 8. What it needs of io

io.md's whole-file operations hold a file's content in one request. The
store needs more of io, which its file requests (`skein_io::file`) give:

- **an open file written at its end and synced,** kept open across
  requests: the log segments and the payload logs;
- **a file written a chunk at a time,** then replaced over its target as
  io.md section 5 orders it: the snapshot;
- **reads by offset** up to a stated length: recovery, and payload logs
  read back;
- **creating a file with given permission bits** (a store of secrets is
  `0o600`), and removing and listing.

The fake machine behind the simulator models what a crash keeps: synced
data; of what is not synced, any writes in any order, the last torn at
any byte; a directory entry only once its directory is synced; a rename
whole or not at all. Its faults include failed and short writes, and
failed syncs.

## 9. Verification

- **The machine's world** runs commits, loads and snapshots against a
  model map, on the simulated disk, crashing at every operation and every
  sync boundary, then recovering. Its referee: the recovered map is the
  last acknowledged commit, or that plus a whole uncertain one in flight,
  never part of a commit; numbers contiguous; every load equal to the
  model's.
- **Snapshots under load:** commits racing a snapshot at every chunk,
  crashes at each phase of its replacement.
- **Payload logs, later:** a crash between the data's sync and the
  commit leaves the extent as it was, and the next append overwrites the
  tail.
- **Over the real ring:** a scratch-directory integration test commits,
  closes, reopens on the same root descriptor and verifies the value. A
  killed process leaves the page cache, so the ring shows integration,
  not power loss: that is the crash model's (section 8), which no test
  against a kernel can check.

## 10. For temper

- **The engine's store** (temper's `domain/engine.md`, section 5): the
  children's records and keys as values and keys, the root's commit as
  one `Commit`, loads by key range at restart; secrets in a second store,
  in a root of their own, `0o600`.
- **Transcripts** as payload logs: a turn's bytes appended and synced,
  then committed with its spend and the messages it took; the worker's
  acknowledgement after that commit, as today.
- **Retention is temper's:** ended tasks moved out of the map, or into
  payload logs, past a horizon, so the budget holds for years.

### 10.1 How large, roughly

Back of the envelope, for a busy small team's deployment: a few people,
tens of repositories, about **300 tasks a day**, two thirds of them
agents' (bounded in practice by LLM spend: a few hundred dollars a day
at one or two dollars a task), the rest procedures and people's.

| What | Per task | Per year (~110k tasks) |
|---|---|---|
| the task: spec, contract, lineage, state, result | 2–4 KB | |
| messages kept (most change tasks hear none; chats and coordinators many) | 1–5 KB | |
| effects and their keys, connector state for its resources | 1–3 KB | |
| funding and spend | ~0.5 KB | |
| the map's per-entry overhead (~15 entries) | ~1.5 KB | |
| **the map, no retention** | **5–15 KB** | **0.5–1.7 GB** |
| **the map, ended tasks cut to a ~1 KB summary after 30 days** | | **~0.1–0.2 GB live, plus ~0.1 GB of summaries a year** |
| transcripts: 20–100 turns of 5–30 KB, bounded by the context window | ~0.2–1 MB an agent run | **~20–40 GB**, ~5–10 GB compressed |

**Room for what comes later.** Connectors beyond the forge (test
environments, deployments, CI systems, issue trackers, chat, cloud
accounts) add resources with state of their own (an environment, a
release, a rollout: 1–5 KB each), news from their events, subscriptions,
and procedure tasks (provisioning, deploying, rolling back, tearing
down), perhaps doubling the tasks a day. Sizing for **three to five
times the forge-only figures** gives:

| What | Per year |
|---|---|
| the map, no retention | ~2–8 GB |
| the map, with the 30-day horizon | ~0.5–1 GB live, plus ~0.3–0.5 GB of summaries a year |
| transcripts | ~50–150 GB, ~15–40 GB compressed |

So a budget of **1–2 GB for the map** covers a busy deployment with
several connectors and a retention horizon, for years; a store that is
designed to reach about 10 GB in memory before its limits bite leaves
room for the cases this estimate is wrong about. Past that, retention
tightens first, then redb.md's approach or an on-disk index.

- **The map fits in memory comfortably** with a retention horizon, and
  even without one for the first year or two on a server; the live
  working set (tasks not ended) is tens of megabytes, a few hundred with
  several connectors.
- **Transcripts do not,** which is why they are payload logs: a few
  hundred megabytes a day, on disk, with a retention of their own.
- **Write rate is low:** tens of thousands of commits a day (task
  changes, turns' extents, effects), under one a second on average and
  tens a second at peaks; with group commit, one sync of a few
  milliseconds covers a burst. The log grows by roughly 10–20 MB a day
  before snapshots fold it.
- **Restart** reads a snapshot of a few hundred megabytes, about a
  second from an SSD, and a log bounded by the snapshot's trigger.

## 11. Where the risk is

The store itself is small: a map, a frame codec, group commit, a
recovery rule, a chunked snapshot and payload logs' extents. The risk is
around it:

- **a faithful crash model** in the simulator, since the tests are only
  as good as what it says a crash keeps (section 8);
- **sync failures** handled as fatal, everywhere a sync is awaited
  (section 4);
- **the fuzzy snapshot's argument** (section 5), the one part that is not
  obvious;
- **memory accounting** close enough to the allocator's to make the
  budget mean something (section 7).

## 12. Settled choices and later work

- Frames and snapshot chunks use version 1 and CRC-32C. Snapshot files
  have a checksummed trailer; recovery rejects one without it.
- `Limits` supplies fixed segment and snapshot thresholds, and a timeout
  for each file request. A hung kernel operation is cancelled; its owner
  receives one terminal failure after the operation settles.
- One store owns one directory and one file driver.
- Later, when a user needs them: payload logs (section 6); several
  stores sharing a sync; reads at one commit across pages, for a
  consistent view of many; `Load` in reverse order, where reversed keys
  do not serve.
- When the dataset outgrows memory, retention comes first, then values
  on disk: keys in memory with where their values lie, values read by
  offset, compaction in place of snapshots, and the index rebuilt at
  start from hint files. `Get` and `Load` then answer after their reads,
  not in the step that asks; the rest of the vocabulary stays. The
  approach in redb.md is the other way past memory.
