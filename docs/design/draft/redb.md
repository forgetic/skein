# redb over Skein's loop

Draft, 2026-10-05. A possible single-threaded redb integration. This is
an exploration, not yet a contract for skein-io or a decision to add redb
to skein. Read it with [the programming model](../../foundation/programming-model.md),
[the testing strategy](../../foundation/testing-strategy.md), and
[the file design](../io.md).

## 1. The seam

redb's `StorageBackend` is pluggable, but its `len`, `read`, `set_len`,
`write`, and `sync_data` methods return synchronously. Skein submits file
operations to the kernel and receives completions in later loop
iterations. A backend that submits an io_uring operation and waits inside
one of those methods would keep the single thread from servicing the rest
of the loop.

The proposed backend therefore serves every redb call from a virtual
file in memory. It applies writes to that file immediately and records
the storage operations, including their order and sync boundaries. A
separate persistence machine submits the recorded operations through
Skein's file I/O and reports the client write complete only after the
required disk sync completes. The backend's synchronous success means
that the *memory* operation succeeded; it is not a claim that the client
write is durable. The adapter, rather than redb's `commit()` return,
owns that external durability promise.

Only one Skein loop owns the database, its virtual file, and the disk
file. Requests are classified before entering redb: a read uses
`Database::begin_read()`, while any request that may modify data uses
`Database::begin_write()`. A write transaction may also read. A redb
read transaction cannot turn into a write transaction midway through a
request. The adapter serializes writers itself, so `begin_write()` is
never called while another write transaction exists; redb would block in
that case.

## 2. Two views during persistence

At rest, the virtual file and the durable disk file represent the same
redb commit logically; their unreferenced bytes need not be identical.
To start a write:

1. Open a redb read transaction before the write. This pins the last
   durable view.
2. Run the client's write transaction against the virtual file. Apply
   backend reads and writes synchronously in memory and capture all
   storage mutations and sync boundaries, including writes made before
   `commit()` and any pending mutations from an earlier aborted attempt.
3. Commit redb in memory. Its live database now has the new view. Keep
   the pinned read transaction and withhold the client's reply.
4. While the persistence machine writes to disk, route new read requests
   through the pinned read transaction. Queue or refuse further write
   requests according to a configured limit.
5. Once persistence and its final sync complete, release the pinned read
   transaction, expose redb's new view to reads, and report the write's
   durable success. Drain queued writes one at a time.

redb's MVCC gives a read transaction its own root and keeps the pages it
can reach from being reclaimed while it lives. A read request during
persistence must use that pinned transaction; calling `begin_read()` on
the live database then would see the speculative commit. An iterator or
other borrowed redb value stays within its request. If reads ever span
requests, their lifetimes and view switches need explicit ownership.

The virtual file must **not** be reverted to its pre-write bytes after
redb commits. Redb has already advanced its own roots, allocator, and
cache state. Replacing the backing bytes under that live handle would
make those states disagree. The old view belongs to the pinned read
transaction, not to a rewritten backing store.

```
                     virtual file       client-visible reads     disk file
ready                commit N           commit N                 commit N
persisting N+1       commit N+1         pinned commit N         N, then replay
synced N+1           commit N+1         commit N+1               commit N+1
```

## 3. The virtual backend

The backend holds a bounded virtual file and supports redb's byte-offset
operations, including exact reads, zero-filled growth, truncation, and
length queries. Its `write` copies the borrowed input before returning;
it cannot keep redb's slice for later I/O. Its `read` fills redb's
borrowed output before returning. A write trace owns its bytes and
records `set_len`, `write`, and `sync_data` in call order. The adapter
also has to account for backend operations during database creation,
open, recovery, compaction, and close, rather than assuming that only
client write transactions touch storage.

`StorageBackend` requires `Send + Sync` and takes `&self` for these
methods. The virtual file and trace therefore need safe interior
mutability, such as a mutex scoped to this adapter, even though the
service calls it from one thread. Its lock must never be held while
calling back into redb or waiting for Skein I/O.

The first implementation could keep the whole file in RAM. A paged or
copy-on-write representation may reduce copying later, but neither
changes the transaction protocol. The limit must include the virtual
file, redb's own cache, pages pinned by readers, captured write bytes,
pending requests, and buffers owned by kernel operations. Growth beyond
the admitted limit is an error before the adapter promises success.

Treat this as an in-memory redb backend with an external persistence
protocol. `sync_data()` is a memory-side ordering marker for the
adapter. It must not be exposed as a normal persistent `StorageBackend`
whose redb `commit()` return is advertised as disk durability. Use
redb's immediate commit path so the trace contains every write and sync
barrier required for that commit. `Durability::None` has different redb
semantics and is not the basis of this design.

## 4. Replaying a commit

The persistence machine consumes an immutable trace. For in-place replay,
this is the ordered backend history since the last durable sync, not
necessarily only the calls made by the successful client transaction.
An abort can leave changed but unreferenced bytes in the virtual file;
discarding its storage calls without proving that no later commit uses
them would break the correspondence with disk. The machine performs every
file length change and write in the order needed by redb, and does not
cross a recorded sync barrier until all earlier operations have
completed. An io_uring submission order alone does not establish that
completion order. Short writes are continued; errors and late
completions are accounted for before buffers or file handles are
released. The final success event requires the disk sync corresponding
to redb's completed commit.

redb's crash-safety argument depends on how its page writes, header
update, and syncs reach storage. The trace replay must preserve those
dependencies. It must be tested by crashing after each operation and
after each sync boundary, then reopening the on-disk database with
redb. A trace is a storage-operation log, not an application-level
transaction log; replay code must not coalesce or reorder writes until
that transformation has its own correctness argument.

A simpler persistence variant is to freeze a complete, valid virtual
file image, write it to a temporary file, sync the file, rename it over
the database file, and sync the directory. This avoids in-place replay
ordering, but copies the entire database per commit and needs proof that
the captured image is reopenable while the redb handle remains open.
Neither variant allows another process to open or modify the physical
file during operation. Multi-process redb locking and cache
invalidation are outside this draft.

The current `skein-io` high-level file requests have not been built, and
the kernel records lack some operations needed for in-place redb replay,
such as opening an existing file read-write and changing its length.
Adding those records, their ring and simulator implementations, and the
file machine above them is part of this approach. The whole-image
variant instead uses the planned atomic replacement sequence in
`io.md` section 5.

## 5. Failure and visibility

The adapter has one pending durable write at a time. Its states are:

| State | Reads | Writes | Client write result |
|---|---|---|---|
| Opening | unavailable | unavailable | unavailable |
| Ready | new redb read transactions | admit one | pending |
| Persisting | pinned old read transaction | queue or refuse | pending |
| Recovering | unavailable | unavailable | failed or uncertain |

If the redb memory transaction aborts before commit, keep the old logical
view. Carry its backend mutations forward for the next replay, rebuild
the virtual file from the durable image, or use a complete-image
checkpoint. Which mutations can safely be omitted from a delta trace
requires proof against redb's actual call patterns.

If disk persistence fails after the memory commit, the live redb handle
is ahead of disk. It cannot be repaired by restoring
old bytes in place. Stop admitting requests, settle outstanding I/O,
discard that handle, and reopen from the disk state redb recovers. A
failed sync can leave the commit's outcome uncertain; the client must not
receive a durable-success reply based only on redb's memory commit.
An application transaction identifier stored in the same redb write can
help identify the outcome after recovery, but its presence after a failed
sync alone is not proof of power-loss durability.

Cancellation after the memory commit does not undo persistence. The
machine must finish or recover the disk operation and deliver exactly
one terminal outcome. A timeout may stop the client waiting, but the
pending write still holds its buffers and its pinned old view until it
settles. Shutdown likewise waits for the in-flight persistence or
enters recovery on the next start.

The ordering of client-visible effects matters: a reply, event, or
outbound request caused by the new data must wait for the same durable
completion if it promises that data survived a crash. Reads served
during persistence see commit N; after durable completion they see N+1.

## 6. Place in Skein

This is an adapter at the service/shell boundary, not an ordinary
`no_std` step crate. redb brings its own internal state, traits, locks,
allocation, and destructors, contrary to the small-Rust rules for
Skein's pure machines. A service could exchange bounded database
requests and result records with the adapter, while the service loop
invokes redb only against memory and drives disk work through Skein's
file records. The adapter must state this exception openly; it cannot
claim that redb execution itself is a pure Skein step.

Even with disk I/O deferred, a large redb operation or commit can use
unbounded CPU time relative to one Skein iteration. Admission limits
on request size and operations per transaction control inputs, but
redb's internal work also depends on database size and history. The
implementation must measure loop latency and set an acceptable bound
for its workload. If strict per-iteration work bounds are required,
stock redb is not enough: its call stack would need yield points.

## 7. Verification before adoption

- Check the exact redb version's backend call patterns for open, read,
  write, abort, commit, close, and compaction. Pin the version while
  validating the persistence protocol.
- In a memory-only world, assert that a read pinned before a write keeps
  returning N while the live database is at N+1, including table scans
  and page reclamation pressure.
- In simulated worlds, inject short writes, failures, cancellation, and
  crashes at every recorded storage operation and sync boundary. Reopen
  the physical image with redb and check that it is either the last
  acknowledged commit or an allowed uncertain in-flight commit, never
  an invalid or partial client-visible state.
- Run the same protocol over the real ring, including process kill and
  restart around each persistence phase. Compare acknowledged writes
  with reopened contents and measure service latency during redb calls.

Relevant redb interfaces and rationale:
[`StorageBackend`](https://docs.rs/redb/latest/redb/trait.StorageBackend.html),
[`Database` and explicit transactions](https://docs.rs/redb/latest/redb/struct.Database.html),
and [redb's design](https://github.com/cberner/redb/blob/master/docs/design.md).
