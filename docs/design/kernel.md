# The kernel boundary

Provisional, 2026-10-03. The design of the boundary below io: the records
io submits to a backend, the completions the backend hands back, and what
every backend promises. The records live in `skein-io`'s `kernel` module,
whose module documentation states each rule of the contract exactly; this
document is the design behind it.

## 1. In one page

- **Records, not calls.** An operation goes down, and its completion
  comes up, handing the operation back. This is all the ring backend, the
  simulator and any later backend share, and all they implement.
- **Memory moves with the operation.** A buffer the kernel reads or
  writes is a `Box` inside the record, held by the backend until the
  completion hands it back.
- **Plain values only.** Kernel structures and error numbers stay in the
  backend; io sees addresses, counts and a skein error enum, and needs no
  `libc`.
- **Every operation completes exactly once,** cancelled or not, and a
  cancel is an operation of its own.
- **Sockets behave as Linux's do,** and the simulator matches them; one
  conformance suite holds every backend to the contract.
- **Files stay beneath a root.** A root is an open directory. Only `Open`
  takes a path, which the kernel resolves beneath its root; every other
  operation on a name acts on one entry of an open directory.

## 2. In skein

```
                    io (step code)
           Submit ↓              ↑ Complete
   ┌───────────────┬────────────────┬───────────────────────────────┐
   │ ring          │ sim            │ readiness (later)             │
   │ io_uring      │ the simulated  │ wait for readiness, make the  │
   │               │ kernel         │ syscall, complete             │
   │ production    │ tests          │ hosts without io_uring        │
   └───────────────┴────────────────┴───────────────────────────────┘
```

io (io.md) is the only user of the records. The ring and the readiness
backend live in the shell (shell.md); the simulator is simulator.md. The
records are step-code types, in `skein-io`, so that io can name them and
the backends can depend on io.

## 3. The records

```rust
// skein-io's kernel module, a sketch
// io -> kernel
pub struct Submit   { pub op: Token, pub kind: Op }
// kernel -> io: the operation handed back, and what came of it
pub struct Complete { pub op: Token, pub kind: Op, pub result: Result<Done, Error> }

// one success shape per operation
pub enum Done { Nothing, Count(u32), Fd(Fd), Accepted { fd: Fd, peer: Addr }, Bound(Addr), Stat(Stat) }

pub enum Op {
    // sockets
    Socket   { family: Family },
    Bind     { fd: Fd, addr: Addr },
    Listen   { fd: Fd, backlog: u32 },
    Accept   { fd: Fd },
    Connect  { fd: Fd, addr: Addr },
    Recv     { fd: Fd, buf: Box<[u8]> },
    Send     { fd: Fd, bytes: Box<[u8]>, from: u32 },
    Shutdown { fd: Fd },
    Close    { fd: Fd },
    // files, beneath a root (section 6.1)
    Open          { root: Fd, path: Box<[u8]>, how: OpenHow },   // Read, Directory, Create
    Read          { fd: Fd, buf: Box<[u8]>, at: u64 },
    Write         { fd: Fd, bytes: Box<[u8]>, from: u32, at: u64 },
    Sync          { fd: Fd },
    Stat          { fd: Fd },
    Rename        { from_dir: Fd, from: Box<[u8]>, to_dir: Fd, to: Box<[u8]> },
    Remove        { dir: Fd, name: Box<[u8]>, directory: bool },
    MakeDirectory { dir: Fd, name: Box<[u8]> },
    List          { fd: Fd, entries: Box<[Entry]>, names: Box<[u8]> },  // synchronous
    // processes
    Wait     { pidfd: Fd },
    // ... spawn, signal, make a pipe: synchronous (section 6)
    Cancel   { target: Token },
}
```

The completion hands back the operation it answers, buffers and all,
beside its result: a `Done` of the one shape that operation succeeds
with, or an `Error`. Every record comes back up, whatever happened. An
error means the operation did nothing usable: a failed `Socket` or
`Accept` made no descriptor.

## 4. Memory and values

- **Memory moves with the operation.** io moves a buffer down inside the
  record, and the backend holds the record until the operation's
  completion, which moves the buffer back up: filled, or with the count
  sent. A `Recv` comes back with its buffer filled up to the count, a
  `Send` with its bytes untouched.
  - While the kernel holds the buffer, no other code can name it: the
    compiler's half of the rule (programming-model.md, 6.2).
  - The backend's `unsafe` is the other half. It takes addresses only
    from records it holds, and gives a record back only after the
    operation's completion. It never drops, copies or replaces a `Box`.
- **Kernel structures belong to the backend.** A socket address, a
  `statx` buffer, a `siginfo` and an `open_how` live in the backend's
  in-flight table, beside the record. They are decoded into plain values
  (`Addr`, `Stat`, `Exit`) before they go up. A backend for another
  kernel translates.
- **A path or a name is bytes,** a `Box<[u8]>` in the record with no NUL
  in it. The NUL-terminated string the kernel reads is the backend's own
  copy, held beside the record until its completion.
- **Errors cross as a skein enum:** the errors io handles by name, plus an
  `Other` code. Each backend maps its kernel's error numbers onto it, per
  operation: the same number can mean different things on a cancel and
  on a receive.
- **Socket options are backend defaults, not records,** until a service
  pulls one: every descriptor is close-on-exec, a socket that binds gets
  `SO_REUSEADDR`, an IPv6 socket gets `IPV6_V6ONLY` (families never mix:
  it neither binds nor reaches an IPv4-mapped address), connected and
  accepted sockets get `TCP_NODELAY`, and a send never raises `SIGPIPE`.

## 5. Completion and cancelling

- **Every operation completes exactly once,** cancelled or not, with its
  token. Completions arrive in any order, even on one descriptor.
- **Single-shot operations only, to start:** one submission, one
  completion, and every cancel takes an operation slot of its own. The
  completion queue can then be sized from io's operation slab, so it
  cannot overflow, and every receive and accept is a choice io made while
  it had room.
- **A cancelled operation still completes:** as cancelled, or with what it
  did before the cancel landed. Until then, io keeps the entity
  *settling* (programming-model.md, 5.3).
- **A cancel completes too,** before or after its target:
  - it stopped the target, which then completes cancelled. A stopped
    receive, send or accept took nothing: what had arrived waits for the
    next one. A stopped connect may still have reached its peer, which
    sees the connection end when io closes it;
  - too late: the target had completed or could not be stopped, and
    completes with its own result, or cancelled if the kernel interrupted
    it;
  - not submitted at all, when the backend failed, the target running on.

## 6. What every backend does

- **Sockets behave as Linux's do,** and the simulator matches them:
  - a bind answers with the address bound, its port chosen when it asked
    for port 0; two sockets may bind one address, and the second listen
    fails;
  - a full accept queue delays a connect, never refuses it (refused means
    nothing listened there), until it times out; a failed accept takes no
    waiting connection, and a connection closed or reset while it waits
    is still accepted;
  - a send's count is what the kernel accepted, and a half-close goes out
    behind every completed send; receiving still works after it;
  - a receive of zero bytes means the stream ended, which is not proof of
    a graceful close;
  - a reset fails one operation: a send at once, a receive after the
    bytes already received; after it, receives give zero bytes and sends
    and half-closes fail. An end that already received the peer's end of
    stream hears of no reset, and a send after the peer closed may
    succeed, its bytes lost, until the peer's reset arrives, then fails
    without a reset, the connection closed;
  - a descriptor is closed only by a close. Closing with unread data
    resets the peer; closing a listener resets the connections waiting on
    it.
- **Some operations are synchronous:** spawning, signalling, making a
  pipe, listing a directory. None of these is a ring operation at the
  kernel floor (shell.md), and some are not ring operations at all. The
  backend performs them when they are submitted and completes them at the
  next reap, in the same records, so io cannot tell the difference.
- **Timers are not operations.** The shell waits for completions with one
  timeout: the earliest deadline over every layer.

### 6.1 Files beneath a root

What the records promise is stated exactly in the kernel module's
documentation, with each operation's errors; the decisions behind it:

- **A root is an open directory:** one the shell opened at startup
  (shell.md, 6), or one an `Open` of a directory made beneath another. A
  root opened beneath a root is a root like any other: nothing it opens
  leaves it, though its parent's other entries lie just above.
- **`Open` resolves beneath its root, and follows the symbolic links that
  stay there.** It is `openat2` with `RESOLVE_BENEATH` and
  `RESOLVE_NO_MAGICLINKS`: `..` above the root, an absolute path, and a
  link leading out of the root fail with `Escape` (`EXDEV`), checked by
  the kernel as it resolves, so no race with a rename gets out. Links that
  stay beneath the root are followed, since a workspace holds them (a
  repository's `CLAUDE.md -> AGENTS.md`); `RESOLVE_NO_SYMLINKS` would
  refuse those too and keep nothing more in. Magic links (`/proc/*/fd/*`)
  are refused by name, with `RESOLVE_NO_MAGICLINKS`, as the man page
  asks: `RESOLVE_BENEATH` refuses them today, but may not always.
- **Every other operation on a name acts on one entry of an open
  directory.** `renameat`, `unlinkat`, `mkdirat` and `statx` take no
  `RESOLVE_*` flags, and a path with a `/` in it could leave the root
  through `..` or a link. So `Rename`, `Remove` and `MakeDirectory` take a
  name, never a path (no `/`, not `.` or `..`, which `Op::is_valid`
  holds), and `Stat` takes a descriptor. io opens the directory a name
  lies in first, beneath the root (io.md, 5). None of them follows the
  entry it names: removing a link removes the link.
- **The opens io makes, not `open`'s flags.** `OpenHow` is `Read` (an
  existing file or directory), `Directory` (an existing directory: a
  root, or one to list) or `Create` (a new file, exclusively, to write).
  Each is one well-defined case for the simulator and the suite to hold,
  and more come when a user pulls them: opening a path only to stat it
  (`O_PATH`), say, which `Read` cannot when the file is unreadable.
- **Reads and writes are at an offset,** never at the descriptor's
  position, so concurrent ones need no order. Both may be short: a `Read`
  at the end of the file, and whenever the backend says (POSIX allows it;
  Linux does not cut a regular file's read short but at its end, and the
  simulator draws it); a `Write` likewise, continued by io from where it
  stopped, as a `Send` is. An offset that would pass `i64::MAX` is an
  invalid record: the kernel's offsets are signed, and the ring would read
  `u64::MAX` as the descriptor's position.
- **`Rename` is the atomic step.** It replaces its target whole, so the
  idiom that replaces a file is: `Create` a temporary in the same
  directory, `Write` it, `Sync` it, `Close` it, `Rename` it over the old
  name, `Sync` the directory. A reader sees the old file or the new, and
  after the last `Sync` the new one survives a crash.
- **`List` hands back entries as plain values,** `getdents64`'s structures
  staying in the backend: each entry's kind and its name, packed into the
  record's `names`, at most `entries.len()` of them, `.` and `..` left
  out, from where the last `List` of the descriptor stopped. It is
  synchronous: `getdents64` is not a ring operation. A `names` of at
  least 255 bytes always takes the next entry, so a `List` stops short but
  never makes no progress while entries are left; `Count(0)` is the end.
  A filesystem whose names may be longer than 255 bytes (one storing them
  in another encoding) fails a `List` with `NameTooLong` when its next
  name fits in none of `names`, rather than answer the end; entries a
  `List` took are handed back even if the directory's position could not
  be set back after them.
- **Files complete promptly and are never cancelled.** No file operation
  waits on a peer, so io waits for each, as it does a `Socket` or a
  `Close`, and a `Cancel` of one is a broken invariant. What lies beneath
  a root is files, directories and links: a FIFO's `Open` would block,
  and is outside the contract.
- **Errors are named per operation,** on a table the module documentation
  keeps and `Complete::is_valid` checks: an operation on files answers its
  own errors, never a socket's or `Cancelled`, and an operation on sockets
  never answers a file's. The same number means different things per
  operation: `EXDEV` is `Escape` on an `Open` and `Other` on a `Rename`
  (two filesystems); `ENOENT` is `NotFound` on a file and `TooLate` on a
  cancel; `EEXIST` is `Exists` on a `Create` and `NotEmpty` on a `Rename`
  over a directory.
- **Modes are backend defaults,** as socket options are: a new file is
  `0o666` and a new directory `0o777`, less the process's umask, and every
  descriptor is close-on-exec.

## 7. Broken invariants

Some mistakes io never makes, so a backend may assume they never happen:

- an invalid record (an empty receive buffer, a send with nothing left);
- a token already in flight, or a cancel of a cancel;
- an address of the wrong family;
- more than one receive, send or accept in flight on a socket, or
  anything beside a connect;
- any operation but close after a failed connect, a connect on a socket
  that is not fresh, a listen on an unbound socket;
- a half-close on a socket that is not a connection, or during a send;
- an operation on files on a socket's descriptor, or one on sockets on a
  file's; a read on a descriptor not opened to read, a write on one not
  opened to create, a list on one opened to create;
- a cancel of an operation on files;
- a close with anything else in flight on its descriptor, an operation on
  files being on every descriptor it names.

The simulator fails the world on each; the ring checks the first two at
submit.

## 8. Testing

The conformance suite is the contract's executable form
(testing-strategy.md, 5). It is a crate of its own,
`testing/skein-conformance`, over a small backend interface (open a
process, submit, reap, enter, let time pass), and runs against the
simulator (`tests/conformance/sim`) and against the ring on the real
kernel (`tests/conformance/ring`), each of which implements the interface
for its backend.

- **Each scenario is a scripted sequence of records** that returns what it
  saw, and its check names the rule of the contract behind each
  assertion. An answer that waits on the peer's reset or acknowledgement
  is retried until it changes.
- **A driver checks every completion on the way:** valid for its
  operation, one per submission, the record handed back with its buffer
  in the same `Box`, and every descriptor closed at the end.
- **On the simulator,** each scenario runs over calm seeds and seeds of
  every fault loopback can show: a few of chaos in the focused suite, many
  in the fuzzy one (testing-strategy.md, 8). Every outcome a race allows
  must appear over the fuzzy suite's seeds, and a calm world must pair
  each race as the ring does.
- **On the ring,** each scenario runs once, and fails, saying so, if
  io_uring is not usable.
- What no record can observe (close-on-exec, `TCP_NODELAY`, the modes of
  new files) is not checked; `SO_REUSEADDR` and `IPV6_V6ONLY` are, by
  their effects.
- **Files beneath a root of the scenario's own.** Each scenario on files
  lays out its root from a tree in the suite's own vocabulary (files,
  directories, symbolic links, each with its mode): on the ring, a scratch
  directory made per scenario beneath the system's temporary directory,
  with a directory beside it for a link to lead out to, and removed with
  the backend; in the simulator, the minimal fake machine (testing.md, 4).
  The scenarios: a file made, written at offsets over itself and past its
  end, synced, read back and stated; renames over a file, across
  directories, of a file over a directory and the other way, over a full
  and an empty directory, beneath itself, over itself, and the idiom that
  replaces a file whole; removals of files, directories, a link and a file
  open; new directories, and one removed while open; listings whole, one
  entry at a time, and cut short by long names; a root beneath a root;
  thirty-nine paths that leave their root or stay beneath it; and what the
  owner may not do. Each names every error its operations can be made to
  answer on a healthy scratch directory, and, where two could answer, the
  one Linux checks first: a removed directory before a name's length, a
  final `/` before the last name on a create, both directories of a
  `Rename` before its source, its source before its target, a name
  looked up before its directory is written. The driver checks that every
  path, name and buffer comes back in its `Box`, written only where the
  count says.
- **What only the simulator shows:** a disk beyond a healthy one (no
  space, a filesystem gone read-only, an I/O error) is the simulator's
  own tests' (simulator.md, 6), and an `Open` past the descriptor limit a
  scenario on the simulator only. `EMLINK` (`TooManyLinks` on a `Rename`
  or a `MakeDirectory`) neither backend provokes. A `Rename` across
  filesystems (`Other(EXDEV)`) is the ring's own test, between the
  temporary directory and `/dev/shm` where they are two mounts. Short reads and writes
  are an outcome the simulator draws and the ring never gives on a
  regular file: the fuzzy suite asserts both counts appear over its
  seeds, and a calm world counts every byte, as the ring does.

## 9. Open questions

- **Fixed files** change what an `Fd` names: a slot in the ring's table
  instead of a descriptor. Decide when it has been measured.
- **What else lies beneath a root.** An `Open` of a FIFO blocks until a
  writer comes, which no deadline above can stop, since io never cancels
  a file's operation. If a service's roots may hold one, the backend opens
  with `O_NONBLOCK`, or io refuses what a `Stat` says is neither a file
  nor a directory.

## 10. Not built yet

- **The records for processes,** with their rules and their conformance
  scenarios, when io pulls them.
- **The descriptor limit on the ring.** The simulator checks it; lowering
  a process's limit on the real kernel takes `unsafe` outside the ring
  adapter, or a child process, and neither is allowed.
