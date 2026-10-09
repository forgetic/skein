# The kernel boundary

Provisional, 2026-10-03, revised 2026-10-09. The design of the boundary
below io: the records io submits to a backend, the completions the
backend hands back, and what every backend promises. The records live in
`skein-io`'s `kernel` module, whose module documentation states each rule
of the contract exactly; this document is the design behind it.

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
pub enum Done {
    Nothing, Count(u32), Fd(Fd), Accepted { fd: Fd, peer: Addr }, Bound(Addr),
    Stat(Stat),                       // kind, size, mode, owner, links (6.1)
    Spawned { pidfd: Fd }, Exit(Exit), ServiceSignal(ServiceSignal),
    Usage(Usage), Window(Window),     // the process's own (6.3)
}

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
    Append        { fd: Fd, bytes: Box<[u8]>, from: u32 },       // at the end (6.1)
    Sync          { fd: Fd },
    Stat          { fd: Fd },
    Rename        { from_dir: Fd, from: Box<[u8]>, to_dir: Fd, to: Box<[u8]> },
    Remove        { dir: Fd, name: Box<[u8]>, directory: bool },
    MakeDirectory { dir: Fd, name: Box<[u8]>, mode: u32 },     // less the umask (io.md, 5.3)
    List          { fd: Fd, entries: Box<[Entry]>, names: Box<[u8]> },  // synchronous
    // processes (section 6.2)
    Spawn     { spawn: Box<Spawn> },                         // synchronous
    Wait      { pidfd: Fd, reap: bool },
    Signal    { pidfd: Fd, signal: Signal, to: Target },     // synchronous
    PipeRead  { fd: Fd, buf: Box<[u8]> },
    PipeWrite { fd: Fd, bytes: Box<[u8]>, from: u32 },
    // the process's own (section 6.3)
    ReadSignal { fd: Fd },
    Usage,                                                   // synchronous
    Window     { fd: Fd },                                   // synchronous
    Cancel     { target: Token },
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
  - **The backend allocates nothing that goes up.** A `Spawn` comes back
    with the parent's end of each requested pipe written into its own
    pipe table, and its completion carries only the child's descriptor.
    What a process owns is then always allocated within its own calls
    (simulator.md, 5).
- **Kernel structures belong to the backend.** A socket address, a
  `statx` buffer, a `siginfo`, an `rusage` and an `open_how` live in the
  backend's in-flight table, beside the record. They are decoded into
  plain values (`Addr`, `Stat`, `Exit`, `Usage`) before they go up. A
  backend for another kernel translates.
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
- **Some operations are synchronous:** spawning, signalling, listing a
  directory, reading the process's resource usage and its terminal's
  size. None of these is a ring operation at the kernel floor (shell.md),
  and some are not ring operations at all. The backend performs them when
  they are submitted and completes them at the next reap, in the same
  records, so io cannot tell the difference.
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
  asks: `RESOLVE_BENEATH` refuses them today, but may not always. That
  answers `ELOOP` before the escape is looked at, so a magic link is
  `TooManyLinks`, not `Escape`.
- **A `..` that races is tried again.** `RESOLVE_BENEATH` cannot tell a
  `..` from an escape when a rename or a mount anywhere on the system
  moves under it, and answers `EAGAIN` for the caller to retry. The
  backend does, not io: the ring pushes the same `Open` again from its
  slot, up to 16 times, and only one still racing goes up, as
  `Other(11)`. io_uring's worker meets the race only once a quick attempt
  inline has too, so it is rare, about one `Open` in a thousand under a
  storm of renames; the ring's own tests provoke it with renames in its
  workers beside creates through `..`. The simulator has no race, and
  never answers it.
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
- **`Append` is the one write at the descriptor's position.** It is for a
  file written as a stream (io.md, 5.1): a descriptor the shell opened at
  startup to write at its end (`O_WRONLY | O_APPEND`, created with the
  mode its configuration gives), or a standard output that is a regular
  file. The ring writes at the position (offset −1): with `O_APPEND` the
  kernel moves it to the file's end before each write, so each `Append`
  lands at the end, after whatever another process appended meanwhile;
  without it, after the last write. It may be short, and io continues
  from its count, as a `Write`. One is in flight on a descriptor at a
  time, so the pieces of one stream land in order. No `Open` makes such
  a descriptor yet: opening one beneath a root while the service runs
  comes when a user needs it.
- **`Stat` answers five values:** the kind, the size, the permission
  bits, the owner (its user ID) and the number of links, from one
  `statx`. The checks of a private file need the last two: one link, and
  the owner of the directory it lies in (io.md, 5.3).
- **`Rename` is the atomic step.** It replaces its target whole, so the
  idiom that replaces a file is: `Stat` the old file for its permission
  bits, `Create` a temporary in the same directory with them, `Write` it,
  `Sync` it, `Close` it, `Rename` it over the old name, `Sync` the
  directory. A reader sees the old file or the new, and after the last
  `Sync` the new one survives a crash. Its pieces are io's decisions
  (io.md, 5): the temporary's name drawn from the seed, drawn again on
  `Exists`; the old file's bits kept, which is why `Create` takes a mode
  and `Stat` answers one (and why `MakeDirectory` takes one too: a private
  directory is made `0o700`, io.md, 5.3); a symbolic link at the target replaced by the
  file, as `Rename` replaces a link and does not follow it, since writing
  through links is not offered.
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
- **An `Open`, `Read`, `Write`, `Append` or `Sync` may be cancelled.**
  None waits on a peer, but on a filesystem that can stall (NFS whose
  server went away, a FUSE daemon that hangs) one may wait for good, and
  io must be able to give up on it. So a `Cancel` may target one, with a socket's
  outcomes (section 5): it stops one still queued for io_uring's worker,
  or interrupts one running, or comes too late, the target answering its
  own result. Even on tmpfs a `Read` goes to the worker, and the ring's
  conformance run sees its `Cancel` stop it in some runs and come too late
  in others. The ring reads `ECANCELED` and `EINTR` as `Cancelled` on an
  operation a `Cancel` was submitted for. A `Stat`, `Rename`, `Remove`,
  `MakeDirectory` or `List` completes without one: a `Cancel` of one is a
  broken invariant, until a filesystem that stalls them pulls it.
- **Only files and directories open.** A FIFO beneath a root opened to
  read opens at once, and its first `Read` then waits for a writer's
  bytes for good, which nothing above can stop. So every `Open` goes down
  with `O_NONBLOCK | O_NOCTTY`, and the ring stats what it opened, at once,
  in its own call, as a `List` makes its own: anything but a regular file
  or a directory (a FIFO, a device) is closed and answered `NotAFile`,
  and a regular file is made blocking again, so that io_uring sends a read
  it cannot do at once to its worker rather than answer `EAGAIN`. A
  socket, or a device with no driver, the kernel itself refuses (`ENXIO`,
  `NotAFile` too). A device's own `open` may still act (a tape rewinds),
  so roots belong on filesystems mounted `nodev`, where the kernel refuses
  every device with `Permission`; the shell's startup says so
  (shell.md, 6).
- **Errors are named per operation,** on a table the module documentation
  keeps and `Complete::is_valid` checks: an operation on files answers its
  own errors, never a socket's or `Cancelled`, and an operation on sockets
  never answers a file's. The same number means different things per
  operation: `EXDEV` is `Escape` on an `Open` and `Other` on a `Rename`
  (two filesystems); `ENOENT` is `NotFound` on a file and `TooLate` on a
  cancel; `EEXIST` is `Exists` on a `Create` and `NotEmpty` on a `Rename`
  over a directory.
- **Modes are backend defaults,** as socket options are, but for the one
  a `Create` asks for: a new file is `0o666` unless asked otherwise, and a
  new directory `0o777`, less the process's umask, and every descriptor
  is close-on-exec. The fake machine keeps a umask of `0o022`; the suite
  compares a created file's owner bits, which no usual umask takes.

### 6.2 Processes

- **A child is a pidfd from its spawn,** and the leader of a process
  group of its own (io.md, 6). `Spawn` is synchronous: glibc's
  `pidfd_spawn`, with the group set in its attributes.
- **`Wait` observes an exit, or reaps it.** Both are the ring's `waitid`
  on the pidfd. A `Wait` that does not reap (`WEXITED | WNOWAIT`)
  completes with the child's exit and leaves it a zombie, which keeps its
  PID, and so its group's ID, from being given to another process. A
  `Wait` that reaps (`WEXITED`) follows one that observed the exit, at
  io's close of the child, and completes at once. Until it is reaped, a
  child's resource usage is not among the process's children's (6.3).
- **`Signal` goes to the child, or to its group,** both through the
  child's pidfd: `pidfd_send_signal`, with no flag for the child, and
  with `PIDFD_SIGNAL_PROCESS_GROUP` for the group the child leads, which
  the kernel floor has. No record carries a PID: the pidfd names the
  child, and through it the group the child leads, so no signal reaches a
  stranger. The unreaped leader is still a member of its group, so a
  signal to a group whose other members are gone succeeds and changes
  nothing.
- **Pipes** are the parent's ends of what `Spawn` made, read and written
  through the ring, as sockets are, with a pipe's errors.

### 6.3 The process's own

- **`ReadSignal`** reads one blocked signal from the signalfd the shell
  opened at startup: `SIGINT`, `SIGTERM`, and `SIGWINCH` for a service at
  a terminal (io.md, 7; shell.md, 6.3).
- **`Usage` reads what the process has used,** in one synchronous call:
  `getrusage` for the process itself (`RUSAGE_SELF`) and for its reaped
  children (`RUSAGE_CHILDREN`). Each part answers user and system CPU
  time, as `Duration`s, and the peak resident size, in bytes (the kernel
  counts it in KiB, which the backend multiplies, checked). The
  children's CPU is the sum over every child reaped so far, and over what
  each of them reaped in turn; their peak is the largest any one of them
  reached. A child that has exited but is not yet reaped is in neither
  part.
- **`Window` reads a terminal's size,** rows and columns, from the
  descriptor it names (`TIOCGWINSZ`). The shell found at startup that the
  descriptor is a terminal (shell.md, 6.3); one that has since hung up
  fails with the kernel's error, as `Other`.

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
- a cancel of a stat, a rename, a removal, a new directory or a listing;
- a close with anything else in flight on its descriptor, an operation on
  files being on every descriptor it names;
- an `Append` beside another in flight on its descriptor, or on one not
  open to write;
- a `Wait` beside another on the same pidfd, or one that reaps before a
  `Wait` has reported the exit; any operation on a pidfd after its reap
  but its close;
- a cancel of a spawn, a signal, a usage or a window.

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
  forty-four paths that leave their root, stay beneath it, or name a FIFO,
  which opens as no file; and what the
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
  scenario on the simulator only. A `Cancel` of a file's `Read` runs on
  both: the ring stops it or comes too late, from one run to the next,
  and requires a pairing the simulator draws; the simulator's `hung`
  fault, a filesystem gone away, adds the `Read` that waits for good,
  which no scratch directory shows, and every pairing must appear over
  the fuzzy seeds. `EMLINK` (`TooManyLinks` on a `Rename`
  or a `MakeDirectory`) neither backend provokes. A `Rename` across
  filesystems (`Other(EXDEV)`) is the ring's own test, between the
  temporary directory and `/dev/shm` where they are two mounts. Short reads and writes
  are an outcome the simulator draws and the ring never gives on a
  regular file: the fuzzy suite asserts both counts appear over its
  seeds, and a calm world counts every byte, as the ring does.
- **Appending.** A scenario opens a file to append as the shell does, in
  its own root: `Append`s land at the end in order, after bytes another
  descriptor appended between them, and a file that was not empty keeps
  what it held; a short `Append` is continued from its count; a `Cancel`
  stops one or comes too late, as for a `Write`. The suite's tree gains a
  second name for a file (a hard link), so that `Stat` answers two links
  for it, one for the rest, and the owner every scenario's files have.
- **Processes,** with the fixture program of both backends (echo its
  input, exit with a status, never exit): pipes through chosen
  descriptors, an exit status kept, a `Wait` that waits while the child
  lives, a signal, a kill. To these the groups add: the fixture starting
  a child of its own in its group and exiting; a `Wait` that observes the
  exit, then a signal to the group, which ends the grandchild and its
  hold on a pipe; then the reaping `Wait`. Usage, in a process that has
  reaped no child before: the children's part zero, still zero after the
  observing `Wait`, and with a peak above zero after the reap; and the
  process's own CPU and peak never falling between two reads.

## 9. Open questions

- **Fixed files** change what an `Fd` names: a slot in the ring's table
  instead of a descriptor. Decide when it has been measured.

## 10. Not built yet

- **The descriptor limit on the ring.** The simulator checks it; lowering
  a process's limit on the real kernel takes `unsafe` outside the ring
  adapter, or a child process, and neither is allowed.
- **The records io's next parts pull** (io.md, 9): `Append`; `Stat`'s
  owner and links; a `Wait` that does not reap, and `Signal`'s target,
  the group; `Usage` and `Window`. Today a `Wait` reaps, and a `Signal`
  reaches the child alone. Each comes with its conformance scenario
  (section 8) on both backends.
