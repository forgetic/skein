# The shell kit

Provisional, 2026-10-03, revised 2026-10-09. The design of
`skein-shell`: what a service's `main` runs its loop on. The kernel (the
io_uring backend of the kernel boundary, kernel.md), the clock and the
seed; the loop itself, over a service's `Host`, and how it ends; what
startup reads, trust roots among it, and a terminal's modes; later, the
readiness backend. It is
ordinary Rust, and the only `unsafe` in skein that a service runs lives
here, in one module, the ring adapter. (The counting allocator,
test-only, has the other: testing.md, 6.)

## 1. In one page

- **One loop, written once.** skein provides the kernel (open, submit,
  reap), the clock, the seed, and `drive`: the loop of
  programming-model.md, section 2, over a service's `Host` (section 12).
  The service brings its `iterate`, its startup and a per-iteration hook.
  skein's world harness hosts the same `Host`, so a world runs the shell
  as it ships.
- **The end is settlement.** The loop exits once the service holds
  nothing: everything it owns has reported closed and io is empty. Only
  io's deadlines bound that; no timer, grace or thread decides it
  (section 13).
- **The ring adapter makes no decisions.** It maps each record onto one
  submission entry, keeps the record in its in-flight table, and when the
  entry completes decodes the result and hands the record back.
- **One thread, one kernel floor.** The ring is set up for a single
  issuer, and assumes a kernel at or above one floor, with no checking for
  individual features; a probe at startup confirms the floor.
- **Startup refuses rather than degrades:** a worst case past the
  configured memory, or a ring that cannot be set up, stops the service
  before its loop runs.
- **Optimisations stay behind the records.** None changes code above io.

## 2. In skein

`skein-shell` depends on lib, io (for the records), and the only crates
from outside: `io-uring` and `libc`. A service's `main` depends on it and
on the service's own `iterate`.

- **`Host` and `drive` are here** (section 12), moved from `skein-world`,
  which re-exports both. A service's shell then links no testing crate,
  and its worlds host the shell's own `Host`.
- **It reads trust roots as bytes** (6.2). `skein-tls` turns them into its
  configuration, so the shell needs no TLS crate.

## 3. The kernel

- **Open, submit, reap.** `Kernel::open` takes a size, the most operations
  in flight at once (cancels included, as io's operation slab counts
  them); both rings are sized from it, so the completion queue cannot
  overflow, and submit takes no record past it. Submit pushes a queue of
  records and enters the ring once; reap moves completions into a queue,
  up to its room.
- **Waiting is the loop's call.** Submit takes how to wait: not at all
  (work is pending above), until a monotonic deadline (the earliest over
  every layer), or until a completion arrives (no deadline armed, which
  asserts that something is in flight). Timers are not operations: this
  one timeout covers them all.
- **Set up for one thread:** a single issuer, with deferred task running,
  so completions are processed only when the loop enters the ring.
- **The in-flight table** has one slot per operation, allocated once at
  its final capacity and never moved while anything is in flight. A slot
  holds the record and the kernel structures its operation needs, which
  never leave the module. An operation's `user_data` is its slot's index
  and generation, so a completion or an async cancel can never name the
  slot's next operation.
- **Paths are the slot's own.** A record's path or name has no NUL; the
  slot holds the NUL-terminated copy the kernel reads, made at submit and
  freed with the slot, beside the `open_how` an `Open` hands over and the
  `statx` buffer a `Stat` reads back.
- **Synchronous operations run at submit.** A record that is not a ring
  operation (listing a directory: `getdents64` has none; spawning;
  signalling; reading the process's resource usage) is performed when it
  is submitted, and its completion waits in the table's ready list for
  the next reap, so io cannot tell it from a ring operation (kernel.md,
  6). A `List` reads with `getdents64` into one buffer of the table's,
  copies out what fits the record, and sets the directory's position back
  to just past the last entry it took. A `Usage` reads `getrusage` twice,
  for the process and for its reaped children (kernel.md, 6.3).
- **The `unsafe`, and why it is sound,** is stated in the ring adapter's
  module documentation, case by case: the pointers handed to the kernel,
  entering the ring, the socket address casts, the synchronous calls, and
  dropping a kernel with operations in flight, which leaks the table so
  that the kernel never writes into freed memory.
- **Its checks:** the first two broken invariants of the kernel boundary
  (an invalid record, a token already in flight) at submit.

## 4. The kernel floor

- **One floor, no feature checks.** The first floor is 6.12, an LTS
  release that has every ring operation io uses: for sockets, and for
  files `OPENAT2`, `READ`, `WRITE`, `FSYNC`, `STATX`, `RENAMEAT`,
  `UNLINKAT` and `MKDIRAT`, the newest of them from 5.15. An operation
  newer than the floor stays synchronous until the floor moves: making a
  pipe, for example, has been a ring operation only since 6.16; listing a
  directory has none at all. Lowering the floor for a deployment that
  needs it means making the operations newer than the new floor
  synchronous, and io cannot see that change.
- **The probe at startup** checks the ring for every operation it uses. It
  confirms the floor; it does not choose between features. If an
  operation is missing, or the ring cannot be set up at all (io_uring
  missing, disabled, or refused by a seccomp profile), the shell refuses
  to start. Once the readiness backend exists, it takes that backend
  instead.

## 5. The clock and the seed

- **The clock gives two times,** read together once per iteration:
  `CLOCK_MONOTONIC`, for every deadline (the kernel's wait deadline is on
  this clock), and `CLOCK_REALTIME`, for things about the world. The loop
  hands both to the steps in `Env`.
- **The seed** comes from `getrandom`, once, at startup; everything random
  after it is drawn from the layers' state. It fails, and the service
  does not start, when the kernel refuses `getrandom`.

## 6. Startup

Before its loop, a service's `main`, with the kit:

1. reads its configuration, the limits of every layer among it;
2. checks the sum of the layers' worst cases against the configured
   memory, and refuses to start past it (programming-model.md, 6.3);
3. blocks the termination signals that io will read, so they arrive as
   `Shutdown` events (io.md);
4. opens the first roots for io's files, from configuration, with
   `open_root` (io.md, 5; kernel.md, 6.1). A root, a workspace, belongs
   on a filesystem mounted `nodev`: the ring refuses to open a device
   beneath it, but only after the device's own `open` has run. It opens
   the files it appends to beneath them, a trace among them, which io
   adopts as streams once the service is made (io.md, 5.1);
5. resolves the configured peer names into addresses (io.md);
6. reads certificates, keys and root stores for TLS: the machine's trust
   roots, or a DER file (6.2; tls.md);
7. reads the seed and opens the kernel.

What startup reads, its configuration and certificates, it reads with
the kit (6.1). Then `main` makes the service and calls `drive`
(section 12).

`panic = "abort"` in every profile: a panic is fail-stop, and a
supervisor restarts the process.

### 6.1 Reading files at startup

Startup runs before io, so what it reads it reads with the kit, never
through a `File` (programming-model.md, 2.1). `read_file` reads one whole
file, up to a stated maximum, through the ring adapter's own calls: it
opens without blocking and never as a controlling terminal, checks that
what it opened is a regular file, reads it, and closes it. It answers the
bytes, or why not: absent, not a regular file, past its maximum, or the
kernel's error. Once the loop runs, files are io's (io.md, 5).

### 6.2 Trust roots

A TLS client trusts the roots its configuration names, one of two:

- **The machine's.** The bundle that the `SSL_CERT_FILE` environment
  variable names, or else the first that exists of the distributions'
  bundle files (`/etc/ssl/certs/ca-certificates.crt` and its kin, in an
  order the module documentation fixes). A bundle is PEM: the kit takes
  each certificate out of it as DER, and skips what is not a certificate.
  A directory of hashed certificates (`SSL_CERT_DIR`) is not read
  (section 10).
- **A DER file:** one certificate, for an issuer or a provider on a
  private network, or a test's fake.

Each is read with `read_file` within bounds that are configuration: the
bundle's bytes, the number of certificates and each one's bytes. The kit
hands the certificates to `skein-tls`, which builds its configuration
from them (tls.md, 3.4), skips and counts one it cannot parse, and
refuses roots of none. Every connection shares the roots, so they count
once in the worst case. A machine with neither a bundle nor
`SSL_CERT_FILE` refuses to start when its configuration asks for the
machine's roots.

- **A bundle is read in RFC 7468's lax form.** Text outside blocks is
  ignored. A block runs from its `-----BEGIN <label>-----` line to the
  next `END` line or the next `BEGIN` line, whichever comes first. Its
  base64 may hold whitespace and line breaks anywhere.
- **Blocks that are not certificates** (any label but `CERTIFICATE`:
  `TRUSTED CERTIFICATE`, `X509 CRL`, a key) are skipped without being
  decoded, and counted as others.
- **A malformed certificate block is skipped and counted, never a
  refusal.** A block is malformed when it:
  - does not end with its own `END CERTIFICATE` line;
  - carries encapsulated headers;
  - has invalid base64: a character outside the alphabet, or padding
    anywhere but at the end.

  Reading goes on at the next `BEGIN` line. The rest of the bundle still
  serves, as it does past a certificate that does not parse (tls.md,
  3.4). Skipping takes trust away and never adds it, and a damaged bundle
  should not stop every service on the machine.
- **The bounds refuse.** They are configuration, so a bundle past them is
  refused, naming the bound, never cut short to fit it:
  - the bundle's bytes;
  - the number of certificates taken out as DER;
  - each one's bytes, checked as its block decodes. The refusal names
    the certificate's place among the file's certificate blocks,
    malformed ones included.

  A block decodes until its first defect or its bound, whichever comes
  first, and that decides which it is. A malformed block is not kept, so
  it does not count towards the number of certificates.
- **Startup says the counts once** on standard error: how many roots
  were taken; how many `skein-tls` skipped as unparsable; how many blocks
  were malformed; and how many were others.

### 6.3 Terminal modes

A service with a person at a terminal (an interactive front end) may
want its input a key at a time, without the terminal's echo or line
editing, and the window's size. These are shell effects, made with the
kit, since step code makes no syscall and the shell's rules allow only
the ring adapter's:

- **Raw input at startup.** If standard input is a terminal and the
  configuration asks for it, startup reads the terminal's modes, keeps
  them, and sets the ones asked for: no echo, no line editing, and the
  terminal's own signals kept or not, as the service says. Standard input
  is then adopted as a read stream, as an inherited pipe is (io.md, 3).
  Standard input that is not a terminal is left as it is, and the service
  hears that it has none.
- **Restored at the end.** `main` restores the kept modes once `drive`
  returns, on every path out of it, before it writes its last line to
  standard error. A process that aborts, or is killed, cannot: the
  terminal stays as it was set, as with any program that sets it.
- **The window's size** is read at startup, and again when `SIGWINCH`
  arrives: startup blocks it with the termination signals, so it reaches
  io through the same signalfd, as an event that the window changed
  (io.md, 7), and the service asks for the new size with a synchronous
  record (kernel.md, 6.3).

A service without a terminal never asks for any of this, and a line
pipe needs none of it.

## 7. The readiness backend (later)

This backend is for hosts where io_uring is not available: a container
whose seccomp profile refuses io_uring (Docker's default profile does), a
kernel with `io_uring_disabled` set, a kernel below the floor, or macOS.

- **It emulates completions.** It waits for readiness (epoll or kqueue),
  makes the syscall without blocking, and completes the record. Regular
  files are always ready, so file operations run synchronously. epoll and
  kqueue differ only in the wait call.
- **It costs nothing on the ring path.** The shell picks a backend at
  startup and holds it in an enum, matched once per submit and once per
  reap. Step code never knows which backend runs.
- **The kernel boundary keeps it cheap:** records that carry their own
  memory and plain values, single-shot operations, and synchronous
  operations already in the vocabulary. A feature only io_uring has is
  always an optimisation, never something io's behaviour relies on.
- **macOS would only be a convenience for development.** Processes there
  are not pidfds (kqueue's process filter stands in), and containment is
  Linux-only.

It is not built until a deployment needs it.

## 8. Deferred optimisations

Each sits behind the same records, keeps the contract of the kernel
boundary (explicit backpressure, one completion per operation, a
completion queue that cannot overflow), is measured before it goes in,
and changes no code above io:

- multishot accept and receive with a provided-buffer ring, so idle
  sockets hold no buffer;
- registered buffers; fixed files (kernel.md, 9); zero-copy send; linked
  operations;
- exact-size receives for a fill demand: io allocates the `Box`, the
  kernel reads straight into it, and the copy out of the intake
  disappears;
- shared immutable buffers (`Arc<[u8]>`, confined to io) for large values
  sent to many slow readers, or zero-copy relaying;
- batched submits: submitting every few rounds, or when about to wait,
  which saves syscalls under pipelining at the cost of delaying each
  submission by up to that many rounds. Today the loop reaps and submits
  every round: going round costs a completion-queue read, a clock read,
  and a ring syscall only when there is something to submit or the kernel
  has completions to flush, and reaping every round keeps one peer's
  backlog from holding up other connections' completions and timers;
- kernel TLS (tls.md).

## 9. Testing

- **The ring's own tests** (`tests/ring`) run on the real kernel, on
  loopback and in a scratch directory: its slots and waits, a large
  transfer through short sends, a large file through its reads and
  writes, what it completes itself (a `List` among them), a listing's
  position handed from one `List` to the next across its own buffer, a
  root opened at startup, the invariants it asserts, and dropping it with
  operations in flight.
- **The conformance suite** runs against the ring, each scenario once
  (`tests/conformance/ring`; kernel.md, 8).
- **The real loop** exercises the rest: the probe at startup, the signal
  path, and the ring adapter's `unsafe` under a sanitizer
  (testing-strategy.md, 2.8).
- **`drive`'s own tests,** in the real loop, over scripted hosts: one that
  ends by itself returns its exit at once; one that waits on a keep time
  after its last word fails the teardown check (testing-strategy.md, 6);
  a termination signal while it closes aborts what is still closing; the
  hook runs once per iteration, after `iterate`.
- **Startup reads** (`tests/ring`): a whole file within its maximum, one
  past it, a FIFO and a directory refused; a PEM bundle with certificates,
  other blocks, a certificate that does not parse, and malformed blocks
  of each kind (invalid base64, unterminated, ended by another label,
  with headers), each skipped and counted while the rest are taken; a
  bundle past each bound refused; a DER file.
- **Terminal modes,** end to end under a pseudo-terminal
  (testing-strategy.md, 2.9): input read a key at a time, the modes found
  again once the binary exits, and a size change heard.

## 10. Open questions

- **The real loop in CI:** a kernel at the floor, with io_uring allowed by
  the CI's container profile.
- **Sanitizers on the ring adapter:** Miri cannot run the ring, so which of
  the address and leak sanitizers the real loop runs under.
- **Processes in the readiness backend on macOS,** which has no pidfd and
  no `clone3`, if that backend is ever built.
- **A hashed certificate directory** (`SSL_CERT_DIR`) as the machine's
  trust roots, on a machine with no bundle file: read it, or refuse to
  start as today (6.2).
- **A terminal suspended and resumed** (`SIGTSTP`, `SIGCONT`): whether
  the shell restores the modes before the process stops and sets them
  again when it continues, which takes a signal the shell handles while
  the loop runs (6.3).

## 11. Not built yet

- **Startup** (section 6): names, TLS's configuration, and the files a
  service appends to. The ring's probe is built, and so are `open_root`
  for roots and `open_termination_signals`, which blocks `SIGINT` and
  `SIGTERM` and opens the signalfd io adopts; the echo's `main` runs the
  rest of startup as a service's would (examples.md, 4):
  its limits checked, each machine's largest demand within io's caps, and
  the sum of the worst cases within the memory configured, before the
  seed and the kernel. Startup is each service's `main`, with the kit, so
  the kit holds no startup function of its own.
- **The records io's next parts pull** (kernel.md, 10): a signal to a
  child's group, a wait that does not reap and the reap at close, an
  append, a `Stat` that answers owner and links, and reading resource
  usage. Sockets, files and processes are built: spawning, waiting,
  signalling a child, its pipes, and reading termination signals, with
  listing a directory, spawning and signalling the synchronous ones.
- **`Host` and `drive` in skein-shell,** with the hook (section 12).
  Today `Host` is `skein-world`'s, and each `main` writes its loop out;
  the echo's already ends once the service holds nothing (section 13).
- **Startup reads and trust roots** (6.1, 6.2), and **terminal modes**
  (6.3).
- **The readiness backend** and the deferred optimisations.

## 12. The loop: `Host` and `drive`

The loop of programming-model.md, section 2 is written once, here, not in
every service's `main`.

- **`Host`** is what the loop needs of a service, and what skein's world
  harness needs of a process it hosts: one trait, so a world runs a
  service's shell as it ships. A service's shell implements it over its
  `iterate`:
  - `iterate(now, wall)`: one turn, both passes and the reclaim point;
  - its completion and submission queues;
  - `work_pending(now)`, and `next_deadline()`, its earliest over every
    layer;
  - `next_policy_deadline()`, its earliest deadline other than io's own,
    the mechanics' (io.md, 2): its close and retry deadlines, and the
    stall deadlines owners state for files. It is the one the teardown
    invariant watches (testing-strategy.md, 6);
  - `is_empty()`, when it holds nothing, and `exit()`, its exit status
    once it does;
  - `worst_case()`, and `operations()`, the size of its ring;
  - `drain()`, the hook below, which does nothing unless the service
    gives it something to do.
- **`drive(kernel, clock, host)`** is the loop: reap, read the clock,
  iterate, drain; then return the host's exit once it holds nothing
  (section 13), or else submit, waiting not at all while work is pending,
  until the earliest deadline, or until a completion. A service's `main`
  is its startup, then `drive`.
- **The hook** runs once per iteration, after `iterate` and before the
  submit, while the service is a frozen snapshot. It is for drains that
  belong to the shell: reading what the service holds for its operator
  and saying it once, such as the address it listens at. It is shell code
  under the shell's rules (programming-model.md, 2.1): a short line to
  standard error is the most it writes, and it writes no file and starts
  no thread. Output that is large, or must be complete, such as a trace,
  is an io stream instead (io.md, 5.1). The world harness calls the hook
  at the same point, so the drains a world runs are the ones that ship
  (testing.md, 5).

## 13. The end

- **The loop exits when the service holds nothing:** every slab empty,
  nothing in flight, every queue empty, and io holding no entity. `drive`
  then returns the service's exit, and `main` returns it. It does not
  exit at a signal, at an answer or at a timer: after its last word, a
  service closes everything it owns (programming-model.md, 5.2), and the
  loop ends when that has settled.
- **Only io's deadlines bound it.** A service's teardown bound is io's
  close timeout, the write deadline of a stream it still writes after its
  last word (io.md, 5.1), and the moment its cancels take to settle;
  nothing else in it waits on time once its last word is out. A
  supervisor derives its grace from that bound, and its grace exceeds
  it.
- **A signal while closing aborts.** A termination signal arrives as io's
  `Shutdown` at any time (io.md, 7). Before the last word, the service
  decides what it means; after it, the remaining graceful closes become
  aborts, and the loop ends as soon as those settle. A second signal
  means the same as the first.
- **Nothing outlives the loop.** There is no thread to join, no buffer to
  flush and no grace timer: what the service wrote went through io, which
  has settled it, and the kernel is dropped with nothing in flight. A
  service never ends with `std::process::exit` while it holds anything.
  After `drive` returns, `main` only undoes what startup set with the
  kit, such as a terminal's modes (6.3), and writes nothing to the
  service's files.
- **What the process used,** for a service that reports it at its end:
  the kernel's count of CPU time and peak resident size, for the process
  and for the children it has reaped. The service reads it through io
  (io.md, 6.1), a `Usage` the kernel answers at submit, after its last
  child has closed, since io reaps a child only at its close, and before
  the stream it reports on closes. After `drive` returns no stream is
  open to report on.
- **A supervisor's kill is a backstop** for a process that does not end:
  a bug, or an operation the kernel will not interrupt (notes.md). It is
  never how a healthy process ends.
- **Checked** in every world, by the teardown invariant
  (testing-strategy.md, 6), and in the real loop by `drive`'s own tests
  (section 9).
