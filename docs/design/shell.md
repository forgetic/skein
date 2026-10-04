# The shell kit

Provisional, 2026-10-03. The design of `skein-shell`: what a service's
`main` runs its loop on. The kernel (the io_uring backend of the kernel
boundary, kernel.md), the clock and the seed; later, the readiness
backend. It is ordinary Rust, and the only `unsafe` in skein that a
service runs lives here, in one module, the ring adapter. (The counting
allocator, test-only, has the other: testing.md, 6.)

## 1. In one page

- **The loop belongs to the service.** skein provides the kernel (open,
  submit, reap), the clock and the seed, and no `run`. A service's loop is
  about ten lines over them (programming-model.md, section 2).
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
- **The `unsafe`, and why it is sound,** is stated in the ring adapter's
  module documentation, case by case: the pointers handed to the kernel,
  entering the ring, the socket address casts, the synchronous calls, and
  dropping a kernel with operations in flight, which leaks the table so
  that the kernel never writes into freed memory.
- **Its checks:** the first two broken invariants of the kernel boundary
  (an invalid record, a token already in flight) at submit.

## 4. The kernel floor

- **One floor, no feature checks.** The first floor is 6.12, an LTS
  release that has every ring operation io uses. An operation newer than
  the floor stays synchronous until the floor moves: making a pipe, for
  example, has been a ring operation only since 6.16. Lowering the floor
  for a deployment that needs it means making the operations newer than
  the new floor synchronous, and io cannot see that change.
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
4. opens the first roots for io's files, from configuration (io.md);
5. resolves the configured peer names into addresses (io.md);
6. reads certificates, keys and root stores for TLS (tls.md);
7. reads the seed and opens the kernel.

`panic = "abort"` in every profile: a panic is fail-stop, and a
supervisor restarts the process.

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
  loopback: its slots and waits, a large transfer through short sends,
  what it completes itself, the invariants it asserts, and dropping it
  with operations in flight.
- **The conformance suite** runs against the ring, each scenario once
  (`tests/conformance/ring`; kernel.md, 8).
- **The real loop** exercises the rest: the probe at startup, the signal
  path, and the ring adapter's `unsafe` under a sanitizer
  (testing-strategy.md, 2.8).

## 10. Open questions

- **The real loop in CI:** a kernel at the floor, with io_uring allowed by
  the CI's container profile.
- **Sanitizers on the ring adapter:** Miri cannot run the ring, so which of
  the address and leak sanitizers the real loop runs under.
- **Processes in the readiness backend on macOS,** which has no pidfd and
  no `clone3`, if that backend is ever built.

## 11. Not built yet

- **Startup** (section 6): blocking the termination signals, roots, names,
  and TLS's configuration. The ring's probe is built, and the echo's
  `main` runs the rest of startup as a service's would (examples.md, 4):
  its limits checked, each machine's largest demand within io's caps, and
  the sum of the worst cases within the memory configured, before the
  seed and the kernel. Startup is each service's `main`, with the kit, so
  the kit holds no startup function of its own.
- **The operations for files and processes,** and the synchronous ones
  (spawning, signalling, making a pipe, listing a directory), when io
  pulls them. Sockets are built.
- **The readiness backend** and the deferred optimisations.
