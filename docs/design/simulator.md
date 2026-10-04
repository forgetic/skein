# The simulator

Provisional, 2026-10-03. The design of `skein-sim`: the simulated kernel
that every world from io worlds up runs on (testing-strategy.md, 2). It
is a backend of the kernel boundary (kernel.md), for every process of one
world, with every choice drawn from one seed. Its module documentation
states each choice it makes where the contract leaves one.

## 1. In one page

- **It plays the kernel, not the program.** It answers the records of
  the kernel boundary as the contract allows: the ring, the network
  between the world's sockets, and the clock. What a program does comes
  from the embedder's fake machine.
- **The world runs the services, not the simulator.** For each simulated
  process it offers the same submit and reap as the shell's kernel, and
  the world's own loop calls each service's `iterate` in turn. Nothing in
  the simulator calls into a service.
- **One seed, one run.** Every choice (latency, short transfers, faults,
  how a race ends) is drawn from the seed, and every submission and
  completion goes into a trace, so a failing seed prints and replays.
- **It checks its client.** It fails the world on every broken invariant
  of the kernel boundary, and asserts its own promises at every step.
- **It is kept honest** by the conformance suite, which runs the same
  scenarios against it and against the ring on the real kernel.

## 2. In skein

`skein-sim` depends on lib and io (for the records). It is ordinary Rust
(programming-model.md, 10.2): std is in, but no clock, no thread, no OS
randomness and no hash map with a random seed, so a seed replays. It
needs no kernel layouts and no `unsafe`. skein's own io worlds and
simulated worlds use it, and so do a service's simulated worlds. Its own
tests are a crate of their own, `tests/sim`, and the conformance suite
runs against it in `tests/conformance/sim` (testing.md, 6).

## 3. The world

- **Processes.** A world holds processes, each with its own descriptors,
  counted up from 3 and never reused, up to a descriptor limit. A
  service, or a test's scripted owner, drives each through two calls:
  submit, which takes every record of a `Queue<Submit>`, and reap, which
  moves delivered completions into a `Queue<Complete>`, up to its room.
- **When effects happen.** As on the ring, a process's waiting operations
  are decided when it enters the kernel: at its submit, with its reap
  standing in for the loop's wait. What the network does (bytes landing,
  a connection made, an end of stream or a reset reaching a socket)
  happens at once, whatever the process. Latency delays only the
  delivery of a completion.
- **The network** joins the world's sockets over loopback: listeners and
  their accept queues, connects, byte streams through bounded receive
  buffers, half-closes, resets.
- **The clock.** The simulator owns time, and moves it only when asked:
  to the next thing due (a late completion, a raced cancel, a connect's
  timeout), or to a given instant. A world that hosts services moves it
  to the earlier of what the simulator has due and the services' earliest
  deadline, so an idle world jumps straight there. The wall clock starts
  at a fixed date, so it replays too.
- **The machine seam.** File and process operations go to the embedder's
  fake machine, which answers the file operations and runs the programs.
  A spawned program may be another service, which the world then starts
  and hosts, so one world can hold a parent and the children it starts.
  skein ships no fake machine.

## 4. Faults

A world's configuration sets its sizes and limits (receive buffers,
accept queues, descriptors, connect timeouts) and each fault's chance:

- latency, which reorders completions, even on one descriptor;
- short receives and short sends;
- resets, refused connects, timeouts and `NoBufferSpace`, which model the
  network beyond loopback, so that io meets them in a world that has only
  loopback;
- a cancel that lands late, racing its target; a cancel the backend
  cannot submit;
- the reset of a closed peer arriving late, so one more send succeeds,
  its bytes lost.

A calm configuration has no faults and roomy buffers; a chaotic one turns
every fault on, often enough that a few hundred seeds meet each one, with
small buffers so that sends are cut and stall.

## 5. What it checks

- **At every submit,** every broken invariant of the kernel boundary
  (kernel.md, 7), and any operation on a descriptor the process does not
  hold, fails the world with a panic naming the seed and the end of the
  trace.
- **At every completion it makes:** a valid completion of the operation's
  shape, each token completed once, every record handed back.
- **At quiescence, on request:** nothing in flight, and every descriptor
  closed.

Memory is not the simulator's to check, though a simulated world checks it
at every iteration (testing-strategy.md, 6). The world's harness, whose
loop calls each service's `iterate` and which knows their worst cases,
checks with the counting allocator, `skein-heap` (testing.md, 5), each
hosted service's heap against its own worst case (programming-model.md,
6.3). The services and the simulator share one thread, so the allocator
sees one heap; the harness tells each service's part apart by metering
around that service's own calls, building it and each `iterate`: what the
heap grew by within them is the service's. A service's submit and reap
run simulator code, but outside those calls, and nothing one service
allocates is freed by another or by the simulator, which hands every
buffer back and never drops, copies or replaces one (kernel.md). So the
simulator's own heap (its trace, the bytes in its network) and the
harness's are left out.

## 6. Testing

- **Its own tests** (`tests/sim`) submit records by hand: each rule of
  the contract, each broken invariant, and a client and a server
  exchanging bytes, calm and replayed; and, in the fuzzy suite
  (testing-strategy.md, 8), under chaos over a few hundred seeds,
  asserting that every fault fell.
- **The conformance suite** (kernel.md, 8) is `testing/skein-conformance`,
  over a small backend interface the simulator implements in
  `tests/conformance/sim` and the ring in `tests/conformance/ring`. Where
  the simulator draws among outcomes the kernel allows, every one of them
  must appear over the fuzzy suite's seeds; where the kernel answers one
  way, the simulator must too. A behaviour found in the kernel that the
  simulator lacks goes into the suite first, then into the simulator.

## 7. Not built yet

- **Files and processes,** and with them the machine seam and hosting the
  services a spawn starts, when io pulls them. Sockets are built.
- **A state digest** in the trace, beside the records (lib.md, 11).

Hosting services is built for sockets: `skein-world` (testing.md, 5)
hosts each process's `iterate` over the simulator, moving time to the
earlier of `next_due` and the processes' earliest deadline only when no
process has work and none has deferred work (section 3), and the echo's
worlds run on it (examples.md, 6). So is the check of memory at every
iteration, which is the harness's, not the simulator's (section 5): it
meters around the processes' own calls, building each and each
`iterate`, with the counting allocator's span, so that what grew within
the simulator's calls and the harness's own is left out.
