# The simulator

Provisional, 2026-10-03, revised 2026-10-09. The design of `skein-sim`:
the simulated kernel that every world from io worlds up runs on
(testing-strategy.md, 2). It is a backend of the kernel boundary
(kernel.md), for every process of one world, with every choice drawn from
one seed. Its module documentation states each choice it makes where the
contract leaves one.

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

### 3.1 The machine seam, for files

The seam is data, as the kernel boundary is: no callback, no trait. The
machine is a value the world owns, beside the simulator, and the world
moves what crosses between them, as it moves records between its
processes and the simulator.

- **Calls out, answers in.** An operation on files that passes the checks
  and draws no failure becomes a `Call`, which waits in the simulator
  until the world takes it (`Sim::calls`). The world hands each to its
  machine, and gives the machine's `Answer`s back (`Sim::answer`), each
  echoing its call's `Ticket`; the simulator makes the completion of each,
  buffers filled from what the answer carried.
- **In the machine's names.** A call names what the machine has open by
  its `Handle`, which the simulator holds behind each descriptor of a file
  or a directory, beside how it was opened. Paths and names go as copies;
  a `Read` asks for a count and is answered with the bytes; a `Write`
  carries the bytes to write; a `List` asks for a count and a room for
  names, and is answered with each entry's kind and name.
- **Who keeps what.** The simulator keeps what is the kernel's:
  descriptors and their limit, the contract's checks, faults, latency,
  the trace. The machine keeps what is the filesystem's: what lies beneath
  its roots, resolved beneath a root as `openat2` resolves, refused as a
  real filesystem refuses, and what each handle has open. It answers each
  call once, exactly as asked: faults are drawn by the simulator, not by
  the machine.
- **Roots.** The shell opens the first roots at startup; in a world, the
  world asks its machine for a root and gives the handle to a process
  (`Sim::root`), which then holds it as a descriptor.
- **Files appended to.** The shell opens a file to append at startup
  (shell.md, 6); in a world, the world asks its machine for the file,
  made if it is absent, and gives the handle to a process as a descriptor
  open to append, as it gives a root. An `Append` goes to the machine as a
  write at the file's end at the moment the simulator delivers it, so
  appends from two descriptors interleave whole, as the kernel's do. It
  meets the faults of a `Write` (section 4), `hung` among them.
- **`Stat`** answers the owner and the links the machine keeps for what is
  open: every file of the minimal machine has one owner, and a second
  name made in a scenario's tree gives a file two links (kernel.md, 6.1).
- **The machine's shape is a step machine's:** its state, one call in, one
  answer out. It is ordinary Rust, a test's; a service's fake machine can
  be the same step machine its other tiers host.

### 3.2 Processes and groups

A spawn goes to the machine as a call naming the program, its arguments
and its pipes; the machine runs the program, or the world starts the
service it names and hosts it (section 3). The simulator keeps what is
the kernel's: each child's pidfd, its pipes, its exit and its group.

- **Groups.** Each child leads a group of its own from its spawn. A
  program the machine runs may start a child of its own, which joins its
  group unless the program leaves it; the minimal machine's program that
  does so stands in for a command that leaves a descendant behind. A
  signal to the group reaches every member, a hosted service included,
  as a termination signal on its signal source.
- **Observed, then reaped.** A `Wait` that does not reap completes at
  the exit and leaves the child the simulator's zombie: still in its
  group, its usage not yet its parent's. The reaping `Wait` releases it.
  A descendant whose parent exits first is reparented away, and never
  counted in anyone's usage.
- **Usage.** Each process's own usage is what the world sets for it,
  zero unless set, so a world replays; the children's part takes a
  child's own at its reap, summed for CPU and the largest for the peak,
  as the kernel's does (kernel.md, 6.3).

### 3.3 Cuts at an operation

A world may cut a process at any operation, as a power loss or a kill
would: to show that what a service keeps durably survives (io.md, 5.2).

- **The cut** is at a chosen point in the process's submissions: the
  process is dropped with what it has in flight, which never completes,
  and its descriptors go with it.
- **A kill cuts one process; a power loss cuts the machine:** every
  process on it is dropped at the same point, and each restarts over
  what the machine kept.
- **What the machine keeps** depends on the cut. After a kill, every
  operation that completed stands, as the kernel's cache keeps it. After
  a power loss, what a crash keeps: what was synced; unsynced writes
  landed or not, in any order, the last one torn at any byte; a
  directory's entries changed by a rename or a create only once the
  directory was synced, and a rename whole or not at all (kv.md, 8). The
  choices are drawn from the seed.
- **After the cut** the world starts the process again over what the
  machine kept, with the roots it had, and the referee checks what it
  recovers: for a durable replace, the old content or the new, whole.
- **A cut at every operation.** A scenario cuts at each operation of a
  sequence in turn, over a few seeds each, so every point between two
  syncs is met.

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
  its bytes lost;
- short reads and short writes of files;
- a disk beyond a healthy one: no space, a filesystem gone read-only, an
  I/O error, and the kernel out of memory, each failing an operation on
  files before the machine is asked; and a filesystem that hangs an
  `Open`, `Read`, `Write`, `Append` or `Sync` until a `Cancel` stops it,
  the machine never asked: an append stream's write deadline, and its
  close deadline while it closes, meet it there (io.md, 5.1).

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
- **At every answer of the machine:** an answer to a call waiting, of its
  call's shape, with no more bytes or entries than were asked, and a new
  handle that no process holds.
- **At quiescence, on request:** nothing in flight, and every descriptor
  closed.

Memory is not the simulator's to check, though a simulated world checks it
at every iteration (testing-strategy.md, 6), with the counting allocator,
`skein-heap` (testing.md, 5). The harness checks each hosted process's
heap against its own worst case (programming-model.md, 6.3). One thread,
one heap: a process's part is what grew within the calls that run its
code (making it, and each `iterate`), metered with a span, at its peak
within each call. The simulator's submit and reap, the referee and the
harness run between those calls, so the simulator's trace and network and
the harness's heap are left out. This holds while nothing a process owns
is allocated or freed outside its calls: the backend hands every buffer
back and never drops, copies or replaces one (kernel.md); the queues it
fills and drains are the process's own, bounded and made with it; and the
referee changes a process only through flags that allocate nothing. Once
settled, the harness checks it: each process, dropped, frees exactly what
was metered as its own, which also finds a leak.

## 6. Testing

- **Its own tests** (`tests/sim`) submit records by hand: each rule of
  the contract, each broken invariant, and a client and a server
  exchanging bytes, calm and replayed; files through the seam to skein's
  minimal fake machine, each broken invariant of files and of the seam,
  each fault of files, and a replay; and, in the fuzzy suite
  (testing-strategy.md, 8), the exchange and a workload of files under
  chaos over a few hundred seeds, asserting that every fault fell.
  Processes, groups, appends and cuts add theirs: a signal to a group
  reaching a member the leader started, before and after the leader's
  exit; a zombie's usage joining its parent's children's only at the
  reap; two descriptors appending to one file, each piece whole at the
  end; a hung `Append` stopped by its `Cancel`; and a cut at each
  operation of a durable replace, the file then old or new and whole.
- **The conformance suite** (kernel.md, 8) is `testing/skein-conformance`,
  over a small backend interface the simulator implements in
  `tests/conformance/sim` and the ring in `tests/conformance/ring`. Where
  the simulator draws among outcomes the kernel allows, every one of them
  must appear over the fuzzy suite's seeds; where the kernel answers one
  way, the simulator must too. A behaviour found in the kernel that the
  simulator lacks goes into the suite first, then into the simulator.

## 7. Not built yet

- **Groups, the observing `Wait` and its reap, and usage** (3.2). Today
  a `Wait` reaps at the exit, and a signal reaches the child alone.
- **Files appended to, and `Stat`'s owner and links** (3.1).
- **Cuts at an operation** (3.3). The minimal machine's crash model is
  built, and skein-kv's tests cut by hand: they drop the simulator and
  the store and build both again over what the machine kept. The
  simulator's own cut, which keeps the world and its other processes,
  is not.
- **A state digest** in the trace, beside the records (lib.md, 11).

Sockets, files through the machine seam, and processes are built:
spawns through the seam's calls for programs, pipes, exits, signals to a
child, termination signals on a process's signal source, and hosting the
services a spawn starts. `skein-world` serves the machine's calls after
each submit, and opens a process's startup roots in its machine.

Hosting services is built: `skein-world` (testing.md, 5)
hosts each process's `iterate` over the simulator, moving time to the
earlier of `next_due` and the processes' earliest deadline only when no
process has work and none has deferred work (section 3), and the echo's
worlds run on it (examples.md, 6). So is the check of memory at every
iteration, which is the harness's, not the simulator's (section 5): it
meters around the processes' own calls, building each and each
`iterate`, with the counting allocator's span, so that what grew within
the simulator's calls and the harness's own is left out, and checks once
settled that each process, dropped, frees what was metered as its own.
The harness's own tests (`tests/world`) show it holds time while deferred
work waits, and catches a process past its worst case and one that leaks.
