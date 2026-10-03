# skein: overview

Provisional, 2026-10-02. The design of skein: the parts that every
service written in the programming style shares, so that a service
writes only its model and its own protocols. Section 12 lists what is
still open. Nothing is built yet.

The design is four documents, each the one place for its concepts:

- **`overview.md`** (this one): what skein is and how its parts work. The
  stream types, io's records, the kernel boundary, the backends, the shell
  kit, the simulator and the protocol machines.
- **`programming-style.md`**: the rules all step code follows, in skein
  and in every service. Step functions, boundaries, handles and
  lifecycle, memory, flow control, time, the Rust subset and testing.
- **`consumers.md`**: how a service is built on skein. Who writes what,
  the loop, the three layers, the protocol layer, sub-models, worlds, and
  how to start.
- **`testing-pyramid.md`**: how skein is tested, tier by tier, what is
  real and what is fake in each, and what skein supplies for a service's
  own tiers.

Read this one first, then the style, then consumers.md if you are
writing a service.

## 1. In one page

- **skein is the kit under a service.** It holds:
  - lib: handles, slabs, queues, cursors, deadlines and time;
  - io: sockets, pipes, files and processes;
  - the shell kit: the io_uring adapter, the clock and the seed;
  - the simulated kernel;
  - the protocol machines that belong to no one application: HTTP/1.1,
    server-sent events, JSON and TLS.

  A service brings its model, its own protocol layer, its `iterate` and
  its `main`.
- **A kit, not a framework.** skein has no service trait, no generic
  loop, no scheduler and no callbacks. A service calls the parts by
  name, in its own loop of about ten lines (consumers.md, section 4).
  Nothing in skein calls into a service, not even the simulator
  (section 9).
- **Built when pulled.** A part is built when its first user needs it,
  and that user is its first test. temper is the first user.
- **io_uring is the kernel interface.** io talks to the kernel in owned
  records. An operation goes down carrying its memory, and its
  completion comes back carrying the same memory. The ring adapter maps
  each record onto one submission, with no indirection. The simulator
  implements the same records. A readiness backend (epoll, kqueue) may
  later implement them a third time, for hosts without io_uring.
  Nothing above the records changes.
- **Every byte boundary has one shape.** A stream is read by demand and
  written by move. That holds for a socket, a pipe, the plaintext above
  TLS and the body above HTTP alike. A protocol machine takes a stream
  below it and gives a stream, or messages, above it. A connection
  stacks machines by calling them in order. Machines depend on lib only,
  not on io and not on each other.
- **Counted and bounded throughout.** Every part exports its `Limits`
  and its `worst_case` (programming-style.md, 5.4), and a service adds
  them up.
- **One exception to "nothing from outside".** TLS wraps rustls. It is
  the only step crate with a dependency from outside the workspace, and
  the only one that is not deterministic. The simulator runs without
  it.

## 2. Scope

### 2.1 In skein

| Part | Crate | Kind |
|---|---|---|
| lib | `skein-lib` | step |
| io | `skein-io` | step |
| HTTP/1.1 and server-sent events | `skein-http` | step |
| JSON | `skein-json` | step |
| TLS | `skein-tls` | step: the exception (10.4) |
| the shell kit | `skein-shell` | ordinary Rust, with the only `unsafe` |
| the simulated kernel | `skein-sim` | ordinary Rust |

skein also holds the programming style, the workspace lints and
`clippy.toml` (programming-style.md, 9.4). A service's workspace copies
the lints and `clippy.toml`.

### 2.2 Not in skein

- **Models, and protocols that only one application speaks:** an LLM
  provider's API, a forge's API, temper's protocol between worker and
  engine. They are built on skein's machines.
- **A service's wiring:** its `iterate`, the sum of its worst cases, its
  `main` (consumers.md).
- **What a simulated program does.** The simulator plays the kernel. The
  files and programs a scenario needs come from the embedder's fake
  machine, which plugs in at a seam (section 9).
- **Schedulers, async and threads.** There is one loop per process. A
  service that needs more cores runs more processes.

## 3. Layout

```
skein/
  Cargo.toml       workspace: profiles and lints (programming-style.md, 9.4)
  clippy.toml      disallowed types and macros for the step crates
  docs/design/     overview.md (this document), programming-style.md, consumers.md,
                   testing-pyramid.md
  crates/
    skein-lib/     Id, Slab, Token, ReplyTo, Queue, List, Map, Set, Stack,
                   Reader, Writer, bytes, Deadlines, Time, Duration, Wall, Rng, Env;
                   stream, Intake
    skein-io/      io::up, io::down: sockets, pipes, files, processes;
                   the operation and completion records
    skein-http/    HTTP/1.1 heads, bodies, client and server connections;
                   server-sent events
    skein-json/    a bounded tokenizer, a sized writer
    skein-tls/     a TLS stream over rustls
    skein-shell/   the ring adapter, the readiness backend (later), clock,
                   seed, synchronous operations
    skein-sim/     the simulated kernel, faults, the counting allocator,
                   the conformance suite
  fuzz/            one target per machine
  examples/        small services that drive io end to end: echo, an HTTP server
```

```
crate          depends on
skein-lib      nothing
skein-io       lib
skein-http     lib
skein-json     lib
skein-tls      lib, rustls
skein-shell    lib, io, io-uring, libc
skein-sim      io
```

Machines do not depend on io. The stream below a machine may be a
socket, a pipe or TLS, and the machine cannot tell which. A service's
protocol crate depends on io and on the machines it stacks. It is the
one place where they meet.

## 4. Streams

The byte part of io's vocabulary (demand, deliver, room, send, end) is
the same wherever bytes flow, so it is defined once, in lib, and every
byte boundary uses it. The rules for reading by demand and writing by
move are programming-style.md, 3.3; this section is the types.

```rust
// lib::stream, a sketch
pub enum Read { Nothing, Fill(u32), Scan { until: Delimiter, max: u32 } }

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

- **The side below holds the carry-over.** Bytes that were received but
  not yet demanded belong to whoever received them, under that side's
  cap: io for a socket, TLS for its plaintext. `lib::Intake` is that
  buffer, written once:
  - it appends what arrives;
  - it meets a fill or a scan as soon as it can, and a scan that reaches
    its maximum with no delimiter in it delivers exactly the maximum: the
    demand was for at most that many bytes, so the side above sees a scan
    that does not end with the delimiter and treats it as a framing error;
  - it reports the room left, so receiving stops at the cap.
- **A stack is a connection's states, called in order,** with no
  pipeline type and no timers in the machines (programming-style.md,
  3.4). Flow control runs through the whole stack: a reader that stops
  demanding stops the peer, through every machine, and no buffer grows
  past its cap (programming-style.md, section 6). An LLM client's stack:

  ```
  model
    ▲  typed calls
  the service's decoder
    ▲  tokens
  json
    ▲  events
  sse
    ▲  body stream
  http client
    ▲  stream: plaintext
  tls
    ▲  stream: ciphertext
  io socket
  ```

## 5. io

io is the lowest step layer of every service (consumers.md, section 5).
It owns:

- sockets, pipes, files and child processes;
- the operations in flight on them;
- the receive and send queues.

It is the only layer that sees a descriptor or a kernel error.

```rust
// skein-io, a sketch of the protocol side
pub enum Request {
    Listen  { owner: Token, addr: Addr },
    Connect { owner: Token, addr: Addr },
    Bind    { socket: Token, owner: Token },          // attach to an accepted socket
    Reject  { socket: Token },
    Stream  { stream: Token, down: stream::Down },    // sockets and pipes alike
    File    { owner: Token, root: Token, op: FileOp },
    Spawn   { owner: Token, spawn: Spawn },
    Signal  { child: Token, signal: Signal },
    Close   { entity: Token },                        // graceful (5.1); one Closed follows
    Abort   { entity: Token },
}

pub enum Event {
    Listening { owner: Token, listener: Token },
    Accepted  { listener: Token, socket: Token },
    Connected { owner: Token, socket: Token },
    Stream    { owner: Token, up: stream::Up },
    File      { owner: Token, result: FileResult },
    Spawned   { owner: Token, child: Token, pipes: Pipes },
    Exited    { owner: Token, exit: Exit },
    Shutdown  { signal: Signal },                     // a termination signal to the service
    Failed    { owner: Token, error: Error },
    Closed    { owner: Token },                       // terminal
}
```

### 5.1 Sockets and pipes

- **The entities** are listeners, sockets, and a child's three pipes.
  Sockets and pipes are both streams. Above io, they differ only in how
  they were made.
- **One receive is in flight** per stream while its intake has room,
  whether or not anything has been demanded yet.
- **One send is in flight** per stream, in order. The rest of the output
  waits in a queue under the output cap. A short send continues from an
  offset into the same `Box`; the remainder is never copied.
- **Accepts are re-armed one at a time** while the listener's owner has
  room, up to a configured number per iteration. A flood of connections
  then waits in the kernel's backlog (programming-style.md, section 6).
- **Close is graceful:**
  1. flush the queued output;
  2. half-close;
  3. read and discard until the peer ends or the close deadline passes;
  4. close.

  This matters when a response goes out while the peer is still
  sending, such as a refusal in the middle of an upload. The response
  reaches the peer instead of being lost to a reset. Abort closes at
  once.
- **Addresses are resolved before io.** io connects to an address, never
  to a name (10.5).

### 5.2 Files

- **Paths resolve beneath a root.** A root is an open directory, and it
  is an io entity. Every file operation names a root and a path relative
  to it. The kernel resolves the path beneath the root (`openat2` with
  `RESOLVE_BENEATH`), so neither `..` nor a symlink can leave it.
- **Where roots come from.** The shell opens the first roots at startup,
  from configuration. Any other root is opened beneath an existing one.
- **Whole-file operations come first:**
  - read a file, up to a stated maximum;
  - write a file, replacing it atomically: write a temporary, sync it,
    rename it;
  - stat;
  - list a directory, up to a stated count;
  - make a directory;
  - remove;
  - rename.

  Each one is one request with one terminal event. Beneath it, io runs
  the open, the reads or writes, and the close.
- **File streams come later,** when a user needs to read a file by
  demand because it is too large to hold.

### 5.3 Processes

- **Spawn** takes a program, its arguments, its environment, a working
  directory beneath a root, and which pipes to make. It answers with a
  child and its pipes.
  - It is `clone3` with `CLONE_PIDFD`, so the child is a pidfd from the
    start. The kernel's own check then stops a reused PID from being
    signalled.
  - Containment (namespaces, `CLONE_INTO_CGROUP`) is a spawn option,
    given as data.
- **Exit** is a wait on the pidfd, through the ring (`waitid`).
- **Signals** go through `pidfd_send_signal`.
- **A child is *closed*** once it has exited and its pipes and pidfd are
  closed. It has one terminal event, which comes after all of those.

### 5.4 Signals to the service

Termination signals are blocked, read from a signalfd through the ring,
and arrive as `Shutdown` events. The model decides what shutting down
means.

## 6. The kernel boundary

Below io, every backend speaks the same records: an **operation** goes
down and its **completion** comes up. This is the contract that the
ring adapter, the simulator and any later backend implement, and it is
all they share.

```rust
// skein-io, a sketch of the kernel side
// io -> kernel
pub struct Submit   { pub op: Token, pub kind: Op }
// kernel -> io: the operation handed back, and what came of it
pub struct Complete { pub op: Token, pub kind: Op, pub result: Result<Done, Error> }

// one success shape per operation
pub enum Done { Nothing, Count(u32), Fd(Fd), Accepted { fd: Fd, peer: Addr }, Bound(Addr) }

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
    // files, beneath a root
    Open     { root: Fd, path: Box<[u8]>, how: OpenHow },
    Read     { fd: Fd, buf: Box<[u8]>, at: u64 },
    Write    { fd: Fd, bytes: Box<[u8]>, from: u32, at: u64 },
    Sync     { fd: Fd },
    Stat     { root: Fd, path: Box<[u8]> },
    // ... rename, remove, make a directory, list a directory
    // processes
    Wait     { pidfd: Fd },
    // ... spawn, signal, make a pipe: synchronous (below)
    Cancel   { target: Token },
}
```

The completion hands back the operation it answers, buffers and all,
beside its result (a `Done` of the one shape that operation succeeds
with, or an `Error`), so every record comes back up whatever happened.

- **Memory moves with the operation.** A buffer that the kernel will
  read or write is a `Box` inside the record. io moves it down, and the
  backend holds the record until the operation's completion. The
  completion moves the buffer back up: filled, or with the count sent.
  - While the kernel holds the buffer, no other code can name it. That
    is the compiler's half of programming-style.md, 5.3.
  - The backend's `unsafe` is the other half. It takes addresses only
    from records it holds, and it gives a record back only after the
    operation's completion.
- **Kernel structures belong to the backend.** A socket address, a
  `statx` buffer, a `siginfo` and an `open_how` live in the backend's
  in-flight table, beside the record. They are decoded into plain values
  (`Addr`, `Stat`, `Exit`) before they go up. io never sees a kernel
  layout, so it needs no `libc`. A backend for another kernel
  translates.
- **Socket options are backend defaults, not records,** until a service
  pulls one: every descriptor is close-on-exec, a socket that binds gets
  `SO_REUSEADDR`, an IPv6 socket gets `IPV6_V6ONLY` (families never mix:
  it neither binds nor reaches an IPv4-mapped address), connected and
  accepted sockets get `TCP_NODELAY`, and a send never raises `SIGPIPE`.
- **Errors cross as a skein enum:** the errors io handles by name, plus
  an `Other` code. Each backend maps its kernel's error numbers onto it,
  per operation: the same number can mean different things on a cancel
  and on a receive.
- **Every operation completes exactly once,** cancelled or not. A
  cancelled operation still completes: either as cancelled, or with
  what it did before the cancel landed. Until then, io keeps the entity
  *settling* (programming-style.md, 4.3).
- **A cancel completes too,** before or after its target: it stopped
  the target, which then completes cancelled (a stopped receive, send or
  accept took nothing, what had arrived waiting for the next one; a
  stopped connect may still have reached its peer, which sees the
  connection end when io closes it); too late (the target had completed
  or could not be stopped, and completes with its own result or
  cancelled, if the kernel interrupted it); or not submitted at all, when
  the backend failed, the target running on.
- **Single-shot operations only, to start:** one submission, one
  completion, and every cancel takes an operation slot of its own. The
  completion queue can then be sized from io's operation slab, so it
  cannot overflow. Every receive and every accept is then a choice io
  made while it had room.
- **Sockets behave as Linux's do,** and the simulator matches them:
  - a bind answers with the address bound, its port chosen when it
    asked for port 0; two sockets may bind one address, and the second
    listen fails;
  - a full accept queue delays a connect, never refuses it (refused
    means nothing listened there), until it times out; a failed accept
    takes no waiting connection, and a connection closed or reset while
    it waits is still accepted;
  - a send's count is what the kernel accepted, and a half-close goes
    out behind every completed send; receiving still works after it;
  - a receive of zero bytes means the stream ended, which is not proof
    of a graceful close;
  - a reset fails one operation: a send at once, a receive after the
    bytes already received; after it, receives give zero bytes and sends
    and half-closes fail. An end that already received the peer's end of
    stream hears of no reset, and a send after the peer closed may
    succeed once, its bytes lost, then fails without a reset, the
    connection closed;
  - a descriptor is closed only by a close. Closing with unread data
    resets the peer; closing a listener resets the connections waiting
    on it.

  `skein_io::kernel`'s module documentation states each rule exactly.
- **Broken invariants** are mistakes io never makes, so backends may
  assume they never happen: an invalid record (an empty receive
  buffer, a send with nothing left), a token already in flight, a
  cancel of a cancel, an address of the wrong family, more than one
  receive, send or accept in flight on a socket or anything beside a
  connect, any operation but close after a failed connect, a connect on
  a socket that is not fresh, a listen on an unbound socket, a half-close
  on a socket that is not a connection or during a send, and a close
  with anything else in flight. The
  simulator fails the world on each; the ring checks the first two at
  submit.
- **Some operations are synchronous:** spawning, signalling, making a
  pipe, listing a directory. None of these is a ring operation at the
  kernel floor (7.1), and some are not ring operations at all. The
  backend performs them when they are submitted and completes them at
  the next reap, in the same records, so io cannot tell the difference.
- **Timers are not operations.** The shell waits for completions with a
  timeout: the earliest deadline over every layer (programming-style.md,
  section 8).

## 7. Backends

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

### 7.1 The ring

- **The only `unsafe`** in a skein service lives here, in `skein-shell`,
  on top of the `io-uring` crate. The ring adapter:
  - maps each record onto one submission entry;
  - keeps the record in its in-flight table;
  - when the entry completes, decodes the result and hands the record
    back.

  It makes no decisions.
- **Set up for one thread:** a single issuer, with deferred task
  running, so completions are processed only when the loop reaps.
- **One kernel floor, with no checking for individual features.** The
  first floor is 6.12, an LTS release that has every ring operation io
  uses. An operation newer than the floor stays synchronous until the
  floor moves; making a pipe, for example, has been a ring operation
  only since 6.16. Lowering the floor for a deployment that needs it
  means making the operations newer than the new floor synchronous, and
  io cannot see that change.
- **At startup** the shell probes the ring for every operation it uses.
  The probe confirms the floor; it does not choose between features. If
  an operation is missing, or if the ring cannot be set up at all, the shell
  refuses to start. Once the readiness backend exists, it takes the
  readiness backend instead.
- **Deferred optimisations, behind the same records:**
  - multishot accept and receive with a provided-buffer ring, so idle
    sockets hold no buffer;
  - registered buffers;
  - fixed files;
  - zero-copy send;
  - linked operations;
  - exact-size receives for a fill demand: io allocates the `Box`, the
    kernel reads straight into it, and the copy out of the intake
    disappears;
  - shared immutable buffers (`Arc<[u8]>`, confined to io) for large
    values sent to many slow readers, or zero-copy relaying;
  - batched submits: submitting every few rounds, or when about to wait,
    which saves syscalls under pipelining at the cost of delaying each
    submission by up to that many rounds;
  - kernel TLS.

  Each must keep the contract of section 6: explicit backpressure, one
  terminal event per operation, and a completion queue that cannot
  overflow. Each is measured before it goes in, and none changes code
  above io (programming-style.md, 5.6).

### 7.2 The simulator

As a backend, the simulator is ordinary Rust over the same records. It
needs no kernel layouts and no `unsafe`. Section 9 describes it.

### 7.3 The readiness backend (later)

This backend is for hosts where io_uring is not available:

- a container whose seccomp profile refuses io_uring (Docker's default
  profile does);
- a kernel with `io_uring_disabled` set;
- a kernel below the floor;
- macOS.

It emulates completions. It waits for readiness (epoll or kqueue),
makes the syscall without blocking, and completes the record. Regular
files are always ready, so file operations run synchronously. epoll and
kqueue differ only in the wait call.

- **It costs nothing on the ring path.** The shell picks a backend at
  startup and holds it in an enum. That enum is matched once per submit
  and once per reap. Step code never knows which backend is running.
- **Section 6 is what keeps it cheap:** records that carry their own
  memory and plain values, single-shot operations, and synchronous
  operations already in the vocabulary. A feature only io_uring has is
  always an optimisation, never something io's behaviour relies on.
- **macOS would only be a convenience for development.** Processes there
  are not pidfds (kqueue's process filter stands in), and containment
  is Linux-only.

It is not built until a deployment needs it.

## 8. The shell kit

- **The loop belongs to the service** (consumers.md, section 4). skein
  provides `Kernel` (open, reap, submit), `Clock` and the seed. It does
  not provide a `run`.
- **The clock gives two times,** monotonic and wall, read once per
  iteration and handed to the steps through `Env` (programming-style.md,
  section 8).
- **The seed** comes from `getrandom`, once, at startup.
- **At startup,** before opening the kernel, the shell:
  - checks the worst case against the configured memory
    (programming-style.md, 5.4);
  - blocks the termination signals that io will read (5.4).
- **Panics abort,** and a supervisor restarts the process.

## 9. The simulator

`skein-sim` plays the kernel at the boundary of section 6, for one
service or for many in one world. The world runs the services, not the
simulator. For each simulated process, the simulator offers the same
reap and submit as the shell's `Kernel`, and the world's loop calls each
service's `iterate` in turn.

- **The ring:** it takes the submitted records and decides when and how
  each one completes. That covers latency, short receives and sends,
  errors and resets. It races every cancel, and delivers completions
  after a cancel. Every choice is drawn from a seed.
- **The network** between the world's sockets: listeners, connects, byte
  streams cut and joined at random, half-closes, resets, refused
  connections.
- **The clock:** the simulator owns time. When every service is idle, it
  jumps time to the next completion or deadline.
- **The machine seam.** File and process operations are passed to the
  embedder's fake machine, which answers the file operations and runs
  the programs. A spawned program may be another service, which the
  world then starts and hosts, so one world can hold a parent and the
  children it starts. skein ships no machine. Its own tests use a minimal one.
- **Invariants of the kernel boundary,** on top of the style's universal
  ones and its memory check (programming-style.md, section 10):
  - at every iteration, one completion per operation, and every record
    handed back;
  - at quiescence, no operation in flight;
  - at every submit, none of section 6's broken invariants: each one
    fails the world.
- **Conformance.** One suite of scripted operation sequences runs
  against each backend:
  - the ring, on the real kernel, with loopback sockets and a scratch
    directory;
  - the simulator;
  - later, the readiness backend.

  It checks that each one answers as section 6 allows. This keeps the
  simulator honest: a simulated kernel that drifts from the real one
  tests the wrong thing (testing-pyramid.md, section 5).

## 10. Protocol machines

Each machine is a step crate: `no_std`, written in the style's subset.
Each has its own limits and worst case, and its own unit tests. Its
fuzz target is fed `Bytes` under every demand. Each side of a machine
has entry points of the style's shape, and each entry point declares
its `MAX_OUT`.

```rust
// a machine's entry points, a sketch (the HTTP client)
pub fn up(conn: &mut Client, env: &Env<Limits>, ev: stream::Up,
          above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
pub fn down(conn: &mut Client, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
```

### 10.1 HTTP/1.1

- **Heads are scanned** up to the blank line, under a maximum head size.
  Each is parsed into a bounded head: the request line or status line,
  and the headers as a bounded list of names and values. Header names
  are compared without regard to case.
- **Bodies are framed** in one of three ways:
  - by length: a fill;
  - chunked: a scanned size line, then a fill;
  - for a response, by the end of the stream.

  The body goes up as a stream. The side above demands it, so a slow
  reader stops the peer.
- **A client connection** carries one exchange at a time, and is reused
  when both sides allow it.
  - A response may arrive before the request is fully sent: a refusal
    in the middle of an upload, for example. The upload then stops, and
    the exchange ends with that response.
  - Interim responses (1xx) are skipped.
- **A server connection** takes one request at a time. It parses the
  next request only once the response is queued and there is room for
  the response after it (programming-style.md, section 6).
- **Not planned:** HTTP/2 and upgrades. Revisit them when a peer
  requires them.

### 10.2 Server-sent events

A machine over a body stream:

- lines, under a maximum length;
- fields;
- an event dispatched at each blank line, under a maximum event size.

Its writer side frames events for a server.

### 10.3 JSON

- **A tokenizer,** pulled by demand:
  - the nesting is held in a `lib::Stack` of configured depth
    (programming-style.md, section 7);
  - strings are unescaped and checked as UTF-8, into exact-size
    `Box<[u8]>`s under a maximum length;
  - numbers go up as validated text, never as floats. The consumer
    parses the integers it expects, with checks.
- **A sized writer:** measure first, then write with escaping into
  `Writer::new(len)`.
- **An application decodes its own documents** with small state machines
  over these tokens, in its own protocol layer, into its model's types.
  This follows consumers.md, section 5: a tool call reaches the model
  already typed.

### 10.4 TLS

- **A stream machine:** ciphertext below, plaintext above. Its plaintext
  carry-over sits in its own `Intake`, under a cap. The client comes
  first. A server side comes when a service terminates TLS itself.
- **It wraps rustls's unbuffered connection,** which does no I/O itself
  and builds without std. The shell reads certificates, keys and root
  stores at startup and hands them in as configuration.
- **It is the exception.**
  - It is the only step crate that depends on code from outside the
    workspace.
  - It is the only one that is not deterministic: its cryptography draws
    entropy from the kernel, and it reads wall time from `Env`.

  So the simulator and the protocol worlds run in plaintext. TLS is
  tested on its own, with in-memory handshakes against itself under
  every split of the ciphertext, and in the real loop.
- **Kernel TLS is deferred.** After the handshake, the record layer can
  move into the kernel, and the plaintext stream becomes the socket's
  own.

### 10.5 Names

io connects to addresses only.

- **To start:** the shell resolves the configured peer names at startup
  and hands in the addresses as configuration.
- **Later:** when a service must resolve names while it runs, the next
  protocol machine is a DNS client over UDP sockets. It reads the hosts
  file and the resolver configuration at startup. io gains datagram
  sockets for it.

## 11. The first consumer

temper is the first service built on skein, and what it builds decides
the order in which skein's parts are built:

| temper builds | which pulls from skein |
|---|---|
| the agent's LLM client | io sockets, the ring, the simulator, the HTTP client, server-sent events, JSON, the TLS client |
| the fake LLM provider, as a service | the HTTP server, the server-sent events writer, the JSON writer |
| the worker's processes and workspaces | io processes, pipes and files |
| the engine's forge client and its webhooks | the HTTP client and server |

## 12. Open questions

- **Decoding JSON by hand** into an application's types is verbose
  without serde or traits. If that hurts, the candidate is a generator
  that turns a schema into plain step code at build time, with its
  output checked in and reviewed. Procedural macros stay out.
- **Connection reuse in the HTTP client.** Sequential reuse of one
  connection is in. Still open: a pool of connections per peer, and who
  sets its size.
- **Fixed files** change what an `Fd` names: a slot in the ring's table
  instead of a descriptor. Decide when it has been measured.
- **Processes in the readiness backend on macOS,** which has no pidfd
  and no `clone3`, if that backend is ever built.
- The style's own open questions (programming-style.md, section 12) stay
  open.
