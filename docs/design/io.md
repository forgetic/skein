# io

Provisional, 2026-10-03. The design of `skein-io`'s step layer: the
lowest step layer of every service. It owns sockets, pipes, files and
child processes, the operations in flight on them, and the receive and
send queues, and it is the only layer that sees a descriptor or a kernel
error. Below it is the kernel boundary (kernel.md); above it, a service's
protocol layer.

## 1. In one page

- **Entities, and streams.** io's vocabulary has two parts: entities
  (listen, accept, connect, spawn, a root, close) and bytes, which are the
  stream vocabulary of lib (lib.md, 7), the same for a socket and a pipe.
- **Every completion goes through io first.** io turns the kernel's
  completions into events about its entities, and requests about its
  entities into kernel operations, and absorbs the mechanics in between:
  buffers in flight, short transfers, cancellation, settling, graceful
  close.
- **Flow control is explicit.** A receive is in flight only while the
  intake has room, a send only for queued output under its cap, and an
  accept only while the listener's owner has room.
- **Paths stay beneath a root,** spawned children are pidfds from the
  start, and termination signals arrive as events.
- **Addresses, never names.** io connects to an address; names are
  resolved before it.

## 2. In skein

`skein-io` depends on lib. A service's protocol layer depends on it, and
so do the backends, for the kernel records. io's limits are counted in
its worst case like any layer's: the entities of each kind, the
operations in flight, each stream's intake and queued output.

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
    Close   { entity: Token },                        // graceful (section 3); one Closed follows
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

An accepted socket is announced to the listener's owner, which binds to
it or rejects it (programming-model.md, 4.2).

## 3. Sockets and pipes

- **The entities** are listeners, sockets, and a child's three pipes.
  Sockets and pipes are both streams; above io, they differ only in how
  they were made.
- **One receive is in flight** per stream while its intake has room,
  whether or not anything has been demanded yet. What arrives goes into
  the stream's `Intake`, which meets the demand from above.
- **One send is in flight** per stream, in order. The rest of the output
  waits in a queue under the output cap. A short send continues from an
  offset into the same `Box`; the remainder is never copied.
- **Accepts are re-armed one at a time** while the listener's owner has
  room, up to a configured number per iteration. A flood of connections
  then waits in the kernel's backlog.
- **Close is graceful:**
  1. flush the queued output;
  2. half-close;
  3. read and discard until the peer ends or the close deadline passes;
  4. close.

  This matters when a response goes out while the peer is still sending,
  such as a refusal in the middle of an upload: the response reaches the
  peer instead of being lost to a reset. Abort closes at once.
- **Cancellation is io's to settle.** An operation cancelled by a close, an
  abort or a deadline stays *settling* until its completion arrives, and
  the entity above sees one event (programming-model.md, 5.3).

## 4. Addresses and names

- **io connects to addresses only.**
- **To start:** the shell resolves the configured peer names at startup
  and hands in the addresses as configuration (shell.md, 6).
- **Later:** when a service must resolve names while it runs, the next
  protocol machine is a DNS client over UDP sockets. It reads the hosts
  file and the resolver configuration at startup. io gains datagram
  sockets for it.

## 5. Files

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

  Each is one request with one terminal event. Beneath it, io runs the
  open, the reads or writes, and the close.
- **File streams come later,** when a user needs to read a file by demand
  because it is too large to hold.

## 6. Processes

- **Spawn** takes a program, its arguments, its environment, a working
  directory beneath a root, and which pipes to make. It answers with a
  child and its pipes.
  - It is `clone3` with `CLONE_PIDFD`, so the child is a pidfd from the
    start. The kernel's own check then stops a reused PID from being
    signalled. `std::process` is not used.
  - Containment (namespaces, `CLONE_INTO_CGROUP`) is a spawn option,
    given as data.
- **Exit** is a wait on the pidfd, through the ring (`waitid`).
- **Signals** go through `pidfd_send_signal`.
- **A child is *closed*** once it has exited and its pipes and pidfd are
  closed. It has one terminal event, which comes after all of those.

## 7. Signals to the service

Termination signals are blocked at startup (shell.md, 6), read from a
signalfd through the ring, and arrive as `Shutdown` events. The domain
decides what shutting down means.

## 8. Testing

io's tier is io worlds (testing-strategy.md, 2.6): io over the
simulator, with a scripted owner as the step above it. It listens,
connects, streams, opens files and spawns, in an order the test chooses,
with closes and aborts in every state, under tiny limits and every fault
the simulator injects. The worlds check what io adds: buffers in flight,
cancellation and settling, graceful close, the receive and send queues
under their caps, and the accept budget; and every contract and
invariant of testing-strategy.md, 6.

Files and processes go to skein's minimal fake machine: a few files
beneath a root, a program that echoes its input, one that exits with a
given status, one that never exits.

## 9. Not built yet

All of io's step layer. The records of the kernel boundary below it are
built, for sockets. The order follows what temper pulls:

1. sockets, with io worlds over the simulator, for the agent's LLM
   client;
2. processes, pipes and files, with the minimal fake machine and the
   simulator's machine seam, for the worker;
3. signals to the service, with the shell's startup.

File streams and datagram sockets come when a user needs them.
