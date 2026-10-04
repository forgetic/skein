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
// skein-io, the protocol side, for sockets
pub enum Request {
    Listen  { owner: Token, addr: Addr },
    Connect { owner: Token, addr: Addr },
    Bind    { socket: Token, owner: Token },          // attach to an accepted socket
    Reject  { socket: Token },
    Stream  { stream: Token, down: stream::Down },    // sockets and pipes alike
    // File { owner, root, op }, Spawn { owner, spawn }, Signal { child, signal }
    Close   { entity: Token },                        // graceful (section 3); one Closed follows
    Abort   { entity: Token },
}

pub enum Event {
    Listening  { owner: Token, listener: Token, addr: Addr },
    Accepted   { owner: Token, socket: Token, peer: Addr },  // to the listener's owner
    Connecting { owner: Token, socket: Token },
    Connected  { owner: Token },
    Stream     { owner: Token, up: stream::Up },
    // File { owner, result }, Spawned { owner, child, pipes },
    // Exited { owner, exit }, Shutdown { signal }
    Failed     { owner: Token, error: Error },
    Closed     { owner: Token },                      // terminal
}

pub enum Error { Refused, Unreachable, TimedOut, Reset, Busy, Other }
```

An accepted socket is announced to the listener's owner, which binds to
it or rejects it (programming-model.md, 4.2).

The loop drives io through four entry points, each declaring its
`MAX_OUT` (programming-model.md, section 2): `resume` takes one entry of
the ready list, `up` one completion, `fire` one expired deadline (a
graceful close's, or a retry's), and `down` one request. io's stage in the up pass is `resume` until the
ready list is empty, then `up` for each completion, then `fire` while a
deadline is due; the loop hands io no completion while the ready list
holds something, so what the down pass made is told first. Requests to
the kernel go out as `Submit` records into the queue the loop hands the
kernel.

**Decisions.**

- **Every event names its owner first:** the token the layer above gave
  for the entity concerned. `Accepted` names the listener's owner, which
  answers it.
- **`Listening` carries the bound address,** so that a listener on port 0
  is usable.
- **`Connecting` hands over the socket at once,** in the up pass after the
  `Connect`, so that its owner can close or abort a connect before it
  connects. A listener's setup waits on nothing outside the host, so its
  owner closes it once it hears `Listening`.
- **io has its own `Error`,** carrying only what the layer above can act
  on: `Refused` (nothing listened there), `Unreachable`, `TimedOut` (the
  kernel gave up on the peer), `Reset`, and `Busy`, a refusal at io's
  entrance: no socket slot, or the kernel out of descriptors, local ports
  or buffers. Every other kernel error is `Other`, among them an address
  in use or not this host's; one the contract rules out is an assertion.
  A stream's failure is lib's `Fault`: `Reset` when the peer is gone
  (reset, timed out, unreachable, the pipe broken), `Other` otherwise.
- **io has no connect timeout.** That is policy above
  (programming-model.md, 4): the connection's deadline lives in the
  protocol layer, which aborts the connect. io keeps only the mechanics'
  deadlines: a graceful close's, and a retry's.
- **What found the kernel out of buffers or descriptors is retried after
  `Limits::retry`,** not at once: a receive, a send, a half-close or an
  accept that failed for want of a buffer, an accept that failed with an
  error the kernel gave no name, and an accept out of descriptors, which
  may be the system's (`ENFILE`), given back by another process. Retried
  at once, each would spin the loop for as long as the shortage lasts.
  The retry deadline sits in io's own table beside the close deadlines,
  one of each per entity.
- **Listeners and streams share one slab of sockets,** whose capacity is
  the admission limit: one namespace of tokens, so that `Close` and
  `Abort` name either.
- **A refusal is held until the next up pass.** A `Listen` or `Connect`
  refused for want of a socket slot has no entity to hold its `Failed`
  and `Closed`, so io holds the owner's token in a list of its own,
  bounded by `Limits::refusals`, and `resume` tells it. The loop hands io
  a request only while io can hold a refusal (`Io::takes`), as it
  reserves room in the submissions.
- **The startup check.** Whoever stacks a machine on a stream checks that
  the machine's largest read demand is no more than
  `Limits::largest_read` (the intake cap), and its largest room no more
  than `Limits::largest_room` (the output cap). A demand past either
  could never be met, and io asserts it, as lib's intake does (lib.md,
  7).

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

### 3.1 Entities and owners

| Entity | Made by | Owned by | Named above by | Ends with |
|---|---|---|---|---|
| listener | `Listen` | the owner of the `Listen` | `Listening.listener` | `Closed` |
| socket, connecting | `Connect` | the owner of the `Connect` | `Connecting.socket` | `Closed` |
| socket, accepted | a listener's accept | io until answered; then the owner of the `Bind` | `Accepted.socket` | `Closed` once bound; nothing once rejected |
| operation | io, for an entity | that entity, until its completion | its `Submit` token, below | its completion |

- **A listener and a socket are one kind of entity** in one slab,
  `Limits::sockets` of them, each running the machine of its kind. The
  operation table holds the operations in flight, each for its entity. An
  entity is retired only once nothing of its is in flight, its cancels
  included, so a completion always finds it.
- **One `Closed` per entity with an owner,** the last event naming that
  owner. A `Listen` or `Connect` that fails is told `Failed`, and io
  closes what it made by itself, since its owner may hold no token for
  it. A rejected socket has no owner, and is closed without one.
- **`Failed` and `End` are told once each,** and never after `Closed`.
  After its owner's `Close` or `Abort`, an entity tells only `Closed`.
- **Stale handles.** A request naming an entity that is gone, or closing,
  is dropped. One naming an entity of the wrong kind, or an announced
  socket before its answer, is the layer above's bug, asserted. A
  completion always finds its operation and its entity, which is
  asserted.

### 3.2 The listener

| State | Holds | In flight |
|---|---|---|
| Socket | the address | `Socket` |
| Binding | fd | `Bind` |
| Arming | fd, the bound address | `Listen` |
| Listening | fd, the accept, discards | `Accept` when armed; the closes of discarded sockets |
| Settling | fd, the accept if in flight, cancels, discards | those |
| Releasing | | `Close` |
| Closed | | |

The accept, in `Listening`, is `Armed` (an `Accept` in flight),
`Answering` (a socket announced and not yet bound or rejected: the owner
has no room until it answers, and holds its answer while it has none),
`Idle` (on the ready list for the next iteration's accept batch, among
the starved for a socket slot or a descriptor, or waiting for its retry
deadline), or `Stopped` (an error io cannot retry, told as `Failed`). Arming it takes a free socket slot, no
discard in flight, and room in the accept batch, which counts the accepts
armed in an iteration over every listener. The starved are woken when io
retires an entity or closes a discarded socket.

| State | Event | Next, and what it does |
|---|---|---|
| Socket | `Socket` ok / error | Binding, `Bind` / Closed: `Failed`, `Closed` |
| Binding | `Bind` ok / error | Arming, `Listen` / Releasing: `Failed`, `Close` |
| Arming | `Listen` ok / error | Listening: `Listening`, arm / Releasing: `Failed`, `Close` |
| Listening | `Accept` ok | Answering: `Accepted`; with no slot left, the socket discarded and Idle, starved |
| Listening | `Accept`: no descriptor | Idle, starved, and its retry deadline armed |
| Listening | `Accept`: no buffer, or an error with no name | Idle, its retry deadline armed |
| Listening | `Accept`: the connection's own error | Idle, retried in the next iteration |
| Listening | the retry deadline | arm |
| Listening | `Accept`: any other error | Stopped: `Failed` |
| Listening | a discard done | the starved woken |
| Listening | its socket answered; resumed or woken while Idle | arm |
| Listening | `Close`, `Abort` | Settling: `Cancel` of the armed accept; Settling for the discards; else Releasing: `Close` |
| Settling | `Accept` done | an accepted socket discarded |
| Settling | `Cancel` done | retried, unsubmitted, while the accept waits |
| Settling | nothing left in flight | Releasing: `Close` |
| Releasing | `Close` done | Closed: `Closed` |
| Settling, Releasing, Closed | `Close`, `Abort` | ignored |
| any but Listening and Idle | the retry deadline | impossible: cancelled once the accept leaves Idle |
| Socket, Binding, Arming | `Close`, `Abort` | impossible: the owner has no token yet |
| any | `Bind`, `Reject`, `Stream` | impossible: the layer above's bug |

A socket accepted after the owner closed the listener is discarded by
io, unannounced: the owner asked for no more.

**Accept errors.** Out of descriptors, the listener starves until io
gives one back. Out of buffers, or with the network error of the
connection it took (a reset, a timeout, no route, or an error the kernel
gave no name: accept(2) has these retried), it tries again in the next
iteration. Any other error says the socket no longer listens, and stops
it.

### 3.3 The stream

| State | Holds | In flight | Serves | Deadline |
|---|---|---|---|---|
| Opening | owner, address | `Socket` | | |
| Socket | owner, address | `Socket` | | |
| Connecting | owner, fd | `Connect` | | |
| Announced | fd, its listener | | | |
| Open | owner, fd, intake, demand, reader, writer, output | a `Recv` while the intake has room; a `Send` or a `Shutdown` | the demand | |
| Broken | owner, fd, what is in flight | what was, finishing | | |
| Closing | owner, fd, drain, flush, output | a `Recv`, discarding; a `Send` or a `Shutdown` | | close |
| Settling | owner if bound, fd if made, what it waits for, cancels | those | | |
| Releasing | owner if bound | `Close` | | |
| Closed | | | | |

- **Opening** is a connect whose `Connecting` is not told yet. It is on
  the ready list, which the loop drains before it hands io completions.
- **Open's reader** is `Receiving` (a `Recv` in flight), `Full` (the
  intake is), `Stalled` (no buffer: waiting for the retry deadline),
  `Ended` (the peer ended, `End` not told yet) or `Told`. **Its writer**
  is `Idle`, `Sending` (finishing or not: the half-close follows the last
  of the output), `Stalled` (holding the send's box and offset), `Shutting`,
  `Unshut` (a half-close that found no buffer) or `Shut`. **Closing's
  drain** is a `Recv` in flight, `Stalled`, or done; **its flush** is
  `Sending`, `Stalled`, `Shutting`, `Unshut`, or done.

| State | Event | Next, and what it does |
|---|---|---|
| Opening | resumed | Socket: `Connecting` |
| Socket | `Socket` ok / error | Connecting, `Connect` / Closed: `Failed`, `Closed` |
| Connecting | `Connect` ok / error | Open: `Connected`, `Recv` / Releasing: `Failed`, `Close` |
| Announced | `Bind` / `Reject` | Open, `Recv` / Releasing, `Close`; the listener re-arms |
| Open | `Recv` of n > 0 | the intake appended; delivered |
| Open | `Recv` of 0 | reader Ended; delivered |
| Open | `Send` of n | the same box from its new offset, or the next, or the half-close when finishing; delivered |
| Open | `Shutdown` ok | writer Shut |
| Open | `Recv`, `Send`, `Shutdown`: no buffer | that side stalls, the same again at the retry deadline |
| Open | the retry deadline | what stalled submitted again |
| Open | `Recv`, `Send`, `Shutdown`: any other error | Broken: `Failed` |
| Open | `Demand` | stored; on the ready list |
| Open | `Send` | queued; sent at once by an idle writer |
| Open | `Finish` | the half-close, once flushed |
| Open | resumed | delivered |
| Open | `Close` | Closing, its deadline armed; Releasing if nothing is left to flush or drain |
| Broken | a completion | that operation is done |
| Closing | `Recv` | discarded; again, until 0 or an error |
| Closing | `Send`, `Shutdown` | flushed, then half-closed; an error ends the flush |
| Closing | drained and flushed | Releasing: `Close`, the deadline cancelled |
| Closing | `Recv`, `Send`, `Shutdown`: no buffer | that side stalls, the same again at the retry deadline |
| Closing | the retry deadline | what stalled submitted again |
| Closing | the close deadline | Settling: `Cancel`s; what stalled is dropped |
| Socket, Connecting, Broken | `Close` | as `Abort`: there is nothing to flush |
| Socket, Connecting, Open, Broken, Closing | `Abort` | Settling: `Cancel`s; Releasing if nothing is in flight |
| Closing | `Close` | ignored |
| Settling | a completion | done; an unsubmitted cancel retried while its target waits; nothing left: Releasing, `Close`; or Closed, `Closed`, if no fd was made |
| Releasing | `Close` done | Closed: `Closed` if bound |
| Broken, Closing, Settling, Releasing, Closed | `Stream` | dropped: the stream failed, or its owner closed it |
| Settling, Releasing, Closed | `Close`, `Abort` | ignored |
| Opening, Announced | `Close`, `Abort`, `Stream` | impossible: no token yet, or no answer yet |
| Socket, Connecting | `Stream` | impossible: a stream before it is connected |
| any | `Bind`, `Reject` but Announced | impossible: the layer above's bug |
| any but Settling | a completion `Cancelled` | impossible: io cancels only when settling |

**Delivered** is one function of Open, applied once after every
transition in the up pass (a completion, a deadline), whatever cell made
it, and by `resume` after a request in the down pass
(programming-model.md, 2 and 5.4):

1. the demand's answer, if there is one: `Bytes`, if the intake meets its
   read (never after `End`); or else `Room`, if it asks for room, the
   writer takes sends, and the output has that many bytes and one more
   `Send` free. Either answer ends the demand, read and room;
2. `End`, once, if the peer ended and a read is outstanding that can
   never be met, or none is and the intake is empty;
3. a `Recv`, if none is in flight and the intake has room again.

io keeps the contract of a stream (lib.md, 7) as the side below:

- **A demand is answered at most once:** by `Bytes`, exactly what its
  read asks for, or by `Room`, whichever io can give first; either answer
  ends it, and nothing is outstanding until the side above states its
  next. The side above states its next demand only after an answer, never
  in place of one outstanding, which io asserts.
- **`Read::Nothing` with no room withdraws** the outstanding demand, and
  only when the side above will read no more (it is closing). An answer
  already on its way may still arrive, and the side above drops it. No
  other `Bytes` or `Room` come without a demand.
- **`End` comes once nothing io holds can meet a demand:** with one
  outstanding, when it can never be met; with none, only when nothing is
  held. It comes once, and ends reading only: a read that crosses it is
  never met, but room may still be granted after it, as the stream can
  still send to a peer that only half-closed. A read larger than what is
  left before the end is never met; a side above that must see every
  byte reads by its framing.
- **`Failed` may come at any time,** a demand outstanding or not, and
  nothing follows it but `Closed`. After `End` it says only that the
  stream can no longer send: what was read stands.
- **A scan that meets no delimiter within its maximum delivers exactly
  the maximum,** and the side above decides what that means.
- **Room grants one more `Send`** of up to that many bytes, and the
  output counts a `Send` in flight until all of it is sent. A `Send` past
  the output cap or `Limits::sends`, or after `Finish`, is the layer
  above's bug, asserted; so is room demanded after `Finish`.
- **`Failed` is told at once,** and drops what the intake held and the
  output queued: the peer is gone.
- **Discarding starts with the close,** not after the half-close, so that
  a peer blocked on an upload drains, then reads the response.
- **Only operations that wait are cancelled:** `Accept`, `Connect`,
  `Recv` and `Send`. `Socket`, `Bind`, `Listen`, `Shutdown` and `Close`
  are waited for.
- **No buffer is not a failure.** A `Recv`, `Send` or `Shutdown` that
  fails for want of kernel buffers did nothing: its side *stalls*, a
  stalled send keeping its box and offset, and is submitted again once
  the retry deadline passes. The deadlines a state implies are one
  function of it, applied after every transition: the close deadline runs
  only while closing, the retry deadline only while a side stalls.

### 3.4 Memory

Per socket: the entity in its slab; an intake of `Limits::intake`; a
receive buffer of at most `Limits::receive`; the output, at most
`Limits::output` bytes in at most `Limits::sends` boxes; and a slot in the
ready lists, and two in the deadlines. An entity has at most four
operations in flight (a `Recv`, a `Send` and a cancel of each), so
`operations(limits)` is four per socket, the ring's size (kernel.md, 5);
the operation table holds twice that, as an operation retired in an
iteration keeps its slot until the reclaim point. `worst_case` adds up
what the containers report (programming-model.md, 6.3).

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

Built, for sockets:

- **Step tests** (`crates/skein-io/src/tests/`): each cell of both
  machines driven by hand, the refusals at a full slab, stale tokens
  dropped going down and asserted going up, every outcome of a cancel in
  every order, each call with exactly its `MAX_OUT` of room.
- **io worlds** (`tests/io`, `skein-io-world`): a loop over the
  simulator drives each process's io and a scripted owner, which runs
  each connection by a plan of sends within the room granted and demands
  of every kind; a referee holds each scenario's expectations; the harness
  checks `MAX_OUT` at each call, io's contract with the owner in a ledger,
  the invariants once settled, and replay. Nine scenarios: accept, bind
  and reject; connects made, refused for a slot and by the peer; connects
  waiting on a full backlog, cancelled; an exchange both ways; backpressure
  through io; a refusal mid-upload that still reaches the peer; abort; the
  close deadline; closes and aborts at random moments. A few seeds each,
  calm and chaotic, in the focused suite; 150 of each in the fuzzy one,
  which asserts that every fault fell and that a cancel of each operation
  io cancels was seen to stop it, to come too late, and to go
  unsubmitted.
- **One exchange over the real ring**, in the focused suite.
- **Memory:** io driven by hand to its limits and back, every call
  checked against `worst_case` with the counting allocator.

## 9. Not built yet

Sockets are built, with their io worlds. The order follows what temper
pulls:

1. processes, pipes and files, with the minimal fake machine and the
   simulator's machine seam, for the worker;
2. signals to the service, with the shell's startup.

File streams and datagram sockets come when a user needs them.
Transition coverage of the handlers (testing-strategy.md, 6) waits for
`cargo llvm-cov`, which is not installed.
