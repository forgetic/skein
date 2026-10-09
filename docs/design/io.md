# io

Provisional, 2026-10-03, revised 2026-10-09. The design of `skein-io`'s
step layer: the lowest step layer of every service. It owns sockets,
pipes, files and child processes, the operations in flight on them, and
the receive and send queues, and it is the only layer that sees a
descriptor or a kernel error. Below it is the kernel boundary
(kernel.md); above it, a service's protocol layer.

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
  start, each in a process group of its own, and termination signals
  arrive as events.
- **Files are replaced whole, or appended to.** A record is replaced
  durably, privately when it is a secret (5.2, 5.3); a trace or a log is
  a write stream on a file opened to append, with a write deadline for a
  filesystem that stalls (5.1).
- **Addresses, never names.** io connects to an address; names are
  resolved before it.

## 2. In skein

`skein-io` depends on lib. A service's protocol layer depends on it, and
so do the backends, for the kernel records. io's limits are counted in
its worst case like any layer's: the shared entity slots, the operations
in flight, each stream's intake and queued output, and children's pipe
lists. The command buffers supplied to `Spawn` belong to the caller's
memory bound.

```rust
// skein-io, the protocol side, for sockets
pub enum Request {
    Listen  { owner: Token, addr: Addr },
    Connect { owner: Token, addr: Addr },
    Bind    { socket: Token, owner: Token },          // attach to an accepted socket
    Reject  { socket: Token },
    Stream  { stream: Token, down: stream::Down },    // sockets and pipes alike
    Output  { stream: Token, down: stream::OutputDown }, // independent output reservation
    Spawn   { owner: Token, spawn: Spawn },
    Signal  { child: Token, signal: Signal, to: Target }, // the child, or its group (section 6)
    Usage   { owner: Token },                         // the process's resource usage (6.1)
    // File { owner, root, op }; Window { owner, stream } (section 7)
    Close   { entity: Token },                        // graceful (section 3); one Closed follows
    Abort   { entity: Token },
}

pub enum Event {
    Listening  { owner: Token, listener: Token, addr: Addr },
    Accepted   { owner: Token, socket: Token, peer: Addr },  // to the listener's owner
    Connecting { owner: Token, socket: Token },
    Connected  { owner: Token },
    Stream     { owner: Token, up: stream::Up },
    Output     { owner: Token, up: stream::OutputUp },
    Spawned   { owner: Token, child: Token, pipes: Box<[Token]> },
    Exited    { owner: Token, exit: Exit },
    Usage     { owner: Token, usage: Usage },         // for a Usage: its one terminal
    // File { owner, result }; Shutdown { signal }, Resized, Window { owner, size } (section 7)
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
  deadlines: a graceful close's, and a retry's; and, on files, the
  deadline an owner states for an operation a stalled filesystem may hold
  (5, 5.1).
- **What found the kernel out of buffers or descriptors is retried after
  `Limits::retry`,** not at once: a receive, a send, a half-close or an
  accept that failed for want of a buffer, an accept that failed with an
  error the kernel gave no name, and an accept out of descriptors, which
  may be the system's (`ENFILE`), given back by another process. Retried
  at once, each would spin the loop for as long as the shortage lasts.
  The retry deadline sits in io's own table beside the close deadlines,
  one of each per entity.
- **Listeners, streams, children and pipes share one entity slab,** whose
  capacity is `Limits::sockets`: one namespace of tokens, so that `Close`
  and `Abort` name any of them. A spawn reserves one slot for its child
  and one per requested pipe before it goes to the kernel.
- **A refusal is held until the next up pass.** A `Listen`, `Connect` or
  `Spawn` refused for want of entity slots has no entity to hold its `Failed`
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

- **The entities** are listeners, sockets, children and their chosen pipes.
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
  it. A listener whose accept stops is told `Failed` too, but stays: its
  owner, which holds its token, closes it. A rejected socket has no
  owner, and is closed without one.
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

Sockets and write pipes also expose lib.md, section 7.1's independent
output reservations through `Request::Output` and `Event::Output`. They
keep one pending or granted output token beside the classic demand; a
read-only demand remains live while that output progresses. The native
reservation checks both byte capacity and a Send record slot, including
the write in flight. A matching Send spends the full grant and enters the
same bounded output queue and kernel write path as classic Send. Read
pipes cannot admit output reservations.

Classic room ownership and independent output ownership are mutually
exclusive. Stale Cancel/Send/Release tokens do not affect a current
reservation. Room requests require an open, connected or bound stream;
they are not admitted after failure or closing. Cancelling an admitted
pending reservation stages its exact terminal for the next up pass.
Close/Abort likewise stages Cancelled before Closed; a genuine stream
failure emits the pending reservation's Failed terminal before classic
Failed. A previously emitted Granted is never answered again. Existing
Close/Abort's promise of only Closed applies to the classic face; the
independent face additionally owes its already admitted pending terminal.

The loop reserves the declared maximum for all simultaneous events,
including Bytes, End and an independent output terminal. The extra cell,
staged terminal and actual enum layouts enter io's checked worst-case
bound. IO support alone does not establish TLS support: a TLS consumer
must implement this face natively at both its plaintext and ciphertext
boundaries before a framed connection uses it.

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
  only when the side above will read no more: it is closing, or its read
  crossed `End`. An answer
  already on its way may still arrive, and the side above drops it. No
  other `Bytes` or `Room` come without a demand.
- **`End` comes once nothing io holds can meet a demand:** with one
  outstanding, when it can never be met; with none, only when nothing is
  held. It comes once, and ends reading only. A read that crosses it is
  never met, and `End` does not end the demand: it stays outstanding
  until `Room` answers it, if it asked for room, or until the side above
  withdraws it. Room may still be granted after `End`, as the stream can
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
- **A `Send` is within the room granted** (lib.md, 7), and io holds the
  layer above to it: each open stream keeps what its last `Room` granted,
  less what was sent since. `Room` sets it to the room asked for, each
  `Send` must fit within it and spends it, and a demand for no room leaves
  it as it is. A `Send` past it is the layer above's bug, asserted, in
  every world: it is what keeps the output under its cap by the layer
  above's asking, not by io's refusing.
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

Per entity: a slot in the slab; for each stream, an intake of `Limits::intake`; a
receive buffer of at most `Limits::receive`; the output, at most
`Limits::output` bytes in at most `Limits::sends` boxes; and a slot in the
ready lists, and two in the deadlines. An entity has at most four
operations in flight (a `Recv`, a `Send` and a cancel of each). A pipe
has at most one read or write and its cancel; a child waits and may have
one signal in flight. Thus `operations(limits)` is four per entity slot,
and one for a `Usage` (6.1), the ring's size (kernel.md, 5);
the operation table holds twice that, as an operation retired in an
iteration keeps its slot until the reclaim point. `worst_case` adds up
what the containers report (programming-model.md, 6.3), including a
child's pipe IDs and the temporary arrays on spawn completion. The
caller budgets the command buffers it supplies to `Spawn`.

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
    rename it (below);
  - stat;
  - list a directory, up to a stated count;
  - make a directory;
  - remove;
  - rename.

  Each is one request with one terminal event. Beneath it, io runs the
  open, the reads or writes, and the close.
- **What the records allow** (kernel.md, 6.1). Only an `Open` resolves a
  path beneath a root, so io opens the directory a name lies in, beneath
  the root, before it renames or removes that name or makes a directory
  there, and it stats what it has opened: a stat needs the file readable,
  until an open for a path only is pulled. Reads and writes may be short,
  and io continues them; a listing comes an entry count at a time, and
  io's stated count is a limit it checks as the entries come.
- **Replacing a file, in order:** `Stat` the old file, opened to read, for
  its permission bits; `Create` the temporary, beside the target in its
  directory, with those bits; `Write` it all; `Sync` it; `Close` it;
  `Rename` it over the target; `Sync` the directory. A reader sees the old
  file or the new, never part of either, and after the last `Sync` the
  new one survives a crash (kernel.md, 6.1).
  - The temporary is named from the target's name and a suffix drawn
    from io's seeded randomness; a `Create` that finds the name taken
    (`Exists`) draws another.
  - The old file's permission bits are kept. A target that does not
    exist, or cannot be opened to read, is made with the default,
    `0o666` less the umask.
  - A symbolic link at the target is replaced by the file, not written
    through: the link's own name gets the new file. Writing through links
    is not offered.
- **A deadline can give up on a file** (kernel.md, 6.1). An `Open`,
  `Read`, `Write` or `Sync` on a filesystem that stalls may wait for good,
  so a whole-file operation has a deadline, and at it io cancels what is
  in flight, tells the owner, and keeps the entity settling until the
  cancelled operation completes, as for a socket (section 3). Settling
  entities still hold their slots and their operations, so io caps the
  operations on files in flight, and takes no request past the cap
  (`FileIo::takes`).
- **Beyond whole files:** a file appended to as a stream (5.1); the
  durable replace that services keep their records with (5.2); and
  private files, for secrets (5.3). A file read as a stream, by demand,
  comes when a user needs to read one too large to hold.

### 5.1 Append streams

A file written as the service goes, a trace or a log, is a stream like a
write pipe's: lib's stream vocabulary (lib.md, 7), output only, driven by
the ring like any stream. It replaces a shell thread that writes the
file beside the loop (programming-model.md, 2.1).

- **Made by adoption.** The shell opens the file at startup, beneath one
  of its roots (shell.md, 6), to write at its end, creating it with the
  mode configuration gives if it is absent, and io adopts the descriptor
  as a write stream, as it adopts an inherited pipe. A standard output
  that is a regular file, as when a person redirects it, is adopted the
  same way. Opening one beneath a root while the service runs, as a file
  request, comes when a user needs it.
- **Writes go at the end,** one in flight, in order, each an `Append`
  (kernel.md, 6.1), from the output queue under its cap, as for a pipe:
  room, a `Send` within it, `Finish`. A short write continues from its
  offset in the same box. Each write
  lands at the file's end, so another process appending to the same file
  may land between two pieces of one `Send` cut short: a file has one
  appending writer, or its writers accept that.
- **The kernel's copy, not the disk's.** A write is done once the kernel
  took its bytes, which then outlive the process but not a crash of the
  machine. The stream does not sync; what must survive a crash is
  replaced whole (5.2).
- **A write deadline gives up on a stalled filesystem.** A regular file
  has no peer to stop reading, but its filesystem may stall (kernel.md,
  6.1), and a write that never completes would hold the stream's owner,
  and every step that waits on its room, for good. So the owner states,
  when it adopts the stream, the most one write may stay in flight. io
  arms that deadline with each write and, at it, cancels the write, keeps
  the stream settling until the write's completion arrives, drops what
  is queued, and tells the owner the stream failed (`Failed`), once. The
  owner decides what an abandoned stream means: a trace's sink records
  it where it still can. As for a whole-file operation (section 5), the
  deadline is the owner's policy and io's mechanism.
- **Close flushes.** `Close` writes what is queued, then closes the
  descriptor: there is no half-close, and nothing to drain. While the
  stream closes, its close deadline (section 3) replaces the write
  deadline, as a function of its state, so a filesystem that stalls at
  the end costs the stream its last bytes, never the process its end
  (programming-model.md, 5.2). `Abort` drops what is queued and cancels
  the write in flight.
- **Backpressure is the owner's choice.** An owner that must lose
  nothing asks for room before it encodes (programming-model.md, 7), so a
  slow disk holds its queue, and through it the step that fills it. One
  that may lose drops and counts when the room does not come
  (programming-model.md, section 3).

### 5.2 Durable replace

Replacing a file whole, in the order above, is how a service keeps a
record it must neither lose nor tear: a store's file, a configuration it
writes, a token record. It is the idiom of kernel.md, 6.1 (`Create`,
`Write`, `Sync`, `Rename`, then `Sync` of the directory) as one request,
run by io.

- **One request, one terminal,** answered `Stored` only after the
  directory's `Sync`: the new content then survives a crash. Until then
  the old file stands, and a reader sees the old file or the new, never
  part of either.
- **What a crash leaves:** the old file or the new, whole, and at most a
  temporary beside the target, named from it. The next replace of the
  target does not depend on it, and a scan may remove it.
- **A version check, if asked.** The request may carry a digest of the
  content it expects to replace. io checks it once more, just before the
  rename, and a target that changed is answered `Conflict`, with nothing
  replaced, so two writers of one file do not lose each other's updates
  in silence.
- **The mode** is the old file's, or `0o600` for a private file (5.3).
- **Its deadline** is the owner's, as for every whole-file operation. A
  replace that fails or times out removes its temporary before its one
  terminal, and leaves the old file as it was.
- **Not here:** updating part of a file, or a set of records larger than
  one write. Those are skein-kv's (kv.md).

### 5.3 Private files

A secret a service keeps between runs, such as a token record or a key,
lives in a private directory, readable by the service's user alone. io's
part is the modes and the checks; what the files hold, and their names,
are the owner's (oauth.md, 6).

- **A private root.** The directory is made `0o700` if it is absent, and
  opened beneath its root as a root of its own, without following a link
  at its name. io states what it opened and refuses it (`Permission`)
  unless it is a directory with no group or other permission bits.
- **Private files are made `0o600`,** less the umask, which only takes
  bits away, and are replaced whole with that mode (5.2), never with the
  old file's: a file found with group or other bits is not copied
  forward.
- **Reading one checks what was opened:** a regular file, reached
  without following a link, with no group or other permission bits, one
  link, and the directory's owner. Anything else is refused unread. It is
  read whole within a stated maximum (section 5).
- **What io does not do:** encrypt, lock, or keep a secret out of memory.
  A secret's bytes are its owner's, which keeps them out of logs, traces
  and `Debug` (oauth.md, 4).

The checks of owner and links need the kernel's `Stat` to answer both
(kernel.md, 6.1).

## 6. Processes

- **Spawn** takes a program, its arguments, its environment, a working
  directory beneath a root, and which pipes to make. It answers with a
  child and its pipes.
  - The ring adapter uses glibc's `pidfd_spawn`, so the child is a pidfd from
    the start. Its file actions install requested pipes at chosen child
    descriptors and close everything else on exec. The kernel's own check
    then stops a reused PID from being signalled. `std::process` is not used.
  - The child starts in a process group of its own, so a terminal's
    interrupt reaches the service alone, which decides what its children
    hear (section 7).
  - A requested pipe has a chosen child descriptor and direction. Its
    parent end is a one-way stream token in the `Spawned` event, in request
    order. Each pipe consumes one entity slot alongside the child.
- **Exit** is a wait on the pidfd, through the ring (`waitid`). It
  observes the exit without reaping the child (`WNOWAIT`): the child
  stays unreaped, holding its PID, until io closes it.
- **Signals** go to the child alone, or to its process group: `Signal`
  names which (`Target::Child` or `Target::Group`). Both go through the
  child's pidfd, `pidfd_send_signal`, the group's with the kernel's
  process-group scope, which reaches the group the child leads from its
  spawn (kernel.md, 6.2). The group's ID is the child's PID, which the
  kernel cannot give another process while the child is unreaped. A
  signal to the group therefore reaches the child and whatever it started
  that stayed in its group, and never a stranger, even once the child
  itself has exited.
- **A child is *closed*** once it has exited and its pipes and pidfd are
  closed. It has one terminal event, which comes after all of those.
  Closing a child sends `Kill` to its group, whether or not the child
  itself has exited, then reaps it: what it started in its group ends
  with it. The exit and pipe closures still settle before its `Closed`
  event.
- **A timeout is the owner's** (programming-model.md, 4). An owner that
  gives a child a deadline signals its group when it passes, then closes
  it. Whatever the child started that holds its pipes then ends too, and
  the pipes with it, so a command that timed out is answered within its
  deadline and io's close deadline, not when the last of its descendants
  chooses to exit.
- **The group is an interim, before contained trees.** A descendant that
  leaves the group (`setsid`, `setpgid`) escapes its signals and outlives
  its child, and nothing bounds or counts what a group may use: a
  descendant reparented away from the child is in no one's resource
  usage (6.1). Contained trees (draft/process.md) replace it later: a
  cgroup per tree, proved empty, with per-tree limits and accounting and
  a view of the file system. They are not designed into io yet.

### 6.1 Resource usage

A service may report what it used, at its end: CPU time and peak
resident size, its own and its children's. io reads them from the kernel
for it.

- **One request, one terminal.** `Usage` answers `Usage` with two parts:
  the process's own, and its reaped children's. Each holds user and
  system CPU time, and the peak resident size in bytes. The children's
  CPU is the sum over the children reaped so far, and their peak is the
  largest one of them reached, as the kernel counts them.
- **It holds no entity.** The kernel reads it at submit (kernel.md, 6.3).
  One may be in flight at a time, in an operation slot of its own; a
  second while one is in flight is the layer above's bug, asserted.
- **A child counts once io has reaped it,** at its close (section 6),
  not when it exits: a service that wants every child in its report asks
  after its last child's `Closed`. What a child started counts only if
  the child itself reaped it: a descendant reparented away, as one the
  group's `Kill` ends after the child exited, is not counted (section 6).
- **When to ask is the service's.** A service that writes a report at its
  end asks once its children have closed and before the stream it writes
  the report to closes (shell.md, 13).

## 7. Signals to the service

Termination signals are blocked at startup (shell.md, 6), read from a
signalfd through the ring, and arrive as `Shutdown` events. The domain
decides what shutting down means.

A service at a terminal also blocks `SIGWINCH` at startup (shell.md,
6.3). It is read from the same signalfd and arrives as `Resized`, after
which the owner asks for the size: `Window { owner, stream }`, naming the
stream that reads the terminal, answered `Window { owner, size }`, rows
and columns, its one terminal (kernel.md, 6.3).

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
given status, one that never exits. Its files are built, behind the
simulator's machine seam (testing.md, 4).

Built, for sockets:

- **Step tests** (`crates/skein-io/src/tests/`): each cell of both
  machines driven by hand, the refusals at a full slab, stale tokens
  dropped going down and asserted going up, every outcome of a cancel in
  every order, each call with exactly its `MAX_OUT` of room; sends within
  the room granted, and one past it, or with none granted, asserted.
- **io worlds** (`tests/io`, `skein-io-world`): a loop over the
  simulator drives each process's io and a scripted owner, which runs
  each connection by a plan of sends within the room granted and demands
  of every kind; a referee holds each scenario's expectations; the harness
  checks `MAX_OUT` and the accept batch at each call, both halves of the
  stream contract in a ledger (io's, and the owner's as the side above),
  the invariants once settled, and replay. Thirteen scenarios: accept,
  bind and reject; connects made, refused for a slot and by the peer;
  connects waiting on a full backlog, cancelled; descriptors run out, so
  a socket is refused and a listener starves; two listeners under one
  accept batch; a socket discarded for want of a slot; a burst of
  connects past the slab and the refusals; an exchange both ways;
  backpressure through io; a refusal mid-upload that still reaches the
  peer, which hears the end and no reset; abort; the close deadline;
  closes and aborts at random moments. Each admission point's scenario
  checks its trace for evidence the point was reached. A few seeds each,
  calm and chaotic, in the focused suite; 150 of each in the fuzzy one,
  which asserts that every fault fell and that a cancel of each operation
  io cancels was seen to stop it, to come too late, and to go
  unsubmitted.
- **One exchange over the real ring**, in the focused suite.
- **Memory:** io driven by hand to its limits and back, every call
  checked against `worst_case` with the counting allocator, at four sets
  of limits, one of them with receive buffers that dwarf the rest; and a
  listener's life, its sockets announced, rejected, bound and discarded.
- **Processes and pipes:** step tests exercise spawn, wait, signal, the
  one-way pipe streams and child closure after its pipes. A separate
  counting-allocator test fills the entity slab with a child and its
  pipes, checks the spawn completion and arms a read on every pipe
  against `worst_case`.

To come with what section 9 builds:

- **Process groups:** on the real kernel, a child that starts a
  descendant holding its pipe and exits, then a signal to its group,
  which ends the descendant and the pipe; a close of a child whose group
  still runs; a signal to a group after its leader exited, before its
  close. In the simulator, groups the minimal machine's programs make,
  and the same cases.
- **Append streams:** io worlds over the simulator with sends within the
  room granted, short writes, a close that flushes, and the simulator's
  hung `Write`, met by the write deadline while open and by the close
  deadline while closing; the conformance suite, on the ring and the
  simulator, for a write at the end of a file opened to append.
- **Durable replace and private files:** every step of the replace
  failed in turn, the temporary removed each time, and a conflict; a
  private directory and file refused for each bit, link and kind; and,
  once the simulator can cut a process at an operation (simulator.md,
  3.3), a cut at each `Sync` and `Rename`, after which the target is old
  or new and whole.
- **Resource usage:** in the simulator, a usage asked before and after a
  child's close, its part joining the children's only at the close; on
  the real kernel, the conformance suite's (kernel.md, 8).

## 9. Remaining work

Built:

- sockets, and socket io worlds;
- processes with pipes, each child in a process group of its own;
- signals to the service, read from the signalfd the shell opens at
  startup (section 7);
- files, as io's file layer (`FileIo`): the whole-file operations of
  section 5, each one request with one terminal under its owner's
  deadline: a load within a maximum, a scan within an entry count and a
  byte bound, a store that replaces a file durably with its version
  check (5.2), stat, rename and remove; and open, read, write, sync and
  close for an owner that keeps a file open, as skein-kv does.

Not built yet, in the order their first users pull them:

1. signalling a child's group, and reaping a child only when it closes
   (section 6);
2. append streams (5.1), private files (5.3) and resource usage (6.1),
   for a trace, a token store and a report at a process's end;
3. making a directory as a file request (section 5);
4. streams that read files, and datagram sockets, when a user needs them.

Transition coverage of the handlers (testing-strategy.md, 6) waits for
`cargo llvm-cov`, which is not installed.

## 10. Open questions

- **A stream's fault for a stall.** A write deadline fails an append
  stream with lib's `Fault::Other`, as a full disk does. Whether `Fault`
  should name a stall, so its owner can tell a filesystem that stopped
  from one that refused.
- **Operations the kernel cannot interrupt.** A cancelled write on a
  filesystem in uninterruptible sleep keeps its stream settling, and its
  process from ending (notes.md).
