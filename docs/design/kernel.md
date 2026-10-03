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
- a close with anything else in flight.

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
- What no record can observe (close-on-exec, `TCP_NODELAY`) is not
  checked; `SO_REUSEADDR` and `IPV6_V6ONLY` are, by their effects.

## 9. Open questions

- **Fixed files** change what an `Fd` names: a slot in the ring's table
  instead of a descriptor. Decide when it has been measured.

## 10. Not built yet

- **The records for files and processes,** with their rules and their
  conformance scenarios (a scratch directory as the root), when io pulls
  them. Sockets are built.
- **The descriptor limit on the ring.** The simulator checks it; lowering
  a process's limit on the real kernel takes `unsafe` outside the ring
  adapter, or a child process, and neither is allowed.
