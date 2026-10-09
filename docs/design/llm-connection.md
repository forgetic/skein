# LLM connections

Provisional, 2026-10-07; revised 2026-10-09. `skein-llm-connection` runs
LLM calls end to end for the protocol layer that owns it. For each call it:

- connects through io;
- runs TLS, or plaintext to an endpoint on loopback;
- drives skein's LLM client (llm.md);
- arms the deadlines the client leaves to its owner;
- reuses connections that are idle;
- makes a call wait when no connection or memory can take it yet.

When its owner closes it, it closes everything it keeps and says so. Its
owner gives it calls and gets their answers, so a service that calls LLMs
translates its own vocabulary and writes no connection code. smith's
agent is its first user.

## 1. In one page

- **A component of a protocol layer.** It is a step machine with its own
  state, limits, worst case and most outputs per entry point, which a
  service's protocol layer owns and routes to (programming-model.md,
  sections 4 and 4.2). It depends on:
  - lib;
  - io's vocabulary;
  - skein's TLS machine;
  - skein's LLM client.
- **Calls in, answers out.** The owner starts a call with an endpoint, a
  prompt, a credential and its deadlines. It hears the call's deltas,
  completed blocks and one terminal: completed, failed or cancelled.
- **Connections are its own:**
  - a bounded pool per endpoint;
  - one call per connection at a time;
  - reused once a response has drained;
  - closed after an idle while, or at once when the owner closes the
    component.
- **A call that cannot be served now waits.** When no connection can
  take it, or the memory it needs is in use, an admitted call waits its
  turn, bounded by the owner's declared conversations and within its own
  whole deadline. A pool at capacity is never a refusal.
- **Memory is admitted, not multiplied.** Each call reserves what its
  request and its answer can hold from one memory pool before it reaches
  a connection. The worst case is the pool, not every connection at its
  maxima at once.
- **Deadlines are given per call and armed here,** each as a function of
  the phase the call and its connection are in: connecting, TLS's
  handshake, the response's head, idleness between events, and the call
  as a whole. A deadline that passes fails the call as timed out, naming
  the deadline, with the client's evidence of whether the request was
  sent. A closing connection has none: io's close deadline bounds it.
- **The owner close drains.** The component refuses new calls at its
  entrance, closes idle connections at once and busy ones when their calls
  end, and reports `closed` once every socket has settled. Its idle keep
  is policy for a live service, never the way it ends
  (programming-model.md, section 5.2). An owner that wants an abort
  cancels its calls, or aborts.
- **No retries and no policy.** Whether to call again is the owner's
  domain's.
- **Credentials pass through.** The owner hands the credential's value with
  each call. This component keeps none beyond the call's request.

## 2. In skein

- **`skein-llm-connection`** depends on lib, io, `skein-tls` and
  `skein-llm`. It knows no service's domain, tools or credentials store.
- **The owner's protocol layer:**
  - gives io its own tokens for the component's entities, keeping the
    component's token in each, so io's events reach the component that
    asked;
  - reports the component's earliest deadline, and fires its deadlines at
    the point where it fires its own;
  - closes the component as part of its own end, and counts it settled
    only on the component's `closed` (programming-model.md, section 5.2).

## 3. Endpoints

An endpoint is configuration, given when the component is made. It has:

- **an address,** resolved before the loop starts (io.md, section 4);
- **its transport:** TLS, with the server name and what to trust it by;
  or plaintext, refused at configuration unless the address is on
  loopback. Plaintext serves a server on the same machine, such as a local
  model's, and the replaying tiers' fakes (testing-strategy.md, 4.4);
- **skein's LLM endpoint:** the dialect, the path, and the headers the
  dialect needs, such as an identity profile (llm.md). Its headers may not
  name a field the client writes itself, the dialect's per-call headers
  included (llm.md, section 4.6);
- **its limits,** derived for its dialect from the owner's declaration
  for the models the endpoint serves (llm.md, section 4.2). The request
  head's limit is measured from the endpoint's headers and the owner's
  bound on a credential when the component is made.

The owner names an endpoint by its index in that configuration.

## 4. Calls

| From the owner | What it does |
|---|---|
| start | a call: its token, the endpoint, the prompt, the credential and its deadlines |
| next | ask for one more delta, block or the terminal, as skein's client does |
| cancel | end the call; its terminal follows |
| close | the owner close: refuse new calls, drain the rest (5.1) |
| abort | a close that does not wait: cancel every call, abort every socket (5.1) |

| To the owner | What it says |
|---|---|
| refused | a start not admitted, and why; no terminal follows |
| delta | a piece of the answer as it comes |
| block | a completed block |
| completed | the call's completion: its blocks, stop reason and usage |
| failed | the class, the evidence, and the provider's details within bounds |
| cancelled | the call ended at the owner's request |
| closed | after a close or an abort: no call is left and every socket has settled |

- **Admission is at the entrance.** A start is refused at once, with no
  terminal, in these cases:
  - the owner has closed or aborted the component: the refusal says the
    component is closed, never that the request was at fault
    (programming-model.md, section 5.2);
  - the endpoint is unknown;
  - skein's client refuses the request, naming the limit it passes
    (llm.md, section 2.5);
  - the call's memory reservation is larger than the whole pool, so no
    wait could admit it;
  - more calls are outstanding, running or waiting, than the owner
    declared conversations.

  A pool at capacity is not among them: the call waits (4.1).
- **One terminal per call,** after which its token is the owner's again.
- **One `closed` per component,** after a close or an abort, and nothing
  after it.

### 4.1 Waiting for a connection

- **A call waits** when it is admitted but cannot run yet: no idle
  connection to its endpoint is free and no new one may open (the pool,
  or the endpoint's share of it, is full), or its memory reservation does
  not fit what the pool has left (section 7).
- **It runs** as soon as both hold: a connection to its endpoint is free,
  reused or newly opened, an idle connection to another endpoint closing
  to make room if it must; and its reservation fits.
- **Arrival order.** Waiting calls run in the order they started. A call
  waiting for its own endpoint's connection does not hold back a call to
  another endpoint, but no call passes an older one in the memory pool,
  so none waits for ever while calls keep ending.
- **The wait counts against the call's whole deadline,** which runs from
  its start; no other deadline runs while it waits (section 6). A call
  whose whole deadline passes while it waits fails as timed out, with
  evidence unsent.
- **A cancel** of a waiting call answers `cancelled` at once: nothing went
  below.
- **The queue is bounded** by the owner's declared conversations (the
  `calls` limit, section 7): each conversation has at most one call
  outstanding, so the owner never meets the bound unless it breaks its
  own declaration.

Once it runs, a call that waited is a call like any other.

## 5. Connections

- **Opening:** a socket to the endpoint's address, then TLS where the
  endpoint has it, before the call's request goes up. A connection that
  fails to open fails its call as unsent.
- **Reuse:** when a response has drained, skein's client says it is
  reusable, and the connection takes the oldest call waiting for its
  endpoint, or waits for the next call to that endpoint.
- **Idle connections** close after the configured idle time, when the
  pool needs the room for a call to another endpoint, or at once when the
  owner closes the component.
- **Closing** follows io's and TLS's lifecycles: TLS's close, then the
  socket's. A connection is gone only once io reports its socket settled;
  the physical binding stays the component's until then.

### 5.1 The owner close

- **Close drains.** From the close on, every start is refused. Idle
  connections close at once. A busy connection's call runs to its
  terminal; the connection then drains and takes a call waiting for its
  endpoint, if one is, and otherwise closes at once, without draining,
  never turning idle. Calls already waiting are served as connections
  and memory free, as before the close: every admitted call ends with its
  ordinary terminal.
- **`closed`** goes up once, when no call is left and every connection's
  socket has settled at io. Until then the component has work, and the
  owner keeps routing io's answers to it.
- **Abort** is a close that does not wait. Every call still running or
  waiting ends `cancelled`, and every socket, idle, busy or already
  closing, is aborted at io; `closed` follows io's settlement. An owner may
  equally cancel its own calls and then close: the pool never decides to
  cancel the owner's calls on a close.
- **Repeats.** A second close is inert. An abort after a close turns what
  is still closing gracefully into aborts, as a termination signal does
  for the process (programming-model.md, section 5.2). A close after an
  abort is inert.
- **Bounded work.** Each entrance starts at most one connection's close,
  within its most outputs; the rest follow at later entrances while the
  component has work.
- **Settlement is io's.** The component arms no deadline for a closing
  connection (section 6): io's close deadline bounds a peer that never
  answers the close (io.md, section 3.3).

## 6. Deadlines

| Deadline | Runs from | Ends | Its failure names |
|---|---|---|---|
| connect | the connection's start | the socket connected | `Connect` |
| handshake | the socket connected | TLS ready | `Handshake` |
| head | the request's first byte, again at each piece of upload room granted | the response's head | `Head` |
| idle | the response's head, again at each event, the provider's pings included | the call's terminal | `Idle` |
| whole | the call's start, its wait included | its terminal | `Whole` |

Each is the owner's, per call, and none is required. One that passes ends
the call through skein's client's abort as timed out, naming the
deadline's phase (llm.md, section 2.5), and the failure carries the
client's evidence: unsent, possibly sent, or a response received.

- **Armed by phase.** Which deadlines run is an exhaustive function of the
  phase the call or its connection is in, applied in one place after
  every transition (programming-model.md, section 5.4):

  | Phase | What runs |
  |---|---|
  | waiting, the call holding no connection | whole |
  | connecting | connect, whole |
  | handshaking | handshake, whole |
  | calling, before the response's head | head, whole |
  | calling, streaming | idle, whole |
  | draining: the terminal given, the body read to its end | idle |
  | idle | the idle keep |
  | closing | nothing: io's close deadline bounds it |
  | closed | nothing |

  An idleness that passes while a connection drains closes it, with no
  terminal: its call already had one.
- **Progress, not duration.** Head and idle measure the peer's progress
  (programming-model.md, section 7): a peer reading a large request
  re-arms head with each piece of room it grants, and a stream that keeps
  sending events never meets idle. Only whole bounds a call's length, and
  it is the owner's time, not the provider's.
- **Retry by phase is the owner's.** Head and idle say the peer stalled;
  whole says the owner's own time ran out (llm.md, section 2.5).

## 7. Limits and the worst case

| Limit | What it bounds |
|---|---|
| endpoints | the configured endpoints |
| connections | the pool, across endpoints |
| per endpoint | the connections one endpoint may hold |
| calls | the calls outstanding at once, running or waiting: the owner's declared conversations |
| memory | the bytes calls may reserve at once: the owner's LLM memory pool |
| idle keep | how long an idle connection stays in a live service |

- **Derived, not configured.** `calls` is the owner's declared
  conversations (llm.md, section 4.1). `connections` is at least `calls`,
  as a connection carries one call at a time; `per endpoint` may be lower,
  and calls to that endpoint then wait. These relationships are checked
  when the component is made, and a violation refuses the configuration
  and names the relationship (llm.md, section 4.3).
- **Pieces follow the transport.** The client's upload piece (HTTP's
  `send`) is TLS's plaintext record, and its reads (HTTP's `read`, the SSE
  reader's chunk) are what TLS delivers at once (tls.md, section 3.5); on
  a plaintext endpoint, io's intake and output caps. A piece smaller than
  a record costs a record and a pass of the loop each: skein-tls seals
  each piece as it comes and never holds one back to fill a record
  (tls.md, section 6). Tests lower them to cut streams small
  (testing-strategy.md, section 3); configurations do not.
- **Each call reserves** at admission its measured request and the
  receiving bound its endpoint's limits derive (llm.md, section 4.2), and
  releases them at its terminal. A call whose reservation does not fit
  what is left waits (4.1); one larger than the whole pool is refused.
  The prompt it was started with is the owner's until the client prepares
  the request from it.
- **The worst case** is:
  - the memory pool, which holds every call's request and receiving
    state;
  - for each connection of the pool, what it holds whatever its call:
    io's buffers for its socket, TLS's state and skein's client's fixed
    state;
  - the waiting calls' records, `calls` of them;
  - and the component's own state.

  The pool at least one largest call's reservation is checked when the
  component is made, by name.

## 8. Testing

- **A protocol world** (testing-strategy.md, 2.5): the component, the
  owner's routing, and skein's fake LLM peer (fake-llm.md) over in-memory
  streams cut at random, with these faults and stories:
  - connections refused;
  - handshakes failing;
  - a slow head, and a slow upload that keeps its head deadline alive by
    the room it grants;
  - an idle stall, and a steady stream that outlasts any fixed bound and
    completes;
  - each deadline passing, its failure naming its phase;
  - a truncated body;
  - cancels racing terminals;
  - reuse after drain, and a drain that stalls until idleness closes it;
  - a pool at its limit: more calls to one endpoint than it may hold
    connections, each completing after its wait; a wait that outlasts its
    whole deadline, failing unsent; a cancel while waiting;
  - the memory pool full, a call waiting for another's terminal;
  - the owner's close in every phase (waiting, connecting, handshaking,
    calling before the head, streaming, draining, idle, closing), crossed
    with a call's terminal racing it: one terminal per call, `closed` once
    and last, and no spin, so that when only io's settlement is left the
    component has no work and no deadline past due;
  - an abort after a close, and a close after an abort.
- **A simulated world** (testing-strategy.md, 2.7), with io over the
  simulator and the fake peer as a simulated server. The peer stays live
  until the component settles; one peer ignores the half-close, and io's
  close deadline settles its socket.
- **The teardown invariant** (testing-strategy.md, section 6): a world
  that ends by itself fires no deadline after the owner's close other
  than io's close and retry deadlines. Idle keeps stay at shipped values
  or beyond the world's horizon, and fakes never hang up to help the
  component settle unless the story is a peer's hang-up.
- **Memory:** the pool at its size and the memory pool full, against the
  worst case.
- **Transports:** the worlds that replay run plaintext endpoints. TLS
  endpoints run in the real loop, and in the protocol world's handshake
  cases, which do not replay (tls.md).
- **Budgets:** the focused suite runs one story for each phase, fault and
  wait; the cross product of the close with racing terminals runs in the
  fuzzy suite (testing-strategy.md, section 8).

## 9. Open questions

- **Names while running:** resolving an endpoint's name again when its
  addresses change, once io has a DNS client (io.md, section 4).
- **Proxies,** and HTTP/2, if a provider requires either.
- **Fairness in the memory pool.** Arrival order lets one large call hold
  back smaller ones behind it while it waits. Whether a bounded bypass is
  worth its complexity waits for a measurement under load.
