# LLM connections

Provisional, 2026-10-07. `skein-llm-connection` runs LLM calls end to end
for the protocol layer that owns it. For each call it:

- connects through io;
- runs TLS, or plaintext to an endpoint on loopback;
- drives skein's LLM client (llm.md);
- arms the deadlines the client leaves to its owner;
- reuses connections that are idle.

Its owner gives it calls and gets their answers, so a service that calls
LLMs translates its own vocabulary and writes no connection code. smith's
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
  - closed after an idle while.
- **Deadlines are given per call and armed here:** connecting, TLS's
  handshake, the response's head, idleness between events, and the call as
  a whole. A deadline that passes fails the call, with the client's
  evidence of whether the request was sent.
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
    the point where it fires its own.

## 3. Endpoints

An endpoint is configuration, given when the component is made. It has:

- **an address,** resolved before the loop starts (io.md, section 4);
- **its transport:** TLS, with the server name and what to trust it by;
  or plaintext, refused at configuration unless the address is on
  loopback. Plaintext serves a server on the same machine, such as a local
  model's, and the replaying tiers' fakes (testing-strategy.md, 4.4);
- **skein's LLM endpoint:** the dialect, the path, and the headers the
  dialect needs, such as an identity profile (llm.md).

The owner names an endpoint by its index in that configuration.

## 4. Calls

| From the owner | What it does |
|---|---|
| start | a call: its token, the endpoint, the prompt, the credential and its deadlines |
| next | ask for one more delta, block or the terminal, as skein's client does |
| cancel | end the call; its terminal follows |

| To the owner | What it says |
|---|---|
| delta | a piece of the answer as it comes |
| block | a completed block |
| completed | the call's completion: its blocks, stop reason and usage |
| failed | the class, the evidence, and the provider's details within bounds |
| cancelled | the call ended at the owner's request |

- **Admission is at the entrance.** A call is refused at once in these
  cases:
  - the owner has closed the pool;
  - the pool is full and no connection may be opened;
  - the endpoint is unknown;
  - skein's client refuses the request's size.
- **One terminal per call,** after which its token is the owner's again.

## 5. Connections

- **Opening:** a socket to the endpoint's address, then TLS where the
  endpoint has it, before the call's request goes up. A connection that
  fails to open fails its call as unsent.
- **Reuse:** when a response has drained, skein's client says it is
  reusable, and the connection waits for the next call to that endpoint.
- **Idle connections** close after the configured idle time, or when the
  pool needs the room for another endpoint.
- **Closing** follows io's and TLS's lifecycles. A connection is gone only
  once its socket has settled.
- **Owner shutdown:** `Component::close` permanently refuses new calls and
  schedules the whole pool to close, including reusable idle connections.
  Each `fire` starts at most one binding's close. Active calls end once as
  cancelled; completed calls get no second terminal. Repeated closes are
  inert, and the owner continues routing io answers through settlement.

## 6. Deadlines

| Deadline | Runs from | Ends |
|---|---|---|
| connect | the connection's start | the socket connected |
| handshake | the socket connected | TLS ready |
| head | the request's first byte | the response's head |
| idle | each event | the next one, the provider's pings included |
| whole | the call's start | its terminal |

Each is the owner's, per call, and none is required. One that passes ends
the call through skein's client's abort, and the failure carries the
client's evidence: unsent, possibly sent, or a response received.

## 7. Limits and the worst case

| Limit | What it bounds |
|---|---|
| endpoints | the configured endpoints |
| connections | the pool, across endpoints |
| per endpoint | the connections one endpoint may hold |
| idle keep | how long an idle connection stays |

The worst case is the pool at its size, each connection holding:

- io's buffers for its socket;
- TLS's state;
- skein's client's worst case;

and the component's own state.

## 8. Testing

- **A protocol world** (testing-strategy.md, 2.5): the component, the
  owner's routing, and skein's fake LLM peer (fake-llm.md) over in-memory
  streams cut at random, with these faults:
  - connections refused;
  - handshakes failing;
  - a slow head;
  - an idle stall;
  - a truncated body;
  - cancels racing terminals;
  - reuse after drain;
  - owner shutdown before drain and while idle, without another terminal;
  - a pool at its limit.
- **A simulated world** (testing-strategy.md, 2.7), with io over the
  simulator and the fake peer as a simulated server.
- **Memory:** the pool at its size, against the worst case.
- **Transports:** the worlds that replay run plaintext endpoints. TLS
  endpoints run in the real loop, and in the protocol world's handshake
  cases, which do not replay (tls.md).

## 9. Open questions

- **Names while running:** resolving an endpoint's name again when its
  addresses change, once io has a DNS client (io.md, section 4).
- **Proxies,** and HTTP/2, if a provider requires either.
