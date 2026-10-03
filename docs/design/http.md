# HTTP

Provisional, 2026-10-03. The design of `skein-http`: the protocol
machines for HTTP/1.1, client and server, and for server-sent events,
reader and writer. They are step machines of a connection's stack
(programming-model.md, 4): each depends on lib only, and meets the
stream below it and the user above it through entry points of the
model's shape.

## 1. In one page

- **HTTP/1.1 only.** Heads are scanned under a maximum size; bodies are
  framed by length, by chunks, or by the end of the stream; the body goes
  up as a stream that the side above demands.
- **One exchange at a time.** A client connection carries one exchange at
  a time and is reused when both sides allow it; a server connection
  parses the next request only once the response is queued and there is
  room for the one after it.
- **Server-sent events** are a machine over a body stream: lines, fields,
  and an event at each blank line, each under a maximum.
- **Both sides of each machine** are skein's (client and server, reader
  and writer), and each is tested against the other and against
  transcripts of real peers.

## 2. In skein

`skein-http` depends on lib only. A service's protocol layer stacks it:
over TLS's plaintext, a socket or a pipe, whichever the connection has,
and under the service's own decoders. An LLM client's stack, from the
socket up:

```
domain
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

Each machine has its own `Limits` and `worst_case`, and each entry point
declares its `MAX_OUT`:

```rust
// a machine's entry points, a sketch (the HTTP client)
pub fn up(conn: &mut Client, env: &Env<Limits>, ev: stream::Up,
          above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
pub fn down(conn: &mut Client, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
```

## 3. HTTP/1.1

- **Heads are scanned** up to the blank line, under a maximum head size.
  Each is parsed into a bounded head: the request line or status line,
  and the headers as a bounded list of names and values. Header names are
  compared without regard to case.
- **Bodies are framed** in one of three ways:
  - by length: a fill;
  - chunked: a scanned size line, then a fill;
  - for a response, by the end of the stream.

  The body goes up as a stream. The side above demands it, so a slow
  reader stops the peer.
- **A client connection** carries one exchange at a time, and is reused
  when both sides allow it.
  - A response may arrive before the request is fully sent: a refusal in
    the middle of an upload, for example. The upload then stops, and the
    exchange ends with that response.
  - Interim responses (1xx) are skipped.
- **A server connection** takes one request at a time. It parses the next
  request only once the response is queued and there is room for the
  response after it (programming-model.md, section 7).
- **Not planned:** HTTP/2 and upgrades. Revisit them when a peer requires
  them.

## 4. Server-sent events

A machine over a body stream:

- lines, under a maximum length;
- fields;
- an event dispatched at each blank line, under a maximum event size.

Its writer side frames events for a server.

## 5. Testing

- **Machine worlds** (testing-strategy.md, 2.4): each machine from a seed,
  with a stream below that cuts the peer's bytes at random, grants room
  late, ends early or fails, and a user above that demands slowly, stops,
  and closes in every state. The peer's bytes are the other side of the
  same machine, transcripts of real clients and servers (curl, a forge's
  API, an LLM provider's stream), each with what it must decode to, and
  generated messages, valid and mutated. A peer that misbehaves the way
  real ones do (an oversized head, a chunk size that overflows, an event
  that never ends) is a transcript too.
- **Fuzzing:** one target per machine, fed `Bytes` under every demand.
- **Protocol worlds** (testing-strategy.md, 2.5): an LLM client's HTTP,
  server-sent events and JSON against a server's, joined by bytes; a slow
  reader at the top of one end stops the writer at the top of the other,
  and a response arrives mid-upload.

## 6. Open questions

- **Connection reuse in the client.** Sequential reuse of one connection
  is in. Still open: a pool of connections per peer, and who sets its
  size.

## 7. Not built yet

All of it. temper pulls the client and the server-sent events reader
first, for the agent's LLM client; then the server and the writer, for
the fake LLM provider; then both, for the engine's forge client and its
webhooks.
