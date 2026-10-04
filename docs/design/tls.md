# TLS

Provisional, 2026-10-04. The design of `skein-tls`: a TLS stream machine
over rustls, ciphertext below and plaintext above. It is the one
exception of programming-model.md, section 3: the only step code that
depends on a crate from outside skein and the service, and the only step
code that is not deterministic. The client is built; the server side is
not (section 8).

## 1. In one page

- **A stream machine.** Ciphertext comes from the stream below; plaintext
  goes up as a stream to the machine above, which cannot tell it from a
  socket. TLS meets lib.md, 7's contract twice: as the side above of the
  ciphertext, and as the side below of the plaintext.
- **It wraps rustls's unbuffered connection,** which does no I/O, keeps no
  clock, and builds without std, with ring for its cryptography.
- **Records by fills.** It reads a record at a time, its header then its
  body, and hands each to rustls, which deciphers it in place.
- **The client comes first;** a server side comes when a service
  terminates TLS itself.
- **Not deterministic,** so the replaying tiers run in plaintext, and TLS
  is tested on its own, against rustls's own server in memory.

## 2. In skein

`skein-tls` depends on lib and rustls, pinned, without its default
features, with ring (programming-model.md, 2.1). A service's protocol
layer stacks it between io's socket and the machines above it (http.md,
2). It has its own `Limits` and `worst_case`, each entry point declares
its `MAX_OUT`, and it says what it waits for:

```rust
// skein_tls::client, the machine
pub fn up(client: &mut Client, env: &Env<Limits>, ev: stream::Up,
          above: &mut Queue<Event>, below: &mut Queue<stream::Down>)
pub fn down(client: &mut Client, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<stream::Down>)

// made at startup, from configuration (shell.md, 6)
Config::new(roots: RootCertStore, alpn: &[&[u8]]) -> Result<Config, Refusal>
Name::new(text: &str) -> Option<Name>
Client::new(&config, name, &limits) -> Client
```

Whoever stacks the client checks at startup that its largest demands fit
the stream below, `client::LARGEST_READ` and `client::largest_room`
against the socket's intake and output caps, and that the machine above
fits it: `Limits::read` and `Limits::send` hold the HTTP client's
`largest_read` and `largest_room`.

## 3. The machine

### 3.1 The side above

```rust
pub enum Request {
    Handshake,                  // first and once: Ready or Failed answers it
    Stream(stream::Down),       // the plaintext stream
    Close,                      // Closed answers it
}

pub enum Event {
    Ready(Agreed),              // for Handshake: the version, and the protocol ALPN agreed
    Stream(stream::Up),         // the plaintext stream: Bytes, Room, End, Failed
    Failed(Error),              // the connection failed, after the stream heard it
    Closed,                     // for Close: terminal
}
```

- **`Handshake` starts the connection,** in its step: rustls's connection
  is made then, its `ClientHello` written, with the step's `env.wall` as
  the time it checks certificates against. The plaintext stream may be
  demanded before `Ready`: its demands wait for the handshake. A
  connection that holds a call while it handshakes (temper's llm.md, 8)
  makes the call on `Ready`.
- **The plaintext stream** keeps lib.md, 7: each demand answered at most
  once, exactly, from the client's intake; `End` once the server's
  `close_notify` was read and nothing held meets a demand; `Room` once
  room for the records of what the side above demanded came below. A
  `Room` grants one `Send`, as io's does (io.md, 3.3); a demand of room
  comes only once the last grant was sent within, and a grant the side
  above sent nothing in gives way to the next.
- **`Finish`** sends `close_notify`, then finishes the stream below. The
  side above may read on.
- **`Close` ends the client in any state.** Once the handshake is done,
  and unless the stream below was finished, it writes `close_notify`
  (unless it was written), sends what TLS owes below within room it
  demands, then answers `Closed`: it withdraws a demand that only reads,
  and keeps one for room, as an answer of room may be on its way and what
  TLS owes goes within it. Otherwise, or once the stream below fails, it
  withdraws what it demanded and answers `Closed` at once. It does not
  close the stream below, which its owner closes.
- **What it waits for**, `waiting()`: `Handshake` (not started),
  `Handshaking` (the server's records, or room for the client's: the
  connection arms its handshake deadline on `env.now`), `Room`, `Bytes`
  (the server's records, for a demand of the side above), `Above`, `Close`
  (it failed) and `Nothing`.

### 3.2 The side below

- **A record at a time:** a fill of its 5-byte header, then one of the
  length it gives, each appended to the records held and handed to
  rustls, which judges a header as it comes (its type, its version, a
  length within what TLS allows) and deciphers and joins records in
  place. `LARGEST_READ` is TLS 1.2's longest body, 2^14 + 2,048 bytes.
- **The plaintext carry-over** sits in the client's own `lib::Intake`,
  under a cap of `Limits::read` and one record's plaintext beside it
  (programming-model.md, 4.3): a demand the intake does not meet always
  leaves room for the next record, which is read only then. rustls holds
  no plaintext between steps.
- **It reads only what is needed:** the server's records while the
  handshake runs, and after it, only for a demand of the side above's the
  intake does not meet. A read stated alone, ahead of a demand, would
  hold a demand that room for the side above's next `Send` could not join
  (http.md, 3.1).
- **Room, in one demand with the read:** for what TLS owes (a flight of
  the handshake, `close_notify`), alone and first; or, once the handshake
  is done, for the side above's demand of `n` bytes of room, `room_for(n)`:
  its records, 29 bytes each at most (TLS 1.2's AES-GCM), and 54 bytes of
  slack for what may go before them: in TLS 1.3, the key update that
  answers the server's and one rustls asks for itself, 27 bytes each; in
  TLS 1.2, the refusal of a renegotiation, 31. What TLS owes that arose
  while the side above held its grant goes in the same `Send`, in front:
  records go in the order rustls sealed them.
- **The ciphertext held** is one box of `Limits::records`, not an intake,
  as rustls reads and writes one slice: a record at least, and the
  records of a handshake message that spans several, the server's
  certificates among them. A message longer than that fails, `TooLong`.

### 3.3 Ends and failures

| What happens | The plaintext stream hears | `Failed` |
|---|---|---|
| the server's `close_notify` | `End`, once what is held meets no demand | |
| the stream below ends without it, after the handshake | what is held, then `Failed(Invalid)` | `Truncated` |
| it ends before or during the handshake | `Failed(Invalid)` | `Truncated` |
| a record fails to decrypt | `Failed(Invalid)` | `Decrypt` |
| the server's certificate is refused | `Failed(Invalid)` | `Certificate(why)` |
| anything else wrong with the server's records | `Failed(Invalid)` | `Protocol`, `TooLong` |
| the server sends a fatal alert | `Failed(Reset)` | `Alert(description)` |
| the stream below fails | `Failed(fault)`, at once | `Stream(fault)` |
| rustls fails for a reason of its own | `Failed(Other)` | `Other` |

- **A truncation is never taken for an end** (RFC 8446, 6.1): it fails the
  stream as invalid, the peer broken or hostile, so that a body read to
  the end of the stream is not cut short unseen. What was deciphered
  before it was authenticated, and still meets demands first.
- **Why a certificate is refused:** `Expired` and `NotYetValid` at the
  wall time the handshake started at, `Name`, `Issuer` (no root trusted),
  or `Invalid`.
- **After a failure,** nothing more is sent, not even rustls's alert; the
  client waits for its close, which withdraws what it demanded.

### 3.4 Configuration

Configuration is data (shell.md, 6). The shell reads the roots a service
trusts, and hands them to `Config::new` with the protocols it offers by
ALPN, in order of preference: a configuration every connection shares,
rustls's, with both versions of TLS, no client certificate, and no
session resumed, as a session cache would be state shared between
connections. It refuses roots of none, and protocols empty, longer than
255 bytes, or of more than `ALPN` (256) bytes in all. Each connection
gets the server's `Name`: a DNS name, sent by SNI, or an IP address.

### 3.5 Limits and the worst case

```rust
pub struct Limits {
    pub read: u32,      // the most the side above demands at once; the intake holds it and a record's plaintext
    pub send: u32,      // the most room it demands at once, in plaintext
    pub records: u32,   // the ciphertext held: a record at least (MAX_RECORD), and a message that spans several
}
```

- **`worst_case(&limits)`** is the client's own: the intake, the records
  held, `FLIGHT` (2,048 bytes) for what TLS owes, and the delivery it
  reads, at most `LARGEST_READ`, which a step that encrypts holds as much
  of rustls's instead; and rustls's, measured (section 5): 16 KB of its
  own state, and twice the server's certificates, which the records held
  bound. `None` for a read or room of nothing, or records shorter than one.
- **`largest_room`** is the larger of `room_for(send)` and `FLIGHT`.
- **`UP_MAX_OUT`** is two events and three requests: `Ready` or an answer,
  or the stream told it failed and `Failed`; below, what TLS owes sent,
  the stream finished, and the next demand. **`DOWN_MAX_OUT`** is two and
  two.

## 4. The exception

- **rustls, and ring beneath it,** are the only step code from outside
  skein and the service. ring builds with `cc`, without cmake and without
  std; aws-lc-rs, rustls's default, would bring a C build skein does not
  need. rustls is pinned (0.23.41), and ring by `Cargo.lock` (0.17.14).
- **Not deterministic:** ring draws its randoms and keys from the kernel,
  through `getrandom`: the only kernel calls step code makes, when the
  handshake starts and as it runs. That is the documented exception;
  nothing else impure comes in.
- **No clock:** rustls asks a time provider for the time it checks
  certificates against. Each connection's configuration carries one that
  answers the `env.wall` of the step that started its handshake, read
  once; the handshake deadline bounds how far it lags the wall.
- **What rustls's API makes the crate use:** an `Arc` of the shared
  configuration, a trait object for the time, a hand-written `Debug`
  for its connection, `Vec`s in the configuration, and arms for the
  states rustls may add to its `non_exhaustive` enums, unreachable for
  0.23's; each in place, with a scoped `expect` where a lint objects.

So the simulator and the protocol worlds run in plaintext, and nothing
else in step code may follow it.

## 5. Testing

TLS is tested on its own (testing-strategy.md, 4.4), against rustls's own
server, in memory: rustls's buffered connection over byte slices. Its
certificates are fixtures (`tests/tls/fixtures`, made by `make.sh` with
OpenSSL): a root, an intermediate, the server's certificate for
`skein.test` and `127.0.0.1`, one of 40 KB for 1,500 names, and one that
a root no one trusts signed, all valid from 2026 to 2036, checked at a
wall time each test chooses.

- **Step tests** (`crates/skein-tls/src/tests/`): the limits and what they
  price, the configuration's refusals, names, the faults, the bytes held
  for rustls; and the machine fed by hand: its first flight, records no
  server writes (not TLS, past TLS's lengths, an alert, a message longer
  than the records held), the stream ending and failing before and
  during the handshake, closes, and each bug of the side above's.
- **Machine worlds** (`tests/tls`, `skein-tls-world`): one client from a
  seed between the server's ciphertext, cut at random, room granted late,
  ended or failed, and a user that reads with demands of every shape,
  slowly, writes within the room granted, finishes, and closes in every
  state. They check both streams' contracts as they go, and each run
  against its scenario: the plaintext each side received, the server's
  ending (`close_notify`, a truncation, a corrupted record, nothing), a
  key update, certificates refused at the wall time handed in, a chain
  longer than the records held, and `close_notify` sent on a finish or a
  close. Focused tests aim at one outcome each: every version, a retry,
  ALPN, an alert, every split of the ciphertext a byte at a time, a slow
  reader that fills the stream below, and closes in every state.
- **Not replayed:** what rustls draws from the kernel changes a record's
  length, and with it where the pieces fall. The worlds assert only what
  does not depend on it.
- **The machines stacked** (`tests/tls/tests/stack.rs`): the HTTP client
  over the TLS client, as a connection routes between them, a call made
  on `Ready`, a body uploaded in records and a response read by length
  and by chunks.
- **Memory** (`tests/tls/tests/memory.rs`, with the counting allocator):
  every call of an entry point a step of the meter. The server runs on
  the same thread, between the client's steps; its heap and the
  harness's are measured with a span around what is done between steps,
  and each step is checked against the bound plus that heap. This is how
  rustls's part was measured: past the client's buffers, 6 KB for a
  handshake, 10 KB with the longest ALPN list, 21 KB in a step that
  deciphers or encrypts a record of 16 KB, and 82 KB for the 39 KB chain,
  held twice. The longest chain reaches three quarters of the worst case.
  Dropped, the client frees what it held.
- **The fuzzy suite** (`tests/tls/tests/fuzzy_world.rs`): 400 runs of
  scenarios and neighbours drawn from each seed, every version, retries,
  ALPN, the big chain, key updates, every ending and every refused
  certificate, asserting that each outcome, each wait a close came in,
  and each oddity of the neighbours fell.

## 6. Decisions

- **The handshake starts with a request,** `Handshake`, not with the
  client: `Client::new` draws nothing from the kernel, and the step that
  starts it reads `env.wall`.
- **Certificates are checked at the wall time of that step,** carried by
  a time provider of each connection's configuration, not one read at the
  moment rustls verifies: that would take a cell the step writes `env.wall`
  into, interior mutability the model rules out, to gain no more than the
  handshake's duration.
- **ring, not aws-lc-rs:** it builds without cmake or std, and is pure
  enough to pin.
- **No read ahead of a demand,** and room for the side above's demand
  joined to the read: otherwise a read stated alone holds the demand that
  room for an upload needs, and the client waits for a server that waits
  for it.
- **What TLS owes goes first and alone,** within room it demands for it,
  one `Send` a grant, as io grants one; what arose while the side above
  held its grant goes in front of the side above's `Send`, within
  `room_for`'s slack.
- **A truncation fails the stream as invalid,** never ends it, once what
  was deciphered is delivered; a peer's fatal alert is a reset.
- **No session resumed,** and no alert sent after a failure.
- **rustls is built in tests as it ships,** without its debug assertions
  (the workspace's `Cargo.toml`): one holds any alert after `close_notify`
  to be a bug, which a half-closed connection reading a corrupted record
  reaches; release builds compile it out.

## 7. Open questions

- **A deterministic TLS** (notes.md): rustls's ring provider draws its
  key shares from ring's own randomness, not from the provider's, so a
  provider that draws from the seed would need key exchanges of its own.
- **Session resumption,** which would save a round trip on each new
  connection to the same server, at the price of a cache shared between
  connections.

## 8. Not built yet

- **The server side,** when a service terminates TLS: temper's engine,
  for its webhooks and its workers on other hosts.
- **Client certificates,** with it.
- **The real loop:** a loopback exchange through the shell, against a
  local rustls server; and the shell reading root stores at startup
  (shell.md, 6).
- **Kernel TLS:** after the handshake, the record layer could move into
  the kernel, and the plaintext stream become the socket's own (shell.md,
  8).
- **The fuzz target** (`fuzz/`, fed records under every demand), which
  waits for a nightly toolchain; the fuzzy suite stands in for it.
