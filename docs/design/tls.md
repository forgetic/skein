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
- **The plaintext stream** keeps lib.md, 7, and holds the side above to
  it as io does: each demand answered at most once, exactly, from the
  client's intake; `End` once the server's `close_notify` was read and
  nothing held meets a demand; `Room` once room for the records of what
  the side above demanded came below. A `Room` grants one `Send`, as io's
  does (io.md, 3.3), and a demand of room comes only once the last grant
  was sent within, by a `Send`, empty or not, or given up by `Finish`. A
  read that crosses `End` is never met, and its demand stays outstanding,
  as io's does: until `Room` answers it, if it asked room, or until the
  side above withdraws it. A side above that writes on after the end
  waits for that `Room`, or withdraws the demand and then demands room
  alone; room that came below for the demand withdrawn is handed on to the
  next or asked for again (3.2).
- **`Finish`** sends `close_notify`, then finishes the stream below. It
  comes with no demand outstanding, unless a read that crossed `End`: a
  read the client is still reading below for would hold `close_notify`
  behind it, against a server that waits for the end of what it is sent
  before it answers. The side above may read on after it.
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
- **At the stream's end,** a read the client has outstanding crosses it
  and is never met, and io keeps the demand outstanding (lib.md, 7). The
  client reads nothing more: it withdraws a demand that only read at
  once, and one that asked room too stays until `Room` answers it. Either
  way it is then free to demand room after the end, for a peer that only
  half-closed still reads.
- **Room, in one demand with the read:** for what TLS owes (a flight of
  the handshake, `close_notify`), alone and first; or, once the handshake
  is done, for the side above's demand of `n` bytes of room, `room_for(n)`:
  its records, 29 bytes each at most (TLS 1.2's AES-GCM), and 54 bytes of
  slack for what may go before them: in TLS 1.3, the key update rustls
  asks for as its keys reach their limit and a second, as the first's own
  record reaches it, 27 bytes each (the one that answers the server's
  comes alone, as it starts the keys over); in TLS 1.2, the refusal of a
  renegotiation, 31. What TLS owes that arose
  while the side above held its grant goes in the same `Send`, in front:
  records go in the order rustls sealed them. Room that comes for a
  demand the side above withdrew meanwhile is held for no one: handed on
  at its next demand of room if it takes that demand's records, or given
  up, so that room is asked for again, as a grant not sent within gives
  way to the next (io.md, 3.3).
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
| a `HelloRetryRequest`'s cookie too long to echo within `FLIGHT` | `Failed(Invalid)` | `FlightTooLong` |
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
  own state, and the decoded form of the longest handshake message,
  20 bytes for each of its bytes, which the fewer of `Limits::records` and
  rustls's 64 KB (`MAX_HANDSHAKE`, 65,539 with its header) bound: a server
  sends a message whole within the records held. The factor is a hostile
  server's: a message of the shortest entries a list allows, 3 bytes each
  in TLS 1.2 and 5 in TLS 1.3 (empty certificates, names of a byte),
  decodes to an element of 24 or 48 bytes each, in a list that grows by
  doubling and is copied once, and a chain is kept. `None` for a read or
  room of nothing, or records shorter than one.
- **`largest_room`** is the larger of `room_for(send)` and `FLIGHT`.
- **`UP_MAX_OUT`** is two events and three requests: `Ready` or an answer,
  or the stream told it failed and `Failed`; below, what TLS owes sent or
  a read the end crossed withdrawn, the stream finished, and the next
  demand. **`DOWN_MAX_OUT`** is two and two.

### 3.6 Native independent output

The explicit `client::native` face uses lib.md section 7.1 at both boundaries:
read-only classic Demand/Up, plus named OutputDown/OutputUp. It shares the
concrete rustls/session/held engine with the classic client. Its constructor
fixes the face; the classic face's contracts are its own. A classic
positive-room Demand or classic Send on the native face violates its
receiving precondition before effects. No adapter over a
classic combined lower demand can provide this independence.

Each native connection retains separate upper read, upper output, lower read
and lower output cells, with one lower token generator and no history. Lower
Tokens are checked, never reused and permanently exhausted without wrapping.
An upper output right is admitted only positive and within Limits.send, with
idle output and usable writing; it can wait during an admitted Handshake but
cannot be granted before Ready. Grant requires actual lower bytes and one Send
slot. Its lower token remains the affine backing until matching Send/Release,
empty Send, permitted Finish or actual close/failure consumes it. Check identity
before size: stale operations are inert; only matching oversize is a caller bug.

Native ciphertext room is checked `room_for(P) + FLIGHT`, with FLIGHT's actual
2048-byte owed-control cap, and native largest room is the maximum of that
requirement at Limits.send and FLIGHT. The complete owed prefix can grow while
upper grant is held; TLS must preserve its backing slot instead of spending it
on unrelated output. TLS-owned smaller flight grants send a fitting ordered
prefix and retain its suffix for a fresh right.
`client::native::LowerLimits` describes actual configured lower caps:
`read` covers LARGEST_READ, `output` covers checked native largest_room,
and `sends` is at least one queued Send slot beside one in-flight/stalled Send.
`Client::new(config, name, limits, lower)` checks these before buffer allocation
or cloning configuration, returning None if the lower cannot honour them. A
lower configured sufficiently but one byte short of credit at the moment
admits the request and waits. Bytes and queue slots are checked separately;
independence needs no second queued Send slot.

A matching upper cancellation settles Wanted once; lower cancellation retains
its real identity until the actual lower winner arrives. An already emitted
upper Grant remains its winner. A genuine failure settles pending upper output
before classic plaintext Failed and TLS Failed, without answering it twice when
both lower output Failed and classic Failed arrive. Genuine Ready is retained
even when a following checked-token exhaustion fails output in the same step.
Native `UP_MAX_OUT` is four events above and three requests below, and
`DOWN_MAX_OUT` three and three; the classic maxima are section 3.5's.

The native lower bridge carries actual Closed for the exact ciphertext entity.
A recorded TLS self-cancel consumes only its output right. Unsolicited matching
lower Cancelled instead makes transport unusable, stops fresh demands/output,
settles still-Wanted upper output Cancelled once and emits TransportClosing
unless local Close is already pending. The coordinating owner requests native
Close and closes/aborts actual lower IO; TLS owns no timer or physical descriptor.
No lower Failed, clean End, authenticated bytes or parse error is invented.

Actual lower Closed first settles any still-Wanted upper output Cancelled,
including pre-Ready with no lower Room and a new upper request crossing an older
self-cancel. An emitted upper winner is preserved. Real lower obligations must
already have settled; missing/wrong settlement is a trusted-bridge invariant
violation. Without local Close, TransportClosed ends native Handshake/read and
the connection. With local Close pending, Client::Closed answers it once after
all actual lower output rights settle. A native Close queued after
TransportClosing may arrive after TransportClosed: it is inert before classic
core dispatch and creates no second Closed. Classic Close-after-Closed asserts.
The owner retains physical resource responsibility and validates generation.

Native Finish requires Ready and the existing no-live-upper-read/no-pending-
output restrictions. After permitted closing withdrawal it admits no further
positive reads. This read-no-more contract is narrower than the classic
face's, which keeps pre-Ready Finish and read-after-Finish.
Ready withdrawal retains one inline exact Fill-size witness for an actual
Bytes winner already emitted by the lower resource. Its matching owned box is
dropped before copying, processing or encryption, including across permitted
Finish; no read acknowledgement or new terminal is required. Withdrawal can win
without Bytes. No further positive read is admitted, and physical Closed clears
the witness. During Handshake, withdrawal of the upper read leaves genuine
internal ciphertext reads progressing through Ready.

The native memory bound includes actual wrapper/cell layouts and checked work
allowance max(LARGEST_READ,native_room_for(Limits.send)), with drop-before-
process/encrypt sequencing. Complete owed-plus-sealed ciphertext is counted
before handoff and on error-before-emission; overlapping allocations require
the checked sum. Upper input remains caller ownership, not presumed released
at a grant. Existing buffers/rustls scratch, IO queue/flight boxes and typed
queues are priced once. The whole FLIGHT allocation and checked reservation
envelope stay priced even when the logical output is smaller.

The pinned rustls 0.23.41 coalesces several requested TLS 1.3 KeyUpdates
into one deferred notification while the caller holds its grant: the native
tests admit 32 requests and observe one notification beside the maximal
plaintext send. A real TLS 1.2 HelloRequest produces a sealed refusal while
the upper grant is held; it and a later `close_notify` drain in order,
prefix then suffix, across an older, smaller queued grant. Capacity tests
tell allocation bounds apart from the pinned peer's admitted control-record
limits. TLS ciphertext is nondeterministic; simulator scheduling makes no
claim to replay it.

## 4. The exception

- **rustls and its dependencies,** ring's among them, are the only step
  code from outside skein and the service: rustls-webpki,
  rustls-pki-types, ring, untrusted, getrandom, libc, cfg-if, subtle,
  zeroize and once_cell. ring builds with `cc`, without cmake and without
  std; aws-lc-rs, rustls's default, would bring a C build skein does not
  need. rustls is pinned (0.23.41), the rest by `Cargo.lock` (ring
  0.17.14).
- **Not deterministic:** ring draws its randoms and keys from the kernel,
  through `getrandom`: the only kernel calls step code makes, when the
  handshake starts and as it runs. That is the documented exception;
  nothing else impure comes in.
- **`getrandom` on Linux** makes the getrandom(2) system call. Where it
  cannot, a kernel older than 3.17 (below the shell's floor, shell.md, 4)
  or a seccomp filter that refuses it with `EPERM`, it falls back to
  `/dev/urandom`, once a poll of `/dev/random` says the pool is ready: it
  opens the file the first time it draws, under a lock of its own, and
  keeps the descriptor open for the life of the process, a file io knows
  nothing of, read with plain `read` calls. getrandom's
  `linux_disable_fallback` feature would make that an error instead;
  skein does not set it.
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
`skein.test` and `127.0.0.1`, one of 40 KB for 1,500 names, one that a
root no one trusts signed, one signed by itself, one of a CA, and one
for client authentication only, all valid from 2026 to 2036, checked at
a wall time each test chooses.

- **Step tests** (`crates/skein-tls/src/tests/`): the limits and what they
  price, the configuration's refusals, names, the faults, the bytes held
  for rustls; and the machine fed by hand: its first flight, records no
  server writes (not TLS, past TLS's lengths, an alert, a message longer
  than the records held, a retry whose cookie no flight holds), the
  stream ending and failing before and during the handshake, closes, and
  the bugs it asserts: limits it cannot honour, a second handshake, the
  stream before it, a second demand, a read or room past the limits, a
  `Send` without room, a `Finish` with room or a read outstanding, a
  close after close, and an answer no demand asked for.
- **Machine worlds** (`tests/tls`, `skein-tls-world`): one client from a
  seed between the server's ciphertext, cut at random, room granted late,
  ended or failed, and a user that reads with demands of every shape,
  slowly, writes within the room granted, finishes, and closes as the
  client waits for the handshake, room, bytes, the side above, or its
  close after a failure. They check both streams' contracts as they go,
  and each run against its scenario: the plaintext each side received,
  the server's ending (`close_notify`, a truncation, a corrupted record,
  nothing), the server's key update in TLS 1.3, certificates refused at
  the wall time handed in, a chain longer than the records held,
  `close_notify` sent on a finish or a close, the server reading every
  record the client sends, and, left alone, the end the server makes
  reached and the request all sent, so that a client that waits for ever
  fails. Focused tests aim at one
  outcome each: each version, a retry, ALPN, an alert, a certificate
  refused for each reason, every split of the ciphertext a byte at a
  time, a slow reader that fills the stream below, the same closes, a
  read that crosses the end held until withdrawn, room held for a demand
  withdrawn, a corrupted record read after `close_notify` went, the key
  updates rustls starts at its keys' limit, and a renegotiation request,
  sealed with the server's keys taken out, refused alone or in front of
  the side above's data. Nothing reaches `Other`, rustls failing for a
  reason of its own.
- **Not replayed:** what rustls draws from the kernel changes a record's
  length, and with it where the pieces fall. The worlds assert only what
  does not depend on it.
- **The machines stacked** (`tests/tls/tests/stack.rs`): the HTTP client
  over the TLS client, as a connection routes between them, a call made
  on `Ready`, a body uploaded in records, a response read by length and
  by chunks, and one read to the end of the stream: whole once
  `close_notify` ends it, failed `Stream(Invalid)` when the stream is cut
  without it.
- **Memory** (`tests/tls/tests/memory.rs`, with the counting allocator):
  every call of an entry point a step of the meter. The server runs on
  the same thread, between the client's steps; its heap and the
  harness's are measured with a span around what is done between steps,
  and each step is checked against the bound plus that heap. This is how
  rustls's part was measured, past the client's buffers: 6 KB for a
  handshake, 10 KB with the longest ALPN list, 21 KB in a step that
  deciphers or encrypts a record of 16 KB, and 82 KB for the 39 KB chain,
  held twice. A hostile server's messages cost more: sent first, in the
  clear, and decoded whole before rustls judges them, a chain of empty
  certificates and a request of names of a byte reach 18 times their
  length (298 and 303 KB for messages of 16 KB, 1.18 and 1.20 MB for
  64 KB), unknown extensions 3.5 times; a chain padded with empty
  certificates, which rustls accepts and keeps, 20 times in TLS 1.2 (1.30
  MB past the buffers) and 18 in TLS 1.3. That chain reaches 97% of the
  worst case. Dropped, the client frees what it held.
- **The fuzzy suite** (`tests/tls/tests/fuzzy_world.rs`): 400 runs of
  scenarios and neighbours drawn from each seed: each version, retries,
  ALPN, the big chain, the server's key updates, each ending and each
  certificate refused. It asserts that what it draws fell: no failure,
  the stream's own failure, a truncation, a record that fails to
  decrypt, among them one read after `close_notify` went, a message
  longer than the records held, each reason a certificate is refused,
  each version, a protocol agreed, a retry, records read past a key
  update, a close in each wait the worlds reach, and each oddity of the
  neighbours. It draws no alert, no protocol error and no cookie too long
  to echo: the focused tests reach those.

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
- **A read that crosses the end stays outstanding,** both ways, as
  lib.md, 7 says and io does, until `Room` answers it or it is withdrawn:
  the client withdraws its own, as it reads no more after the end, unless
  it asked room, and holds the side above to the same, so that a side
  above that works over TLS works over a socket.
- **`Finish` comes with no read outstanding,** but one that crossed the
  end, rather than `close_notify` waiting behind a read below.
- **The length of what rustls encrypts is asked until two answers
  agree:** a query that finds the keys at their limit schedules a key
  update that the next call writes before the data, and that update's own
  record, sealed at the limit, schedules a second. A length that never
  settles, or a write that does not fit it, fails the connection, Other.
- **A truncation fails the stream as invalid,** never ends it, once what
  was deciphered is delivered; a peer's fatal alert is a reset.
- **No session resumed,** and no alert sent after a failure.
- **rustls is built as it ships in every dev build,** not only the
  tests', without its debug assertions (`[profile.dev.package.rustls]` in
  the workspace's `Cargo.toml`): one holds any alert after `close_notify`
  to be a bug, and aborts a half-closed connection that reads a corrupted
  record (section 8 drafts the issue); release builds compile it out.
  Cargo honours profiles only in the root manifest, so a workspace that
  depends on skein-tls copies the override into its own: until it does,
  its debug builds abort on a peer's error after the client's
  `close_notify`.

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
- **An issue for rustls,** drafted against 0.23.41 and not filed, so that
  the override of section 6 can go:

  > **`send_fatal_alert` panics in debug builds after `send_close_notify`**
  >
  > `CommonState::send_close_notify` sets `sent_fatal_alert`, so that no
  > alert follows `close_notify`; `send_fatal_alert` begins with
  > `debug_assert!(!self.sent_fatal_alert)` (common_state.rs:567). A
  > client that sends `close_notify` and reads on, as a half-closed
  > connection lets it (RFC 8446, 6.1: each side closes its own
  > direction), reaches it on the first fatal error in what it reads: a
  > record that fails to decrypt (conn.rs:1133, `BadRecordMac`), a message
  > that does not decode, one that comes out of turn. A debug build
  > panics; a release build queues a fatal alert behind the
  > `close_notify`.
  >
  > To reproduce with `UnbufferedClientConnection`: complete a handshake,
  > `queue_close_notify` and send it, then hand `process_tls_records` the
  > server's next application-data record with a byte of it changed.
  > Expected: `Error::DecryptError`. With debug assertions: a panic at
  > common_state.rs:567.
  >
  > Suggested fix: once `close_notify` was sent, `send_fatal_alert` sends
  > no alert and returns the error, as `send_close_notify` sends nothing
  > after a fatal alert; the assertion then guards only against a second
  > fatal alert.
