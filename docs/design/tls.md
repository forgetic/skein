# TLS

Provisional, 2026-10-03. The design of `skein-tls`: a TLS stream machine
over rustls, ciphertext below and plaintext above. It is the one
exception of programming-model.md, section 3: the only step code that
depends on a crate from outside skein and the service, and the only step
code that is not deterministic.

## 1. In one page

- **A stream machine.** Ciphertext comes from the stream below; plaintext
  goes up as a stream to the machine above, which cannot tell it from a
  socket.
- **It wraps rustls's unbuffered connection,** which does no I/O itself
  and builds without std.
- **The client comes first;** a server side comes when a service
  terminates TLS itself.
- **Not deterministic,** so the replaying tiers run in plaintext, and TLS
  is tested on its own.

## 2. In skein

`skein-tls` depends on lib and rustls. A service's protocol layer stacks
it between io's socket and the machines above it (http.md, 2). It has its
own `Limits` and `worst_case`, and each entry point declares its
`MAX_OUT`.

## 3. The machine

- **Its plaintext carry-over** sits in its own `lib::Intake`, under a cap:
  TLS is the side below for the machine above, and meets its demands
  exactly (programming-model.md, 4.3). A record of up to 16 KB decrypted
  for a demand of a few bytes leaves the rest there.
- **Configuration is data.** The shell reads certificates, keys and root
  stores at startup and hands them in as configuration (shell.md, 6).
- **Wall time** comes from `Env` (`env.wall`), for a certificate's
  validity; it never arms a deadline. Handshake deadlines are armed by
  the connection that stacks the machine, on `env.now`, like every
  machine's.
- **A record that fails to decrypt** fails the stream above as invalid
  (`Fault::Invalid`): the peer is broken or hostile.

## 4. The exception

- It is the only step crate that depends on code from outside skein and
  the service: rustls, and the crypto provider beneath it.
- It is the only one that is not deterministic: its cryptography draws
  entropy from the kernel.

So the simulator and the protocol worlds run in plaintext, and nothing
else in step code may follow it.

## 5. Testing

TLS is tested on its own (testing-strategy.md, 4.4): in-memory handshakes
against itself under every split of the ciphertext, against transcripts
where they can be replayed, and in the real loop. A deterministic TLS for
the replaying tiers is an open question of the foundation
(notes.md).

## 6. Not built yet

All of it. temper pulls the client first, for the agent's LLM client.
Kernel TLS is deferred: after the handshake, the record layer could move
into the kernel, and the plaintext stream become the socket's own
(shell.md, 8).
