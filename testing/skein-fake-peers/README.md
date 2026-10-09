# Scripted peers on loopback

`llm::Peer` and `oauth::Peer` implement `skein_world::Host`. They are
independent processes in the shared simulated or real harness; their only
connection to a client is through io sockets. `address()` exposes the bound
listener address after Listening. `shutdown()` sets an allocation-free flag
that the next iteration acts on: close admission, finish queued output, and
settle every connection. Delayed machine terminals still run after disconnect.

Both use `Transport::Plaintext` or `Transport::Tls`. TLS uses the fixed chain
in Skein's TLS fixtures, for `skein.test` and `127.0.0.1`; the client trusts
the fixture root. TLS writes reserve io output room of their own beside
reads. Plaintext worlds replay their kernel trace and outside observations;
TLS worlds do not replay.

The caller supplies immutable `Limits`: io bounds, simultaneous connections,
internal queue counts, plaintext/ciphertext caps and retained observation
counts/bytes. A queue has at least 64 records. Transport startup checks io's
largest demands; TLS also needs at least one 16 KiB plaintext record.
Exhausting an observation cap fails the world before any payload copy,
rather than losing an outside record. Observations remain borrowed until
the process drops, so a metered process retains all their ownership. A
caller copying records counts those copies separately.

For the LLM peer, construct the script `Domain` from its seed, fault/latency
`Config`, scripts and caller argument menu, and move it into `llm::Peer::new`
with that same `Config`, the byte peer's `Config` and `Limits`, and its
explicit credential. Each accepted connection gets the real shared HTTP/SSE
Server; domain replies route through the real Service. The referee can see
accepted and closed connections, complete neutral queries and domain
terminals, including undeliverable late ones. The script/menu and answer caps
include their owned wrappers.

For the issuer, `oauth::Peer::new` takes its registration `Config` and
issuer Limits plus HTTP server limits. Use configured `http` endpoints with
plaintext, or `https` with TLS. Token and authorization URL paths route to
the issuer. Install seeded delay, rotation, error or malformed-answer
plans using `queue` during metered construction. Approved authorization GETs
return a 302 Location to the exact registered redirect URI, with escaped code
and state. Token POSTs retain the domain's registration, PKCE, spent-code and
refresh-rotation checks. HTTP head limits must fit the maximum redirect.
The world registers endpoints with the address it chooses; the peer's io
address and the URL metadata are separate explicit inputs.

Each module exports `worst_case`, and each Host reports its complete bound
and maximum operations. `tests/llm-connection` demonstrates independently
hosted plaintext and TLS clients, an exact script/answer-cap full delayed
pool, outside replay and per-process memory. `tests/oauth` demonstrates a
three-process plaintext sign-in and refresh, with a scripted browser following
the redirect through the client's listener, maximum token/plan/code/
rotation state, replay and separately metered heaps. Their short-operation
seed sweeps live in the fuzzy profile.

Contracts: `fake-llm.md`, section 3; `oauth.md`, section 5;
`testing-strategy.md`, sections 4, 6 and 7;
`programming-model.md`, section 10.2.

The LLM peer reads a body of announced length in batches up to the HTTP
read cap, and a chunked body a byte at a time. A request reaches the
domain only at HTTP's `End`.
