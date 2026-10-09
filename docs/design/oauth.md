# OAuth client

Provisional, 2026-10-09. `skein-oauth` is a bounded, sans-IO OAuth
client for one exchange at a time. It prepares authorization-code sign-in and
refresh requests, accepts the resulting HTTP answers, and hands token records
to its caller. It knows no account, provider, endpoint discovery, token store,
browser, or service policy. The caller supplies endpoints, client
registration, clocks, limits, and fresh random bytes. It owns the redirect
listener, HTTP/TLS connection, durability, and the decision to grant a token
after keeping it. skein's driver, `skein-oauth-accounts` (section 6), is that
caller for any service that wants one: it owns those for its owner, so a
service writes no listener, connection or token store of its own.

The client covers refresh; public sign-in with PKCE and a loopback redirect;
and confidential sign-in with a client secret and the web's redirect. Its
records may also be access-only: a token obtained elsewhere, never
refreshed. Device authorization is outside this component. The same HTTP
records travel through `skein-http` as in `skein-llm`.

## 1. Boundary and entities

The caller supplies a `Registration` with an authorization URL, token
endpoint, client id, redirect URI, requested scope, token wire format, and
optional client secret. The authorization URL and token endpoint are `https`,
or `http` on loopback for an issuer on the same machine, such as a replaying
tier's fake (testing-strategy.md, 4.4). It admits one `Client` machine for an
exchange. The endpoint binding is kept through the exchange; a redirect cannot
choose a new token endpoint. A public client requires an already bound
loopback listener and a redirect URI whose loopback address, port, and path
match that listener. The confidential client has a secret supplied beside its
registration. The caller routes the browser's redirect to the same in-progress
client and checks the request's actual URI against the bound redirect URI
before giving its query parameters to the machine. A caller using multiple
issuers keeps each sign-in bound to its issuer and redirect; the machine never
infers an issuer from the redirect or token response.

A public client's redirect URI is `http://127.0.0.1:PORT/path`,
`http://[::1]:PORT/path`, or `http://localhost:PORT/path`, the last for an
issuer that registered its public client with that name, as some do. The port
is explicit and not zero, and the path is present. A `localhost` redirect is
bound to 127.0.0.1 and never resolved: the name is a spelling the issuer
requires, not an address to look up. It is compared exactly, byte for byte
with the registered URI, in the registration and in the browser's redirect:
the redirect's authority must be `localhost:PORT` as registered, so
`127.0.0.1:PORT`, `localhost.`, `localhost.example`, another case, another
port or another path is refused before the machine sees the query.

`Client` owns only a sign-in or refresh in progress: its registration, private
state value and optional PKCE verifier, one authorization code or prior token
record, deadlines, retry count, and at most one pending HTTP exchange. It
does not own an account table. The caller may create several clients, under
its own admission cap. One accepted exchange emits one terminal `Tokens` or
`Failed`; `Visit` and `Http` are demands, not terminals.

Token and authorization-code bytes are opaque secrets. The saved record is a
versioned encoding of access token, refresh token if it has one, expiry,
caller's opaque record name and generation, and optional bounded claim
metadata. It never contains a client secret, state, code, or PKCE verifier.
The caller keeps a candidate record durably before lending its access token.
On refresh, an omitted replacement refresh token preserves the previous one,
and generation increases once. A crash before keeping a rotated record may
leave the old refresh token unusable; the caller reports that according to its
own policy.

An **access-only record** has no refresh token. It holds a token obtained
elsewhere and handed in by the caller, such as another program's sign-in that
the caller may read but must not rotate: a shared refresh token, once
rotated, would sign the other program out. An access-only record has no
refresh state, so no refresh can be asked of it; it is lent until it expires,
and only its source renews it (section 6.3).

## 2. Sign-in and refresh

The public flow uses a fresh transaction-specific verifier of 43–128 allowed
ASCII bytes, and `S256`: base64url without padding of SHA-256 over the
verifier. The verifier is supplied by the caller's seeded random source;
the machine never draws entropy. It also takes an independent fresh state
value. The authorization URL carries `response_type=code`, client id,
registered redirect URI, state, requested scope, challenge, and
`code_challenge_method=S256`. `Visit` gives the URL to the caller to show or
open. The caller's loopback listener remains bound while awaiting redirect.

The confidential flow carries the same state and redirect binding. It uses
its client secret in the token exchange. PKCE with `S256` is enabled when the
registration says the issuer supports it; this is recommended even for
confidential clients. A registration cannot ask for an unsupported challenge
method. The secret never enters the authorization URL. The caller presents
the `Visit` URL through its web sign-in flow.

On redirect, the client compares the returned state in constant time with
the in-progress state, refuses duplicates or missing parameters, and consumes
the code only once. OAuth `error` on a matching redirect is a typed failure.
A wrong state or redirect URI is refused without an HTTP exchange. The
machine sends a bounded POST to the configured token endpoint with
`grant_type=authorization_code`, code, client id, redirect URI, plus verifier
for PKCE and secret for a confidential client. Refresh sends
`grant_type=refresh_token`, client id, prior refresh token, and client secret
when needed. The caller chooses JSON or form encoding for this endpoint;
both encoders measure before allocating. The response is a bounded JSON token
or error document. Unknown extension fields are ignored only within the
document, nesting, token-count, and string bounds. Recognized fields may not
occur twice. The bearer token type is checked before a record is returned.

Refresh receives a complete prior record or refresh state, which only a record
with a refresh token has. It does not start two exchanges for the same record;
the caller serializes each account's refreshes. A response yields a candidate
with an absolute expiry calculated from the injected wall clock, shortened by
any read-only JWT `exp` claim the caller requested. `remaining` is
recalculated when durability completes or when a grant is encoded. JWT claims
are metadata from a token obtained at the configured endpoint, never proof of
authentication; this crate does not verify signatures or use claims to choose
an issuer or account.

## 3. Machine and deadlines

`step` consumes one event and writes bounded requests. `resume` emits held
work when output room exists. The owner reserves the published maximum output
before either call. Every deadline is compared with an injected monotonic
`Time`; wall time is used only to calculate saved expiry. The caller bounds
the absolute sign-in time, each HTTP attempt, retry count and backoff ceiling.
No delay extends the absolute deadline. A retry is armed only for a transient
failure and only when the entire attempt budget still fits; its backoff grows
to the configured ceiling. An HTTP outcome includes whether a request was
proved unsent, may have been sent, or received a response. Refresh after an
ambiguous send is allowed only as a caller policy decision because a rotating
refresh token may already be spent.

| State | Holds and waits for | Event → state and output |
|---|---|---|
| Idle | limits and registration | `SignIn` → AwaitRedirect, emit `Visit`; `Refresh` → AwaitHttp, emit `Http` |
| AwaitRedirect | state, redirect, optional verifier, sign-in deadline | matching `Redirected` → AwaitHttp, emit code-exchange `Http`; OAuth error → Done, `Failed`; wrong state, replay, cancellation, or deadline → Done, `Failed` |
| AwaitHttp | exchange and HTTP deadline | successful `Http` → Done, emit `Tokens`; invalid grant or malformed answer → Done, `Failed`; transient answer or transport failure → Backoff or Done; deadline → Backoff or Done |
| Backoff | exchange, retry time, absolute deadline | `Tick` at retry time → AwaitHttp, emit `Http`; absolute deadline or retry budget exhausted → Done, `Failed` |
| Done | terminal already emitted | late redirect, HTTP answer, or tick → ignored; explicit reset → Idle |

`Cancel` in any active state emits one cancellation failure and goes to Done.
An event for a different exchange cannot complete the current one. The HTTP
transport always produces a terminal outcome for each emitted request;
otherwise the injected deadline closes it. The machine does not own socket
settlement. A successful token response consumes the code or refresh attempt;
it is never replayed by the machine. A retry of a sign-in code exchange is
permitted only if the prior request was proved unsent. The caller can start
another sign-in after a failure with a new state and verifier.

## 4. Failure vocabulary and secrets

`SignInAgain` means an authorization code or refresh grant was rejected or
revoked (`invalid_grant`), or the matching redirect returned denial.
`RetryLater` holds a bounded retry delay for rate limits, server failures,
transport failure or deadline exhaustion after the machine's retry budget.
`Malformed` means a bounded peer answer failed syntax, required-field, type,
duplicate-field, or token checks. `InvalidRedirect` and `Limit` identify
local admission failures. Client authentication failure is a separate
`ClientRejected` so a wrong server registration is not presented as a
person's revoked grant. Untrusted descriptions are never placed in a domain
record or log. A status alone does not prove a grant was revoked.

Every type carrying a token, authorization code, verifier, state, or client
secret omits its bytes from `Debug` and logs. HTTP request bodies and headers
that contain them are treated as secrets by the owner. Authorization URLs
contain state and challenge and are handed only to the person's visit flow;
the URL is not a durable record. Error descriptions and JWT claims are
untrusted metadata. Only an authenticated TLS token endpoint, or an
`http` one on loopback (section 1), can supply a usable bearer token.

## 5. Bounds and verification

`Limits` caps authorization URL, endpoint and redirect URI, client id and
secret, scope, state, verifier, code, request body, response document, JSON
depth and tokens, string, access and refresh tokens, error detail, record,
and optional JWT payload bytes. Encoders measure the complete escaped URL or
body before allocating. Readers reject overflow before storing. `worst_case`
counts the in-progress machine, one request and answer, parsing temporaries,
candidate record and prior record, with checked arithmetic. The caller adds
HTTP/TLS and its own account table and queues. The maximum output per step
is one demand plus one terminal in the case where an answer completes an
exchange; exact values are exported with the implementation.

Focused tests cover both sign-in types, state and redirect mismatch, duplicate
and late redirect, PKCE challenge and form/JSON encodings, refresh rotation
with and without a new refresh token, expiry and skew, refusals, malformed
answers, and every limit boundary. Redirect tests admit `localhost` with an
explicit port and path, and refuse `localhost.evil`, `localhost.`,
`localhost:0`, a missing port, and a redirect whose authority is `127.0.0.1`
for a `localhost` registration. Record tests round-trip access-only records. A
separate fake issuer drives sign-in, refresh, revocation, delay, replay, and
transport faults through the real client machine. It also runs as a process on
loopback, as the fake LLM peer does (fake-llm.md, 3). Randomized cases use the
fuzzy profile. Memory high water is checked against `worst_case`.

The protocol choices follow [RFC 7636](https://www.rfc-editor.org/rfc/rfc7636.html),
[RFC 8252](https://www.rfc-editor.org/rfc/rfc8252.html), and
[RFC 9700](https://www.rfc-editor.org/rfc/rfc9700.html). Current
[Gitea OAuth2 documentation](https://docs.gitea.com/development/oauth2-provider/)
describes authorization-code public and confidential registrations, PKCE,
and JSON or form token POSTs. The LLM subscription endpoints are caller
configuration; provider-specific admission remains with those consumers.

## 6. The driver

`skein-oauth-accounts` runs sign-in, refresh and keeping end to end for the
protocol layer that owns it, and lends access tokens. smith's local host
needs it for its accounts, and temper's engine for signing people in through
the forge. Designed now; not built.

### 6.1 In skein

- **A component of a protocol layer,** as `skein-llm-connection` is
  (llm-connection.md, 1): a step machine with its own state, limits, worst
  case and most outputs per entry point, which its owner routes io's events
  to by the tokens it gave (programming-model.md, 4.2). It depends on lib,
  io's vocabulary, `skein-http`, `skein-tls` and the client of sections 1 to
  5. It knows no service's domain, account names or policy.
- **Its owner** reports its earliest deadline and fires its deadlines where
  it fires its own, and seeds its random state, from which each sign-in's
  state and verifier are drawn (programming-model.md, 9).

### 6.2 Accounts

An account is configuration, given when the component is made and named by
its index. It has:

- **its registration** (section 1), and the token endpoint's address,
  resolved before the loop (io.md, 4), with its transport: TLS, with the
  roots it trusts (shell.md, 6.2), or plaintext on loopback;
- **its source:** sign-in, whose records come from the issuer through this
  component; or handed in, whose access-only records the owner supplies;
- **its keeper** (6.4).

### 6.3 Requests and events

| From the owner | What it does |
|---|---|
| sign in | starts an account's sign-in; the owner shows the visit |
| redirected | a redirect the owner's web server received, for a confidential client |
| hand in | an access-only record, for a handed-in account |
| grant | asks for an account's access token, held until released |
| rejected | the provider refused a lent token |
| release | the owner no longer holds the account's grant |
| kept | the answer to a keep, for an account the owner keeps: kept, or not |
| cancel | ends an account's sign-in or refresh; its terminal follows |
| close | the owner's close: refuse new requests, drain the rest (6.6) |
| abort | a close that does not wait: cancel every exchange, abort every socket (6.6) |

| To the owner | What it says |
|---|---|
| visit | the authorization URL to show or open, once per sign-in |
| signed in | a sign-in that completed: its record kept (6.4), its grant ready to ask for; the sign-in's success terminal |
| granted | an account's access token, its generation and how long it is valid |
| keep | a record to keep durably, for an account the owner keeps (6.4) |
| expiring | an access-only token has reached its lead before expiry, once |
| failed | a sign-in, refresh or grant failed: section 4's class, or expired; a sign-in whose record could not be kept fails as not kept, and nothing is lent |
| closed | the component has settled: its one terminal |

- **One exchange per account at a time** (section 2). A grant asked for
  while the account's refresh runs is answered by that refresh.
- **Kept before lent.** A new record is kept (6.4) before its token is
  granted. An owner's `kept` that arrives after an abort, or for a keep
  no longer pending, is stale and dropped. One that cannot be kept is not lent, and the previous generation
  stays usable while it is valid (section 1).
- **Refreshed while held.** While a grant is held, the component refreshes
  the account's record its refresh lead before expiry and tells the owner
  the new generation, `granted` again. A grant asked for when the token
  expires within the lead refreshes first. An account whose grant nobody
  holds is not refreshed. A `rejected` token refreshes once; a second
  rejection of the same generation fails the grant.
- **Access-only records are lent, never refreshed.** A grant of one is
  answered while it is valid. At its lead before expiry the owner hears
  `expiring`, once, so that it can ask a person to renew the token at its
  source; past expiry, or once its token is rejected, a grant fails as
  expired, until the owner hands in a newer record. The component never
  writes to the record's source.

### 6.4 Keeping records

- **The keeper is the owner's choice, per account:**
  - skein's private files (io.md, 5.3): one file per account, replaced
    whole and durable before a grant (io.md, 5.2), and loaded at the start;
  - or the owner's own store, as a service keeps its secrets among its own
    records: the owner hands in the kept record when it makes the
    component, and each new one goes out as `keep`, lent only once the
    owner answers that it is kept.
- **A handed-in record is not kept:** its source is the owner's, which
  hands in a fresh one.
- **Nothing else is kept:** a sign-in's state, code and verifier, and the
  client secret, live only in the exchange.

### 6.5 The redirect

- **A public client's redirect** reaches a loopback listener the component
  binds on the redirect's address and port (127.0.0.1 for `localhost`) only
  while a sign-in waits for it. It reads one request head with skein-http's
  server, within its limits, checks the request's URI exactly against the
  registration (section 1), answers a small fixed page, and closes the
  connection. A request for another path, or past the limits, is answered
  with a refusal and closed, the sign-in still waiting. The listener closes
  with the sign-in's terminal.
- **A confidential client's redirect** reaches the owner's web server. The
  owner hands it in (`redirected`), and the component checks it the same way.
- **The component never opens a browser:** the owner shows `visit`.

### 6.6 Owner close

The component follows programming-model.md, 5.2.

- **Close drains.** From the owner's close on, it refuses every request
  at its entrance, as closed, never as the request's fault. What is idle
  closes at once: an account with no exchange holds nothing, and no
  refresh starts after the close, as a refresh lead is policy for a live
  service. An exchange in flight with the issuer finishes, its record is
  kept, and then its connection closes. A sign-in that waits for a person is the owner's
  request too, and still waits, within its sign-in deadline: the
  component never cancels its owner's work on a close. An owner that
  will not wait for a person cancels the sign-in first (`cancel`), which
  ends it at once, failed as cancelled, and closes its listener.
- **`closed`** goes up once, when no exchange is left and every socket
  the component made has settled at io.
- **Abort** is a close that does not wait: every exchange ends failed as
  cancelled, and every socket, the listeners' among them, is aborted at
  io. A rotated refresh token whose answer an abort cuts off may be lost,
  as after a crash (section 1). An abort after a close turns what still
  closes into aborts, as a termination signal does for the process; a
  close after an abort changes nothing.
- **No keep time.** Each exchange opens its own connection and closes it
  when the exchange ends: exchanges are minutes apart, and a pool would
  only add a keep time to close.

### 6.7 Limits and the worst case

| Limit | What it bounds |
|---|---|
| accounts | the configured accounts |
| exchanges | sign-ins and refreshes at once, across accounts |
| listeners | loopback listeners at once |
| refresh lead | how long before expiry a held grant is refreshed, and an access-only one announced |

The client's own limits are section 5's. The worst case is each account's
current and candidate records, and per exchange the client's worst case,
`skein-http`'s, TLS's and io's buffers for one connection; per listener, one
accepted connection and its request head. The trusted roots are shared
configuration, counted once.

### 6.8 Testing

- **A protocol world** (testing-strategy.md, 2.5): the component, its
  owner's routing, the fake issuer (section 5), and a scripted browser that
  follows each visit to the listener, over streams cut at random. Sign-in of
  both clients, refresh while held, rotation, a rejected token, a keep that
  fails, an access-only record expiring and handed in again, a `localhost`
  redirect, wrong redirects, and the owner's close in every state, a
  sign-in waiting for a person among them, cancelled by the owner or
  still waiting; an abort after a close, and a close after an abort; one
  terminal per request, and `closed` once and last.
- **A simulated world** (testing-strategy.md, 2.7), with io over the
  simulator, the fake issuer as a simulated server, and private files on the
  minimal fake machine (testing.md, 4): a filesystem that fails or stalls a
  keep, and a private directory or file with the wrong mode. Every world
  ends under the teardown invariant (testing-strategy.md, 6).
- **Memory:** every limit at once, against the worst case.
- **Transports:** the replaying tiers run plaintext issuers on loopback; TLS
  to an issuer runs in the real loop (tls.md).

## 7. Open questions

- **A `localhost` redirect and IPv6.** The listener binds 127.0.0.1 only. A
  browser that resolves `localhost` to `::1` and does not fall back would
  find nothing there; whether to bind `[::1]` beside it, for the same
  redirect.
- **Device authorization,** for a person whose browser is on another
  machine, and a pasted redirect: whether either joins the client, and in
  which order.
- **A credential helper,** a program that prints a token, as another source
  of access-only records beside handing them in.
