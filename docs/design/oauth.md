# OAuth client

`skein-oauth` is a bounded, sans-IO OAuth client for one exchange at a time.
It prepares authorization-code sign-in and refresh requests, accepts the
resulting HTTP answers, and hands token records to its caller. It knows no
account, provider, endpoint discovery, token store, browser, or service policy.
The caller supplies endpoints, client registration, clocks, limits, and fresh
random bytes. It owns the redirect listener, HTTP/TLS connection, durability,
and the decision to grant a token after keeping it.

This design covers the flows decided in `groundwork-plan/02-oauth.md`:
refresh; public sign-in with PKCE and a loopback redirect; and confidential
sign-in with a client secret and the web's redirect. Device authorization is
outside this component. The same HTTP records travel through `skein-http`
as in `skein-llm`.

## 1. Boundary and entities

The caller admits one `Client` with an authorization URL, token endpoint,
client id, redirect URI, requested scope, and token wire format. The endpoint
binding is kept through the exchange; a redirect cannot choose a new token
endpoint. A public client requires an already bound loopback listener and a
redirect URI whose loopback address, port, and path match that listener. The
confidential client has a secret supplied beside its registration. The caller
routes the browser's redirect to the same in-progress client and checks the
request's actual URI against the bound redirect URI before giving its query
parameters to the machine. A caller using multiple issuers keeps each sign-in
bound to its issuer and redirect; the machine never infers an issuer from the
redirect or token response.

`Client` owns only a sign-in or refresh in progress: its registration, private
state value and optional PKCE verifier, one authorization code or prior token
record, deadlines, retry count, and at most one pending HTTP exchange. It
does not own an account table. The caller may create several clients, under
its own admission cap. One accepted exchange emits one terminal `Tokens` or
`Failed`; `Visit` and `Http` are demands, not terminals.

Token and authorization-code bytes are opaque secrets. The saved record is a
versioned encoding of access token, refresh token, expiry, caller's opaque
record name and generation, and optional bounded claim metadata. It never
contains a client secret, state, code, or PKCE verifier. The caller keeps a
candidate record durably before lending its access token. On refresh, an
omitted replacement refresh token preserves the previous one, and generation
increases once. A crash before keeping a rotated record may leave the old
refresh token unusable; the caller reports that according to its own policy.

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

Refresh receives a complete prior record or refresh state. It does not start
two exchanges for the same record; the caller serializes each account's
refreshes. A response yields a candidate with an absolute expiry calculated
from the injected wall clock, shortened by any read-only JWT `exp` claim the
caller requested. `remaining` is recalculated when durability completes or
when a grant is encoded. JWT claims are metadata from a token obtained at the
configured TLS endpoint, never proof of authentication; this crate does not
verify signatures or use claims to choose an issuer or account.

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
untrusted metadata. Only an authenticated TLS token endpoint can supply a
usable bearer token.

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
answers, and every limit boundary. A separate fake issuer drives sign-in,
refresh, revocation, delay, replay, and transport faults through the real
client machine. Randomized cases use the fuzzy profile. Memory high water is
checked against `worst_case`.

The protocol choices follow [RFC 7636](https://www.rfc-editor.org/rfc/rfc7636.html),
[RFC 8252](https://www.rfc-editor.org/rfc/rfc8252.html), and
[RFC 9700](https://www.rfc-editor.org/rfc/rfc9700.html). Current
[Gitea OAuth2 documentation](https://docs.gitea.com/development/oauth2-provider/)
describes authorization-code public and confidential registrations, PKCE,
and JSON or form token POSTs. The LLM subscription endpoints are caller
configuration; provider-specific admission remains with those consumers.
