# skein-oauth

`skein-oauth` contains bounded OAuth token documents, a versioned saved-token
record and an optional JWT metadata reader. It has no IO, account policy or
provider binding. The caller supplies limits, clocks, token endpoints and
any nested JWT claim path it wants to retain as metadata. Reading claims
does not verify a JWT signature and must not be used to authenticate a
person or token.

`SavedToken` carries an opaque caller key, a generation, access and refresh
tokens, optional metadata and an absolute expiry. `rotate` keeps the prior
refresh token when the response omits one. The caller makes a candidate
durable before lending its access token. `remaining` recalculates validity
when the caller grants it. The record uses big-endian fields with a `SKOT`
magic and version 1; readers check every length and reject trailing bytes.

`Client` runs one sign-in or refresh at a time. It emits a visit URL or token
POST and takes a terminal HTTP response with send evidence. It retries only
when the request was proved unsent or the issuer returned a rate limit; an
ambiguous refresh attempt is handed back to the caller. The caller owns the
redirect listener, transport, clocks, token persistence, and use of tokens.
The public flow requires PKCE S256 and a loopback redirect; the confidential
flow supplies its secret only at the token endpoint.

The codecs and client follow [oauth.md](../../docs/design/oauth.md). The fake
issuer is added in the next increment.
