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

The codecs follow [oauth.md](../../docs/design/oauth.md). The client machine
and its fake issuer are added in the next increments.
