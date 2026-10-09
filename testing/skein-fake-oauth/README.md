# skein-fake-oauth

An independent bounded OAuth issuer for worlds over `skein-oauth::Client`.
It parses the emitted authorization URL and token POSTs, keeps one-use codes
and rotating refresh tokens, and returns bounded token or error documents.
The issuer consumes a refresh token and rotates it when it handles an
accepted request, before its delayed response is delivered. Dropping that
response does not undo the rotation. The fake has no LLM or application
policy.

It works on records: over a network, the caller's `skein-http` stack frames
them, as `skein-fake-peers`' `oauth::Peer` does.
