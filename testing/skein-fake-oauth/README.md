# skein-fake-oauth

An independent bounded OAuth issuer for worlds over `skein-oauth::Client`.
It parses the emitted authorization URL and token POSTs, keeps one-use codes
and rotating refresh tokens, and returns bounded token or error documents.
The issuer consumes a refresh token and rotates it when it handles an
accepted request, before its delayed response is delivered. Dropping that
response does not undo the rotation. The fake has no LLM or application
policy.

The rotating issuer comes from Smith's scripted OAuth peer at `d218817`.
This crate keeps its credential and response semantics; its HTTP framing,
connection routing, and LLM access checks do not belong to this record-level
world. The caller's `skein-http` stack owns framing when used over a network.
