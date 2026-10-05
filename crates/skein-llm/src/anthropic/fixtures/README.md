These are redacted subject recordings from the adjacent tongs repository,
`crates/tongs/tests/fixtures/anthropic/*/recordings/tongs`, captured on
2026-06-13 by recorder f4e0a2b (see each `meta.json`). The tongs online
`subject_record.rs` harness forwarded requests through a loopback capture
pump to the real provider; consequently the request Host is loopback.
`response.sse` contains raw HTTP chunk framing as well as the provider's
SSE bytes. Headers containing bearer/account/session/request identities
were redacted by the recorder at capture time.

These recordings establish actual provider response grammar and successful
historical tongs requests. They are not captures of Claude Code or Codex
itself, are not newly captured by temper, and do not establish current
subscription identity requirements. Synthetic tests are separate in
`src/tests.rs`.
