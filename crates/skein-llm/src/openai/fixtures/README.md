Redacted subject recordings copied from the adjacent tongs repository,
`crates/tongs/tests/fixtures/codex/*/recordings/tongs`. Each `meta.json`
records capture 2026-06-13, tongs client 0.1.0 and recorder f4e0a2b.
The online `subject_record.rs` harness forwards through a loopback capture
pump to the real ChatGPT Codex route, explaining the loopback request Host.
`response.sse` retains the HTTP chunk framing and provider SSE bytes.
The recorder redacts bearer, account, session and request identities.

These are successful historical tongs requests and actual provider responses,
not captures of Codex itself and not fresh temper captures. They do not
establish current subscription identity. The single-text archive includes encrypted reasoning. This archive has no
Codex parallel call scenario; parallel ordering remains covered by labeled
synthetic tests until a real capture is available.
