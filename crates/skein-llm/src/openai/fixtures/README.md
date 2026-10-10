Redacted recordings of successful requests to ChatGPT's Codex route,
copied from tongs (`crates/tongs/tests/fixtures/codex/*/recordings/tongs`);
each `meta.json` records when and by which recorder. tongs'
`subject_record.rs` harness forwarded through a loopback capture pump,
hence the loopback request Host. `response.sse` keeps the HTTP chunk
framing around the provider's SSE bytes. Bearer, account, session and
request identities are redacted.

They show the provider's response grammar, not what a subscription admits
today. The single-text archive includes encrypted reasoning. There is no
capture of parallel calls; synthetic tests cover their ordering.

`provider-completed.json` is the real-structure completion captured by the
smith Codex spike, copied unchanged from
`crates/smith/tests/fixtures/provider-completed.json` at smith `8e83931`
(`bench/smith-codex-2026-10-08`). Its large response output and per-item
usage attribution reproduce the wire-token cliff; the selective decoder
retains only status and reported usage. It is archived evidence, not a
fresh live probe.
