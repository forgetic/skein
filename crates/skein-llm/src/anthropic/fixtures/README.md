Redacted recordings of successful requests to Anthropic's Messages route,
copied from tongs (`crates/tongs/tests/fixtures/anthropic/*/recordings/tongs`);
each `meta.json` records when and by which recorder. tongs'
`subject_record.rs` harness forwarded through a loopback capture pump,
hence the loopback request Host. `response.sse` keeps the HTTP chunk
framing around the provider's SSE bytes. Bearer, account, session and
request identities are redacted.

They show the provider's response grammar, not what a subscription admits
today. Synthetic tests are in `src/tests.rs`.
