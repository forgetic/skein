# skein

A spec-first, **cancel-correct**, capability-secure async runtime for Rust —
structured concurrency where every task is owned by a region, cancellation is a
protocol rather than a silent drop, and effects flow through an explicit
capability context (`Cx`).

`skein` is a **maintained, MIT-licensed clean-room fork of `asupersync` 0.2.4**,
the last upstream release published under a plain MIT license. (Upstream 0.2.5+
carry an OpenAI/Anthropic license rider, and 0.3.3+ require a nightly compiler.)
skein builds on **stable Rust**, is OSI-licensed, and is shared across our
projects (temper, smith, …).

## Workspace layout

| crate | purpose |
|-------|---------|
| `crates/skein` | the runtime: scheduler, `Cx`/cancellation, channels/sync, timers, io, net/tcp + h1 http, process, bytes, codecs |
| `crates/skein-macros` | optional proc-macros for the structured-concurrency surface (`scope`, `spawn`, `join`, `race`, `session_protocol`) |
| `crates/franken-{decision,evidence,kernel}` | the evidence-ledger / decision-kernel support crates the runtime depends on (MIT siblings, vendored at 0.2.0) |

## Using it

```toml
[dependencies]
skein = { path = "../skein/crates/skein" }   # or a git dependency
```

```rust
use skein::cx::Cx;
use skein::runtime::Runtime;
```

The crate's public module layout matches upstream asupersync 0.2.4, so code
written against `asupersync::…` ports by renaming the import root to `skein::…`.

## Lineage and the clean-room rule

This is a fork of `Dicklesworthstone/asupersync` @ 0.2.4. **Post-0.2.4 upstream
code is rider-licensed and must never be copied into this repository.** Every
change here is written clean-room against the 0.2.4 base, the relevant RFCs, and
our own needs. The full changelog of divergences — and the status of known
upstream bugs as re-verified against this 0.2.4 base — is in
[`crates/skein/FORK_NOTES.md`](crates/skein/FORK_NOTES.md).

Highlights of what skein changed vs stock 0.2.4:
- ambient `Cx` is installed for the duration of each task poll (not only on the
  completion path), so futures can reach their capability context;
- `RuntimeHandle::spawn_with_cx` / `Runtime::current_handle()` conveniences;
- an async child-process API (`Child::wait_with_output_async`,
  `Command::output_async`) that drains pipes through the reactor and reaps
  without blocking the runtime thread;
- RFC 9112 hardening of the h1 request parser (strict chunk-size and
  Content-Length).

## License

MIT — see [LICENSE](LICENSE). Original copyright Jeffrey Emanuel (asupersync
0.2.4); fork modifications copyright Free Ekanayaka.
