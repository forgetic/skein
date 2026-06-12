# skein

A spec-first, **cancel-correct**, capability-secure async runtime for Rust —
structured concurrency where every task is owned by a region, cancellation is a
protocol rather than a silent drop, and effects flow through an explicit
capability context (`Cx`).

skein builds on **stable Rust**, is MIT-licensed, and is shared across our
projects (temper, smith, …).

## Workspace layout

| crate | purpose |
|-------|---------|
| `crates/skein` | the runtime: scheduler, `Cx`/cancellation, channels/sync, timers, io, net/tcp + h1 http, process, bytes, codecs |
| `crates/skein-macros` | optional proc-macros for the structured-concurrency surface (`scope`, `spawn`, `join`, `race`) |
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

## Origin

skein was forked from `asupersync` 0.2.4, the last upstream release published
under a plain MIT license. It is developed as a standalone project: upstream
is not tracked, and post-0.2.4 upstream code (which carries a more restrictive
license rider) must never be copied into this repository — all changes here
are written clean-room against the 0.2.4 base, the relevant RFCs, and our own
needs.

## License

MIT — see [LICENSE](LICENSE). Original copyright Jeffrey Emanuel (the forked
0.2.4 base); fork modifications copyright Free Ekanayaka.
