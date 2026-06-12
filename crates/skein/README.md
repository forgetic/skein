# skein

A spec-first, **cancel-correct**, capability-secure async runtime for Rust.

Structured concurrency where correctness is structural, not conventional:

| Guarantee | What it means |
|-----------|---------------|
| **No orphan tasks** | Every spawned task is owned by a region; region close waits for all children |
| **Cancel-correctness** | Cancellation is request → drain → finalize, never silent data loss |
| **Bounded cleanup** | Cleanup budgets are *sufficient conditions*, not hopes |
| **Capability security** | Effects (spawn, time, I/O, randomness) flow through an explicit `Cx` |
| **Deterministic testing** | The lab runtime makes concurrency deterministic and replayable |

## Quick start

```toml
[dependencies]
skein = { path = "../skein/crates/skein" }   # or a git dependency
```

```rust
use skein::cx::Cx;
use skein::runtime::RuntimeBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = RuntimeBuilder::new().build()?;
    runtime.block_on(async {
        // your async work
    });
    Ok(())
}
```

## Layout

- `runtime` — schedulers (three-lane work-stealing, current-thread), reactor
  (epoll/kqueue/io_uring), timers, blocking pool
- `cx` — the capability context: budgets, cancellation, scoped regions
- `channel`, `sync` — cancel-correct channels and synchronization primitives
- `net`, `http` — TCP/UDP/Unix sockets and an RFC 9112-hardened HTTP/1.1 stack
- `process` — async child processes driven through the reactor
- `lab` — the deterministic lab runtime: virtual time, chaos injection, trace
  capture and replay (`spork`)

See the [workspace README](../../README.md) for the project's origin and
licensing.

## License

MIT
