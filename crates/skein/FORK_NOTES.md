# skein fork notes

`skein` is a hard fork of **asupersync 0.2.4**, the last release published under
a plain MIT license (the OpenAI/Anthropic license rider was added in 0.2.5;
0.3.3+ additionally require a nightly toolchain). It is a standalone,
maintainable, OSI-licensed async runtime that builds on stable Rust, shared
across our projects (temper, smith, …).

The crate was renamed `asupersync` → `skein` and `asupersync-macros` →
`skein-macros`; the public module layout is otherwise unchanged, so consumer
code ports by renaming the import root (`asupersync::…` → `skein::…`).

Upstream source: `Dicklesworthstone/asupersync` @ 0.2.4 (crates.io, yanked but
MIT). Sibling crates `franken-decision`, `franken-evidence`, `franken-kernel`
are kept at their MIT 0.2.0 versions under `../` (sibling members of this
workspace).

## License boundary (important)

Post-0.2.4 upstream code carries the OpenAI/Anthropic rider and **must not** be
copied into this tree. All fixes and additions below were written clean-room
against the 0.2.4 codebase, RFCs, and consumer needs — not ported from 0.3.x.

## Changes vs stock 0.2.4

### 1. Ambient `Cx` during task poll (correctness fix) — `src/runtime/scheduler/three_lane.rs`
Stock 0.2.4 installs the task's capability context (`Cx::set_current`) only on
the task *completion* path, as a hot-path optimization — so `Cx::current()`
returns `None` inside a running task's future body. temper's engine relies on
the task's `Cx` being ambient during execution (clock access, `current_cx()`,
`engine_now()`, `sleep_for()`). Fix: clone the task's `Cx` in the poll
dispatch (both global and local task paths) and install it around
`stored.poll(&mut cx)`. Costs ~3 Arc refcount ops per poll; required for
correctness.

### 2. `RuntimeHandle::spawn_with_cx` / `try_spawn_with_cx` — `src/runtime/builder.rs`, `src/runtime/state.rs`
Spawn a task whose factory receives the task's own `Cx` by value (a root-region
child that observes shutdown cancellation), instead of relying on ambient
`Cx::current()`. Backed by `RuntimeState::create_task_with_cx`, which mirrors
`create_task` but threads the `Cx` to the factory.

### 3. `Runtime::current_handle()` — `src/runtime/builder.rs`
Returns the runtime handle ambient to the current thread (worker threads and
the `block_on` thread). Implemented with a thread-local holding a shared
write-once slot containing a `Weak<RuntimeInner>` (published after the runtime
`Arc` is built). `Weak` avoids a reference cycle that would otherwise keep the
runtime alive forever and break shutdown.

### 4. Async child process API — `src/process.rs`
- `Child::wait_with_output_async(&mut self, &Cx) -> WaitWithOutput` — drains
  stdout/stderr concurrently through the reactor and reaps the child via
  non-blocking `try_wait`, never blocking the runtime thread. While a pipe is
  open the reactor drives wakeups; once both reach EOF but the child is not yet
  reaped (a brief race) a short capped-backoff timer re-polls. Cancel-safe:
  dropping the future leaves the borrowed `Child` intact, so
  `kill_on_drop(true)` still terminates it.
- `Command::output_async(&mut self, &Cx)` — the async counterpart of `output()`.

### 5. h1 request-framing hardening (RFC 9112) — `src/http/h1/codec.rs`
Stock 0.2.4 already rejects the main smuggling vectors (CL+TE together,
duplicate Content-Length / Transfer-Encoding, `Transfer-Encoding` tokens other
than bare `chunked`, CR/LF in header values, empty header names, obs-fold). Two
parsers were too lax and are now strict:
- **chunk-size** (`parse_chunk_size_line`): was `split(';').trim()` +
  `from_str_radix`, which accepted surrounding whitespace and a `+` sign. Now
  requires `1*HEXDIG` exactly (per §7.1) before the optional `;chunk-ext`.
- **Content-Length** (`parse_content_length`): was `.trim().parse()`, which
  accepted whitespace and a `+` sign. Now requires `1*DIGIT` exactly (§6.2).

Black-box batteries that lock this in currently live in the temper consumer
(`temper-io-engine/tests/h1_security.rs` — RFC 9112 cases — and `h1_fuzz_lite.rs`
— seeded random + byte-at-a-time vs whole-buffer agreement, decoder must never
panic). They drive only skein's public `Http1Codec` API and could move into this
repo as integration tests.

## Stock 0.2.4 bugs inherited from upstream (re-verified vs the 0.3.4 review)

The 0.3.4 review found several defects. Their status in this 0.2.4 base:
- **block_on 1→25ms sleep backoff cliff** — ABSENT. `run_future_with_budget`
  (`src/runtime/builder.rs`) parks/`yield_now`s; it never `thread::sleep`s. The
  cliff was introduced after 0.2.4, so temper's `block_on` engine loop is safe.
- **Semaphore `try_acquire` panic-leak** — ABSENT. 0.2.4's `try_acquire`
  decrements under the lock and returns `Result`; no panic, no leak.
- **`spawn_blocking` inline fallback** — PRESENT (`src/runtime/spawn_blocking.rs`,
  ~line 157): with a `Cx` but no blocking-pool handle it runs the closure
  **inline on the calling thread**. temper MUST ensure its engine `Cx` carries a
  blocking-pool handle, or any `spawn_blocking` (incl. all `fs` ops) blocks the
  loop. This is a deliberate determinism fallback upstream; left unchanged so the
  lab runtime stays deterministic — it is a temper-config obligation, not a fork
  patch.
- **`BytesMut`/`Vec<u8>` `BufMut` contract** — PRESENT: `chunk_mut()` returns an
  empty slice and `advance_mut(n>0)` panics. Only bites *generic* `BufMut`
  writers (`put_slice`-via-`chunk_mut`); temper's own paths use `put_slice`
  directly. Don't feed these to a third-party generic-`BufMut` codec.
- **`File` AsyncRead/Write blocks in `poll`** — PRESENT (`src/fs/file.rs`):
  poll-based file I/O does blocking syscalls inline. temper avoids it by routing
  fs through `spawn_blocking` (subject to the pool-handle requirement above).
- **`Child::drop` reaping** — DIFFERENT: default `kill_on_drop=false` makes drop
  a no-op (no waitpid, no kill). temper sets `kill_on_drop(true)` everywhere it
  may abandon a child, and our `wait_with_output_async` is cancel-safe by leaving
  the `Child` intact for that drop-kill.

## Re-verification checklist on any future change
- Build the whole workspace on **stable** rustc (`cargo check --workspace`); the
  fork must never require nightly.
- Run a consumer's behavioural canaries — for temper:
  `cargo test -p temper-io-engine --test timer_pump --test engine_loop`
  (timer lost-wakeup + http-with-timers) and the h1 batteries
  (`--test h1_security --test h1_fuzz_lite`).
- Run the consumer's full suite (temper: 1155 tests at last port).

Note: this crate's own in-tree `#[cfg(test)]` suite does not currently build as
`cargo test` — several `include_str!`/`include_bytes!` golden/artifact files
were excluded from the upstream 0.2.4 package. Restoring those (from the
upstream tag, pre-rider) is the path to running skein's own tests; until then,
verification rides on `cargo check` + consumer suites.
