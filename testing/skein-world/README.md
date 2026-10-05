# World harnesses

`skein-world` serves two kinds of world, both following
`testing-strategy.md`. Each supplies its own fakes, translations, settings
and scenario expectations; the harness knows none of its domain policy.

The crate-root `World`, `Host` and process `Referee` drive processes through
`iterate` on skein's simulator; `real` runs the same processes over the real
ring. Their interfaces remain unchanged.

The `domain` module is the domain-only world's shared machinery, extracted
from temper's `tests/world` at `23d7caa` when smith became its second user:

- `Schedule`, `Key` and latency `Span`: deterministic delivery times,
  FIFO ties, withdrawal and unique names from the world's injected seed.
- `Stage`: admits an event only with room for the whole step's `max_out`;
  the world calls the domain and manages its iteration and reclaim point.
- `Ledger`: opens requests by key and checks their one terminal obligation.
- `Trace` and `assert_replays`: compare boundary order and final outcome.
- `Referee`, `Expectations`, `Judge` and `Verdict`: scenario safety,
  liveness deadlines and immediate or scheduled fault injection. This
  observation referee is separate from the crate-root process referee.
- `domain::heap`: explicit reexports of `skein-heap`'s existing `Counting`,
  `Meter` and `Measured`; there is one allocator implementation.

These helpers are ordinary Rust used only by tests. The world supplies
clock values, limits and observations, bounds the work it scripts and
checks quiescence. No helper performs IO or reads a clock.

The preserved referee regressions and utility contract tests are under
`tests/world/tests/domain_{referee,utilities}.rs`:

```sh
cargo nextest run -p skein-world-tests --test domain_referee --test domain_utilities
```

The full merge gate also runs formatting, workspace clippy and both
nextest profiles, under the 15-second focused and 60-second fuzzy budgets
of `testing-strategy.md`, section 8.
