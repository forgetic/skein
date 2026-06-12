# Spork deterministic ordering

Spork's deterministic replay relies on a stable, totally ordered trace of
runtime events. Every event carries a monotonically increasing sequence
number assigned at emission time, a virtual timestamp, and a kind-specific
payload. Replays compare traces structurally: two runs are equivalent when
their event sequences match kind-for-kind and field-for-field.

For that comparison to stay meaningful across releases, the event taxonomy
below is **stable**: a kind's `stable_name` and its required field set are
frozen once shipped. New kinds may be added, but existing entries must not
be renamed, removed, or have fields dropped. The
`trace_event_taxonomy_is_documented` test in `src/trace/event.rs` asserts
that every `TraceEventKind` has an entry here, so additions to the enum must
be reflected in this table.

## Event taxonomy

Each entry maps a kind's stable name to the fields its payload must carry:

### Task lifecycle

- `spawn` => `task, region`
- `schedule` => `task, region`
- `yield` => `task, region`
- `wake` => `task, region`
- `poll` => `task, region`
- `complete` => `task, region`

### Cancellation

- `cancel_request` => `task, region, reason`
- `cancel_ack` => `task, region, reason`

### Regions

- `region_created` => `region, parent`
- `region_close_begin` => `region, parent`
- `region_close_complete` => `region, parent`
- `region_cancelled` => `region, reason`

### Obligations

- `obligation_reserve` => `obligation, task, region, kind, state, duration_ns, abort_reason`
- `obligation_commit` => `obligation, task, region, kind, state, duration_ns, abort_reason`
- `obligation_abort` => `obligation, task, region, kind, state, duration_ns, abort_reason`
- `obligation_leak` => `obligation, task, region, kind, state, duration_ns, abort_reason`

### Virtual time and timers

- `time_advance` => `old, new`
- `timer_scheduled` => `timer_id, deadline`
- `timer_fired` => `timer_id, deadline`
- `timer_cancelled` => `timer_id, deadline`

### I/O

- `io_requested` => `token, interest`
- `io_ready` => `token, readiness`
- `io_result` => `token, bytes`
- `io_error` => `token, kind`

### Determinism support

- `rng_seed` => `seed`
- `rng_value` => `value`
- `checkpoint` => `sequence, active_tasks, active_regions`
- `futurelock_detected` => `task, region, idle_steps, held`
- `chaos_injection` => `kind, task, detail`
- `user_trace` => `message`

### Monitors and links

- `monitor_created` => `monitor_ref, watcher, watcher_region, monitored`
- `monitor_dropped` => `monitor_ref, watcher, watcher_region, monitored`
- `down_delivered` => `monitor_ref, watcher, monitored, completion_vt, reason`
- `link_created` => `link_ref, task_a, region_a, task_b, region_b`
- `link_dropped` => `link_ref, task_a, region_a, task_b, region_b`
- `exit_delivered` => `link_ref, from, to, failure_vt, reason`

## Ordering guarantees

Within a single trace:

1. Sequence numbers are unique and strictly increasing in emission order.
2. A task's `spawn` precedes any of its `schedule`/`poll`/`wake`/`yield`
   events, which precede its `complete`.
3. `cancel_request` precedes the matching `cancel_ack`.
4. A region's `region_created` precedes `region_close_begin`, which precedes
   `region_close_complete`.
5. An obligation's `obligation_reserve` precedes exactly one of
   `obligation_commit`, `obligation_abort`, or `obligation_leak`.
6. `time_advance` events carry `old < new`; virtual time never moves
   backwards.
