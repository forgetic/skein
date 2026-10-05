# skein

skein is a kit for network services written in Rust as single-threaded
step machines over Linux io_uring: one thread, one loop, no async. A
service built on it writes only its domain, its own protocols and a short
`main`; skein supplies the rest.

Everyone who builds skein, or builds on it, reads the foundation first:

- [`docs/foundation/programming-model.md`](docs/foundation/programming-model.md):
  how the code is written, in skein and in every service built on it.
- [`docs/foundation/testing-strategy.md`](docs/foundation/testing-strategy.md):
  how it is tested.
- [`docs/foundation/notes.md`](docs/foundation/notes.md): the questions
  still open about both.

`docs/design/` holds the design of each of skein's parts, one document per
part, each the brief for the agent that builds it;
[`testing.md`](docs/design/testing.md): how skein itself is tested; and
[`examples.md`](docs/design/examples.md): the example services a service
starts from.

## What skein holds

| Part | Crate | What | Design |
|---|---|---|---|
| lib | `skein-lib` | handles, slabs, bounded containers, cursors, streams, deadlines, time | [lib.md](docs/design/lib.md) |
| the kernel boundary | `skein-io` (`kernel`) | the records io submits to a backend and the completions it gets back | [kernel.md](docs/design/kernel.md) |
| io | `skein-io` | sockets, pipes, files, processes, signals to the service | [io.md](docs/design/io.md) |
| the shell kit | `skein-shell` | the io_uring backend, the clock, the seed, startup | [shell.md](docs/design/shell.md) |
| the simulator | `skein-sim` | the simulated kernel and its faults; beside it, the conformance suite (`skein-conformance`) | [simulator.md](docs/design/simulator.md) |
| the fake checkout | `skein-fake-checkout` | for tests: deterministic files, scripted commands and local git mechanics; worlds own policy and delivery | [fake-checkout.md](docs/design/fake-checkout.md) |
| the counting allocator | `skein-heap` | for tests: the heap counted, and each step checked against its worst case | [testing.md](docs/design/testing.md) |
| the world harness | `skein-world` | for tests: each process's `iterate` in one loop, over the simulator or the ring, with a referee | [examples.md](docs/design/examples.md) |
| HTTP | `skein-http` | HTTP/1.1 client and server, server-sent events | [http.md](docs/design/http.md) |
| JSON | `skein-json` | a bounded tokenizer, a sized writer | [json.md](docs/design/json.md) |
| TLS | `skein-tls` | the TLS client, a stream over rustls's unbuffered connection, with ring | [tls.md](docs/design/tls.md) |
| LLM | `skein-llm` | provider-neutral calls, streaming deltas and completions; ChatGPT/Codex subscription access | [llm.md](docs/design/llm.md) |
| the browser kit | `skein-browser` | for tests: a headless Chromium driven over the DevTools protocol on a pipe, by role and name, from the loop; designed, not built | [browser.md](docs/design/browser.md) |

```
crate          depends on
skein-lib      nothing
skein-io       lib
skein-http     lib
skein-json     lib
skein-tls      lib, rustls (and ring beneath it)
skein-llm      lib, http, json
skein-browser  lib, json
skein-shell    lib, io, io-uring, libc
skein-sim      lib, io
```

The crates from outside skein are few, and each is an exception the
programming model names (its 2.1 and 3): `io-uring` and `libc`, the
shell's, for the ring adapter; and `rustls` and its dependencies, `ring`
among them for its cryptography, the TLS client's.

- **A kit, not a framework.** skein has no service trait, no generic loop,
  no scheduler and no callbacks. A service calls the parts by name, in
  its own loop of about ten lines. Nothing in skein calls into a service,
  not even the simulator.
- **io_uring is the kernel interface.** io talks to the kernel in owned
  records: an operation goes down carrying its memory, and its completion
  comes back carrying the same memory. The ring backend maps each record
  onto one submission; the simulator implements the same records; a
  readiness backend (epoll, kqueue) may later implement them a third
  time. Nothing above the records changes.
- **Counted and bounded throughout.** Every part exports its `Limits` and
  its `worst_case`, and a service adds them up.
- **Built when pulled.** A part is built when its first user needs it,
  and that user is its first test. temper is the first user, and what it
  builds decides the order:

  | temper builds | which pulls from skein |
  |---|---|
  | the agent's LLM client | io sockets, the ring, the simulator, the HTTP client, server-sent events, JSON, the TLS client |
  | the fake LLM provider, as a service | the HTTP server, the server-sent events writer, the JSON writer |
  | the worker's processes and workspaces | io processes, pipes and files |
  | the engine's forge client and its webhooks | the HTTP client and server |

## Not in skein

- **Domains, and protocols only one application speaks:** a forge's API,
  temper's protocol between worker and
  engine. They are built on skein's machines.
- LLM calls shared by services are in `skein-llm`; a service still owns its
  tool schemas and execution, OAuth renewal, connection/TLS ownership and
  retry policy.
- **A service's wiring:** its `iterate`, the sum of its worst cases, its
  `main`.
- **What a simulated program does.** The simulator plays the kernel; the
  files and programs a scenario needs come from the service's own fake
  machine, which plugs into the simulator.
- **Schedulers, async and threads.** There is one loop per process. A
  service that needs more cores runs more processes.

## Building a service on skein

| Part | Written by | Crate |
|---|---|---|
| lib, io, the machines for foreign protocols | skein | `skein-lib`, `skein-io`, `skein-http`, `skein-json`, `skein-tls` |
| the protocol layer: its connections, its own machines and decoders | the service | `protocol` |
| the domain, and its child domains | the service | `domain` |
| `iterate`, and the sum of the worst cases | the service | `service` |
| `main`: configuration, startup, the loop | the service | `shell`, on `skein-shell` |
| the simulator | skein | `skein-sim` |
| the counting allocator, for memory tests | skein | `skein-heap` |
| the world harness: the loop, the referee, the heap at every iteration | skein | `skein-world` |
| the worlds, the fakes, the fake machine | the service | its tests |
| the lints and `clippy.toml` | copied from skein | the workspace |

```
crate      depends on
domain     skein-lib                                  and its child domains
protocol   skein-lib, skein-io, the skein machines it stacks, domain
service    skein-lib, skein-io, protocol, domain
shell      skein-lib, skein-io, service, skein-shell
tests      skein-lib, skein-io, service, skein-sim, skein-heap, skein-world
```

Every crate may name `skein-lib`, and every one but the domain's
`skein-io`, as the role graph has it (programming-model.md, 4). The shell
and the tests reach the domain and the protocol layer only through the
service, which re-exports what they name of them, their limits.

Before code:

1. the wire protocol, sized, with every length and limit; or, for a
   foreign protocol, the skein machines it stacks;
2. the limits of each layer, and the worst case they imply;
3. the entities of each layer, who owns each, and how they bind;
4. the state machines: states, what each holds, the total transition
   table, and the demands and deadlines of each state.

Then, in order:

1. the domain, with its step tests and domain worlds;
2. the protocol layer, on skein's machines, its own machines fuzzed alone;
3. the service and its `main` last, with the simulator standing in for
   the kernel until then.

What the service finds missing in skein along the way is added to skein,
with the service as its first user and first test.

skein's own examples are built this way, and a service starts from them:
the echo in `examples/echo`, with its steps before code, its fake client
and its worlds in [examples.md](docs/design/examples.md).
