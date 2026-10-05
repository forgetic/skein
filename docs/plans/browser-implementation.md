# Implementing skein-browser

Implemented, 2026-10-06. The plan used to build `skein-browser`, the browser
testing kit of [the browser design](../design/browser.md), and what it
needs that skein does not have yet. Read it with
[the programming model](../foundation/programming-model.md),
[the testing strategy](../foundation/testing-strategy.md),
[io](../design/io.md), [the kernel](../design/kernel.md),
[the shell](../design/shell.md) and [testing](../design/testing.md).
The sketches give shapes, not final code: names and details are settled
as each increment is built.

The implementation keeps operation transitions together in
`testing/skein-browser/src/browser.rs`, uses a bounded JSON document view
for CDP replies, and serves fixed pages from `tests/browser/src/serve.rs`.
The real suite covers six scenarios and runs under the `browser` nextest
profile. Real CDP transcript fixtures remain to be captured on a machine
that permits the ring backend.

## 1. In one page

- **The machine first, the real browser last.** The kit's machine, its
  wire and its world against a fake browser need almost nothing new from
  skein. Only the tests against real Chromium need processes.
- **Three prerequisites:**
  - a held buffer read as a stream, moved into lib (2.1), which is small;
  - io's processes, with pipes at descriptors the spawner chooses (2.2),
    which is large, and which temper's worker needs too;
  - pages for the real browser to load, from a small page server in the
    kit's test crate on skein-http's server (2.3), so that the HTTP
    example is not needed.
- **Six increments for the kit:** the wire; persons and pages; finding
  and waiting; actions; reports and bounds; the real browser (section 4).
- **Verified by a fake, held to the real one.** A machine world drives
  the kit against a scripted fake browser that misbehaves on purpose. The
  facts the fake assumes are then checked against real Chromium, and
  transcripts of real Chromium feed the step tests.
- **A spike on 2026-10-05,** with Chromium 153, checked the design's main
  assumptions (section 7).

## 2. Prerequisites

### 2.1 A held buffer as a stream (skein-lib)

`skein_http::sse::Data` serves a whole buffer as the side below of a
stream, which is how an event's data reaches a JSON tokenizer. The kit
needs the same for each CDP message, and depends on lib and JSON only, so
the type moves into lib, and `sse::Data` becomes a re-export:

```rust
// skein_lib::stream
pub struct Held { bytes: Box<[u8]>, at: usize, ended: bool }

impl Held {
    pub fn new(bytes: Box<[u8]>) -> Held;
    /// The answer to a demand: exactly what it reads, or End.
    pub fn answer(&mut self, read: Read) -> Option<Up>;
}
```

### 2.2 Processes (kernel, simulator, ring, io)

io.md, section 6 designs processes, and kernel.md, simulator.md and
shell.md list them as not built. temper's worker pulls the same work, so
it is built once, for both, as its own increments through skein's gate.
If the worker gets there first, it gets a plan of its own and this
section shrinks to a reference. The kit adds one thing to the design:
**pipes at descriptors the spawner chooses**, beyond the standard three.

```rust
// skein-io's kernel records, added
pub enum Op {
    ..
    Spawn  { spawn: Box<Spawn> },          // synchronous (kernel.md, 6)
    Wait   { pidfd: Fd },                  // through the ring: waitid
    Signal { pidfd: Fd, signal: Signal },  // synchronous: pidfd_send_signal
}

pub struct Spawn {
    pub program: Box<[u8]>,
    pub args: Box<[Box<[u8]>]>,
    pub env: Box<[Box<[u8]>]>,
    pub root: Fd, pub dir: Box<[u8]>,      // the working directory, beneath a root
    pub pipes: Box<[Pipe]>,                // the descriptors the child gets, and nothing else
}

pub struct Pipe { pub child: u32, pub way: Way }   // the child's descriptor; it reads (In) or writes (Out)

// skein-io, the protocol side, added
pub enum Request { .., Spawn { owner: Token, spawn: Spawn }, Signal { child: Token, signal: Signal } }
pub enum Event   { .., Spawned { owner: Token, child: Token, pipes: Box<[Token]> },
                       Exited  { owner: Token, exit: Exit } }
```

- **A child gets exactly what it is given:** the pipes asked for, at
  their descriptors, `/dev/null` for any of 0 to 2 not asked for, and
  nothing else. Every other descriptor is close-on-exec.
- **The ring adapter spawns with glibc's `pidfd_spawn`:** posix_spawn's
  file actions put each pipe at its descriptor, and the child is a pidfd
  from the start, as io.md, 6 asks. `clone3` with `CLONE_PIDFD` is the
  fallback if that proves wrong. `std::process` stays banned.
- **Pipes are streams** (io.md, 3), and a child is closed once it has
  exited and its pipes and pidfd are closed (io.md, 6).
- **The simulator** runs programs through the machine seam, and the
  minimal fake machine gains its programs: one that echoes, one that
  exits with a status, one that never exits.
- **Conformance,** on both backends: a spawn, pipes both ways at chosen
  descriptors, the end of a pipe at exit, an exit status, a signal, and a
  child that never exits killed.

### 2.3 Pages to load (tests/browser)

The real browser's pages come from a page server in the kit's test
crate: skein-http's server over io's sockets, on loopback port 0, in the
same loop as the kit, serving fixed pages from memory with the headers
each needs (a content security policy among them). This replaces the
HTTP example that browser.md, 9 and 10 name. Increment 6 updates those
sections.

### 2.4 The machine

Chromium on the development machine (153 when this was written). The
tests find it on `PATH` as `chromium`, unless the environment names
another. The test reads that, never the kit, which takes the path as
configuration.

## 3. The crate

### 3.1 Layout

```
testing/skein-browser/
├── Cargo.toml          skein-lib, skein-json
└── src/
    ├── lib.rs          the crate's doc; re-exports; up, down, fire; MAX_OUT; worst_case
    ├── boundary.rs     Request, Event, Below, Down, Query, Seen, Expect, Refusal, Trouble, Go, Key
    ├── limits.rs       Limits, worst_case, largest_read, largest_room
    ├── command.rs      what the owner spawns: program, arguments, environment, pipes
    ├── browser.rs      Browser: its phase, persons, pages, operations, the command table
    ├── wire/
    │   ├── frame.rs    NUL framing, both ways
    │   ├── encode.rs   each command, measured then written
    │   ├── decode.rs   replies by the shape each command expects, and events
    │   └── base64.rs   a screenshot's PNG
    ├── op/
    │   ├── open.rs     opening a page
    │   ├── go.rs       navigation, reload, history
    │   ├── find.rs     find, and await over it
    │   ├── press.rs    the checks, then the mouse
    │   ├── input.rs    type, compose, key
    │   └── report.rs   snapshot, screenshot
    ├── trouble.rs      the events that say something went wrong
    ├── trace.rs        trace records
    ├── tests.rs
    └── tests/          step tests per area
```

It is a step crate: `no_std` with `alloc`, `forbid(unsafe_code)`, and
the step crates' lints, as `skein-echo-client` is (testing.md, 6).

### 3.2 The browser's state

```rust
pub struct Browser {
    phase: Phase,                        // Starting, Ready, Closing, Closed
    persons: Slab<Person>,               // the owner's token, the context's id, its pages
    pages: Slab<Page>,                   // the owner's token, target, session, its operations
    ops: Slab<Op>,                       // the owner's token, its page, its state
    commands: Map<u64, Pending>,         // id → who waits, and the reply shape it expects
    next_id: u64,                        // ids in order, never reused
    reading: Option<Held>,               // the message being decoded
    writing: Queue<Box<[u8]>>,           // commands, sized, waiting for room
    deadlines: Deadlines<Due>,           // polls, answers
    stderr: Tail,                        // the last bytes, for the report
}

enum Pending {
    Version,
    Context(Id<Person>),
    Target(Id<Page>), Attach(Id<Page>), Enable(Id<Page>), Navigate(Id<Page>),
    Step(Id<Op>, Shape),                 // an operation's next reply, and what it must hold
}
```

### 3.3 An operation is a state machine

Each operation steps from a reply to its next command with the
transition idiom (programming-model.md, 5.4), never a closure:

```rust
enum Press {
    Scrolling,                          // DOM.scrollIntoViewIfNeeded sent
    Measuring,                          // DOM.getContentQuads sent
    Hitting { at: Point },              // DOM.getNodeForLocation sent
    Within  { at: Point, hit: u64 },    // DOM.describeNode of the node: is the hit inside it?
    Pressing { at: Point },             // mousePressed sent
    Releasing,                          // mouseReleased sent; its reply is Done
}

enum Await {
    Looking { until: Time },            // a find in flight
    Resting { until: Time },            // the poll's deadline armed
}

fn press(op: &mut Press, reply: Reply, out: &mut Steps) -> Next;  // Next: a command, a terminal, or a refusal
```

### 3.4 Reading a message

- **Framing:** a scan to the NUL under `Limits::message`, as lib's
  `Read::Scan`. The message is held whole (2.1).
- **Two passes over a held message:** the first finds `id`, `method` and
  `sessionId`, skipping everything else; the second decodes the payload
  with the decoder the command table, or the event's method, chooses.
  Key order then never matters.
- **Decoders keep only their fields:** an accessibility node's role,
  name, value, the states the kit reports and its backend node id, up to
  `Limits::matches`, with the rest counted; a quad's eight numbers as
  whole pixels; an error's code and message.

### 3.5 Limits

```rust
pub struct Limits {
    pub persons: u32, pub pages: u32, pub ops: u32,
    pub commands: u32,         // in flight
    pub message: u32,          // a message read
    pub command: u32,          // a command written
    pub matches: u32,          // kept per query
    pub text: u32,             // a name, a value, a trouble's text
    pub snapshot: u32, pub screenshot: u32, pub stderr: u32,
    pub poll: Duration,        // between looks while awaiting
    pub answer: Duration,      // the most a command waits for its reply
}
```

## 4. Increments

Each is a branch through skein's gate (`scripts/check.sh`), merged
`--ff-only`.

0. **Prerequisites.** The held stream (2.1) first, since it is small.
   Processes (2.2) on their own track, needed only by increment 6.
1. **The wire.** Framing, encoding, decoding, the command table, and the
   browser from `Starting` to `Ready` and through `Closing`, with
   stderr's tail. Step tests use synthetic transcripts, written from
   CDP's documentation and marked as such, until increment 6 captures
   real ones.
2. **Persons and pages.** Contexts, targets, flattened sessions,
   enabling, the viewport, navigation to the load event, `Go`, trouble,
   crashes, and every entity closing with one terminal. The machine world
   starts here, with the fake browser (section 5).
3. **Finding and waiting.** `Find`, `Await` and its polls, scopes, boxes,
   and matches past the limit counted.
4. **Actions.** `Press` and its checks, `Type`, `Compose` and `Key`, with
   each refusal.
5. **Reports and bounds.** `Snapshot`, `Screenshot`, `worst_case`
   against the counting allocator, and the fuzzy sweeps.
6. **The real browser.** After processes: the page server, the tests
   against Chromium, real transcripts in place of the synthetic ones, the
   browser suite (`--profile browser`) in `scripts/check.sh`, and
   browser.md, 9 and 10 updated.
7. **Out of provisional.** browser.md settled from what the increments
   learned; testing.md's status; the README's "designed, not built"
   dropped.

## 5. Tests

```
testing/skein-browser/src/tests/  step tests: frame, encode, decode, each operation, limits
tests/browser/                    skein-browser-world
├── Cargo.toml                    skein-lib, skein-json, skein-browser, skein-world; skein-heap for memory;
│                                 skein-io, skein-shell, skein-http, skein-scratch for the real browser
├── src/lib.rs                    what the world plays and checks, in its module doc
├── src/fake.rs                   the fake browser: a scripted CDP peer over in-memory pipes
├── src/page.rs                   a fake page: its accessibility tree, boxes, what a press does
├── src/world.rs                  the kit, the fake and a scripted test in one loop; Settings::calm, ::random
├── src/referee.rs                the contracts (below)
├── src/serve.rs                  the page server for the real browser (2.3)
├── src/pages.rs                  the fixed pages it serves
├── transcripts/                  real Chromium's messages, each with the version that wrote it
└── tests/
    ├── wire.rs                   framing and the command table, through the world
    ├── pages.rs                  persons, pages, navigation, closing
    ├── find.rs                   finding, scoping, waiting met and missed
    ├── press.rs                  presses landing, and each refusal
    ├── input.rs                  typing, composing, keys
    ├── trouble.rs                exceptions, console and log errors, crashes
    ├── referee.rs                the referee fails what it must, one test per rule
    ├── memory.rs                 at the limits, the heap under worst_case
    ├── fuzzy_world.rs            random scenarios and faults, many seeds
    ├── fuzzy_memory.rs           random load against worst_case
    └── browser_*.rs              against real Chromium: the browser suite only
```

- **The fake browser is ordinary Rust.** Only this world needs it, so it
  stays a script of the world (testing-strategy.md, 4). It answers the
  commands the kit sends from pages a scenario describes:

```rust
pub struct FakePage { nodes: Vec<FakeNode>, url: String }
pub struct FakeNode {
    id: u64, role: String, name: String, value: String, states: States,
    rect: Option<Rect>, parent: Option<u64>,
    covered_by: Option<u64>,           // what a hit test at its centre finds instead
    on_press: Vec<Effect>,             // what a press does to the page
}
pub enum Effect { Rename { node: u64, name: String }, Remove { node: u64 },
                  Add { node: FakeNode }, Navigate { url: String }, Throw { text: String } }
```

  The seed drives its faults: replies late, interleaved with events, or
  never; a node changing between the steps of a press; a page or the
  browser crashing mid-operation; a message past the limit.
- **The referee:** one terminal per operation; nothing after `Closed`;
  every command answered or failed; no mouse event dispatched for a press
  that was refused; a `Met` only when the fake's page satisfied the
  expectation at that moment.
- **The browser suite:** `browser_*.rs`, run only by `--profile browser`.
  The default and fuzzy profiles leave them out, so neither needs
  Chromium. Its budget is set when its first tests are measured.

## 6. For temper

temper's web uses the kit in its browser tier (temper's
`docs/design/web/architecture.md`, 7.3). temper builds the person's DOM
face over the kit's requests, a harness that fails a test on any trouble
its scenario did not expect, and the engine serving the bundle in the
real loop. Its own browser suite stands on skein's.

## 7. What a spike showed

On 2026-10-05, a throwaway Python script drove the installed Chromium
(153.0.8010.52) over the pipe:

- **The pipe works as designed:** NUL-ended JSON on descriptors 3 and 4,
  ready about 0.2 s after spawning, gone about 0.1 s after
  `Browser.close`. Without both descriptors open at exec, Chromium exits
  saying so, which is why the kit's command names its pipes.
- **Flattened sessions** carry the session id on the replies and events
  of a page.
- **Finding:** `Accessibility.queryAXTree` matches by role and name
  together. By name alone, it matches an element and its text node both.
  Typed values and the focused state show on the accessibility node.
- **Boxes have fractions** (67.4375), so whole pixels are needed.
- **The hit test** names the element on top. A press on a button under
  an overlay found the overlay, which is `Covered`. Some points answer
  "No node found at given location", and the kit reads that as
  `Covered` too. `DOM.describeNode` with depth -1 lists a node's subtree,
  enough to tell a hit inside it with no script.
- **A node gone** answers "No node found for given backend id": `Gone`.
- **Trouble:** a content security policy violation is a `Log.entryAdded`
  of source `security` at level `error`; `console.error` is a
  `Runtime.consoleAPICalled` of type `error`; an uncaught throw is a
  `Runtime.exceptionThrown`.

## 8. Remaining questions

- Whether polling for `Await` should gain DOM mutation events if it
  proves too slow in service tests.
- Chromium's own sandbox under a service's containment (browser.md, 11).
- Capturing real CDP transcript fixtures for focused decoder tests.
