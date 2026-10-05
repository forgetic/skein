# Browser

Implemented, 2026-10-06. The design of `skein-browser`: a testing kit
that drives a real, headless Chromium from a service's loop, so that a
service with a web can test it as a person uses it. It is a step machine
over the child's pipes, speaking the DevTools protocol (CDP), with no
dependency beyond skein. Its first user is temper's web (temper's
`docs/design/web/architecture.md`, 7.3).

## 1. In one page

- **A browser is a child process.** The owner spawns Chromium through io
  with the command the kit gives. The kit speaks CDP over two of the
  child's pipes: no WebSocket, no WebDriver, no driver binary, no Node.
- **One loop, one thread.** The kit is a step machine like skein's
  others (programming-model.md): requests from the test, the pipes'
  events from below, deadlines fired by the loop. It never sleeps, and
  it has no async runtime.
- **A test sees what a person sees.** It finds things by role and
  accessible name in the browser's accessibility tree, presses them,
  types into them, and waits for what should show. It runs no script in
  the page and reads no page state.
- **Actions check what a person would.** A press lands only on something
  on screen, enabled and not covered by something else. Otherwise it is
  refused, saying why, and the test can wait and try again.
- **Waiting is a deadline, never a sleep.** An expectation is looked for
  again at a short interval until it is met or its deadline passes. A
  missed expectation reports what was there instead.
- **Trouble is reported, never judged.** The kit reports the page's
  exceptions, console errors, log errors (a content security policy
  violation among them) and crashes. Whether one fails the test is the
  test's call.
- **Bounded like the rest of skein.** Persons, pages, operations,
  commands in flight and each message are counted under limits, and
  `worst_case` covers them.
- **Held to the real browser.** The kit is tested against a fake browser
  in a machine world, and the facts that fake relies on are checked
  against real Chromium, as the simulator is held to the kernel.

## 2. In skein

`testing/skein-browser` depends on lib and `skein-json`. Like `skein-llm`,
it is a composition crate rather than a primitive machine. It sits in
`testing/` because services use it only in tests, but it is step code
under the Rust subset, so it runs in a loop beside the service it
tests.

```
the test: a scripted person, a referee
  ▲  events                ▼  requests
skein-browser             persons, pages, operations; CDP's commands and replies
  ▲  stream::Up            ▼  stream::Down
io: the child's pipes     descriptor 3 (commands), 4 (replies and events), stderr
  ▲                        ▼
Chromium, headless
```

```rust
// skein_browser, the machine
pub fn up(browser: &mut Browser, env: &Env<Limits>, ev: Below,
          above: &mut Queue<Event>, below: &mut Queue<Down>)
pub fn down(browser: &mut Browser, env: &Env<Limits>, rq: Request,
            above: &mut Queue<Event>, below: &mut Queue<Down>)
pub fn fire(browser: &mut Browser, env: &Env<Limits>, now: Time,
            above: &mut Queue<Event>, below: &mut Queue<Down>)

pub enum Below { Commands(stream::Up), Replies(stream::Up), Errors(stream::Up) }
pub enum Down  { Commands(stream::Down), Replies(stream::Down), Errors(stream::Down) }
```

Each entry point declares its `MAX_OUT`. At startup the owner checks
that the kit's largest read and room fit the pipes' intake and output
caps, as for any machine stacked on a stream (io.md, 2).

## 3. The browser

- **The command.** `skein_browser::command` gives what the owner spawns
  (io.md, 6): Chromium's program path (the owner's configuration), its
  arguments, its environment, and the pipes to make, which are the
  child's descriptors 3 and 4 and its stderr. The arguments are:
  - `--headless` and `--remote-debugging-pipe`;
  - `--user-data-dir`, a fresh directory beneath the owner's scratch
    root, so that no two browsers share a profile and nothing survives
    a run;
  - quiet and offline: no first run, no default-browser check, no
    background networking, sync, extensions, component updates or
    default apps;
  - `about:blank` as the first page.

  The environment is minimal, with a fixed locale and time zone, so
  that a page renders dates and numbers the same everywhere.
- **Ready** comes once the browser answers its first command
  (`Browser.getVersion`). The event carries the version, which the test
  writes into its report.
- **stderr is read and kept,** its last `Limits::stderr` bytes, for the
  failure report. Chromium writes there freely, so nothing in it is a
  fault.
- **Closing** sends `Browser.close`, then tells the owner to end the
  child, whose exit the owner awaits through io, with a deadline and
  then a kill, as for any child (io.md, 6). It is a lifecycle
  notification, as `skein-llm`'s `Close` is: the kit stops, and the
  pipes and the child stay the owner's to settle.
- **A browser that ends on its own** (its replies' pipe ends, or it
  breaks) fails every operation in flight with `Crashed`, closes every
  page and person, and is `Closed`.

## 4. The wire

- **Framing.** Each message is one JSON document ending in a NUL byte,
  both ways. The kit reads replies with a scan to the NUL under
  `Limits::message` (lib.md, 7), and writes each command whole, sized,
  within the room granted. A message past the limit fails the browser:
  the limit is the test's to raise.
- **Commands, replies and events.** A command carries an id, a method,
  its parameters and, for a page, its session. Its reply carries the id,
  with a result or an error. An event carries a method and its
  parameters. Ids are drawn in order and never reused. Each command
  in flight is an entry in a bounded table, holding the operation it
  belongs to and the shape of reply it expects.
- **One pipe, many pages.** A page is a target attached in flattened
  mode, so its commands and events travel on the same pipe, tagged with
  its session id.
- **Decoding keeps only what it needs.** A bounded JSON document view
  selects the fields for each expected reply and enabled event. Unknown
  fields, and events it has not enabled, are skipped. Ids and node ids
  are parsed as integers, and coordinates as whole CSS pixels with their
  fractions dropped, so no float enters the kit. A screenshot's base64
  is decoded into its PNG's bytes.
- **The domains used:** `Browser` and `Target` (contexts, pages,
  sessions); `Page` (navigation and its lifecycle, screenshots);
  `Emulation` (a fixed viewport); `Accessibility` and `DOM` (finding
  nodes, their boxes, the hit test, focus, scrolling into view); `Input`
  (mouse, keys, text, composition); and `Runtime`, `Log` and `Inspector`
  (exceptions, console and log entries, crashes). The kit never sends
  `Runtime.evaluate` or `Runtime.callFunctionOn` to run script. Several
  of these commands are experimental in CDP, which is why the real
  browser is checked (section 9).

## 5. The vocabulary

The kit's vocabulary is its own; the test translates its person's
actions into it.

```rust
pub enum Request {                                       // from the test
    Person { person: Token },                            // a browser context: its own cookies and storage
    Page   { person: Token, page: Token, url: Box<[u8]> },
    Go     { page: Token, op: Token, to: Go },           // an address, reload, back, forward
    Find   { page: Token, op: Token, query: Query },     // one look
    Await  { page: Token, op: Token, query: Query, expect: Expect, within: Duration },
    Press  { page: Token, op: Token, node: Node },
    Type   { page: Token, op: Token, node: Node, text: Box<[u8]> },
    Compose { page: Token, op: Token, node: Node, text: Box<[u8]> },  // an IME composition, left open
    Key    { page: Token, op: Token, key: Key },         // Enter, Escape, Tab, Backspace, arrows
    Snapshot   { page: Token, op: Token },               // the accessibility tree, as text
    Screenshot { page: Token, op: Token },
    Close  { entity: Token },                            // a page, a person, or the browser
}

pub enum Event {                                         // to the test
    Ready   { version: Box<[u8]> },
    Opened  { page: Token },
    Found   { op: Token, seen: List<Seen>, more: u32 },  // more: matches past the limit, counted
    Met     { op: Token, seen: List<Seen> },
    Missed  { op: Token, seen: List<Seen>, more: u32 },  // what was there when the deadline passed
    Done    { op: Token },                               // a navigation, a press, typing, a key
    Refused { op: Token, why: Refusal },                 // Gone, Hidden, Covered, Disabled, Crashed
    Snapshot   { op: Token, text: Box<[u8]> },
    Screenshot { op: Token, png: Box<[u8]> },
    Trouble { page: Token, trouble: Trouble, text: Box<[u8]> },  // an exception, a console or log error, a crash
    Closed  { owner: Token },                            // terminal
}

pub struct Query { role: Box<[u8]>, name: Box<[u8]>, within: Option<Node>, boxes: bool }
pub struct Seen  { node: Node, role: Box<[u8]>, name: Box<[u8]>, value: Box<[u8]>,
                   states: States, rect: Option<Rect> }   // focused, disabled, checked, expanded, selected
pub enum Expect  { Present, Count(u32), Absent }
```

- **A person is a browser context,** with its own cookies and storage.
  Two pages of one person share sign-ins and local storage, and each has
  its own session storage, as two tabs do.
- **A node is the browser's backend node id,** which names one element
  for as long as it lives. A node since removed is `Gone`, as a stale
  handle is.
- **Names are exact,** as the page computes them for assistive
  technology: an element's label, or its text. Patterns are out. A
  query may be scoped `within` a node found before, such as a button
  within a card. `boxes` asks for each match's place on screen too.
- **Every operation has one terminal event:** `Found`, `Met`, `Missed`,
  `Done`, `Refused`, `Snapshot` or `Screenshot`. Every person, page and
  the browser itself ends with one `Closed`.

## 6. The operations

Each operation is an entity with its own state, stepping from one reply
to the next command, never a closure waiting on a reply:

| Operation | Its steps |
|---|---|
| opening a page | create a target in the person's context, attach to it, enable the domains it reports on, set the viewport, navigate; `Opened` once the load event fires |
| go | navigate, reload, or move through the history; `Done` once the navigation commits: the load event for a new document, the same-document event for one that stays |
| find | the accessibility nodes matching the role and the name, under the scope or the document; their properties; their boxes, if asked |
| await | a find, again every `Limits::poll`, until the expectation holds (`Met`) or `within` passes (`Missed`) |
| press | the node scrolled into view; its box's centre; refused `Hidden` without a box, `Disabled` if its accessibility node says so, `Covered` if the hit test at that point finds something outside it; then the mouse moved, pressed and released there |
| type | the node focused; the text inserted |
| compose | the node focused; a composition set and left open, which a later `Type` commits |
| key | the key down and up, with the text it makes |
| snapshot | the accessibility tree under the document, as indented lines of role, name, value and states, cut at `Limits::snapshot` |
| screenshot | the viewport as PNG, under `Limits::screenshot` |

- **A press is done when the browser has taken it,** not when its
  effects show. What follows a press (a request, a page redrawn) the test
  awaits.
- **Covered is not a failure.** A dialog that has not closed yet, or a
  toast over a button, covers it for a moment. The test decides whether
  to wait and press again.
- **Every command has a deadline,** `Limits::answer`. A browser that does
  not answer in time fails the operation with what was pending, so a
  hung browser fails a test rather than holding it up.

## 7. Trouble

The kit enables, on every page, the events that say something went
wrong, and reports each as `Trouble` with its text, cut to
`Limits::text`:

- an exception thrown and not caught;
- a console message at error level;
- a log entry at error level, which is how a content security policy
  violation, a failed load or a blocked request shows;
- the page crashing, which also fails its operations in flight with
  `Crashed`.

The kit judges none of them. A test fails on any `Trouble` it did not
expect, and a scenario that provokes one (a page made to fail, for
example) expects it. That is the test's harness, not the kit.

## 8. Limits and memory

`Limits` bound persons, pages, operations in flight, commands in flight,
a message each way, matches kept per query, the bytes of a name or
value, a snapshot, a screenshot, a `Trouble`'s text and stderr's tail.
They also set the poll interval and the answer deadline. Commands are
sized before they are written, and replies are kept only as their
decoders' fields. `worst_case` adds the slabs, the command table, the
queues, one message being read, the largest command and the largest
screenshot; no size computation may wrap. The memory test drives the
kit to its limits against the fake browser and checks the counting
allocator's high water against `worst_case`.

## 9. Testing

- **Step tests:** framing (a message split across reads, several in one
  read, one past the limit), command encoding, bounded decoding, and
  operation transitions use synthetic CDP replies. The separate browser
  suite checks those assumptions against Chromium.
- **The machine world:** the kit against a fake browser, a scripted CDP
  peer over in-memory pipes. Its pages and accessibility trees come from
  the scenario, and the seed drives what goes wrong:
  - nodes that move, are covered, are disabled or vanish between the
    steps of a press;
  - replies that come late, interleaved with events, or never;
  - a page or the browser crashing in the middle of an operation;
  - a message past the limit.

  A referee holds the contracts: one terminal per operation, nothing
  after `Closed`, every command answered or failed, and a press never
  dispatched on a node that was refused. A few seeds run in the focused
  suite, and many in the fuzzy one.
- **The real browser:** the kit in the real loop against Chromium,
  opening fixed pages served by a small `skein-http` server in the same loop: a
  button that changes a heading, a form, a dialog that covers a button,
  a button far down a long page, a script that throws, and a resource the
  content security policy blocks. These tests check the facts the fake
  browser assumes: the framing, flattened sessions, what the
  accessibility queries match, boxes and the hit test, and the events
  each step waits for. A new Chromium version is a run of these, and a
  fact that changed is fixed in the fake first. They need Chromium and do
  not replay, so they form a suite of their own, beside the focused and
  fuzzy suites. A missing Chromium fails that suite, naming what is
  missing; it is never a skip.

The kit's tests are a crate of their own, `tests/browser`
(`skein-browser-world`) (testing.md, 6).

## 10. What it asks of others

- **io:** a spawn that makes pipes beyond the standard three, as a list of
  the child's descriptors, each with its direction. The ring and simulator
  process backends are built (io.md, 6).
- **A service using it:**
  - spawning the browser with `command`, and owning the child, its pipes
    and the scratch root its profile is made in;
  - Chromium's path, in its configuration;
  - a face that turns its person's actions into the kit's requests;
  - a harness that fails a test on unexpected `Trouble` and writes the
    snapshot, the screenshot and stderr's tail into the failure report.

## 11. Open questions

- **Chromium's own sandbox** under a service's containment: whether the
  namespaces it needs are there, or the real loop's sandbox stands in for
  it.
- **Other browsers.** CDP is Chromium's; Firefox and WebKit would need
  WebDriver BiDi, a second wire, if a bug ever shows only there.
- **Text by pattern,** should exact names prove too strict for text that
  carries numbers or times.
- **The browser suite's budget,** set when its first tests are built.
