# Development process

Provisional, 2026-10-09. How work goes from an idea to `main` in skein and
in every project built on it (smith, temper, jig): who designs, who plans,
who builds, where each of them writes, and how they talk to each other.
Read it before starting a design, a plan or a run.

**How to read this.** Section 1 is the whole process in one page. Section
2 names the roles and section 3 says where everything lives. Sections 4 to
10 follow the work: designing, planning, starting a run, coordinating,
implementing, landing and escalating. Section 11 is each machine's build
infrastructure, section 12 several machines at once, and section 13 the
questions still open. Section 8, the implementer's card, stands on its
own: it is what an implementing agent reads.

## 1. In one page

- **Four roles, two agent programs.** The designer and the planner
  (Claude Code, Opus 5.5 at xhigh effort, interactive with the user) turn
  an idea into design documents and a plan. The coordinator (Codex,
  gpt-6.1-sol at high effort) runs the plan with implementers (Codex
  sub-agents, same model and effort), as many at once as the plan and the
  machine allow.
- **The design is the contract.** It lives in each repository's
  `docs/design/`, and only the designer edits it. Drafts live in
  `docs/design/draft/`. `docs/design/STATUS.md` says how far the code is
  from the design, and which plans take it there.
- **A plan breaks a design into sessions of small increments.** It lives
  in `docs/plans/<plan>/` of the most downstream repository it changes,
  with a code-organization document and interface sketches for the
  boundaries that matter. A session is one implementer's work; an
  increment lands on its own.
- **A run is a plan executing on one machine.** Its state lives outside
  the repositories, in `~/src/rust/tmp/<repository>/<plan>/`: the ledger,
  the log, the escalation files and the session worktrees. A machine runs
  at most one plan; several machines run different plans at once.
- **Every increment lands through `land`:** rebase on `origin/main`, run
  the gate, record its evidence in the commit, push. The push is the
  merge, and its rejection says that main moved. The local `main` follows.
- **Heavy commands queue.** They go through `heavy` into the machine's
  pueue queue: a gate lane that runs alone, and a build lane that runs a
  few at once. kache caches the crates that did not change.
- **Escalation goes through files.** An implementer stops on a real gap
  and tells the coordinator; the coordinator writes `blocked.md`; the
  designer answers in `unblock.md`, amending the design or the plan where
  needed. Only decisions with long-term consequences that are hard to
  change reach the user.
- **Everything else is decided where it arises,** and reported:
  implementation choices by the implementer, design gaps by the designer.

## 2. Roles

| Role | Runs as | Decides | Writes |
|---|---|---|---|
| Designer | Claude Code, Opus 5.5, xhigh | the design, within the user's delegation | `docs/design/**` in any repository, `STATUS.md` included; plans during a run |
| Planner | the same, in the designer's session or a fresh one | how the design is cut into sessions and increments | `docs/plans/<plan>/` |
| Coordinator | Codex `-p coordinator` (gpt-6.1-sol, high) | what starts when, within the plan's dependencies and the machine | the run's directory |
| Implementer | Codex sub-agent, same model and effort | implementation choices within the design and the plan | code and tests in its session's worktree |

### 2.1 The designer

- **Talks with the user.** It reads the design, `STATUS.md`, skein's
  foundation documents and the code, and turns an idea into a draft or
  into contract text.
- **Is the only role that edits `docs/design/`,** in any repository,
  including `draft/` and `STATUS.md`.
- **Answers a run's escalations** (section 10). During a run it may amend
  the design and the plan, with Markdown-only landings.
- **Decides within the user's delegation.** What stays with the user is in
  section 10.4.

### 2.2 The planner

- **Turns a landed design into a plan** (section 5). It is the designer's
  model and effort, often the designer's own session.
- **Does not change the design while planning.** A gap the plan exposes is
  closed by the designer, under the designer's rules, before the plan
  relies on it.

### 2.3 The coordinator

- **Runs one plan on one machine** (section 7): sets up the run, starts a
  session as soon as its dependencies have landed, records every report,
  keeps the ledger, watches the machine and escalates.
- **Never designs, decides, reviews or writes code.** It never edits a
  design or a plan, never answers a design question, and never fixes
  anything itself. It checks reports with git only.

### 2.4 The implementer

- **Implements one session at a time,** increment by increment, in its own
  worktree (section 8). It lands its own increments and resolves its own
  rebase conflicts.
- **Escalates to the coordinator,** and only on real gaps (section 8.7).

## 3. Where things live

### 3.1 Repositories

- **Each project is a repository under `~/src/rust/`,** on GitHub under
  `forgetic`: skein, then smith, which builds on skein, then temper, which
  builds on both. jig is built inside temper's repository (`temper/jig/`)
  until it moves to its own.
- **Dependencies point upstream only.** An upstream repository's code never
  depends on a downstream one. Its documents may name a consumer as the
  reason for a feature, unless its `AGENTS.md` says otherwise: jig never
  names temper.
- **A primary checkout (`~/src/rust/<repository>`) stays on `main`,
  clean, and equal to `origin/main`.** Nobody works in it. `land` moves it
  forward, and the code graph indexes it, so the graph always describes
  `main`.
- **Each repository names its gate and its budgets:** `scripts/check.sh`
  runs the gate, and `docs/development/workflow.md` (or skein's
  `docs/design/testing.md`, section 6) explains it, with the suites'
  15-second and 60-second budgets (testing-strategy.md, section 8). Its
  `AGENTS.md` holds what an agent must know before touching its code.

### 3.2 The design

- **`docs/design/` holds the contract:** what the code must do, cited by
  the code as `<file>.md, section N`.
- **`docs/design/draft/` holds drafts:** ideas worked out but not yet
  contract. A plan never implements a draft. When a draft becomes design,
  its content moves into the design documents and the draft is deleted,
  or cut down to what is still open.

### 3.3 The status file

Each repository's `docs/design/STATUS.md` says how far its code is from
its design, and which plans take it there:

```
# Status

Updated <date>. How far <repository>'s code is from its design, and which
plans take it there. The designer keeps this file (skein's
development.md, section 3.3).

## 1. Plans

| Plan | State | Takes the design to | Where |
|---|---|---|---|

## 2. The design

| Document | Built | Left |
|---|---|---|

## 3. Not planned

- **<item>** (`<document>.md`, section N): what it is, and what it waits for.

## 4. Drafts

| Draft | About | Next |
|---|---|---|
```

- **A plan's row** names it by its directory, `<plan>`. Its state is
  `drafting`, `ready` (landed, not started), `active on <machine> since
  <date>` or `done <date>`. A plan that changes several repositories has a
  row in each; an upstream repository's row names the plan with its
  repository ("smith's reliability"), and jig's never names temper.
- **Built** is `yes`, `partly` or `no`. **Left** names what is not built,
  by section, and the plan that builds it, or "not planned".
- **The designer keeps the file.** It changes it in the same landing as the
  design or the plan that changes what it says, and changes a plan's state
  when its run starts and ends (sections 6 and 10.5).

### 3.4 Plans

A plan lives in `docs/plans/<plan>/` of the most downstream repository it
changes: the reason for a plan, and the proof that it is done, live with
its consumer, and an upstream repository's history stays its own. A plan
that changes only skein lives in skein; one that changes skein and smith
lives in smith.

| File | Holds |
|---|---|
| `README.md` | inputs, the session table and its dependencies, the rules particular to this plan, "Done when" |
| `code-organization.md` | crates, modules, names, public API, test layout, the owner of each shared interface |
| `<repository>/NN-<name>.md` | one session: its increments and interface sketches |
| `decisions.md` | the decisions the plan rests on, and those left to the user |
| `later.md` | what the design names but the plan does not do, and when it should be done |
| `findings/` | evidence the plan cites, if any (section 4.1) |

The shapes are in section 5.2. A plan is committed and pushed before its
run starts.

### 3.5 A run's directory

A run lives in `~/src/rust/tmp/<repository>/<plan>/`, where
`<repository>` is the plan's own. `~/src/rust/tmp/active-plan` names the
machine's run. Nothing here is versioned.

| File | Written by | Holds |
|---|---|---|
| `ledger.tsv` | the coordinator, through `plan set` | one row per increment: `step`, `repo`, `session`, `state`, `slot`, `branch`, `commit`, `updated`, `title`, `note` (section 7.3) |
| `status.md` | `plan status` | the ledger, rendered, checked against `origin/main` |
| `log.md` | the coordinator | every report as received, every escalation and its answer; appended only |
| `context.md` | the coordinator | what a restarted coordinator needs; rewritten, never appended |
| `heartbeat` | the coordinator | touched at every wake-up |
| `.watch` | `plan watch` | the escalations and events the designer has seen |
| `blocked.md` | the coordinator | open escalations (section 10.2) |
| `unblock.md` | the designer | the answer (section 10.3) |
| `steer.md` | the designer | an instruction nobody asked for (section 10.3) |
| `paused.md`, `done.md` | the coordinator | a pause's checkpoint; the run's end |
| `sessions/<repository>-<NN>.md` | the implementer | its session's checkpoint (section 8.6) |
| `logs/` | `heavy` | each heavy command's output, and `heavy.tsv`, the queue's statistics (section 11.2); without an active run, `~/src/rust/tmp/logs/` |
| `wt/<repository>-<NN>/` | `plan worktree` | the session's worktree, with its own `target/` |

The escalation files are written whole and renamed into place, so the
other side never reads half a file.

### 3.6 The kit

skein's `scripts/dev/` holds the tools every repository uses, and
`scripts/dev/workspace/` the workspace's own files:
`~/src/rust/AGENTS.md`, its `CLAUDE.md`, and the coordinator's Codex
profile. `plan check --fix` links the tools into `~/.local/bin`, so they
are on every agent's `PATH`, and the files into place. The tools read
`DEV_ROOT` (default `~/src/rust`), `DEV_TMP` (`$DEV_ROOT/tmp`) and
`DEV_REPOS` (`skein smith temper`).

| Tool | Does |
|---|---|
| `heavy [--gate] [--tail N] command…` | queues a heavy command, waits, prints the tail of its log, exits with its status (section 11) |
| `land [--docs] [--local] [--gate COMMAND] [--tries N]` | lands the current branch (section 9); `LAND_LOCK` names the lock for `--local` |
| `plan check [--fix] [PLAN_DIR]` | checks the machine and the repositories, and the plan when given one (sections 6 and 12) |
| `plan claim PLAN_DIR`, `plan release` | start and end the machine's run |
| `plan worktree [--remove] REPOSITORY NAME` | a session's worktree, at `origin/main` |
| `plan set STEP KEY=VALUE…` | adds or changes a ledger row |
| `plan status` | renders `status.md` |
| `plan watch` | waits until the designer has something to do (section 10.5) |

`HEAVY_BUILD_JOBS`, `HEAVY_BUILD_SLOTS` and `HEAVY_TAIL` tune the queue;
`PLAN_MIN_DISK_GB`, `PLAN_MIN_MEM_GB`, `PLAN_QUIET` and `PLAN_POLL` tune
`plan`.

## 4. Designing

### 4.1 How a design starts

- **In conversation.** The user brings an idea, a report or a spike. The
  designer reads what the idea touches: the design documents, `STATUS.md`,
  skein's foundation documents and the code, through the code graph.
- **Its result is a draft or contract text.** A draft lands in
  `docs/design/draft/`. Contract text lands in the design documents, with
  `STATUS.md` updated in the same landing.
- **A large change goes through evidence and decisions first:**
  - **Spikes** are branches that try a change quickly (`spike/*`,
    `bench/*`). They are evidence and are never merged.
  - **Findings** are written by read-only sub-agents, one per theme, each
    entry with its symptom, root cause, the spike's change, an assessment
    (keep, rework, redo, drop), the clean fix and its tests. When a plan
    follows, they go in its `findings/`.
  - **Decisions** are the questions the findings raise, each with the
    designer's recommendation. The user answers those that are theirs
    (section 10.4); the designer decides the rest. They go in the plan's
    `decisions.md`.

### 4.2 Writing in parallel

When a change spans many documents, the designer writes a brief and has
sub-agents write and review in parallel.

- **The brief gives:**
  - the inputs, in the order they bind;
  - the house style (section 4.3);
  - a shared vocabulary: the names every writer uses for the concepts
    that cross documents;
  - an assignment table, one writer per set of documents, with no
    document in two sets;
  - the shape of the final report: files changed, sections whose scope
    changed, what other documents must say for the writer's own to hold,
    and what is left open.
- **Writers then reviewers.** Writers edit only their own documents, in
  the design worktree, and never commit. Reviewers then own disjoint sets
  of documents. They check every cross-document item the writers
  reported, and fix their own side. The designer pins the names that
  settle a conflict, and applies what is left.
- **Sub-agents report in their final message.** The designer keeps the
  reports in its session's scratch directory.
- **A design worktree** is a branch `design/<topic>` checked out in
  `~/src/rust/tmp/design/<repository>-<topic>/`.

### 4.3 House style

- **A title, then a dated status line:** "Provisional, 2026-10-09", or
  "revised 2026-10-09".
- **"1. In one page" first,** with bold-led bullets, then numbered
  sections. Cross-references read "run.md, section 5.3", or skein's
  "programming-model.md, section 5.2".
- **Never renumber.** Code cites sections by number. New material goes into
  an existing section or a new subsection; a new top-level section goes
  after the last one.
- **Contracts, not history.** A design never cites a spike, a finding or a
  benchmark number. It justifies by principle and by other design
  documents.
- **Policy above, numbers below.** Domain documents state rules. Numbers
  belong in the documents a repository names for them (smith:
  `protocol/limits.md` derives limits, `shell.md` holds presets).
- **Keep what is right.** Change what a decision changes. Leave open what
  no decision settles, in the document's "Open questions".
- **The world sections** name the stories and invariants that check the
  contract, within the suites' budgets.

### 4.4 Landing a design

A design lands as a Markdown-only branch, with `land --docs` (section
9.4), and with `STATUS.md` updated in the same landing. Its commit body
says what changed and why.

## 5. Planning

### 5.1 What to settle first

These defaults hold unless the user says otherwise. The planner asks the
user only what they and the design leave open, and asks before writing.

- **Parallel.** Sessions run as many at once as dependencies and the
  machine allow. The plan keeps true dependencies only, at the
  granularity of an increment where it can.
- **Formats are pre-release.** Any format may change. Within a plan, each
  format's version changes once, at the first landing that changes it.
  Readers refuse other versions, and nothing is translated.
- **APIs may break** where the design asks. Consumers in repositories the
  plan does not change are fixed separately, in a plan of their own.
- **Scope.** The plan names the repositories it changes; the others are
  read only.
- **Credentials and live services.** The plan names each one it uses, how
  it is borrowed (read only, from the user's existing login), and that no
  agent signs in.
- **Evidence beyond the gate.** The plan says what proves an increment
  where the repository asks for more than its gate (smith: benchmark
  probes, `docs/design/benchmarks.md`).
- **Language.** Rust, for everything.
- **Code organization and interface sketches** are always part of the
  plan.

### 5.2 The plan's files

**`README.md`:**
- the title, and a dated line with each repository's start commit;
- **Inputs:** the design documents, `decisions.md`, `findings/`;
- **The sessions:** per repository, a table with columns File, Session,
  Needs at start, and Increments that need more. Then how many sessions
  can start at once, and the order in which to start them when slots are
  short: first the ones others wait on;
- **Rules of this plan:** only what this document, the repositories'
  `AGENTS.md` and their workflows do not say. For example: fences ("this
  increment lands after every other skein increment"), version owners,
  probes. A plan never restates this document;
- **Done when:** observable conditions.

**`code-organization.md`:**
- crates, with their kinds and names, and what each holds;
- modules, naming, public API, where tests go;
- one owner per shared interface: the session that defines it, which
  every other session uses by name.

**A session file, `<repository>/NN-<name>.md`:**
- **Title and summary:** "Session NN: …", with one paragraph.
- **Read first:** the design sections, decisions and findings it builds
  on.
- **What the design asks:** a short restatement, with section references.
- **Choices this plan makes,** within the design's latitude.
- **Increments,** numbered N.1, N.2, …. Each gives its scope, what it
  needs beyond the session's start, the crates and files it touches (with
  shared hotspots named), its tests by tier, its evidence (or "none:
  deterministic tests cover it") and what it removes.
- **Interface sketches:** in Rust, the shape of the interfaces another
  crate or session depends on, or that are easy to get wrong.
  - Signatures only, with no bodies.
  - One doc line per type and variant: what it is, who sends it, its
    terminal.
  - A module path header on each sketch, and a few lines of prose on who
    owns what and what ends what.
  - A sketch shows decisions, not every field. Where it and landed code
    disagree, the landed code wins, unless the design asks for the change.
- **Questions to raise when reached:** what the design leaves open that
  changes observable behaviour.
- **Done when.**

**`decisions.md`:** numbered decisions, each with a short rationale, and
a closing section "For the user", listing the long-term choices the user
may veto.

**`later.md`:** a table with columns Item, Design and When.

### 5.3 Increments

- **Small, and landed on its own.** Each passes the gate within budget and
  leaves `main` coherent: nothing half-built visible to users, no dead
  code. Prefer several small increments to one large one: parallel
  sessions rebase on each other.
- **The increments that need nothing outside come first,** so the session
  can start at once.
- **Dependencies are named by capability,** for example "skein 03's
  `TimedOut` phase", never by increment number alone: numbers move.
- **Upstream first** (section 9.5): an increment that needs an upstream
  change waits for it, and repins in a commit of its own, at its start.
- **Shared files are named,** so the coordinator sees likely conflicts.
- **Open behaviour stays open.** An increment does not decide what the
  design leaves open if the choice is observable; it names the question.
- **A new test world** costs at most about 1 second focused and 5 seconds
  fuzzy. Anything beyond that is a decision to raise, not one to take.
- **A session may reorder its own increments** when an earlier one needs a
  later one, the later one needs nothing the earlier provides, and landing
  it first leaves nothing unguarded or half-built on `main`. It says so in
  its report and its commit body. Reordering never moves work across
  sessions and never changes an increment's scope.

### 5.4 Before the run

- **A consistency review.** A sub-agent reads the whole plan against the
  design and looks for dependency cycles, fences that contradict each
  other, shared interfaces without one owner, and increments that cannot
  land alone. The planner fixes what it finds.
- **The plan lands** with `land --docs`, and `STATUS.md` gets the plan's
  row, `ready`.

## 6. Starting a run

1. **The plan is on `origin/main`,** with its row `ready` in `STATUS.md`.
2. **The machine is ready.** `plan check --fix <plan-dir>` passes. It
   checks the plan and its `STATUS.md` row, the primary checkouts (on
   `main`, clean, equal to `origin/main`), the tools, pueued and the
   queue's lanes, memory, disk and the coordinator's Codex profile.
   `--fix` links the tools, starts pueued as a user service, and creates
   and starts the lanes.
3. **The run is claimed:** `plan claim <plan-dir>` creates its directory
   and names it in `~/src/rust/tmp/active-plan`.
4. **The designer marks the plan** `active on <machine> since <date>` in
   each `STATUS.md` that has its row, with `land --docs`. The row is what
   keeps another machine from claiming the same plan.
5. **The coordinator starts,** from `~/src/rust`:

   ```
   codex -p coordinator "Coordinate the run of <plan-dir>, as skein's
   docs/foundation/development.md, section 7, says."
   ```

6. **The designer watches,** on the same machine: the run's files are
   local. In the designer's Claude session: "Watch the run of `<plan>` as
   development.md, section 10.5, says."

## 7. Coordinating

### 7.1 Setting up

1. Read this document's sections 3.5, 3.6 and 7 to 12, the plan's
   `README.md` and `code-organization.md`, and the head of each session
   file: its increments and dependencies.
2. Fill the ledger: one `plan set <step> repo=… session=… title=…
   state=…` per increment. A step is named `<repository>-<NN>.<item>`, for
   example `smith-05.3`.
3. Write `context.md`.

### 7.2 Starting sessions

- **Start every session whose start dependencies have landed,** unless
  memory or disk is short (section 11.4).
- **When there are fewer slots than sessions,** start first the sessions
  others wait on, in the README's order. Never hold a slot idle to keep
  the order.
- **The worktree:** `plan worktree <repository> <NN>`.
- **The spawn.** Spawn with `fork_turns: "none"`, so the implementer does
  not inherit the coordinator's conversation, and give it this prompt:

  ```
  You are an implementer. Read ~/src/rust/skein/docs/foundation/
  development.md, section 8, then ~/src/rust/<repository>/AGENTS.md,
  then <plan-dir>/README.md, then <plan-dir>/<repository>/<file>.
  Implement that session in <worktree>, one increment at a time. Your
  checkpoint is <run>/sessions/<repository>-<NN>.md.
  ```

  A sub-agent starts in the coordinator's directory, so it does not load
  its repository's `AGENTS.md` by itself: the prompt names it. When
  restarting a session, add what you found: its last landed step, its
  branch, and whether its worktree has uncommitted changes.
- **Slots.** Codex counts finished sub-agent threads against
  `max_threads`. Give each thread a slot name (`w1` to `w8`) and record it
  in the ledger's `slot` column. When a spawn is refused, reuse a finished
  slot with `followup_task`: say that its previous session is over, and
  give the new session's prompt.

### 7.3 Reports

An implementer reports when an increment lands, when it stops on a
question, and when its session ends or yields (section 8.6). After each
report:

1. Append it to `log.md`, as received, under a heading naming the
   repository, the session and the time.
2. Check it with git only: the commit is on `origin/main`, its `Step`
   trailer names the step, and the worktree is clean. Read no code, diff or
   test unless a check fails.
3. `plan set` the step's new state and commit, then `plan status`.
4. Start what the plan now allows, and resume the sessions waiting for
   what landed.

A report's question goes into `blocked.md` (section 10.2), unless it is
only a dependency not yet landed: that is not a block, and the session is
resumed when the dependency lands.

**A step's state** in the ledger is one of:
- `waiting`: something it needs has not landed;
- `ready`: it can start;
- `running`: its session is on it, with the slot and the branch;
- `blocked`: on an escalation, named in the note;
- `landed`: with its commit;
- `dropped`: by an instruction, named in the note.

### 7.4 Waiting

- **Between events, sleep about a minute** (Codex's `sleep`; an
  implementer's message ends it early).
- **At every wake-up:** touch `heartbeat`, and look for `unblock.md` and
  `steer.md`.
- **Every 15 minutes:** check memory and disk (section 11.4).
- **A finished implementer** ends its turn with its last report. Give a
  session that yielded its next step with `followup_task`.
- **The user is reached only through the designer.** The coordinator uses
  no other channel to the user.

### 7.5 Keeping state

- **`context.md` is the coordinator's memory.** Rewrite it whenever the
  state changes: the slot map, the sessions running and their last step,
  open escalations, and decisions in force from answers and steers that
  the plan does not yet say. Write full sentences.
- **After a compaction or a restart,** re-read this section, `context.md`
  and the ledger before acting.

### 7.6 Pausing, resuming, ending

- **Pause** at the user's or the designer's request. Start nothing new,
  and let running increments land or stop at a clean point. Write
  `paused.md`: each session's worktree, branch, head and last landed step.
- **Resume after a restart.**
  1. Read `context.md`, the ledger and the end of `log.md`. Append
     `paused.md`, if there is one, to `log.md`, and delete it.
  2. Check with git: each worktree's branch, whether it is clean, and
     whether its work has landed.
  3. Run `plan check`.
  4. Restart each session that was running, with what you found. A
     worktree with uncommitted changes belongs to a session that stopped
     mid-increment: its implementer inspects them, then finishes or
     discards them. The coordinator never discards them itself.
- **End** when every step has landed or been dropped.
  1. Write `done.md`: the plan's "Done when", checked against the ledger.
  2. Remove the worktrees.
  3. `plan release`.

  The designer then marks the plan `done <date>` in `STATUS.md`.

## 8. Implementing: the implementer's card

### 8.1 Each increment

1. **Read the increment** in the session file, and the design sections its
   "Read first" names. Read only those, not the whole design.
2. **Branch from `origin/main`** in your worktree:
   `git fetch origin && git switch -c <plan>/<repository>-<NN>-<item>
   origin/main`.
3. **Implement it with its tests,** following the repository's `AGENTS.md`
   and skein's `programming-model.md` and `testing-strategy.md`.
4. **Land it** with `land` (section 8.5), then report (section 8.6).

The code graph indexes the primary checkouts, which are on `main`: use it
for structure, and confirm every name it gives in your worktree.

### 8.2 Rules

- **The design is the contract.** Never edit `docs/design/`, the plan or
  the run's files other than your checkpoint. Where the code seems to
  need a different design, stop (section 8.7).
- **Implementation choices are yours:** names, module placement, values
  within the presets' ranges. Report them; do not ask.
- **Work only in your worktree.** Never touch a primary checkout or another
  session's worktree.
- **No `git stash`:** the stash is shared by every worktree of a
  repository. Commit work in progress on your branch instead.
- **Upstream first** (section 9.5).
- **Removing a mechanism removes all of it:** code, limits, accounting,
  facts, tests and docs.
- **Each test lands with the code it covers.**

### 8.3 Heavy commands

`heavy`, `land` and `plan` are on your `PATH`, from skein's
`scripts/dev/`.

- **`heavy <command>`** runs what compiles, and the tests of the crates you
  touch: `cargo check`, `clippy`, `build`, `doc`, `nextest -p …`. It uses
  the build lane.
- **`heavy --gate <command>`** runs, alone on the machine, whole suites,
  measurements (with the profiles and flags your repository's workflow
  documents, such as `--profile measure -j 1`), probes and benchmarks.
  `land` runs the gate itself.
- **Light commands run directly:** git, `cargo fmt`, reading, `rg`.
- **Never set `CARGO_BUILD_JOBS` or `NEXTEST_TEST_THREADS`,** nor pass
  `-j` beyond what the workflow documents: the lanes set the width.
- **Never nest them:** `heavy land`, or `heavy` inside a heavy command,
  waits for itself.
- **Read a command's log from the path `heavy` prints.** Never dump
  `pueue status`. If your tool call times out, the task runs on: `heavy`
  printed its id first, and `pueue wait <id>` and `pueue log <id>` reach
  it.

### 8.4 Commits

```
<Subject, imperative, at most 72 characters>

<What changed and why. The design section it follows. Where it departs
from a sketch, how and why. The evidence the repository asks for beyond
the gate.>

Plan: <plan>
Step: <repository>-<NN>.<item>
```

`land` adds the trailers `Gate-Tree` and `Gate-Result`. A commit that
landed without its body is never rewritten; the body goes in the report.

### 8.5 Landing

Run `land` from your worktree (section 9). By its exit status:

- **2, refused:** fix what it names. "Nothing to land" after a 5 means the
  commit had landed.
- **3, conflicts:** resolve them against the design, never by dropping
  another session's change. Then `git rebase --continue`, and `land` again.
  If resolving needs a decision, stop (section 8.7).
- **4, the gate failed:** fix it, and `land` again.
- **6, main kept moving:** `land` again a little later.
- **5, 7 or 8:** report it, and stop.

### 8.6 Reports and the checkpoint

**When to message the coordinator:** when an increment lands, when you stop
on a question, and when the session ends or yields. Send nothing else; in
particular, no progress messages. After a landing, send the report and go
on. When the session is done, or nothing is left that does not wait
(it yields), end your turn with the report as your final answer. Use this
shape, in full sentences:

```
Session: <repository> <NN>
Step: <step>, <landed | question | blocked | done | yielded>
Commit: <full hash> on origin/main, parent <full hash>
Gate: <the Gate-Result trailer>
Evidence: <runs, "none: deterministic tests cover it", or "pending: …">
Next: <the next step, or what it waits for, by capability>
Questions: <none, or each question with the design text quoted, the
options, your proposal, and what you continue with meanwhile>
Agreements: <none, or what you agreed with another session>
Worktree: <clean, or what is in it>
```

**The checkpoint.** After each report, overwrite your checkpoint,
`sessions/<repository>-<NN>.md`, with the same facts and with what is in
progress. A restarted implementer continues from it.

### 8.7 When to stop

- **Stop only for a real gap:** the design, `decisions.md` and the session
  file are silent or contradict each other, and the choice changes
  behaviour that something outside the code can observe. Quote what they
  say.
- **A dependency not landed yet is not a block.** Do the increments that
  do not need it, then report what you wait for.
- **While a question is open,** go on with whatever does not depend on it.

## 9. Landing

### 9.1 What `land` does

From a clean worktree on the branch to land, `land`:

1. rebases the branch on `origin/main`;
2. runs the repository's gate, `scripts/check.sh`, through `heavy --gate`;
3. writes the evidence into the tip commit's trailers;
4. pushes the tip to `origin`'s `main`;
5. moves the local `main`, and the checkout that has it, to the tip.

A push only fast-forwards, so it is the lock. If it is rejected, main
moved, and `land` starts again from the rebase. It gives up after five
tries. Its exit status:

| Status | Means |
|---|---|
| 0 | landed |
| 2 | refused before starting, or nothing to land |
| 3 | the rebase stopped on conflicts, left in progress |
| 4 | the gate failed |
| 5 | the local `main` could not follow: with a push, the commit landed on `origin/main`; with `--local`, nothing landed |
| 6 | main kept moving |
| 7 | fetching or pushing failed |
| 8 | another command failed |

### 9.2 Evidence

- **`Gate-Tree`** is the tree the gate ran on. **`Gate-Result`** gives the
  test count and time of each suite, in the order the gate runs them, and
  the time the gate ran alone.
- **Amending the message keeps the tree,** so the evidence holds for what
  lands.
- **A tree already gated is not gated again,** with the same gate and the
  same compiler. `land` keeps the results under `~/src/rust/tmp/gates/`.
- **Anyone can check the evidence:** the landed commit's tree equals its
  `Gate-Tree`.

### 9.3 Conflicts

A rebase that stops on conflicts is left in progress, for the branch's
author to resolve. `land` exits 3. An implementer resolves its own
conflicts (section 8.5), and escalates only when resolving them needs a
decision.

### 9.4 Markdown only

`land --docs` checks that the branch changes only Markdown files, and
skips the gate. It is how designs, plans and `STATUS.md` land. Rust doc
comments are code, and are gated.

### 9.5 Pins across repositories

- **Dependencies on other repositories of the workspace** are git
  dependencies on their GitHub URLs, pinned by `Cargo.lock`.
- **Pin only revisions on the upstream `origin/main`.** A downstream
  increment waits until the upstream change has landed.

### 9.6 The local main

- **The local `main` only follows `origin/main`.** `land` fast-forwards
  it, and the checkout that has it, after each push.
- **If the local `main` has commits that `origin` lacks,** `land` pushes
  but cannot follow, and exits 5. The coordinator gives an implementer the
  reconciliation: rebase those commits on `origin/main`, then land them.
- **Never force-push, and never move `main` backwards,** without the
  user's approval.
- **A repository that does not push yet** lands with `land --local`: on
  the local `main`, under a lock, with no push.

## 10. Escalation

### 10.1 From the implementer

An implementer stops on a real gap (section 8.7) and reports it to the
coordinator, in a Codex message, with the question in the report's
shape (section 8.6).

### 10.2 From the coordinator: `blocked.md`

The coordinator writes the escalation to `blocked.md` in the run's
directory, then keeps everything else going.

```
# Blocked

## <repository> <NN>.<item>: <topic>, <UTC time>

- Kind: design gap | plan defect | cross-session scope | infrastructure |
  credentials
- Since: <time>. Still running: <sessions>.
- Needed: <the decision, in one sentence>.

<The report, as received.>
```

- **One file.** A new escalation while one is open goes under a new
  heading in the same file.
- **The coordinator never answers** a design question itself, and never
  writes `unblock.md`.

### 10.3 From the designer: `unblock.md` and `steer.md`

```
# Unblock

Answer to the blocked report of <time> (<headings answered>).

## Decision

## Landed

<Design and plan commits, with their hashes.>

## Instructions

1. …
```

- **When the coordinator finds `unblock.md`:**
  1. it appends both files to `log.md`, under "Blocked and unblocked,
     <time>";
  2. it deletes `unblock.md`, and removes from `blocked.md` the headings
     the answer names, deleting the file when none is left;
  3. it follows the instructions, which take precedence over the plan
     where they differ;
  4. it updates `context.md`.
- **`steer.md`** is an instruction nobody asked for: a plan amended, a
  pause, a rule changed. The coordinator handles it the same way.

### 10.4 What the designer decides

- **A design gap.** The designer decides, amends the design with `land
  --docs`, then amends the session files that mention the point.
- **A plan defect.** It fixes the plan. When the same kind of block comes
  back, it adds a standing rule: to the plan's README, or to this
  document if the rule is general.
- **What goes to the user:**
  - decisions with long-term consequences that would be hard to change;
  - any language other than Rust;
  - what the user reserved.

  A question for the user goes into `decisions.md`, under "For the user".
  The run blocks on it only if no work can continue without it.

### 10.5 The designer's watch

- **The watch runs on the run's machine.** The designer runs `plan watch`
  in the background (in Claude Code, a Bash command with
  `run_in_background`). It ends, saying why, when:
  - `blocked.md` has headings it has not shown, and no answer is pending;
  - the coordinator's heartbeat is older than ten minutes;
  - the run pauses or ends.
- **After each answer,** the designer runs the watch again.
- **When the run starts and ends,** the designer marks the plan `active`
  and then `done` in `STATUS.md` (section 3.3).
- **When the designer's context grows past about half its window,** a
  fresh designer session takes over. It reads `blocked.md` first: the
  watch does not show again what it has shown. This document, the plan,
  `decisions.md` and `log.md` are enough to go on.

### 10.6 Stopping the line

**Stop the line** for one repository, or for everything, when:
- a gate cannot be brought back within its budget;
- a design contradiction blocks more than one session;
- an upstream change breaks a downstream gate beyond its session's scope;
- the queue or its daemon fails, or a lane stays paused with nothing
  holding it (`plan check` shows it, `--fix` starts it);
- memory or disk runs short (section 11.4);
- credentials fail, which pauses every session using them;
- reconciling `main` would need a force-push.

**To stop the line:**
1. start nothing new in its scope;
2. let running increments land or stop at a clean point;
3. write `blocked.md`;
4. wait for the answer.

## 11. Build infrastructure

### 11.1 The queue

- **pueued runs as a systemd user service,** never from an agent's command:
  a daemon started by an agent dies with it. Lingering
  (`loginctl enable-linger`) keeps it running when the user logs out.
- **Two lanes:**
  - **gate** (parallelism 1) runs gates, whole suites, measurements,
    probes and benchmarks. A gate task pauses every other pueue group,
    waits for their running tasks, runs alone, then resumes them, so
    suite times are measured on a quiet machine;
  - **build** (parallelism 2) runs everything else, each task with
    `HEAVY_BUILD_JOBS` (4) cargo jobs.
- **A gate task killed outright** (`pueue kill`) cannot resume what it
  paused. `plan check --fix` starts a paused lane again.
- **`heavy` is the only way in.** Nobody changes a lane's parallelism,
  except as section 11.4 says.

### 11.2 Logs

`heavy` writes each command's whole output to the run's `logs/` and
prints only its tail and the log's path. It also appends a line to
`logs/heavy.tsv`: the task's id, its lane, when it ended, the seconds it
waited and ran, its exit status, the memory available as it ended (GB),
its directory, its log and its command. Then it removes the task from
pueue, which keeps the daemon's state small.

### 11.3 The cache

- **kache is the global `rustc-wrapper`.** Its keys do not depend on
  paths, so a fresh worktree reuses every crate that did not change.
- **It gains most when worktrees are new,** and little on the crates a
  session keeps editing.
- **Mind its store.** Watch its size against its cap (`kache report`).
- **`cargo clippy --fix` fails on its read-only outputs.** Fix lints by
  hand, or run with `RUSTC_WRAPPER=` set empty.

### 11.4 Resources

- **Each session's `target/` takes 1 to 8 GB.**
- **The coordinator checks** available memory (`free -g`) and disk (`df -h
  ~/src`) before each start, and every 15 minutes.
- **Below 6 GB of memory or 40 GB of disk,** it holds new starts.
- **Below 3 GB of memory,** it narrows the build lane to one task (`pueue
  parallel 1 --group build`), and widens it again above 6 GB. It says so in
  `log.md` each time.
- **Below 1.5 GB of memory or 15 GB of disk,** it stops the line.
- **To free disk,** it may remove the `target/` of a session that is not
  running.

### 11.5 Budgets

The suites' budgets are wall time on the reference machine: 8 threads,
15 GB of memory, through the gate lane. Gate times on another machine name
that machine.

## 12. Several machines

- **One run per machine.** `~/src/rust/tmp/active-plan` names it.
  Several machines run different plans at once; a plan's `STATUS.md` row
  keeps a second machine from claiming it (section 6).
- **The designer of a run works on the run's machine,** where its files
  are.
- **A new machine** clones the repositories into `~/src/rust/` and runs
  `skein/scripts/dev/plan check --fix`. It links the tools and the
  workspace's files, sets up the queue, and names what is still missing.
- **Shared:** the GitHub repositories, and with them the designs, plans and
  `STATUS.md`. **Per machine:** runs, the queue, the cache, worktrees and
  credentials.
- **Progress is visible from any machine.** A plan's `STATUS.md` row says
  where it runs, and every landed step's `Plan` and `Step` trailers are on
  `origin/main`.
- **Plans running at once avoid each other's hotspots.** When two must
  share one, the second plan's README names the first.
- **Another machine landing on the same repository** is, to `land`, main
  moving: rebase, gate again, push.

## 13. Open questions

- **The build lane's width.** Two tasks of four jobs each is untested
  under a real run's load. The next run measures memory and the queue
  (`logs/heavy.tsv`) and settles it.
- **Gating again when main moves** is most of the queue's time. A single
  lander that rebases and gates several branches in turn could cut it.
- **A doorbell for the coordinator.** `codex queue --thread <id>` could
  deliver `unblock.md` at once, instead of the coordinator's minute of
  polling. Untested.
- **Closing finished Codex threads,** instead of reusing slots, if the
  runtime allows it.
- **Credentials under load.** Does a run's work that borrows the Codex
  login (benchmarks, probes) put the coordinator's own session at risk?
  A `CODEX_HOME` of their own for such work is a candidate answer.
- **Pushing from Codex sub-agents** is untested: their sandbox and network
  settings must allow `land` to push.
