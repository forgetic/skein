# Agent guidance for ~/src/rust

This directory holds the Rust projects, each a git repository with its own
`AGENTS.md`, and the machine's working files in `tmp/`. Read a
repository's `AGENTS.md` before touching its code.

This file is skein's `scripts/dev/workspace/AGENTS.md`, linked into
`~/src/rust/`: change it there, and land it in skein.

## The projects

| Repository | What it is | Builds on | Its gate |
|---|---|---|---|
| `skein` | the io and protocol kit, and the foundation documents | — | `scripts/check.sh` |
| `smith` | the agent kit | skein | `docs/development/workflow.md`, section 1 |
| `temper` | the application | skein, smith | `docs/development/workflow.md`, section 1 |
| `temper/jig` | the kit carved out of temper, built in temper's repository until it moves | skein | temper's |

Dependencies point upstream only: skein names neither smith nor temper,
smith never names temper, and jig never names temper.

## The process

skein's `docs/foundation/development.md` says how work goes from an idea
to `main`: the roles (designer, planner, coordinator, implementer), where
designs, plans and runs live, how increments land, and how a run
escalates. Its tools are in `skein/scripts/dev/`: `heavy`, `land` and
`plan`.

- **The design** of each project is in `<repository>/docs/design/`, with
  drafts in `docs/design/draft/`. Only the designer edits it.
- **How far each project is from its design** is in
  `<repository>/docs/design/STATUS.md`.
- **Plans** are in `docs/plans/<plan>/` of the most downstream repository
  they change.
- **A run's state** is in `tmp/<repository>/<plan>/`, and
  `tmp/active-plan` names this machine's run.

## Starting things

`heavy`, `land` and `plan` are skein's `scripts/dev/` tools, linked into
`~/.local/bin`.

**A new machine.** Clone skein, smith and temper from
`git@github.com:forgetic/` into `~/src/rust/`, then run
`skein/scripts/dev/plan check --fix`. It links the tools, this file, its
`CLAUDE.md` and the coordinator's Codex profile, sets up the queue, and
names what is still missing (pueue, kache, jq, cargo-nextest, the git
settings below).

**A design.** Start Claude (Opus 5.5, effort xhigh) in `~/src/rust`, and
say:

> Design: <the idea>. Work as the designer of skein's
> docs/foundation/development.md, sections 2.1 and 4.

**A plan.** In the designer's session or a fresh one:

> Plan <the design documents, or the STATUS.md rows> as development.md,
> section 5, says, in <repository>/docs/plans/<name>/.

**A run.** Run these from `~/src/rust`:

1. `plan check --fix <repository>/docs/plans/<name>`, until it passes.
2. `plan claim <repository>/docs/plans/<name>`
3. Start the coordinator:

   ```
   codex -p coordinator "Coordinate the run of <repository>/docs/plans/<name>,
   as skein's docs/foundation/development.md, section 7, says."
   ```

4. Then, in the designer's Claude session, on the same machine:

   > The run of <name> has started: mark it active and watch it, as
   > development.md, sections 6 and 10.5, say.

**Where things stand:**
- a run: `cat tmp/<repository>/<plan>/status.md`, or `plan status`;
- a project: `<repository>/docs/design/STATUS.md`.

## Until the reliability plan ends

The reliability plan, started before development.md, runs on
`debian-16gb-hel1-1` from `~/src/rust/plans/reliability-plan/`, under its
own rules: its own `heavy` wrapper and queue group, its merge lock, and
landings on that machine's local `main` of skein and smith, without a
push. That `main` is pushed to GitHub from time to time. Until the plan
ends:

- **Other machines do not land in skein or smith,** designs and plans
  included: a landing there would split GitHub's `main` from that
  machine's. temper and jig are free.
- **On `debian-16gb-hel1-1`:**
  - no other run starts: `plan check` fails on purpose, since smith's
    primary checkout stays on `bench/smith-codex-2026-10-08`, as the
    plan's sessions expect;
  - in skein and smith, land with `land --local` under the plan's merge
    lock, `LAND_LOCK=~/src/rust/<repository>/.git/reliability-merge.lock`,
    then push `main` under the same lock: `flock <lock> git -C
    ~/src/rust/<repository> push origin main refs/notes/commits`;
  - a gate-lane task pauses the plan's queue group too while it runs: keep
    them few.
- **smith and temper have no `scripts/check.sh` yet.** A code change there
  gives `land` its workflow's section 1 as the gate: `land --gate '<the
  four commands, joined with &&>'`.
- **Cargo's git URLs still name `git.ekanayaka.io`.** Every machine
  rewrites them to GitHub, and cargo fetches with git:
  - `git config --global url.git@github.com:forgetic/.insteadOf
    https://git.ekanayaka.io/ai/`
  - `[net] git-fetch-with-cli = true` in `~/.cargo/config.toml`.

When the plan ends:
- the Cargo git URLs move to GitHub;
- smith's primary checkout returns to `main`, and every landing pushes;
- smith and temper get `scripts/check.sh`;
- the push rules in the repositories' `AGENTS.md` become "land pushes";
- the plan moves to `smith/docs/plans/reliability/`.
