# Contained process trees

Provisional, 2026-10-07. io spawns one child and signals it (io.md,
section 6). A contained tree is a child and everything it starts:

- held whole;
- seeing the file system through a view in which only named directories
  are writable;
- stopped as a whole;
- proved empty.

Services that run commands they do not trust need this: smith's agent for
its tools and checks, and its hosts for the agent processes they spawn.
This document says what a tree guarantees. How io provides it is decided
when trees are built (section 7).

## 1. In one page

- **Held whole.** Whatever a tree's processes start stays in the tree,
  double forks included.
- **A view of the file system:**
  - the directories it is given are where it sees them, each writable or
    not;
  - named paths within them, such as a repository's git directory, are
    read-only;
  - everything else is read-only, with a private temporary directory.
- **An environment given whole.** Nothing is inherited from the service:
  no variable and no descriptor beyond the pipes asked for.
- **Stopped in three steps,** each the owner's choice of when:
  - politely, a signal to the tree's first process;
  - terminated, the same signal to every process in it;
  - killed, all at once.
- **Proved empty.** A tree is gone only once it holds no process, as the
  kernel reports. Only then is what is left of it closed.
- **Nested.** A process in a tree may contain trees of its own within it,
  which are held, stopped and proved empty with it.
- **Deadlines are the owner's,** as everywhere in io: for the spawn, and
  for each step of stopping.

## 2. In skein

- **io gains trees** beside children: the same pipes as streams, the same
  slab, and one terminal each.
- **What the machine must allow** follows from how trees are built. A
  service checks it at startup, and refuses to start without it.

## 3. Spawning a tree

The owner gives:

- **the program,** its arguments and its environment;
- **the working directory,** a path within the view;
- **the view:**
  - each directory, as a host path and the path the tree sees it at,
    writable or not;
  - the paths inside them that stay read-only;
- **the pipes,** as for a child;
- **limits,** if any: memory and the number of processes.

io answers once:

- **spawned,** with the tree and its pipes, once the command is executing in
  its view;
- **or failed,** saying what failed: holding the tree, its view, or the
  command itself.

## 4. Stopping and the end

| Step | What io does |
|---|---|
| polite | sends the termination signal to the first process |
| terminate | sends the termination signal to every process in the tree |
| kill | kills every process in the tree at once |

- **The first process's exit** goes up with its status. The tree may still
  hold others.
- **Empty** goes up when the tree holds no process. After it, io closes
  what is left, and the tree's terminal, closed, comes once that has
  settled.
- **Closing a tree** that is not empty kills it first, then settles the
  same way.

## 5. Limits and the worst case

- **Trees** share io's entity slab with children and their pipes. A tree
  takes one slot, plus one per pipe.
- **A view** is bounded in directories and in read-only paths.

## 6. Testing

- **The simulator** gains trees:
  - spawned, exiting first-process-first or all at once;
  - processes that ignore the polite signal or the termination;
  - a tree that forks after a signal;
  - empty reported late.
- **The fake machine** keeps a tree's view, so a world can check that a
  command wrote only where its view allows.
- **Conformance tests** run the same cases on the kernel: double forks stay
  held, a git directory is read-only inside a writable one, and a kill
  empties the tree.

## 7. Open questions

- **How io holds a tree and builds its view:** cgroups and mount namespaces
  are the likely means. What each asks of the machine, and any fallback
  where the machine does not allow it, is decided when trees are built.
- **The network:** whether a tree has a network by default, with the owner
  allowing it per tree.
- **Trees in one process:** smith's agent in its host's process, where each
  run's commands could share a tree per run (smith's `domain/host.md`,
  section 12).
