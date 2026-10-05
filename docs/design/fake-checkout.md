# The fake checkout

## 1. Ownership and use

`skein-fake-checkout` is a dependency-free ordinary-Rust test kit
(programming-model.md, section 10.2; testing-strategy.md, section 4.3).
Its initial implementation was extracted without behavioral changes from
`temper` commit `246164f`, `tests/fake-checkout/src/{lib,git}.rs`, when
smith became its second user. The source's historical `.git/temper-*`
bookkeeping names remain part of this compatibility extraction.

It keeps byte-path files, directories, links and special nodes, registered
roots, fresh file versions, command scripts and local git metadata. It
performs no host IO and knows no service's domain types, grants, remote
policy, clock, deliveries or cancellations. A world translates its own
boundary records and decides when these synchronous mechanics run.
Service-specific fakes and worlds remain owned by the service. This kit
is separate from `skein-fake-machine`, whose inode, permission and open
handle model answers the simulated kernel.

Fixtures own all sizes: roots, nodes, contents, scripts and commit graphs.
There is no production `worst_case`, concurrency, allocator or global
clock here. Ordered maps and injected scripts make the same calls replay
identically; overflow checks remain enabled in every workspace profile.
Invalid fixture setup and unknown root names are client assertions, not
service refusals.

## 2. Files and searches

Fixture paths are global byte paths without a leading slash; the empty
path is the root. `mkdir`, `write`, `link`, `special` and `remove` lay out
or change nodes directly, without following links. A write creates missing
parents and issues a new version even for identical content; versions are
never reused. Root registration makes directories and issues a name;
registrations are not closed or removed with their nodes.

`exists`, `content`, `version`, `files` and `tree` are pure queries without
link following. `files` borrows all regular files; `tree` copies regular
files strictly beneath a path, including git metadata. `replace_tree`
removes descendants except git nodes at every depth and their ancestors,
then writes supplied relative entries with fresh versions. Fixture tree
paths must be valid, and replacements incompatible with node kinds assert.

`load`, `scan`, `store` and `search` resolve beneath a registered root.
Resolution skips empty and dot components, rejects parent traversal above
that root and absolute link targets, and follows at most 40 relative
links. The input path itself is not separately checked for a leading
slash. This is no full POSIX model: no permissions, path/name length
limits, open handles or NUL validation. A service's adapter owns its own
input validation.

A load returns owned content and version only within its content byte cap.
A scan returns the first immediate entries in byte-name order and counts
all omitted entries; entry links are classified, not followed. A store
follows no link component, including the last. Its expected absence or
regular-file version is compared before missing parents are installed.
A refusal leaves nodes and versions unchanged; success issues a fresh
version. The refusal enum distinguishes missing/nonregular paths,
escapes, links, excessive link resolution, oversized content and actual
version conflicts.

Search matches literal bytes, ordered by path and one-based line number,
skipping hidden descendants and following no descendant links. Its
optional glob is a name suffix with an optional leading `*`. An unclosed
`(` yields a fixed diagnostic; this is not a regex implementation. The
requested hit count and aggregate retained path/text bytes bound the
result. The final text can be a prefix; remaining matches are counted.
Searching a regular file returns an empty relative path for its hits.

## 3. Scripted commands

`program` installs a script by exact command bytes. `spawn` resolves a
working directory, clones that script and captures registered root paths
and write permissions. The builtin `env` prints exactly the supplied
entries; an unknown command produces a short script with exit code 127.
Durations, output and code/signal exits are values for the world: no time
advances and no process runs in this kit.

`finish` applies script changes in order. Only the deepest captured root
containing a change can authorize it; that root must be writable. Any
`.git` component in any ASCII case is protected. Allowed changes write
regular files or remove subtrees; invalid fixture replacements assert.
There is no terminal state or exactly-once fence: calling `finish` twice
can write twice and issue more versions. Worlds own timing, partial
application cuts, cancellation, output truncation and terminal ledgers.

## 4. Local git and its remote boundary

The caller implements `git::Remote`: synchronous repository tip queries,
fetch selection, branch creation, push policy, and a commit store queried
for both parents and trees. Remote names and fault policy are opaque to
the kit. IDs increase topologically in a finite acyclic graph; every
queried commit exists. Nonmerge store returns `None` for an unchanged
tree. Merge store always returns a fresh ID with both supplied parents,
even for an unchanged tree. Local operations never move references.

Clone requires an absent destination and imports both parent chains of
all branch tips, without checking out files. Fetch imports only missing
objects in both parent chains, retaining existing object versions.
Checkout requires a local object, replaces non-git files and clears merge
metadata. Commit requires its local parent and snapshots regular files
less git paths; unchanged content makes no object. Removing a working
directory removes all its local object-presence markers.

Merge requires checked-out head metadata and a local second parent. It
chooses the greatest-ID common ancestor, sufficient for the small
single-base histories tested here; it does not implement recursive Git
merge bases. Independent line edits combine; overlapping edits and
incompatible additions/deletions produce markers and a sorted conflict
list. Line comparison uses quadratic scratch in the changed line counts.
An explicit merge commit checks marker lines only in originally conflicted
paths; deletion resolves one. It records both parents and clears merge
metadata on success. Missing inputs or unresolved conflicts refuse before
creating an object or changing that metadata.

Local markers are `.git/objects/COMMIT` with decimal IDs, checked-out head
is `.git/temper-head` with eight little-endian bytes, second parent is
`.git/MERGE_HEAD`, and conflict paths are values beneath
`.git/temper-conflicts/INDEX`. Preserve these bytes across legacy adoption.

Push asserts local object presence, then delegates with `None` for the
old-head condition. `push_expected` forwards `Some(expected)` unchanged.
The remote enforces reference policy and exact old-head matching;
rejection/refusal leaves the reference where it was. Branch creation is a
direct delegation and does not inspect the local checkout. Transport
latency, retries, authorization and remote fault scheduling stay in the
world, not this crate.

## 5. Coverage and budgets

Focused leaf tests exercise contracts previously covered by consuming
worlds: versioned stores and nonmutation, link/root refusals, sorted
bounded scans/searches, command script isolation and protected writes,
tree replacement, ancestry, local-only commits, merges and conditional
push forwarding. The remote fixture is private and deliberately small;
it tests this boundary rather than implementing a reusable forge.

The fuzzy binary sweeps small independent line edits and conflicts, checks
expected trees/parents/object presence and compares the complete checkout
and remote fixture on replay. Existing services retain their worlds and
referees. New tests run under the existing focused 15-second and fuzzy
one-minute suites (testing-strategy.md, section 8). Timings are measured
at the integration gate; this extraction makes no unmeasured budget claim.
