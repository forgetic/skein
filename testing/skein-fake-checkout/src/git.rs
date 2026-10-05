//! Local git mechanics over `Checkout` (fake-checkout.md, section 4).
//! Keeps object-presence, checked-out head and merge-conflict metadata in the
//! checkout; commit graphs and trees live in the caller-supplied `Remote`.
//! Never knows a service's repository policy, transport, authorization, clock
//! or delivery lifecycle (testing-strategy.md, section 4.3).
//!
//! Clone/fetch bring both parent chains; checkout replaces non-git files;
//! commit snapshots them without moving a remote reference. Merge combines
//! small single-base histories and writes conflict markers; an explicit merge
//! commit records both parents even for an unchanged tree. Push delegates
//! policy and expected-head checks to `Remote`. All calls return synchronously.
//!
//! The fixture owns graph/tree sizes and acyclic increasing commit IDs.
//! Merge chooses the greatest common ID and allocates quadratic line-diff
//! scratch, so this is no full Git implementation or production memory bound
//! (programming-model.md, section 10.2; fake-checkout.md, section 4).

use std::collections::{BTreeMap, BTreeSet};

use crate::{Checkout, in_git};

/// Owned regular-file bytes keyed by path relative to a working-tree root.
/// Fixture supplies valid paths and sizes (fake-checkout.md, section 4).
pub type Tree = BTreeMap<Vec<u8>, Vec<u8>>;

/// World-supplied synchronous remote/store boundary. The kit forwards
/// repository and branch bytes; the implementation owns policy, availability
/// and fault answers. Commit IDs increase topologically in a finite acyclic
/// graph; all queried IDs must exist. No delivery state or clock is held here
/// (fake-checkout.md, section 4).
pub trait Remote {
    /// Clone asks for all repository branch tips, or a remote refusal. No
    /// checkout mutation occurs before this returns (fake-checkout.md, section 4).
    fn heads(&mut self, remote: &[u8]) -> Result<Vec<u64>, Fault>;

    /// Fetch asks for the selected tip, or a remote refusal. The implementation
    /// resolves the selector; the kit imports its missing ancestry afterward
    /// (fake-checkout.md, section 4).
    fn fetch(&mut self, remote: &[u8], want: Want<'_>) -> Result<u64, Fault>;

    /// Caller delegates branch creation at `commit`; an existing branch stays
    /// where it is. Implementation checks repository/commit policy
    /// (fake-checkout.md, section 4).
    fn create(&mut self, remote: &[u8], branch: &[u8], commit: u64) -> Result<Created, Fault>;

    /// Caller delegates push policy. `None` supplies no old-head condition;
    /// `Some` requires that exact old head in addition to the implementation’s
    /// fast-forward rules. Rejection/refusal must leave the reference unchanged
    /// (fake-checkout.md, section 4).
    fn push(&mut self, remote: &[u8], branch: &[u8], commit: u64, expected: Option<u64>) -> Result<Pushed, Fault>;

    /// Pure first-parent lookup of an existing commit; `None` for its root
    /// (fake-checkout.md, section 4).
    fn parent(&self, commit: u64) -> Option<u64>;

    /// Pure second-parent lookup; `None` for a nonmerge commit
    /// (fake-checkout.md, section 4).
    fn merge_parent(&self, commit: u64) -> Option<u64>;

    /// Pure owned tree lookup of an existing commit, bounded by the fixture
    /// (fake-checkout.md, section 4).
    fn tree(&self, commit: u64) -> Tree;

    /// Store the supplied tree with existing parent IDs. With no second parent,
    /// return `None` only when unchanged from `parent`; with a second parent,
    /// always issue a fresh ID recording both parents, even if unchanged.
    /// No remote reference moves (fake-checkout.md, section 4).
    fn store(&mut self, parent: u64, merging: Option<u64>, tree: Tree) -> Option<u64>;
}

/// Remote implementation’s terminal refusal to a synchronous call. The kit
/// propagates it without starting checkout changes (fake-checkout.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fault {
    /// Remote has no requested repository, branch or commit (fake-checkout.md, section 4).
    Missing(
        /// Missing remote name kind (fake-checkout.md, section 4).
        What,
    ),

    /// Remote refuses the requested operation (fake-checkout.md, section 4).
    Refused,

    /// Remote cannot be reached (fake-checkout.md, section 4).
    Unreachable,
}

/// Remote name kind that was not found (fake-checkout.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum What {
    /// Repository address (fake-checkout.md, section 4).
    Repository,

    /// Branch name (fake-checkout.md, section 4).
    Branch,

    /// Commit ID (fake-checkout.md, section 4).
    Commit,
}

/// Caller’s fetch selector; interpreted entirely by `Remote`
/// (fake-checkout.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Want<'a> {
    /// Named branch tip (fake-checkout.md, section 4).
    Branch(
        /// Borrowed branch bytes for this call (fake-checkout.md, section 4).
        &'a [u8],
    ),

    /// Specific commit ID (fake-checkout.md, section 4).
    Commit(
        /// Commit ID in the remote’s store (fake-checkout.md, section 4).
        u64,
    ),

    /// Remote’s default branch tip (fake-checkout.md, section 4).
    Default,
}

/// Remote’s branch-creation answer (fake-checkout.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Created {
    /// Absent branch was created at the requested commit (fake-checkout.md, section 4).
    Created,

    /// Branch already existed and was left unchanged (fake-checkout.md, section 4).
    Exists,
}

/// Remote’s push answer, including policy/expected-head rejection
/// (fake-checkout.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Pushed {
    /// Reference now names the requested commit (fake-checkout.md, section 4).
    Pushed,

    /// Policy or old-head condition rejected the push without moving the reference (fake-checkout.md, section 4).
    Rejected,
}

/// Required head metadata or local object is absent. Checkout/commit/merge
/// returns this before changing the working tree (fake-checkout.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NotFetched;

/// World adapter clones into an absent global directory: all commits
/// reachable from every tip through both parents, recorded as local objects,
/// without a checked-out tree. Remote failure leaves the checkout unchanged;
/// an existing destination asserts (fake-checkout.md, section 4).
pub fn clone_repository(
    forge: &mut impl Remote,
    checkout: &mut Checkout,
    remote: &[u8],
    at: &[u8],
) -> Result<(), Fault> {
    let heads = forge.heads(remote)?;
    let mut reached = BTreeSet::new();
    let mut pending = heads;
    while let Some(commit) = pending.pop() {
        if !reached.insert(commit) {
            continue;
        }
        pending.extend(forge.parent(commit));
        pending.extend(forge.merge_parent(commit));
    }
    assert!(!checkout.exists(at), "a clone goes where nothing is");
    checkout.mkdir(&[at, b"/.git"].concat());
    for commit in reached {
        checkout.write(&object(at, commit), b"");
    }
    Ok(())
}

/// World adapter imports the selected commit and missing ancestry through
/// both parents into `at`; existing object versions stay unchanged. Does not
/// check out files or move remote references. Remote refusal leaves checkout
/// unchanged (fake-checkout.md, section 4).
pub fn fetch(
    forge: &mut impl Remote,
    checkout: &mut Checkout,
    remote: &[u8],
    at: &[u8],
    want: Want<'_>,
) -> Result<u64, Fault> {
    let fetched = forge.fetch(remote, want)?;
    let mut pending = vec![fetched];
    let mut reached = BTreeSet::new();
    while let Some(commit) = pending.pop() {
        if !reached.insert(commit) || checkout.exists(&object(at, commit)) {
            continue;
        }
        checkout.write(&object(at, commit), b"");
        pending.extend(forge.parent(commit));
        pending.extend(forge.merge_parent(commit));
    }
    Ok(fetched)
}

/// World adapter forwards branch creation directly to `Remote`; no local
/// checkout or object-presence check (fake-checkout.md, section 4).
pub fn create(forge: &mut impl Remote, remote: &[u8], branch: &[u8], commit: u64) -> Result<Created, Fault> {
    forge.create(remote, branch, commit)
}

/// World adapter replaces non-git descendants with an existing local commit’s
/// tree, writes head metadata and clears merge metadata. Local object absence
/// returns `NotFetched` without mutation (fake-checkout.md, section 4).
pub fn check_out(forge: &impl Remote, checkout: &mut Checkout, at: &[u8], commit: u64) -> Result<(), NotFetched> {
    if !checkout.exists(&object(at, commit)) {
        return Err(NotFetched);
    }
    checkout.replace_tree(at, &forge.tree(commit));
    checkout.write(&[at, b"/.git/temper-head"].concat(), &commit.to_le_bytes());
    checkout.remove(&[at, b"/.git/temper-conflicts"].concat());
    checkout.remove(&[at, b"/.git/MERGE_HEAD"].concat());
    Ok(())
}

/// World adapter snapshots non-git regular files on a locally held parent.
/// Unchanged tree returns `None`; a new commit records local presence and head.
/// No remote reference moves; missing parent returns before mutation
/// (fake-checkout.md, section 4).
pub fn commit(
    forge: &mut impl Remote,
    checkout: &mut Checkout,
    at: &[u8],
    parent: u64,
) -> Result<Option<u64>, NotFetched> {
    if !checkout.exists(&object(at, parent)) {
        return Err(NotFetched);
    }
    let mut tree = checkout.tree(at);
    tree.retain(|path, _| !in_git(path));
    let Some(commit) = forge.store(parent, None, tree) else {
        return Ok(None);
    };
    checkout.write(&object(at, commit), b"");
    checkout.write(&[at, b"/.git/temper-head"].concat(), &commit.to_le_bytes());
    Ok(Some(commit))
}

/// World adapter forwards a locally held commit to `Remote::push` with no
/// old-head condition. The remote owns fast-forward and creation policy;
/// missing local object asserts (fake-checkout.md, section 4).
pub fn push(
    forge: &mut impl Remote,
    checkout: &Checkout,
    remote: &[u8],
    at: &[u8],
    commit: u64,
    branch: &[u8],
) -> Result<Pushed, Fault> {
    assert!(checkout.exists(&object(at, commit)), "a push is of a commit the working tree has");
    forge.push(remote, branch, commit, None)
}

/// Merge result after the working tree and merge metadata were written;
/// no commit or remote reference was created (fake-checkout.md, section 4).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Merged {
    /// Sorted relative paths requiring resolution, each holding conflict
    /// markers. Count/bytes are bounded by the fixture’s trees
    /// (fake-checkout.md, section 4).
    pub conflicts: Vec<Vec<u8>>,
}

/// World adapter merges checked-out head and locally held `theirs`, using
/// the greatest-ID common ancestor. Preserves git nodes, writes markers for
/// conflicts and records merge metadata. Missing head/object returns before
/// mutation; malformed head bytes assert. No commit/reference moves
/// (fake-checkout.md, section 4).
pub fn merge(forge: &impl Remote, checkout: &mut Checkout, at: &[u8], theirs: u64) -> Result<Merged, NotFetched> {
    let raw = checkout.content(&[at, b"/.git/temper-head"].concat()).ok_or(NotFetched)?;
    let ours = u64::from_le_bytes(raw.try_into().expect("checkout stores an eight-byte head"));
    if !checkout.exists(&object(at, theirs)) {
        return Err(NotFetched);
    }
    let ours_tree = forge.tree(ours);
    let theirs_tree = forge.tree(theirs);
    let base = common_ancestor(forge, ours, theirs).map_or_else(Tree::new, |commit| forge.tree(commit));
    let paths: BTreeSet<_> = base.keys().chain(ours_tree.keys()).chain(theirs_tree.keys()).cloned().collect();
    let mut tree = Tree::new();
    let mut conflicts = Vec::new();
    for path in paths {
        let old = base.get(&path);
        let left = ours_tree.get(&path);
        let right = theirs_tree.get(&path);
        let content = if left == right || right == old {
            left.cloned()
        } else if left == old {
            right.cloned()
        } else if let (Some(old), Some(left), Some(right)) = (old, left, right) {
            if let Some(merged) = merge_lines(old, left, right) {
                Some(merged)
            } else {
                conflicts.push(path.clone());
                Some(markers(left, right))
            }
        } else {
            conflicts.push(path.clone());
            Some(markers(left.map_or(&[], Vec::as_slice), right.map_or(&[], Vec::as_slice)))
        };
        if let Some(content) = content {
            tree.insert(path, content);
        }
    }
    checkout.replace_tree(at, &tree);
    checkout.write(&[at, b"/.git/MERGE_HEAD"].concat(), &theirs.to_le_bytes());
    checkout.remove(&[at, b"/.git/temper-conflicts"].concat());
    for (index, path) in conflicts.iter().enumerate() {
        checkout.write(&[at, format!("/.git/temper-conflicts/{index}").as_bytes()].concat(), path);
    }
    Ok(Merged { conflicts })
}

/// Merge commit refusal before creating an object or changing metadata
/// (fake-checkout.md, section 4).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CommitFailure {
    /// One of the supplied parents is not locally held (fake-checkout.md, section 4).
    NotFetched,

    /// Originally conflicted paths still contain marker lines (fake-checkout.md, section 4).
    Unresolved {
        /// Relative paths still containing marker lines, in recorded conflict
        /// order; fixture-sized (fake-checkout.md, section 4).
        files: Vec<Vec<u8>>,
    },
}

/// World adapter commits the current non-git tree with two supplied locally
/// held parents, even if unchanged. Only originally conflicted paths are
/// checked for marker lines; deleting one resolves it. Success writes object
/// presence/head and clears merge metadata, without moving references.
/// `Remote::store` must return a merge ID or this asserts
/// (fake-checkout.md, section 4).
pub fn commit_merging(
    forge: &mut impl Remote,
    checkout: &mut Checkout,
    at: &[u8],
    parent: u64,
    merging: u64,
) -> Result<u64, CommitFailure> {
    if !checkout.exists(&object(at, parent)) || !checkout.exists(&object(at, merging)) {
        return Err(CommitFailure::NotFetched);
    }
    let files: Vec<_> = checkout
        .tree(&[at, b"/.git/temper-conflicts"].concat())
        .into_values()
        .filter(|path| checkout.content(&[at, b"/", path].concat()).is_some_and(has_markers))
        .collect();
    if !files.is_empty() {
        return Err(CommitFailure::Unresolved { files });
    }
    let mut tree = checkout.tree(at);
    tree.retain(|path, _| !in_git(path));
    let commit = forge.store(parent, Some(merging), tree).expect("a merge always records both parents");
    checkout.write(&object(at, commit), b"");
    checkout.write(&[at, b"/.git/temper-head"].concat(), &commit.to_le_bytes());
    checkout.remove(&[at, b"/.git/temper-conflicts"].concat());
    checkout.remove(&[at, b"/.git/MERGE_HEAD"].concat());
    Ok(commit)
}

/// World adapter forwards a locally held commit and `Some(expected)` old
/// head to `Remote::push`. The implementation owns conditional fast-forward
/// policy; missing local object asserts (fake-checkout.md, section 4).
pub fn push_expected(
    forge: &mut impl Remote,
    checkout: &Checkout,
    remote: &[u8],
    at: &[u8],
    commit: u64,
    branch: &[u8],
    expected: u64,
) -> Result<Pushed, Fault> {
    assert!(checkout.exists(&object(at, commit)), "a push is of a locally held commit");
    forge.push(remote, branch, commit, Some(expected))
}

fn ancestors(forge: &impl Remote, start: u64) -> BTreeSet<u64> {
    let mut reached = BTreeSet::new();
    let mut pending = vec![start];
    while let Some(commit) = pending.pop() {
        if reached.insert(commit) {
            pending.extend(forge.parent(commit));
            pending.extend(forge.merge_parent(commit));
        }
    }
    reached
}

fn common_ancestor(forge: &impl Remote, ours: u64, theirs: u64) -> Option<u64> {
    // Store names are increasing topological numbers. The latest common
    // ancestor suffices for the world's single-base histories.
    let left = ancestors(forge, ours);
    let right = ancestors(forge, theirs);
    left.intersection(&right).copied().max()
}

fn markers(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut result = b"<<<<<<< ours\n".to_vec();
    result.extend_from_slice(left);
    if !left.ends_with(b"\n") {
        result.push(b'\n');
    }
    result.extend_from_slice(b"=======\n");
    result.extend_from_slice(right);
    if !right.ends_with(b"\n") {
        result.push(b'\n');
    }
    result.extend_from_slice(b">>>>>>> theirs\n");
    result
}

fn has_markers(content: &[u8]) -> bool {
    content
        .split(|byte| *byte == b'\n')
        .any(|line| line.starts_with(b"<<<<<<< ") || line == b"=======" || line.starts_with(b">>>>>>> "))
}

#[derive(PartialEq, Eq)]
struct Edit<'a> {
    start: usize,
    end: usize,
    replacement: Vec<&'a [u8]>,
}

fn edits<'a>(base: &[&[u8]], changed: &[&'a [u8]]) -> Vec<Edit<'a>> {
    let prefix = base.iter().zip(changed).take_while(|(left, right)| left == right).count();
    let suffix = base[prefix..]
        .iter()
        .rev()
        .zip(changed[prefix..].iter().rev())
        .take_while(|(left, right)| left == right)
        .count();
    let base = &base[prefix..base.len() - suffix];
    let changed = &changed[prefix..changed.len() - suffix];
    let mut lengths = vec![vec![0; changed.len() + 1]; base.len() + 1];
    for i in (0..base.len()).rev() {
        for j in (0..changed.len()).rev() {
            lengths[i][j] = if base[i] == changed[j] {
                lengths[i + 1][j + 1] + 1
            } else {
                lengths[i + 1][j].max(lengths[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut result = Vec::new();
    while i < base.len() || j < changed.len() {
        if i < base.len() && j < changed.len() && base[i] == changed[j] {
            i += 1;
            j += 1;
            continue;
        }
        let start = i;
        let mut replacement = Vec::new();
        while i < base.len() || j < changed.len() {
            if i < base.len() && j < changed.len() && base[i] == changed[j] {
                break;
            }
            if j < changed.len() && (i == base.len() || lengths[i][j + 1] >= lengths[i + 1][j]) {
                replacement.push(changed[j]);
                j += 1;
            } else {
                i += 1;
            }
        }
        result.push(Edit { start: prefix + start, end: prefix + i, replacement });
    }
    result
}

fn merge_lines(base: &[u8], left: &[u8], right: &[u8]) -> Option<Vec<u8>> {
    let base: Vec<_> = base.split_inclusive(|byte| *byte == b'\n').collect();
    let left: Vec<_> = left.split_inclusive(|byte| *byte == b'\n').collect();
    let right: Vec<_> = right.split_inclusive(|byte| *byte == b'\n').collect();
    let mut changes = edits(&base, &left);
    for edit in edits(&base, &right) {
        if changes.contains(&edit) {
            continue;
        }
        if changes.iter().any(|old| {
            (old.start < edit.end && edit.start < old.end)
                || (old.start == old.end && old.start >= edit.start && old.start <= edit.end)
                || (edit.start == edit.end && edit.start >= old.start && edit.start <= old.end)
        }) {
            return None;
        }
        changes.push(edit);
    }
    changes.sort_by_key(|edit| edit.start);
    let mut result = Vec::new();
    let mut cursor = 0;
    for edit in changes {
        for line in &base[cursor..edit.start] {
            result.extend_from_slice(line);
        }
        for line in edit.replacement {
            result.extend_from_slice(line);
        }
        cursor = edit.end;
    }
    for line in &base[cursor..] {
        result.extend_from_slice(line);
    }
    Some(result)
}

fn object(at: &[u8], commit: u64) -> Vec<u8> {
    [at, format!("/.git/objects/{commit}").as_bytes()].concat()
}
