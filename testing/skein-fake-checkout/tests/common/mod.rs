//! Private finite graph/transport answers for leaf tests; not a service forge.
//! The kit is checked against explicit trees and call arguments (fake-checkout.md,
//! section 5; testing-strategy.md, sections 2.2 and 7).

use std::collections::BTreeMap;

use skein_fake_checkout::Checkout;
use skein_fake_checkout::git::{self, Created, Fault, Pushed, Remote, Tree, Want, What};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Commit {
    parent: Option<u64>,
    merging: Option<u64>,
    tree: Tree,
}

/// Tiny synchronous graph store with exact-head rejection, not a general forge.
/// Tests supply existing parent IDs and finite trees; it keeps no domain state
/// (fake-checkout.md, section 5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Store {
    commits: BTreeMap<u64, Commit>,
    branches: BTreeMap<Vec<u8>, u64>,
    next: u64,

    /// Scripted transport refusal for the focused boundary scenarios (fake-checkout.md, section 5).
    pub(super) fault: Option<Fault>,

    /// Last exact push arguments observed at the remote boundary (fake-checkout.md, section 5).
    pub(super) last_push: Option<(Vec<u8>, u64, Option<u64>)>,
    moves: usize,
}

impl Store {
    /// One root commit, ID 1, at the fixture's `main` branch (fake-checkout.md, section 5).
    pub(super) fn new(tree: Tree) -> Store {
        Store {
            commits: BTreeMap::from([(1, Commit { parent: None, merging: None, tree })]),
            branches: BTreeMap::from([(b"main".to_vec(), 1)]),
            next: 2,
            fault: None,
            last_push: None,
            moves: 0,
        }
    }

    /// Pure branch lookup for the independent expected reference check (fake-checkout.md, section 5).
    pub(super) fn branch(&self, branch: &[u8]) -> Option<u64> {
        self.branches.get(branch).copied()
    }

    /// Number of reference moves performed by the private fixture (fake-checkout.md, section 5).
    pub(super) fn moves(&self) -> usize {
        self.moves
    }

    fn check(&self, remote: &[u8]) -> Result<(), Fault> {
        if remote != b"repo" {
            return Err(Fault::Missing(What::Repository));
        }
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        Ok(())
    }
}

impl Remote for Store {
    fn heads(&mut self, remote: &[u8]) -> Result<Vec<u64>, Fault> {
        self.check(remote)?;
        Ok(self.branches.values().copied().collect())
    }

    fn fetch(&mut self, remote: &[u8], want: Want<'_>) -> Result<u64, Fault> {
        self.check(remote)?;
        match want {
            Want::Branch(branch) => self.branch(branch).ok_or(Fault::Missing(What::Branch)),
            Want::Commit(commit) => {
                self.commits.contains_key(&commit).then_some(commit).ok_or(Fault::Missing(What::Commit))
            }
            Want::Default => self.branch(b"main").ok_or(Fault::Missing(What::Branch)),
        }
    }

    fn create(&mut self, remote: &[u8], branch: &[u8], commit: u64) -> Result<Created, Fault> {
        self.check(remote)?;
        if !self.commits.contains_key(&commit) {
            return Err(Fault::Missing(What::Commit));
        }
        if self.branches.contains_key(branch) {
            return Ok(Created::Exists);
        }
        self.branches.insert(branch.to_vec(), commit);
        self.moves += 1;
        Ok(Created::Created)
    }

    fn push(&mut self, remote: &[u8], branch: &[u8], commit: u64, expected: Option<u64>) -> Result<Pushed, Fault> {
        self.check(remote)?;
        assert!(self.commits.contains_key(&commit), "test push uses an existing commit");
        self.last_push = Some((branch.to_vec(), commit, expected));
        // Only exact-head policy is needed by these core boundary tests.
        // Production/service fake remotes own their full reference policy.
        if expected.is_some() && expected != self.branch(branch) {
            return Ok(Pushed::Rejected);
        }
        self.branches.insert(branch.to_vec(), commit);
        self.moves += 1;
        Ok(Pushed::Pushed)
    }

    fn parent(&self, commit: u64) -> Option<u64> {
        self.commits.get(&commit).expect("test queries an existing commit").parent
    }

    fn merge_parent(&self, commit: u64) -> Option<u64> {
        self.commits.get(&commit).expect("test queries an existing commit").merging
    }

    fn tree(&self, commit: u64) -> Tree {
        self.commits.get(&commit).expect("test queries an existing commit").tree.clone()
    }

    fn store(&mut self, parent: u64, merging: Option<u64>, tree: Tree) -> Option<u64> {
        let old = self.commits.get(&parent).expect("test stores on an existing parent");
        if merging.is_none() && old.tree == tree {
            return None;
        }
        assert!(merging.is_none_or(|commit| self.commits.contains_key(&commit)), "test merge parent exists");
        let commit = self.next;
        self.next = self.next.checked_add(1).expect("small test graph");
        self.commits.insert(commit, Commit { parent: Some(parent), merging, tree });
        Some(commit)
    }
}

/// Owned byte tree from explicit independent expectations (fake-checkout.md, section 5).
pub(super) fn tree(files: &[(&[u8], &[u8])]) -> Tree {
    files.iter().map(|(path, bytes)| (path.to_vec(), bytes.to_vec())).collect()
}

/// One root commit cloned and checked out at `work`, without moving references (fake-checkout.md, section 5).
pub(super) fn cloned(files: &[(&[u8], &[u8])]) -> (Store, Checkout, u64) {
    let mut store = Store::new(tree(files));
    let mut checkout = Checkout::new();
    git::clone_repository(&mut store, &mut checkout, b"repo", b"work").expect("fixture clone");
    git::check_out(&store, &mut checkout, b"work", 1).expect("clone imported root");
    (store, checkout, 1)
}

/// Regular working files excluding metadata, for explicit expected-tree checks (fake-checkout.md, section 5).
pub(super) fn files(checkout: &Checkout) -> Tree {
    let mut files = checkout.tree(b"work");
    files.retain(|path, _| !skein_fake_checkout::in_git(path));
    files
}

/// Independent tiny line-edit/conflict scenario and complete replay snapshot.
/// Seed numbers select explicit edits, not kit-derived expectations (fake-checkout.md, section 5).
pub(super) fn merge_case(seed: u64) -> String {
    let base = [b"a\n".to_vec(), b"b\n".to_vec(), b"c\n".to_vec()];
    let left_index = usize::try_from(seed % 3).expect("three lines");
    let right_index = usize::try_from((seed / 3) % 3).expect("three lines");
    let left_changed = seed % 4 != 0;
    let right_changed = seed % 5 != 0;
    let mut left = base.clone();
    let mut right = base.clone();
    if left_changed {
        left[left_index] = format!("left-{seed}\n").into_bytes();
    }
    if right_changed {
        right[right_index] = format!("right-{seed}\n").into_bytes();
    }
    let left_bytes = left.concat();
    let right_bytes = right.concat();
    let conflict = left_changed && right_changed && left_index == right_index;
    let mut expected = base;
    if left_changed {
        expected[left_index] = left[left_index].clone();
    }
    if right_changed {
        expected[right_index] = right[right_index].clone();
    }
    let (mut store, mut checkout, first) = cloned(&[(b"code", b"a\nb\nc\n")]);
    checkout.write(b"work/code", &left_bytes);
    let ours = git::commit(&mut store, &mut checkout, b"work", first).expect("local root").unwrap_or(first);
    let theirs = store.store(first, None, tree(&[(b"code", &right_bytes)])).unwrap_or(first);
    git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Commit(theirs)).expect("known side");
    let before = format!("{checkout:?}");
    git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Commit(theirs)).expect("repeat fetch");
    assert_eq!(format!("{checkout:?}"), before, "already imported ancestry keeps versions");
    let merged = git::merge(&store, &mut checkout, b"work", theirs).expect("both parents local");
    if conflict {
        assert_eq!(merged.conflicts, [b"code".to_vec()]);
        let markers =
            [b"<<<<<<< ours\n".as_slice(), &left_bytes, b"=======\n", &right_bytes, b">>>>>>> theirs\n"].concat();
        assert_eq!(checkout.content(b"work/code"), Some(markers.as_slice()));
        assert_eq!(
            git::commit_merging(&mut store, &mut checkout, b"work", ours, theirs),
            Err(git::CommitFailure::Unresolved { files: vec![b"code".to_vec()] })
        );
        checkout.write(b"work/code", &expected.concat());
    } else {
        assert!(merged.conflicts.is_empty());
    }
    assert_eq!(files(&checkout), tree(&[(b"code", &expected.concat())]));
    let commit = git::commit_merging(&mut store, &mut checkout, b"work", ours, theirs).expect("resolved or clean");
    assert_eq!(store.parent(commit), Some(ours));
    assert_eq!(store.merge_parent(commit), Some(theirs));
    assert_eq!(store.tree(commit), tree(&[(b"code", &expected.concat())]));
    assert_eq!(store.branch(b"main"), Some(first));
    assert_eq!(store.moves(), 0, "local work moves no remote references");
    assert!(store.last_push.is_none(), "local work sent no push");
    assert!(store.fault.is_none(), "sweep uses explicit successful transport answers");
    for parent in [first, ours, theirs, commit] {
        let path = format!("work/.git/objects/{parent}");
        assert!(checkout.exists(path.as_bytes()), "each used commit is locally held");
    }
    assert_eq!(checkout.content(b"work/.git/temper-head"), Some(commit.to_le_bytes().as_slice()));
    assert!(!checkout.exists(b"work/.git/MERGE_HEAD"));
    assert!(!checkout.exists(b"work/.git/temper-conflicts"));
    format!("{checkout:?}\n{store:?}")
}
