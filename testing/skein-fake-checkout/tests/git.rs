//! Local git boundaries extracted from consuming checkout-world stories;
//! service remote policy and routing stay outside this kit (fake-checkout.md, section 4).

mod common;

use skein_fake_checkout::Checkout;
use skein_fake_checkout::git::{self, CommitFailure, Created, Fault, NotFetched, Pushed, Remote, Want, What};

use common::{Store, cloned, files, merge_case, tree};

#[test]
fn clone_and_fetch_import_both_parent_chains_without_rewriting_existing_objects() {
    let mut store = Store::new(tree(&[(b"code", b"first")]));
    let left = store.store(1, None, tree(&[(b"code", b"left")])).expect("left");
    let right = store.store(1, None, tree(&[(b"code", b"right")])).expect("right");
    let tip = store.store(left, Some(right), tree(&[(b"code", b"merged")])).expect("two parents");
    assert_eq!(git::create(&mut store, b"repo", b"side", tip), Ok(Created::Created));
    let mut cloned = Checkout::new();
    git::clone_repository(&mut store, &mut cloned, b"repo", b"work").expect("all tips");
    assert_eq!(files(&cloned), tree(&[]), "clone has no checked-out tree");
    for commit in [1, left, right, tip] {
        git::check_out(&store, &mut cloned, b"work", commit).expect("both parent chains imported");
    }
    assert_eq!(files(&cloned), tree(&[(b"code", b"merged")]));
    let mut fetched = Checkout::new();
    fetched.mkdir(b"work/.git");
    git::fetch(&mut store, &mut fetched, b"repo", b"work", Want::Commit(left)).expect("one side");
    let first_version = fetched.version(b"work/.git/objects/1");
    let left_version = fetched.version(b"work/.git/objects/2");
    git::fetch(&mut store, &mut fetched, b"repo", b"work", Want::Branch(b"side")).expect("merge graph");
    assert_eq!(fetched.version(b"work/.git/objects/1"), first_version);
    assert_eq!(fetched.version(b"work/.git/objects/2"), left_version);
    for commit in [1, left, right, tip] {
        git::check_out(&store, &mut fetched, b"work", commit).expect("both parents fetched");
    }
    let before = format!("{fetched:?}");
    git::fetch(&mut store, &mut fetched, b"repo", b"work", Want::Commit(tip)).expect("same graph");
    assert_eq!(format!("{fetched:?}"), before, "repeated fetch makes no new versions");
}

#[test]
fn checkout_preserves_nested_git_and_commit_snapshots_only_working_files_without_moving_refs() {
    let (mut store, mut checkout, first) = cloned(&[(b"README", b"hello")]);
    checkout.write(b"work/.git/index", b"kept");
    checkout.write(b"work/vendor/lib/.GIT/HEAD", b"nested");
    checkout.write(b"work/vendor/lib/stray", b"removed");
    git::check_out(&store, &mut checkout, b"work", first).expect("local");
    assert!(!checkout.exists(b"work/vendor/lib/stray"));
    assert_eq!(checkout.content(b"work/vendor/lib/.GIT/HEAD"), Some(b"nested".as_slice()));
    assert_eq!(git::commit(&mut store, &mut checkout, b"work", first), Ok(None));
    checkout.write(b"work/README", b"changed");
    let second = git::commit(&mut store, &mut checkout, b"work", first).expect("local parent").expect("changed");
    assert_eq!(store.tree(second), tree(&[(b"README", b"changed")]));
    assert_eq!(store.parent(second), Some(first));
    assert_eq!(store.merge_parent(second), None);
    assert_eq!(store.branch(b"main"), Some(first));
    assert_eq!(store.moves(), 0);
    assert_eq!(checkout.content(b"work/.git/temper-head"), Some(second.to_le_bytes().as_slice()));
}

#[test]
fn unfetched_inputs_and_transport_refusals_leave_the_checkout_unchanged() {
    let (mut store, mut checkout, first) = cloned(&[(b"code", b"first")]);
    let missing = store.store(first, None, tree(&[(b"code", b"next")])).expect("external commit");
    let before = format!("{checkout:?}");
    assert_eq!(git::check_out(&store, &mut checkout, b"work", missing), Err(NotFetched));
    assert_eq!(git::commit(&mut store, &mut checkout, b"work", missing), Err(NotFetched));
    assert_eq!(git::merge(&store, &mut checkout, b"work", missing), Err(NotFetched));
    assert_eq!(git::commit_merging(&mut store, &mut checkout, b"work", first, missing), Err(CommitFailure::NotFetched));
    assert_eq!(format!("{checkout:?}"), before);
    assert_eq!(
        git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Branch(b"absent")),
        Err(Fault::Missing(What::Branch))
    );
    assert_eq!(
        git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Commit(999)),
        Err(Fault::Missing(What::Commit))
    );
    store.fault = Some(Fault::Unreachable);
    assert_eq!(git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Default), Err(Fault::Unreachable));
    assert_eq!(git::clone_repository(&mut store, &mut checkout, b"repo", b"other"), Err(Fault::Unreachable));
    assert_eq!(format!("{checkout:?}"), before);
    assert!(!checkout.exists(b"other"));
    store.fault = None;
    git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Commit(missing)).expect("now fetched");
    git::check_out(&store, &mut checkout, b"work", missing).expect("local now");
    checkout.remove(b"work");
    assert_eq!(git::check_out(&store, &mut checkout, b"work", first), Err(NotFetched));
    git::clone_repository(&mut store, &mut checkout, b"repo", b"work").expect("fresh clone");
    assert_eq!(
        git::check_out(&store, &mut checkout, b"work", missing),
        Err(NotFetched),
        "external unreferenced object was not cloned"
    );
}

#[test]
fn pushes_forward_exact_conditions_and_refusals_and_creation_leaves_existing_branches() {
    let (mut store, mut checkout, first) = cloned(&[(b"code", b"first")]);
    checkout.write(b"work/code", b"next");
    let next = git::commit(&mut store, &mut checkout, b"work", first).expect("local").expect("change");
    assert_eq!(git::create(&mut store, b"repo", b"main", next), Ok(Created::Exists));
    assert_eq!(store.branch(b"main"), Some(first));
    assert_eq!(git::create(&mut store, b"repo", b"side", next), Ok(Created::Created));
    assert_eq!(git::push_expected(&mut store, &checkout, b"repo", b"work", next, b"main", next), Ok(Pushed::Rejected));
    assert_eq!(store.last_push, Some((b"main".to_vec(), next, Some(next))));
    assert_eq!(store.branch(b"main"), Some(first));
    assert_eq!(git::push_expected(&mut store, &checkout, b"repo", b"work", next, b"main", first), Ok(Pushed::Pushed));
    assert_eq!(store.last_push, Some((b"main".to_vec(), next, Some(first))));
    assert_eq!(git::push(&mut store, &checkout, b"repo", b"work", next, b"other"), Ok(Pushed::Pushed));
    assert_eq!(store.last_push, Some((b"other".to_vec(), next, None)));
    store.fault = Some(Fault::Refused);
    let before = format!("{checkout:?}");
    let moves = store.moves();
    assert_eq!(git::push(&mut store, &checkout, b"repo", b"work", first, b"main"), Err(Fault::Refused));
    assert_eq!(git::create(&mut store, b"repo", b"refused", next), Err(Fault::Refused));
    assert_eq!(store.moves(), moves);
    assert_eq!(store.branch(b"main"), Some(next));
    assert_eq!(format!("{checkout:?}"), before);
}

#[test]
fn clean_and_conflicted_line_edits_replay_with_independent_expected_trees_and_parents() {
    for seed in [0, 1, 7, 11] {
        assert_eq!(merge_case(seed), merge_case(seed), "complete fixture replay, seed {seed}");
    }
}

#[test]
fn conflict_deletion_resolves_without_a_failed_commit_allocating_an_object() {
    let (mut store, mut checkout, first) = cloned(&[(b"code", b"old\n")]);
    checkout.write(b"work/code", b"ours\n");
    let ours = git::commit(&mut store, &mut checkout, b"work", first).expect("local").expect("changed");
    let theirs = store.store(first, None, tree(&[(b"code", b"theirs\n")])).expect("side");
    git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Commit(theirs)).expect("side fetched");
    assert_eq!(git::merge(&store, &mut checkout, b"work", theirs).expect("local").conflicts, [b"code".to_vec()]);
    assert_eq!(checkout.content(b"work/.git/temper-conflicts/0"), Some(b"code".as_slice()));
    assert_eq!(checkout.content(b"work/.git/MERGE_HEAD"), Some(theirs.to_le_bytes().as_slice()));
    let before = format!("{checkout:?}\n{store:?}");
    assert_eq!(
        git::commit_merging(&mut store, &mut checkout, b"work", ours, theirs),
        Err(CommitFailure::Unresolved { files: vec![b"code".to_vec()] })
    );
    assert_eq!(format!("{checkout:?}\n{store:?}"), before);
    checkout.remove(b"work/code");
    let merged = git::commit_merging(&mut store, &mut checkout, b"work", ours, theirs).expect("deletion resolves");
    assert_eq!(merged, theirs + 1, "refusal stored no commit");
    assert!(store.tree(merged).is_empty());
    assert_eq!(store.parent(merged), Some(ours));
    assert_eq!(store.merge_parent(merged), Some(theirs));
    assert!(!checkout.exists(b"work/.git/MERGE_HEAD"));
    assert!(!checkout.exists(b"work/.git/temper-conflicts"));
}

#[test]
fn merge_preserves_additions_and_deletions_and_even_unchanged_trees_record_both_parents() {
    let (mut store, mut checkout, first) = cloned(&[(b"clean", b"old"), (b"conflict", b"old")]);
    checkout.write(b"work/conflict", b"modified");
    checkout.write(b"work/ours", b"added");
    let ours = git::commit(&mut store, &mut checkout, b"work", first).expect("local").expect("change");
    let theirs = store.store(first, None, tree(&[(b"theirs", b"added too")])).expect("deletions");
    git::fetch(&mut store, &mut checkout, b"repo", b"work", Want::Commit(theirs)).expect("side");
    assert_eq!(git::merge(&store, &mut checkout, b"work", theirs).expect("merge").conflicts, [b"conflict".to_vec()]);
    assert!(!checkout.exists(b"work/clean"));
    assert_eq!(checkout.content(b"work/ours"), Some(b"added".as_slice()));
    assert_eq!(checkout.content(b"work/theirs"), Some(b"added too".as_slice()));
    assert_eq!(
        checkout.content(b"work/conflict"),
        Some(b"<<<<<<< ours\nmodified\n=======\n\n>>>>>>> theirs\n".as_slice())
    );
    checkout.write(b"work/conflict", b"resolved");
    let merged = git::commit_merging(&mut store, &mut checkout, b"work", ours, theirs).expect("resolved");
    assert_eq!(store.tree(merged), tree(&[(b"conflict", b"resolved"), (b"ours", b"added"), (b"theirs", b"added too")]));
    git::check_out(&store, &mut checkout, b"work", theirs).expect("unchanged tree selected");
    let unchanged = git::commit_merging(&mut store, &mut checkout, b"work", first, theirs).expect("still a merge");
    assert_eq!(store.tree(unchanged), store.tree(theirs));
    assert_eq!(store.parent(unchanged), Some(first));
    assert_eq!(store.merge_parent(unchanged), Some(theirs));
    assert_eq!(store.moves(), 0);
}
