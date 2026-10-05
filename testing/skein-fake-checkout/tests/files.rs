//! File/process contracts from consuming domain-world stories, independently
//! checked at the kit boundary (fake-checkout.md, sections 2 and 3).

use std::collections::BTreeMap;
use std::time::Duration;

use skein_fake_checkout::{Checkout, Exit, Expect, Failure, Kind, Program, Searched, in_git};

#[test]
fn stores_compare_versions_before_creating_parents_or_issuing_versions() {
    let mut checkout = Checkout::new();
    let root = checkout.root(b"work");
    assert_eq!(checkout.root_path(root), b"work");
    let first = checkout.store(root, b"file", b"same", Expect::Absent).expect("absent");
    let before = format!("{checkout:?}");
    assert_eq!(checkout.store(root, b"file", b"changed", Expect::Absent), Err(Failure::Conflict { now: Some(first) }));
    assert_eq!(
        checkout.store(root, b"new/deep/file", b"changed", Expect::Is(first)),
        Err(Failure::Conflict { now: None })
    );
    assert_eq!(format!("{checkout:?}"), before, "refusals change no node, root, script or version");
    let second = checkout.store(root, b"file", b"same", Expect::Is(first)).expect("same current version");
    assert_eq!(second, first + 1, "refusals issued no version, identical content still does");
    let third = checkout.store(root, b"new/deep/file", b"new", Expect::Absent).expect("make missing parents");
    assert_eq!(checkout.load(root, b"new/deep/file", 3), Ok((b"new".to_vec(), third)));
    assert!(checkout.exists(b"work/new/deep"));
    assert_eq!(checkout.load(root, b"file", 3), Err(Failure::TooLarge { size: 4 }));
}

#[test]
fn roots_follow_forty_relative_links_and_refuse_escapes_and_linked_stores() {
    let mut checkout = Checkout::new();
    let root = checkout.root(b"work");
    let version = checkout.write(b"work/file", b"inside");
    checkout.write(b"outside", b"outside");
    checkout.link(b"work/relative", b"./file");
    checkout.link(b"work/parent", b"../outside");
    checkout.link(b"work/absolute", b"/outside");
    for index in (0..40).rev() {
        let path = format!("work/link{index}");
        let target = if index == 39 { "file".to_owned() } else { format!("link{}", index + 1) };
        checkout.link(path.as_bytes(), target.as_bytes());
    }
    checkout.link(b"work/too-many", b"link0");
    assert_eq!(checkout.load(root, b"./relative", 6), Ok((b"inside".to_vec(), version)));
    assert_eq!(checkout.load(root, b"link0", 6), Ok((b"inside".to_vec(), version)));
    assert_eq!(checkout.load(root, b"too-many", 6), Err(Failure::Loop));
    for path in [b"../outside".as_slice(), b"parent", b"absolute"] {
        assert_eq!(checkout.load(root, path, 99), Err(Failure::Escapes));
    }
    let before = format!("{checkout:?}");
    assert_eq!(checkout.store(root, b"relative", b"x", Expect::Is(version)), Err(Failure::Linked));
    assert_eq!(checkout.store(root, b"relative/child", b"x", Expect::Absent), Err(Failure::Linked));
    assert_eq!(format!("{checkout:?}"), before);
}

#[test]
fn scans_are_immediate_sorted_and_bounded_and_refusals_distinguish_node_kinds() {
    let mut checkout = Checkout::new();
    let root = checkout.root(b"work");
    checkout.write(b"work/b", b"file");
    checkout.write(b"work/a/deep", b"child");
    checkout.link(b"work/c", b"missing");
    checkout.special(b"work/d");
    assert_eq!(
        checkout.scan(root, b"", 2),
        Ok((vec![(b"a".to_vec(), Kind::Directory), (b"b".to_vec(), Kind::File)], 2))
    );
    assert_eq!(checkout.scan(root, b"", 0), Ok((Vec::new(), 4)));
    assert_eq!(
        checkout.scan(root, b"", 10),
        Ok((
            vec![
                (b"a".to_vec(), Kind::Directory),
                (b"b".to_vec(), Kind::File),
                (b"c".to_vec(), Kind::Link),
                (b"d".to_vec(), Kind::Special),
            ],
            0
        ))
    );
    assert_eq!(checkout.scan(root, b"b", 1), Err(Failure::NotDirectory));
    assert_eq!(checkout.load(root, b"a", 99), Err(Failure::NotFile));
    assert_eq!(checkout.load(root, b"absent", 99), Err(Failure::Missing));
    assert_eq!(checkout.load(root, b"b/child", 99), Err(Failure::NotDirectory));
    assert_eq!(checkout.store(root, b"d", b"x", Expect::Absent), Err(Failure::NotFile));
    assert_eq!(checkout.content(b"work/d"), None);
    assert_eq!(checkout.version(b"work/c"), None);
}

#[test]
fn search_bounds_retained_paths_and_text_without_following_hidden_or_linked_descendants() {
    let mut checkout = Checkout::new();
    let root = checkout.root(b"work");
    checkout.write(b"work/a.rs", b"needle\nneedle again\n");
    checkout.write(b"work/b.rs", b"needle\n");
    checkout.write(b"work/c.txt", b"needle\n");
    checkout.write(b"work/.hidden/x.rs", b"needle\n");
    checkout.link(b"work/z.rs", b"a.rs");
    let found = checkout.search(root, b"", b"needle", Some(b"*.rs"), (9, 6)).expect("literal");
    assert_eq!(found.hits, vec![(b"a.rs".to_vec(), 1, b"ne".to_vec())]);
    assert_eq!(found.more, 2, "hidden files, linked alias and wrong suffix do not match");
    let zero = checkout.search(root, b"", b"needle", Some(b"*.rs"), (0, 99)).expect("zero cap");
    assert!(zero.hits.is_empty());
    assert_eq!(zero.more, 3);
    let file = checkout.search(root, b"b.rs", b"needle", None, (1, 6)).expect("regular file");
    assert_eq!(file.hits, vec![(Vec::new(), 1, b"needle".to_vec())]);
    assert_eq!(file.more, 0);
    assert_eq!(
        checkout.search(root, b"", b"(", None, (1, 9)),
        Err(Searched::Unreadable(b"rg: regex parse error: unclosed group\n".to_vec()))
    );
    assert_eq!(checkout.search(root, b"absent", b"needle", None, (1, 9)), Err(Searched::Failed(Failure::Missing)));
}

#[test]
fn spawn_snapshots_scripts_and_finish_obeys_deepest_roots_and_git_protection() {
    let mut checkout = Checkout::new();
    let writable = checkout.root(b"work");
    let readonly = checkout.root(b"work/private");
    checkout.write(b"work/private/file", b"kept");
    checkout.write(b"work/.GiT/HEAD", b"kept");
    checkout.write(b"work/sub/.git/HEAD", b"kept");
    checkout.write(b"outside", b"kept");
    checkout.program(
        b"change",
        Program {
            duration: Duration::from_millis(7),
            output: b"script output".to_vec(),
            exit: Exit::Signal(9),
            changes: vec![
                (b"work/public".to_vec(), Some(b"changed".to_vec())),
                (b"work/private/file".to_vec(), None),
                (b"work/.GiT/HEAD".to_vec(), None),
                (b"work/sub/.git/HEAD".to_vec(), None),
                (b"outside".to_vec(), None),
            ],
        },
    );
    let process =
        checkout.spawn(writable, b"", b"change", &[], &[(writable, true), (readonly, false)]).expect("directory");
    checkout.program(
        b"change",
        Program { duration: Duration::ZERO, output: Vec::new(), exit: Exit::Code(0), changes: Vec::new() },
    );
    assert_eq!(process.program.duration, Duration::from_millis(7));
    assert_eq!(process.program.exit, Exit::Signal(9));
    assert_eq!(process.program.output, b"script output");
    checkout.finish(&process);
    assert_eq!(checkout.content(b"work/public"), Some(b"changed".as_slice()));
    for path in [b"work/private/file".as_slice(), b"work/.GiT/HEAD", b"work/sub/.git/HEAD", b"outside"] {
        assert_eq!(checkout.content(path), Some(b"kept".as_slice()));
    }
    let first = checkout.version(b"work/public").expect("first effect");
    checkout.finish(&process);
    assert_eq!(checkout.version(b"work/public"), Some(first + 1), "the world, not this kit, fences duplicate effects");
    assert!(in_git(b"work/sub/.GIT/HEAD"));
    assert!(!in_git(b"work/.gitignore"));
}

#[test]
fn builtin_environment_unknown_command_and_working_directory_checks_are_synchronous() {
    let mut checkout = Checkout::new();
    let root = checkout.root(b"work");
    checkout.write(b"work/file", b"x");
    let env = vec![(b"A".to_vec(), b"one".to_vec()), (b"B".to_vec(), b"two".to_vec())];
    let process = checkout.spawn(root, b"", b"env", &env, &[]).expect("env");
    assert_eq!(process.program.output, b"A=one\nB=two\n");
    assert_eq!(process.program.exit, Exit::Code(0));
    assert_eq!(process.program.duration, Duration::from_millis(1));
    let missing = checkout.spawn(root, b"", b"unknown", &[], &[]).expect("scripted not-found exit");
    assert_eq!(missing.program.exit, Exit::Code(127));
    assert_eq!(missing.program.output, b"sh: unknown: not found\n");
    assert_eq!(checkout.spawn(root, b"file", b"env", &[], &[]), Err(Failure::NotDirectory));
    assert_eq!(checkout.spawn(root, b"absent", b"env", &[], &[]), Err(Failure::Missing));
}

#[test]
fn replacing_a_tree_preserves_nested_git_nodes_and_removing_it_removes_objects() {
    let mut checkout = Checkout::new();
    checkout.write(b"work/old", b"old");
    checkout.write(b"work/.git/objects/1", b"");
    checkout.write(b"work/vendor/lib/.GIT/HEAD", b"nested");
    checkout.write(b"work/vendor/lib/stray", b"remove");
    checkout.write(b"elsewhere/file", b"kept");
    let tree = BTreeMap::from([(b"new/deep".to_vec(), b"new".to_vec())]);
    checkout.replace_tree(b"work", &tree);
    assert!(!checkout.exists(b"work/old"));
    assert!(!checkout.exists(b"work/vendor/lib/stray"));
    assert_eq!(checkout.content(b"work/vendor/lib/.GIT/HEAD"), Some(b"nested".as_slice()));
    assert!(checkout.exists(b"work/vendor/lib"));
    assert_eq!(checkout.content(b"work/new/deep"), Some(b"new".as_slice()));
    assert_eq!(checkout.files().len(), 4);
    let mut actual = checkout.tree(b"work");
    actual.retain(|path, _| !in_git(path));
    assert_eq!(actual, tree);
    checkout.remove(b"work");
    assert!(!checkout.exists(b"work/.git/objects/1"));
    assert!(checkout.tree(b"work").is_empty());
    assert_eq!(checkout.content(b"elsewhere/file"), Some(b"kept".as_slice()));
}
