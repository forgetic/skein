//! The machine's own bookkeeping, which the conformance suite cannot see:
//! what it keeps while open and frees once closed, handles issued once,
//! the most links one resolution follows, where a listing resumes, and a
//! moved directory's `..`. What it answers, case by case, the suite holds
//! to the real kernel (kernel.md, 8).

use alloc::vec::Vec;

use crate::fs::{How, Is, Item, Machine, Refusal};

fn machine(items: &[Item]) -> (Machine, crate::Opened) {
    let mut machine = Machine::new();
    let root = machine.lay(items);
    (machine, root)
}

#[test]
fn a_file_removed_lives_while_open_and_goes_once_closed() {
    let (mut machine, root) = machine(&[Item::file(b"f", b"bytes")]);
    let laid = machine.nodes.len();
    let file = machine.open(root, b"f", How::Read).unwrap();
    machine.remove(root, b"f", false).unwrap();
    assert_eq!(machine.nodes.len(), laid, "kept while open");
    assert_eq!(machine.read(file, 0, 16).unwrap(), b"bytes");
    machine.close(file);
    assert_eq!(machine.nodes.len(), laid - 1, "freed once closed");
    machine.close(root);
    assert_eq!(machine.open_handles(), 0);
}

#[test]
fn a_file_renamed_over_goes_unless_open() {
    let (mut machine, root) = machine(&[Item::file(b"a", b"new"), Item::file(b"b", b"old")]);
    let laid = machine.nodes.len();
    let old = machine.open(root, b"b", How::Read).unwrap();
    machine.rename(root, b"a", root, b"b").unwrap();
    assert_eq!(machine.nodes.len(), laid, "the old file kept while open");
    assert_eq!(machine.read(old, 0, 8).unwrap(), b"old");
    machine.close(old);
    assert_eq!(machine.nodes.len(), laid - 1);
}

#[test]
fn handles_are_issued_once() {
    let (mut machine, root) = machine(&[Item::file(b"f", b"")]);
    let first = machine.open(root, b"f", How::Read).unwrap();
    machine.close(first);
    let second = machine.open(root, b"f", How::Read).unwrap();
    assert_ne!(first, second);
}

#[test]
#[should_panic(expected = "a handle the machine never issued, or closed")]
fn a_closed_handle_is_its_clients_bug() {
    let (mut machine, root) = machine(&[Item::file(b"f", b"")]);
    let file = machine.open(root, b"f", How::Read).unwrap();
    machine.close(file);
    let stat = machine.stat(file);
    panic!("stated a closed handle: {stat:?}");
}

/// Forty links followed in one resolution, and no more.
#[test]
fn a_resolution_follows_forty_links() {
    let mut items = vec![Item::file(b"l0", b"end")];
    for n in 1..=41_u32 {
        items.push(Item::link(format!("l{n}").as_bytes(), format!("l{}", n - 1).as_bytes()));
    }
    let (mut machine, root) = machine(&items);
    let file = machine.open(root, b"l40", How::Read).unwrap();
    assert_eq!(machine.read(file, 0, 8).unwrap(), b"end");
    assert_eq!(machine.open(root, b"l41", How::Read), Err(Refusal::Loop));
}

#[test]
fn a_listing_resumes_after_the_last_name_it_handed_back() {
    let (mut machine, root) = machine(&[Item::file(b"b", b""), Item::file(b"d", b"")]);
    let first = machine.list(root, 1, 255).unwrap();
    assert_eq!(first, [(Is::File, b"b".to_vec().into_boxed_slice())]);
    let a = machine.open(root, b"a", How::Create).unwrap();
    let c = machine.open(root, b"c", How::Create).unwrap();
    let rest: Vec<_> = machine.list(root, 8, 255).unwrap().into_iter().map(|(_, name)| name.to_vec()).collect();
    assert_eq!(rest, [b"c".to_vec(), b"d".to_vec()], "a name made before the cursor is not seen");
    assert!(machine.list(root, 8, 255).unwrap().is_empty());
    machine.close(a);
    machine.close(c);
}

#[test]
fn a_moved_directory_finds_its_new_parent_by_dot_dot() {
    let (mut machine, root) =
        machine(&[Item::directory(b"from"), Item::directory(b"to"), Item::file(b"to/mark", b"here")]);
    machine.rename(root, b"from", root, b"moved").unwrap();
    let to = machine.open(root, b"to", How::Directory).unwrap();
    machine.rename(root, b"moved", to, b"inner").unwrap();
    let mark = machine.open(root, b"to/inner/../mark", How::Read).unwrap();
    assert_eq!(machine.read(mark, 0, 8).unwrap(), b"here");
    assert_eq!(machine.rename(root, b"to", to, b"x"), Err(Refusal::Beneath));
}

/// A device opens as no file, as on a filesystem not mounted `nodev`, and
/// lists as itself; a FIFO likewise.
#[test]
fn a_device_and_a_fifo_open_as_no_file() {
    let (mut machine, root) = machine(&[Item::device(b"null"), Item::fifo(b"pipe")]);
    assert_eq!(machine.open(root, b"null", How::Read), Err(Refusal::NotAFile));
    assert_eq!(machine.open(root, b"pipe", How::Read), Err(Refusal::NotAFile));
    assert_eq!(machine.open(root, b"pipe", How::Directory), Err(Refusal::NotADirectory));
    let listed = machine.list(root, 8, 255).unwrap();
    let kinds: Vec<_> = listed.into_iter().map(|(is, _)| is).collect();
    assert_eq!(kinds, [Is::Device, Is::Fifo]);
    machine.remove(root, b"null", false).unwrap();
    machine.close(root);
}
