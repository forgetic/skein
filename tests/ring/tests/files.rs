//! The ring adapter's own part in files, against the real kernel, in a
//! scratch directory (shell.md, 9): a root opened at startup, a `List` it
//! runs itself at submit, the position it hands from one `List` to the next
//! across its own buffer, a large file through its reads and writes, and an
//! invalid record refused. The records' behaviour is the conformance
//! suite's, in `tests/conformance/ring`.
//!
//! A machine without `io_uring` fails every test here, saying so.

#[path = "files/startup.rs"]
mod startup;

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use skein_io::kernel::{Done, Entry, Error, Fd, Kind, LONGEST_NAME, Op, OpenHow};
use skein_lib::Token;
use skein_ring_tests::{World, pattern};
use skein_scratch::Scratch;
use skein_shell::open_root;

fn root(dir: &Path) -> Fd {
    open_root(dir).expect("a scratch directory opens as a root")
}

fn open(world: &mut World, root: Fd, path: &[u8], how: OpenHow) -> Fd {
    match world.run(Op::Open { root, path: Box::from(path), how }).result {
        Ok(Done::Fd(fd)) => fd,
        other => panic!("an open of {}: {other:?}", path.escape_ascii()),
    }
}

/// One `List` of `fd` with room for `entries` and `names` bytes: each name
/// and kind it handed back.
fn list(world: &mut World, fd: Fd, entries: usize, names: usize) -> Vec<(Vec<u8>, Kind)> {
    let op = Op::List { fd, entries: vec![Entry::BLANK; entries].into(), names: vec![0; names].into() };
    let listed = world.run(op);
    let (Ok(Done::Count(n)), Op::List { entries, names, .. }) = (listed.result, listed.kind) else {
        panic!("a list completion has the expected result and operation")
    };
    let mut got = Vec::new();
    for entry in &entries[..usize::try_from(n).expect("a count fits a usize")] {
        got.push((entry.name(&names).expect("a name within names").to_vec(), entry.kind));
    }
    got
}

#[test]
fn a_root_is_a_directory_opened_at_startup() {
    let scratch = Scratch::new("ring");
    fs::write(scratch.path().join("file"), b"x").unwrap();
    let mut world = World::new(4);
    let dir = root(scratch.path());
    assert_eq!(open_root(&scratch.path().join("file")), Err(libc::ENOTDIR));
    assert_eq!(open_root(&scratch.path().join("missing")), Err(libc::ENOENT));
    assert_eq!(open_root(Path::new("nul\0inside")), Err(libc::EINVAL));
    let file = open(&mut world, dir, b"file", OpenHow::Read);
    world.close(file);
    world.close(dir);
    world.settle();
}

/// `getdents64` is not a ring operation: the adapter lists at submit, and
/// the completion waits for the next reap, entering the ring or not.
#[test]
fn a_list_runs_at_its_submit_and_completes_at_the_next_reap() {
    let scratch = Scratch::new("ring");
    fs::write(scratch.path().join("only"), b"").unwrap();
    let mut world = World::new(4);
    let dir = root(scratch.path());
    let op = Op::List { fd: dir, entries: vec![Entry::BLANK; 4].into(), names: vec![0; 256].into() };
    let token = world.start(op);
    world.reap();
    let listed = world.arrived.remove(&token).expect("completed at the reap, with no wait");
    assert_eq!(listed.result, Ok(Done::Count(1)));
    assert_eq!(list(&mut world, dir, 4, 256), [], "then the end");
    world.close(dir);
    world.settle();
}

/// Entries past what one `getdents64` reads, listed a few at a time, and
/// one at a time where a long name fills the names: each comes once, the
/// position handed from one `List` to the next whatever the adapter read.
#[test]
fn a_list_resumes_where_it_stopped_across_the_adapters_buffer() {
    const MANY: usize = 1500;
    let scratch = Scratch::new("ring");
    let mut expected = BTreeSet::new();
    for n in 0..MANY {
        let name = format!("an-entry-with-a-longish-name-{n:05}");
        fs::write(scratch.path().join(&name), b"").unwrap();
        expected.insert((name.into_bytes(), Kind::File));
    }
    let long = vec![b'l'; 200];
    fs::create_dir(scratch.path().join(String::from_utf8(long.clone()).unwrap())).unwrap();
    expected.insert((long, Kind::Directory));

    let mut world = World::new(4);
    let dir = root(scratch.path());
    let mut seen = BTreeSet::new();
    let mut lists = 0_u32;
    loop {
        let got = list(&mut world, dir, 7, LONGEST_NAME);
        if got.is_empty() {
            break;
        }
        assert!(got.len() <= 7, "no more than the entries");
        lists += 1;
        for entry in got {
            assert!(seen.insert(entry.clone()), "each entry once: {:?}", entry.0.escape_ascii().to_string());
        }
    }
    assert_eq!(seen, expected, "every entry");
    assert!(lists >= 215, "seven entries a list at most, of 1501: {lists}");
    world.close(dir);
    world.settle();
}

#[test]
fn a_large_file_is_written_and_read_back_at_its_offsets() {
    const LEN: usize = 8 << 20;
    let scratch = Scratch::new("ring");
    let mut world = World::new(4);
    let dir = root(scratch.path());
    let bytes = pattern(LEN);
    let file = open(&mut world, dir, b"large", OpenHow::Create { mode: None });
    let mut from = 0_u32;
    while usize::try_from(from).unwrap() < LEN {
        let op = Op::write(file, bytes.clone(), from, u64::from(from)).unwrap();
        let Ok(Done::Count(n)) = world.run(op).result else { panic!("a write") };
        from += n;
    }
    assert_eq!(world.run(Op::Sync { fd: file }).result, Ok(Done::Nothing));
    world.close(file);

    let file = open(&mut world, dir, b"large", OpenHow::Read);
    let mut read = Vec::with_capacity(LEN);
    loop {
        let op = Op::read(file, vec![0; 1 << 20].into(), u64::try_from(read.len()).unwrap()).unwrap();
        let done = world.run(op);
        let (Ok(Done::Count(n)), Op::Read { buf, .. }) = (done.result, done.kind) else { panic!("a read") };
        if n == 0 {
            break;
        }
        read.extend_from_slice(&buf[..usize::try_from(n).unwrap()]);
    }
    assert!(read == *bytes, "the bytes written, read back in order");
    world.close(file);
    world.close(dir);
    world.settle();
}

#[test]
#[should_panic(expected = "io submits only valid records")]
fn a_name_that_could_leave_its_directory_is_refused_at_submit() {
    let mut world = World::new(4);
    world.submit(Token::new(1), Op::Remove { dir: Fd::new(0), name: Box::from(&b"../out"[..]), directory: false });
}

#[test]
fn an_error_of_a_file_names_what_the_contract_says() {
    let scratch = Scratch::new("ring");
    let mut world = World::new(4);
    let dir = root(scratch.path());
    let missing = world.run(Op::Open { root: dir, path: Box::from(&b"missing"[..]), how: OpenHow::Read });
    assert_eq!(missing.result, Err(Error::NotFound));
    let out = world.run(Op::Open { root: dir, path: Box::from(&b"../escape"[..]), how: OpenHow::Read });
    assert_eq!(out.result, Err(Error::Escape), "EXDEV, from RESOLVE_BENEATH");
    world.close(dir);
    world.settle();
}

/// A `Rename` from one filesystem to another: `EXDEV`, which on a `Rename`
/// is no escape but `Other` (kernel.md, 6.1). Needs `/dev/shm` on a mount
/// apart from the temporary directory's; without one, there is nothing to
/// rename across, and the test says so by passing having checked nothing.
#[test]
fn a_rename_across_filesystems_is_other_not_an_escape() {
    let shm = Path::new("/dev/shm");
    let here = Scratch::new("ring");
    let Ok(there_meta) = fs::metadata(shm) else { return };
    if there_meta.dev() == fs::metadata(here.path()).unwrap().dev() {
        return;
    }
    let there = Scratch::new_in(shm, "ring");
    fs::write(here.path().join("a"), b"a").unwrap();
    let mut world = World::new(4);
    let (from, to) = (root(here.path()), root(there.path()));
    let rename = Op::Rename { from_dir: from, from: Box::from(&b"a"[..]), to_dir: to, to: Box::from(&b"b"[..]) };
    assert_eq!(world.run(rename).result, Err(Error::Other(libc::EXDEV)));
    world.close(from);
    world.close(to);
    world.settle();
}

/// `RESOLVE_BENEATH` answers `EAGAIN` when a `..` races a rename or a mount
/// anywhere on the system. Renames run in the ring's own workers, beside
/// `Open`s through `..`, from this one thread, until one `Open` has raced:
/// the adapter submitted it again (kernel.md, 6.1), and none failed for it.
/// About one in a thousand races; the test gives up after sixty times that.
#[test]
fn an_open_through_dot_dot_racing_renames_is_submitted_again() {
    const OPENS: u32 = 60_000;
    const RENAMERS: usize = 40;
    const OPENERS: usize = 8;
    let scratch = Scratch::new("ring");
    fs::create_dir(scratch.path().join("sub")).unwrap();
    fs::write(scratch.path().join("x"), b"x").unwrap();
    for n in 0..RENAMERS {
        fs::write(scratch.path().join(format!("a{n}")), b"").unwrap();
    }
    let mut world = World::new(64);
    let dir = root(scratch.path());
    let rename = |n: usize, flipped: bool| {
        let (from, to) = if flipped { ("b", "a") } else { ("a", "b") };
        let (from, to) = (format!("{from}{n}"), format!("{to}{n}"));
        Op::Rename { from_dir: dir, from: from.into_bytes().into(), to_dir: dir, to: to.into_bytes().into() }
    };
    // A create is not tried inline, but in the ring's workers, beside the
    // renames: a read is, and io_uring itself retries one that raced there.
    let mut made = 0_u32;
    let mut open = || {
        made += 1;
        Op::Open { root: dir, path: format!("sub/../c{made}").into_bytes().into(), how: OpenHow::Create { mode: None } }
    };
    let mut renaming = std::collections::BTreeMap::new();
    let mut flipped = [false; RENAMERS];
    for n in 0..RENAMERS {
        renaming.insert(world.start(rename(n, false)), n);
    }
    let mut opening = BTreeSet::new();
    for _ in 0..OPENERS {
        opening.insert(world.start(open()));
    }
    let mut closing = BTreeSet::new();
    let (mut opened, mut refused) = (0_u32, Vec::new());
    let deadline = world.later(skein_lib::Duration::from_secs(30));
    while !renaming.is_empty() || !opening.is_empty() || !closing.is_empty() {
        let racing = opened < OPENS && world.kernel.resubmitted() == 0 && refused.is_empty();
        world.turn(deadline);
        for (token, complete) in std::mem::take(&mut world.arrived) {
            if let Some(n) = renaming.remove(&token) {
                assert_eq!(complete.result, Ok(Done::Nothing), "a rename of a file of its own");
                flipped[n] = !flipped[n];
                if racing {
                    renaming.insert(world.start(rename(n, flipped[n])), n);
                }
            } else if opening.remove(&token) {
                opened += 1;
                match complete.result {
                    Ok(Done::Fd(fd)) => {
                        closing.insert(world.start(Op::Close { fd }));
                    }
                    other => refused.push(other),
                }
                if racing {
                    opening.insert(world.start(open()));
                }
            } else {
                assert!(closing.remove(&token), "a completion of the test's");
                assert_eq!(complete.result, Ok(Done::Nothing));
            }
        }
    }
    assert!(refused.is_empty(), "an Open raced to EAGAIN is submitted again: {refused:?}");
    assert!(world.kernel.resubmitted() > 0, "no Open raced a rename in {opened}");
    world.close(dir);
    world.settle();
}

#[test]
#[should_panic(expected = "only Stat and Close may use a path-only descriptor")]
fn path_only_descriptors_cannot_read() {
    let scratch = Scratch::new("ring-path-only");
    fs::write(scratch.path().join("file"), b"x").unwrap();
    let mut world = World::new(4);
    let root = root(scratch.path());
    let path = open(&mut world, root, b"file", OpenHow::PathNoFollow);
    world.run(Op::Read { fd: path, buf: Box::from([0; 1]), at: 0 });
}
