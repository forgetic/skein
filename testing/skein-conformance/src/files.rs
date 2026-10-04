//! The scenarios on files (kernel.md, 6.1): each lays out a root of its
//! own ([`Item`]), runs records beneath it, and returns what it saw, which
//! its [`Check`] holds to the rules of the contract (`skein_io::kernel`,
//! module documentation, files), naming each. Every completion is checked
//! on the way too, by the driver, its paths and buffers handed back.

use alloc::boxed::Box;
use alloc::collections::BTreeSet;
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;

use skein_io::kernel::{Done, Error, Fd, Kind, Op, OpenHow, Stat};

pub use crate::run::{Entries, Shortness};
use crate::run::{NAMES, Run, unexpected};
use crate::{Backend, Check, Item};

const NOTHING: Result<Done, Error> = Ok(Done::Nothing);

/// Fails, naming the rule and what was seen, unless `holds`.
fn rule<T: Debug>(holds: bool, rule: &str, seen: &T) {
    assert!(holds, "the contract: {rule}; saw {seen:?}");
}

/// A name of 256 bytes, one past the longest.
fn too_long() -> Vec<u8> {
    vec![b'n'; 256]
}

/// The bytes of a file, from a `Read` of all of it.
#[expect(clippy::unnecessary_wraps, reason = "an answer expected, of the shape a read gives")]
fn file(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    Ok(bytes.to_vec())
}

/// Each entry of a listing, with its kind.
fn entries(listed: &[(&[u8], Kind)]) -> Entries {
    let mut set = BTreeSet::new();
    for (name, kind) in listed {
        set.insert((name.to_vec(), *kind));
    }
    set
}

/// A file made, written at offsets (over itself and past its end), synced,
/// read back from another descriptor, and stated; and the root's files and
/// a directory read and stated.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FileLifecycle {
    /// `abcdef` at 0, `XY` at 2, `Z` at 9: each `Write`'s counts.
    pub writes: [Result<Shortness, Error>; 3],
    /// A `Sync` of the file, and of a directory.
    pub synced: [Result<Done, Error>; 2],
    pub stat_written: Result<Stat, Error>,
    pub read_back: Result<Vec<u8>, Error>,
    pub reads: Shortness,
    /// `Read`s at the end of the file, and past it.
    pub at_the_end: [Result<Vec<u8>, Error>; 2],
    pub stat_read: Result<Stat, Error>,
    /// The root's file, and a file in a directory, read whole.
    pub planted: [Result<Vec<u8>, Error>; 2],
    /// A `Stat` of a directory opened as one, and of one opened to read.
    pub directories: [Result<Stat, Error>; 2],
    /// A `Read` of a directory opened to read.
    pub read_of_directory: Result<Done, Error>,
}

#[must_use]
pub fn file_lifecycle<B: Backend>(backend: &mut B) -> FileLifecycle {
    let tree = [Item::file(b"a.txt", b"hello"), Item::directory(b"d"), Item::file(b"d/inner", b"inside")];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);

    let new = run.open(process, root, b"new", OpenHow::Create).expect("a new file in the root");
    let writes = [
        run.write_all(process, new, 0, b"abcdef"),
        run.write_all(process, new, 2, b"XY"),
        run.write_all(process, new, 9, b"Z"),
    ];
    let synced_file = run.sync(process, new);
    let stat_written = run.stat(process, new);
    run.close(process, new);

    let new = run.open(process, root, b"new", OpenHow::Read).expect("the file made");
    let (read_back, reads) = run.read_all(process, new);
    let at_the_end = [read_at(&mut run, process, new, 10), read_at(&mut run, process, new, 100)];
    let stat_read = run.stat(process, new);
    run.close(process, new);

    let planted = [run.contents(process, root, b"a.txt"), run.contents(process, root, b"d/inner")];
    let d = run.open(process, root, b"d", OpenHow::Directory).expect("a directory");
    let synced_directory = run.sync(process, d);
    let as_directory = run.stat(process, d);
    run.close(process, d);
    let d = run.open(process, root, b"d", OpenHow::Read).expect("a directory opens to read");
    let read_of_directory = run.call(process, Op::Read { fd: d, buf: Box::from([0; 8]), at: 0 }).result;
    let as_read = run.stat(process, d);
    run.close(process, d);
    run.close(process, root);
    run.finish();
    FileLifecycle {
        writes,
        synced: [synced_file, synced_directory],
        stat_written,
        read_back,
        reads,
        at_the_end,
        stat_read,
        planted,
        directories: [as_directory, as_read],
        read_of_directory,
    }
}

/// One `Read` of 8 bytes at `at`.
fn read_at<B: Backend>(run: &mut Run<'_, B>, process: B::Process, fd: Fd, at: u64) -> Result<Vec<u8>, Error> {
    let complete = run.call(process, Op::read(fd, Box::from([0; 8]), at).expect("room to read"));
    match (complete.kind, complete.result) {
        (Op::Read { buf, .. }, Ok(Done::Count(n))) => {
            Ok(buf.get(..usize::try_from(n).expect("a u32 fits a usize")).expect("within the buffer").to_vec())
        }
        (_, Err(error)) => Err(error),
        (_, other) => unexpected("a Read answers with a count", &other),
    }
}

impl FileLifecycle {
    /// How the `Write`s counted, over all three.
    #[must_use]
    pub fn writes(&self) -> Shortness {
        let mut seen = Shortness::default();
        for written in self.writes.iter().flatten() {
            seen = seen.and(*written);
        }
        seen
    }
}

impl Check for FileLifecycle {
    fn check(&self) {
        for written in &self.writes {
            rule(written.is_ok(), "a Write writes, a short one continued from where it stopped", written);
        }
        assert_eq!(self.synced, [NOTHING, NOTHING], "the contract: Sync flushes a file, or a directory's entries");
        let length = Ok(Stat { kind: Kind::File, size: 10 });
        assert_eq!(self.stat_written, length, "the contract: Stat answers a file's kind and length");
        let expected = file(b"abXYef\0\0\0Z");
        assert_eq!(self.read_back, expected, "the contract: Write at an offset, zeros in a gap past the end");
        assert_eq!(self.at_the_end, [Ok(Vec::new()), Ok(Vec::new())], "the contract: a Read at or past the end is 0");
        assert_eq!(self.stat_read, length, "the contract: Stat answers a file's kind and length");
        assert_eq!(self.planted, [file(b"hello"), file(b"inside")], "the root's files, beneath it");
        for stat in &self.directories {
            let kind = stat.map(|stat| stat.kind);
            assert_eq!(kind, Ok(Kind::Directory), "the contract: Stat answers a directory's kind");
        }
        assert_eq!(self.read_of_directory, Err(Error::IsADirectory), "the contract: a Read of a directory");
    }
}

/// Renames: over a file, across directories, of what is missing, of a file
/// over a directory and the other way round, over a full directory and an
/// empty one, beneath itself, over itself, a name too long; and the idiom
/// that replaces a file whole.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Renames {
    pub over_a_file: Result<Done, Error>,
    /// The name renamed over holds the moved file; the old name is gone.
    pub moved: [Result<Vec<u8>, Error>; 2],
    pub across: Result<Done, Error>,
    pub across_read: Result<Vec<u8>, Error>,
    pub missing: Result<Done, Error>,
    pub file_over_directory: Result<Done, Error>,
    pub directory_over_file: Result<Done, Error>,
    pub over_a_full_directory: Result<Done, Error>,
    pub over_an_empty_directory: Result<Done, Error>,
    /// The empty directory renamed over is the moved one; the old name gone.
    pub after_over_empty: [Result<(), Error>; 2],
    pub beneath_itself: Result<Done, Error>,
    pub over_itself: Result<Done, Error>,
    pub over_itself_read: Result<Vec<u8>, Error>,
    pub too_long: Result<Done, Error>,
    /// The idiom: the new file's `Sync`, its `Rename` over the old, the
    /// directory's `Sync`; what the name then holds, and the temporary's
    /// name.
    pub replaced: [Result<Done, Error>; 3],
    pub replaced_read: [Result<Vec<u8>, Error>; 2],
}

#[must_use]
pub fn rename<B: Backend>(backend: &mut B) -> Renames {
    let tree = [
        Item::file(b"a", b"old"),
        Item::file(b"b", b"other"),
        Item::directory(b"e"),
        Item::directory(b"f"),
        Item::file(b"f/g", b"g"),
        Item::directory(b"h"),
        Item::directory(b"d"),
        Item::file(b"d/c", b"c"),
        Item::file(b"target", b"stale"),
    ];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);
    let over_a_file = run.rename(process, (root, b"a"), (root, b"b"));
    let moved = [run.contents(process, root, b"b"), run.contents(process, root, b"a")];
    let d = run.open(process, root, b"d", OpenHow::Directory).expect("a directory");
    let across = run.rename(process, (root, b"b"), (d, b"x"));
    let across_read = run.contents(process, root, b"d/x");
    let missing = run.rename(process, (root, b"missing"), (root, b"y"));
    let file_over_directory = run.rename(process, (d, b"c"), (root, b"e"));
    let directory_over_file = run.rename(process, (root, b"e"), (d, b"c"));
    let over_a_full_directory = run.rename(process, (root, b"h"), (root, b"f"));
    let over_an_empty_directory = run.rename(process, (root, b"e"), (root, b"h"));
    let after_over_empty =
        [opens(&mut run, process, root, b"h", OpenHow::Directory), opens(&mut run, process, root, b"e", OpenHow::Read)];
    let beneath_itself = run.rename(process, (root, b"d"), (d, b"dd"));
    let over_itself = run.rename(process, (root, b"target"), (root, b"target"));
    let over_itself_read = run.contents(process, root, b"target");
    let too_long = run.rename(process, (root, &too_long()), (root, b"y"));

    let tmp = run.open(process, root, b"tmp", OpenHow::Create).expect("a temporary");
    run.write_all(process, tmp, 0, b"fresh").expect("the temporary written");
    let synced = run.sync(process, tmp);
    run.close(process, tmp);
    let renamed = run.rename(process, (root, b"tmp"), (root, b"target"));
    let directory_synced = run.sync(process, root);
    let replaced_read = [run.contents(process, root, b"target"), run.contents(process, root, b"tmp")];
    run.close(process, d);
    run.close(process, root);
    run.finish();
    Renames {
        over_a_file,
        moved,
        across,
        across_read,
        missing,
        file_over_directory,
        directory_over_file,
        over_a_full_directory,
        over_an_empty_directory,
        after_over_empty,
        beneath_itself,
        over_itself,
        over_itself_read,
        too_long,
        replaced: [synced, renamed, directory_synced],
        replaced_read,
    }
}

/// Opens `path` and closes it: whether it opened.
fn opens<B: Backend>(
    run: &mut Run<'_, B>,
    process: B::Process,
    root: Fd,
    path: &[u8],
    how: OpenHow,
) -> Result<(), Error> {
    let fd = run.open(process, root, path, how)?;
    run.close(process, fd);
    Ok(())
}

impl Check for Renames {
    fn check(&self) {
        assert_eq!(self.over_a_file, NOTHING, "the contract: Rename replaces a file with a file");
        assert_eq!(self.moved, [file(b"old"), Err(Error::NotFound)], "the contract: Rename moves the entry");
        assert_eq!(self.across, NOTHING, "the contract: Rename moves an entry to another directory");
        assert_eq!(self.across_read, file(b"old"), "the contract: Rename moves the entry");
        assert_eq!(self.missing, Err(Error::NotFound), "the contract: Rename of an entry that does not exist");
        assert_eq!(self.file_over_directory, Err(Error::IsADirectory), "the contract: a file over a directory");
        assert_eq!(self.directory_over_file, Err(Error::NotADirectory), "the contract: a directory over a file");
        assert_eq!(self.over_a_full_directory, Err(Error::NotEmpty), "the contract: over a directory with entries");
        assert_eq!(self.over_an_empty_directory, NOTHING, "the contract: an empty directory with a directory");
        assert_eq!(self.after_over_empty, [Ok(()), Err(Error::NotFound)], "the contract: Rename moves the entry");
        assert_eq!(self.beneath_itself, Err(Error::InvalidArgument), "the contract: never beneath itself");
        assert_eq!(self.over_itself, NOTHING, "the contract: an entry renamed over itself is left as it is");
        assert_eq!(self.over_itself_read, file(b"stale"), "the contract: left as it is");
        assert_eq!(self.too_long, Err(Error::NameTooLong), "the contract: a name longer than 255 bytes");
        assert_eq!(self.replaced, [NOTHING, NOTHING, NOTHING], "the contract: the idiom that replaces a file whole");
        let replaced = [file(b"fresh"), Err(Error::NotFound)];
        assert_eq!(self.replaced_read, replaced, "the contract: Rename is atomic, and replaces what `to` named");
    }
}

/// Removes: a file, twice; a directory, as one and not; a full one; a file
/// as a directory; a symbolic link, its target kept; a file open; a name
/// too long.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Removes {
    pub file: [Result<Done, Error>; 2],
    /// An empty directory without `directory`, then with it.
    pub empty_directory: [Result<Done, Error>; 2],
    pub full_directory: Result<Done, Error>,
    pub file_as_directory: Result<Done, Error>,
    pub link: Result<Done, Error>,
    pub link_target: Result<Vec<u8>, Error>,
    pub open_file: Result<Done, Error>,
    /// What the open file's descriptor still reads, and its name after.
    pub open_file_read: [Result<Vec<u8>, Error>; 2],
    pub too_long: Result<Done, Error>,
}

#[must_use]
pub fn remove<B: Backend>(backend: &mut B) -> Removes {
    let tree = [
        Item::file(b"f", b"f"),
        Item::directory(b"e"),
        Item::directory(b"n"),
        Item::file(b"n/x", b"x"),
        Item::file(b"g", b"g"),
        Item::link(b"l", b"g"),
        Item::file(b"o", b"open"),
    ];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);
    let file = [run.remove(process, root, b"f", false), run.remove(process, root, b"f", false)];
    let empty_directory = [run.remove(process, root, b"e", false), run.remove(process, root, b"e", true)];
    let full_directory = run.remove(process, root, b"n", true);
    let n = run.open(process, root, b"n", OpenHow::Directory).expect("a directory");
    let file_as_directory = run.remove(process, n, b"x", true);
    run.close(process, n);
    let link = run.remove(process, root, b"l", false);
    let link_target = run.contents(process, root, b"g");
    let o = run.open(process, root, b"o", OpenHow::Read).expect("a file");
    let open_file = run.remove(process, root, b"o", false);
    let (still, _) = run.read_all(process, o);
    run.close(process, o);
    let open_file_read = [still, run.contents(process, root, b"o")];
    let too_long = run.remove(process, root, &too_long(), false);
    run.close(process, root);
    run.finish();
    Removes {
        file,
        empty_directory,
        full_directory,
        file_as_directory,
        link,
        link_target,
        open_file,
        open_file_read,
        too_long,
    }
}

impl Check for Removes {
    fn check(&self) {
        assert_eq!(self.file, [NOTHING, Err(Error::NotFound)], "the contract: Remove of a file, then of nothing");
        let directory = [Err(Error::IsADirectory), NOTHING];
        assert_eq!(
            self.empty_directory, directory,
            "the contract: Remove of a directory, without and with `directory`"
        );
        assert_eq!(self.full_directory, Err(Error::NotEmpty), "the contract: Remove of a directory with entries");
        assert_eq!(
            self.file_as_directory,
            Err(Error::NotADirectory),
            "the contract: Remove with `directory` of a file"
        );
        assert_eq!(self.link, NOTHING, "the contract: removing a symbolic link acts on the link");
        assert_eq!(self.link_target, file(b"g"), "the contract: removing a symbolic link acts on the link");
        assert_eq!(self.open_file, NOTHING, "the contract: Remove of a file open");
        let read = [file(b"open"), Err(Error::NotFound)];
        assert_eq!(self.open_file_read, read, "the contract: a file removed while open stays readable");
        assert_eq!(self.too_long, Err(Error::NameTooLong), "the contract: a name longer than 255 bytes");
    }
}

/// New directories: one made, twice; over a file; in a file; a name too
/// long; and in a directory removed while open, with an `Open` beneath it
/// and a `List` of it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MakeDirectories {
    pub made: [Result<Done, Error>; 2],
    pub made_listed: Result<Vec<(Vec<u8>, Kind)>, Error>,
    pub over_a_file: Result<Done, Error>,
    pub in_a_file: Result<Done, Error>,
    pub too_long: Result<Done, Error>,
    /// The directory removed while open; then a `MakeDirectory` in it, an
    /// `Open` to create beneath it, and a `List` of it.
    pub removed: Result<Done, Error>,
    pub in_removed: [Result<Done, Error>; 3],
}

#[must_use]
pub fn make_directory<B: Backend>(backend: &mut B) -> MakeDirectories {
    let tree = [Item::file(b"a.txt", b"a"), Item::directory(b"gone")];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);
    let made = [run.make_directory(process, root, b"m"), run.make_directory(process, root, b"m")];
    let m = run.open(process, root, b"m", OpenHow::Directory).expect("the directory made");
    let made_listed = run.list(process, m, 4, NAMES);
    run.close(process, m);
    let over_a_file = run.make_directory(process, root, b"a.txt");
    let a = run.open(process, root, b"a.txt", OpenHow::Read).expect("a file");
    let in_a_file = run.make_directory(process, a, b"x");
    run.close(process, a);
    let too_long = run.make_directory(process, root, &too_long());
    let gone = run.open(process, root, b"gone", OpenHow::Directory).expect("a directory");
    let removed = run.remove(process, root, b"gone", true);
    let create = match run.open(process, gone, b"x", OpenHow::Create) {
        Ok(fd) => {
            run.close(process, fd);
            NOTHING
        }
        Err(error) => Err(error),
    };
    let listed = match run.list(process, gone, 4, NAMES) {
        Ok(listed) => unexpected("a List of a directory removed fails", &listed),
        Err(error) => Err(error),
    };
    let in_removed = [run.make_directory(process, gone, b"x"), create, listed];
    run.close(process, gone);
    run.close(process, root);
    run.finish();
    MakeDirectories { made, made_listed, over_a_file, in_a_file, too_long, removed, in_removed }
}

impl Check for MakeDirectories {
    fn check(&self) {
        assert_eq!(self.made, [NOTHING, Err(Error::Exists)], "the contract: MakeDirectory, then of a name taken");
        assert_eq!(self.made_listed, Ok(Vec::new()), "the contract: a new directory is empty");
        assert_eq!(self.over_a_file, Err(Error::Exists), "the contract: MakeDirectory of a name a file takes");
        assert_eq!(self.in_a_file, Err(Error::NotADirectory), "the contract: the descriptor is not a directory's");
        assert_eq!(self.too_long, Err(Error::NameTooLong), "the contract: a name longer than 255 bytes");
        assert_eq!(self.removed, NOTHING, "the contract: Remove of an empty directory open");
        for answer in &self.in_removed {
            assert_eq!(*answer, Err(Error::NotFound), "the contract: a directory removed while open is empty for good");
        }
    }
}

/// Listings: a directory's every entry and kind, at once and one at a time;
/// the end, and the end again; an empty directory; long names one a `List`
/// in the least names; a file.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Listing {
    pub whole: Result<Entries, Error>,
    pub again_at_the_end: Result<Vec<(Vec<u8>, Kind)>, Error>,
    pub singly: Result<(Entries, Vec<usize>), Error>,
    pub empty: Result<Vec<(Vec<u8>, Kind)>, Error>,
    /// Two names of 200 bytes, in names of 255: how many each `List` took.
    pub long_names: Result<Vec<usize>, Error>,
    pub of_a_file: Result<Vec<(Vec<u8>, Kind)>, Error>,
}

#[must_use]
pub fn list<B: Backend>(backend: &mut B) -> Listing {
    let long_a = vec![b'a'; 200];
    let long_b = vec![b'b'; 200];
    let tree = [
        Item::directory(b"l"),
        Item::file(b"l/one", b"1"),
        Item::file(b"l/two", b"2"),
        Item::directory(b"l/three"),
        Item::link(b"l/four", b"one"),
        Item::directory(b"e"),
        Item::directory(b"n"),
        Item::file(&[b"n/".as_slice(), &long_a].concat(), b"a"),
        Item::file(&[b"n/".as_slice(), &long_b].concat(), b"b"),
        Item::file(b"x", b"x"),
    ];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);
    let l = run.open(process, root, b"l", OpenHow::Directory).expect("a directory");
    let whole = run.list_all(process, l, 16).map(|(all, _)| all);
    let again_at_the_end = run.list(process, l, 16, NAMES);
    run.close(process, l);
    let l = run.open(process, root, b"l", OpenHow::Directory).expect("a directory");
    let singly = run.list_all(process, l, 1);
    run.close(process, l);
    let e = run.open(process, root, b"e", OpenHow::Directory).expect("a directory");
    let empty = run.list(process, e, 4, NAMES);
    run.close(process, e);
    let n = run.open(process, root, b"n", OpenHow::Directory).expect("a directory");
    let long_names = run.list_all(process, n, 4).map(|(_, counts)| counts);
    run.close(process, n);
    let x = run.open(process, root, b"x", OpenHow::Read).expect("a file");
    let of_a_file = run.list(process, x, 4, NAMES);
    run.close(process, x);
    run.close(process, root);
    run.finish();
    Listing { whole, again_at_the_end, singly, empty, long_names, of_a_file }
}

impl Check for Listing {
    fn check(&self) {
        let expected = entries(&[
            (b"one", Kind::File),
            (b"two", Kind::File),
            (b"three", Kind::Directory),
            (b"four", Kind::Symlink),
        ]);
        assert_eq!(self.whole, Ok(expected.clone()), "the contract: List hands back every entry and its kind");
        assert_eq!(self.again_at_the_end, Ok(Vec::new()), "the contract: Count(0) at the end, and after it");
        let Ok((singly, counts)) = &self.singly else {
            unexpected("a directory listed one entry at a time", &self.singly);
        };
        assert_eq!(*singly, expected, "the contract: each List from where the last stopped");
        rule(counts.iter().all(|&count| count == 1), "at most entries.len() a List", counts);
        assert_eq!(self.empty, Ok(Vec::new()), "the contract: an empty directory lists nothing");
        let long = Ok(vec![1, 1]);
        assert_eq!(self.long_names, long, "the contract: a List stops short when the next name does not fit");
        assert_eq!(self.of_a_file, Err(Error::NotADirectory), "the contract: a List of a non-directory");
    }
}

/// A root opened beneath a root: what it reaches, and what it does not,
/// though its parent holds it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Nested {
    pub inner: Result<Vec<u8>, Error>,
    pub up: Result<(), Error>,
    pub down_and_up: Result<Vec<u8>, Error>,
    pub down_and_out: Result<(), Error>,
    pub from_the_outer: Result<Vec<u8>, Error>,
    /// A file made beneath the nested root, read from the outer.
    pub made: Result<Vec<u8>, Error>,
    /// A root beneath the nested one: a file in it, and its `..`.
    pub deeper: [Result<Vec<u8>, Error>; 2],
}

#[must_use]
pub fn nested_roots<B: Backend>(backend: &mut B) -> Nested {
    let tree = [
        Item::file(b"top", b"top"),
        Item::directory(b"sub"),
        Item::file(b"sub/inner", b"inner"),
        Item::directory(b"sub/deeper"),
        Item::file(b"sub/deeper/leaf", b"leaf"),
    ];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);
    let nested = run.open(process, root, b"sub", OpenHow::Directory).expect("a root beneath the root");
    let inner = run.contents(process, nested, b"inner");
    let up = opens(&mut run, process, nested, b"../top", OpenHow::Read);
    let down_and_up = run.contents(process, nested, b"deeper/../inner");
    let down_and_out = opens(&mut run, process, nested, b"deeper/../../top", OpenHow::Read);
    let from_the_outer = run.contents(process, root, b"sub/inner");
    let made = opens(&mut run, process, nested, b"made", OpenHow::Create);
    let made = made.and_then(|()| run.contents(process, root, b"sub/made"));
    let deeper = run.open(process, nested, b"deeper", OpenHow::Directory).expect("a root beneath that one");
    let leaf = run.contents(process, deeper, b"leaf");
    let above = opens(&mut run, process, deeper, b"..", OpenHow::Directory).map(|()| Vec::new());
    run.close(process, deeper);
    run.close(process, nested);
    run.close(process, root);
    run.finish();
    Nested { inner, up, down_and_up, down_and_out, from_the_outer, made, deeper: [leaf, above] }
}

impl Check for Nested {
    fn check(&self) {
        assert_eq!(self.inner, file(b"inner"), "the contract: a root beneath a root is a root like any other");
        assert_eq!(self.up, Err(Error::Escape), "the contract: `..` above the root, though its parent holds it");
        assert_eq!(self.down_and_up, file(b"inner"), "the contract: `..` that stays beneath the root");
        assert_eq!(self.down_and_out, Err(Error::Escape), "the contract: `..` above the root");
        assert_eq!(self.from_the_outer, file(b"inner"), "the root beneath is the outer's directory");
        assert_eq!(self.made, file(b""), "a file made beneath a root is its parent's too");
        assert_eq!(self.deeper, [file(b"leaf"), Err(Error::Escape)], "the contract: each root keeps its own");
    }
}

/// What an `Open` came to: a file's bytes, read whole, or that it opened
/// (a directory, or a file made), or its error.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Outcome {
    File(Vec<u8>),
    Opened,
    Refused(Error),
}

/// Each path the escapes scenario opens, how, and what the kernel answers.
fn escape_cases() -> Vec<(Vec<u8>, OpenHow, Outcome)> {
    let refused = Outcome::Refused;
    let a = || Outcome::File(b"a".to_vec());
    let long_name = [b"sub/".as_slice(), &too_long()].concat();
    let long_path = b"sub/".repeat(1025);
    vec![
        (b"..".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"../a.txt".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"sub/../../a.txt".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"/etc/passwd".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"abs".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"abs/passwd".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"out".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"up".to_vec(), OpenHow::Read, refused(Error::Escape)),
        (b"in".to_vec(), OpenHow::Read, a()),
        (b"chain".to_vec(), OpenHow::Read, a()),
        (b"sub/../a.txt".to_vec(), OpenHow::Read, a()),
        (b"sub//../a.txt".to_vec(), OpenHow::Read, a()),
        (b"loop1".to_vec(), OpenHow::Read, refused(Error::TooManyLinks)),
        (b"dangle".to_vec(), OpenHow::Read, refused(Error::NotFound)),
        (b"dangle".to_vec(), OpenHow::Create, refused(Error::Exists)),
        (b"in".to_vec(), OpenHow::Create, refused(Error::Exists)),
        (b"sub".to_vec(), OpenHow::Create, refused(Error::Exists)),
        (b".".to_vec(), OpenHow::Create, refused(Error::Exists)),
        (b"new/".to_vec(), OpenHow::Create, refused(Error::IsADirectory)),
        (b"sub/new".to_vec(), OpenHow::Create, Outcome::Opened),
        (b".".to_vec(), OpenHow::Directory, Outcome::Opened),
        (b"sub/..".to_vec(), OpenHow::Directory, Outcome::Opened),
        (b"sub/".to_vec(), OpenHow::Directory, Outcome::Opened),
        (b"a.txt".to_vec(), OpenHow::Directory, refused(Error::NotADirectory)),
        (b"a.txt/".to_vec(), OpenHow::Read, refused(Error::NotADirectory)),
        (b"a.txt/x".to_vec(), OpenHow::Read, refused(Error::NotADirectory)),
        (b"missing".to_vec(), OpenHow::Read, refused(Error::NotFound)),
        (b"missing/x".to_vec(), OpenHow::Read, refused(Error::NotFound)),
        (b"missing/x".to_vec(), OpenHow::Create, refused(Error::NotFound)),
        (Vec::new(), OpenHow::Read, refused(Error::NotFound)),
        (long_name, OpenHow::Read, refused(Error::NameTooLong)),
        (long_path, OpenHow::Read, refused(Error::NameTooLong)),
    ]
}

/// Paths that leave their root, and paths that stay: `..`, absolute paths,
/// symbolic links out and in, a loop, a dangling link, a final `/`, names
/// and paths too long.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Escapes {
    pub outcomes: Vec<(Vec<u8>, OpenHow, Outcome)>,
}

#[must_use]
pub fn escapes<B: Backend>(backend: &mut B) -> Escapes {
    let tree = [
        Item::file(b"a.txt", b"a"),
        Item::directory(b"sub"),
        Item::link(b"abs", b"/etc"),
        // The ring lays a directory `outside` beside the root.
        Item::link(b"out", b"../outside/secret"),
        Item::link(b"up", b"sub/../.."),
        Item::link(b"in", b"a.txt"),
        Item::link(b"chain", b"sub/../in"),
        Item::link(b"loop1", b"loop2"),
        Item::link(b"loop2", b"loop1"),
        Item::link(b"dangle", b"missing"),
    ];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);
    let mut outcomes = Vec::new();
    for (path, how, _) in escape_cases() {
        let outcome = match (run.open(process, root, &path, how), how) {
            (Ok(fd), OpenHow::Read) => {
                let (read, _) = run.read_all(process, fd);
                run.close(process, fd);
                match read {
                    Ok(bytes) => Outcome::File(bytes),
                    Err(error) => Outcome::Refused(error),
                }
            }
            (Ok(fd), OpenHow::Directory | OpenHow::Create) => {
                run.close(process, fd);
                Outcome::Opened
            }
            (Err(error), _) => Outcome::Refused(error),
        };
        outcomes.push((path, how, outcome));
    }
    run.close(process, root);
    run.finish();
    Escapes { outcomes }
}

impl Check for Escapes {
    fn check(&self) {
        for ((path, how, seen), (_, _, expected)) in self.outcomes.iter().zip(escape_cases()) {
            let shown = path.escape_ascii().to_string();
            assert_eq!(
                *seen, expected,
                "the contract: Open resolves beneath its root, following the links that stay there: {shown:.80} {how:?}"
            );
        }
    }
}

/// What the owner may not do: read a file of mode 0, write a directory of
/// mode `0o555`, search one of mode `0o600`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Permissions {
    pub unreadable: Result<(), Error>,
    /// In the directory that may not be written: an `Open` to create, a
    /// `Remove`, a `MakeDirectory`, a `Rename` out of it.
    pub unwritable: [Result<Done, Error>; 4],
    /// What may still be done there: read a file, list it.
    pub readable: Result<Vec<u8>, Error>,
    pub listed: Result<Vec<(Vec<u8>, Kind)>, Error>,
    /// Through the directory that may not be searched: an `Open` of a path
    /// across it, and one beneath it opened as a root.
    pub unsearchable: [Result<(), Error>; 2],
    /// What may still be done with it: open it, list it, stat it.
    pub listable: Result<Entries, Error>,
    pub stat: Result<Kind, Error>,
}

#[must_use]
pub fn permissions<B: Backend>(backend: &mut B) -> Permissions {
    let tree = [
        Item::file(b"locked", b"secret").mode(0o000),
        Item::directory(b"ro").mode(0o555),
        Item::file(b"ro/keep", b"keep"),
        Item::directory(b"closed").mode(0o600),
        Item::file(b"closed/inside", b"inside"),
    ];
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &tree);
    let unreadable = opens(&mut run, process, root, b"locked", OpenHow::Read);
    let create = opens(&mut run, process, root, b"ro/new", OpenHow::Create).map(|()| Done::Nothing);
    let ro = run.open(process, root, b"ro", OpenHow::Directory).expect("a directory that may be read");
    let unwritable = [
        create,
        run.remove(process, ro, b"keep", false),
        run.make_directory(process, ro, b"m"),
        run.rename(process, (ro, b"keep"), (root, b"moved")),
    ];
    let listed = run.list(process, ro, 4, NAMES);
    let readable = run.contents(process, root, b"ro/keep");
    run.close(process, ro);
    let across = opens(&mut run, process, root, b"closed/inside", OpenHow::Read);
    let closed = run.open(process, root, b"closed", OpenHow::Directory).expect("a directory that may be read");
    let beneath = opens(&mut run, process, closed, b"inside", OpenHow::Read);
    let listable = run.list_all(process, closed, 4).map(|(all, _)| all);
    let stat = run.stat(process, closed).map(|stat| stat.kind);
    run.close(process, closed);
    run.close(process, root);
    run.finish();
    Permissions { unreadable, unwritable, readable, listed, unsearchable: [across, beneath], listable, stat }
}

impl Check for Permissions {
    fn check(&self) {
        assert_eq!(self.unreadable, Err(Error::Permission), "the contract: Read opens only what may be read");
        for answer in &self.unwritable {
            let rule = "the contract: an entry made, removed or moved in a directory not writable";
            assert_eq!(*answer, Err(Error::Permission), "{rule}");
        }
        assert_eq!(self.readable, file(b"keep"), "a directory not writable is still searched");
        assert_eq!(self.listed, Ok(vec![(b"keep".to_vec(), Kind::File)]), "and listed");
        for answer in &self.unsearchable {
            assert_eq!(*answer, Err(Error::Permission), "the contract: a directory on the path may not be searched");
        }
        let inside = Ok(entries(&[(b"inside", Kind::File)]));
        assert_eq!(self.listable, inside, "a directory that may not be searched is still listed");
        assert_eq!(self.stat, Ok(Kind::Directory), "and stated");
    }
}

/// Past the descriptor limit, which only the simulator can set low: an
/// `Open` fails, and the next succeeds once a descriptor is free. Needs a
/// backend whose processes hold fewer than 64 descriptors.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OpenLimit {
    pub past: Result<(), Error>,
    pub then: Result<(), Error>,
}

#[must_use]
pub fn open_past_the_descriptor_limit<B: Backend>(backend: &mut B) -> OpenLimit {
    let mut run = Run::new(backend);
    let process = run.process();
    let root = run.root(process, &[Item::file(b"a", b"a")]);
    let mut open = Vec::new();
    let mut past = Ok(());
    for _ in 0..64_u32 {
        match run.open(process, root, b"a", OpenHow::Read) {
            Ok(fd) => open.push(fd),
            Err(error) => {
                past = Err(error);
                break;
            }
        }
    }
    if let Some(fd) = open.pop() {
        run.close(process, fd);
    }
    let then = opens(&mut run, process, root, b"a", OpenHow::Read);
    for fd in open {
        run.close(process, fd);
    }
    run.close(process, root);
    run.finish();
    OpenLimit { past, then }
}

impl Check for OpenLimit {
    fn check(&self) {
        assert_eq!(self.past, Err(Error::TooManyOpenFiles), "the contract: an Open past the descriptor limit");
        assert_eq!(self.then, Ok(()), "the contract: an Open once a descriptor is free");
    }
}
