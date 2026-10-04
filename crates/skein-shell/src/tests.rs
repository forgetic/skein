//! The ring adapter's own parts no record shows: its `List`, called directly
//! on a directory a test lays out, for a next name longer than all of
//! `names`, which Linux's filesystems never give; and the flags a file it
//! opened is left with.

use std::fs;

use skein_io::kernel::{Complete, Done, Entry, Op, OpenHow, Submit};
use skein_lib::{Queue, Token};
use skein_scratch::Scratch;

use crate::ring::{Config, Kernel, Wait, close, list, open_root, status_flags};

#[test]
fn a_name_that_fits_in_no_names_is_too_long_and_stays_next() {
    let scratch = Scratch::new("shell");
    fs::write(scratch.path().join("a-name-of-twenty-bytes"), b"").expect("a file laid");
    let dir = open_root(scratch.path()).expect("a scratch directory opens");
    let mut listing = vec![0; 4096];
    let mut entries = [Entry::BLANK; 2];
    let mut small = [0_u8; 8];
    assert_eq!(list(dir.raw(), &mut entries, &mut small, &mut listing), Err(libc::ENAMETOOLONG));
    let mut names = [0_u8; 255];
    assert_eq!(list(dir.raw(), &mut entries, &mut names, &mut listing), Ok(1), "the name is still next");
    let first = entries.first().expect("an entry").name(&names);
    assert_eq!(first, Some(&b"a-name-of-twenty-bytes"[..]));
    assert_eq!(list(dir.raw(), &mut entries, &mut names, &mut listing), Ok(0), "then the end");
    close(dir.raw());
}

/// Every `Open` goes down without blocking, so that a FIFO opens to be
/// refused; a file it opened is blocking again, so that io_uring sends a
/// read it cannot do at once to its worker (kernel.md, 6.1).
#[test]
fn a_file_opened_blocks_again() {
    let scratch = Scratch::new("shell");
    fs::write(scratch.path().join("f"), b"f").expect("a file laid");
    let dir = open_root(scratch.path()).expect("a scratch directory opens");
    let mut kernel = Kernel::open(Config { operations: 4 }).expect("io_uring is usable here");
    let mut submissions = Queue::with_capacity(1);
    let path = Box::from(&b"f"[..]);
    submissions.push(Submit { op: Token::new(1), kind: Op::Open { root: dir, path, how: OpenHow::Read } });
    kernel.submit(&mut submissions, Wait::Forever);
    let mut completions = Queue::with_capacity(1);
    kernel.reap(&mut completions);
    let Some(Complete { result: Ok(Done::Fd(file)), .. }) = completions.pop() else { panic!("the file opened") };
    let flags = status_flags(file.raw()).expect("an open descriptor's flags");
    assert_eq!(flags & libc::O_NONBLOCK, 0, "the file blocks");
    close(file.raw());
    close(dir.raw());
}
