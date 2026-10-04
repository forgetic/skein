//! The ring adapter's own `List`, called directly on a directory a test lays
//! out, for what no valid record can make it meet on Linux's filesystems,
//! whose names fit the least `names` a record holds: a next name longer
//! than all of `names`.

use std::fs;

use skein_io::kernel::Entry;
use skein_scratch::Scratch;

use crate::ring::{close, list, open_root};

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
