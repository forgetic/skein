//! A scratch directory, for tests on the real kernel (testing.md, 6): made
//! empty beneath the system's temporary directory, and removed when
//! dropped, whatever it holds and whatever modes a test gave what is in it,
//! a failing test's included. The conformance suite lays out a root there
//! for each scenario on files (`tests/conformance/ring`; kernel.md, 8), and
//! the ring's own tests their files (`tests/ring`).
//!
//! Ordinary Rust (programming-model.md, 10.2): it runs in tests only.

#![forbid(unsafe_code)]

use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::thread;

/// How many names a new scratch directory tries before it gives up.
const TRIES: u32 = 1000;

/// A directory of its own, removed when dropped.
#[derive(Debug)]
pub struct Scratch {
    path: PathBuf,
}

impl Scratch {
    /// A new, empty directory, `skein-<label>-<pid>-<n>` beneath the
    /// system's temporary directory, `n` the first not taken.
    #[must_use]
    pub fn new(label: &str) -> Scratch {
        let base = std::env::temp_dir();
        let pid = std::process::id();
        for n in 0..TRIES {
            let path = base.join(format!("skein-{label}-{pid}-{n}"));
            match fs::create_dir(&path) {
                Ok(()) => return Scratch { path },
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => fail(&format!("a scratch directory beneath {}: {error}", base.display())),
            }
        }
        fail(&format!("no scratch directory name free beneath {} in {TRIES} tries", base.display()))
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Removes the directory and all it holds, every directory beneath it given
/// back its owner's permissions first. A failure is loud, unless the test
/// is already failing.
impl Drop for Scratch {
    fn drop(&mut self) {
        let removed = reopen(&self.path).and_then(|()| fs::remove_dir_all(&self.path));
        if let Err(error) = removed
            && !thread::panicking()
        {
            fail(&format!("the scratch directory {} was not removed: {error}", self.path.display()));
        }
    }
}

/// Fails the test, loudly.
#[expect(clippy::panic, reason = "a test's scratch directory fails the test, as an assertion does")]
fn fail(what: &str) -> ! {
    panic!("{what}");
}

/// Gives `dir`, and every directory beneath it, its owner's read, write and
/// search permissions, so that it can be emptied. Symbolic links are not
/// followed: what they lead to is not the scratch directory's.
fn reopen(dir: &Path) -> std::io::Result<()> {
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            reopen(&entry.path())?;
        }
    }
    Ok(())
}
