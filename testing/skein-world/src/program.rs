//! Program selection shared by simulated and real worlds (examples.md,
//! section 6; simulator.md, section 3). A registry keeps factories and their
//! admission limits, never service state. Factories receive a spawn's exact
//! arguments and environment, inherited pipes, independently opened startup
//! roots and a signal source.

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_io::kernel::{Fd, Spawn};

/// Inherited descriptors supplied by a harness to a hosted program.
#[derive(Debug)]
pub struct Inherited {
    /// Requested child descriptor numbers paired with backend descriptors.
    pub pipes: Vec<(u32, Fd)>,
    pub signal: Fd,
    /// Named directory descriptors, each owned by this child until close or exit.
    pub roots: Vec<(Box<[u8]>, Fd)>,
    /// Named append files, each owned by this child until close or exit.
    pub appends: Vec<(Box<[u8]>, Fd)>,
}

/// A program the scenario asks a harness to host, ending at the host's exit.
#[derive(Debug)]
pub struct HostedProgram<P> {
    pub program: Box<[u8]>,
    pub make: fn(&Spawn, &Inherited) -> P,
    /// Maximum simultaneous instances, used to provision the real ring.
    pub instances: u32,
    /// Maximum operations per instance, checked against the constructed host.
    pub operations: u32,
}

/// A named directory the scenario asks each child to open at startup.
/// Paths use the scenario's filesystem namespace in both backends.
#[derive(Debug)]
pub struct StartupRoot {
    pub name: Box<[u8]>,
    pub path: Box<[u8]>,
}

/// Selects a child's startup directories from its exact launch configuration.
pub type StartupRoots = fn(&Spawn) -> Vec<StartupRoot>;

/// An append file opened beneath a startup root for each hosted launch.
#[derive(Debug)]
pub struct StartupAppend {
    pub name: Box<[u8]>,
    /// Filesystem path of the directory opened temporarily for this file.
    pub root: Box<[u8]>,
    /// The file's path beneath that directory.
    pub path: Box<[u8]>,
    /// Creation permissions; an existing file retains its mode and contents.
    pub mode: u32,
}

/// Selects a child's startup append files from its exact launch configuration.
pub type StartupAppends = fn(&Spawn) -> Vec<StartupAppend>;

#[derive(Debug)]
pub(crate) struct Startup {
    pub(crate) roots: StartupRoots,
    pub(crate) appends: StartupAppends,
}

pub(crate) fn no_appends(_spawn: &Spawn) -> Vec<StartupAppend> {
    Vec::new()
}

pub(crate) fn check_appends(appends: &[StartupAppend]) {
    for (index, append) in appends.iter().enumerate() {
        assert!(!append.name.is_empty(), "startup append files have names");
        assert!(!appends.iter().take(index).any(|other| other.name == append.name), "startup append names are unique");
    }
}

pub(crate) fn no_roots(_spawn: &Spawn) -> Vec<StartupRoot> {
    Vec::new()
}

pub(crate) fn check_roots(roots: &[StartupRoot]) {
    for (index, root) in roots.iter().enumerate() {
        assert!(!root.name.is_empty(), "startup roots have names");
        assert!(!roots.iter().take(index).any(|other| other.name == root.name), "startup root names are unique");
    }
}

/// Machine calls a simulated world forwards when no hosted program matches.
pub trait Machine {
    /// Settles the cut's handles; power loss applies the machine's seeded
    /// crash model (simulator.md, 3.3). A cutting world implements this.
    fn cut(&mut self, _cut: crate::Cut, _held: &[skein_sim::Handle], _seed: u64) {
        crate::fail("a world with cuts implements its machine's cut settlement");
    }

    /// Opens a fresh independently owned directory handle for a startup path.
    /// A machine with no startup directories refuses by default.
    fn open_root(&mut self, _path: &[u8]) -> Result<skein_sim::Handle, skein_io::kernel::Error> {
        Err(skein_io::kernel::Error::NotFound)
    }

    /// Releases a startup handle not admitted because a later open failed.
    /// Machines that implement `open_root` must also implement this rollback.
    fn close_root(&mut self, _root: skein_sim::Handle) {
        crate::fail("a machine that opens startup roots implements their rollback");
    }

    /// Opens an append file beneath a startup root, without admitting a child.
    fn open_append(
        &mut self,
        _root: skein_sim::Handle,
        _path: &[u8],
        _mode: u32,
    ) -> Result<skein_sim::Handle, skein_io::kernel::Error> {
        Err(skein_io::kernel::Error::NotFound)
    }

    /// Rolls back a file opened for a launch that failed before admission.
    /// The default uses the same handle release as startup roots.
    fn close_append(&mut self, file: skein_sim::Handle) {
        self.close_root(file);
    }

    fn step(&mut self, call: skein_sim::Call, answers: &mut skein_lib::Queue<skein_sim::Answer>);
}

/// The default world has no external file or program operations.
#[derive(Debug)]
pub struct NoMachine;

impl Machine for NoMachine {
    fn step(&mut self, call: skein_sim::Call, _answers: &mut skein_lib::Queue<skein_sim::Answer>) {
        crate::fail(&alloc::format!("a world with machine operations configures its machine: {call:?}"));
    }
}
