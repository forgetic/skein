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
