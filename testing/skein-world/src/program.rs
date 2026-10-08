//! Program selection shared by simulated and real worlds (examples.md,
//! section 6; simulator.md, section 3). A registry keeps factories and their
//! admission limits, never service state. Factories receive a spawn's exact
//! arguments and environment, inherited pipes and a signal source.

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_io::kernel::{Fd, Spawn};

/// Inherited descriptors supplied by a harness to a hosted program.
#[derive(Debug)]
pub struct Inherited {
    /// Requested child descriptor numbers paired with backend descriptors.
    pub pipes: Vec<(u32, Fd)>,
    pub signal: Fd,
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

/// Machine calls a simulated world forwards when no hosted program matches.
pub trait Machine {
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
