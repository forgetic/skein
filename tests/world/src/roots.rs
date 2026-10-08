//! Shared startup-root controls run unchanged over simulated and real files.

use skein_fake_machine::{How, Machine as Files, Opened};
use skein_io::kernel::{Error, OpenHow, Spawn};
use skein_lib::Queue;
use skein_sim::{Answer, Call, Handle};
use skein_world::{Inherited, Machine, StartupRoot};

use crate::hosted::{Act, Script};

/// The scenario's directory namespace, with independently opened handles.
#[derive(Debug)]
pub struct RootFiles {
    pub files: Files,
    base: Opened,
}

impl RootFiles {
    #[must_use]
    pub fn new() -> Self {
        let mut files = Files::new();
        let base = files.lay(&[]);
        Self { files, base }
    }

    pub fn parent_root(&mut self) -> Handle {
        Handle::new(self.files.open(self.base, b".", How::Directory).expect("parent root").raw())
    }

    pub fn finish(&mut self) {
        self.files.close(self.base);
        assert_eq!(self.files.open_handles(), 0, "all parent, child and rollback handles closed");
    }
}

impl Default for RootFiles {
    fn default() -> Self {
        Self::new()
    }
}

impl Machine for RootFiles {
    fn open_root(&mut self, path: &[u8]) -> Result<Handle, Error> {
        self.files
            .open(self.base, path, How::Directory)
            .map(|opened| Handle::new(opened.raw()))
            .map_err(|_| Error::NotFound)
    }

    fn close_root(&mut self, root: Handle) {
        self.files.close(Opened::new(root.raw()));
    }

    fn step(&mut self, call: Call, answers: &mut Queue<Answer>) {
        skein_fake_machine::step(&mut self.files, call, answers);
    }
}

/// Both names select the same directory, opened separately for each owner.
#[must_use]
pub fn roots(spawn: &Spawn) -> Vec<StartupRoot> {
    assert_eq!(spawn.env.as_ref(), &[Box::from(&b"KEY=value"[..])], "exact launch environment");
    vec![
        StartupRoot { name: Box::from(&b"launch"[..]), path: spawn.args[0].clone() },
        StartupRoot { name: Box::from(&b"workspace"[..]), path: spawn.args[0].clone() },
    ]
}

/// A later missing directory checks rollback of the first successful open.
#[must_use]
pub fn missing_roots(spawn: &Spawn) -> Vec<StartupRoot> {
    let mut selected = roots(spawn);
    selected[1].path = [spawn.args[0].as_ref(), b"/missing"].concat().into_boxed_slice();
    selected
}

/// The file script observes the child's roots only through kernel records.
#[must_use]
pub fn file_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    assert_eq!(inherited.roots.len(), 2);
    assert_eq!(inherited.roots[0].0.as_ref(), b"launch");
    assert_eq!(inherited.roots[1].0.as_ref(), b"workspace");
    assert_ne!(inherited.roots[0].1, inherited.roots[1].1, "two independent child descriptors");
    Script::child(
        inherited,
        &[
            Act::Read(0),
            Act::OpenRoot(0, b"note", OpenHow::Create { mode: Some(0o600) }),
            Act::WriteFile(2, b"from child"),
            Act::OpenRoot(1, b"note", OpenHow::Read),
            Act::ReadFile(3),
            Act::Write(1, b"ready"),
        ],
    )
}

/// Holds both roots open while the parent kills a waiting child.
#[must_use]
pub fn waiting_child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::Read(0), Act::Write(1, b"ready"), Act::Read(0)])
}

/// The parent closes its own root before the child starts file operations.
#[must_use]
pub fn parent(root: skein_io::kernel::Fd, path: &[u8], kill: bool) -> Script {
    let mut acts = vec![Act::Spawn, Act::CloseLaunch, Act::Write(0, b"go"), Act::Read(1)];
    if kill {
        acts.push(Act::Signal(skein_io::kernel::Signal::Kill));
    }
    acts.extend([Act::Read(1), Act::Wait]);
    let mut parent = Script::parent(root, &acts);
    parent.args = Box::new([Box::from(path)]);
    parent
}

/// A seeded root story with completion reordering, shared by replay and sweeps.
#[must_use]
pub fn simulated(seed: u64, kill: bool, missing: bool) -> skein_world::Outcome<Script, RootFiles> {
    let mut machine = RootFiles::new();
    let root = machine.parent_root();
    let faults = skein_sim::Faults {
        latency: 1000,
        latency_max: skein_lib::Duration::from_millis(1),
        ..skein_sim::Faults::NONE
    };
    let mut world = skein_world::World::new(
        seed,
        skein_sim::Config { faults, ..skein_sim::Config::calm() },
        crate::hosted::Judge,
        skein_world::Memory::Checked,
    )
    .with_machine(machine);
    world.host_roots(
        skein_world::HostedProgram {
            program: Box::from(&b"hosted"[..]),
            make: if kill { waiting_child } else { file_child },
            instances: 1,
            operations: 8,
        },
        if missing { missing_roots } else { roots },
    );
    world.spawn_root(root, |fd| {
        if missing {
            let mut script = Script::parent(fd, &[Act::Spawn]);
            script.args = Box::new([Box::from(&b"."[..])]);
            script
        } else {
            parent(fd, b".", kill)
        }
    });
    world.run()
}
