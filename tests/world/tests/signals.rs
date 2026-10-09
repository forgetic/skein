//! Referee signals use the same controls in simulated and real worlds.

use skein_io::kernel::{ServiceSignal, Spawn};
use skein_lib::Time;
use skein_sim::{Config, Handle};
use skein_world::{Controls, HostedProgram, Inherited, Memory, Referee, World};
use skein_world_tests::hosted::{Act, RootMachine, Script};

#[derive(Debug)]
struct Judge {
    controls: Controls,
    target: usize,
    sent: bool,
    passed: bool,
    ready: bool,
}

impl Referee<Script> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Script]) {
        if !self.sent && (procs.len() > self.target) {
            let host = self.target;
            self.controls.signal(host, ServiceSignal::Terminate);
            self.controls.signal(host, ServiceSignal::Interrupt);
            self.sent = true;
        }
    }
    fn observe(&mut self, _now: Time, procs: &[Script]) {
        self.ready = procs.len() > self.target;
        if let Some(script) = procs.get(self.target) {
            self.passed = script.signals == [ServiceSignal::Terminate, ServiceSignal::Interrupt];
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        (self.ready && !self.sent).then_some(Time::ZERO)
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        self.passed
    }
}

fn child(_spawn: &Spawn, inherited: &Inherited) -> Script {
    Script::child(inherited, &[Act::ReadSignal, Act::ReadSignal, Act::Write(1, b"heard")])
}

fn run(hosted: bool) -> skein_world::Outcome<Script, RootMachine> {
    let mut world = World::new_controlled(7, Config::calm(), Memory::Unchecked, |controls| Judge {
        controls,
        target: usize::from(hosted),
        sent: false,
        passed: false,
        ready: !hosted,
    })
    .with_machine(RootMachine);
    if hosted {
        world.host(HostedProgram { program: Box::from(&b"hosted"[..]), make: child, instances: 1, operations: 8 });
        world.spawn_root(Handle::new(1), |root| {
            Script::parent(root, &[Act::Spawn, Act::Read(1), Act::Read(1), Act::Wait])
        });
    } else {
        world.spawn_signals(|signal| {
            Script::child(&Inherited { pipes: vec![], roots: vec![], signal }, &[Act::ReadSignal, Act::ReadSignal])
        });
    }
    world.run()
}

#[test]
fn a_script_hears_the_referees_signals_in_order_with_its_other_completions() {
    for hosted in [false, true] {
        let first = run(hosted);
        let second = run(hosted);
        assert_eq!(first.procs[usize::from(hosted)].signals, [ServiceSignal::Terminate, ServiceSignal::Interrupt]);
        assert_eq!(first.trace, second.trace, "referee signal delivery replays");
        if hosted {
            assert_eq!(first.procs[0].received, b"heard");
            assert_eq!(first.procs[0].child_exit, Some(skein_io::kernel::Exit::Code(0)));
        }
    }
}
