//! Groups, retained exits, reaped resources and the kernel's process invariants.
use skein_io::kernel::{Done, Exit, Fd, Op, Pipe, Resources, ServiceSignal, Signal, Spawn, Target, Usage, Way};
use skein_lib::{Duration, Queue};
use skein_sim::{Answer, Pid, Program, Reply};
use skein_sim_tests::World;

fn child(world: &mut World, parent: Pid, root: Fd, program: Program, output: bool) -> (Fd, Option<Fd>) {
    world.serving = false;
    let spawn = Spawn {
        root,
        dir: Box::from(b".".as_slice()),
        program: Box::from(b"modeled".as_slice()),
        args: Box::new([]),
        env: Box::new([]),
        pipes: if output { Box::from([Pipe { child: 1, way: Way::Out, parent: None }]) } else { Box::new([]) },
    };
    let token = world.submit(parent, Op::Spawn { spawn: Box::new(spawn) });
    let mut calls = Queue::with_capacity(1);
    world.sim.calls(&mut calls);
    let call = calls.pop().expect("the spawn asks the machine");
    let mut answers = Queue::with_capacity(1);
    answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(program)) });
    world.sim.answer(&mut answers);
    let complete = world.reap_one(parent, token);
    world.serving = true;
    let (Op::Spawn { spawn }, Ok(Done::Spawned { pidfd })) = (complete.kind, complete.result) else {
        panic!("spawn succeeded")
    };
    (pidfd, spawn.pipes.first().and_then(|pipe| pipe.parent))
}

fn rooted(program: Program) -> (World, Pid, Fd, Fd) {
    let mut world = World::calm();
    let parent = world.spawn();
    let root = world.root(parent, &[]);
    let (pidfd, _) = child(&mut world, parent, root, program, false);
    (world, parent, root, pidfd)
}

fn usage(world: &mut World, parent: Pid) -> Usage {
    let Ok(Done::Usage(usage)) = world.call(parent, Op::Usage).result else { panic!("usage succeeds") };
    usage
}

#[test]
fn a_group_signal_reaches_a_member_the_leader_started_before_and_after_its_exit() {
    for exit_leader in [false, true] {
        let mut world = World::calm();
        let parent = world.spawn();
        let root = world.root(parent, &[]);
        let (pidfd, output) = child(&mut world, parent, root, Program::Fork { exit_leader }, true);
        let output = output.expect("a requested output pipe");
        if exit_leader {
            world.call(parent, Op::Wait { pidfd, reap: false });
        }
        let reading = world.submit(parent, Op::PipeRead { fd: output, buf: Box::from([0_u8; 1]) });
        assert!(world.reap(parent).is_empty(), "the descendant keeps the pipe open");
        let signalling = world.submit(parent, Op::Signal { pidfd, signal: Signal::Kill, to: Target::Group });
        let completed = world.reap(parent);
        assert_eq!(
            completed.iter().find(|complete| complete.op == reading).expect("the pipe ended").result,
            Ok(Done::Count(0))
        );
        assert_eq!(
            completed.iter().find(|complete| complete.op == signalling).expect("the group signal completed").result,
            Ok(Done::Nothing)
        );
        if !exit_leader {
            world.call(parent, Op::Wait { pidfd, reap: false });
        }
        world.call(parent, Op::Wait { pidfd, reap: true });
        world.close(parent, output);
        world.close(parent, pidfd);
        world.close(parent, root);
        world.settled(parent);
    }
}

#[test]
fn a_zombies_usage_joins_its_parents_children_only_at_the_reap() {
    let (mut world, parent, root, pidfd) = rooted(Program::Exit(0));
    let child_usage =
        Resources { user: Duration::from_millis(3), system: Duration::from_millis(2), peak_rss_bytes: 4096 };
    world.sim.set_child_usage(parent, pidfd, child_usage);
    assert_eq!(usage(&mut world, parent), Usage::ZERO);
    world.call(parent, Op::Wait { pidfd, reap: false });
    assert_eq!(usage(&mut world, parent), Usage::ZERO, "observation retains the zombie");
    world.call(parent, Op::Wait { pidfd, reap: true });
    assert_eq!(usage(&mut world, parent).children, child_usage);
    let (second, _) = child(&mut world, parent, root, Program::Exit(0), false);
    world.sim.set_child_usage(parent, second, Resources { peak_rss_bytes: 1024, ..child_usage });
    world.call(parent, Op::Wait { pidfd: second, reap: false });
    world.call(parent, Op::Wait { pidfd: second, reap: true });
    let sum = usage(&mut world, parent).children;
    assert_eq!(sum.user, Duration::from_millis(6));
    assert_eq!(sum.system, Duration::from_millis(4));
    assert_eq!(sum.peak_rss_bytes, 4096, "the peak is a maximum, never a sum");
    world.close(parent, second);
    world.close(parent, pidfd);
    world.close(parent, root);
    world.settled(parent);
}

#[test]
fn a_hosted_service_in_the_group_hears_a_termination_signal() {
    let (mut world, parent, root, leader) = rooted(Program::Never);
    let (member, _) = child(&mut world, parent, root, Program::Service, false);
    let (service, _) = world.sim.bind_service(parent, member);
    let source = world.sim.open_signal_source(service);
    world.sim.join_group(parent, member, parent, leader);
    world.call(parent, Op::Signal { pidfd: leader, signal: Signal::Terminate, to: Target::Group });
    assert_eq!(
        world.call(service, Op::ReadSignal { fd: source }).result,
        Ok(Done::ServiceSignal(ServiceSignal::Terminate))
    );
    assert!(world.sim.service_running(service), "termination is delivered to the service's signal source");
    world.close(service, source);
    world.sim.finish_service(service, Exit::Code(0));
    for pidfd in [leader, member] {
        world.call(parent, Op::Wait { pidfd, reap: false });
        world.call(parent, Op::Wait { pidfd, reap: true });
        world.close(parent, pidfd);
    }
    world.close(parent, root);
    world.settled(parent);
    world.settled(service);
}

#[test]
#[should_panic(expected = "a second Wait")]
fn a_second_wait_on_the_pidfd_fails_the_world() {
    let (mut world, parent, _, pidfd) = rooted(Program::Never);
    world.submit(parent, Op::Wait { pidfd, reap: false });
    world.submit(parent, Op::Wait { pidfd, reap: false });
}

#[test]
#[should_panic(expected = "a reap before an observed exit")]
fn a_reap_before_an_observed_exit_fails_the_world() {
    let (mut world, parent, _, pidfd) = rooted(Program::Exit(0));
    world.submit(parent, Op::Wait { pidfd, reap: true });
}

#[test]
#[should_panic(expected = "a second Wait")]
fn a_reap_before_the_observing_completion_is_reported_fails_the_world() {
    let (mut world, parent, _, pidfd) = rooted(Program::Exit(0));
    world.submit(parent, Op::Wait { pidfd, reap: false });
    world.submit(parent, Op::Wait { pidfd, reap: true });
}

#[test]
#[should_panic(expected = "an operation after the reap")]
fn a_signal_after_the_reap_fails_the_world() {
    let (mut world, parent, _, pidfd) = rooted(Program::Exit(0));
    world.call(parent, Op::Wait { pidfd, reap: false });
    world.call(parent, Op::Wait { pidfd, reap: true });
    world.submit(parent, Op::Signal { pidfd, signal: Signal::Kill, to: Target::Group });
}

#[test]
#[should_panic(expected = "which io never cancels")]
fn a_cancel_of_usage_fails_the_world() {
    let mut world = World::calm();
    let parent = world.spawn();
    let target = world.submit(parent, Op::Usage);
    world.submit(parent, Op::Cancel { target });
}
