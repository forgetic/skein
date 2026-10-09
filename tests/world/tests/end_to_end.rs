//! Binary startup and outside observations over the world's shared ring.

use skein_io::kernel::{Complete, Exit, Signal, Submit};
use skein_lib::{Duration, Queue, Time, Wall};
use skein_shell::Clock;
use skein_world::end_to_end::{Binary, Command, Mode, StartError};
use skein_world::{Host, Referee, real};
use skein_world_tests::hosted::{Act, Script};

#[derive(Debug)]
enum Process {
    Child(Binary),
    Person(Script),
}

impl Process {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Child(child) => child,
            Self::Person(person) => person,
        }
    }

    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Child(child) => child,
            Self::Person(person) => person,
        }
    }
}

impl Host for Process {
    fn drain(&mut self) {
        self.host_mut().drain();
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        self.host_mut().iterate(now, wall);
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        self.host_mut().completions()
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        self.host_mut().submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.host().work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.host().next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.host().is_empty()
    }
    fn worst_case(&self) -> u64 {
        self.host().worst_case()
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}

struct Judge {
    signal: bool,
    sent: bool,
    act_at: Option<Time>,
}

impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        if self.signal && !self.sent {
            let Process::Child(child) = &mut procs[0] else { panic!("binary observer") };
            if child.stderr().ends_with(b"ready\n") {
                child.signal(Signal::Terminate);
                self.sent = true;
            }
        }
    }
    fn observe(&mut self, now: Time, procs: &[Process]) {
        let Process::Child(child) = &procs[0] else { panic!("binary observer") };
        self.act_at = (self.signal && !self.sent && child.stderr().ends_with(b"ready\n")).then_some(now);
    }
    fn next_deadline(&self) -> Option<Time> {
        self.act_at
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        !self.signal || self.sent
    }
}

fn command(script: &str) -> Command {
    Command {
        program: "/bin/sh".into(),
        arguments: vec!["-c".into(), script.into(), "fixture".into(), "argument".into()],
        environment: vec![("KEY".into(), "value".into())],
        directory: "/tmp".into(),
    }
}

fn run(command: Command, mode: Mode, acts: &[Act], signal: bool) -> real::Outcome<Process> {
    let mut binary = Binary::start(command, mode, 16 * 1024).unwrap_or_else(|error| match error {
        StartError::Ring(why) => panic!("io_uring is not usable here, so end to end cannot run: {why}"),
        StartError::Directory(errno) => panic!("binary directory failed: errno {errno}"),
        StartError::Tree(error) => panic!("tree startup failed: {error:?}"),
        StartError::Child(error) => panic!("binary startup failed: {error:?}"),
    });
    let streams = binary.take_streams().expect("the person's streams");
    let mut world = real::World::new(Judge { signal, sent: false, act_at: None });
    world.spawn_with_fds(binary.descriptors(), || Process::Child(binary));
    world.spawn_with_fds(streams.descriptors(), || Process::Person(Script::pipes(streams.descriptors(), acts)));
    world.run(&Clock::new(), Duration::from_secs(1))
}

fn observations(outcome: &real::Outcome<Process>) -> (&Binary, &Script) {
    let Process::Child(child) = &outcome.procs[0] else { panic!("binary observer") };
    let Process::Person(person) = &outcome.procs[1] else { panic!("scripted person") };
    (child, person)
}

#[test]
fn binary_pipes_preserve_arguments_environment_directory_stderr_and_exit() {
    let outcome = run(
        command(
            "test \"$PWD\" = /tmp || exit 90; test \"$1:$KEY\" = argument:value || exit 91; read value; printf 'answer:%s' \"$value\"; printf 'diagnostic' >&2; exit 17",
        ),
        Mode::Pipes,
        &[Act::Write(0, b"hello\n"), Act::Close(0), Act::Read(0), Act::Read(0), Act::Read(0)],
        false,
    );
    let (child, person) = observations(&outcome);
    assert_eq!(person.received, b"answer:hello");
    assert_eq!(child.stderr(), b"diagnostic");
    assert_eq!(child.exit_status(), Some(Exit::Code(17)));
}

#[test]
fn terminal_is_controlling_and_the_persons_interrupt_reaches_only_the_child() {
    let outcome = run(
        command(
            "test -t 0 && test -t 1 && test ! -t 2 || exit 90; test -r /dev/tty || exit 91; trap 'printf interrupted >&2; exit 23' INT; printf 'ready\\n'; read value; exit 92",
        ),
        Mode::Terminal,
        &[Act::Read(0), Act::Write(0, b"\x03"), Act::Read(0), Act::Read(0), Act::Read(0), Act::Read(0)],
        false,
    );
    let (child, person) = observations(&outcome);
    assert!(person.received.starts_with(b"ready\r\n"));
    assert_eq!(child.stderr(), b"interrupted");
    assert_eq!(child.exit_status(), Some(Exit::Code(23)));
}

#[test]
fn referee_signal_and_child_exit_are_shared_ring_events() {
    let outcome = run(
        command("trap 'printf terminated >&2; exit 31' TERM; printf 'ready\\n' >&2; read value"),
        Mode::Pipes,
        &[Act::Read(1), Act::Read(1)],
        true,
    );
    let (child, _) = observations(&outcome);
    assert_eq!(child.stderr(), b"ready\nterminated");
    assert_eq!(child.exit_status(), Some(Exit::Code(31)));
}

#[test]
fn invalid_binary_reports_a_startup_failure() {
    let mut command = command("exit 0");
    command.program = "/missing-skein-binary".into();
    assert!(matches!(
        Binary::start(command, Mode::Terminal, 1024),
        Err(StartError::Child(skein_io::kernel::Error::NotFound))
    ));
}

#[test]
fn an_abandoned_binary_is_reaped_and_its_untransferred_descriptors_are_closed() {
    let binary = Binary::start(command("read value"), Mode::Pipes, 1024).expect("the binary starts");
    let descriptors = binary.descriptors();
    drop(binary);
    for descriptor in descriptors {
        let path = format!("/proc/self/fd/{}", descriptor.raw());
        assert!(std::fs::metadata(path).is_err(), "abandonment closes every owned descriptor");
    }
}

struct TreeJudge {
    end: bool,
    sent: bool,
}

impl Referee<Process> for TreeJudge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        let Process::Child(child) = &mut procs[0] else { panic!("binary observer") };
        if self.end && !self.sent && ready_descendant(child.stderr()).is_some() {
            child.end();
            self.sent = true;
        }
    }
    fn observe(&mut self, _now: Time, _procs: &[Process]) {}
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        !self.end || self.sent
    }
}

fn fixture(mode: &str) -> Command {
    Command {
        program: env!("CARGO_BIN_EXE_world_process_fixture").into(),
        arguments: if mode == "exit" { vec!["exit".into(), "0".into()] } else { vec![mode.into()] },
        environment: Vec::new(),
        directory: "/tmp".into(),
    }
}

fn tree_run(mode: &str, force_walk: bool, expectation: skein_world::end_to_end::Expectation) -> real::Outcome<Process> {
    let binary = Binary::start_with_tree(fixture(mode), Mode::Pipes, 4096, expectation, force_walk)
        .expect("tree fixture starts");
    let mut world = real::World::new(TreeJudge { end: mode == "fork-live", sent: false });
    world.spawn_with_fds(binary.descriptors(), || Process::Child(binary));
    world.run(&Clock::new(), Duration::from_secs(1))
}

fn tree_child(outcome: &real::Outcome<Process>) -> &Binary {
    let Process::Child(child) = &outcome.procs[0] else { panic!("binary observer") };
    child
}

fn descendant(stderr: &[u8]) -> u32 {
    ready_descendant(stderr).expect("fixture names its descendant")
}

fn ready_descendant(stderr: &[u8]) -> Option<u32> {
    std::str::from_utf8(stderr)
        .expect("fixture stderr is text")
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .find_map(|line| line.strip_prefix("descendant:").and_then(|pid| pid.trim().parse().ok()))
}

fn assert_tree_counted(child: &Binary, force_walk: bool) {
    use skein_world::end_to_end::{Method, PeakScope};
    let counts = child.counts().expect("the whole tree settled before counts");
    eprintln!("tree counts: {counts:?}");
    assert!(counts.peak_rss_bytes > 0, "the kernel counts resident memory");
    assert!(
        counts.user.as_nanos().checked_add(counts.system.as_nanos()).expect("CPU fits") > 0,
        "the kernel counts tree CPU"
    );
    if force_walk {
        assert_eq!(counts.method, Method::Walk);
    }
    match counts.method {
        Method::Cgroup => assert_eq!(counts.peak_scope, PeakScope::Tree),
        Method::Walk => assert!(matches!(counts.peak_scope, PeakScope::LargestProcess { .. })),
    }
}

#[test]
fn a_descendant_left_in_the_group_is_ended_and_counted() {
    for force_walk in [true, false] {
        let outcome = tree_run("fork-exit", force_walk, skein_world::end_to_end::Expectation::Measure);
        let child = tree_child(&outcome);
        assert!(
            !std::path::Path::new(&format!("/proc/{}", descendant(child.stderr()))).exists(),
            "the descendant was reaped"
        );
        assert_tree_counted(child, force_walk);
    }
}

#[test]
fn a_descendant_that_leaves_its_group_and_session_is_still_settled() {
    for force_walk in [true, false] {
        let outcome = tree_run("escape-exit", force_walk, skein_world::end_to_end::Expectation::Measure);
        let child = tree_child(&outcome);
        assert!(
            !std::path::Path::new(&format!("/proc/{}", descendant(child.stderr()))).exists(),
            "the escaped descendant was reaped"
        );
        assert_tree_counted(child, force_walk);
    }
}

#[test]
fn a_binary_that_exits_at_once_settles_with_counts_above_zero() {
    for force_walk in [true, false] {
        let outcome = tree_run("exit", force_walk, skein_world::end_to_end::Expectation::EndsWithBinary);
        assert_tree_counted(tree_child(&outcome), force_walk);
    }
}

#[test]
fn an_expected_end_fails_naming_a_process_still_running() {
    for force_walk in [true, false] {
        let failure = std::panic::catch_unwind(|| {
            tree_run("fork-exit", force_walk, skein_world::end_to_end::Expectation::EndsWithBinary)
        })
        .expect_err("the fixture left a living descendant");
        let text = failure.downcast_ref::<String>().expect("the keeper fails with its diagnostic");
        assert!(text.contains("tree processes still ran: ["), "the diagnostic names the leftover PID: {text}");
    }
}

#[test]
fn a_measured_binary_is_killed_and_counted() {
    for force_walk in [true, false] {
        let outcome = tree_run("fork-live", force_walk, skein_world::end_to_end::Expectation::Measure);
        let child = tree_child(&outcome);
        assert_eq!(child.exit_status(), Some(Exit::Signal(9)));
        assert!(!std::path::Path::new(&format!("/proc/{}", descendant(child.stderr()))).exists());
        assert_tree_counted(child, force_walk);
    }
}

#[test]
fn a_dropped_observer_ends_its_tree() {
    use skein_shell::{Config, Kernel, Wait};
    for force_walk in [true, false] {
        let mut child = Binary::start_with_tree(
            fixture("fork-live"),
            Mode::Pipes,
            4096,
            skein_world::end_to_end::Expectation::Measure,
            force_walk,
        )
        .expect("the fixture starts");
        let mut kernel = Kernel::open(Config { operations: child.operations() }).expect("a real ring");
        let clock = Clock::new();
        let until = clock.now().now.saturating_add(Duration::from_secs(1));
        while ready_descendant(child.stderr()).is_none() {
            let now = clock.now();
            assert!(now.now < until, "the fixture became ready before its deadline");
            kernel.reap(child.completions());
            child.iterate(now.now, now.wall);
            kernel.submit(child.submissions(), Wait::Until(now.now.saturating_add(Duration::from_millis(1))));
        }
        let pid = descendant(child.stderr());
        let descriptors = child.descriptors();
        drop(child);
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists(), "the abandoned descendant was reaped");
        for fd in descriptors {
            assert!(std::fs::metadata(format!("/proc/self/fd/{}", fd.raw())).is_err());
        }
        let mut completions = Queue::with_capacity(8);
        while kernel.in_flight() > 0 {
            kernel.reap(&mut completions);
            while completions.pop().is_some() {}
            if kernel.in_flight() > 0 {
                kernel.submit(&mut Queue::with_capacity(0), Wait::Until(until));
            }
            assert!(clock.now().now < until, "the killed tree completes its interrupted operations");
        }
    }
}

#[test]
fn a_walk_keeps_an_existing_parts_child_untouched() {
    let mut other = std::process::Command::new("/bin/sleep").arg("60").spawn().expect("another part starts its child");
    let outcome = tree_run("fork-exit", true, skein_world::end_to_end::Expectation::Measure);
    assert_tree_counted(tree_child(&outcome), true);
    assert!(other.try_wait().expect("the other child can be checked").is_none(), "the keeper left its child alone");
    other.kill().expect("the other part ends its child");
    other.wait().expect("the other part reaps its child");
}

#[test]
fn a_walk_refuses_another_observer_during_its_accounting_interval() {
    let child = Binary::start_with_tree(
        fixture("fork-live"),
        Mode::Pipes,
        4096,
        skein_world::end_to_end::Expectation::Measure,
        true,
    )
    .expect("the first observer starts");
    let second = Binary::start_with_tree(
        fixture("exit"),
        Mode::Pipes,
        4096,
        skein_world::end_to_end::Expectation::Measure,
        false,
    );
    assert!(matches!(second, Err(StartError::Tree(skein_io::kernel::Error::Other(16)))));
    drop(child);
    let outcome = tree_run("exit", true, skein_world::end_to_end::Expectation::Measure);
    assert_tree_counted(tree_child(&outcome), true);
}

#[test]
fn delegated_cgroups_keep_two_binaries_separate_until_both_are_removed() {
    use skein_world::end_to_end::{Expectation, Method};
    let first = Binary::start_with_tree(fixture("fork-live"), Mode::Pipes, 4096, Expectation::Measure, false)
        .expect("the first observer starts");
    let second = Binary::start_with_tree(fixture("exit"), Mode::Pipes, 4096, Expectation::Measure, false);
    let second = match second {
        Ok(second) => second,
        Err(StartError::Tree(skein_io::kernel::Error::Other(16))) => {
            drop(first);
            eprintln!("no delegated cgroup: concurrent observers require containment");
            return;
        }
        Err(error) => panic!("second tree startup failed: {error:?}"),
    };
    drop(first);
    let mut world = real::World::new(TreeJudge { end: false, sent: false });
    world.spawn_with_fds(second.descriptors(), || Process::Child(second));
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert_eq!(tree_child(&outcome).counts().expect("the second tree settled").method, Method::Cgroup);
    assert_tree_counted(tree_child(&outcome), false);
}
