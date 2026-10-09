//! The echo's shipped main against two fake clients, observed only through
//! loopback, stderr and its ring exit (testing-strategy.md, section 2.9).
//! This test belongs to the shell package so Cargo supplies its actual binary.

use skein_echo_world::proc::Proc;
use skein_echo_world::scenarios::{client_limits, plan};
use skein_io::kernel::{Addr, Complete, Exit, Signal, Submit};
use skein_lib::{Duration, Queue, Time, Wall};
use skein_shell::Clock;
use skein_world::end_to_end::{Binary, Command, Mode, StartError};
use skein_world::{Host, Referee, real};

#[derive(Debug)]
enum Process {
    Echo(Binary),
    Client(Proc),
}

impl Process {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Echo(binary) => binary,
            Self::Client(client) => client,
        }
    }

    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Echo(binary) => binary,
            Self::Client(client) => client,
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
    listening: Option<Addr>,
    told: bool,
    clients_done: bool,
    signalled: bool,
    exit: Option<Exit>,
    now: Time,
    deadline: Time,
}

impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        if !self.told
            && let Some(addr) = self.listening
        {
            for process in &mut *procs {
                match process {
                    Process::Client(Proc::Client { client, .. }) => client.dial(addr),
                    Process::Client(Proc::Echo { .. }) => unreachable!("the binary is the only echo"),
                    Process::Echo(_) => {}
                }
            }
            self.told = true;
        }
        if self.clients_done && !self.signalled {
            let Process::Echo(binary) = procs.first_mut().expect("the binary observer") else {
                panic!("the binary observer")
            };
            binary.signal(Signal::Terminate);
            self.signalled = true;
        }
    }

    fn observe(&mut self, now: Time, procs: &[Process]) {
        self.now = now;
        let Process::Echo(binary) = procs.first().expect("the binary observer") else { panic!("the binary observer") };
        for line in binary.stderr().split_inclusive(|byte| *byte == b'\n') {
            if line.ends_with(b"\n")
                && let Some(address) = line.strip_prefix(b"skein-echo: listening at ")
            {
                self.listening = Some(
                    std::str::from_utf8(address).expect("printed address").trim().parse().expect("socket address"),
                );
            }
        }
        if let Some(exit) = binary.exit_status() {
            assert_eq!(
                exit,
                Exit::Code(0),
                "echo startup/shutdown failed: {}",
                String::from_utf8_lossy(binary.stderr())
            );
            assert!(self.signalled, "the echo stays alive until the referee's termination signal");
            self.exit = Some(exit);
        }
        self.clients_done = true;
        for process in procs.get(1..).expect("the two fake clients") {
            let Process::Client(client) = process else { panic!("a fake client") };
            let client = client.as_client().expect("a fake client");
            let seen = client.seen(0);
            assert_eq!(seen.broken, 0, "loopback did not break");
            if seen.done.is_some() {
                assert!(seen.complete, "every line was answered before the fake ended");
            }
            self.clients_done &= seen.done.is_some();
        }
    }

    fn next_deadline(&self) -> Option<Time> {
        if (!self.told && self.listening.is_some()) || (self.clients_done && !self.signalled) {
            Some(self.now)
        } else if self.passed() {
            None
        } else {
            Some(self.deadline)
        }
    }

    fn overdue(&self, now: Time) -> Option<String> {
        (now >= self.deadline && !self.passed()).then(|| {
            format!(
                "echo end to end did not settle: listening={}, clients_done={}, signalled={}, exited={}",
                self.listening.is_some(),
                self.clients_done,
                self.signalled,
                self.exit.is_some()
            )
        })
    }

    fn passed(&self) -> bool {
        self.clients_done && self.signalled && self.exit.is_some()
    }
}

#[test]
fn the_shipped_echo_answers_two_loopback_clients_and_terminates_successfully() {
    let clock = Clock::new();
    let start = clock.now().now;
    let binary = Binary::start(
        Command {
            program: env!("CARGO_BIN_EXE_skein-echo").into(),
            arguments: vec!["127.0.0.1:0".into()],
            environment: vec![],
            directory: std::env::current_dir().expect("test working directory"),
        },
        Mode::Pipes,
        16 * 1024,
    )
    .unwrap_or_else(|error| match error {
        StartError::Ring(why) => panic!("io_uring is not usable here, so the echo end to end cannot run: {why}"),
        StartError::Directory(errno) => panic!("echo working directory failed: errno {errno}"),
        StartError::Tree(error) => panic!("tree startup failed: {error:?}"),
        StartError::Child(error) => panic!("starting the shipped echo failed: {error:?}"),
    });
    let mut world = real::World::new(Judge {
        listening: None,
        told: false,
        clients_done: false,
        signalled: false,
        exit: None,
        now: start,
        deadline: start.saturating_add(Duration::from_secs(1)),
    });
    world.spawn_with_fds(binary.descriptors(), || Process::Echo(binary));
    for seed in [7, 11] {
        let plans = [plan(start, seed)];
        world.spawn(|| Process::Client(Proc::client(client_limits(1), &plans)));
    }
    let outcome = world.run(&clock, Duration::from_secs(2));
    let Process::Echo(binary) = outcome.procs.first().expect("the binary observer") else {
        panic!("the shipped binary observer")
    };
    assert_eq!(binary.exit_status(), Some(Exit::Code(0)));
    assert!(outcome.end.saturating_since(outcome.start) < Duration::from_secs(1));
}
