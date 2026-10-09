//! The shipped loop over scripted hosts, with real completions and signals.

use skein_io::kernel::{Complete, Done, Exit, Fd, Op, ServiceSignal, Submit};
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_shell::{Clock, Config, Host, Kernel, drive};
use skein_world_tests::hosted::{Act, Script};

fn kernel() -> Kernel {
    Kernel::open(Config { operations: 8 }).expect("the real ring")
}

#[test]
fn a_host_that_ends_by_itself_returns_its_exit_at_once() {
    let mut host = Script::new(&[]);
    host.terminal = Some(Exit::Code(7));
    let mut kernel = kernel();
    assert_eq!(drive(&mut kernel, &Clock::new(), &mut host), Exit::Code(7));
    assert_eq!(host.iterations, 1);
    assert_eq!(host.drains, 1);
    assert_eq!(kernel.in_flight(), 0);
}

#[test]
fn the_hook_runs_once_per_iteration_after_iterate_and_before_the_submit() {
    let (read, write) = skein_shell::open_signal_pipe().expect("pipes");
    let mut host = Script::pipes(vec![read, write], &[Act::Write(1, b"hook"), Act::Read(0)]);
    drive(&mut kernel(), &Clock::new(), &mut host);
    assert_eq!(host.received, b"hook");
    assert!(host.iterations > 2);
    assert_eq!(host.iterations, host.drains);
}

#[test]
fn drive_waits_for_the_earliest_deadline_or_a_completion() {
    let clock = Clock::new();
    let deadline = clock.now().now.saturating_add(Duration::from_millis(2));
    let mut host = Script::new(&[Act::Pause(deadline)]);
    drive(&mut kernel(), &clock, &mut host);
    assert!(clock.now().now >= deadline);
    assert_eq!(host.drains, host.iterations);
}

/// A raw close script: a silent pipe holds its drain read until a signal
/// cancels it. The second signal does not introduce another close or cancel.
struct Closing {
    signals: Fd,
    read: Fd,
    write: Fd,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    stage: u8,
    pending: u32,
    deadline: Time,
    heard: bool,
}

impl Host for Closing {
    fn iterate(&mut self, _now: Time, _wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            self.pending -= 1;
            if matches!(complete.result, Ok(Done::ServiceSignal(ServiceSignal::Terminate))) {
                self.heard = true;
                self.submissions.push(Submit { op: Token::new(3), kind: Op::Cancel { target: Token::new(1) } });
                self.pending += 1;
            }
        }
        if self.stage == 0 {
            self.submissions
                .push(Submit { op: Token::new(1), kind: Op::PipeRead { fd: self.read, buf: Box::new([0; 8]) } });
            self.submissions.push(Submit { op: Token::new(2), kind: Op::ReadSignal { fd: self.signals } });
            self.pending = 2;
            self.stage = 1;
        } else if self.stage == 1 && self.pending == 0 {
            for (index, fd) in [self.signals, self.read, self.write].into_iter().enumerate() {
                self.submissions.push(Submit {
                    op: Token::new(u64::try_from(index).expect("small").checked_add(4).expect("small")),
                    kind: Op::Close { fd },
                });
                self.pending += 1;
            }
            self.stage = 2;
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        !self.completions.is_empty() || !self.submissions.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        Some(self.deadline)
    }
    fn is_empty(&self) -> bool {
        self.stage == 2 && self.pending == 0 && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        4096
    }
    fn operations(&self) -> u32 {
        8
    }
}

#[test]
fn a_termination_signal_while_closing_aborts_and_drive_returns_when_those_settle() {
    let clock = Clock::new();
    let deadline = clock.now().now.saturating_add(Duration::from_secs(5));
    let signals = skein_shell::open_termination_signals().expect("blocked termination signals");
    let (read, write) = skein_shell::open_signal_pipe().expect("silent peer");
    let mut host = Closing {
        signals,
        read,
        write,
        completions: Queue::with_capacity(8),
        submissions: Queue::with_capacity(8),
        stage: 0,
        pending: 0,
        deadline,
        heard: false,
    };
    skein_shell::signal_current_thread(ServiceSignal::Terminate).expect("termination");
    let mut kernel = kernel();
    assert_eq!(drive(&mut kernel, &clock, &mut host), Exit::Code(0));
    assert!(host.heard);
    assert!(clock.now().now < deadline);
    assert_eq!(kernel.in_flight(), 0);
}

#[test]
fn worlds_drain_once_per_iteration_in_the_simulator_and_real_loop() {
    let mut simulated = skein_world::World::new(
        7,
        skein_sim::Config::calm(),
        skein_world_tests::hosted::Judge,
        skein_world::Memory::Unchecked,
    );
    simulated.spawn(|| Script::new(&[]));
    let outcome = simulated.run();
    assert_eq!(outcome.procs[0].iterations, outcome.procs[0].drains);
    assert!(outcome.procs[0].drains > 0);
    let mut real = skein_world::real::World::new(skein_world_tests::hosted::Judge);
    real.spawn(|| Script::new(&[]));
    let outcome = real.run(&Clock::new(), Duration::from_secs(1));
    assert_eq!(outcome.procs[0].iterations, outcome.procs[0].drains);
    assert!(outcome.procs[0].drains > 0);
}
