//! One exchange through io over the real ring (testing-strategy.md, 2.8, the
//! real loop's first step): io and its scripted owner in one process, whose
//! loop reaps from the shell's kernel, runs io's stages, and submits back,
//! waiting until io's or the owner's next deadline. The owner listens on
//! loopback, connects to itself, and exchanges a message with an echo, both
//! ends in the one io; everything closes, and io and the ring hold nothing.
//!
//! A machine without `io_uring` fails here, saying so, as the ring's own
//! tests do.

use skein_io::operations;
use skein_io_world::owner::{Dial, Directory, Owner, Reads, Serve, ServeClose, Target};
use skein_io_world::scenarios::{echo, expecting, framed, limits, local, message, plan};
use skein_io_world::world::Proc;
use skein_lib::{Duration, Rng};
use skein_shell::{Clock, Config, Kernel, Wait};

#[test]
fn an_exchange_through_io_over_the_ring() {
    let limits = limits(3);
    let operations = operations(&limits).expect("a small ring");
    let mut kernel = match Kernel::open(Config { operations }) {
        Ok(kernel) => kernel,
        Err(error) => panic!("io_uring is not usable here, so io cannot run on the ring: {error}"),
    };
    let clock = Clock::new();
    let start = clock.now().now;
    let at = |offset: u64| start.saturating_add(Duration::from_millis(offset));

    let sent = framed(&message(&mut Rng::new(7), 3_000));
    let serve = Serve {
        name: "server",
        addr: local(0),
        answers: vec![Some(echo("echo", limits.largest_room(), Reads::Mixed(16)))],
        close: ServeClose::Answered(at(10_000)),
    };
    let client = expecting(plan("client", sent.clone(), 32, Reads::Mixed(16)), &sent);
    let dial = Dial { to: Target::Named("server"), at: start, plan: client };
    let mut proc = Proc::new(limits, Owner::new(7, vec![serve], vec![dial]));
    let mut directory = Directory::new();

    let deadline = at(10_000);
    loop {
        let now = clock.now();
        assert!(now.now < deadline, "the exchange ends within ten seconds");
        kernel.reap(&mut proc.completions);
        proc.iterate(now.now, now.wall, &mut directory);
        if proc.owner.done() && proc.io.is_empty() && proc.subs.is_empty() && kernel.in_flight() == 0 {
            break;
        }
        let wait = if proc.busy(now.now) {
            Wait::No
        } else {
            let mut until = deadline;
            for next in [proc.io.next_deadline(), proc.owner.next_deadline(now.now)].into_iter().flatten() {
                until = until.min(next);
            }
            Wait::Until(until)
        };
        kernel.submit(&mut proc.subs, wait);
    }
    let client = proc.owner.conn("client").expect("the client connected");
    assert!(
        client.received == *sent,
        "the echo returned every byte, in order: {} of {}",
        client.received.len(),
        sent.len()
    );
    assert!(!client.broken(), "nothing broke on loopback");
    proc.ledger.settled();
}

fn group_story(story: skein_io_world::processes::Story) {
    use skein_io_world::processes::{Judge, Process};
    use std::os::unix::ffi::OsStrExt;
    let root = skein_shell::open_root(std::path::Path::new("/tmp")).expect("the fixture's directory opens");
    let program = std::path::Path::new(env!("CARGO_BIN_EXE_io_process_fixture"));
    let process = Process::new(root, program.as_os_str().as_bytes(), story, true);
    let mut world = skein_world::real::World::new(Judge::default());
    world.spawn_with_fds(vec![root], || process);
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    outcome.procs[0].check();
}

#[test]
fn a_descendant_holding_the_pipe_ends_with_a_signal_to_the_group() {
    group_story(skein_io_world::processes::Story::SignalRunning);
}
#[test]
fn closing_a_child_whose_group_still_runs_ends_the_group() {
    group_story(skein_io_world::processes::Story::CloseExited);
}
#[test]
fn a_signal_to_the_group_after_its_leader_exited_reaches_the_group() {
    group_story(skein_io_world::processes::Story::SignalExited);
}

#[test]
fn an_append_stream_on_redirected_regular_output_preserves_every_byte_and_closes() {
    use skein_io::kernel::Fd;
    use skein_io_world::append::{CONTENTS, Evidence, Judge, Story, Writer};
    use std::os::fd::{AsRawFd, IntoRawFd};
    let scratch = skein_scratch::Scratch::new("append-stream-ring");
    let path = scratch.path().join("trace");
    std::fs::write(&path, b"prefix:").expect("existing output file");
    let file = std::fs::OpenOptions::new().write(true).open(&path).expect("redirected output");
    assert_eq!(
        skein_shell::prepare_output(Fd::new(file.as_raw_fd())).expect("output kind"),
        skein_shell::OutputKind::Append
    );
    let fd = Fd::new(file.into_raw_fd());
    let mut world = skein_world::real::World::new(Judge);
    world.spawn_with_fds(vec![fd], || Writer::new(fd, Story::Close, Duration::from_secs(1)));
    let outcome = world.run(&Clock::new(), Duration::from_secs(1));
    assert_eq!(std::fs::read(path).expect("the stream output"), CONTENTS);
    assert_eq!(outcome.procs[0].failures(), 0);
    assert_eq!(outcome.procs[0].events.last(), Some(&Evidence::Closed));
}

#[test]
fn a_pipe_output_keeps_the_pipe_face_at_startup() {
    use std::os::fd::AsRawFd;
    let mut child =
        std::process::Command::new("cat").stdin(std::process::Stdio::piped()).spawn().expect("a pipe output");
    let input = child.stdin.take().expect("child input pipe");
    assert_eq!(
        skein_shell::prepare_output(skein_io::kernel::Fd::new(input.as_raw_fd())).expect("output kind"),
        skein_shell::OutputKind::Pipe
    );
    drop(input);
    assert!(child.wait().expect("cat ends at pipe EOF").success());
}
