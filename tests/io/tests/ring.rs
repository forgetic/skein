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
        answers: vec![Some(echo("echo", 64, Reads::Mixed(16)))],
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
