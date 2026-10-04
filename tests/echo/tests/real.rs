//! The echo on the real loop (testing-strategy.md, 2.8; examples.md, 7): the
//! echo as it ships and its fake clients, each on a ring of its own, in one
//! thread and one loop over the shell's kernel, on loopback, with the
//! referee's deadlines on the real clock. It shows what only the real
//! kernel can: the ring adapter under the whole service, and real sockets
//! between real processes' worth of io. It does not replay; what fails here
//! is rerun in the simulated worlds, where it does.
//!
//! A machine where `io_uring` cannot be used fails here, saying so, as the
//! ring's own tests do.

use skein_echo_client::{Plan, Then};
use skein_echo_world::proc::Proc;
use skein_echo_world::referee::{EchoReferee, Expect, Shutdown};
use skein_echo_world::scenarios::{LINE, client_limits, plan, server};
use skein_io::kernel::Addr;
use skein_lib::{Duration, Rng};
use skein_shell::Clock;
use skein_world::real;

/// The echo's idle deadline here: short, so that the test is quick.
const IDLE: Duration = Duration::from_millis(200);

#[test]
fn the_echo_serves_refuses_and_idles_out_its_clients_on_the_real_ring() {
    let clock = Clock::new();
    let start = clock.now().now;
    let at = |ms: u64| start.saturating_add(Duration::from_millis(ms));
    let mut rng = Rng::new(7);
    let mut limits = server(Duration::from_millis(20));
    limits.protocol.idle = IDLE;
    let listen: Addr = "127.0.0.1:0".parse().expect("an address");

    // Three connections at once against two sessions, each of eight lines
    // ahead of their answers in pieces cut short, retried when refused.
    let many = |rng: &mut Rng| Plan {
        lines: 8,
        ahead: 3,
        piece: 5,
        retries: 40,
        backoff: Duration::from_millis(20),
        ..plan(start, rng.next_u64())
    };
    let first = [many(&mut rng), many(&mut rng), many(&mut rng)];
    // One whose third line is past the limit; and, once the others are long
    // done, one that lingers until the echo idles it out.
    let second = [
        Plan {
            lines: 4,
            long: Some(2),
            shortest: LINE,
            then: Then::Linger,
            retries: 40,
            ..plan(at(60), rng.next_u64())
        },
        Plan { lines: 1, then: Then::Linger, ..plan(at(300), rng.next_u64()) },
    ];
    let by = at(5_000);
    let mut expect = Vec::new();
    for conn in 0..3 {
        expect.push(Expect::Served { at: 1, conn, by });
        expect.push(Expect::Finished { at: 1, conn, by });
    }
    expect.push(Expect::TooLong { at: 2, conn: 0, by });
    // Less a margin: on the ring, the client hears its last answer a moment
    // after the echo arms its deadline.
    expect.push(Expect::Idled { at: 2, conn: 1, idle: Duration::from_millis(150), by });
    expect.push(Expect::Finished { at: 2, conn: 0, by });
    expect.push(Expect::Finished { at: 2, conn: 1, by });

    let procs = vec![
        Proc::echo(limits, listen, rng.next_u64()),
        Proc::client(client_limits(3), &first),
        Proc::client(client_limits(2), &second),
    ];
    let referee = EchoReferee::new(7, expect, Shutdown::WhenDone);
    let outcome = real::run(procs, referee, &clock, Duration::from_secs(10));
    let took = outcome.end.saturating_since(outcome.start);
    assert!(took < Duration::from_secs(5), "over within five seconds: {} ms", took.as_nanos() / 1_000_000);
    let seen = outcome.procs[2].as_client().expect("a fake client").seen(0);
    assert_eq!(seen.answered, 2, "the lines before the long one were answered");
}
