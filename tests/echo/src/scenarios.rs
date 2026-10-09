//! The echo's scenarios (examples.md, 7), each a world built from a seed and
//! a simulator's configuration: the echo as process 0, listening on a port
//! the simulator picks, and fake clients after it, under tiny limits (two
//! sessions, three connections, five sockets, lines of sixteen bytes), so
//! that every admission point is in reach. A calm configuration holds each
//! scenario to everything it expects; under faults a stream may break, and
//! the scenario expects only that every connection finishes, while the fake
//! clients still check every answer they get.
//!
//! The focused tests run a few seeds of each (`tests/scenarios.rs`); the
//! fuzzy ones sweep many, calm and chaotic (`tests/fuzzy_scenarios.rs`).

use std::net::{Ipv4Addr, SocketAddr};

use skein_echo_client::{self as client, HalfClose, Plan, Then};
use skein_echo_service as service;
use skein_echo_service::{domain, protocol};
use skein_io::kernel::Addr;
use skein_lib::{Duration, Rng, Time};
use skein_sim::{Config, Faults};
use skein_world::{Memory, World};

use crate::proc::Proc;
use crate::referee::{EchoReferee, Expect, Shutdown};

/// An echo world.
pub type EchoWorld = World<Proc, EchoReferee>;

/// A scenario: a world from a seed and a configuration.
pub type Scenario = fn(u64, Config) -> EchoWorld;

/// The echo's idle deadline, and how far it is spread.
pub const IDLE: Duration = Duration::from_secs(2);
pub const SPREAD: Duration = Duration::from_millis(100);

/// The server's line limit.
pub const LINE: u32 = 16;

/// When every scenario must be over, in simulated time.
const END: Time = Time::from_nanos(60_000_000_000);

/// The echo is always the first process.
pub const ECHO: usize = 0;

/// The echo's limits: tiny, so that each layer refuses at its own entrance
/// in some scenario. Six io slots (one for signals), so that io still accepts past the
/// protocol layer's three connections, which still bind past the domain's
/// two sessions.
#[must_use]
pub const fn server(spread: Duration) -> service::Limits {
    service::Limits {
        io: skein_io::Limits {
            sockets: 6,
            refusals: 1,
            intake: 24,
            receive: 8,
            output: 32,
            sends: 2,
            accepts: 1,
            backlog: 2,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(10),
        },
        protocol: protocol::Limits { conns: 3, line: LINE, idle: IDLE, spread, retry: Duration::from_millis(10) },
        domain: domain::Limits { sessions: 2 },
        queue: 4,
    }
}

/// A fake client's limits, for `conns` connections.
#[must_use]
pub const fn client_limits(conns: u32) -> client::Limits {
    client::Limits {
        io: skein_io::Limits {
            sockets: conns,
            refusals: 1,
            intake: 24,
            receive: 8,
            output: 32,
            sends: 2,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(10),
        },
        line: LINE,
        queue: 4,
    }
}

/// Where the echo listens: a port the simulator picks.
#[must_use]
pub fn listen() -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

#[must_use]
pub const fn ms(n: u64) -> Time {
    Time::from_nanos(n * 1_000_000)
}

/// Whether the configuration injects no fault, so that every stream holds.
#[must_use]
pub fn calm(config: &Config) -> bool {
    config.faults == Faults::NONE
}

/// A plan of three lines of one to sixteen bytes, one at a time, read as they
/// come, finished once answered.
#[must_use]
pub const fn plan(at: Time, seed: u64) -> Plan {
    Plan {
        at,
        seed,
        lines: 3,
        shortest: 1,
        longest: LINE,
        long: None,
        ahead: 1,
        piece: LINE,
        read_from: Some(Time::ZERO),
        send_limit: u64::MAX,
        then: Then::Finish,
        half_close: None,
        abort_at: None,
        retries: 0,
        backoff: Duration::from_millis(10),
    }
}

/// A world with the echo in it, and these expectations.
fn world(seed: u64, config: Config, expect: Vec<Expect>, shutdown: Shutdown) -> EchoWorld {
    let mut world = World::new_controlled(seed, config, Memory::Checked, |controls| {
        EchoReferee::new(seed, expect, shutdown, controls)
    });
    world.spawn_signals(|signal| {
        let mut proc = Proc::echo(server(SPREAD), listen(), seed);
        match &mut proc {
            Proc::Echo { svc, .. } => svc.svc.adopt_signals(signal).expect("signal slot"),
            Proc::Client { .. } => unreachable!("echo"),
        }
        proc
    });
    world
}

/// A fake client process with `plans`.
fn spawn(world: &mut EchoWorld, plans: &[Plan]) -> usize {
    let conns = u32::try_from(plans.len()).expect("few plans");
    world.spawn(|| Proc::client(client_limits(conns), plans))
}

fn between(rng: &mut Rng, low: u64, high: u64) -> u32 {
    u32::try_from(rng.between(low, high)).expect("small")
}

/// Many clients: three processes of two connections each, most connecting
/// at once, the rest a little later, each sending a few lines of random
/// lengths, some ahead of their answers, in pieces cut at random, and
/// ending as each plan says. Six connections against two sessions and three
/// connections: the burst is refused in part, at the domain's entrance and
/// the protocol layer's, and retried until served.
#[must_use]
pub fn clients(seed: u64, config: Config) -> EchoWorld {
    let mut rng = Rng::new(seed ^ 0x00c1_1e47);
    let mut expect = Vec::new();
    let mut processes = Vec::new();
    for at in 1..=3 {
        let mut plans = Vec::new();
        for conn in 0..2 {
            let then = match rng.below(3) {
                0 => Then::Finish,
                1 => Then::Close,
                _ => Then::Abort,
            };
            plans.push(Plan {
                lines: between(&mut rng, 2, 6),
                ahead: between(&mut rng, 1, 3),
                piece: between(&mut rng, 1, u64::from(LINE)),
                then,
                retries: 40,
                backoff: Duration::from_millis(rng.between(5, 25)),
                ..plan(if rng.chance(250) { ms(10) } else { Time::ZERO }, rng.next_u64())
            });
            if calm(&config) {
                expect.push(Expect::Served { at, conn, by: END });
            }
            expect.push(Expect::Finished { at, conn, by: END });
        }
        processes.push(plans);
    }
    let mut world = world(seed, config, expect, Shutdown::WhenDone);
    for plans in &processes {
        spawn(&mut world, plans);
    }
    world
}

/// A line too long: one connection's third line is twice the limit, so the
/// echo answers the first two, then `too long`, and closes; another
/// connection beside it is served all the while.
#[must_use]
pub fn too_long(seed: u64, config: Config) -> EchoWorld {
    let mut rng = Rng::new(seed ^ 0x7001);
    let long = Plan { lines: 4, long: Some(2), then: Then::Linger, piece: 7, ..plan(ms(1), rng.next_u64()) };
    let beside = Plan { lines: 5, ahead: 2, ..plan(ms(1), rng.next_u64()) };
    let mut expect = vec![Expect::Finished { at: 1, conn: 0, by: END }, Expect::Finished { at: 1, conn: 1, by: END }];
    if calm(&config) {
        expect.push(Expect::TooLong { at: 1, conn: 0, by: END });
        expect.push(Expect::Served { at: 1, conn: 1, by: END });
    }
    let mut world = world(seed, config, expect, Shutdown::WhenDone);
    spawn(&mut world, &[long, beside]);
    world
}

/// Refusals at the entrance: two connections take both sessions and linger;
/// two more arrive together, the first bound and told `busy` at the
/// domain's entrance, the second rejected at the protocol layer's, its
/// three connections all taken. Once the first two idle out, a fifth is
/// served.
#[must_use]
pub fn busy(seed: u64, config: Config) -> EchoWorld {
    let mut rng = Rng::new(seed ^ 0xb057);
    let linger = |rng: &mut Rng| Plan { lines: 1, then: Then::Linger, ..plan(ms(1), rng.next_u64()) };
    let late = |rng: &mut Rng| Plan { lines: 1, ..plan(ms(200), rng.next_u64()) };
    let plans = [
        linger(&mut rng),
        linger(&mut rng),
        late(&mut rng),
        late(&mut rng),
        Plan { lines: 2, ..plan(ms(3_000), rng.next_u64()) },
    ];
    let mut expect = Vec::new();
    for conn in 0..5 {
        expect.push(Expect::Finished { at: 1, conn, by: END });
    }
    if calm(&config) {
        let by = ms(1).saturating_add(IDLE).saturating_add(SPREAD).saturating_add(Duration::from_secs(1));
        expect.push(Expect::Idled { at: 1, conn: 0, idle: IDLE, by });
        expect.push(Expect::Idled { at: 1, conn: 1, idle: IDLE, by });
        expect.push(Expect::Busy { at: 1, conn: 2, by: ms(1_000) });
        expect.push(Expect::TurnedAway { at: 1, conn: 3, by: ms(1_000) });
        expect.push(Expect::Served { at: 1, conn: 4, by: END });
    }
    let mut world = world(seed, config, expect, Shutdown::WhenDone);
    spawn(&mut world, &plans);
    world
}

/// Idle timeouts: a connection that sends nothing, and one that sends half a
/// line and then nothing; once the echo closed them, one that sends two
/// lines and then nothing. The echo closes each once its idle deadline
/// passes, never before.
#[must_use]
pub fn idle(seed: u64, config: Config) -> EchoWorld {
    let mut rng = Rng::new(seed ^ 0x1d1e);
    let (first, then) = (ms(5), ms(3_000));
    let plans = [
        Plan { lines: 0, then: Then::Linger, ..plan(first, rng.next_u64()) },
        Plan { lines: 1, shortest: 10, send_limit: 5, then: Then::Linger, ..plan(first, rng.next_u64()) },
        Plan { lines: 2, then: Then::Linger, ..plan(then, rng.next_u64()) },
    ];
    let mut expect = Vec::new();
    for (conn, at) in [(0, first), (1, first), (2, then)] {
        if calm(&config) {
            let by = at.saturating_add(IDLE).saturating_add(SPREAD).saturating_add(Duration::from_secs(1));
            expect.push(Expect::Idled { at: 1, conn, idle: IDLE, by });
            expect.push(Expect::Finished { at: 1, conn, by });
        } else {
            expect.push(Expect::Finished { at: 1, conn, by: END });
        }
    }
    let mut world = world(seed, config, expect, Shutdown::WhenDone);
    spawn(&mut world, &plans);
    world
}

/// The bytes of a simulated socket's receive buffer in the backpressure
/// scenario, small, so that sends stall.
const BUFFER: u32 = 64;

/// A client that stops reading: one connection sends forty lines ahead of
/// their answers and never reads them, so the echo's output fills, it stops
/// reading, and backpressure stops the client, holding it to what the
/// buffers between them hold, until the echo idles the connection out and
/// its close discards the rest; the client, which cannot hear that end
/// behind what it never read, gives up by itself. Another connection reads
/// only after a second, and is served once it does.
#[must_use]
pub fn backpressure(seed: u64, config: Config) -> EchoWorld {
    let config = Config { buffer: BUFFER, ..config };
    let mut rng = Rng::new(seed ^ 0xbac4);
    let full = Plan {
        lines: 40,
        shortest: LINE,
        ahead: 40,
        read_from: None,
        then: Then::Linger,
        abort_at: Some(ms(5_000)),
        ..plan(ms(1), rng.next_u64())
    };
    let late = Plan { lines: 10, shortest: LINE, ahead: 10, read_from: Some(ms(1_000)), ..plan(ms(1), rng.next_u64()) };
    let (echo, client) = (server(SPREAD).io, client_limits(2).io);
    // Every buffer between what the client handed io and what the echo's
    // protocol layer reads, then every one back, and a line in flight in it.
    let most = u64::from(client.output + BUFFER + echo.receive + echo.intake)
        + u64::from(LINE)
        + u64::from(echo.output + BUFFER + client.receive + client.intake);
    let mut expect = vec![
        Expect::Holds { at: 1, conn: 0, most, until: ms(1).saturating_add(IDLE) },
        Expect::Finished { at: 1, conn: 0, by: END },
        Expect::Finished { at: 1, conn: 1, by: END },
    ];
    if calm(&config) {
        expect.push(Expect::Served { at: 1, conn: 1, by: ms(1_500) });
        // Once the echo idles the stopped client out, its close discards
        // what the client sends, which then hands io the rest of its lines.
        let close = server(SPREAD).io.close_timeout;
        let by = ms(1).saturating_add(IDLE).saturating_add(SPREAD).saturating_add(close).saturating_add(MARGIN);
        expect.push(Expect::Handed { at: 1, conn: 0, bytes: 40 * u64::from(LINE), by });
    }
    let mut world = world(seed, config, expect, Shutdown::WhenDone);
    spawn(&mut world, &[full, late]);
    world
}

/// A margin past a deadline, for what follows it in the same instant of a
/// calm world, in a few iterations.
const MARGIN: Duration = Duration::from_millis(500);

/// A half-close with lines unanswered: one connection sends three lines
/// ahead of their answers and half-closes, so that the echo hears the end
/// while it answers the last; another sends three and a piece of a fourth,
/// with no end of line, then half-closes. The echo answers every whole line,
/// then ends each stream, the piece dropped.
#[must_use]
pub fn half_close(seed: u64, config: Config) -> EchoWorld {
    let mut rng = Rng::new(seed ^ 0x4a1f);
    let whole = Plan {
        lines: 3,
        ahead: 3,
        half_close: Some(HalfClose { after: 3, tail: 0 }),
        then: Then::Linger,
        ..plan(ms(1), rng.next_u64())
    };
    let piece = Plan {
        lines: 4,
        ahead: 4,
        half_close: Some(HalfClose { after: 3, tail: 5 }),
        then: Then::Linger,
        ..plan(ms(1), rng.next_u64())
    };
    let mut expect = vec![Expect::Finished { at: 1, conn: 0, by: END }, Expect::Finished { at: 1, conn: 1, by: END }];
    if calm(&config) {
        for conn in 0..2 {
            expect.push(Expect::Ends { at: 1, conn, by: ms(1).saturating_add(MARGIN) });
        }
    }
    let mut world = world(seed, config, expect, Shutdown::WhenDone);
    spawn(&mut world, &[whole, piece]);
    world
}

/// The echo at its worst case (testing-strategy.md, 8): with sessions for
/// all three of its connections, three clients that send forty lines ahead
/// of their answers and never read fill every connection's intake, receive
/// and output at once, and a fourth is rejected, until the clients give up.
/// What the echo held at its peak is measured against its worst case by the
/// focused test, to show how near the bound comes.
#[must_use]
pub fn worst(seed: u64, config: Config) -> EchoWorld {
    let config = Config { buffer: BUFFER, ..config };
    let mut rng = Rng::new(seed ^ 0x3057);
    let mut limits = server(SPREAD);
    limits.domain.sessions = 3;
    let mut world = World::new_controlled(seed, config, Memory::Checked, |controls| {
        EchoReferee::new(seed, Vec::new(), Shutdown::WhenDone, controls)
    });
    world.spawn_signals(|signal| {
        let mut proc = Proc::echo(limits, listen(), seed);
        match &mut proc {
            Proc::Echo { svc, .. } => svc.svc.adopt_signals(signal).expect("signal slot"),
            Proc::Client { .. } => unreachable!("echo"),
        }
        proc
    });
    let mut plans = Vec::new();
    for _ in 0..4 {
        plans.push(Plan {
            lines: 40,
            shortest: LINE,
            ahead: 40,
            read_from: None,
            then: Then::Linger,
            abort_at: Some(ms(1_000)),
            ..plan(ms(1), rng.next_u64())
        });
    }
    spawn(&mut world, &plans);
    world
}

/// Closes and resets in every state: two clients of three connections each,
/// with plans drawn from the seed: lines or none, sent whole or cut short,
/// read from the start, late or never, finished, closed, aborted or left to
/// idle out, and aborts at random moments; and the echo shut down at a
/// random moment among them, so that later connects are refused.
#[must_use]
pub fn closes(seed: u64, config: Config) -> EchoWorld {
    let mut rng = Rng::new(seed ^ 0xc105e);
    let mut expect = Vec::new();
    let mut processes = Vec::new();
    for at in 1..=2 {
        let mut plans = Vec::new();
        for conn in 0..3 {
            let read_from = match rng.below(10) {
                0 => None,
                1 | 2 => Some(Time::from_nanos(rng.below(100_000_000))),
                _ => Some(Time::ZERO),
            };
            let then = match rng.below(4) {
                0 => Then::Finish,
                1 => Then::Close,
                2 => Then::Abort,
                _ => Then::Linger,
            };
            // A connection that never reads is ended by an abort of its own:
            // the end of its peer's stream waits behind what it never read.
            let abort_at = if read_from.is_none() || rng.chance(500) {
                Some(Time::from_nanos(rng.below(60_000_000)))
            } else {
                None
            };
            let send_limit = if rng.chance(300) { rng.below(40) } else { u64::MAX };
            plans.push(Plan {
                lines: between(&mut rng, 0, 4),
                ahead: between(&mut rng, 1, 3),
                piece: between(&mut rng, 1, u64::from(LINE)),
                read_from,
                send_limit,
                then,
                abort_at,
                retries: between(&mut rng, 0, 2),
                ..plan(Time::from_nanos(rng.below(40_000_000)), rng.next_u64())
            });
            expect.push(Expect::Finished { at, conn, by: END });
        }
        processes.push(plans);
    }
    let shutdown = Shutdown::At(Time::from_nanos(rng.between(20_000_000, 60_000_000)));
    let mut world = world(seed, config, expect, shutdown);
    for plans in &processes {
        spawn(&mut world, plans);
    }
    world
}

/// A shutdown while connections live: one lingers, served, through the
/// shutdown, and the echo still idles it out; one connects after it and is
/// refused, as nothing listens any more.
#[must_use]
pub fn shutdown(seed: u64, config: Config) -> EchoWorld {
    let mut rng = Rng::new(seed ^ 0x5407);
    let lingers = Plan { lines: 2, then: Then::Linger, ..plan(ms(1), rng.next_u64()) };
    let after = plan(ms(100), rng.next_u64());
    let mut expect = vec![Expect::Finished { at: 1, conn: 0, by: END }, Expect::Finished { at: 1, conn: 1, by: END }];
    if calm(&config) {
        let by = ms(1).saturating_add(IDLE).saturating_add(SPREAD).saturating_add(Duration::from_secs(1));
        expect.push(Expect::Served { at: 1, conn: 0, by: ms(10) });
        expect.push(Expect::Idled { at: 1, conn: 0, idle: IDLE, by });
        expect.push(Expect::Refused { at: 1, conn: 1, by: ms(200) });
    }
    let mut world = world(seed, config, expect, Shutdown::At(ms(50)));
    spawn(&mut world, &[lingers, after]);
    world
}

/// Every scenario, by name, for the sweeps.
pub const SCENARIOS: [(&str, Scenario); 9] = [
    ("clients", clients),
    ("too_long", too_long),
    ("busy", busy),
    ("idle", idle),
    ("backpressure", backpressure),
    ("closes", closes),
    ("shutdown", shutdown),
    ("half_close", half_close),
    ("worst", worst),
];
