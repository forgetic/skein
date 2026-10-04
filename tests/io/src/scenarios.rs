//! The scenarios of io's worlds (io.md, 8), each a world built from a seed
//! and a simulator's configuration, under tiny limits: slabs of two or three
//! sockets, intakes and outputs of a few dozen bytes. A calm configuration
//! holds each scenario to everything it expects; under faults, a stream may
//! break, and what it received need only be a prefix of what was sent.
//!
//! The focused tests run a few seeds of each (`tests/scenarios.rs`); the
//! fuzzy ones sweep hundreds, calm and chaotic
//! (`tests/fuzzy_scenarios.rs`).

use std::net::{Ipv4Addr, SocketAddr};

use skein_io::kernel::Addr;
use skein_io::{Error, Limits};
use skein_lib::stream::Delimiter;
use skein_lib::{Duration, Rng, Time};
use skein_sim::{Config, Faults};

use crate::owner::{Dial, Owner, Plan, Reads, Serve, ServeClose, Target, Total, When};
use crate::referee::{Expect, Referee};
use crate::world::World;

/// The limits of every process: tiny, so that every admission point is in
/// reach.
#[must_use]
pub const fn limits(sockets: u32) -> Limits {
    Limits {
        sockets,
        refusals: 2,
        intake: 16,
        receive: 7,
        output: 32,
        sends: 2,
        accepts: 1,
        backlog: 2,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    }
}

/// When every scenario must be over, in simulated time.
const END: Time = Time::from_nanos(60_000_000_000);

const fn ms(n: u64) -> Time {
    Time::from_nanos(n * 1_000_000)
}

/// `127.0.0.1:port`.
#[must_use]
pub fn local(port: u16) -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// Whether the configuration injects no fault, so that every stream holds.
#[must_use]
pub fn calm(config: &Config) -> bool {
    config.faults == Faults::NONE
}

/// Random bytes, fewer than `most`, with line feeds and carriage returns
/// among them for the scans to find.
#[must_use]
pub fn short(rng: &mut Rng, most: u64) -> Box<[u8]> {
    let len = usize::try_from(rng.below(most)).expect("short");
    message(rng, len)
}

/// Random bytes, with line feeds and carriage returns among them for the
/// scans to find.
#[must_use]
pub fn message(rng: &mut Rng, len: usize) -> Box<[u8]> {
    let mut bytes = Vec::with_capacity(len);
    for _ in 0..len {
        bytes.push(match rng.below(16) {
            0 => b'\n',
            1 => b'\r',
            _ => u8::try_from(rng.below(256)).expect("a byte"),
        });
    }
    bytes.into_boxed_slice()
}

/// A plan that sends `send` and reads with `reads`, finishes, and closes once
/// both directions are done.
#[must_use]
pub fn plan(name: &'static str, send: Box<[u8]>, chunk: u32, reads: Reads) -> Plan {
    Plan {
        name,
        send,
        chunk,
        wait: 0,
        echo: false,
        reads,
        total: Total::Unknown,
        finish: true,
        close: When::Done,
        abort: false,
    }
}

/// A plan that echoes what it reads, framed by a length before it, then
/// finishes and closes.
#[must_use]
pub fn echo(name: &'static str, chunk: u32, reads: Reads) -> Plan {
    Plan { echo: true, total: Total::Framed, ..plan(name, Box::new([]), chunk, reads) }
}

/// `body`, after its length in two bytes, big-endian: what an echo server
/// reads whole.
#[must_use]
pub fn framed(body: &[u8]) -> Box<[u8]> {
    let mut bytes = u16::try_from(body.len()).expect("a short body").to_be_bytes().to_vec();
    bytes.extend_from_slice(body);
    bytes.into_boxed_slice()
}

/// Expects to receive `bytes`, all of them.
#[must_use]
pub fn expecting(plan: Plan, bytes: &[u8]) -> Plan {
    Plan { total: Total::Known(bytes.len()), ..plan }
}

/// `conn` of process `at` is closed by `by`: always, for a client's (process
/// 1), which is made when it dials; for a server's (process 0), unless a
/// fault kept its client from connecting.
fn closed(at: usize, conn: &'static str, by: Time, config: &Config) -> Expect {
    Expect::Closed { at, conn, by, optional: at == 0 && !calm(config) }
}

fn serve(name: &'static str, answers: Vec<Option<Plan>>) -> Serve {
    Serve { name, addr: local(0), answers, close: ServeClose::Answered(ms(30_000)) }
}

fn dial(to: &'static str, at: Time, plan: Plan) -> Dial {
    Dial { to: Target::Named(to), at, plan }
}

/// Listen on port 0, accept, bind the first socket and reject the second:
/// the first exchanges both ways, the second is closed unread. With a slab of
/// two sockets, the listener and the first fill it, so the second waits in
/// the kernel's backlog until the first is reclaimed.
#[must_use]
pub fn accept(seed: u64, config: Config) -> World {
    let served = expecting(plan("served", Box::from(&b"welcome"[..]), 4, Reads::Mixed(8)), b"hello");
    let first = expecting(plan("first", Box::from(&b"hello"[..]), 4, Reads::Mixed(8)), b"welcome");
    let second = expecting(plan("second", Box::new([]), 4, Reads::Fill(4)), b"");
    let mut expect =
        vec![closed(0, "served", END, &config), closed(1, "first", END, &config), closed(1, "second", END, &config)];
    if calm(&config) {
        expect.push(Expect::Receives { at: 0, conn: "served", bytes: Box::from(&b"hello"[..]), by: END });
        expect.push(Expect::Receives { at: 1, conn: "first", bytes: Box::from(&b"welcome"[..]), by: END });
        expect.push(Expect::Receives { at: 1, conn: "second", bytes: Box::new([]), by: END });
    } else {
        // Whichever client the server binds, it receives "hello" or nothing.
        expect.push(Expect::Prefix { at: 0, conn: "served", bytes: Box::from(&b"hello"[..]), by: END });
    }
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    world.spawn(limits(2), Owner::new(seed, vec![serve("server", vec![Some(served), None])], Vec::new()));
    let dials = vec![dial("server", Time::ZERO, first), dial("server", ms(1), second)];
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// Connects: two made, a third refused for want of a socket slot (`Busy`),
/// and one to an address nothing listens on (`Refused`).
#[must_use]
pub fn connects(seed: u64, config: Config) -> World {
    let refusals: &'static [Error] =
        if calm(&config) { &[Error::Refused] } else { &[Error::Refused, Error::Busy, Error::TimedOut] };
    let expect = vec![
        Expect::Fails { at: 1, conn: "c", errors: &[Error::Busy], by: END },
        Expect::Fails { at: 1, conn: "d", errors: refusals, by: END },
        closed(1, "a", END, &config),
        closed(1, "b", END, &config),
        closed(1, "c", END, &config),
        closed(1, "d", END, &config),
    ];
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let sink = |name| plan(name, Box::new([]), 4, Reads::Fill(4));
    world.spawn(
        limits(3),
        Owner::new(seed, vec![serve("server", vec![Some(sink("a-served")), Some(sink("b-served"))])], Vec::new()),
    );
    let say = |name| plan(name, Box::from(&b"hi"[..]), 4, Reads::Fill(1));
    let dials = vec![
        dial("server", Time::ZERO, say("a")),
        dial("server", Time::ZERO, say("b")),
        dial("server", Time::ZERO, say("c")),
        Dial { to: Target::Addr(local(9)), at: ms(2_000), plan: say("d") },
    ];
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// Connects that wait: the listener's slab is full, so it accepts no more,
/// and its backlog of two holds the next two connections; the connects past
/// them wait in the kernel, until their owners abort them, which cancels
/// them. The listener closes on the connections still waiting.
#[must_use]
pub fn backlog(seed: u64, config: Config) -> World {
    let names = ["q-0", "q-1", "q-2", "q-3", "q-4"];
    let mut expect = Vec::new();
    let mut dials = Vec::new();
    for (n, name) in names.into_iter().enumerate() {
        expect.push(closed(1, name, END, &config));
        let abort = Plan { close: When::At(ms(50)), abort: true, ..plan(name, Box::new([]), 4, Reads::Never) };
        dials.push(dial("server", ms(u64::try_from(n).expect("a few")), abort));
    }
    let holder = Plan { finish: false, close: When::At(ms(100)), ..plan("holder", Box::new([]), 4, Reads::Never) };
    let serve = Serve { name: "server", addr: local(0), answers: vec![Some(holder)], close: ServeClose::At(ms(200)) };
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    world.spawn(limits(2), Owner::new(seed, vec![serve], Vec::new()));
    world.spawn(limits(5), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// Descriptors run out, two a process: the third connect's socket finds
/// none, which io tells as `Busy`; and the listener, holding its own and one
/// accepted, finds none for its next accept, so it starves until one comes
/// back or its retry deadline passes, while the second client waits in the
/// kernel's backlog.
#[must_use]
pub fn descriptors(seed: u64, config: Config) -> World {
    let config = Config { max_fds: 2, ..config };
    let mut rng = Rng::new(seed ^ 0xfd);
    let messages = [framed(&short(&mut rng, 100)), framed(&short(&mut rng, 100))];
    let mut expect = Vec::new();
    for (name, bytes) in ["d-0", "d-1"].into_iter().zip(&messages) {
        expect.push(closed(1, name, END, &config));
        expect.push(if calm(&config) {
            Expect::Receives { at: 1, conn: name, bytes: bytes.clone(), by: END }
        } else {
            Expect::Prefix { at: 1, conn: name, bytes: bytes.clone(), by: END }
        });
    }
    expect.push(closed(1, "d-2", END, &config));
    if calm(&config) {
        expect.push(Expect::Fails { at: 1, conn: "d-2", errors: &[Error::Busy], by: END });
    }
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let answers = vec![Some(echo("e-0", 16, Reads::Mixed(16))), Some(echo("e-1", 16, Reads::Mixed(16)))];
    world.spawn(limits(3), Owner::new(seed, vec![serve("server", answers)], Vec::new()));
    let mut dials = Vec::new();
    for (name, bytes) in ["d-0", "d-1"].into_iter().zip(messages) {
        dials.push(dial("server", Time::ZERO, expecting(plan(name, bytes.clone(), 16, Reads::Mixed(16)), &bytes)));
    }
    dials.push(dial("server", Time::ZERO, plan("d-2", Box::from(&b"hi"[..]), 4, Reads::Fill(1))));
    world.spawn(limits(3), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// Two listeners under a batch of one accept an iteration: their listens
/// complete together, and the second's first accept waits an iteration
/// (io.md, 3.2).
#[must_use]
pub fn batch(seed: u64, config: Config) -> World {
    let mut rng = Rng::new(seed ^ 0xba7c);
    let messages = [framed(&short(&mut rng, 100)), framed(&short(&mut rng, 100))];
    let mut expect = Vec::new();
    for (name, bytes) in ["b-0", "b-1"].into_iter().zip(&messages) {
        expect.push(closed(1, name, END, &config));
        expect.push(if calm(&config) {
            Expect::Receives { at: 1, conn: name, bytes: bytes.clone(), by: END }
        } else {
            Expect::Prefix { at: 1, conn: name, bytes: bytes.clone(), by: END }
        });
    }
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let serves = vec![
        serve("one", vec![Some(echo("one-0", 16, Reads::Mixed(16)))]),
        serve("two", vec![Some(echo("two-0", 16, Reads::Mixed(16)))]),
    ];
    world.spawn(Limits { accepts: 1, ..limits(4) }, Owner::new(seed, serves, Vec::new()));
    let mut dials = Vec::new();
    for ((name, to), bytes) in [("b-0", "one"), ("b-1", "two")].into_iter().zip(messages) {
        dials.push(dial(to, Time::ZERO, expecting(plan(name, bytes.clone(), 16, Reads::Mixed(16)), &bytes)));
    }
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// A server under a slab of two that dials itself: its accept is armed while
/// a slot is free, its own connect takes that slot, and the socket the accept
/// brings finds none. io discards it, unannounced, and the dialer hears the
/// end of a stream no one answered.
#[must_use]
pub fn discard(seed: u64, config: Config) -> World {
    let mut expect = vec![closed(0, "dialer", END, &config)];
    if calm(&config) {
        expect.push(Expect::Ends { at: 0, conn: "dialer", by: END });
    }
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let serve = Serve { name: "server", addr: local(0), answers: Vec::new(), close: ServeClose::At(ms(50)) };
    let dialer = expecting(plan("dialer", Box::new([]), 4, Reads::Fill(1)), b"");
    world.spawn(limits(2), Owner::new(seed, vec![serve], vec![dial("server", Time::ZERO, dialer)]));
    world
}

/// A burst of connects past the socket slab and the refusals io holds, in
/// one tick: two made, two refused, and io takes no more requests until the
/// next up pass tells those refusals (`Io::takes`); then the last two are
/// refused too.
#[must_use]
pub fn burst(seed: u64, config: Config) -> World {
    let names = ["x-0", "x-1", "x-2", "x-3", "x-4", "x-5"];
    let mut rng = Rng::new(seed ^ 0xb0857);
    let messages = [framed(&short(&mut rng, 100)), framed(&short(&mut rng, 100))];
    let mut expect = Vec::new();
    for name in names {
        expect.push(closed(1, name, END, &config));
    }
    if calm(&config) {
        for (name, bytes) in names.into_iter().zip(&messages) {
            expect.push(Expect::Receives { at: 1, conn: name, bytes: bytes.clone(), by: END });
        }
        for name in &names[2..] {
            expect.push(Expect::Fails { at: 1, conn: name, errors: &[Error::Busy], by: END });
        }
    }
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let answers = vec![Some(echo("y-0", 16, Reads::Mixed(16))), Some(echo("y-1", 16, Reads::Mixed(16)))];
    world.spawn(limits(3), Owner::new(seed, vec![serve("server", answers)], Vec::new()));
    let mut dials = Vec::new();
    for (n, name) in names.into_iter().enumerate() {
        let plan = match messages.get(n) {
            Some(bytes) => expecting(plan(name, bytes.clone(), 16, Reads::Mixed(16)), bytes),
            None => plan(name, Box::from(&b"hi"[..]), 4, Reads::Fill(1)),
        };
        dials.push(dial("server", Time::ZERO, plan));
    }
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// Two clients exchange random bytes with an echo server, both ways, every
/// read a fill, a scan for `\n` or `\r\n`, or a single byte, every send cut
/// to a chunk drawn from the seed.
#[must_use]
pub fn exchange(seed: u64, config: Config) -> World {
    let mut rng = Rng::new(seed ^ 0xec40);
    let chunk = |rng: &mut Rng| u32::try_from(rng.between(1, 32)).expect("a small chunk");
    let one = framed(&short(&mut rng, 400));
    let two = framed(&short(&mut rng, 400));
    let receives = |at, conn, bytes: &[u8]| -> Expect {
        if calm(&config) {
            Expect::Receives { at, conn, bytes: Box::from(bytes), by: END }
        } else {
            Expect::Prefix { at, conn, bytes: Box::from(bytes), by: END }
        }
    };
    let expect = vec![receives(1, "one", &one), receives(1, "two", &two)];
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let answers = vec![
        Some(echo("echo-1", chunk(&mut rng), Reads::Mixed(16))),
        Some(echo("echo-2", chunk(&mut rng), Reads::Mixed(16))),
    ];
    world.spawn(limits(3), Owner::new(seed, vec![serve("server", answers)], Vec::new()));
    let dials = vec![
        dial("server", Time::ZERO, expecting(plan("one", one.clone(), chunk(&mut rng), Reads::Mixed(16)), &one)),
        dial("server", Time::ZERO, expecting(plan("two", two.clone(), chunk(&mut rng), Reads::Mixed(16)), &two)),
    ];
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// The kernel's receive buffer in the backpressure scenario.
pub const BUFFER: u32 = 64;

/// A server that does not demand stops its peer: until it starts reading,
/// the client hands io no more than the server's kernel buffer, its intake
/// and the client's output hold; then everything arrives.
#[must_use]
pub fn backpressure(seed: u64, config: Config) -> World {
    let config = Config { buffer: BUFFER, ..config };
    let mut rng = Rng::new(seed ^ 0xbac4);
    let upload = message(&mut rng, 2_000);
    let start = ms(50);
    let most = u64::from(BUFFER + limits(2).intake + limits(2).output);
    let expect = vec![
        Expect::Holds { at: 1, conn: "upload", most, until: start },
        if calm(&config) {
            Expect::Receives { at: 0, conn: "sink", bytes: upload.clone(), by: END }
        } else {
            Expect::Prefix { at: 0, conn: "sink", bytes: upload.clone(), by: END }
        },
        closed(1, "upload", END, &config),
    ];
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let sink = expecting(plan("sink", Box::new([]), 4, Reads::From(start, 16)), &upload);
    world.spawn(limits(2), Owner::new(seed, vec![serve("server", vec![Some(sink)])], Vec::new()));
    let upload = Plan { close: When::Sent, ..plan("upload", upload, 16, Reads::Never) };
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), vec![dial("server", Time::ZERO, upload)]));
    world
}

/// A refusal in the middle of an upload: the server reads the first line,
/// answers, and closes gracefully while the client is still sending. The
/// answer reaches the client, not lost to a reset (io.md, 3).
#[must_use]
pub fn refusal_mid_upload(seed: u64, config: Config) -> World {
    let mut rng = Rng::new(seed ^ 0x9e7);
    let mut upload = b"PUT\n".to_vec();
    for _ in 0..3_000 {
        upload.push(b'a' + u8::try_from(rng.below(26)).expect("a letter"));
    }
    let answer = |at| {
        if calm(&config) {
            Expect::Receives { at, conn: "uploader", bytes: Box::from(&b"no\n"[..]), by: END }
        } else {
            Expect::Prefix { at, conn: "uploader", bytes: Box::from(&b"no\n"[..]), by: END }
        }
    };
    let mut expect = vec![answer(1), closed(0, "refuser", END, &config), closed(1, "uploader", END, &config)];
    // Calm, the close is graceful all the way: the refuser half-closes and
    // drains, so the uploader hears the end, not a reset that would lose the
    // answer.
    if calm(&config) {
        expect.push(Expect::Ends { at: 1, conn: "uploader", by: END });
    }
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let refuser = Plan {
        wait: 4,
        finish: false,
        close: When::Sent,
        ..plan("refuser", Box::from(&b"no\n"[..]), 4, Reads::Scan(Delimiter::LF, 16))
    };
    world.spawn(limits(2), Owner::new(seed, vec![serve("server", vec![Some(refuser)])], Vec::new()));
    let uploader = expecting(plan("uploader", upload.into_boxed_slice(), 16, Reads::Scan(Delimiter::LF, 16)), b"no\n");
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), vec![dial("server", Time::ZERO, uploader)]));
    world
}

/// An abort while a send waits for room and a receive for bytes: both are
/// cancelled, and both ends close.
#[must_use]
pub fn abort(seed: u64, config: Config) -> World {
    let config = Config { buffer: BUFFER, ..config };
    let mut rng = Rng::new(seed ^ 0xab07);
    let upload = message(&mut rng, 2_000);
    let expect = vec![
        closed(1, "aborter", END, &config),
        closed(0, "late", END, &config),
        Expect::Prefix { at: 0, conn: "late", bytes: upload.clone(), by: END },
    ];
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let late = plan("late", Box::new([]), 4, Reads::From(ms(20), 16));
    world.spawn(limits(2), Owner::new(seed, vec![serve("server", vec![Some(late)])], Vec::new()));
    let aborter = Plan { close: When::At(ms(10)), abort: true, ..plan("aborter", upload, 16, Reads::Fill(4)) };
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), vec![dial("server", Time::ZERO, aborter)]));
    world
}

/// A graceful close the peer never ends: the server flushes, half-closes and
/// waits for the client's end, which never comes, until its close deadline
/// aborts it.
#[must_use]
pub fn close_deadline(seed: u64, config: Config) -> World {
    let timeout = limits(2).close_timeout;
    let expect = vec![
        closed(0, "leaver", Time::ZERO.saturating_add(timeout).saturating_add(Duration::from_millis(500)), &config),
        closed(1, "stayer", END, &config),
    ];
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    let leaver = Plan { finish: false, close: When::Sent, ..plan("leaver", Box::from(&b"bye"[..]), 4, Reads::Never) };
    world.spawn(limits(2), Owner::new(seed, vec![serve("server", vec![Some(leaver)])], Vec::new()));
    let stayer = Plan { finish: false, close: When::At(ms(5_000)), ..plan("stayer", Box::new([]), 4, Reads::Never) };
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), vec![dial("server", Time::ZERO, stayer)]));
    world
}

/// Closes and aborts at random moments, in every state: connects in flight,
/// accepts armed, sends and receives waiting, graceful closes under way, the
/// listener closing with sockets waiting; and, with faults, resets, refusals
/// and cancels racing. Everything closes, and what arrives is a prefix of
/// what was sent.
#[must_use]
pub fn closes(seed: u64, config: Config) -> World {
    let mut rng = Rng::new(seed ^ 0xc105e);
    let when = |rng: &mut Rng| match rng.below(3) {
        0 => When::Done,
        _ => When::At(Time::from_nanos(rng.below(30_000_000))),
    };
    let names = ["c-0", "c-1", "c-2"];
    let mut answers = Vec::new();
    for name in ["s-0", "s-1", "s-2"] {
        let chunk = u32::try_from(rng.between(1, 32)).expect("small");
        let echo = Plan { close: when(&mut rng), abort: rng.chance(500), ..echo(name, chunk, Reads::Mixed(16)) };
        answers.push(Some(echo));
    }
    let mut dials = Vec::new();
    let mut expect = Vec::new();
    for name in names {
        let bytes = framed(&short(&mut rng, 300));
        let chunk = u32::try_from(rng.between(1, 32)).expect("small");
        let at = Time::from_nanos(rng.below(20_000_000));
        let reads = expecting(plan(name, bytes.clone(), chunk, Reads::Mixed(16)), &bytes);
        let plan = Plan { close: when(&mut rng), abort: rng.chance(500), ..reads };
        expect.push(Expect::Prefix { at: 1, conn: name, bytes, by: END });
        dials.push(dial("server", at, plan));
    }
    let close = ServeClose::At(Time::from_nanos(rng.between(5_000_000, 40_000_000)));
    let serve = Serve { name: "server", addr: local(0), answers, close };
    let mut world = World::new(seed, config, Referee::new(seed, expect));
    world.spawn(limits(3), Owner::new(seed, vec![serve], Vec::new()));
    world.spawn(limits(2), Owner::new(seed + 1, Vec::new(), dials));
    world
}

/// A scenario: a world from a seed and a configuration.
pub type Scenario = fn(u64, Config) -> World;

/// Every scenario, by name, for the sweeps.
pub const SCENARIOS: [(&str, Scenario); 13] = [
    ("accept", accept),
    ("connects", connects),
    ("backlog", backlog),
    ("descriptors", descriptors),
    ("batch", batch),
    ("discard", discard),
    ("burst", burst),
    ("exchange", exchange),
    ("backpressure", backpressure),
    ("refusal_mid_upload", refusal_mid_upload),
    ("abort", abort),
    ("close_deadline", close_deadline),
    ("closes", closes),
];
