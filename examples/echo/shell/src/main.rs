//! `skein-echo`, the echo example's main (examples.md, 4): its
//! configuration, startup (shell.md, 6), and the loop of
//! programming-model.md, section 2, over the shell kit's kernel and clock.
//!
//! ```text
//! skein-echo [ADDRESS] [--memory BYTES]
//! ```
//!
//! It listens at `ADDRESS` (127.0.0.1:7007 unless given; port 0 picks one)
//! and answers each line with itself. SIGINT and SIGTERM arrive through
//! io (io.md, section 7): it stops admitting clients, drains its sessions
//! and exits successfully. A listener failure exits with a diagnostic.

use std::env;
use std::net::SocketAddr;
use std::process::ExitCode;

use skein_echo_service::{self as service, Limits, Service};
use skein_lib::Duration;
use skein_shell::{Clock, Config, Kernel, Now, Wait};

#[cfg(test)]
mod tests;

const USAGE: &str = "usage: skein-echo [ADDRESS] [--memory BYTES]";

/// Where it listens unless told.
const ADDRESS: &str = "127.0.0.1:7007";

/// The memory the worst case must fit unless told: 64 MiB.
const MEMORY: u64 = 64 << 20;

/// What main is configured with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Configuration {
    addr: SocketAddr,
    /// The bytes the service may hold at its worst.
    memory: u64,
    limits: Limits,
}

fn main() -> ExitCode {
    let configuration = match configure(env::args().skip(1)) {
        Ok(configuration) => configuration,
        Err(why) => {
            eprintln!("skein-echo: {why}\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    match run(&configuration) {
        Ok(()) => ExitCode::SUCCESS,
        Err(why) => {
            eprintln!("skein-echo: {why}");
            ExitCode::FAILURE
        }
    }
}

/// The configuration from the arguments, and the limits of every layer.
fn configure(mut args: impl Iterator<Item = String>) -> Result<Configuration, String> {
    let mut addr = None;
    let mut memory = MEMORY;
    while let Some(arg) = args.next() {
        if arg == "--memory" {
            let bytes = args.next().ok_or("--memory takes a number of bytes")?;
            memory = bytes.parse().map_err(|_| format!("--memory takes a number of bytes, not {bytes:?}"))?;
        } else if addr.is_none() {
            addr = Some(arg.parse().map_err(|_| format!("{arg:?} is not an address and port"))?);
        } else {
            return Err(format!("unexpected argument {arg:?}"));
        }
    }
    let addr = match addr {
        Some(addr) => addr,
        None => ADDRESS.parse().map_err(|_| "the default address parses".to_owned())?,
    };
    Ok(Configuration { addr, memory, limits: limits() })
}

/// The limits of every layer: a thousand connections of lines up to 4 KiB,
/// with fewer sessions than connections and fewer connections than
/// sockets, so that each layer refuses at its own entrance first
/// (programming-model.md, 7). io accepts only while a socket slot is free,
/// so past the listener and the protocol layer's connections it keeps an
/// accept batch of slots more: the sockets the protocol layer rejects, each
/// holding its slot while io closes it.
const fn limits() -> Limits {
    Limits {
        io: skein_io::Limits {
            sockets: 1024 + 1 + 64 + 1,
            refusals: 1,
            intake: 4096,
            receive: 4096,
            output: 8192,
            sends: 8,
            accepts: 64,
            backlog: 1024,
            close_timeout: Duration::from_secs(5),
            retry: Duration::from_millis(50),
        },
        protocol: service::protocol::Limits {
            conns: 1024,
            line: 4096,
            idle: Duration::from_secs(60),
            spread: Duration::from_secs(6),
            retry: Duration::from_millis(100),
        },
        domain: service::domain::Limits { sessions: 1000 },
        queue: 256,
    }
}

/// Startup, then the loop, until a shutdown or listener failure settles.
fn run(configuration: &Configuration) -> Result<(), String> {
    let worst = startup(configuration)?;
    let limits = &configuration.limits;
    // 3. Block termination signals before opening the ring, then adopt their
    // source into io (shell.md, section 6; io.md, section 7).
    let signals = skein_shell::open_termination_signals()
        .map_err(|errno| format!("cannot open termination signals (errno {errno})"))?;
    // 7. The seed, and the kernel, which probes the ring for the floor.
    let seed = skein_shell::seed().map_err(|errno| format!("the kernel refused a seed (errno {errno})"))?;
    let operations = service::operations(limits).ok_or("the ring's size is past a u32")?;
    let mut kernel = Kernel::open(Config { operations }).map_err(|error| error.to_string())?;
    let clock = Clock::new();
    let mut svc = Service::new(limits, configuration.addr, seed);
    svc.adopt_signals(signals).map_err(|_| "the configured io has no slot for termination signals")?;
    eprintln!("skein-echo: at most {worst} bytes of {} configured", configuration.memory);
    let mut told = false;
    let mut short = false;
    loop {
        kernel.reap(svc.completions());
        let Now { now, wall } = clock.now();
        service::iterate(&mut svc, now, wall);
        if svc.is_empty() {
            break;
        }
        let wait = if svc.work_pending(now) {
            Wait::No
        } else {
            match svc.next_deadline() {
                Some(at) => Wait::Until(at),
                None => Wait::Forever,
            }
        };
        if !told && let Some(addr) = svc.listening() {
            eprintln!("skein-echo: listening at {addr}");
            told = true;
        }
        // A shortage the listener waits out is said once, not each retry.
        if !short && let Some(error) = svc.retrying() {
            eprintln!(
                "skein-echo: the listen was refused for want of resources ({error:?}); trying again until it is not"
            );
        }
        short = svc.retrying().is_some() || (short && !told);
        kernel.submit(svc.submissions(), wait);
    }
    match svc.failure() {
        Some(error) => Err(format!("the listener at {} failed: {error:?}", configuration.addr)),
        None => Ok(()),
    }
}

/// Startup's checks (shell.md, 6), before anything is opened: the worst
/// case, in bytes, if the service may start.
fn startup(configuration: &Configuration) -> Result<u64, String> {
    let limits = &configuration.limits;
    // 1. The configuration, the limits of every layer among it, read; and
    // each machine's largest demand within the cap below it (io.md, 2).
    limits.check().map_err(|unusable| format!("the limits cannot run: {unusable:?}"))?;
    // 2. The sum of the worst cases within the memory configured
    // (programming-model.md, 6.3).
    let worst = service::worst_case(limits).ok_or("the worst case is past a u64")?;
    if worst > configuration.memory {
        return Err(format!("the worst case, {worst} bytes, is past the memory configured, {}", configuration.memory));
    }
    // 3. Termination signals are blocked and adopted in run, before the ring.
    // 4. Roots for io's files: the echo opens none.
    // 5. Peer names: the echo dials no one; its address is given as one.
    // 6. TLS: none.
    Ok(worst)
}
