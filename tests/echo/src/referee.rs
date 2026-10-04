//! The echo's referee (testing-strategy.md, 7): each scenario's
//! expectations about what the fake clients saw, never the echo's state;
//! and what belongs to no process, which it injects: the echo's address,
//! told the clients once the echo listens, as a directory would; and the
//! echo's shutdown, once the clients are done or at a given time, so that
//! the world settles.

use std::fmt;

use skein_io::kernel::Addr;
use skein_lib::{Duration, Time};
use skein_world::{Expectation, Expectations, Referee};

use crate::proc::Proc;

/// What a scenario expects of one connection of a fake client: `at` names
/// the client's process, `conn` its plan.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// Liveness: an attempt had every line answered, by `by`.
    Served { at: usize, conn: u32, by: Time },
    /// Liveness: told `busy`, at the domain's entrance, by `by`.
    Busy { at: usize, conn: u32, by: Time },
    /// Liveness: turned away by `by`: told `busy`, or rejected at the
    /// protocol layer's entrance, which closes the socket without a word, so
    /// that the client hears the end, or a reset if its first line was
    /// already there unread.
    TurnedAway { at: usize, conn: u32, by: Time },
    /// Liveness: its line past the limit told `too long`, by `by`.
    TooLong { at: usize, conn: u32, by: Time },
    /// Safety: the server never ends it sooner than `idle` after its last
    /// progress. Liveness: it ends it by `by`. For calm worlds only: under
    /// faults, a server whose side of the stream broke ends it at once, and
    /// latency moves the client's progress after the server's.
    Idled { at: usize, conn: u32, idle: Duration, by: Time },
    /// Safety: until `until`, it handed io no more than `most` bytes:
    /// backpressure stopped it.
    Holds { at: usize, conn: u32, most: u64, until: Time },
    /// Liveness: it handed io at least `bytes`, by `by`.
    Handed { at: usize, conn: u32, bytes: u64, by: Time },
    /// Safety: it never broke. Liveness: every line it sent whole was
    /// answered, then the server ended its stream, by `by`.
    Ends { at: usize, conn: u32, by: Time },
    /// Liveness: a connect was refused, as with nothing listening, by `by`.
    Refused { at: usize, conn: u32, by: Time },
    /// Liveness: done for good, by `by`.
    Finished { at: usize, conn: u32, by: Time },
}

impl Expect {
    const fn whom(&self) -> (usize, u32) {
        match *self {
            Expect::Served { at, conn, .. }
            | Expect::Busy { at, conn, .. }
            | Expect::TurnedAway { at, conn, .. }
            | Expect::TooLong { at, conn, .. }
            | Expect::Idled { at, conn, .. }
            | Expect::Holds { at, conn, .. }
            | Expect::Handed { at, conn, .. }
            | Expect::Ends { at, conn, .. }
            | Expect::Refused { at, conn, .. }
            | Expect::Finished { at, conn, .. } => (at, conn),
        }
    }
}

impl Expectation<Proc> for Expect {
    fn check(&self, now: Time, procs: &[Proc]) -> Result<bool, String> {
        let (at, conn) = self.whom();
        let client = procs.get(at).and_then(Proc::as_client).expect("an expectation names a fake client");
        let seen = client.seen(conn);
        let met = match *self {
            Expect::Served { .. } => seen.complete && !seen.too_long,
            Expect::Busy { .. } => seen.busy > 0,
            Expect::TurnedAway { .. } => seen.busy > 0 || seen.silent > 0 || seen.broken > 0,
            Expect::TooLong { .. } => seen.too_long,
            Expect::Idled { idle, .. } => {
                if let (Some(ended), Some(progress)) = (seen.ended, seen.progress) {
                    let waited = ended.saturating_since(progress);
                    if waited < idle {
                        return Err(format!(
                            "the server ended it {} ms after its last progress, before its idle deadline",
                            waited.as_nanos() / 1_000_000
                        ));
                    }
                }
                seen.ended.is_some()
            }
            Expect::Holds { most, until, .. } => {
                if seen.handed > most {
                    return Err(format!("it handed io {} bytes, past {most}: backpressure failed", seen.handed));
                }
                now >= until
            }
            Expect::Handed { bytes, .. } => seen.handed >= bytes,
            Expect::Ends { .. } => {
                if seen.broken > 0 {
                    return Err("its stream broke".to_owned());
                }
                seen.complete && seen.ended.is_some()
            }
            Expect::Refused { .. } => seen.failed > 0,
            Expect::Finished { .. } => seen.done.is_some(),
        };
        Ok(met)
    }

    fn deadline(&self) -> Time {
        match *self {
            Expect::Served { by, .. }
            | Expect::Busy { by, .. }
            | Expect::TurnedAway { by, .. }
            | Expect::TooLong { by, .. }
            | Expect::Idled { by, .. }
            | Expect::Handed { by, .. }
            | Expect::Ends { by, .. }
            | Expect::Refused { by, .. }
            | Expect::Finished { by, .. } => by,
            Expect::Holds { until, .. } => until,
        }
    }
}

/// Briefly, with its connection and its time in milliseconds.
impl fmt::Debug for Expect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (at, conn) = self.whom();
        let when = self.deadline().as_nanos() / 1_000_000;
        let what = match self {
            Expect::Served { .. } => "served",
            Expect::Busy { .. } => "told busy",
            Expect::TurnedAway { .. } => "turned away",
            Expect::TooLong { .. } => "told too long",
            Expect::Idled { .. } => "idled out, not early",
            Expect::Holds { .. } => "held back",
            Expect::Handed { .. } => "handed its bytes",
            Expect::Ends { .. } => "answered, then ended by the server",
            Expect::Refused { .. } => "refused a connect",
            Expect::Finished { .. } => "finished",
        };
        write!(f, "conn {conn} of process {at} {what} by {when} ms")
    }
}

/// When the referee shuts the echo down.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shutdown {
    /// Once every fake client is done.
    WhenDone,
    /// At this time, whatever the clients are doing.
    At(Time),
}

/// The echo's referee: the expectations, and the two things it injects.
#[derive(Debug)]
pub struct EchoReferee {
    expectations: Expectations<Proc, Expect>,
    shutdown: Shutdown,
    /// The echo's address, once seen, and whether the clients were told it.
    listening: Option<Addr>,
    told: bool,
    /// Every client done, as last seen.
    done: bool,
    /// The echo shut down.
    shut: bool,
    /// When it last observed.
    now: Time,
}

impl EchoReferee {
    #[must_use]
    pub fn new(seed: u64, expectations: Vec<Expect>, shutdown: Shutdown) -> EchoReferee {
        EchoReferee {
            expectations: Expectations::new(seed, expectations),
            shutdown,
            listening: None,
            told: false,
            done: false,
            shut: false,
            now: Time::ZERO,
        }
    }

    /// Whether it should shut the echo down at `now`.
    fn shuts(&self, now: Time) -> bool {
        !self.shut
            && match self.shutdown {
                Shutdown::WhenDone => self.done,
                Shutdown::At(at) => now >= at,
            }
    }
}

impl Referee<Proc> for EchoReferee {
    fn act(&mut self, now: Time, procs: &mut [Proc]) {
        let tell = match self.listening {
            Some(addr) if !self.told => Some(addr),
            Some(_) | None => None,
        };
        let shut = self.shuts(now);
        for proc in procs {
            match proc {
                Proc::Client { client, .. } => {
                    if let Some(addr) = tell {
                        client.dial(addr);
                    }
                }
                Proc::Echo { svc, .. } => {
                    if shut {
                        svc.shutdown();
                    }
                }
            }
        }
        self.told |= tell.is_some();
        self.shut |= shut;
    }

    fn observe(&mut self, now: Time, procs: &[Proc]) {
        self.expectations.observe(now, procs);
        self.now = now;
        let mut done = true;
        for proc in procs {
            match proc {
                Proc::Echo { svc, .. } => {
                    if self.listening.is_none() {
                        self.listening = svc.listening();
                    }
                }
                Proc::Client { client, .. } => done &= client.done(),
            }
        }
        self.done = done;
    }

    fn next_deadline(&self) -> Option<Time> {
        let tell = self.listening.is_some() && !self.told;
        let act = if tell || self.shuts(self.now) {
            Some(self.now)
        } else {
            match self.shutdown {
                Shutdown::At(at) if !self.shut => Some(at),
                Shutdown::WhenDone | Shutdown::At(_) => None,
            }
        };
        let expect = self.expectations.next_deadline();
        match (act, expect) {
            (Some(act), Some(expect)) => Some(act.min(expect)),
            (Some(at), None) | (None, Some(at)) => Some(at),
            (None, None) => None,
        }
    }

    fn overdue(&self, now: Time) -> Option<String> {
        self.expectations.overdue(now)
    }

    fn passed(&self) -> bool {
        self.expectations.passed()
    }
}
