//! The fake client's step tests (testing-strategy.md, 2.1): its plans run
//! over a kernel and a server played by hand: lines sent in pieces and their
//! answers checked; a wrong answer, and one after a refusal, failing the
//! world; `busy` retried after the backoff; the line past the limit; an
//! abort at its time. The worlds it runs in are tests/echo.

#![expect(clippy::disallowed_types, reason = "the hand-played kernel keeps what it holds in Vecs")]

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::net::{Ipv4Addr, SocketAddr};

use skein_io::kernel::{Addr, Complete, Done, Error, Fd, Op, Submit};
use skein_lib::{Duration, Time, Wall};

use crate::{Client, HalfClose, Limits, Plan, Then, iterate, worst_case};

const LIMITS: Limits = Limits {
    io: skein_io::Limits {
        sockets: 2,
        refusals: 1,
        intake: 32,
        receive: 32,
        output: 32,
        sends: 2,
        accepts: 1,
        backlog: 1,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    },
    line: 16,
    queue: 4,
};

const PLAN: Plan = Plan {
    at: Time::from_nanos(1_000),
    seed: 42,
    lines: 3,
    shortest: 1,
    longest: 16,
    long: None,
    ahead: 1,
    piece: 3,
    read_from: Some(Time::ZERO),
    send_limit: u64::MAX,
    then: Then::Finish,
    half_close: None,
    abort_at: None,
    retries: 0,
    backoff: Duration::from_millis(5),
};

fn server() -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 7))
}

/// The client, over a kernel played by hand: what can complete at once
/// does, in the next turn; receives wait for the test, which plays the
/// server.
struct Rig {
    client: Client,
    now: Time,
    done: Vec<Complete>,
    /// Receives in flight.
    held: Vec<Submit>,
    /// What the client sent, not yet answered by the test.
    sent: Vec<u8>,
    shut: bool,
    closed: u32,
    connects: u32,
    next_fd: i32,
}

impl Rig {
    fn new(plan: Plan) -> Rig {
        let mut client = Client::new(&LIMITS, &[plan]);
        client.dial(server());
        Rig {
            client,
            now: Time::ZERO,
            done: Vec::new(),
            held: Vec::new(),
            sent: Vec::new(),
            shut: false,
            closed: 0,
            connects: 0,
            next_fd: 3,
        }
    }

    fn turn(&mut self) {
        while self.client.completions().room() > 0 && !self.done.is_empty() {
            let complete = self.done.remove(0);
            self.client.completions().push(complete);
        }
        iterate(&mut self.client, self.now, Wall::EPOCH);
        while let Some(submit) = self.client.submissions().pop() {
            self.answer(submit);
        }
    }

    fn settle(&mut self) {
        for _ in 0..200_u32 {
            self.turn();
            if self.done.is_empty() && !self.client.work_pending(self.now) {
                return;
            }
        }
        panic!("the client settles");
    }

    /// Moves time to `at` and settles.
    fn at(&mut self, at: Time) {
        self.now = at;
        self.settle();
    }

    fn answer(&mut self, submit: Submit) {
        let Submit { op, kind } = submit;
        let result = match &kind {
            Op::Socket { .. } => {
                let fd = Fd::new(self.next_fd);
                self.next_fd = self.next_fd.checked_add(1).expect("few descriptors");
                Ok(Done::Fd(fd))
            }
            Op::Connect { .. } => {
                self.connects = self.connects.checked_add(1).expect("few");
                Ok(Done::Nothing)
            }
            Op::Send { bytes, from, .. } => {
                let from = usize::try_from(*from).expect("small");
                self.sent.extend_from_slice(&bytes[from..]);
                let left = bytes.len().checked_sub(from).expect("a send from within its bytes");
                Ok(Done::Count(u32::try_from(left).expect("small")))
            }
            Op::Shutdown { .. } => {
                self.shut = true;
                Ok(Done::Nothing)
            }
            Op::Close { .. } => {
                self.closed = self.closed.checked_add(1).expect("few");
                Ok(Done::Nothing)
            }
            Op::Cancel { target } => {
                let mut found = None;
                for (at, held) in self.held.iter().enumerate() {
                    if held.op == *target {
                        found = Some(at);
                    }
                }
                if let Some(at) = found {
                    let held = self.held.remove(at);
                    self.done.push(Complete { op: held.op, kind: held.kind, result: Err(Error::Cancelled) });
                    Ok(Done::Nothing)
                } else {
                    Err(Error::TooLate)
                }
            }
            Op::Recv { .. } => {
                self.held.push(Submit { op, kind });
                return;
            }
            Op::Bind { .. } | Op::Listen { .. } | Op::Accept { .. } => panic!("the client listens to no one"),
            Op::Open { .. }
            | Op::Read { .. }
            | Op::Write { .. }
            | Op::Sync { .. }
            | Op::Stat { .. }
            | Op::Rename { .. }
            | Op::Remove { .. }
            | Op::MakeDirectory { .. }
            | Op::List { .. } => panic!("the client opens no file"),
        };
        self.done.push(Complete { op, kind, result });
    }

    /// The server sends `bytes`, which fit a receive; none is its end.
    fn deliver(&mut self, bytes: &[u8]) {
        let held = self.held.pop().expect("a receive in flight");
        let Submit { op, kind: Op::Recv { fd, mut buf } } = held else { unreachable!("a receive") };
        for (into, byte) in buf.iter_mut().zip(bytes) {
            *into = *byte;
        }
        let n = u32::try_from(bytes.len()).expect("small");
        self.done.push(Complete { op, kind: Op::Recv { fd, buf }, result: Ok(Done::Count(n)) });
        self.settle();
    }

    /// The first line the client sent whole and the test has not answered.
    fn line(&mut self) -> Box<[u8]> {
        let mut end = None;
        for (at, byte) in self.sent.iter().enumerate() {
            if *byte == b'\n' && end.is_none() {
                end = Some(at);
            }
        }
        let end = end.expect("a line sent whole");
        let rest = self.sent.split_off(end.checked_add(1).expect("within the bytes sent"));
        let line = core::mem::replace(&mut self.sent, rest);
        line.into_boxed_slice()
    }

    /// The server echoes the next line.
    fn echo(&mut self) {
        let line = self.line();
        self.deliver(&line);
    }
}

#[test]
fn a_plan_sends_its_lines_in_pieces_checks_each_answer_and_finishes() {
    let mut rig = Rig::new(PLAN);
    rig.settle();
    assert_eq!(rig.connects, 0, "not before its time");
    rig.at(PLAN.at);
    assert_eq!(rig.connects, 1, "connected at its time, once told the server");
    for _ in 0..3_u32 {
        rig.echo();
    }
    let seen = rig.client.seen(0);
    assert_eq!(seen.answered, 3);
    assert!(seen.complete);
    assert!(rig.shut, "finished: half-closed once every line was answered");
    assert_eq!(rig.closed, 0, "and waits for the server's end");
    rig.deliver(b"");
    assert_eq!(rig.closed, 1, "closed once the server ended");
    let seen = rig.client.seen(0);
    assert_eq!((seen.ended, seen.done), (Some(PLAN.at), Some(PLAN.at)));
    assert!(rig.client.is_empty(), "nothing left: {:?}", rig.client);
}

#[test]
fn lines_go_ahead_of_their_answers_as_far_as_the_plan_allows() {
    let mut rig = Rig::new(Plan { ahead: 3, lines: 3, ..PLAN });
    rig.at(PLAN.at);
    let mut lines = 0_u32;
    for byte in &rig.sent {
        if *byte == b'\n' {
            lines = lines.checked_add(1).expect("few");
        }
    }
    assert_eq!(lines, 3, "all three sent before any answer");
}

#[test]
#[should_panic(expected = "byte for byte")]
fn a_wrong_answer_fails_the_world() {
    let mut rig = Rig::new(Plan { shortest: 4, ..PLAN });
    rig.at(PLAN.at);
    let mut line = rig.line();
    line[0] ^= 1;
    rig.deliver(&line);
}

#[test]
#[should_panic(expected = "only a line sent whole")]
fn an_answer_to_no_line_fails_the_world() {
    let mut rig = Rig::new(Plan { send_limit: 0, ..PLAN });
    rig.at(PLAN.at);
    rig.deliver(b"hello\n");
}

#[test]
fn busy_is_retried_after_the_backoff() {
    let mut rig = Rig::new(Plan { retries: 1, ..PLAN });
    rig.at(PLAN.at);
    rig.deliver(b"busy\n");
    rig.deliver(b"");
    assert_eq!(rig.client.seen(0).busy, 1);
    assert_eq!(rig.closed, 1);
    rig.sent.clear();
    rig.at(PLAN.at.saturating_add(PLAN.backoff));
    assert_eq!(rig.connects, 2, "tried again, after its backoff");
    assert_eq!(rig.client.seen(0).ended, None, "the new attempt has not been ended");
    for _ in 0..3_u32 {
        rig.echo();
    }
    assert!(rig.client.seen(0).complete, "served the second time");
}

#[test]
#[should_panic(expected = "nothing after a refusal")]
fn an_answer_after_a_refusal_fails_the_world() {
    let mut rig = Rig::new(PLAN);
    rig.at(PLAN.at);
    rig.deliver(b"busy\n");
    rig.deliver(b"more\n");
}

#[test]
fn the_line_past_the_limit_is_answered_too_long_and_nothing_follows_it() {
    let mut rig = Rig::new(Plan { long: Some(1), lines: 3, then: Then::Linger, ..PLAN });
    rig.at(PLAN.at);
    rig.echo();
    rig.deliver(b"too long\n");
    let seen = rig.client.seen(0);
    assert!(seen.too_long && seen.complete, "the plan's long line was refused, as planned");
    let sent = rig.client.seen(0).handed;
    rig.deliver(b"");
    assert!(rig.client.seen(0).handed == sent, "nothing sent after the refusal");
    assert_eq!(rig.closed, 1, "closed on the server's end, with no retry: complete");
}

#[test]
fn an_abort_at_its_time_stops_whatever_it_does() {
    let abort = PLAN.at.saturating_add(Duration::from_millis(1));
    let mut rig = Rig::new(Plan { abort_at: Some(abort), retries: 5, ..PLAN });
    rig.at(PLAN.at);
    rig.echo();
    rig.at(abort);
    assert_eq!(rig.closed, 1, "aborted: the receive cancelled, then closed");
    let seen = rig.client.seen(0);
    assert_eq!(seen.done, Some(abort), "and no retry");
    assert!(rig.client.is_empty());
}

#[test]
fn a_half_close_sends_its_whole_lines_and_a_piece_then_finishes_and_reads_on() {
    let half_close = Some(HalfClose { after: 2, tail: 3 });
    let mut rig = Rig::new(Plan { lines: 4, ahead: 4, half_close, then: Then::Linger, ..PLAN });
    rig.at(PLAN.at);
    assert!(rig.shut, "finished once it handed its whole lines and the piece");
    let mut ends = 0_u32;
    for byte in &rig.sent {
        if *byte == b'\n' {
            ends = ends.checked_add(1).expect("few");
        }
    }
    assert_eq!(ends, 2, "two whole lines, and a piece with no end of line");
    assert_ne!(rig.sent.last(), Some(&b'\n'), "the piece comes last");
    rig.echo();
    rig.echo();
    let seen = rig.client.seen(0);
    assert!(seen.complete && seen.answered == 2, "answered once its whole lines are");
    assert_eq!(rig.sent.len(), 3, "the piece is left, unanswered");
    rig.deliver(b"");
    assert_eq!(rig.closed, 1, "closed once the server ended");
}

#[test]
fn no_line_begins_with_b_so_none_reads_as_busy() {
    for seed in 0..200_u64 {
        let mut rig = Rig::new(Plan { seed, lines: 1, shortest: 5, longest: 5, ..PLAN });
        rig.at(PLAN.at);
        assert_ne!(rig.sent.first(), Some(&b'b'), "seed {seed}: a line's first byte");
    }
}

#[test]
#[should_panic(expected = "ends by an abort of its own")]
fn a_plan_that_never_reads_and_never_aborts_is_refused() {
    let _rig = Rig::new(Plan { read_from: None, ..PLAN });
}

#[test]
fn the_worst_case_grows_with_its_connections() {
    let one = worst_case(&LIMITS, 1).expect("priced");
    let two = worst_case(&LIMITS, 2).expect("priced");
    assert!(two > one, "a connection more costs more");
    assert_eq!(worst_case(&LIMITS, u32::MAX), None, "past a u64, or its timers past a u32");
}
