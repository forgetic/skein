//! The domain's step tests (testing-strategy.md, 2.1): admission to the slab
//! and the refusal past it, a line answered and counted, a session's end and
//! its slot freed at the reclaim point, and shutdown. Every step is called
//! with exactly its `MAX_OUT` of room, so one that emits more fails the test.

use alloc::boxed::Box;

use skein_lib::{Env, Queue, ReplyTo, Time, Token, Wall};

use crate::{Domain, Event, Limits, MAX_OUT, Reply, Request, step, worst_case};

const LIMITS: Limits = Limits { sessions: 2 };

struct Rig {
    domain: Domain,
    env: Env<Limits>,
    next: u64,
}

impl Rig {
    fn new() -> Rig {
        Rig { domain: Domain::new(&LIMITS), env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: LIMITS }, next: 1 }
    }

    /// One step, with exactly `MAX_OUT` of room; what it emitted.
    fn step(&mut self, event: Event) -> Option<Request> {
        let mut out = Queue::with_capacity(MAX_OUT);
        step(&mut self.domain, &self.env, event, &mut out);
        let request = out.pop();
        assert!(out.is_empty(), "at most MAX_OUT requests");
        request
    }

    /// A call's `ReplyTo`, and the token it names the call by.
    fn call(&mut self) -> (ReplyTo, Token) {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).expect("few calls");
        (ReplyTo::new(token), token)
    }

    /// An `Open`, and its answer, checked to answer its call.
    fn open(&mut self) -> Reply {
        let (reply_to, token) = self.call();
        self.answer(Event::Open { reply_to }, token)
    }

    fn line(&mut self, session: Token, text: &[u8]) -> Reply {
        let (reply_to, token) = self.call();
        self.answer(Event::Line { session, reply_to, text: Box::from(text) }, token)
    }

    fn answer(&mut self, event: Event, call: Token) -> Reply {
        match self.step(event) {
            Some(Request::Reply { to, reply }) => {
                assert_eq!(to.into_token(), call, "the reply answers its own call");
                reply
            }
            Some(Request::Stop) | None => panic!("a call is answered"),
        }
    }

    fn admitted(&mut self) -> Token {
        match self.open() {
            Reply::Admitted { session } => session,
            Reply::Busy | Reply::Echo(_) => panic!("admitted"),
        }
    }
}

#[test]
fn opens_are_admitted_until_the_slab_is_full_then_refused() {
    let mut rig = Rig::new();
    let first = rig.admitted();
    let second = rig.admitted();
    assert_ne!(first, second, "each session is named apart");
    assert_eq!(rig.open(), Reply::Busy, "no third session: refused at the entrance");
    assert_eq!(rig.domain.sessions(), 2, "the refusal made nothing");
}

#[test]
fn a_line_is_answered_with_its_text_and_counted() {
    let mut rig = Rig::new();
    let session = rig.admitted();
    assert_eq!(rig.line(session, b"hello"), Reply::Echo(Box::from(&b"hello"[..])));
    assert_eq!(rig.line(session, b""), Reply::Echo(Box::from(&b""[..])), "an empty line is a line");
    assert_eq!(rig.domain.lines(session), Some(2));
}

#[test]
fn a_session_that_is_gone_frees_its_slot_at_the_reclaim_point() {
    let mut rig = Rig::new();
    let first = rig.admitted();
    let _second = rig.admitted();
    assert_eq!(rig.step(Event::Gone { session: first }), None, "gone is answered by nothing");
    assert_eq!(rig.domain.lines(first), Some(0), "it lasts until the reclaim point");
    assert_eq!(rig.open(), Reply::Busy, "its slot is not free before then");
    rig.domain.reclaim();
    assert_eq!(rig.domain.lines(first), None, "reclaimed");
    let third = rig.admitted();
    assert_ne!(third, first, "a session's name is never reused");
}

#[test]
fn shutdown_stops_admission_once_and_sessions_run_on() {
    let mut rig = Rig::new();
    let session = rig.admitted();
    assert_eq!(rig.step(Event::Shutdown), Some(Request::Stop), "the listener is asked to stop");
    assert!(rig.domain.is_stopped());
    assert_eq!(rig.step(Event::Shutdown), None, "asked once");
    assert_eq!(rig.open(), Reply::Busy, "no one more is admitted, though a slot is free");
    assert_eq!(rig.line(session, b"still"), Reply::Echo(Box::from(&b"still"[..])), "a session runs on");
    let _gone = rig.step(Event::Gone { session });
    rig.domain.reclaim();
    assert!(rig.domain.is_empty());
}

#[test]
#[should_panic(expected = "a line names a session not yet gone")]
fn a_line_for_a_session_gone_is_a_bug() {
    let mut rig = Rig::new();
    let session = rig.admitted();
    let _gone = rig.step(Event::Gone { session });
    rig.domain.reclaim();
    let _reply = rig.line(session, b"late");
}

#[test]
#[should_panic(expected = "a session is told gone once")]
fn a_session_told_gone_twice_is_a_bug() {
    let mut rig = Rig::new();
    let session = rig.admitted();
    let _gone = rig.step(Event::Gone { session });
    rig.domain.reclaim();
    let _again = rig.step(Event::Gone { session });
}

#[test]
fn the_worst_case_is_the_slab_of_sessions_and_grows_with_it() {
    let two = worst_case(&LIMITS).expect("priced");
    let four = worst_case(&Limits { sessions: 4 }).expect("priced");
    assert!(two > 0, "a slab of two takes heap");
    assert_eq!(four, two * 2, "each session costs its slot");
    assert_eq!(worst_case(&Limits { sessions: 0 }), Some(0));
}
