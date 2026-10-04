//! The connection's cells (examples.md, 3.4): a line and its answer, room
//! asked before the next line, the refusals, the peer's end and its stream's
//! failure in every state, the idle deadline, and the retirement that waits
//! for both io and the domain.

use alloc::boxed::Box;

use skein_echo_domain::{Event as Call, Reply};
use skein_lib::stream::Up;
use skein_lib::{Duration, Time};

use super::{LIMITS, Rig, admitted, close, failed, reply, room, scan, send, session, socket, stream};
use crate::{BUSY, Limits, TOO_LONG, worst_case};

#[test]
fn a_line_is_a_call_and_its_answer_goes_back_after_room_for_the_next_is_asked() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    rig.line(conn, session(1), b"hello\n");
    let out = rig.down(reply(conn, Reply::Echo(Box::from(&b"hello"[..]))));
    assert_eq!(out.io, [send(1, b"hello\n"), room(1)], "the answer, then room for the next one");
    let out = rig.up(stream(conn, Up::Room));
    assert_eq!(out.io, [scan(1)], "room granted: the next line is read");
    rig.line(conn, session(1), b"\n");
    let out = rig.down(reply(conn, Reply::Echo(Box::new([]))));
    assert_eq!(out.io, [send(1, b"\n"), room(1)], "an empty line is answered too");
}

#[test]
fn a_socket_past_the_slab_is_rejected_at_the_entrance() {
    let mut rig = Rig::new();
    rig.listen();
    let _first = rig.bound(1);
    let _second = rig.bound(2);
    let out = rig.accept(3);
    assert_eq!(out.io, [skein_io::Request::Reject { socket: socket(3) }], "no slot: rejected");
    assert_eq!(rig.proto.conns(), 2);
}

#[test]
fn a_peer_the_domain_refuses_is_told_busy_and_closed() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.bound(1);
    let _open = rig.up(stream(conn, Up::Room));
    let out = rig.down(reply(conn, Reply::Busy));
    assert_eq!(out.io, [send(1, BUSY), close(1)], "the refusal in the room greeting asked for, then the close");
    rig.closed(conn).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 0, "retired once io closed it");
}

#[test]
fn a_line_past_the_limit_is_told_too_long_and_closed() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    let long = [b'x'; 16];
    let out = rig.up(stream(conn, Up::Bytes(Box::from(&long[..]))));
    assert_eq!(out.io, [send(1, TOO_LONG), close(1)], "the scan met its maximum without the end of a line");
    assert_eq!(out.calls, [Call::Gone { session: session(1) }], "the session ends with the connection");
    rig.closed(conn).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 0);
}

#[test]
fn a_line_exactly_at_the_limit_is_a_line() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    let mut line = [b'x'; 16];
    line[15] = b'\n';
    rig.line(conn, session(1), &line);
}

#[test]
fn the_peer_ending_while_reading_or_draining_closes_and_ends_the_session() {
    let mut rig = Rig::new();
    rig.listen();
    let reading = rig.reading(1, session(1));
    let out = rig.up(stream(reading, Up::End));
    assert_eq!(out.io, [close(1)]);
    assert_eq!(out.calls, [Call::Gone { session: session(1) }]);

    let draining = rig.reading(2, session(2));
    rig.line(draining, session(2), b"a\n");
    let _sent = rig.down(reply(draining, Reply::Echo(Box::from(&b"a"[..]))));
    let out = rig.up(stream(draining, Up::End));
    assert_eq!(out.io, [close(2)], "the answer queued is flushed by io's graceful close");
    assert_eq!(out.calls, [Call::Gone { session: session(2) }]);
}

#[test]
fn the_peer_ending_while_greeting_closes_with_no_session() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.bound(1);
    let out = rig.up(stream(conn, Up::End));
    assert_eq!(out.io, [close(1)]);
    assert!(out.calls.is_empty(), "never admitted, so nothing to tell the domain");
}

#[test]
fn the_peer_ending_while_its_line_is_answered_gets_the_answer_then_the_close() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    rig.line(conn, session(1), b"last\n");
    rig.up(stream(conn, Up::End)).nothing();
    let out = rig.down(reply(conn, Reply::Echo(Box::from(&b"last"[..]))));
    assert_eq!(out.io, [send(1, b"last\n"), close(1)], "the answer goes out before the close");
    assert!(rig.proto.is_ready(), "the session is owed a Gone, up from the down pass");
    let out = rig.drain();
    assert_eq!(out.calls, [Call::Gone { session: session(1) }]);
    rig.closed(conn).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 0);
}

#[test]
fn the_peer_ending_while_admitted_closes_once_the_domain_answers() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.bound(1);
    let _open = rig.up(stream(conn, Up::Room));
    rig.up(stream(conn, Up::End)).nothing();
    let out = rig.down(admitted(conn, session(1)));
    assert_eq!(out.io, [close(1)], "admitted, but the peer ended: closed, not read");
    assert_eq!(rig.drain().calls, [Call::Gone { session: session(1) }]);
}

#[test]
fn a_stream_that_fails_while_admitting_retires_only_after_the_answer() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.bound(1);
    let _open = rig.up(stream(conn, Up::Room));
    let out = rig.up(stream(conn, failed()));
    assert_eq!(out.io, [close(1)]);
    assert!(out.calls.is_empty(), "no session known yet");
    rig.closed(conn).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 1, "the Open is still out: io's Closed is not enough");
    let out = rig.down(admitted(conn, session(1)));
    out.nothing();
    assert_eq!(rig.drain().calls, [Call::Gone { session: session(1) }], "the session it was given ends");
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 0, "retired once both sides are done");
}

#[test]
fn a_stream_that_fails_while_admitting_and_is_refused_ends_with_nothing_owed() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.bound(1);
    let _open = rig.up(stream(conn, Up::Room));
    let _close = rig.up(stream(conn, failed()));
    rig.down(reply(conn, Reply::Busy)).nothing();
    assert!(!rig.proto.is_ready(), "nothing owed");
    rig.closed(conn).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 0);
}

#[test]
fn a_stream_that_fails_while_answering_tells_gone_at_once_and_drops_the_answer() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    rig.line(conn, session(1), b"x\n");
    let out = rig.up(stream(conn, failed()));
    assert_eq!(out.io, [close(1)]);
    assert_eq!(out.calls, [Call::Gone { session: session(1) }], "behind the line, in the domain's queue");
    rig.closed(conn).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 1, "the line is still out");
    rig.down(reply(conn, Reply::Echo(Box::from(&b"x"[..])))).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 0);
}

#[test]
fn a_stream_that_fails_while_greeting_reading_or_draining_closes_at_once() {
    let mut rig = Rig::with(Limits { conns: 3, ..LIMITS });
    rig.listen();
    let greeting = rig.bound(1);
    let out = rig.up(stream(greeting, failed()));
    assert_eq!(out.io, [close(1)]);
    assert!(out.calls.is_empty());
    let reading = rig.reading(2, session(2));
    let out = rig.up(stream(reading, failed()));
    assert_eq!((out.io, out.calls), ([close(2)].into(), [Call::Gone { session: session(2) }].into()));
    let draining = rig.reading(3, session(3));
    rig.line(draining, session(3), b"y\n");
    let _sent = rig.down(reply(draining, Reply::Echo(Box::from(&b"y"[..]))));
    let out = rig.up(stream(draining, failed()));
    assert_eq!((out.io, out.calls), ([close(3)].into(), [Call::Gone { session: session(3) }].into()));
}

#[test]
fn what_io_told_before_it_took_the_close_is_ignored() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    let _close = rig.up(stream(conn, Up::End));
    rig.up(stream(conn, Up::Bytes(Box::from(&b"late\n"[..])))).nothing();
    rig.up(stream(conn, Up::Room)).nothing();
    rig.up(stream(conn, Up::End)).nothing();
    rig.up(stream(conn, failed())).nothing();
    rig.closed(conn).nothing();
}

#[test]
fn a_connection_idle_past_its_deadline_is_closed() {
    let mut rig = Rig::new();
    rig.listen();
    rig.at(Time::from_nanos(1_000));
    let _greeting = rig.bound(1);
    let deadline = Time::from_nanos(1_000).saturating_add(LIMITS.idle);
    assert_eq!(rig.proto.next_deadline(), Some(deadline), "armed when greeting");
    rig.at(Time::from_nanos(500_000_000));
    let _reader = rig.reading(2, session(2));
    assert_eq!(rig.proto.next_deadline(), Some(deadline), "the earliest is the greeting's");
    rig.at(deadline);
    assert!(rig.proto.is_due(deadline));
    let out = rig.fire();
    assert_eq!(out.io, [close(1)], "greeting, never granted room: closed");
    assert!(out.calls.is_empty());
    let reading = Time::from_nanos(500_000_000).saturating_add(LIMITS.idle);
    assert_eq!(rig.proto.next_deadline(), Some(reading), "the reader's, armed when it was admitted");
    rig.at(reading);
    let out = rig.fire();
    assert_eq!(out.io, [close(2)]);
    assert_eq!(out.calls, [Call::Gone { session: session(2) }]);
}

#[test]
fn each_line_read_and_each_room_granted_arms_the_idle_deadline_afresh() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    rig.at(Time::from_nanos(700_000_000));
    rig.line(conn, session(1), b"z\n");
    assert_eq!(rig.proto.next_deadline(), None, "no deadline while a call is out");
    let _sent = rig.down(reply(conn, Reply::Echo(Box::from(&b"z"[..]))));
    let draining = Time::from_nanos(700_000_000).saturating_add(LIMITS.idle);
    assert_eq!(rig.proto.next_deadline(), Some(draining), "draining waits on the peer");
    rig.at(Time::from_nanos(900_000_000));
    let _read = rig.up(stream(conn, Up::Room));
    let reading = Time::from_nanos(900_000_000).saturating_add(LIMITS.idle);
    assert_eq!(rig.proto.next_deadline(), Some(reading), "room granted is progress");
    assert!(!rig.proto.is_due(draining), "the draining deadline is gone");
}

#[test]
fn the_idle_deadline_is_spread_within_its_limit() {
    let spread = Duration::from_millis(100);
    let mut rig = Rig::with(Limits { spread, ..LIMITS });
    rig.listen();
    let _conn = rig.bound(1);
    let at = rig.proto.next_deadline().expect("armed");
    assert!(at >= Time::ZERO.saturating_add(LIMITS.idle), "never before the idle limit");
    assert!(at < Time::ZERO.saturating_add(LIMITS.idle).saturating_add(spread), "within the spread");
}

#[test]
fn a_reply_to_a_connection_gone_is_dropped() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.bound(1);
    let _close = rig.up(stream(conn, Up::End));
    rig.closed(conn).nothing();
    rig.proto.reclaim();
    assert_eq!(rig.proto.conns(), 0);
    rig.down(reply(conn, Reply::Busy)).nothing();
}

#[test]
#[should_panic(expected = "one reply per call")]
fn a_second_reply_to_one_call_is_a_bug() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.reading(1, session(1));
    let _again = rig.down(admitted(conn, session(1)));
}

#[test]
fn the_worst_case_counts_the_slab_and_a_line_per_connection_and_one_more() {
    let two = worst_case(&LIMITS).expect("priced");
    let three = worst_case(&Limits { conns: 3, ..LIMITS }).expect("priced");
    let longer = worst_case(&Limits { line: 32, ..LIMITS }).expect("priced");
    assert!(three > two, "a connection more costs more");
    assert_eq!(
        longer.checked_sub(two),
        Some(48),
        "sixteen bytes more for each line counted: two connections and one more"
    );
    assert_eq!(worst_case(&Limits { conns: u32::MAX, line: u32::MAX, ..LIMITS }), None, "past a u64");
}

#[test]
fn limits_must_hold_a_connection_the_refusals_and_a_deadline() {
    assert!(LIMITS.is_usable());
    assert!(!Limits { conns: 0, ..LIMITS }.is_usable());
    assert!(!Limits { line: 8, ..LIMITS }.is_usable(), "too short for too long");
    assert!(Limits { line: 9, ..LIMITS }.is_usable());
    assert!(!Limits { idle: Duration::ZERO, ..LIMITS }.is_usable());
    assert_eq!(LIMITS.largest_read(), 16);
    assert_eq!(LIMITS.largest_room(), 16);
}
