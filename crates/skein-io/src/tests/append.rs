//! Adopted append streams, their write deadline, and graceful close replacing it.

use alloc::boxed::Box;
use skein_lib::stream::{Down, Fault, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Duration, Time, Token};

use super::{Kind, Rig, limits};
use crate::kernel::{Done, Error, Fd, Op, Submit};
use crate::{Event, Limits, Request};

fn adopt() -> (Rig, Token) {
    let mut rig = Rig::new(limits());
    let stream = rig.io.adopt_append(Fd::new(40), Duration::from_millis(10)).expect("append slot");
    rig.next().nothing();
    (rig, stream)
}

fn send(rig: &mut Rig, stream: Token, bytes: &[u8]) -> super::Out {
    rig.down(Request::Stream {
        stream,
        down: Down::Demand { read: Read::Nothing, room: u32::try_from(bytes.len()).expect("small write") },
    })
    .nothing();
    assert_eq!(rig.next().events, [Event::Stream { owner: stream, up: Up::Room }]);
    rig.down(Request::Stream { stream, down: Down::Send(Box::from(bytes)) })
}

fn finish(rig: &mut Rig, stream: Token, close: Submit) {
    assert_eq!(close.kind, Op::Close { fd: Fd::new(40) });
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: stream }]);
    rig.empty();
}

#[test]
fn append_adoption_returns_the_descriptor_on_a_full_slab() {
    let mut rig = Rig::new(Limits { sockets: 1, ..limits() });
    let stream = rig.io.adopt_append(Fd::new(40), Duration::from_millis(10)).expect("one append slot");
    assert_eq!(rig.io.adopt_append(Fd::new(41), Duration::from_millis(10)), Err(Fd::new(41)));
    rig.next().nothing();
    let close = rig.down(Request::Close { entity: stream }).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn room_and_short_appends_keep_one_box_in_order_and_rearm_each_write() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    let Op::Append { bytes, from, .. } = &first.kind else { panic!("append write") };
    assert_eq!(*from, 0);
    let pointer = bytes.as_ptr();
    send(&mut rig, stream, b"two").nothing();
    rig.at(Time::ZERO.saturating_add(Duration::from_millis(5))).nothing();
    let remainder = rig.complete(first, Ok(Done::Count(1))).take(Kind::Append);
    let Op::Append { bytes, from, .. } = &remainder.kind else { panic!("append continuation") };
    assert_eq!(bytes.as_ptr(), pointer, "a short append keeps its box");
    assert_eq!(*from, 1);
    rig.at(Time::ZERO.saturating_add(Duration::from_millis(10))).nothing();
    let second = rig.complete(remainder, Ok(Done::Count(2))).take(Kind::Append);
    let Op::Append { bytes, from, .. } = &second.kind else { panic!("queued append") };
    assert_eq!(bytes.as_ref(), b"two");
    assert_eq!(*from, 0);
    rig.complete(second, Ok(Done::Count(3))).nothing();
    assert_eq!(rig.io.next_deadline(), None);
    let close = rig.down(Request::Stream { stream, down: Down::Finish }).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn write_deadline_drops_queued_bytes_and_fails_once_while_cancel_settles() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    send(&mut rig, stream, b"two").nothing();
    let mut expired = rig.at(Time::ZERO.saturating_add(Duration::from_millis(10)));
    assert_eq!(expired.events, [Event::Stream { owner: stream, up: Up::Failed(Fault::Other) }]);
    let cancel = expired.take(Kind::Cancel);
    rig.at(Time::ZERO.saturating_add(Duration::from_secs(2))).nothing();
    rig.complete(first, Err(Error::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn a_write_that_wins_its_cancel_does_not_start_the_dropped_queue() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    send(&mut rig, stream, b"two").nothing();
    let cancel = rig.at(Time::ZERO.saturating_add(Duration::from_millis(10))).take(Kind::Cancel);
    rig.complete(cancel, Err(Error::TooLate)).nothing();
    let close = rig.complete(first, Ok(Done::Count(1))).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn close_flushes_under_its_own_deadline_and_short_writes_do_not_move_it() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    send(&mut rig, stream, b"two").nothing();
    rig.at(Time::ZERO.saturating_add(Duration::from_millis(5))).nothing();
    rig.down(Request::Close { entity: stream }).nothing();
    let until = Time::ZERO.saturating_add(Duration::from_millis(1005));
    assert_eq!(rig.io.next_deadline(), Some(until));
    rig.at(Time::ZERO.saturating_add(Duration::from_millis(10))).nothing();
    let remainder = rig.complete(first, Ok(Done::Count(1))).take(Kind::Append);
    assert_eq!(rig.io.next_deadline(), Some(until));
    let second = rig.complete(remainder, Ok(Done::Count(2))).take(Kind::Append);
    assert_eq!(rig.io.next_deadline(), Some(until));
    let close = rig.complete(second, Ok(Done::Count(3))).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn a_hung_close_cancels_and_only_closed_follows_its_request() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    send(&mut rig, stream, b"two").nothing();
    rig.down(Request::Close { entity: stream }).nothing();
    rig.at(Time::ZERO.saturating_add(Duration::from_millis(10))).nothing();
    let mut expired = rig.at(Time::ZERO.saturating_add(Duration::from_secs(1)));
    assert!(expired.events.is_empty(), "a close deadline owes no failure event");
    let cancel = expired.take(Kind::Cancel);
    rig.complete(cancel, Ok(Done::Nothing)).nothing();
    let close = rig.complete(first, Err(Error::Cancelled)).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn abort_drops_the_queue_cancels_the_append_and_only_closes() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    send(&mut rig, stream, b"two").nothing();
    let mut aborted = rig.down(Request::Abort { entity: stream });
    assert!(aborted.events.is_empty());
    let cancel = aborted.take(Kind::Cancel);
    rig.complete(first, Err(Error::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn an_append_error_fails_once_and_drops_the_rest() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    send(&mut rig, stream, b"two").nothing();
    let mut failed = rig.complete(first, Err(Error::NoSpace));
    assert_eq!(failed.events, [Event::Stream { owner: stream, up: Up::Failed(Fault::Other) }]);
    let close = failed.take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn a_cancel_the_backend_could_not_submit_is_retried_without_another_failure() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    let cancel = rig.at(Time::ZERO.saturating_add(Duration::from_millis(10))).take(Kind::Cancel);
    let mut retried = rig.complete(cancel, Err(Error::Other(12)));
    assert!(retried.events.is_empty());
    let cancel = retried.take(Kind::Cancel);
    rig.complete(first, Err(Error::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn an_error_while_flushing_drops_later_appends_without_a_gap_or_an_extra_terminal() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"one").take(Kind::Append);
    send(&mut rig, stream, b"two").nothing();
    rig.down(Request::Close { entity: stream }).nothing();
    let mut failed = rig.complete(first, Err(Error::NoSpace));
    assert!(failed.events.is_empty(), "Close owns only its final Closed");
    let close = failed.take(Kind::Close);
    finish(&mut rig, stream, close);
}

#[test]
fn an_append_deadline_settles_pending_independent_room_before_the_stream_failure() {
    let (mut rig, stream) = adopt();
    let first = send(&mut rig, stream, b"four").take(Kind::Append);
    send(&mut rig, stream, b"more").nothing();
    let right = Token::new(17);
    rig.down(Request::Output { stream, down: OutputDown::Room { right, bytes: 4 } }).nothing();
    rig.next().nothing();
    let mut expired = rig.at(Time::ZERO.saturating_add(Duration::from_millis(10)));
    assert_eq!(
        expired.events,
        [
            Event::Output {
                owner: stream,
                up: OutputUp::Settled { right, outcome: OutputOutcome::Failed(Fault::Other) }
            },
            Event::Stream { owner: stream, up: Up::Failed(Fault::Other) },
        ]
    );
    let cancel = expired.take(Kind::Cancel);
    rig.complete(first, Err(Error::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    finish(&mut rig, stream, close);
}
