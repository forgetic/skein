//! The stream's cells (io.md, 3.3): connecting; the intake meeting demands
//! of every kind; room; short sends continued from their offset; finish; a
//! failure told once; a graceful close and its deadline; aborts, and every
//! outcome a cancel may have (kernel.md, 5).

use alloc::boxed::Box;

use skein_lib::stream::{Delimiter, Down, Fault, Read, Up};
use skein_lib::{Duration, Time, Token};

use super::{Kind, Out, Rig, buffer, connected, filled, kind, limits, local, owner};
use crate::kernel::{Done, Error as Kernel, Fd, Op, Submit};
use crate::{Error, Event, Request};

const FD: Fd = Fd::new(3);

fn ask(rig: &mut Rig, socket: Token, read: Read, room: u32) -> Out {
    rig.down(Request::Stream { stream: socket, down: Down::Demand { read, room } })
}

/// Sends `bytes` as an owner must: within room asked for and granted
/// (lib.md, 7), unless there are none.
fn give(rig: &mut Rig, socket: Token, bytes: &[u8]) -> Out {
    if !bytes.is_empty() {
        let room = u32::try_from(bytes.len()).unwrap();
        ask(rig, socket, Read::Nothing, room).nothing();
        assert_eq!(rig.next().events, [up(Up::Room)], "room for {room} bytes, granted");
    }
    push(rig, socket, bytes)
}

/// A `Send` as it stands, granted room or not.
fn push(rig: &mut Rig, socket: Token, bytes: &[u8]) -> Out {
    rig.down(Request::Stream { stream: socket, down: Down::Send(Box::from(bytes)) })
}

/// A send's completion, `n` bytes sent.
fn sent(rig: &mut Rig, send: Submit, n: u32) -> Out {
    rig.complete(send, Ok(Done::Count(n)))
}

fn up(event: Up) -> Event {
    Event::Stream { owner: owner(1), up: event }
}

fn bytes(of: &[u8]) -> Event {
    up(Up::Bytes(Box::from(of)))
}

/// The length a receive asks for.
fn asks(recv: &Submit) -> usize {
    assert_eq!(kind(&recv.kind), Kind::Recv);
    buffer(&recv.kind).0.len()
}

/// What a send sends: its bytes and its offset.
fn sends(send: &Submit) -> (&[u8], u32) {
    assert_eq!(kind(&send.kind), Kind::Send);
    buffer(&send.kind)
}

#[test]
fn a_connect_is_told_connecting_then_connected_and_receives_at_once() {
    let mut rig = Rig::new(limits());
    let (_socket, recv) = connected(&mut rig, owner(1), FD);
    assert_eq!(asks(&recv), 4, "the receive limit, less than the intake's room");
}

#[test]
fn a_connect_that_fails_is_told_why_then_closed() {
    for (error, told) in [
        (Kernel::Refused, Error::Refused),
        (Kernel::Unreachable, Error::Unreachable),
        (Kernel::TimedOut, Error::TimedOut),
        (Kernel::Reset, Error::Reset),
        (Kernel::NoBufferSpace, Error::Busy),
        (Kernel::AddressNotAvailable, Error::Busy),
        (Kernel::InvalidArgument, Error::Other),
    ] {
        let mut rig = Rig::new(limits());
        let socket = rig.down(Request::Connect { owner: owner(1), addr: local(80) }).take(Kind::Socket);
        let _connecting = rig.next();
        let connect = rig.complete(socket, Ok(Done::Fd(FD))).take(Kind::Connect);
        let mut out = rig.complete(connect, Err(error));
        assert_eq!(out.events, [Event::Failed { owner: owner(1), error: told }], "{error:?}");
        let close = out.take(Kind::Close);
        assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
        rig.empty();
    }
}

#[test]
fn a_connect_whose_socket_cannot_be_made_is_told_busy_and_closed_at_once() {
    let mut rig = Rig::new(limits());
    let socket = rig.down(Request::Connect { owner: owner(1), addr: local(80) }).take(Kind::Socket);
    let _connecting = rig.next();
    let out = rig.complete(socket, Err(Kernel::TooManyOpenFiles));
    assert_eq!(out.events, [Event::Failed { owner: owner(1), error: Error::Busy }, Event::Closed { owner: owner(1) }]);
    assert!(out.subs.is_empty(), "nothing was made to close");
    rig.empty();
}

#[test]
fn bytes_wait_in_the_intake_until_a_fill_demands_them() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let mut out = filled(&mut rig, recv, b"abc");
    assert!(out.events.is_empty(), "nothing demanded, nothing told");
    let recv = out.take(Kind::Recv);
    assert_eq!(asks(&recv), 4, "received again, up to the room left");
    ask(&mut rig, socket, Read::Fill(2), 0).nothing();
    assert_eq!(rig.next().events, [bytes(b"ab")], "met in the next up pass, from the intake");
    ask(&mut rig, socket, Read::Fill(2), 0).nothing();
    rig.next().nothing();
    let out = filled(&mut rig, recv, b"de");
    assert_eq!(out.events, [bytes(b"cd")], "met as soon as it can be");
}

#[test]
fn a_scan_delivers_through_its_delimiter_or_exactly_its_maximum() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let _recv = filled(&mut rig, recv, b"a\ncd").take(Kind::Recv);
    ask(&mut rig, socket, Read::Scan { until: Delimiter::LF, max: 8 }, 0).nothing();
    assert_eq!(rig.next().events, [bytes(b"a\n")]);
    ask(&mut rig, socket, Read::Scan { until: Delimiter::LF, max: 2 }, 0).nothing();
    assert_eq!(rig.next().events, [bytes(b"cd")], "no delimiter within the maximum: exactly the maximum");
}

#[test]
fn a_full_intake_stops_receiving_until_a_demand_takes_from_it() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let recv = filled(&mut rig, recv, b"abcd").take(Kind::Recv);
    let out = filled(&mut rig, recv, b"efgh");
    assert!(out.subs.is_empty(), "the intake is full: no receive in flight");
    ask(&mut rig, socket, Read::Fill(3), 0).nothing();
    let mut out = rig.next();
    assert_eq!(out.events, [bytes(b"abc")]);
    assert_eq!(asks(&out.take(Kind::Recv)), 3, "received again, as far as the room made");
}

#[test]
fn the_end_is_told_once_behind_what_the_intake_holds() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let recv = filled(&mut rig, recv, b"ab").take(Kind::Recv);
    let out = filled(&mut rig, recv, b"");
    assert!(out.events.is_empty() && out.subs.is_empty(), "no demand, and bytes held: no end yet");
    ask(&mut rig, socket, Read::Fill(1), 0).nothing();
    assert_eq!(rig.next().events, [bytes(b"a")], "a byte is still held: no end yet");
    ask(&mut rig, socket, Read::Fill(2), 0).nothing();
    assert_eq!(rig.next().events, [up(Up::End)], "a demand that cannot be met any more");
    assert!(rig.next().events.is_empty(), "it stays outstanding, never met, and End is not told again");
}

#[test]
fn a_read_that_crosses_the_end_is_never_met() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let recv = filled(&mut rig, recv, b"ab").take(Kind::Recv);
    filled(&mut rig, recv, b"").nothing();
    ask(&mut rig, socket, Read::Fill(3), 0).nothing();
    assert_eq!(rig.next().events, [up(Up::End)]);
    // Withdrawn, as a closing reader does, then nothing more.
    ask(&mut rig, socket, Read::Nothing, 0).nothing();
    rig.next().nothing();
}

#[test]
fn room_is_still_granted_after_the_end() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    assert_eq!(filled(&mut rig, recv, b"").events, [up(Up::End)]);
    ask(&mut rig, socket, Read::Fill(1), 4).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)], "the peer half-closed: the stream may still send");
    let send = give(&mut rig, socket, b"late").take(Kind::Send);
    sent(&mut rig, send, 4).nothing();
}

#[test]
fn a_demand_is_answered_once_by_its_bytes_or_else_its_room() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let recv = filled(&mut rig, recv, b"ab").take(Kind::Recv);
    // Both could be given: the bytes answer it, and the room is not granted.
    ask(&mut rig, socket, Read::Fill(2), 4).nothing();
    assert_eq!(rig.next().events, [bytes(b"ab")]);
    let out = filled(&mut rig, recv, b"cd");
    assert!(out.events.is_empty(), "answered: nothing is outstanding until the next demand");
    // Only the room can be given: it answers, and the read is dropped.
    ask(&mut rig, socket, Read::Fill(4), 4).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)]);
    rig.next().nothing();
    // A demand of nothing withdraws the one outstanding.
    ask(&mut rig, socket, Read::Fill(3), 0).nothing();
    ask(&mut rig, socket, Read::Nothing, 0).nothing();
    rig.next().nothing();
}

#[test]
fn the_end_with_nothing_held_is_told_at_once_demanded_or_not() {
    let mut rig = Rig::new(limits());
    let (_socket, recv) = connected(&mut rig, owner(1), FD);
    assert_eq!(filled(&mut rig, recv, b"").events, [up(Up::End)]);
}

#[test]
fn room_is_granted_once_the_output_has_it() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    ask(&mut rig, socket, Read::Nothing, 8).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)]);
    let send = give(&mut rig, socket, b"hello").take(Kind::Send);
    ask(&mut rig, socket, Read::Nothing, 4).nothing();
    rig.next().nothing();
    assert_eq!(sent(&mut rig, send, 5).events, [up(Up::Room)], "granted as the send frees the output");
    ask(&mut rig, socket, Read::Nothing, 0).nothing();
    rig.next().nothing();
}

#[test]
fn a_short_send_goes_on_from_its_offset_in_the_same_box() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let send = give(&mut rig, socket, b"hello").take(Kind::Send);
    let address = sends(&send).0.as_ptr();
    let mut out = sent(&mut rig, send, 2);
    let rest = out.take(Kind::Send);
    assert_eq!(sends(&rest), (&b"hello"[..], 2), "the rest, from the offset");
    assert_eq!(sends(&rest).0.as_ptr(), address, "in the same box: the remainder is never copied");
    sent(&mut rig, rest, 3).nothing();
}

#[test]
fn sends_queue_behind_the_one_in_flight_and_go_in_order() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let first = give(&mut rig, socket, b"ab").take(Kind::Send);
    give(&mut rig, socket, b"cd").nothing();
    give(&mut rig, socket, b"").nothing();
    let second = sent(&mut rig, first, 2).take(Kind::Send);
    assert_eq!(sends(&second), (&b"cd"[..], 0));
    sent(&mut rig, second, 2).nothing();
}

#[test]
fn finish_half_closes_once_the_output_is_flushed() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let send = give(&mut rig, socket, b"ab").take(Kind::Send);
    rig.down(Request::Stream { stream: socket, down: Down::Finish }).nothing();
    rig.down(Request::Stream { stream: socket, down: Down::Finish }).nothing();
    let shutdown = sent(&mut rig, send, 2).take(Kind::Shutdown);
    rig.complete(shutdown, Ok(Done::Nothing)).nothing();

    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let shutdown = rig.down(Request::Stream { stream: socket, down: Down::Finish }).take(Kind::Shutdown);
    assert_eq!(shutdown.kind, Op::Shutdown { fd: FD }, "an idle writer half-closes at once");
}

#[test]
#[should_panic(expected = "no Send after Finish")]
fn a_send_after_finish_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let _shutdown = rig.down(Request::Stream { stream: socket, down: Down::Finish });
    let _send = push(&mut rig, socket, b"late");
}

#[test]
#[should_panic(expected = "no room is demanded after Finish")]
fn room_after_finish_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let _shutdown = rig.down(Request::Stream { stream: socket, down: Down::Finish });
    let _ask = ask(&mut rig, socket, Read::Nothing, 1);
}

#[test]
#[should_panic(expected = "a Send within the room granted: no more than the last Room")]
fn a_send_with_no_room_granted_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let _send = push(&mut rig, socket, b"unasked");
}

#[test]
#[should_panic(expected = "a Send within the room granted: no more than the last Room")]
fn a_send_past_the_room_granted_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    ask(&mut rig, socket, Read::Nothing, 4).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)]);
    let _within = push(&mut rig, socket, b"123");
    let _past = push(&mut rig, socket, b"45");
}

#[test]
fn room_granted_is_spent_by_sends_and_a_demand_for_none_leaves_it() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    ask(&mut rig, socket, Read::Nothing, 5).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)]);
    let _first = push(&mut rig, socket, b"ab").take(Kind::Send);
    // A read alone leaves the grant: three bytes of it are still held.
    ask(&mut rig, socket, Read::Fill(1), 0).nothing();
    push(&mut rig, socket, b"cde").nothing();
}

#[test]
#[should_panic(expected = "a Send within the room granted: no more than the last Room")]
fn a_new_grant_replaces_what_was_left_of_the_last() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    ask(&mut rig, socket, Read::Nothing, 3).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)]);
    ask(&mut rig, socket, Read::Nothing, 1).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)]);
    let _past = push(&mut rig, socket, b"ab");
}

#[test]
#[should_panic(expected = "no more than Limits::sends")]
fn a_send_past_the_queue_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    // Room for four bytes, spent in sends of one: more sends than the queue
    // holds, though within the room.
    ask(&mut rig, socket, Read::Nothing, 4).nothing();
    assert_eq!(rig.next().events, [up(Up::Room)]);
    for _ in 0..4_u32 {
        let _send = push(&mut rig, socket, b"a");
    }
}

#[test]
#[should_panic(expected = "a fill past the intake's cap")]
fn a_fill_past_the_intake_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let _ask = ask(&mut rig, socket, Read::Fill(9), 0);
}

#[test]
#[should_panic(expected = "room past the output cap")]
fn room_past_the_output_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let _ask = ask(&mut rig, socket, Read::Nothing, 9);
}

#[test]
#[should_panic(expected = "a stream request comes once the stream is connected")]
fn a_stream_request_before_connected_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let _socket = rig.down(Request::Connect { owner: owner(1), addr: local(80) });
    let socket = match rig.next().events.as_slice() {
        [Event::Connecting { socket, .. }] => *socket,
        other => panic!("Connecting: {other:?}"),
    };
    let _ask = ask(&mut rig, socket, Read::Fill(1), 0);
}

/// When a retry falls due, `ms` milliseconds from the start.
fn retry_at(ms: u64) -> Time {
    Time::ZERO.saturating_add(Duration::from_millis(ms)).saturating_add(limits().retry)
}

#[test]
fn no_buffer_is_retried_at_the_retry_deadline_not_failed() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let out = rig.complete(recv, Err(Kernel::NoBufferSpace));
    assert!(out.events.is_empty() && out.subs.is_empty(), "not a failure, and not again at once");
    let mut out = rig.at(retry_at(0));
    assert_eq!(asks(&out.take(Kind::Recv)), 4, "received again at the deadline");
    let send = give(&mut rig, socket, b"abc").take(Kind::Send);
    let send = sent(&mut rig, send, 1).take(Kind::Send);
    rig.at(Time::from_nanos(20_000_000));
    assert!(rig.complete(send, Err(Kernel::NoBufferSpace)).subs.is_empty(), "the send waits for its deadline");
    rig.down(Request::Stream { stream: socket, down: Down::Finish }).nothing();
    let mut out = rig.at(retry_at(20));
    let again = out.take(Kind::Send);
    assert_eq!(sends(&again), (&b"abc"[..], 1), "the same box, from the same offset");
    let shutdown = sent(&mut rig, again, 2).take(Kind::Shutdown);
    rig.at(Time::from_nanos(40_000_000));
    assert!(rig.complete(shutdown, Err(Kernel::NoBufferSpace)).subs.is_empty(), "the half-close waits too");
    let _shutdown = rig.at(retry_at(40)).take(Kind::Shutdown);
}

#[test]
fn a_graceful_close_retries_what_found_no_buffer_and_its_abort_drops_it() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let send = give(&mut rig, socket, b"abc").take(Kind::Send);
    rig.complete(recv, Err(Kernel::NoBufferSpace)).nothing();
    rig.complete(send, Err(Kernel::NoBufferSpace)).nothing();
    // Closing with both sides stalled: they wait for their retry, under the
    // close deadline.
    rig.down(Request::Close { entity: socket }).nothing();
    let mut out = rig.at(retry_at(0));
    let recv = out.take(Kind::Recv);
    let send = out.take(Kind::Send);
    assert_eq!(sends(&send), (&b"abc"[..], 0));
    // A stall again, then the abort: the stalled send is dropped, the
    // receive in flight cancelled.
    rig.complete(send, Err(Kernel::NoBufferSpace)).nothing();
    let cancel = rig.down(Request::Abort { entity: socket }).take(Kind::Cancel);
    assert_eq!(rig.io.next_deadline(), None, "no deadline left once settling");
    rig.complete(recv, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_failure_is_told_once_and_the_stream_waits_for_its_owner_s_close() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let send = give(&mut rig, socket, b"abc").take(Kind::Send);
    let out = rig.complete(recv, Err(Kernel::Reset));
    assert_eq!(out.events, [up(Up::Failed(Fault::Reset))]);
    assert!(out.subs.is_empty(), "nothing is received or sent any more");
    rig.complete(send, Err(Kernel::BrokenPipe)).nothing();
    push(&mut rig, socket, b"dropped").nothing();
    ask(&mut rig, socket, Read::Fill(1), 1).nothing();
    rig.next().nothing();
    let close = rig.down(Request::Close { entity: socket }).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_broken_stream_closing_cancels_the_receive_still_in_flight() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let send = give(&mut rig, socket, b"abc").take(Kind::Send);
    assert_eq!(rig.complete(send, Err(Kernel::Reset)).events, [up(Up::Failed(Fault::Reset))]);
    let cancel = rig.down(Request::Close { entity: socket }).take(Kind::Cancel);
    filled(&mut rig, recv, b"").nothing();
    let close = rig.complete(cancel, Err(Kernel::TooLate)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_fault_says_whether_the_peer_is_gone() {
    for (error, fault) in [
        (Kernel::Reset, Fault::Reset),
        (Kernel::TimedOut, Fault::Reset),
        (Kernel::Unreachable, Fault::Reset),
        (Kernel::NotConnected, Fault::Reset),
        (Kernel::InvalidArgument, Fault::Other),
        (Kernel::Other(5), Fault::Other),
    ] {
        let mut rig = Rig::new(limits());
        let (_socket, recv) = connected(&mut rig, owner(1), FD);
        assert_eq!(rig.complete(recv, Err(error)).events, [up(Up::Failed(fault))], "{error:?}");
    }
}

#[test]
fn a_graceful_close_flushes_half_closes_and_discards_until_the_peer_ends() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let send = give(&mut rig, socket, b"bye").take(Kind::Send);
    ask(&mut rig, socket, Read::Fill(1), 0).nothing();
    rig.down(Request::Close { entity: socket }).nothing();
    assert_eq!(rig.io.next_deadline(), Some(Time::ZERO.saturating_add(limits().close_timeout)));
    rig.next().nothing();
    let mut out = filled(&mut rig, recv, b"more");
    assert!(out.events.is_empty(), "discarded, the demand dropped: the owner closed");
    let recv = out.take(Kind::Recv);
    assert_eq!(asks(&recv), 4, "received again, to discard");
    let shutdown = sent(&mut rig, send, 3).take(Kind::Shutdown);
    rig.complete(shutdown, Ok(Done::Nothing)).nothing();
    let close = filled(&mut rig, recv, b"").take(Kind::Close);
    assert_eq!(rig.io.next_deadline(), None, "the close deadline cancelled");
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_graceful_close_with_nothing_left_closes_at_once() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    assert_eq!(filled(&mut rig, recv, b"").events, [up(Up::End)]);
    let shutdown = rig.down(Request::Stream { stream: socket, down: Down::Finish }).take(Kind::Shutdown);
    rig.complete(shutdown, Ok(Done::Nothing)).nothing();
    let close = rig.down(Request::Close { entity: socket }).take(Kind::Close);
    assert_eq!(rig.io.next_deadline(), None, "nothing to wait for: no deadline");
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
}

#[test]
fn a_graceful_close_of_a_full_intake_starts_discarding() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let recv = filled(&mut rig, recv, b"abcd").take(Kind::Recv);
    assert!(filled(&mut rig, recv, b"efgh").subs.is_empty(), "the intake is full");
    let mut out = rig.down(Request::Close { entity: socket });
    assert_eq!(asks(&out.take(Kind::Recv)), 4, "a receive to discard what the peer still sends");
    let _shutdown = out.take(Kind::Shutdown);
}

#[test]
fn the_close_deadline_aborts_a_close_the_peer_never_ends() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let shutdown = rig.down(Request::Close { entity: socket }).take(Kind::Shutdown);
    rig.complete(shutdown, Ok(Done::Nothing)).nothing();
    rig.at(Time::from_nanos(999_999_999)).nothing();
    let cancel = rig.at(Time::ZERO.saturating_add(limits().close_timeout)).take(Kind::Cancel);
    assert_eq!(cancel.kind, Op::Cancel { target: recv.op });
    rig.complete(recv, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

/// A cancel's answer, given what its target completed with: it stopped the
/// target, or it was too late.
fn answer(target: Result<Done, Kernel>) -> Result<Done, Kernel> {
    if target == Err(Kernel::Cancelled) { Ok(Done::Nothing) } else { Err(Kernel::TooLate) }
}

/// What a target and its cancel complete with, the cancel's answer the one
/// the target's result implies.
fn race(target: Result<Done, Kernel>) -> Race {
    (target, answer(target))
}

/// A cancel too late, whose target the kernel interrupted anyway: the target
/// completes `Cancelled` all the same (kernel.md, 5).
const INTERRUPTED: Race = (Err(Kernel::Cancelled), Err(Kernel::TooLate));

/// What a target completes with, and what its cancel answers.
type Race = (Result<Done, Kernel>, Result<Done, Kernel>);

/// An abort with a receive and a send in flight, then their completions and
/// their cancels' in `order`: each outcome a cancel may have (kernel.md, 5).
fn abort_in_order(order: [usize; 4], (recv_result, cancel_recv_result): Race, (send_result, cancel_send_result): Race) {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let send = give(&mut rig, socket, b"abc").take(Kind::Send);
    let mut out = rig.down(Request::Abort { entity: socket });
    assert_eq!(out.kinds(), [Kind::Cancel, Kind::Cancel], "both waiting operations cancelled");
    let cancel_send = out.subs.pop().unwrap();
    let cancel_recv = out.subs.pop().unwrap();
    let mut pending = [
        Some((recv, recv_result)),
        Some((send, send_result)),
        Some((cancel_recv, cancel_recv_result)),
        Some((cancel_send, cancel_send_result)),
    ];
    let mut close = None;
    for at in order {
        let (submit, result) = pending[at].take().unwrap();
        let mut out = rig.complete(submit, result);
        assert!(out.events.is_empty(), "nothing told before the close");
        if !out.subs.is_empty() {
            close = Some(out.take(Kind::Close));
        }
    }
    let close = close.expect("closed once all four completed");
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn an_abort_cancels_what_waits_and_settles_every_outcome_in_every_order() {
    let outcomes = [
        (race(Err(Kernel::Cancelled)), race(Err(Kernel::Cancelled))),
        (race(Ok(Done::Count(2))), race(Ok(Done::Count(3)))),
        (race(Ok(Done::Count(0))), race(Err(Kernel::Cancelled))),
        (race(Err(Kernel::Reset)), race(Err(Kernel::BrokenPipe))),
        (INTERRUPTED, INTERRUPTED),
        (INTERRUPTED, race(Ok(Done::Count(1)))),
    ];
    let orders = [[0, 1, 2, 3], [2, 3, 0, 1], [0, 2, 1, 3], [3, 1, 2, 0]];
    for (recv, send) in outcomes {
        for order in orders {
            abort_in_order(order, recv, send);
        }
    }
}

#[test]
fn a_cancel_not_submitted_is_asked_again_while_its_target_waits() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let cancel = rig.down(Request::Abort { entity: socket }).take(Kind::Cancel);
    let again = rig.complete(cancel, Err(Kernel::Other(11))).take(Kind::Cancel);
    assert_eq!(again.kind, Op::Cancel { target: recv.op });
    let again = rig.complete(again, Err(Kernel::InvalidArgument)).take(Kind::Cancel);
    rig.complete(recv, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(again, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_close_while_connecting_cancels_the_connect_and_closes_what_it_made() {
    for connect_result in [Err(Kernel::Cancelled), Ok(Done::Nothing), Err(Kernel::Refused)] {
        let mut rig = Rig::new(limits());
        let socket_op = rig.down(Request::Connect { owner: owner(1), addr: local(80) }).take(Kind::Socket);
        let socket = match rig.next().events.as_slice() {
            [Event::Connecting { socket, .. }] => *socket,
            other => panic!("Connecting: {other:?}"),
        };
        let connect = rig.complete(socket_op, Ok(Done::Fd(FD))).take(Kind::Connect);
        let cancel = rig.down(Request::Close { entity: socket }).take(Kind::Cancel);
        assert_eq!(cancel.kind, Op::Cancel { target: connect.op });
        rig.complete(connect, connect_result).nothing();
        let close = rig.complete(cancel, answer(connect_result)).take(Kind::Close);
        assert_eq!(close.kind, Op::Close { fd: FD }, "a stopped connect may have reached its peer: closed");
        assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
        rig.empty();
    }
}

#[test]
fn a_close_while_the_socket_is_made_closes_it_once_it_is() {
    for made in [true, false] {
        let mut rig = Rig::new(limits());
        let socket_op = rig.down(Request::Connect { owner: owner(1), addr: local(80) }).take(Kind::Socket);
        let socket = match rig.next().events.as_slice() {
            [Event::Connecting { socket, .. }] => *socket,
            other => panic!("Connecting: {other:?}"),
        };
        rig.down(Request::Abort { entity: socket }).nothing();
        if made {
            let close = rig.complete(socket_op, Ok(Done::Fd(FD))).take(Kind::Close);
            assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
        } else {
            let out = rig.complete(socket_op, Err(Kernel::TooManyOpenFiles));
            assert_eq!(out.events, [Event::Closed { owner: owner(1) }], "nothing made, nothing to close: no Failed");
        }
        rig.empty();
    }
}

#[test]
fn requests_after_a_close_are_dropped_and_an_abort_escalates_it() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), FD);
    let shutdown = rig.down(Request::Close { entity: socket }).take(Kind::Shutdown);
    ask(&mut rig, socket, Read::Fill(1), 1).nothing();
    push(&mut rig, socket, b"late").nothing();
    rig.down(Request::Stream { stream: socket, down: Down::Finish }).nothing();
    rig.down(Request::Close { entity: socket }).nothing();
    let cancel = rig.down(Request::Abort { entity: socket }).take(Kind::Cancel);
    assert_eq!(rig.io.next_deadline(), None, "the abort cancels the close deadline");
    rig.complete(shutdown, Ok(Done::Nothing)).nothing();
    rig.complete(recv, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    rig.down(Request::Abort { entity: socket }).nothing();
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
#[should_panic(expected = "a demand is stated once the last is answered")]
fn a_demand_in_place_of_one_outstanding_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), FD);
    let _ask = ask(&mut rig, socket, Read::Fill(2), 0);
    let _again = ask(&mut rig, socket, Read::Fill(1), 0);
}
