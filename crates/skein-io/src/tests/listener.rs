//! The listener's cells (io.md, 3.2): made, bound and listening; accepting
//! one socket at a time, each announced and answered; its accept errors; the
//! accept batch; and closing, with its accept in flight and the races of its
//! cancel.

use alloc::boxed::Box;

use skein_lib::Token;

use super::{Kind, Rig, limits, listening, local, owner};
use crate::kernel::{Done, Error as Kernel, Fd, Op};
use crate::{Error, Event, Limits, Request};

const LISTENER: Fd = Fd::new(3);
const ACCEPTED: Fd = Fd::new(4);

#[test]
fn a_listener_is_made_bound_and_listening_then_told_its_address() {
    let mut rig = Rig::new(limits());
    let mut out = rig.down(Request::Listen { owner: owner(1), addr: local(0) });
    let socket = out.take(Kind::Socket);
    out.nothing();
    let mut out = rig.complete(socket, Ok(Done::Fd(LISTENER)));
    let bind = out.take(Kind::Bind);
    assert_eq!(bind.kind, Op::Bind { fd: LISTENER, addr: local(0) }, "it binds the address asked for");
    out.nothing();
    let mut out = rig.complete(bind, Ok(Done::Bound(local(40001))));
    let listen = out.take(Kind::Listen);
    assert_eq!(listen.kind, Op::Listen { fd: LISTENER, backlog: limits().backlog });
    let mut out = rig.complete(listen, Ok(Done::Nothing));
    let accept = out.take(Kind::Accept);
    assert_eq!(accept.kind, Op::Accept { fd: LISTENER }, "the first accept is armed at once");
    match out.events.as_slice() {
        [Event::Listening { owner: told, addr, .. }] => {
            assert_eq!((*told, *addr), (owner(1), local(40001)), "port 0 is told resolved");
        }
        other => panic!("Listening: {other:?}"),
    }
}

#[test]
fn a_listener_that_cannot_be_made_is_told_why_and_closed_without_a_close() {
    for (error, told) in [(Kernel::TooManyOpenFiles, Error::Busy), (Kernel::NoBufferSpace, Error::Busy)] {
        let mut rig = Rig::new(limits());
        let socket = rig.down(Request::Listen { owner: owner(1), addr: local(0) }).take(Kind::Socket);
        let out = rig.complete(socket, Err(error));
        assert_eq!(out.events, [Event::Failed { owner: owner(1), error: told }, Event::Closed { owner: owner(1) }]);
        assert!(out.subs.is_empty(), "no descriptor was made, so none is closed");
        rig.empty();
    }
}

#[test]
fn a_listener_that_cannot_bind_or_listen_is_told_then_closed() {
    for at in [Kind::Bind, Kind::Listen] {
        let mut rig = Rig::new(limits());
        let socket = rig.down(Request::Listen { owner: owner(1), addr: local(0) }).take(Kind::Socket);
        let mut pending = rig.complete(socket, Ok(Done::Fd(LISTENER))).take(Kind::Bind);
        if at == Kind::Listen {
            pending = rig.complete(pending, Ok(Done::Bound(local(40001)))).take(Kind::Listen);
        }
        let mut out = rig.complete(pending, Err(Kernel::AddressInUse));
        assert_eq!(out.events, [Event::Failed { owner: owner(1), error: Error::Other }], "an address in use is Other");
        let close = out.take(Kind::Close);
        assert_eq!(close.kind, Op::Close { fd: LISTENER });
        assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
        rig.empty();
    }
}

/// An accepted socket, announced: its token.
fn announced(rig: &mut Rig, accept: crate::kernel::Submit, fd: Fd) -> Token {
    let out = rig.complete(accept, Ok(Done::Accepted { fd, peer: local(50000) }));
    assert!(out.subs.is_empty(), "no accept is armed until the owner answers");
    match out.events.as_slice() {
        [Event::Accepted { owner: told, socket, peer }] => {
            assert_eq!((*told, *peer), (owner(1), local(50000)), "announced to the listener's owner");
            *socket
        }
        other => panic!("Accepted: {other:?}"),
    }
}

#[test]
fn an_accepted_socket_is_announced_and_the_next_accept_waits_for_the_answer() {
    let mut rig = Rig::new(Limits { sockets: 3, ..limits() });
    let (_listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let socket = announced(&mut rig, accept, ACCEPTED);
    rig.next().nothing();
    let mut out = rig.down(Request::Bind { socket, owner: owner(2) });
    let recv = out.take(Kind::Recv);
    assert_eq!(recv.kind, Op::Recv { fd: ACCEPTED, buf: Box::from([0; 4]) }, "the bound socket receives at once");
    assert_eq!(out.take(Kind::Accept).kind, Op::Accept { fd: LISTENER }, "the answer lets the listener accept again");
}

#[test]
fn a_rejected_socket_is_closed_and_tells_no_one() {
    let mut rig = Rig::new(Limits { sockets: 3, ..limits() });
    let (_listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let socket = announced(&mut rig, accept, ACCEPTED);
    rig.next().nothing();
    let mut out = rig.down(Request::Reject { socket });
    let close = out.take(Kind::Close);
    assert_eq!(close.kind, Op::Close { fd: ACCEPTED });
    let _accept = out.take(Kind::Accept);
    rig.complete(close, Ok(Done::Nothing)).nothing();
    rig.next().nothing();
    assert_eq!(rig.io.sockets(), 1, "the rejected socket is reclaimed; the listener stays");
}

#[test]
fn an_owner_that_holds_its_answer_holds_the_accepts_back() {
    let mut rig = Rig::new(Limits { sockets: 4, ..limits() });
    let (_listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let _socket = announced(&mut rig, accept, ACCEPTED);
    for _ in 0..3_u32 {
        rig.next().nothing();
    }
}

#[test]
fn accepts_out_of_descriptors_wait_for_one_to_be_given_back() {
    let mut rig = Rig::new(Limits { sockets: 3, ..limits() });
    let (_listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    rig.complete(accept, Err(Kernel::TooManyOpenFiles)).nothing();
    rig.next().nothing();
    rig.next().nothing();
    // A connection closes: a descriptor is given back, and a slot.
    let (socket, recv) = super::connected(&mut rig, owner(2), Fd::new(9));
    let cancel = rig.down(Request::Abort { entity: socket }).take(Kind::Cancel);
    rig.complete(recv, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(2) }]);
    let mut out = rig.next();
    assert_eq!(out.take(Kind::Accept).kind, Op::Accept { fd: LISTENER }, "the starved listener accepts again");
}

#[test]
fn accepts_that_fail_for_a_connection_or_a_buffer_are_retried_next_iteration() {
    for error in [Kernel::NoBufferSpace, Kernel::Reset, Kernel::TimedOut, Kernel::Unreachable, Kernel::Other(71)] {
        let mut rig = Rig::new(limits());
        let (_listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
        rig.complete(accept, Err(error)).nothing();
        let mut out = rig.next();
        assert_eq!(out.take(Kind::Accept).kind, Op::Accept { fd: LISTENER }, "{error:?} is retried");
    }
}

#[test]
fn an_accept_error_io_cannot_retry_stops_the_listener() {
    let mut rig = Rig::new(limits());
    let (listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let out = rig.complete(accept, Err(Kernel::InvalidArgument));
    assert_eq!(out.events, [Event::Failed { owner: owner(1), error: Error::Other }]);
    assert!(out.subs.is_empty(), "no accept again");
    rig.next().nothing();
    let close = rig.down(Request::Close { entity: listener }).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn the_accept_batch_is_shared_by_every_listener_in_an_iteration() {
    let mut rig = Rig::new(Limits { sockets: 4, accepts: 1, ..limits() });
    let (_first, _addr, _accept) = listening(&mut rig, owner(1), LISTENER);
    let socket = rig.down(Request::Listen { owner: owner(2), addr: local(0) }).take(Kind::Socket);
    let bind = rig.complete(socket, Ok(Done::Fd(Fd::new(5)))).take(Kind::Bind);
    let listen = rig.complete(bind, Ok(Done::Bound(local(40002)))).take(Kind::Listen);
    let out = rig.complete(listen, Ok(Done::Nothing));
    assert!(out.subs.is_empty(), "the batch is spent this iteration: {:?}", out.kinds());
    let mut out = rig.next();
    assert_eq!(out.take(Kind::Accept).kind, Op::Accept { fd: Fd::new(5) }, "armed in the next iteration");
}

#[test]
fn a_full_slab_starves_the_listener_until_a_socket_is_reclaimed() {
    let mut rig = Rig::new(Limits { sockets: 2, ..limits() });
    let (_listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let socket = announced(&mut rig, accept, ACCEPTED);
    let mut out = rig.down(Request::Bind { socket, owner: owner(2) });
    let recv = out.take(Kind::Recv);
    assert!(out.subs.is_empty(), "no slot for another socket: no accept");
    rig.next().nothing();
    let cancel = rig.down(Request::Abort { entity: socket }).take(Kind::Cancel);
    rig.complete(recv, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(2) }]);
    let mut out = rig.next();
    let _accept = out.take(Kind::Accept);
}

#[test]
fn a_socket_accepted_with_no_slot_left_is_discarded() {
    let mut rig = Rig::new(Limits { sockets: 2, ..limits() });
    let (_listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    // A connect takes the last slot while the accept is in flight.
    let _socket = rig.down(Request::Connect { owner: owner(2), addr: local(80) }).take(Kind::Socket);
    let mut out = rig.complete(accept, Ok(Done::Accepted { fd: ACCEPTED, peer: local(50000) }));
    assert!(out.events.is_empty(), "nothing announced");
    let discard = out.take(Kind::Close);
    assert_eq!(discard.kind, Op::Close { fd: ACCEPTED }, "the socket refused at io's entrance");
    rig.complete(discard, Ok(Done::Nothing)).nothing();
}

#[test]
fn a_listener_closing_cancels_its_accept_then_closes() {
    let mut rig = Rig::new(limits());
    let (listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let cancel = rig.down(Request::Close { entity: listener }).take(Kind::Cancel);
    assert_eq!(cancel.kind, Op::Cancel { target: accept.op });
    rig.complete(accept, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_socket_accepted_as_the_listener_closes_is_discarded_unannounced() {
    let mut rig = Rig::new(limits());
    let (listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let cancel = rig.down(Request::Abort { entity: listener }).take(Kind::Cancel);
    // The cancel came too late: the accept completed with a socket.
    rig.complete(cancel, Err(Kernel::TooLate)).nothing();
    let mut out = rig.complete(accept, Ok(Done::Accepted { fd: ACCEPTED, peer: local(50000) }));
    assert!(out.events.is_empty(), "the owner asked for no more sockets");
    let discard = out.take(Kind::Close);
    assert_eq!(discard.kind, Op::Close { fd: ACCEPTED });
    let close = rig.complete(discard, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(close.kind, Op::Close { fd: LISTENER }, "the listener closes once nothing of it is in flight");
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_cancel_the_backend_could_not_submit_is_tried_again_while_the_accept_waits() {
    let mut rig = Rig::new(limits());
    let (listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let cancel = rig.down(Request::Close { entity: listener }).take(Kind::Cancel);
    let again = rig.complete(cancel, Err(Kernel::Other(11))).take(Kind::Cancel);
    assert_eq!(again.kind, Op::Cancel { target: accept.op }, "the same target, asked again");
    rig.complete(accept, Err(Kernel::Cancelled)).nothing();
    let close = rig.complete(again, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_cancel_unsubmitted_after_its_accept_completed_is_not_tried_again() {
    let mut rig = Rig::new(limits());
    let (listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let cancel = rig.down(Request::Close { entity: listener }).take(Kind::Cancel);
    rig.complete(accept, Err(Kernel::NoBufferSpace)).nothing();
    let close = rig.complete(cancel, Err(Kernel::InvalidArgument)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.empty();
}

#[test]
fn a_listener_closes_once_whatever_waited_on_it_is_answered() {
    let mut rig = Rig::new(Limits { sockets: 3, ..limits() });
    let (listener, _addr, accept) = listening(&mut rig, owner(1), LISTENER);
    let socket = announced(&mut rig, accept, ACCEPTED);
    // Closed while its socket waits for an answer, which still comes.
    let close = rig.down(Request::Close { entity: listener }).take(Kind::Close);
    let mut out = rig.down(Request::Bind { socket, owner: owner(2) });
    let _recv = out.take(Kind::Recv);
    assert!(out.subs.is_empty(), "a closing listener accepts no more");
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    // Closing twice, and closing a closed one, change nothing.
    rig.down(Request::Close { entity: listener }).nothing();
    rig.next().nothing();
    rig.down(Request::Abort { entity: listener }).nothing();
}

#[test]
#[should_panic(expected = "a stream request names a stream")]
fn a_stream_request_naming_a_listener_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (listener, _addr, _accept) = listening(&mut rig, owner(1), LISTENER);
    let _out = rig.down(Request::Stream { stream: listener, down: skein_lib::stream::Down::Finish });
}

#[test]
#[should_panic(expected = "an answer names a socket")]
fn an_answer_naming_a_listener_is_the_owner_s_bug() {
    let mut rig = Rig::new(limits());
    let (listener, _addr, _accept) = listening(&mut rig, owner(1), LISTENER);
    let _out = rig.down(Request::Reject { socket: listener });
}
