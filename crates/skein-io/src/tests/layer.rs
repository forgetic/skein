//! What io does beside its two machines (io.md, 2 and 3.1): refusals at a
//! full slab, held until the next up pass; stale tokens, dropped going down
//! and asserted going up; and its limits and worst case.

use skein_lib::stream::{Down, Read};
use skein_lib::{Duration, Token};

use super::{Kind, Rig, connected, limits, listening, local, owner};
use crate::kernel::{Complete, Done, Fd, Op};
use crate::{Error, Event, Io, Limits, Request, operations, worst_case};

#[test]
fn a_connect_past_the_socket_slab_is_refused_and_told_in_the_next_up_pass() {
    let mut rig = Rig::new(Limits { sockets: 1, refusals: 2, ..limits() });
    let _socket = rig.down(Request::Connect { owner: owner(1), addr: local(80) }).take(Kind::Socket);
    rig.down(Request::Connect { owner: owner(2), addr: local(80) }).nothing();
    rig.down(Request::Listen { owner: owner(3), addr: local(0) }).nothing();
    assert!(!rig.io.takes(), "both refusals held: io takes no more requests");
    let mut told = rig.next();
    match told.events.pop() {
        Some(Event::Connecting { owner: connecting, .. }) => assert_eq!(connecting, owner(1)),
        other => panic!("the connect io took is told Connecting last: {other:?}"),
    }
    assert_eq!(
        told.events,
        [
            Event::Failed { owner: owner(2), error: Error::Busy },
            Event::Closed { owner: owner(2) },
            Event::Failed { owner: owner(3), error: Error::Busy },
            Event::Closed { owner: owner(3) },
        ],
        "the refusals first, in order"
    );
    assert!(rig.io.takes(), "told, the refusals are let go");
}

#[test]
fn a_request_naming_a_socket_that_is_gone_is_dropped() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), Fd::new(3));
    let cancel = rig.down(Request::Abort { entity: socket }).take(Kind::Cancel);
    rig.complete(recv, Err(crate::kernel::Error::Cancelled)).nothing();
    let close = rig.complete(cancel, Ok(Done::Nothing)).take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    // Retired, not yet reclaimed: still found, and closed.
    rig.down(Request::Stream { stream: socket, down: Down::Finish }).nothing();
    rig.down(Request::Abort { entity: socket }).nothing();
    rig.next().nothing();
    // Reclaimed, and the slot taken by another socket: the old token is stale.
    let (_other, _recv) = connected(&mut rig, owner(2), Fd::new(4));
    rig.down(Request::Stream { stream: socket, down: Down::Demand { read: Read::Fill(1), room: 1 } }).nothing();
    rig.down(Request::Close { entity: socket }).nothing();
    rig.down(Request::Bind { socket, owner: owner(3) }).nothing();
    rig.next().nothing();
}

#[test]
#[should_panic(expected = "a completion names an operation in flight")]
fn a_completion_naming_no_operation_in_flight_is_a_bug() {
    let mut rig = Rig::new(limits());
    let mut up = skein_lib::Queue::with_capacity(4);
    let mut subs = skein_lib::Queue::with_capacity(4);
    let complete = Complete { op: Token::new(7), kind: Op::Close { fd: Fd::new(3) }, result: Ok(Done::Nothing) };
    crate::up(&mut rig.io, &rig.env, complete, &mut up, &mut subs);
}

#[test]
#[should_panic(expected = "a completion names an operation in flight")]
fn a_completion_of_an_operation_completed_and_reclaimed_is_a_bug() {
    let mut rig = Rig::new(limits());
    let (_listener, _addr, accept) = listening(&mut rig, owner(1), Fd::new(3));
    let stale = crate::kernel::Submit { op: accept.op, kind: Op::Accept { fd: Fd::new(3) } };
    rig.complete(accept, Err(crate::kernel::Error::NoBufferSpace)).nothing();
    let _retried = rig.next();
    let _out = rig.complete(stale, Ok(Done::Nothing));
}

#[test]
fn the_ring_is_four_operations_per_socket_and_the_worst_case_grows_with_every_limit() {
    let base = limits();
    assert_eq!(operations(&base), Some(8));
    let at = worst_case(&base).expect("tiny limits fit a u64");
    for bigger in [
        Limits { sockets: 3, ..base },
        Limits { refusals: 3, ..base },
        Limits { intake: 9, ..base },
        Limits { receive: 5, ..base },
        Limits { output: 9, ..base },
        Limits { sends: 3, ..base },
    ] {
        assert!(worst_case(&bigger).expect("fits") > at, "{bigger:?} costs more than {base:?}");
    }
    assert_eq!(worst_case(&Limits { close_timeout: Duration::from_secs(9), accepts: 9, backlog: 9, ..base }), Some(at));
    assert_eq!(worst_case(&Limits { sockets: u32::MAX, ..base }), None, "past a u64, or the ring's u32");
    assert_eq!(operations(&Limits { sockets: u32::MAX, ..base }), None);
}

#[test]
fn the_startup_check_is_the_intake_and_output_caps() {
    let limits = Limits { intake: 300, output: 200, ..limits() };
    assert_eq!((limits.largest_read(), limits.largest_room()), (300, 200));
}

#[test]
#[should_panic(expected = "usable limits")]
fn io_does_not_run_under_limits_it_cannot_use() {
    let _io = Io::new(&Limits { refusals: 0, ..limits() });
}

#[test]
fn every_limit_of_zero_but_the_sockets_is_unusable() {
    let base = limits();
    for unusable in [
        Limits { refusals: 0, ..base },
        Limits { intake: 0, ..base },
        Limits { receive: 0, ..base },
        Limits { output: 0, ..base },
        Limits { sends: 0, ..base },
        Limits { accepts: 0, ..base },
    ] {
        assert!(!unusable.is_usable(), "{unusable:?}");
    }
    assert!(Limits { sockets: 0, backlog: 0, close_timeout: Duration::ZERO, ..base }.is_usable());
}
