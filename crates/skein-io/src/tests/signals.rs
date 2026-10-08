//! Signal intake through io's public entry points (io.md, section 7).

use super::{Kind, Rig, limits};
use crate::kernel::{Done, Error, Fd, Op, ServiceSignal};
use crate::{Event, Limits, Request};

#[test]
fn signal_source_rearms_and_settles_its_read_before_close() {
    let mut rig = Rig::new(Limits { sockets: 1, ..limits() });
    let source = rig.io.adopt_signals(Fd::new(50)).expect("one entity slot");
    assert_eq!(rig.io.adopt_signals(Fd::new(51)), Err(Fd::new(51)));
    let read = rig.next().take(Kind::ReadSignal);
    assert_eq!(read.kind, Op::ReadSignal { fd: Fd::new(50) });

    let mut out = rig.complete(read, Ok(Done::ServiceSignal(ServiceSignal::Interrupt)));
    assert_eq!(out.events, [Event::Shutdown { signal: ServiceSignal::Interrupt }]);
    let read = out.take(Kind::ReadSignal);
    let mut out = rig.complete(read, Ok(Done::ServiceSignal(ServiceSignal::Terminate)));
    assert_eq!(out.events, [Event::Shutdown { signal: ServiceSignal::Terminate }]);
    let read = out.take(Kind::ReadSignal);

    let cancel = rig.down(Request::Close { entity: source }).take(Kind::Cancel);
    rig.complete(cancel, Ok(Done::Nothing)).nothing();
    let close = rig.complete(read, Err(Error::Cancelled)).take(Kind::Close);
    assert_eq!(close.kind, Op::Close { fd: Fd::new(50) });
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: source }]);
    rig.empty();
}

#[test]
fn signal_failure_and_close_echo_the_service_binding_instead_of_its_io_handle() {
    let mut rig = Rig::new(Limits { sockets: 1, ..limits() });
    let owner = super::owner(9);
    let source = rig.io.adopt_signals_for(Fd::new(50), owner).expect("one entity slot");
    assert_ne!(source, owner, "the io handle and the service binding are independent");
    assert_eq!(rig.io.adopt_signals_for(Fd::new(51), owner), Err(Fd::new(51)));
    let read = rig.next().take(Kind::ReadSignal);
    let mut out = rig.complete(read, Err(Error::Permission));
    assert_eq!(out.events, [Event::Failed { owner, error: crate::Error::Other }]);
    let close = out.take(Kind::Close);
    assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner }]);
    rig.empty();
}
