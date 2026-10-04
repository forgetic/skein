//! The listener's cells (examples.md, 3.4): listening from the ready list, a
//! listen that fails, an accept that stops, the domain's `Stop` in every
//! state, a socket announced while it closes, and the shutdown told the
//! domain.

use skein_echo_domain::{Event as Call, Request as Domain};
use skein_io::{Error, Event as Told, Request as Io};
use skein_lib::Time;
use skein_lib::stream::Up;

use super::{LIMITS, LISTENER, Rig, addr, close, socket, stream};

#[test]
fn the_listener_listens_from_the_ready_list_and_keeps_its_address() {
    let mut rig = Rig::new();
    assert!(rig.proto.is_ready(), "made unopened, on the ready list");
    assert_eq!(rig.proto.listening(), None);
    rig.listen();
    assert!(!rig.proto.is_ready());
    assert!(!rig.proto.is_empty(), "listening");
}

#[test]
fn a_listen_that_fails_is_closed_by_io_and_its_failure_kept() {
    let mut rig = Rig::new();
    let out = rig.resume();
    let [Io::Listen { owner, .. }] = out.io.as_slice() else { panic!("listens: {out:?}") };
    let owner = *owner;
    rig.up(Told::Failed { owner, error: Error::Other }).nothing();
    assert_eq!(rig.proto.failure(), Some(Error::Other));
    rig.up(Told::Closed { owner }).nothing();
    assert_eq!(rig.proto.failure(), Some(Error::Other), "kept once closed, for main to say why it stopped");
    assert!(rig.proto.is_empty(), "nothing left: the service has stopped");
}

#[test]
fn a_listen_refused_for_want_of_resources_is_asked_again_after_the_retry() {
    let mut rig = Rig::new();
    let out = rig.resume();
    let [Io::Listen { owner, .. }] = out.io.as_slice() else { panic!("listens: {out:?}") };
    let owner = *owner;
    rig.up(Told::Failed { owner, error: Error::Busy }).nothing();
    rig.up(Told::Closed { owner }).nothing();
    assert_eq!(rig.proto.failure(), None, "a shortage is not a failure");
    assert!(!rig.proto.is_empty(), "it will listen again");
    let again = Time::ZERO.saturating_add(LIMITS.retry);
    assert_eq!(rig.proto.next_deadline(), Some(again));
    assert!(!rig.proto.is_due(Time::ZERO));
    rig.at(again);
    let out = rig.fire();
    assert_eq!(out.io, [Io::Listen { owner, addr: addr() }], "asked again, at the same address");
    rig.up(Told::Listening { owner, listener: LISTENER, addr: addr() }).nothing();
    assert_eq!(rig.proto.listening(), Some(addr()));
}

#[test]
fn a_stop_while_the_listener_waits_to_listen_again_closes_it() {
    let mut rig = Rig::new();
    let out = rig.resume();
    let [Io::Listen { owner, .. }] = out.io.as_slice() else { panic!("listens: {out:?}") };
    let owner = *owner;
    rig.up(Told::Failed { owner, error: Error::Busy }).nothing();
    rig.down(Domain::Stop).nothing();
    rig.up(Told::Closed { owner }).nothing();
    assert!(rig.proto.is_empty(), "stopped while its listen failed: it will not listen again");
    let mut rig = Rig::new();
    let _listen = rig.resume();
    rig.up(Told::Failed { owner, error: Error::Busy }).nothing();
    rig.up(Told::Closed { owner }).nothing();
    rig.down(Domain::Stop).nothing();
    assert_eq!(rig.proto.next_deadline(), None, "stopped while it waited");
    assert!(rig.proto.is_empty());
}

#[test]
fn a_listener_whose_accept_stops_is_closed() {
    let mut rig = Rig::new();
    rig.listen();
    let owner = rig.listener_owner();
    let out = rig.up(Told::Failed { owner, error: Error::Other });
    assert_eq!(out.io, [Io::Close { entity: LISTENER }], "io keeps it for its owner to close");
    assert_eq!(rig.proto.listening(), None);
    rig.up(Told::Closed { owner }).nothing();
    assert_eq!(rig.proto.failure(), Some(Error::Other));
}

#[test]
fn shutdown_is_told_the_domain_and_its_stop_closes_the_listener() {
    let mut rig = Rig::new();
    rig.listen();
    rig.proto.shutdown();
    rig.proto.shutdown();
    assert!(rig.proto.is_ready());
    let out = rig.resume();
    assert_eq!(out.calls, [Call::Shutdown], "told once, from the ready list");
    assert!(!rig.proto.is_ready());
    let out = rig.down(Domain::Stop);
    assert_eq!(out.io, [Io::Close { entity: LISTENER }]);
    rig.down(Domain::Stop).nothing();
    let owner = rig.listener_owner();
    rig.up(Told::Closed { owner }).nothing();
    assert_eq!(rig.proto.failure(), None, "stopped, not failed");
    assert!(rig.proto.is_empty());
}

#[test]
fn a_stop_while_the_listen_is_out_closes_the_listener_once_it_listens() {
    let mut rig = Rig::new();
    let out = rig.resume();
    let [Io::Listen { owner, .. }] = out.io.as_slice() else { panic!("listens: {out:?}") };
    let owner = *owner;
    rig.down(Domain::Stop).nothing();
    let out = rig.up(Told::Listening { owner, listener: LISTENER, addr: addr() });
    assert_eq!(out.io, [Io::Close { entity: LISTENER }]);
    assert_eq!(rig.proto.listening(), None, "never listening for the service");
}

#[test]
fn a_stop_before_the_listener_opens_leaves_it_closed() {
    let mut rig = Rig::new();
    rig.down(Domain::Stop).nothing();
    assert!(!rig.proto.is_ready(), "it will not listen");
    assert!(rig.proto.is_empty());
}

#[test]
fn a_socket_announced_while_the_listener_closes_is_rejected() {
    let mut rig = Rig::new();
    rig.listen();
    let _close = rig.down(Domain::Stop);
    let out = rig.accept(1);
    assert_eq!(out.io, [Io::Reject { socket: socket(1) }]);
    let owner = rig.listener_owner();
    rig.up(Told::Failed { owner, error: Error::Other }).nothing();
    assert_eq!(rig.proto.failure(), Some(Error::Other), "a failure told before io took the close is kept");
}

#[test]
fn the_layer_is_empty_only_once_its_connections_are_gone_too() {
    let mut rig = Rig::new();
    rig.listen();
    let conn = rig.bound(1);
    let _close = rig.down(Domain::Stop);
    let owner = rig.listener_owner();
    rig.up(Told::Closed { owner }).nothing();
    assert!(!rig.proto.is_empty(), "a connection runs on after the listener stops");
    let out = rig.up(stream(conn, Up::End));
    assert_eq!(out.io, [close(1)]);
    rig.closed(conn).nothing();
    assert!(!rig.proto.is_empty(), "retired, not yet reclaimed");
    rig.proto.reclaim();
    assert!(rig.proto.is_empty());
}
