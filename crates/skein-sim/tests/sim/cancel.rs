//! Cancelling: a cancel that wins stops its target, one that loses finds it
//! decided; both complete, in either order.

use skein_io::kernel::{Complete, Done, Error, Op};
use skein_lib::{Duration, Token};
use skein_sim::{Config, Faults};

use crate::support::{World, received, recv_op};

/// Every completion delivered a millisecond late: decided, but in flight.
const SLOW: Faults = Faults { latency: 1000, latency_max: Duration::from_millis(1), ..Faults::NONE };

/// The completions of a cancel and of its target, in delivery order.
fn both(world: &mut World, pid: skein_sim::Pid, cancel: Token, target: Token) -> (Complete, Complete, bool) {
    let mut got = world.reap(pid);
    assert_eq!(got.len(), 2, "the cancel and its target both complete");
    let second = got.pop().unwrap();
    let first = got.pop().unwrap();
    let cancel_first = first.op == cancel;
    let (of_cancel, of_target) = if cancel_first { (first, second) } else { (second, first) };
    assert_eq!((of_cancel.op, of_target.op), (cancel, target));
    (of_cancel, of_target, cancel_first)
}

#[test]
fn a_cancel_of_a_waiting_accept_wins() {
    let mut world = World::calm();
    let server = world.spawn();
    let (listener, _) = world.listener(server);
    let accept = world.submit(server, Op::Accept { fd: listener });
    let cancel = world.submit(server, Op::Cancel { target: accept });
    let (of_cancel, of_target, _) = both(&mut world, server, cancel, accept);
    assert_eq!(of_cancel.result, Ok(Done::Nothing), "found in flight");
    assert_eq!(of_target.result, Err(Error::Cancelled));
    assert_eq!(of_target.kind, Op::Accept { fd: listener }, "the record comes back");
    world.close(server, listener);
    world.settled(server);
}

#[test]
fn a_cancel_of_an_accept_decided_before_it_is_too_late() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (listener, addr) = world.listener(server);
    let fd = world.socket(client);
    world.sim.set_faults(SLOW);
    let accept = world.submit(server, Op::Accept { fd: listener });
    world.submit(client, Op::Connect { fd, addr });
    world.enter(server);
    let cancel = world.submit(server, Op::Cancel { target: accept });
    while world.sim.advance() {}
    let (of_cancel, of_target, _) = both(&mut world, server, cancel, accept);
    assert_eq!(of_cancel.result, Err(Error::TooLate), "decided when the server entered, delivered late");
    assert!(matches!(of_target.result, Ok(Done::Accepted { .. })), "the target says what it did");
}

#[test]
fn a_cancel_of_an_accept_whose_connection_arrived_while_its_process_was_away_stops_it() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (listener, addr) = world.listener(server);
    let accept = world.submit(server, Op::Accept { fd: listener });
    let fd = world.socket(client);
    assert_eq!(world.connect(client, fd, addr), Ok(Done::Nothing), "the network establishes it at once");
    let cancel = world.submit(server, Op::Cancel { target: accept });
    let (of_cancel, of_target, _) = both(&mut world, server, cancel, accept);
    assert_eq!((of_cancel.result, of_target.result), (Ok(Done::Nothing), Err(Error::Cancelled)), "as on the ring");
    let (accepted, _) = world.accept(server, listener);
    assert_eq!(world.send(client, fd, b"kept"), Ok(Done::Count(4)));
    assert_eq!(world.recv(server, accepted, 8), Ok(b"kept".to_vec()), "the connection waited for the next accept");
}

#[test]
fn a_cancel_of_a_waiting_recv_wins_and_takes_no_bytes() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    let recv = world.submit(server, recv_op(s, 8));
    let cancel = world.submit(server, Op::Cancel { target: recv });
    let (of_cancel, of_target, _) = both(&mut world, server, cancel, recv);
    assert_eq!((of_cancel.result, of_target.result), (Ok(Done::Nothing), Err(Error::Cancelled)));
    assert_eq!(world.send(client, c, b"later"), Ok(Done::Count(5)));
    assert_eq!(world.recv(server, s, 8), Ok(b"later".to_vec()), "the next receive gets the bytes");
}

#[test]
fn a_cancel_of_a_recv_that_got_bytes_is_too_late() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    world.sim.set_faults(SLOW);
    let recv = world.submit(server, recv_op(s, 8));
    world.submit(client, Op::Send { fd: c, bytes: Box::from(*b"data"), from: 0 });
    world.enter(server);
    let cancel = world.submit(server, Op::Cancel { target: recv });
    while world.sim.advance() {}
    let (of_cancel, of_target, _) = both(&mut world, server, cancel, recv);
    assert_eq!(of_cancel.result, Err(Error::TooLate), "decided when the server entered, delivered late");
    assert_eq!(received(of_target), Ok(b"data".to_vec()), "the target completes normally");
}

#[test]
fn a_cancel_of_a_recv_whose_bytes_arrived_while_its_process_was_away_stops_it() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    let recv = world.submit(server, recv_op(s, 8));
    assert_eq!(world.send(client, c, b"data"), Ok(Done::Count(4)));
    let cancel = world.submit(server, Op::Cancel { target: recv });
    let (of_cancel, of_target, _) = both(&mut world, server, cancel, recv);
    assert_eq!((of_cancel.result, of_target.result), (Ok(Done::Nothing), Err(Error::Cancelled)), "as on the ring");
    assert_eq!(world.recv(server, s, 8), Ok(b"data".to_vec()), "the bytes wait for the next receive");
}

#[test]
fn a_cancel_of_a_token_not_in_flight_is_too_late() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    let complete = world.call(pid, Op::Cancel { target: Token::new(999) });
    assert_eq!(complete.result, Err(Error::TooLate));
    world.close(pid, fd);
}

#[test]
fn a_cancelled_connect_leaves_only_close() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let listener = world.socket(server);
    let addr = world.bind(server, listener, crate::support::local(0)).unwrap();
    assert_eq!(world.call(server, Op::Listen { fd: listener, backlog: 1 }).result, Ok(Done::Nothing));
    let (first, second) = (world.socket(client), world.socket(client));
    assert_eq!(world.connect(client, first, addr), Ok(Done::Nothing));
    let connect = world.submit(client, Op::Connect { fd: second, addr });
    let cancel = world.submit(client, Op::Cancel { target: connect });
    let (of_cancel, of_target, _) = both(&mut world, client, cancel, connect);
    assert_eq!((of_cancel.result, of_target.result), (Ok(Done::Nothing), Err(Error::Cancelled)));
    world.accept(server, listener);
    assert!(world.reap(client).is_empty(), "the cancelled connect no longer waits for room");
    world.close(client, second);
}

#[test]
fn a_cancel_and_its_target_complete_in_either_order() {
    let (mut cancel_first, mut target_first) = (0_u32, 0_u32);
    for seed in 0..64_u64 {
        let mut world = World::new(seed, Config::calm());
        let server = world.spawn();
        let (listener, _) = world.listener(server);
        let accept = world.submit(server, Op::Accept { fd: listener });
        let cancel = world.submit(server, Op::Cancel { target: accept });
        if both(&mut world, server, cancel, accept).2 {
            cancel_first = cancel_first.checked_add(1).unwrap();
        } else {
            target_first = target_first.checked_add(1).unwrap();
        }
    }
    assert!(cancel_first > 0 && target_first > 0, "{cancel_first} and {target_first}");
}

#[test]
fn a_raced_cancel_lands_late_and_may_lose() {
    let faults = Faults { cancel_race: 1000, latency_max: Duration::from_millis(1), ..Faults::NONE };
    let config = Config { faults, ..Config::calm() };
    let mut world = World::new(5, config);
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);

    let recv = world.submit(server, recv_op(s, 8));
    let cancel = world.submit(server, Op::Cancel { target: recv });
    assert!(world.reap(server).is_empty(), "the cancel has not landed");
    assert_eq!(world.send(client, c, b"won"), Ok(Done::Count(3)));
    world.enter(server);
    assert!(world.sim.advance(), "the cancel lands");
    let (of_cancel, of_target, cancel_first) = both(&mut world, server, cancel, recv);
    assert_eq!(of_cancel.result, Err(Error::TooLate), "the target completed first");
    assert_eq!(received(of_target), Ok(b"won".to_vec()));
    assert!(!cancel_first);

    let recv = world.submit(server, recv_op(s, 8));
    let cancel = world.submit(server, Op::Cancel { target: recv });
    assert!(world.sim.advance(), "the cancel lands");
    let (of_cancel, of_target, _) = both(&mut world, server, cancel, recv);
    assert_eq!((of_cancel.result, of_target.result), (Ok(Done::Nothing), Err(Error::Cancelled)));
}

#[test]
fn a_late_cancel_never_lands_on_a_later_operation_with_its_target_token() {
    let faults = Faults { cancel_race: 1000, latency_max: Duration::from_millis(1), ..Faults::NONE };
    let mut world = World::new(9, Config { faults, ..Config::calm() });
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    let recv = world.token();
    world.submit_as(server, recv, recv_op(s, 8));
    let cancel = world.submit(server, Op::Cancel { target: recv });
    assert_eq!(world.send(client, c, b"one"), Ok(Done::Count(3)));
    let first = world.reap_one(server, recv);
    assert_eq!(received(first), Ok(b"one".to_vec()));
    world.submit_as(server, recv, recv_op(s, 8));
    assert!(world.sim.advance(), "the cancel lands");
    let got = world.reap(server);
    assert_eq!(got.len(), 1, "only the cancel completes");
    assert_eq!((got[0].op, got[0].result), (cancel, Err(Error::TooLate)), "its target is gone");
}

#[test]
fn a_raced_cancel_may_interrupt_its_target_and_say_it_was_too_late() {
    let faults = Faults { cancel_race: 1000, latency_max: Duration::from_millis(1), ..Faults::NONE };
    let (mut stopped, mut interrupted) = (0_u32, 0_u32);
    for seed in 0..32_u64 {
        let mut world = World::new(seed, Config { faults, ..Config::calm() });
        let server = world.spawn();
        let (listener, _) = world.listener(server);
        let accept = world.submit(server, Op::Accept { fd: listener });
        let cancel = world.submit(server, Op::Cancel { target: accept });
        assert!(world.sim.advance(), "the cancel lands");
        let (of_cancel, of_target, _) = both(&mut world, server, cancel, accept);
        assert_eq!(of_target.result, Err(Error::Cancelled), "nothing connected: the accept is stopped");
        match of_cancel.result {
            Ok(Done::Nothing) => stopped = stopped.checked_add(1).unwrap(),
            Err(Error::TooLate) => interrupted = interrupted.checked_add(1).unwrap(),
            other => panic!("seed {seed}: a cancel: {other:?}"),
        }
    }
    assert!(stopped > 0 && interrupted > 0, "{stopped} stopped, {interrupted} interrupted");
}

#[test]
fn a_cancel_the_backend_cannot_submit_leaves_its_target_running() {
    let faults = Faults { cancel_unsubmitted: 1000, ..Faults::NONE };
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    let recv = world.submit(server, recv_op(s, 8));
    world.sim.set_faults(faults);
    let cancel = world.submit(server, Op::Cancel { target: recv });
    let complete = world.reap_one(server, cancel);
    assert!(matches!(complete.result, Err(Error::Other(_))), "not submitted: {:?}", complete.result);
    world.sim.set_faults(Faults::NONE);
    assert_eq!(world.send(client, c, b"still"), Ok(Done::Count(5)));
    assert_eq!(received(world.reap_one(server, recv)), Ok(b"still".to_vec()), "the target ran on");
}
