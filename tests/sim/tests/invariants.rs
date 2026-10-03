//! Each broken invariant of the contract fails the world, as do the checks
//! the simulator adds at quiescence.

use std::net::{Ipv6Addr, SocketAddr};

use skein_io::kernel::{Done, Fd, Op};
use skein_sim_tests::{World, local, recv_op};

#[test]
#[should_panic(expected = "an invalid record")]
fn an_empty_receive_buffer() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    world.submit(client, Op::Recv { fd: c, buf: Box::from(&[][..]) });
}

#[test]
#[should_panic(expected = "an invalid record")]
fn a_send_with_nothing_left() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    world.submit(client, Op::Send { fd: c, bytes: Box::from(&b"ab"[..]), from: 2 });
}

#[test]
#[should_panic(expected = "already in flight")]
fn a_token_reused_while_in_flight() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    let token = world.submit(client, recv_op(c, 4));
    world.submit_as(client, token, Op::Shutdown { fd: c });
}

#[test]
#[should_panic(expected = "already in flight")]
fn a_token_reused_before_its_completion_is_reaped() {
    let mut world = World::calm();
    let pid = world.spawn();
    let token = world.token();
    world.submit_as(pid, token, Op::Socket { family: skein_io::kernel::Family::Ipv4 });
    world.submit_as(pid, token, Op::Socket { family: skein_io::kernel::Family::Ipv4 });
}

#[test]
#[should_panic(expected = "which is a Cancel")]
fn a_cancel_of_a_cancel() {
    let faults = skein_sim::Faults {
        cancel_race: 1000,
        latency_max: skein_lib::Duration::from_millis(1),
        ..skein_sim::Faults::NONE
    };
    let mut world = World::new(1, skein_sim::Config { faults, ..skein_sim::Config::calm() });
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    let recv = world.submit(client, recv_op(c, 4));
    let cancel = world.submit(client, Op::Cancel { target: recv });
    world.submit(client, Op::Cancel { target: cancel });
}

#[test]
#[should_panic(expected = "another family")]
fn a_bind_of_another_family() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    world.submit(pid, Op::Bind { fd, addr: SocketAddr::from((Ipv6Addr::LOCALHOST, 0)) });
}

#[test]
#[should_panic(expected = "another family")]
fn a_connect_to_another_family() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    world.submit(pid, Op::Connect { fd, addr: SocketAddr::from((Ipv6Addr::LOCALHOST, 80)) });
}

#[test]
#[should_panic(expected = "a second Recv")]
fn two_receives_on_one_descriptor() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    world.submit(client, recv_op(c, 4));
    world.submit(client, recv_op(c, 4));
}

#[test]
#[should_panic(expected = "a second Send")]
fn two_sends_on_one_descriptor() {
    let mut world = World::new(1, skein_sim::Config { buffer: 1, ..skein_sim::Config::calm() });
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    world.submit(client, Op::Send { fd: c, bytes: Box::from(&b"ab"[..]), from: 0 });
    world.submit(client, Op::Send { fd: c, bytes: Box::from(&b"cd"[..]), from: 0 });
}

#[test]
#[should_panic(expected = "a second Accept")]
fn two_accepts_on_one_listener() {
    let mut world = World::calm();
    let pid = world.spawn();
    let (listener, _) = world.listener(pid);
    world.submit(pid, Op::Accept { fd: listener });
    world.submit(pid, Op::Accept { fd: listener });
}

#[test]
#[should_panic(expected = "beside a Connect")]
fn anything_beside_a_connect() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let listener = world.socket(server);
    let addr = world.bind(server, listener, local(0)).unwrap();
    world.call(server, Op::Listen { fd: listener, backlog: 1 });
    let (first, second) = (world.socket(client), world.socket(client));
    assert_eq!(world.connect(client, first, addr), Ok(Done::Nothing));
    world.submit(client, Op::Connect { fd: second, addr });
    world.submit(client, Op::Shutdown { fd: second });
}

#[test]
#[should_panic(expected = "after a Connect")]
fn anything_but_close_after_a_failed_connect() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    world.connect(pid, fd, local(80)).unwrap_err();
    world.submit(pid, recv_op(fd, 4));
}

#[test]
#[should_panic(expected = "a Shutdown while a Send")]
fn a_shutdown_during_a_send() {
    let mut world = World::new(1, skein_sim::Config { buffer: 1, ..skein_sim::Config::calm() });
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    world.send(client, c, b"a").unwrap();
    world.submit(client, Op::Send { fd: c, bytes: Box::from(&b"b"[..]), from: 0 });
    world.submit(client, Op::Shutdown { fd: c });
}

#[test]
#[should_panic(expected = "a Close while another operation")]
fn a_close_beside_a_receive() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    world.submit(client, recv_op(c, 4));
    world.submit(client, Op::Close { fd: c });
}

#[test]
#[should_panic(expected = "a Close while another operation")]
fn a_close_beside_a_completion_not_yet_reaped() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    world.submit(pid, Op::Bind { fd, addr: local(0) });
    world.submit(pid, Op::Close { fd });
}

#[test]
#[should_panic(expected = "not open in this process")]
fn an_operation_after_close() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    world.close(pid, fd);
    world.submit(pid, Op::Listen { fd, backlog: 1 });
}

#[test]
#[should_panic(expected = "not open in this process")]
fn an_operation_on_another_process_descriptor() {
    let mut world = World::calm();
    let (a, b) = (world.spawn(), world.spawn());
    world.socket(a);
    let mine = world.socket(a);
    world.submit(b, Op::Close { fd: mine });
}

#[test]
#[should_panic(expected = "not open in this process")]
fn an_operation_on_a_descriptor_never_issued() {
    let mut world = World::calm();
    let pid = world.spawn();
    world.submit(pid, Op::Close { fd: Fd::new(42) });
}

#[test]
#[should_panic(expected = "not quiescent")]
fn quiescence_with_an_operation_in_flight() {
    let mut world = World::calm();
    let pid = world.spawn();
    let (listener, _) = world.listener(pid);
    world.submit(pid, Op::Accept { fd: listener });
    world.sim.assert_quiescent(pid);
}

#[test]
#[should_panic(expected = "not quiescent")]
fn quiescence_with_a_completion_not_reaped() {
    let mut world = World::calm();
    let pid = world.spawn();
    world.submit(pid, Op::Socket { family: skein_io::kernel::Family::Ipv4 });
    world.sim.assert_quiescent(pid);
}

#[test]
#[should_panic(expected = "descriptors open")]
fn quiescence_with_a_descriptor_open() {
    let mut world = World::calm();
    let pid = world.spawn();
    world.socket(pid);
    world.sim.assert_quiescent(pid);
    world.sim.assert_no_open_fds(pid);
}

#[test]
fn a_failure_names_the_seed_and_the_trace() {
    let result = std::panic::catch_unwind(|| {
        let mut world = World::new(77, skein_sim::Config::calm());
        let pid = world.spawn();
        world.submit(pid, Op::Close { fd: Fd::new(3) });
    });
    let payload = result.unwrap_err();
    let message = payload.downcast_ref::<String>().unwrap();
    assert!(message.contains("seed 77"), "{message}");
    assert!(message.contains("Close"), "{message}");
}

#[test]
#[should_panic(expected = "a Listen on an unbound socket")]
fn a_listen_on_an_unbound_socket() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    world.submit(pid, Op::Listen { fd, backlog: 1 });
}

#[test]
#[should_panic(expected = "a Connect on a socket that is not fresh")]
fn a_connect_on_a_listener() {
    let mut world = World::calm();
    let pid = world.spawn();
    let (listener, addr) = world.listener(pid);
    world.submit(pid, Op::Connect { fd: listener, addr });
}

#[test]
#[should_panic(expected = "a Connect on a socket that is not fresh")]
fn a_connect_on_a_connected_socket() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, _) = world.pair(client, server);
    world.submit(client, Op::Connect { fd: c, addr: local(80) });
}

#[test]
#[should_panic(expected = "a Shutdown on a socket with no connection")]
fn a_shutdown_on_a_fresh_socket() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    world.submit(pid, Op::Shutdown { fd });
}

#[test]
#[should_panic(expected = "a Shutdown on a socket with no connection")]
fn a_shutdown_on_a_listener() {
    let mut world = World::calm();
    let pid = world.spawn();
    let (listener, _) = world.listener(pid);
    world.submit(pid, Op::Shutdown { fd: listener });
}

#[test]
#[should_panic(expected = "at least one byte")]
fn a_world_with_no_socket_buffer() {
    drop(skein_sim::Sim::new(1, skein_sim::Config { buffer: 0, ..skein_sim::Config::calm() }));
}
