//! Sockets and the network in a calm world, one rule of the contract a test.

use alloc::boxed::Box;
use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use skein_io::kernel::{Done, Error, Family, Op};
use skein_sim::Config;

use crate::support::{World, local, received, recv_op};

#[test]
fn bytes_flow_both_ways_and_a_half_close_ends_one_direction() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    assert_eq!(world.send(client, c, b"ping"), Ok(Done::Count(4)));
    assert_eq!(world.shutdown(client, c), Ok(Done::Nothing));
    assert_eq!(world.recv(server, s, 64), Ok(b"ping".to_vec()), "the bytes sent before the half-close");
    assert_eq!(world.recv(server, s, 64), Ok(Vec::new()), "then the end of the stream");
    assert_eq!(world.send(server, s, b"pong"), Ok(Done::Count(4)), "the other direction still works");
    assert_eq!(world.recv(client, c, 64), Ok(b"pong".to_vec()), "a receive works after a shutdown");
    assert_eq!(world.send(client, c, b"more"), Err(Error::BrokenPipe), "no sending after a shutdown");
    assert_eq!(world.shutdown(client, c), Ok(Done::Nothing), "a second shutdown, while the connection lasts");
    assert_eq!(world.shutdown(server, s), Ok(Done::Nothing));
    assert_eq!(world.recv(client, c, 64), Ok(Vec::new()));
    assert_eq!(world.shutdown(client, c), Err(Error::NotConnected), "both sides closed");
    world.close(client, c);
    world.close(server, s);
    world.settled(client);
    world.settled(server);
}

#[test]
fn a_bind_to_port_zero_picks_an_ephemeral_port_and_accept_names_the_peer() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (listener, addr) = world.listener(server);
    assert_eq!(addr.ip(), Ipv4Addr::LOCALHOST);
    assert!((32768..=60999).contains(&addr.port()), "the ephemeral range: {addr}");
    let fd = world.socket(client);
    let source = world.bind(client, fd, local(0)).unwrap();
    assert_ne!(source.port(), addr.port(), "a port 0 bind takes a free port");
    assert_eq!(world.connect(client, fd, addr), Ok(Done::Nothing));
    let (accepted, peer) = world.accept(server, listener);
    assert_eq!(peer, source, "the peer is the client's address");
    for (pid, fd) in [(server, listener), (server, accepted), (client, fd)] {
        world.close(pid, fd);
    }
    world.settled(client);
    world.settled(server);
}

#[test]
fn descriptors_count_up_per_process_and_are_never_reused() {
    let mut world = World::calm();
    let (a, b) = (world.spawn(), world.spawn());
    let first = world.socket(a);
    assert_eq!(world.socket(b), first, "each process has its own table");
    world.close(a, first);
    let second = world.socket(a);
    assert!(second.raw() > first.raw(), "a closed number is not given again");
}

#[test]
fn a_connect_where_nothing_listens_is_refused() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    assert_eq!(world.connect(pid, fd, local(80)), Err(Error::Refused));
    world.close(pid, fd);
    let fd = world.socket(pid);
    let addr = SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 80));
    assert_eq!(world.connect(pid, fd, addr), Err(Error::Unreachable), "only loopback is reachable");
    world.close(pid, fd);
    world.settled(pid);
}

#[test]
fn a_listening_address_is_in_use_but_two_sockets_may_bind_one() {
    let mut world = World::calm();
    let pid = world.spawn();
    let (first, second) = (world.socket(pid), world.socket(pid));
    let addr = world.bind(pid, first, local(0)).unwrap();
    assert_eq!(world.bind(pid, second, addr), Ok(addr), "SO_REUSEADDR: both bind");
    assert_eq!(world.listen(pid, first), Ok(Done::Nothing));
    assert_eq!(world.listen(pid, second), Err(Error::AddressInUse), "the second listen fails");
    let third = world.socket(pid);
    assert_eq!(world.bind(pid, third, addr), Err(Error::AddressInUse), "a listener holds the address");
    let any = SocketAddr::from((Ipv4Addr::UNSPECIFIED, addr.port()));
    let fourth = world.socket(pid);
    assert_eq!(world.bind(pid, fourth, any), Err(Error::AddressInUse), "the unspecified address overlaps");
    let elsewhere = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 0));
    let fifth = world.socket(pid);
    assert_eq!(world.bind(pid, fifth, elsewhere), Err(Error::AddressNotAvailable), "not this host's");
    for fd in [first, second, third, fourth, fifth] {
        world.close(pid, fd);
    }
    world.settled(pid);
}

#[test]
fn ipv6_listens_on_its_own_family() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let listener = match world.call(server, Op::Socket { family: Family::Ipv6 }).result {
        Ok(Done::Fd(fd)) => fd,
        other => panic!("a socket: {other:?}"),
    };
    let addr = world.bind(server, listener, SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))).unwrap();
    assert_eq!(world.listen(server, listener), Ok(Done::Nothing));
    let v4 = world.socket(client);
    assert_eq!(world.connect(client, v4, local(addr.port())), Err(Error::Refused), "families never mix");
    let v6 = match world.call(client, Op::Socket { family: Family::Ipv6 }).result {
        Ok(Done::Fd(fd)) => fd,
        other => panic!("a socket: {other:?}"),
    };
    let to = SocketAddr::from((Ipv6Addr::LOCALHOST, addr.port()));
    assert_eq!(world.connect(client, v6, to), Ok(Done::Nothing));
    let (accepted, peer) = world.accept(server, listener);
    assert_eq!(peer.ip(), Ipv6Addr::LOCALHOST);
    for (pid, fd) in [(server, listener), (server, accepted), (client, v4), (client, v6)] {
        world.close(pid, fd);
    }
}

#[test]
fn a_full_accept_queue_delays_a_connect_and_never_refuses_it() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let listener = world.socket(server);
    let addr = world.bind(server, listener, local(0)).unwrap();
    let listen = world.call(server, Op::Listen { fd: listener, backlog: 0 }).result;
    assert_eq!(listen, Ok(Done::Nothing), "a backlog of 0 is clamped to 1");
    let (first, second) = (world.socket(client), world.socket(client));
    assert_eq!(world.connect(client, first, addr), Ok(Done::Nothing), "one connection can always wait");
    let waiting = world.submit(client, Op::Connect { fd: second, addr });
    assert!(world.reap(client).is_empty(), "the second waits for room");
    world.accept(server, listener);
    let complete = world.reap_one(client, waiting);
    assert_eq!(complete.result, Ok(Done::Nothing), "an accept makes room");
}

#[test]
fn a_send_waits_while_the_peer_buffer_is_full_and_resumes_when_read() {
    let mut world = World::new(1, Config { buffer: 16, ..Config::calm() });
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    let bytes: Box<[u8]> = (0..100).collect();
    assert_eq!(world.call(client, Op::Send { fd: c, bytes: bytes.clone(), from: 0 }).result, Ok(Done::Count(16)));
    let stalled = world.submit(client, Op::Send { fd: c, bytes: bytes.clone(), from: 16 });
    assert!(world.reap(client).is_empty(), "no room, no completion");
    assert!(!world.sim.advance(), "the world is idle: the sender stalls");
    assert_eq!(world.recv(server, s, 10), Ok(bytes[..10].to_vec()));
    let complete = world.reap_one(client, stalled);
    assert_eq!(complete.result, Ok(Done::Count(10)), "as much as was read makes room");
    assert_eq!(complete.kind, Op::Send { fd: c, bytes, from: 16 }, "the record comes back untouched");
}

#[test]
fn a_receive_waits_for_bytes() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    let waiting = world.submit(server, recv_op(s, 8));
    assert!(world.reap(server).is_empty(), "nothing to receive yet");
    assert_eq!(world.send(client, c, b"0123456789"), Ok(Done::Count(10)));
    assert_eq!(received(world.reap_one(server, waiting)), Ok(b"01234567".to_vec()), "up to the buffer");
    assert_eq!(world.recv(server, s, 8), Ok(b"89".to_vec()));
}

#[test]
fn closing_with_unread_bytes_resets_the_peer_once() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    assert_eq!(world.send(client, c, b"unread"), Ok(Done::Count(6)));
    world.close(server, s);
    assert_eq!(world.recv(client, c, 8), Err(Error::Reset), "the reset, reported once");
    assert_eq!(world.recv(client, c, 8), Ok(Vec::new()), "then the end of the stream");
    assert_eq!(world.send(client, c, b"x"), Err(Error::BrokenPipe));
    assert_eq!(world.shutdown(client, c), Err(Error::NotConnected));
    world.close(client, c);
    world.settled(client);
    world.settled(server);
}

#[test]
fn a_reset_reaches_a_waiting_send_first_when_it_is_the_only_one() {
    let mut world = World::new(1, Config { buffer: 4, ..Config::calm() });
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    assert_eq!(world.send(client, c, b"full"), Ok(Done::Count(4)));
    let stalled = world.submit(client, Op::Send { fd: c, bytes: Box::from(&b"more"[..]), from: 0 });
    world.close(server, s);
    assert_eq!(world.reap_one(client, stalled).result, Err(Error::Reset));
    assert_eq!(world.recv(client, c, 8), Ok(Vec::new()), "reported once");
}

#[test]
fn a_graceful_close_ends_the_peer_stream_after_its_bytes() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (c, s) = world.pair(client, server);
    assert_eq!(world.send(server, s, b"bye"), Ok(Done::Count(3)));
    world.close(server, s);
    assert_eq!(world.recv(client, c, 8), Ok(b"bye".to_vec()));
    assert_eq!(world.recv(client, c, 8), Ok(Vec::new()), "end of stream, not a reset");
    assert_eq!(world.send(client, c, b"late"), Ok(Done::Count(4)), "accepted, and lost");
    assert_eq!(world.send(client, c, b"late"), Err(Error::BrokenPipe), "the peer's reset came back");
}

#[test]
fn closing_a_listener_resets_the_connections_waiting_on_it() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (listener, addr) = world.listener(server);
    let fd = world.socket(client);
    assert_eq!(world.connect(client, fd, addr), Ok(Done::Nothing));
    world.close(server, listener);
    assert_eq!(world.send(client, fd, b"hello"), Err(Error::Reset));
    assert_eq!(world.send(client, fd, b"hello"), Err(Error::BrokenPipe));
    world.close(client, fd);
    world.settled(client);
    world.settled(server);
}

#[test]
fn closing_a_listener_refuses_the_connects_still_waiting_for_room() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let listener = world.socket(server);
    let addr = world.bind(server, listener, local(0)).unwrap();
    assert_eq!(world.call(server, Op::Listen { fd: listener, backlog: 1 }).result, Ok(Done::Nothing));
    let (first, second) = (world.socket(client), world.socket(client));
    assert_eq!(world.connect(client, first, addr), Ok(Done::Nothing));
    let waiting = world.submit(client, Op::Connect { fd: second, addr });
    world.close(server, listener);
    assert_eq!(world.reap_one(client, waiting).result, Err(Error::Refused), "nothing listens there now");
    world.close(client, first);
    world.close(client, second);
    world.settled(client);
}

#[test]
fn a_connection_reset_while_waiting_is_still_accepted() {
    let mut world = World::calm();
    let (client, server) = (world.spawn(), world.spawn());
    let (listener, addr) = world.listener(server);
    let fd = world.socket(client);
    assert_eq!(world.connect(client, fd, addr), Ok(Done::Nothing));
    assert_eq!(world.send(client, fd, b"hi"), Ok(Done::Count(2)));
    world.close(client, fd);
    let (accepted, _) = world.accept(server, listener);
    assert_eq!(world.recv(server, accepted, 8), Ok(b"hi".to_vec()), "what was sent before the close");
    assert_eq!(world.recv(server, accepted, 8), Ok(Vec::new()));
}

#[test]
fn records_the_kernel_refuses_for_the_socket_state_fail_without_effect() {
    let mut world = World::calm();
    let pid = world.spawn();
    let fd = world.socket(pid);
    assert_eq!(world.call(pid, Op::Accept { fd }).result, Err(Error::InvalidArgument), "not listening");
    assert_eq!(world.listen(pid, fd), Err(Error::InvalidArgument), "not bound");
    assert_eq!(world.recv(pid, fd, 4), Err(Error::NotConnected));
    assert_eq!(world.send(pid, fd, b"x"), Err(Error::BrokenPipe));
    assert_eq!(world.shutdown(pid, fd), Err(Error::NotConnected));
    world.bind(pid, fd, local(0)).unwrap();
    assert_eq!(world.bind(pid, fd, local(0)), Err(Error::InvalidArgument), "bound already");
    world.close(pid, fd);
    world.settled(pid);
}

#[test]
fn the_wall_clock_starts_where_configured_and_moves_with_time() {
    let world = World::calm();
    assert_eq!(world.sim.wall(), Config::calm().wall);
    assert_eq!(world.sim.now(), skein_lib::Time::ZERO);
}
