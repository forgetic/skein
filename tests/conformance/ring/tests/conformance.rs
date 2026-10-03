//! The conformance suite against the ring on the real kernel, on loopback
//! (kernel.md, 8): every scenario of `skein_conformance` that loopback can
//! provoke, each process of a scenario a `Kernel` of its own. The same
//! scenarios run against the simulator in `tests/conformance/sim`.
//!
//! Not here: a failed `Accept` past the descriptor limit, which needs the
//! process's limit lowered, and so `unsafe` outside the ring adapter
//! (programming-model.md, 2.1); it runs on the simulator only.
//!
//! A machine without `io_uring` fails every test here, saying so.

use skein_conformance::{
    Cancelling, Check, Pairing, Race, address_in_use, backpressure, cancel_accept, cancel_accept_racing_a_connect,
    cancel_connect, cancel_connect_established_while_away, cancel_recv, cancel_recv_racing_bytes, closed_before_accept,
    full_accept_queue, graceful_close, ipv6_only, lifecycle, listener_close_resets_waiting, refused,
    reset_after_end_of_stream, send_after_peer_closed, unread_close_meets_recv, unread_close_meets_send, wrong_state,
};
use skein_conformance_ring::Ring;
use skein_io::kernel::Family;

/// Runs `scenario` on a ring of its own and checks what it saw.
fn on_the_ring<S: Check>(scenario: fn(&mut Ring) -> S) {
    scenario(&mut Ring::new()).check();
}

#[test]
fn a_connection_lives_and_ends_over_ipv4() {
    on_the_ring(|ring| lifecycle(ring, Family::Ipv4));
}

#[test]
fn a_connection_lives_and_ends_over_ipv6() {
    on_the_ring(|ring| lifecycle(ring, Family::Ipv6));
}

#[test]
fn a_peer_closes_gracefully() {
    on_the_ring(graceful_close);
}

#[test]
fn a_send_after_the_peer_closed() {
    on_the_ring(send_after_peer_closed);
}

#[test]
fn a_connect_where_nothing_listens() {
    on_the_ring(refused);
}

#[test]
fn where_an_address_is_in_use() {
    on_the_ring(address_in_use);
}

#[test]
fn ipv6_sockets_are_ipv6_only() {
    on_the_ring(ipv6_only);
}

#[test]
fn records_wrong_for_the_socket_state() {
    on_the_ring(wrong_state);
}

#[test]
fn a_full_accept_queue() {
    on_the_ring(full_accept_queue);
}

#[test]
fn a_close_with_bytes_unread_meets_a_recv() {
    on_the_ring(unread_close_meets_recv);
}

#[test]
fn a_close_with_bytes_unread_meets_a_send() {
    on_the_ring(unread_close_meets_send);
}

#[test]
fn a_reset_after_the_end_of_stream() {
    on_the_ring(reset_after_end_of_stream);
}

#[test]
fn a_client_closed_before_its_connection_is_accepted() {
    on_the_ring(closed_before_accept);
}

#[test]
fn backpressure_stalls_a_sender_and_reading_resumes_it() {
    on_the_ring(backpressure);
}

#[test]
fn a_cancel_of_a_waiting_accept() {
    on_the_ring(cancel_accept);
}

#[test]
fn a_cancel_of_a_waiting_recv() {
    on_the_ring(cancel_recv);
}

/// On the ring each race pairs one way: a cancel stops a target whose
/// process has not entered its ring since what it waited for arrived, and
/// is too late for one whose process has.
fn race(scenario: fn(&mut Ring, Race) -> Cancelling) {
    for (race, pairing) in [
        (Race::CancelFirst, Pairing::Stopped),
        (Race::ArrivedAway, Pairing::Stopped),
        (Race::ArrivedEntered, Pairing::Completed),
    ] {
        let seen = scenario(&mut Ring::new(), race);
        seen.check();
        assert_eq!(seen.pairing(), pairing, "{race:?}: {seen:?}");
    }
}

#[test]
fn a_cancel_of_a_recv_racing_bytes() {
    race(cancel_recv_racing_bytes);
}

#[test]
fn a_cancel_of_an_accept_racing_a_connect() {
    race(cancel_accept_racing_a_connect);
}

#[test]
fn a_cancel_of_a_waiting_connect() {
    on_the_ring(cancel_connect);
}

/// The SYN a full accept queue dropped is retransmitted a second later,
/// once an accept made room, and establishes the connection while the
/// client stays out of its ring: its cancel still stops the `Connect`.
#[test]
fn a_cancel_of_a_connect_established_while_its_client_was_away() {
    let seen = cancel_connect_established_while_away(&mut Ring::new());
    seen.check();
    assert_eq!(seen.cancelling.pairing(), Pairing::Stopped, "{seen:?}");
}

#[test]
fn closing_a_listener_resets_the_connections_waiting_on_it() {
    on_the_ring(listener_close_resets_waiting);
}
