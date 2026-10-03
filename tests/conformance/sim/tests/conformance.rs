//! Every scenario, each over the calm seeds and a few of chaos; a failing
//! seed is named, with the end of its trace. The sweeps over many seeds are
//! in `fuzzy_conformance.rs`.

use skein_conformance::{
    Check, Pairing, accept_past_the_descriptor_limit, address_in_use, backpressure, cancel_accept, cancel_connect,
    cancel_connect_established_while_away, cancel_recv, closed_before_accept, full_accept_queue, graceful_close,
    ipv6_only, lifecycle, listener_close_resets_waiting, refused, reset_after_end_of_stream, send_after_peer_closed,
    unread_close_meets_recv, unread_close_meets_send, wrong_state,
};
use skein_conformance_sim::{
    CALM, RACES, SMOKE, Simulated, cancel_chaos, each_seed, loopback_chaos, racing_accept, racing_recv,
};
use skein_io::kernel::Family;
use skein_sim::Config;

/// Calm, then a few seeds with the faults loopback can show.
fn calm_and_chaos<S: Check>(scenario: fn(&mut Simulated) -> S) {
    each_seed(Config::calm(), CALM, scenario);
    each_seed(loopback_chaos(), SMOKE, scenario);
}

#[test]
fn a_connection_lives_and_ends_over_ipv4() {
    calm_and_chaos(|world| lifecycle(world, Family::Ipv4));
}

#[test]
fn a_connection_lives_and_ends_over_ipv6() {
    calm_and_chaos(|world| lifecycle(world, Family::Ipv6));
}

#[test]
fn a_peer_closes_gracefully() {
    calm_and_chaos(graceful_close);
}

#[test]
fn a_send_after_the_peer_closed() {
    calm_and_chaos(send_after_peer_closed);
}

#[test]
fn a_connect_where_nothing_listens() {
    calm_and_chaos(refused);
}

#[test]
fn where_an_address_is_in_use() {
    calm_and_chaos(address_in_use);
}

#[test]
fn ipv6_sockets_are_ipv6_only() {
    calm_and_chaos(ipv6_only);
}

#[test]
fn records_wrong_for_the_socket_state() {
    calm_and_chaos(wrong_state);
}

#[test]
fn a_full_accept_queue() {
    calm_and_chaos(full_accept_queue);
}

#[test]
fn a_close_with_bytes_unread_meets_a_recv() {
    calm_and_chaos(unread_close_meets_recv);
}

#[test]
fn a_close_with_bytes_unread_meets_a_send() {
    calm_and_chaos(unread_close_meets_send);
}

#[test]
fn a_reset_after_the_end_of_stream() {
    calm_and_chaos(reset_after_end_of_stream);
}

#[test]
fn a_client_closed_before_its_connection_is_accepted() {
    calm_and_chaos(closed_before_accept);
}

#[test]
fn backpressure_stalls_a_sender_and_reading_resumes_it() {
    calm_and_chaos(backpressure);
}

#[test]
fn a_cancel_of_a_waiting_accept() {
    calm_and_chaos(cancel_accept);
    each_seed(cancel_chaos(), SMOKE, cancel_accept);
}

#[test]
fn a_cancel_of_a_waiting_recv() {
    calm_and_chaos(cancel_recv);
    each_seed(cancel_chaos(), SMOKE, cancel_recv);
}

#[test]
fn a_cancel_of_a_recv_racing_bytes() {
    for race in RACES {
        calm_and_chaos(racing_recv(race));
        each_seed(cancel_chaos(), SMOKE, racing_recv(race));
    }
}

#[test]
fn a_cancel_of_an_accept_racing_a_connect() {
    for race in RACES {
        calm_and_chaos(racing_accept(race));
        each_seed(cancel_chaos(), SMOKE, racing_accept(race));
    }
}

#[test]
fn a_cancel_of_a_waiting_connect() {
    calm_and_chaos(cancel_connect);
    each_seed(cancel_chaos(), SMOKE, cancel_connect);
}

#[test]
fn a_cancel_of_a_connect_established_while_its_client_was_away() {
    calm_and_chaos(cancel_connect_established_while_away);
    each_seed(cancel_chaos(), SMOKE, cancel_connect_established_while_away);
}

/// A calm world pairs each race as the ring does: a cancel stops a target
/// whose process has not entered since what it waited for arrived.
#[test]
fn a_calm_world_pairs_each_race_as_the_ring_does() {
    let expected = [Pairing::Stopped, Pairing::Stopped, Pairing::Completed];
    for seed in 0..CALM {
        for (race, pairing) in RACES.into_iter().zip(expected) {
            for scenario in [racing_recv(race), racing_accept(race)] {
                let seen = scenario(&mut Simulated::new(seed, Config::calm()));
                assert_eq!(seen.pairing(), pairing, "seed {seed}, {race:?}: {seen:?}");
            }
        }
        let seen = cancel_connect_established_while_away(&mut Simulated::new(seed, Config::calm()));
        assert_eq!(seen.cancelling.pairing(), Pairing::Stopped, "seed {seed}: {seen:?}");
    }
}

#[test]
fn closing_a_listener_resets_the_connections_waiting_on_it() {
    calm_and_chaos(listener_close_resets_waiting);
}

/// Simulator only: the ring's descriptor limit is the process's, which a
/// test cannot lower without `unsafe` (programming-model.md, 2.1).
#[test]
fn an_accept_past_the_descriptor_limit() {
    let few = |config: Config| Config { max_fds: 4, ..config };
    each_seed(few(Config::calm()), CALM, accept_past_the_descriptor_limit);
    each_seed(few(loopback_chaos()), SMOKE, accept_past_the_descriptor_limit);
}

/// Seeds of loopback's chaos in which the peer's reset is late.
const LATE_RESETS: [u64; 2] = [1, 6];

/// The peer's reset, answering a `Send` after it closed, is sometimes late
/// in a chaos world, as over a network: more than one `Send` succeeds.
#[test]
fn a_late_reset_lets_more_than_one_send_succeed() {
    for seed in LATE_RESETS {
        let seen = send_after_peer_closed(&mut Simulated::new(seed, loopback_chaos()));
        assert!(seen.sends.len() > 2, "seed {seed}: {seen:?}");
    }
}

/// The chaos the suite runs with is loopback's: nothing it draws needs a
/// network beyond it.
#[test]
fn loopback_chaos_draws_no_fault_beyond_loopback() {
    let faults = loopback_chaos().faults;
    assert_eq!((faults.reset, faults.refuse, faults.timed_out, faults.no_buffer), (0, 0, 0, 0));
    assert_eq!(faults.cancel_unsubmitted, 0);
    assert!(faults.latency > 0 && faults.short_send > 0 && faults.short_recv > 0 && faults.cancel_race > 0);
    assert!(faults.late_reset > 0, "a late reset is loopback's too, as timing");
}
