//! Every scenario, each over many seeds, calm and with chaos; a failing
//! seed is named, with the end of its trace.

use core::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use alloc::collections::BTreeSet;

use skein_io::kernel::Family;
use skein_sim::conformance::{
    Cancelling, Check, Pairing, Race, accept_past_the_descriptor_limit, address_in_use, backpressure, cancel_accept,
    cancel_accept_racing_a_connect, cancel_connect, cancel_connect_established_while_away, cancel_recv,
    cancel_recv_racing_bytes, closed_before_accept, full_accept_queue, graceful_close, ipv6_only, lifecycle,
    listener_close_resets_waiting, loopback_chaos, refused, reset_after_end_of_stream, send_after_peer_closed,
    unread_close_meets_recv, unread_close_meets_send, wrong_state,
};
use skein_sim::{Config, Faults, Sim};

/// Seeds per scenario: a calm world draws only ports and the order of a
/// cancel and its target; chaos draws much more.
const CALM: u64 = 16;
const CHAOS: u64 = 200;

/// How many trace entries a failure prints.
const TAIL: usize = 60;

/// Runs `scenario` and checks what it saw, for each seed below `seeds`.
fn each_seed<S: Check>(config: Config, seeds: u64, scenario: fn(&mut Sim) -> S) {
    for seed in 0..seeds {
        let mut sim = Sim::new(seed, config);
        let outcome = catch_unwind(AssertUnwindSafe(|| scenario(&mut sim).check()));
        if outcome.is_err() {
            let from = sim.trace().len().saturating_sub(TAIL);
            let tail = skein_sim::render(seed, &sim.trace()[from..]);
            panic!("the scenario failed at seed {seed} of {config:?}\n{tail}");
        }
    }
}

/// Calm, then with the faults loopback can show.
fn calm_and_chaos<S: Check>(scenario: fn(&mut Sim) -> S) {
    each_seed(Config::calm(), CALM, scenario);
    each_seed(loopback_chaos(), CHAOS, scenario);
}

/// The chaos of loopback, and cancels the backend cannot submit: a fault
/// beyond loopback, which only the cancel scenarios meet.
fn cancel_chaos() -> Config {
    let chaos = loopback_chaos();
    let faults = Faults { cancel_unsubmitted: Faults::CHAOS.cancel_unsubmitted, ..chaos.faults };
    Config { faults, ..chaos }
}

#[test]
fn a_connection_lives_and_ends_over_ipv4() {
    calm_and_chaos(|sim| lifecycle(sim, Family::Ipv4));
}

#[test]
fn a_connection_lives_and_ends_over_ipv6() {
    calm_and_chaos(|sim| lifecycle(sim, Family::Ipv6));
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
    each_seed(cancel_chaos(), CHAOS, cancel_accept);
}

#[test]
fn a_cancel_of_a_waiting_recv() {
    calm_and_chaos(cancel_recv);
    each_seed(cancel_chaos(), CHAOS, cancel_recv);
}

/// A target's race, as a scenario of the world alone.
type Racing = fn(Race) -> fn(&mut Sim) -> Cancelling;

/// Each race of a target.
const RACES: [Race; 3] = [Race::CancelFirst, Race::ArrivedAway, Race::ArrivedEntered];

fn racing_recv(race: Race) -> fn(&mut Sim) -> Cancelling {
    match race {
        Race::CancelFirst => |sim| cancel_recv_racing_bytes(sim, Race::CancelFirst),
        Race::ArrivedAway => |sim| cancel_recv_racing_bytes(sim, Race::ArrivedAway),
        Race::ArrivedEntered => |sim| cancel_recv_racing_bytes(sim, Race::ArrivedEntered),
    }
}

fn racing_accept(race: Race) -> fn(&mut Sim) -> Cancelling {
    match race {
        Race::CancelFirst => |sim| cancel_accept_racing_a_connect(sim, Race::CancelFirst),
        Race::ArrivedAway => |sim| cancel_accept_racing_a_connect(sim, Race::ArrivedAway),
        Race::ArrivedEntered => |sim| cancel_accept_racing_a_connect(sim, Race::ArrivedEntered),
    }
}

#[test]
fn a_cancel_of_a_recv_racing_bytes() {
    for race in RACES {
        calm_and_chaos(racing_recv(race));
        each_seed(cancel_chaos(), CHAOS, racing_recv(race));
    }
}

#[test]
fn a_cancel_of_an_accept_racing_a_connect() {
    for race in RACES {
        calm_and_chaos(racing_accept(race));
        each_seed(cancel_chaos(), CHAOS, racing_accept(race));
    }
}

#[test]
fn a_cancel_of_a_waiting_connect() {
    calm_and_chaos(cancel_connect);
    each_seed(cancel_chaos(), CHAOS, cancel_connect);
}

#[test]
fn a_cancel_of_a_connect_established_while_its_client_was_away() {
    calm_and_chaos(cancel_connect_established_while_away);
    each_seed(cancel_chaos(), CHAOS, cancel_connect_established_while_away);
}

/// A calm world pairs each race as the ring does: a cancel stops a target
/// whose process has not entered since what it waited for arrived.
#[test]
fn a_calm_world_pairs_each_race_as_the_ring_does() {
    let expected = [Pairing::Stopped, Pairing::Stopped, Pairing::Completed];
    for seed in 0..CALM {
        for (race, pairing) in RACES.into_iter().zip(expected) {
            for scenario in [racing_recv(race), racing_accept(race)] {
                let seen = scenario(&mut Sim::new(seed, Config::calm()));
                assert_eq!(seen.pairing(), pairing, "seed {seed}, {race:?}: {seen:?}");
            }
        }
        let seen = cancel_connect_established_while_away(&mut Sim::new(seed, Config::calm()));
        assert_eq!(seen.cancelling.pairing(), Pairing::Stopped, "seed {seed}: {seen:?}");
    }
}

#[test]
fn closing_a_listener_resets_the_connections_waiting_on_it() {
    calm_and_chaos(listener_close_resets_waiting);
}

/// Simulator only: the ring's descriptor limit is the process's, which a
/// test cannot lower without `unsafe` (programming-style.md, 9.2).
#[test]
fn an_accept_past_the_descriptor_limit() {
    let few = |config: Config| Config { max_fds: 4, ..config };
    each_seed(few(Config::calm()), CALM, accept_past_the_descriptor_limit);
    each_seed(few(loopback_chaos()), CHAOS, accept_past_the_descriptor_limit);
}

/// The simulator draws among the pairings the contract allows. Over the
/// seeds, the races of a `Recv` and of an `Accept` each meet a cancel that
/// stops the target, one too late for it, and one the backend cannot
/// submit. A target interrupted as it is stopped needs a raced cancel to
/// land while its process is away, before anything arrives: targets that
/// wait for nothing meet it.
#[test]
fn the_simulator_meets_each_pairing_over_the_seeds() {
    let targets: [Racing; 2] = [racing_recv, racing_accept];
    for racing in targets {
        let mut seen = BTreeSet::new();
        for race in [Race::CancelFirst, Race::ArrivedAway] {
            for config in [loopback_chaos(), cancel_chaos()] {
                for seed in 0..CHAOS {
                    seen.insert(racing(race)(&mut Sim::new(seed, config)).pairing());
                }
            }
        }
        let expected = BTreeSet::from([Pairing::Stopped, Pairing::Completed, Pairing::RanOn]);
        assert_eq!(seen, expected, "the pairings of a race");
    }
    let waiting_targets: [fn(&mut Sim) -> Cancelling; 2] = [cancel_accept, cancel_recv];
    for waiting in waiting_targets {
        let mut seen = BTreeSet::new();
        for seed in 0..CHAOS {
            seen.insert(waiting(&mut Sim::new(seed, loopback_chaos())).pairing());
        }
        let expected = BTreeSet::from([Pairing::Stopped, Pairing::Interrupted]);
        assert_eq!(seen, expected, "the pairings of a target that waits for nothing");
    }
}

/// The peer's reset, answering a `Send` after it closed, is sometimes late
/// in a chaos world, as over a network: more than one `Send` succeeds.
#[test]
fn a_late_reset_lets_more_than_one_send_succeed() {
    let mut late = 0_u32;
    for seed in 0..CHAOS {
        if send_after_peer_closed(&mut Sim::new(seed, loopback_chaos())).sends.len() > 2 {
            late = late.checked_add(1).unwrap();
        }
    }
    assert!(late > 0, "no seed had a late reset");
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
