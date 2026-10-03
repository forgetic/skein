//! Every scenario, each over many seeds, calm and with chaos; a failing
//! seed is named, with the end of its trace.

use core::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use skein_io::kernel::{Done, Error, Family};
use skein_sim::conformance::{
    Check, accept_past_the_descriptor_limit, address_in_use, backpressure, cancel_accept,
    cancel_accept_racing_a_connect, cancel_connect, cancel_recv, cancel_recv_racing_bytes, closed_before_accept,
    full_accept_queue, graceful_close, ipv6_only, lifecycle, loopback_chaos, refused, reset_after_end_of_stream,
    send_after_peer_closed, unread_close_meets_recv, unread_close_meets_send, wrong_state,
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

#[test]
fn a_cancel_of_a_recv_racing_bytes() {
    let racing: [fn(&mut Sim) -> _; 2] =
        [|sim| cancel_recv_racing_bytes(sim, true), |sim| cancel_recv_racing_bytes(sim, false)];
    for scenario in racing {
        calm_and_chaos(scenario);
        each_seed(cancel_chaos(), CHAOS, scenario);
    }
}

#[test]
fn a_cancel_of_an_accept_racing_a_connect() {
    let racing: [fn(&mut Sim) -> _; 2] =
        [|sim| cancel_accept_racing_a_connect(sim, true), |sim| cancel_accept_racing_a_connect(sim, false)];
    for scenario in racing {
        calm_and_chaos(scenario);
        each_seed(cancel_chaos(), CHAOS, scenario);
    }
}

#[test]
fn a_cancel_of_a_waiting_connect() {
    calm_and_chaos(cancel_connect);
    each_seed(cancel_chaos(), CHAOS, cancel_connect);
}

/// Simulator only: the ring's descriptor limit is the process's, which a
/// test cannot lower without `unsafe` (programming-style.md, 9.2).
#[test]
fn an_accept_past_the_descriptor_limit() {
    let few = |config: Config| Config { max_fds: 4, ..config };
    each_seed(few(Config::calm()), CALM, accept_past_the_descriptor_limit);
    each_seed(few(loopback_chaos()), CHAOS, accept_past_the_descriptor_limit);
}

/// The simulator draws among the pairings the contract allows: over the
/// seeds, a cancel racing bytes both stops its target and is too late.
#[test]
fn a_racing_cancel_meets_each_outcome_over_the_seeds() {
    let (mut stopped, mut too_late) = (0_u32, 0_u32);
    for seed in 0..CHAOS {
        let seen = cancel_recv_racing_bytes(&mut Sim::new(seed, loopback_chaos()), true);
        seen.check();
        match seen.cancels.last() {
            Some(Ok(Done::Nothing)) => stopped = stopped.checked_add(1).unwrap(),
            Some(Err(Error::TooLate)) => too_late = too_late.checked_add(1).unwrap(),
            other => panic!("seed {seed}: a cancel answered {other:?}"),
        }
    }
    assert!(stopped > 0 && too_late > 0, "{stopped} stopped, {too_late} too late");
}

/// The chaos the suite runs with is loopback's: nothing it draws needs a
/// network beyond it.
#[test]
fn loopback_chaos_draws_no_fault_beyond_loopback() {
    let faults = loopback_chaos().faults;
    assert_eq!((faults.reset, faults.refuse, faults.timed_out, faults.no_buffer), (0, 0, 0, 0));
    assert_eq!(faults.cancel_unsubmitted, 0);
    assert!(faults.latency > 0 && faults.short_send > 0 && faults.short_recv > 0 && faults.cancel_race > 0);
}
