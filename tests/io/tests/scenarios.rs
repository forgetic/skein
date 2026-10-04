//! io's scenarios (io.md, 8), each over a few seeds: calm, where every
//! expectation holds whole, and chaotic, where streams may break. The sweeps
//! over hundreds of seeds are in `fuzzy_scenarios.rs`.

use skein_io::kernel::Error;
use skein_io_world::census::{Answer, Target, batched, cancels, discards, failures};
use skein_io_world::scenarios::{
    abort, accept, backlog, backpressure, batch, burst, close_deadline, closes, connects, descriptors, discard,
    exchange, refusal_mid_upload,
};
use skein_io_world::world::World;
use skein_sim::Config;
use skein_sim::Summary;

const CALM: [u64; 4] = [1, 2, 3, 4];
const CHAOS: [u64; 3] = [5, 6, 7];

fn calm(scenario: fn(u64, Config) -> World) {
    for seed in CALM {
        let _outcome = scenario(seed, Config::calm()).run();
    }
}

fn chaos(scenario: fn(u64, Config) -> World) {
    for seed in CHAOS {
        let _outcome = scenario(seed, Config::chaos()).run();
    }
}

#[test]
fn a_listener_accepts_binds_and_rejects() {
    calm(accept);
    chaos(accept);
}

#[test]
fn connects_are_made_refused_for_want_of_a_slot_and_refused_by_the_peer() {
    calm(connects);
    chaos(connects);
}

#[test]
fn connects_waiting_on_a_full_backlog_are_cancelled_by_their_abort() {
    for seed in CALM {
        let outcome = backlog(seed, Config::calm()).run();
        let answered = cancels(&outcome.trace);
        assert!(answered.contains(&(Target::Connect, Answer::Stopped)), "seed {seed}: a waiting connect stopped");
    }
    chaos(backlog);
}

#[test]
fn a_socket_and_an_accept_out_of_descriptors_are_refused_and_starved() {
    for seed in CALM {
        let outcome = descriptors(seed, Config::calm()).run();
        let failed = failures(&outcome.trace, Error::TooManyOpenFiles);
        assert!(failed.iter().any(|kind| matches!(kind, Summary::Socket { .. })), "seed {seed}: a socket found none");
        assert!(failed.iter().any(|kind| matches!(kind, Summary::Accept { .. })), "seed {seed}: an accept found none");
    }
    chaos(descriptors);
}

#[test]
fn the_accept_batch_holds_a_second_listener_back_an_iteration() {
    for seed in CALM {
        let outcome = batch(seed, Config::calm()).run();
        assert!(batched(&outcome.trace, &outcome.marks), "seed {seed}: a listener's first accept waited");
    }
    chaos(batch);
}

#[test]
fn a_socket_accepted_with_no_slot_left_is_discarded() {
    for seed in CALM {
        let outcome = discard(seed, Config::calm()).run();
        assert!(discards(&outcome.trace) > 0, "seed {seed}: a socket accepted was discarded");
    }
    chaos(discard);
}

#[test]
fn a_burst_of_connects_fills_the_refusals_and_io_takes_no_more_until_told() {
    for seed in CALM {
        let outcome = burst(seed, Config::calm()).run();
        assert!(outcome.held_back[1] > 0, "seed {seed}: io took no more requests for a while");
    }
    chaos(burst);
}

#[test]
fn bytes_go_both_ways_under_demands_of_every_kind() {
    calm(exchange);
    chaos(exchange);
}

#[test]
fn a_server_that_does_not_demand_stops_its_peer() {
    calm(backpressure);
    chaos(backpressure);
}

#[test]
fn a_response_sent_in_the_middle_of_an_upload_reaches_the_peer() {
    calm(refusal_mid_upload);
    chaos(refusal_mid_upload);
}

#[test]
fn an_abort_cancels_what_waits() {
    calm(abort);
    chaos(abort);
}

#[test]
fn a_close_the_peer_never_ends_is_aborted_by_its_deadline() {
    calm(close_deadline);
    chaos(close_deadline);
}

#[test]
fn closes_and_aborts_in_every_state_settle() {
    calm(closes);
    chaos(closes);
}

#[test]
fn the_same_seed_replays_to_the_same_trace() {
    for seed in [11_u64, 12] {
        let first = closes(seed, Config::chaos()).run();
        let second = closes(seed, Config::chaos()).run();
        assert!(first.trace == second.trace, "seed {seed}: the same records, in the same order, at the same times");
        assert_eq!(first.logs, second.logs, "seed {seed}: io told the same");
    }
    let one = exchange(1, Config::chaos()).run();
    let two = exchange(2, Config::chaos()).run();
    assert!(one.trace != two.trace, "another seed, another run");
}
