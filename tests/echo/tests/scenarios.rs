//! The echo's scenarios (examples.md, 7), each over a few seeds: calm, where
//! every expectation holds whole, and chaotic, where streams may break and
//! every connection must still finish. Each admission point's scenario
//! checks what its clients saw for the evidence that it was reached. Every
//! world checks memory at every iteration, under the counting allocator.
//! The sweeps over many seeds are in `fuzzy_scenarios.rs`.

use skein_echo_world::scenarios::{
    EchoWorld, backpressure, busy, clients, closes, half_close, idle, shutdown, too_long, worst,
};
use skein_heap::Counting;
use skein_sim::Config;
use skein_world::Outcome;

use skein_echo_world::proc::Proc;

#[global_allocator]
static HEAP: Counting = Counting;

const CALM: [u64; 3] = [1, 2, 3];
const CHAOS: [u64; 3] = [4, 5, 6];

fn calm(scenario: fn(u64, Config) -> EchoWorld) -> Vec<Outcome<Proc>> {
    CALM.iter().map(|seed| scenario(*seed, Config::calm()).run()).collect()
}

fn chaos(scenario: fn(u64, Config) -> EchoWorld) {
    for seed in CHAOS {
        let _outcome = scenario(seed, Config::chaos()).run();
    }
}

/// What connection `conn` of process `at` saw, at the end of a run.
fn seen(outcome: &Outcome<Proc>, at: usize, conn: u32) -> skein_echo_client::Seen {
    outcome.procs[at].as_client().expect("a fake client").seen(conn)
}

#[test]
fn many_clients_are_served_past_both_refusals_at_the_entrance() {
    let mut busy = 0;
    let mut rejected = 0;
    for outcome in calm(clients) {
        for at in 1..=3 {
            for conn in 0..2 {
                let seen = seen(&outcome, at, conn);
                busy += seen.busy;
                // A socket the protocol layer rejects is closed by io with
                // nothing said: the client hears the end, or, when its first
                // line was already there unread, a reset. Calm, nothing else
                // ends a stream unanswered.
                rejected += seen.silent + seen.broken;
            }
        }
    }
    assert!(busy > 0, "some connection was told busy at the domain's entrance");
    assert!(rejected > 0, "some connection was rejected at the protocol layer's entrance");
    chaos(clients);
}

#[test]
fn a_line_too_long_is_refused_and_the_connection_closed() {
    for outcome in calm(too_long) {
        assert_eq!(seen(&outcome, 1, 0).answered, 2, "the lines before it were answered");
    }
    chaos(too_long);
}

#[test]
fn a_peer_past_the_sessions_is_told_busy_and_one_past_the_connections_is_rejected() {
    for outcome in calm(busy) {
        let (refused, rejected) = (seen(&outcome, 1, 2), seen(&outcome, 1, 3));
        assert_eq!(refused.busy, 1, "the first of the two late ones is told busy");
        assert_eq!(rejected.silent, 1, "the second finds the protocol's connections full: rejected, unanswered");
    }
    chaos(busy);
}

#[test]
fn idle_connections_are_closed_at_their_deadline_and_not_before() {
    calm(idle);
    chaos(idle);
}

#[test]
fn a_client_that_stops_reading_is_stopped_by_backpressure() {
    calm(backpressure);
    chaos(backpressure);
}

#[test]
fn closes_and_resets_in_every_state_settle() {
    calm(closes);
    chaos(closes);
}

#[test]
fn a_shutdown_stops_the_listener_and_lets_its_connections_run_on() {
    calm(shutdown);
    chaos(shutdown);
}

/// A pinned seed: under chaos, a stream of the echo's fails while its line
/// is out with the domain (Answering, then `Failed`), which the domain then
/// answers into a connection closing. Rare, about one seed in four hundred
/// of `clients`, so kept here; found by marking the cell and sweeping.
#[test]
fn a_stream_that_fails_while_its_line_is_answered_settles() {
    let _outcome = clients(387, Config::chaos()).run();
}

#[test]
fn the_same_seed_replays_to_the_same_trace() {
    for seed in [11_u64, 12] {
        let first = closes(seed, Config::chaos()).run();
        let second = closes(seed, Config::chaos()).run();
        assert!(first.trace == second.trace, "seed {seed}: the same records, in the same order, at the same times");
        for at in 1..=2 {
            for conn in 0..3 {
                assert_eq!(seen(&first, at, conn), seen(&second, at, conn), "seed {seed}: the clients saw the same");
            }
        }
    }
    let one = clients(1, Config::chaos()).run();
    let two = clients(2, Config::chaos()).run();
    assert!(one.trace != two.trace, "another seed, another run");
}

#[test]
fn memory_is_checked_at_every_iteration_against_the_worst_cases() {
    let outcome = clients(1, Config::calm()).run();
    let heap = outcome.heap.as_ref().expect("checked");
    assert_eq!(heap.len(), 4, "the echo and three clients, each checked against its own");
    for (at, (most, bound)) in heap.iter().enumerate() {
        assert!(*most > 0 && most <= bound, "process {at} held {most} bytes at most, within {bound}");
    }
    // Dropping the outcome checks that each process frees what it held.
}

#[test]
fn a_half_close_with_lines_unanswered_has_every_whole_line_answered_then_the_end() {
    calm(half_close);
    chaos(half_close);
}

/// Every connection's intake, receive and output full at once, the echo
/// holds about three fifths of its worst case. The rest is the bookkeeping
/// of io's and the protocol layer's B-tree tables (deadlines, ready lists),
/// whose worst cases count a full table's nodes while a few timers are
/// armed. A floor of half catches a world that stops reaching the limits.
#[test]
fn the_echo_driven_to_its_limits_comes_near_its_worst_case() {
    for outcome in calm(worst) {
        let (most, bound) = outcome.heap.as_ref().expect("checked")[0];
        assert!(most * 2 >= bound, "seed {}: the echo held {most} bytes at its limits, of {bound}", outcome.seed);
    }
    chaos(worst);
}
