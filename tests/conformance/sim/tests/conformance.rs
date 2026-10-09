//! Every scenario, each over the calm seeds and a few of chaos; a failing
//! seed is named, with the end of its trace. The sweeps over many seeds are
//! in `fuzzy_conformance.rs`.

use skein_conformance::{
    Check, Pairing, Shortness, accept_past_the_descriptor_limit, address_in_use, backpressure, cancel_accept,
    cancel_connect, cancel_connect_established_while_away, cancel_read, cancel_recv, closed_before_accept, escapes,
    file_lifecycle, full_accept_queue, graceful_close, ipv6_only, lifecycle, list, listener_close_resets_waiting,
    make_directory, nested_roots, open_past_the_descriptor_limit, permissions, processes, refused, remove, rename,
    reset_after_end_of_stream, send_after_peer_closed, unread_close_meets_recv, unread_close_meets_send, wrong_state,
};
use skein_conformance_sim::{
    CALM, RACES, SMOKE, Simulated, cancel_chaos, each_seed, file_cancel_chaos, loopback_chaos, racing_accept,
    racing_recv,
};
use skein_io::kernel::Family;
use skein_sim::Config;

/// Calm, then a few seeds with the faults loopback can show.
fn calm_and_chaos<S: Check>(scenario: fn(&mut Simulated) -> S) {
    each_seed(Config::calm(), CALM, scenario);
    each_seed(loopback_chaos(), SMOKE, scenario);
}

#[test]
fn child_processes_and_pipes() {
    each_seed(Config::calm(), CALM, |world| processes(world, b"process_fixture"));
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
    assert_eq!((faults.no_space, faults.read_only, faults.io_error, faults.hung), (0, 0, 0, 0), "a healthy disk");
    assert!(faults.latency > 0 && faults.short_send > 0 && faults.short_recv > 0 && faults.cancel_race > 0);
    assert!(faults.short_read > 0 && faults.short_write > 0, "short reads and writes are a file's too");
    assert!(faults.late_reset > 0, "a late reset is loopback's too, as timing");
}

#[test]
fn a_file_is_made_written_at_offsets_read_back_and_stated() {
    calm_and_chaos(file_lifecycle);
}

/// A calm world counts every byte a read or a write could, as the ring
/// does for a regular file.
#[test]
fn a_calm_world_reads_and_writes_whole_as_the_ring_does() {
    let whole = Shortness { short: false, full: true };
    for seed in 0..CALM {
        let seen = file_lifecycle(&mut Simulated::new(seed, Config::calm()));
        assert_eq!((seen.reads, seen.writes()), (whole, whole), "seed {seed}: {seen:?}");
    }
}

#[test]
fn renames_over_across_and_beneath() {
    calm_and_chaos(rename);
}

#[test]
fn removes_of_files_directories_and_links() {
    calm_and_chaos(remove);
}

#[test]
fn new_directories_and_one_removed_while_open() {
    calm_and_chaos(make_directory);
}

#[test]
fn listings_whole_one_at_a_time_and_cut_short() {
    calm_and_chaos(list);
}

#[test]
fn a_root_beneath_a_root() {
    calm_and_chaos(nested_roots);
}

#[test]
fn paths_that_leave_their_root_and_paths_that_stay() {
    calm_and_chaos(escapes);
}

#[test]
fn what_the_owner_may_not_do() {
    calm_and_chaos(permissions);
}

/// Simulator only: the ring's descriptor limit is the process's, which a
/// test cannot lower without `unsafe` (programming-model.md, 2.1).
#[test]
fn an_open_past_the_descriptor_limit() {
    let few = |config: Config| Config { max_fds: 4, ..config };
    each_seed(few(Config::calm()), CALM, open_past_the_descriptor_limit);
    each_seed(few(loopback_chaos()), SMOKE, open_past_the_descriptor_limit);
}

/// A calm world answers a `Cancel` of a file's `Read` too late, the `Read`
/// having read, as the ring does in some runs; a world that hangs it is
/// stopped by one.
#[test]
fn a_cancel_of_a_read_of_a_file() {
    for seed in 0..CALM {
        let seen = cancel_read(&mut Simulated::new(seed, Config::calm()));
        seen.check();
        assert_eq!(seen.pairing(), Pairing::Completed, "seed {seed}: {seen:?}");
    }
    each_seed(loopback_chaos(), SMOKE, cancel_read);
    each_seed(file_cancel_chaos(), SMOKE, cancel_read);
}

#[test]
fn the_group_is_signalled_after_its_leader_exits() {
    calm_and_chaos(|world| skein_conformance::groups(world, b"process_fixture"));
}

#[test]
fn usage_counts_the_child_only_after_reaping() {
    calm_and_chaos(|world| skein_conformance::usage(world, b"process_fixture"));
}

#[test]
fn appends_preserve_the_prefix_and_follow_the_other_descriptors_bytes() {
    calm_and_chaos(skein_conformance::appending);
}

#[test]
fn a_cancel_of_an_append_stops_it_or_comes_too_late() {
    each_seed(Config::calm(), CALM, skein_conformance::cancel_append);
    each_seed(loopback_chaos(), SMOKE, skein_conformance::cancel_append);
    each_seed(file_cancel_chaos(), SMOKE, skein_conformance::cancel_append);
}
