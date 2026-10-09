//! Every scenario over many seeds of chaos, and the outcomes the simulator
//! draws among over them: each pairing of a race, and a late reset. A
//! failing seed is named, with the end of its trace.

use std::collections::BTreeSet;

use skein_conformance::{
    Cancelling, Check, Pairing, Race, Shortness, accept_past_the_descriptor_limit, address_in_use, backpressure,
    cancel_accept, cancel_connect, cancel_connect_established_while_away, cancel_read, cancel_recv,
    closed_before_accept, escapes, file_lifecycle, file_metadata, full_accept_queue, graceful_close, ipv6_only,
    lifecycle, list, listener_close_resets_waiting, make_directory, nested_roots, open_past_the_descriptor_limit,
    permissions, refused, remove, rename, reset_after_end_of_stream, send_after_peer_closed, unread_close_meets_recv,
    unread_close_meets_send, wrong_state,
};
use skein_conformance_sim::{
    CHAOS, RACES, Racing, Simulated, cancel_chaos, each_seed, file_cancel_chaos, loopback_chaos, racing_accept,
    racing_recv,
};
use skein_io::kernel::Family;
use skein_sim::Config;

/// With the faults loopback can show.
fn chaos<S: Check>(scenario: fn(&mut Simulated) -> S) {
    each_seed(loopback_chaos(), CHAOS, scenario);
}

#[test]
fn a_connection_lives_and_ends_over_ipv4() {
    chaos(|world| lifecycle(world, Family::Ipv4));
}

#[test]
fn a_connection_lives_and_ends_over_ipv6() {
    chaos(|world| lifecycle(world, Family::Ipv6));
}

#[test]
fn a_peer_closes_gracefully() {
    chaos(graceful_close);
}

#[test]
fn a_send_after_the_peer_closed() {
    chaos(send_after_peer_closed);
}

#[test]
fn a_connect_where_nothing_listens() {
    chaos(refused);
}

#[test]
fn where_an_address_is_in_use() {
    chaos(address_in_use);
}

#[test]
fn ipv6_sockets_are_ipv6_only() {
    chaos(ipv6_only);
}

#[test]
fn records_wrong_for_the_socket_state() {
    chaos(wrong_state);
}

#[test]
fn a_full_accept_queue() {
    chaos(full_accept_queue);
}

#[test]
fn a_close_with_bytes_unread_meets_a_recv() {
    chaos(unread_close_meets_recv);
}

#[test]
fn a_close_with_bytes_unread_meets_a_send() {
    chaos(unread_close_meets_send);
}

#[test]
fn a_reset_after_the_end_of_stream() {
    chaos(reset_after_end_of_stream);
}

#[test]
fn a_client_closed_before_its_connection_is_accepted() {
    chaos(closed_before_accept);
}

#[test]
fn backpressure_stalls_a_sender_and_reading_resumes_it() {
    chaos(backpressure);
}

#[test]
fn a_cancel_of_a_waiting_accept() {
    chaos(cancel_accept);
    each_seed(cancel_chaos(), CHAOS, cancel_accept);
}

#[test]
fn a_cancel_of_a_waiting_recv() {
    chaos(cancel_recv);
    each_seed(cancel_chaos(), CHAOS, cancel_recv);
}

#[test]
fn a_cancel_of_a_recv_racing_bytes() {
    for race in RACES {
        chaos(racing_recv(race));
        each_seed(cancel_chaos(), CHAOS, racing_recv(race));
    }
}

#[test]
fn a_cancel_of_an_accept_racing_a_connect() {
    for race in RACES {
        chaos(racing_accept(race));
        each_seed(cancel_chaos(), CHAOS, racing_accept(race));
    }
}

#[test]
fn a_cancel_of_a_waiting_connect() {
    chaos(cancel_connect);
    each_seed(cancel_chaos(), CHAOS, cancel_connect);
}

#[test]
fn a_cancel_of_a_connect_established_while_its_client_was_away() {
    chaos(cancel_connect_established_while_away);
    each_seed(cancel_chaos(), CHAOS, cancel_connect_established_while_away);
}

#[test]
fn closing_a_listener_resets_the_connections_waiting_on_it() {
    chaos(listener_close_resets_waiting);
}

/// Simulator only: the ring's descriptor limit is the process's, which a
/// test cannot lower without `unsafe` (programming-model.md, 2.1).
#[test]
fn an_accept_past_the_descriptor_limit() {
    each_seed(Config { max_fds: 4, ..loopback_chaos() }, CHAOS, accept_past_the_descriptor_limit);
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
                    seen.insert(racing(race)(&mut Simulated::new(seed, config)).pairing());
                }
            }
        }
        let expected = BTreeSet::from([Pairing::Stopped, Pairing::Completed, Pairing::RanOn]);
        assert_eq!(seen, expected, "the pairings of a race");
    }
    let waiting_targets: [fn(&mut Simulated) -> Cancelling; 2] = [cancel_accept, cancel_recv];
    for waiting in waiting_targets {
        let mut seen = BTreeSet::new();
        for seed in 0..CHAOS {
            seen.insert(waiting(&mut Simulated::new(seed, loopback_chaos())).pairing());
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
        if send_after_peer_closed(&mut Simulated::new(seed, loopback_chaos())).sends.len() > 2 {
            late = late.checked_add(1).unwrap();
        }
    }
    assert!(late > 0, "no seed had a late reset");
}

#[test]
fn a_file_is_made_written_at_offsets_read_back_and_stated() {
    chaos(file_lifecycle);
}

#[test]
fn renames_over_across_and_beneath() {
    chaos(rename);
}

#[test]
fn removes_of_files_directories_and_links() {
    chaos(remove);
}

#[test]
fn new_directories_and_one_removed_while_open() {
    chaos(make_directory);
}

#[test]
fn listings_whole_one_at_a_time_and_cut_short() {
    chaos(list);
}

#[test]
fn a_root_beneath_a_root() {
    chaos(nested_roots);
}

#[test]
fn paths_that_leave_their_root_and_paths_that_stay() {
    chaos(escapes);
}

#[test]
fn what_the_owner_may_not_do() {
    chaos(permissions);
}

/// Simulator only: the ring's descriptor limit is the process's.
#[test]
fn an_open_past_the_descriptor_limit() {
    each_seed(Config { max_fds: 4, ..loopback_chaos() }, CHAOS, open_past_the_descriptor_limit);
}

/// The simulator draws among the counts the contract allows a read and a
/// write: over the seeds, each is cut short, and each counts every byte,
/// which is what the ring does.
#[test]
fn the_simulator_cuts_reads_and_writes_short_and_whole_over_the_seeds() {
    let mut reads = Shortness::default();
    let mut writes = Shortness::default();
    for seed in 0..CHAOS {
        let seen = file_lifecycle(&mut Simulated::new(seed, loopback_chaos()));
        reads = reads.and(seen.reads);
        writes = writes.and(seen.writes());
    }
    let both = Shortness { short: true, full: true };
    assert_eq!((reads, writes), (both, both), "every outcome of a count, over the seeds");
}

/// The simulator draws among the pairings the contract allows a `Cancel` of
/// a file's `Read`: too late for one that read, and stopping, or
/// interrupting, one that hung, as the ring stops one its worker has not
/// read yet; not submitted, the `Read` running on.
#[test]
fn a_cancel_of_a_read_of_a_file_meets_each_pairing_over_the_seeds() {
    let mut seen = BTreeSet::new();
    for seed in 0..CHAOS {
        let mut world = Simulated::new(seed, file_cancel_chaos());
        let cancelling = cancel_read(&mut world);
        cancelling.check();
        seen.insert(cancelling.pairing());
    }
    let expected = BTreeSet::from([Pairing::Stopped, Pairing::Interrupted, Pairing::Completed, Pairing::RanOn]);
    assert_eq!(seen, expected, "the pairings of a cancelled Read of a file");
}

#[test]
fn groups_and_usage_under_chaos() {
    chaos(|world| skein_conformance::groups(world, b"process_fixture"));
    chaos(|world| skein_conformance::usage(world, b"process_fixture"));
}

#[test]
fn appends_continue_short_counts_and_cancel_meets_every_pairing() {
    let mut shorts = Shortness::default();
    let mut pairings = BTreeSet::new();
    for seed in 0..CHAOS {
        let seen = skein_conformance::appending(&mut Simulated::new(seed, loopback_chaos()));
        seen.check();
        shorts = shorts.and(seen.counts);
        let cancel = skein_conformance::cancel_append(&mut Simulated::new(seed, file_cancel_chaos()));
        cancel.check();
        pairings.insert(cancel.pairing());
    }
    assert_eq!(shorts, Shortness { short: true, full: true });
    assert_eq!(pairings, BTreeSet::from([Pairing::Stopped, Pairing::Interrupted, Pairing::Completed, Pairing::RanOn]));
}

#[test]
fn stat_keeps_the_owner_and_counts_hard_links_through_removal() {
    chaos(file_metadata);
}
