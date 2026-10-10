//! The conformance suite against the ring on the real kernel, on loopback
//! and in scratch directories (kernel.md, 8): every scenario of
//! `skein_conformance` that loopback and a healthy scratch directory can
//! provoke, each process of a scenario a `Kernel` of its own. The same
//! scenarios run against the simulator in `tests/conformance/sim`.
//!
//! Not here: a failed `Accept` or `Open` past the descriptor limit, which
//! needs the process's limit lowered, and so `unsafe` outside the ring
//! adapter (programming-model.md, 2.1); they run on the simulator only.
//!
//! A machine without `io_uring` fails every test here, saying so; so does
//! a run as root, whom no mode stops, the scenario on permissions.

use skein_conformance::{
    Cancelling, Check, Pairing, Race, Shortness, address_in_use, backpressure, cancel_accept,
    cancel_accept_racing_a_connect, cancel_connect, cancel_connect_established_while_away, cancel_read, cancel_recv,
    cancel_recv_racing_bytes, closed_before_accept, escapes, file_lifecycle, file_metadata, full_accept_queue,
    graceful_close, ipv6_only, lifecycle, list, listener_close_resets_waiting, make_directory, nested_roots,
    permissions, processes, refused, remove, rename, reset_after_end_of_stream, send_after_peer_closed,
    unread_close_meets_recv, unread_close_meets_send, wrong_state,
};
use skein_conformance_ring::Ring;
use skein_io::kernel::Family;

/// Runs `scenario` on a ring of its own and checks what it saw.
fn on_the_ring<S: Check>(scenario: fn(&mut Ring) -> S) {
    scenario(&mut Ring::new()).check();
}

#[test]
fn child_processes_and_pipes() {
    processes(&mut Ring::new(), env!("CARGO_BIN_EXE_process_fixture").as_bytes()).check();
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

/// A regular file's reads and writes count every byte they could: one of
/// the outcomes the contract allows, the one the calm simulator gives.
#[test]
fn a_file_is_made_written_at_offsets_read_back_and_stated() {
    let seen = file_lifecycle(&mut Ring::new());
    seen.check();
    let whole = Shortness { short: false, full: true };
    assert_eq!((seen.reads, seen.writes()), (whole, whole), "{seen:?}");
}

#[test]
fn renames_over_across_and_beneath() {
    on_the_ring(rename);
}

#[test]
fn removes_of_files_directories_and_links() {
    on_the_ring(remove);
}

#[test]
fn new_directories_and_one_removed_while_open() {
    on_the_ring(make_directory);
}

#[test]
fn listings_whole_one_at_a_time_and_cut_short() {
    on_the_ring(list);
}

#[test]
fn a_root_beneath_a_root() {
    on_the_ring(nested_roots);
}

#[test]
fn paths_that_leave_their_root_and_paths_that_stay() {
    on_the_ring(escapes);
}

#[test]
fn what_the_owner_may_not_do() {
    assert!(skein_conformance_ring::Ring::permissions_checked(), "run as root, whom no mode stops: run as a user");
    on_the_ring(permissions);
}

/// A file's `Read` goes to the ring's worker, even on tmpfs, so its `Cancel`
/// stops it, or comes once it has read: either, as the simulator draws
/// them. That a `Cancel` stops one that waits on a filesystem that stalls,
/// no scratch directory shows; the simulator's `hung` fault does.
#[test]
fn a_cancel_of_a_read_of_a_file_stops_it_or_is_too_late() {
    let seen = cancel_read(&mut Ring::new());
    seen.check();
    let drawn = [Pairing::Stopped, Pairing::Interrupted, Pairing::Completed];
    assert!(drawn.contains(&seen.pairing()), "a pairing the simulator draws: {seen:?}");
}

#[test]
fn the_group_is_signalled_after_its_leader_exits() {
    skein_conformance::groups(&mut Ring::new(), env!("CARGO_BIN_EXE_process_fixture").as_bytes()).check();
}

#[test]
fn usage_counts_the_child_only_after_reaping() {
    skein_conformance::usage(&mut Ring::new(), env!("CARGO_BIN_EXE_process_fixture").as_bytes()).check();
}

#[test]
fn appends_preserve_the_prefix_and_follow_the_other_descriptors_bytes() {
    on_the_ring(skein_conformance::appending);
}

#[test]
fn a_cancel_of_an_append_stops_it_or_comes_too_late() {
    on_the_ring(skein_conformance::cancel_append);
}

#[test]
fn stat_keeps_the_owner_and_counts_hard_links_through_removal() {
    use std::os::unix::fs::MetadataExt;

    let seen = file_metadata(&mut Ring::new());
    seen.check();
    let process = std::fs::metadata("/proc/self").expect("the running user's process directory");
    assert_eq!(seen.root.owner, process.uid(), "statx retains the actual user ID");
}

#[test]
fn path_only_metadata_needs_no_permission_on_its_entry() {
    let status = std::fs::read_to_string("/proc/self/status").expect("Linux capability status");
    let capabilities =
        status.lines().find_map(|line| line.strip_prefix("CapEff:")).expect("effective capabilities").trim();
    let capabilities = u64::from_str_radix(capabilities, 16).expect("hexadecimal effective capabilities");
    let denial_expected = capabilities & (1_u64 << 1_u32) == 0;
    if !denial_expected {
        eprintln!(
            "CAP_DAC_OVERRIDE bypasses mode 000: skipping only the Read-denial assertions; path-only Stat and Close are checked"
        );
    }
    skein_conformance::path_metadata(&mut Ring::new(), denial_expected).check();
}
