//! The conformance suite against the ring on the real kernel, on loopback
//! (testing-pyramid.md, sections 5 and 8): every scenario of
//! `skein_sim::conformance` that loopback can provoke, each process of a
//! scenario a `Kernel` of its own. The same scenarios run against the
//! simulator in `skein-sim`'s tests.
//!
//! Not here: a failed `Accept` past the descriptor limit, which needs the
//! process's limit lowered, and so `unsafe` outside the ring adapter
//! (programming-style.md, 9.2); it runs on the simulator only.
//!
//! A machine without io_uring fails every test here, saying so.

use skein_io::kernel::{Complete, Family, Submit};
use skein_lib::{Duration, Queue, Time};
use skein_shell::{Clock, Config, Kernel, OpenError, Wait};
use skein_sim::conformance::{
    Backend, Check, Pairing, Race, address_in_use, backpressure, cancel_accept, cancel_accept_racing_a_connect,
    cancel_connect, cancel_connect_established_while_away, cancel_recv, cancel_recv_racing_bytes, closed_before_accept,
    full_accept_queue, graceful_close, ipv6_only, lifecycle, listener_close_resets_waiting, refused,
    reset_after_end_of_stream, send_after_peer_closed, unread_close_meets_recv, unread_close_meets_send, wrong_state,
};

/// Operations in flight per process: a scenario has a few at most.
const OPERATIONS: u32 = 16;

/// The longest one wait blocks on one process's ring before the others are
/// entered: their deferred completions run only when they are.
const SLICE: Duration = Duration::from_millis(5);

/// One `Kernel` per process.
struct Ring {
    kernels: Vec<Kernel>,
    clock: Clock,
}

impl Ring {
    fn new() -> Ring {
        Ring { kernels: Vec::new(), clock: Clock::new() }
    }

    fn kernel(&mut self, process: usize) -> &mut Kernel {
        self.kernels.get_mut(process).expect("a process of this ring")
    }
}

impl Backend for Ring {
    type Process = usize;

    fn open(&mut self) -> usize {
        let kernel = match Kernel::open(Config { operations: OPERATIONS }) {
            Ok(kernel) => kernel,
            // Rings closed a moment ago are freed by the kernel in its own
            // time: many opened at once can run out of memory, which is not
            // io_uring missing.
            Err(OpenError::Setup(libc::ENOMEM)) => {
                panic!("the kernel had no memory for another ring (ENOMEM): too many rings at once")
            }
            Err(error) => panic!("io_uring is not usable here, so the ring cannot be tested: {error}"),
        };
        self.kernels.push(kernel);
        self.kernels.len().checked_sub(1).expect("the kernel just pushed")
    }

    fn submit(&mut self, process: usize, records: &mut Queue<Submit>) {
        self.kernel(process).submit(records, Wait::No);
    }

    fn reap(&mut self, process: usize, completions: &mut Queue<Complete>) {
        self.kernel(process).reap(completions);
    }

    fn now(&self) -> Time {
        self.clock.now().now
    }

    fn enter(&mut self, process: usize) {
        self.kernel(process).submit(&mut Queue::with_capacity(0), Wait::No);
    }

    /// Enters every other process's ring, so that what they deferred runs,
    /// then waits on this one's for a slice of `bound`.
    fn pass(&mut self, process: usize, bound: Duration) {
        let mut nothing = Queue::with_capacity(0);
        for (other, kernel) in self.kernels.iter_mut().enumerate() {
            if other != process {
                kernel.submit(&mut nothing, Wait::No);
            }
        }
        let until = self.now().saturating_add(bound.min(SLICE));
        self.kernel(process).submit(&mut nothing, Wait::Until(until));
    }

    /// Blocks the thread: no ring is entered, while the kernel's network
    /// runs on.
    fn sleep(&mut self, span: Duration) {
        std::thread::sleep(std::time::Duration::from_nanos(span.as_nanos()));
    }

    fn assert_settled(&self, process: usize) {
        let kernel = self.kernels.get(process).expect("a process of this ring");
        assert_eq!(kernel.in_flight(), 0, "nothing in flight on process {process}");
    }
}

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
fn race(scenario: fn(&mut Ring, Race) -> skein_sim::conformance::Cancelling) {
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
