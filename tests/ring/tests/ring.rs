//! The ring adapter against the real kernel, on loopback (shell.md,
//! 9): records submitted directly, as io will, and every
//! completion checked against the contract of `skein_io::kernel` as it
//! arrives. What is the adapter's own: its slots and waits, a large transfer
//! through short sends, what it completes itself, the broken invariants it
//! asserts, dropping it. The sockets' behaviour is the conformance suite's,
//! in `tests/conformance/ring`.
//!
//! A machine without `io_uring` (a seccomp profile, `io_uring_disabled`) fails
//! every test here, saying so, rather than passing them silently.

use skein_io::kernel::{Complete, Done, Error, Family, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token};
use skein_ring_tests::{PATIENCE, World, filled, pattern, v4};
use skein_shell::{Clock, Config, Kernel, OpenError, Wait};

#[test]
fn a_large_transfer_takes_short_sends_and_many_receives() {
    const LEN: usize = 32 << 20;
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v4(0));
    let (client, server, _peer) = world.connection(listener, addr);
    let bytes = pattern(LEN);

    // Nothing reads yet, so the first send takes what the socket buffers hold.
    let first = world.run(Op::send(client, bytes.clone(), 0).unwrap());
    let Ok(Done::Count(first_sent)) = first.result else { panic!("a send: {first:?}") };
    assert!(usize::try_from(first_sent).unwrap() < LEN, "a short send: {first_sent} of {LEN}");

    let mut received = Vec::with_capacity(LEN);
    let mut reads = 0_u32;
    let mut from = first_sent;
    let mut sending = None;
    let mut receiving = None;
    let deadline = world.later(Duration::from_secs(60));
    while received.len() < LEN {
        assert!(world.clock.now().now < deadline, "the transfer finished in time");
        if sending.is_none() && usize::try_from(from).unwrap() < LEN {
            sending = Some(world.start(Op::send(client, bytes.clone(), from).unwrap()));
        }
        if receiving.is_none() {
            receiving = Some(world.start(Op::recv(server, vec![0; 64 << 10].into_boxed_slice()).unwrap()));
        }
        world.turn(deadline);
        if let Some(op) = sending
            && let Some(sent) = world.arrived.remove(&op)
        {
            let Ok(Done::Count(n)) = sent.result else { panic!("a send: {sent:?}") };
            from = from.checked_add(n).unwrap();
            sending = None;
        }
        if let Some(op) = receiving
            && let Some(got) = world.arrived.remove(&op)
        {
            let (Ok(Done::Count(n)), Op::Recv { buf, .. }) = (got.result, got.kind) else { panic!("a receive") };
            assert_ne!(n, 0, "the stream has not ended");
            received.extend_from_slice(filled(&buf, n));
            reads = reads.checked_add(1).unwrap();
            receiving = None;
        }
    }
    assert_eq!(usize::try_from(from).unwrap(), LEN, "every byte was sent once");
    assert!(sending.is_none(), "no send left");
    assert!(reads > 1, "many receives: {reads}");
    assert!(received == *bytes, "the bytes arrived in order");

    for fd in [client, server, listener] {
        world.close(fd);
    }
    world.settle();
}

/// The cancel lands, or loses the race; either way the target says what it
/// did, and both complete once.
fn cancel_answer(cancel: &Complete) {
    assert!(
        cancel.result == Ok(Done::Nothing) || cancel.result == Err(Error::TooLate),
        "a cancel finds its target or is too late: {cancel:?}"
    );
}

#[test]
fn a_cancel_of_what_is_not_in_flight_is_too_late() {
    let mut world = World::new(4);
    let fd = world.socket(Family::Ipv4);
    let never = world.token();
    assert_eq!(world.run(Op::Cancel { target: never }).result, Err(Error::TooLate));
    let done = world.start(Op::Listen { fd, backlog: 1 });
    assert_eq!(world.wait(done).result, Ok(Done::Nothing));
    assert_eq!(world.run(Op::Cancel { target: done }).result, Err(Error::TooLate), "it completed");
    world.close(fd);
    world.settle();
}

#[test]
fn submit_takes_no_record_past_the_operations_configured() {
    let mut world = World::new(2);
    let (first, first_addr) = world.listener(v4(0));
    let (second, _second_addr) = world.listener(v4(0));

    let accept_first = world.token();
    let accept_second = world.token();
    let cancel = world.token();
    world.submissions.push(Submit { op: accept_first, kind: Op::Accept { fd: first } });
    world.submissions.push(Submit { op: accept_second, kind: Op::Accept { fd: second } });
    world.submissions.push(Submit { op: cancel, kind: Op::Cancel { target: accept_second } });
    world.kernel.submit(&mut world.submissions, Wait::No);
    assert_eq!(world.submissions.len(), 1, "the third record waits");
    assert_eq!((world.kernel.in_flight(), world.kernel.room()), (2, 0));
    world.outstanding.extend([accept_first, accept_second]);
    let soon = world.later(Duration::from_millis(20));
    world.kernel.submit(&mut world.submissions, Wait::Until(soon));
    assert_eq!(world.submissions.len(), 1, "still no room");

    // A client on a kernel of its own completes the first accept, which
    // frees a slot for the cancel.
    let mut client_world = World::new(2);
    let client = client_world.socket(Family::Ipv4);
    let connect = client_world.start(Op::Connect { fd: client, addr: first_addr });
    let Ok(Done::Accepted { fd: server, .. }) = world.wait(accept_first).result else { panic!("an accept") };
    assert_eq!(client_world.wait(connect).result, Ok(Done::Nothing));

    world.kernel.submit(&mut world.submissions, Wait::No);
    assert!(world.submissions.is_empty(), "the cancel found room");
    world.outstanding.insert(cancel);
    cancel_answer(&world.wait(cancel));
    assert_eq!(world.wait(accept_second).result, Err(Error::Cancelled));

    client_world.close(client);
    client_world.settle();
    for fd in [server, first, second] {
        world.close(fd);
    }
    world.settle();
}

#[test]
fn a_wait_with_a_deadline_returns_by_it_when_nothing_completes() {
    let mut world = World::new(4);
    for in_flight in [false, true] {
        let pending = if in_flight {
            let (listener, _addr) = world.listener(v4(0));
            Some((listener, world.start(Op::Accept { fd: listener })))
        } else {
            None
        };
        let deadline = world.later(Duration::from_millis(50));
        world.kernel.submit(&mut world.submissions, Wait::Until(deadline));
        let woke = world.clock.now().now;
        assert!(woke >= deadline, "it waited for the deadline");
        assert!(woke < deadline.checked_add(Duration::from_secs(1)).unwrap(), "and returned by it");
        world.reap();
        assert!(world.arrived.is_empty(), "nothing completed");
        if let Some((listener, accept)) = pending {
            let cancel = world.start(Op::Cancel { target: accept });
            cancel_answer(&world.wait(cancel));
            assert_eq!(world.wait(accept).result, Err(Error::Cancelled));
            world.close(listener);
        }
    }
    world.settle();
}

#[test]
fn a_deadline_already_past_does_not_wait() {
    let mut world = World::new(4);
    let before = world.clock.now().now;
    world.kernel.submit(&mut world.submissions, Wait::Until(Time::ZERO));
    let after = world.clock.now().now;
    assert!(after < before.checked_add(Duration::from_millis(500)).unwrap(), "it returned at once");
    world.settle();
}

#[test]
fn a_kernel_of_no_operations_does_not_open() {
    assert_eq!(Kernel::open(Config { operations: 0 }).unwrap_err(), OpenError::NoOperations);
}

#[test]
#[should_panic(expected = "io submits only valid records")]
fn an_invalid_record_is_refused_at_submit() {
    let mut world = World::new(4);
    world.submit(Token::new(1), Op::Recv { fd: Fd::new(0), buf: Box::from([]) });
}

#[test]
#[should_panic(expected = "io never reuses a token in flight")]
fn a_token_already_in_flight_is_refused_at_submit() {
    let mut world = World::new(4);
    let (listener, _addr) = world.listener(v4(0));
    let accept = world.start(Op::Accept { fd: listener });
    world.submit(accept, Op::Close { fd: listener });
}

#[test]
fn the_clock_moves_forward_and_the_seed_varies() {
    let clock = Clock::new();
    let first = clock.now();
    let second = clock.now();
    assert!(second.now >= first.now, "monotonic");
    assert!(first.wall.as_secs() > 1_700_000_000, "the wall clock is set: {first:?}");
    let seeds = [skein_shell::seed().unwrap(), skein_shell::seed().unwrap()];
    assert_ne!(seeds[0], seeds[1], "two seeds differ");
}

#[test]
fn a_kernel_dropped_with_operations_in_flight_leaves_their_memory_to_the_kernel() {
    let mut world = World::new(8);
    let (listener, addr) = world.listener(v4(0));
    let (client, server, _peer) = world.connection(listener, addr);
    let _accept = world.start(Op::Accept { fd: listener });
    let _recv = world.start(Op::recv(server, vec![0; 16].into_boxed_slice()).expect("room to receive"));
    drop(world);

    // The descriptors outlive the ring: only a Close record closes one.
    let mut after = World::new(4);
    assert_eq!(after.send(client, b"late"), Ok(Done::Count(4)), "the connection is still open");
    for fd in [client, server, listener] {
        after.close(fd);
    }
    after.settle();
}

#[test]
fn a_wait_forever_returns_with_a_completion() {
    let mut world = World::new(4);
    let socket = world.start(Op::Socket { family: Family::Ipv4 });
    world.kernel.submit(&mut world.submissions, Wait::Forever);
    world.reap();
    let Some(Complete { result: Ok(Done::Fd(fd)), .. }) = world.arrived.remove(&socket) else {
        panic!("the socket completed before the wait returned")
    };
    world.close(fd);
    world.settle();
}

#[test]
#[should_panic(expected = "waiting forever with nothing in flight never returns")]
fn a_wait_forever_with_nothing_in_flight_is_refused() {
    let mut world = World::new(4);
    world.kernel.submit(&mut world.submissions, Wait::Forever);
}

#[test]
fn a_record_the_adapter_completes_itself_arrives_at_the_next_reap_and_cannot_be_cancelled() {
    let mut world = World::new(4);
    // SO_REUSEADDR, set before the Bind goes to the ring, fails on a
    // descriptor that does not exist, so the Bind never reaches the ring.
    let bind = world.token();
    let cancel = world.token();
    world.submissions.push(Submit { op: bind, kind: Op::Bind { fd: Fd::new(-1), addr: v4(0) } });
    world.submissions.push(Submit { op: cancel, kind: Op::Cancel { target: bind } });
    world.kernel.submit(&mut world.submissions, Wait::No);
    world.outstanding.extend([bind, cancel]);
    assert_eq!(world.wait(bind).result, Err(Error::Other(libc::EBADF)), "the backend's own failure");
    assert_eq!(world.wait(cancel).result, Err(Error::TooLate), "nothing in the ring to stop");
    world.settle();
}

#[test]
fn a_reap_takes_only_what_fits_and_the_rest_waits_without_blocking() {
    let mut world = World::new(4);
    let sockets = [world.token(), world.token(), world.token()];
    for op in sockets {
        world.submissions.push(Submit { op, kind: Op::Socket { family: Family::Ipv4 } });
    }
    world.kernel.submit(&mut world.submissions, Wait::No);
    world.outstanding.extend(sockets);

    let mut one = Queue::with_capacity(1);
    let deadline = world.later(PATIENCE);
    while one.is_empty() {
        world.kernel.submit(&mut world.submissions, Wait::Until(deadline));
        world.kernel.reap(&mut one);
    }
    assert_eq!(world.kernel.in_flight(), 2, "two completions wait");
    for _ in 0..2 {
        // Completions are waiting, so a wait with a far deadline returns at once.
        let before = world.clock.now().now;
        world.kernel.submit(&mut world.submissions, Wait::Until(deadline));
        let after = world.clock.now().now;
        assert!(
            after < before.checked_add(Duration::from_millis(500)).expect("in reach"),
            "no wait while completions are queued"
        );
        world.check(&mut one);
        world.kernel.reap(&mut one);
        assert_eq!(one.len(), 1, "one more, as much as fits");
    }
    world.check(&mut one);
    assert_eq!(world.kernel.in_flight(), 0, "all three reaped");
    for op in sockets {
        let Ok(Done::Fd(fd)) = world.wait(op).result else { panic!("a socket") };
        world.close(fd);
    }
    world.settle();
}
